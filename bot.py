"""私人 Discord YouTube 音樂機器人（discord.py + yt-dlp + FFmpeg）"""

import asyncio
import json
import logging
import os
import re
import subprocess
import threading
from collections import deque
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import datetime

import aiohttp
import discord
import yt_dlp
from discord import app_commands
from discord.ext import commands
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import get_default_environment, stdio_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("musicbot")

TOKEN = os.environ["DISCORD_TOKEN"]
GUILD_ID = os.environ.get("GUILD_ID")            # 填了的話 slash 指令會立即同步到該伺服器
COOKIES = os.environ.get("YTDLP_COOKIES", "/app/cookies.txt")
IDLE_TIMEOUT = int(os.environ.get("IDLE_TIMEOUT", "300"))   # 佇列空了多久後自動離開（秒）
MAX_PLAYLIST = int(os.environ.get("MAX_PLAYLIST", "50"))    # 一次最多加入幾首播放清單歌曲

LLAMA_API_URL = os.environ.get("LLAMA_API_URL", "").rstrip("/")  # 例如 http://192.168.0.66:8000/v1，留空則不啟用聊天
LLAMA_API_KEY = os.environ.get("LLAMA_API_KEY", "")
CHAT_HISTORY = int(os.environ.get("CHAT_HISTORY", "20"))    # 聊天時讀取頻道最近幾則訊息當上下文
CHAT_SYSTEM_PROMPT = os.environ.get("CHAT_SYSTEM_PROMPT") or (
    "你是 Discord 伺服器裡的聊天夥伴兼音樂機器人，名字叫 {name}。"
    "請用繁體中文、口語、簡短地回覆，像朋友聊天一樣，必要時才分段。"
    "對話紀錄中每則使用者訊息的開頭是「發言者名稱：」，回覆時不要加這個前綴。"
)
CHAT_TOOL_PROMPT = (
    "你可以用工具真的操作音樂播放：有人想聽歌或要你點歌時呼叫 play_music（一次一首，要多首就分多次呼叫），"
    "要切歌呼叫 skip_song，問現在在播什麼呼叫 get_queue。"
    "絕對不要只用文字假裝已經點歌或切歌，一定要呼叫工具，並根據工具回傳的結果回覆。"
    "點播者是發出最後一則訊息的人。"
)
CHAT_MCP_PROMPT = (
    "需要最新資訊、新聞或你不確定的事實時，先用網路搜尋工具查再回答，並附上參考網址；"
    "數學計算一律用計算工具，不要自己心算。"
)
CHAT_MAX_TOOL_ROUNDS = 5   # 一則訊息最多連續呼叫幾輪工具（模型一次只會點一首）

MCP_CONFIG = os.environ.get("MCP_CONFIG", "/app/mcp_servers.json")   # MCP server 設定檔，不存在就不載入
MCP_RESULT_LIMIT = 6000    # 工具結果最多回傳幾個字給模型，避免網頁全文塞爆 context

YDL_BASE = {
    "format": "bestaudio/best",
    "quiet": True,
    "no_warnings": True,
    "noplaylist": True,
}
if COOKIES and os.path.isfile(COOKIES):
    YDL_BASE["cookiefile"] = COOKIES
    log.info("使用 cookies：%s", COOKIES)

FFMPEG_OPTS = {"options": "-vn"}


@dataclass
class Track:
    title: str
    url: str
    duration: int | None
    requester: str

    def fmt_duration(self) -> str:
        if not self.duration:
            return "?"
        m, s = divmod(int(self.duration), 60)
        h, m = divmod(m, 60)
        return f"{h}:{m:02}:{s:02}" if h else f"{m}:{s:02}"


# ---------- yt-dlp（同步函式，透過 asyncio.to_thread 呼叫） ----------

def search_tracks(query: str, requester: str) -> list[Track]:
    """把網址或關鍵字轉成 Track 清單。只取 metadata，不取串流網址。"""
    is_url = query.startswith(("http://", "https://"))
    opts = dict(YDL_BASE, extract_flat="in_playlist")
    if is_url and "list=" in query:
        opts.update(noplaylist=False, playlistend=MAX_PLAYLIST)
    target = query if is_url else f"ytsearch1:{query}"

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(target, download=False)

    entries = info.get("entries") if "entries" in info else [info]
    tracks = []
    for e in entries or []:
        if not e:
            continue
        url = e.get("webpage_url") or e.get("url") or e.get("id")
        if url and not url.startswith("http"):
            url = f"https://www.youtube.com/watch?v={url}"
        tracks.append(Track(e.get("title") or "未知標題", url, e.get("duration"), requester))
    return tracks


def open_stream(url: str) -> subprocess.Popen:
    """讓 yt-dlp 自己下載音訊並輸出到 stdout，再 pipe 給 FFmpeg。
    yt-dlp 會帶上正確的 headers / cookies / 分段下載，避免 FFmpeg 直連 googlevideo 被 403。"""
    cmd = ["yt-dlp", "-f", "bestaudio/best", "--no-playlist", "--quiet", "--no-warnings", "-o", "-"]
    if "cookiefile" in YDL_BASE:
        cmd += ["--cookies", YDL_BASE["cookiefile"]]
    cmd.append(url)
    # stderr 直接進容器 log，出錯時 docker compose logs 看得到
    return subprocess.Popen(cmd, stdout=subprocess.PIPE)


class BufferedPCMAudio(discord.AudioSource):
    """在背景執行緒預先讀取 PCM，讓 discord.py 每 20ms 取資料時永遠不會卡住。
    discord.py 的播放迴圈若 read() 卡住，事後會不間隔地狂送封包追進度，
    造成開頭被吃掉、聽起來加速；緩衝不足時改回傳靜音，維持穩定節奏。"""

    FRAME_SIZE = discord.opus.Encoder.FRAME_SIZE   # 20ms 的 48kHz 雙聲道 16-bit PCM
    SILENCE = b"\x00" * FRAME_SIZE

    def __init__(self, source: discord.AudioSource, prebuffer: float = 2.0, max_buffer: float = 15.0):
        self.source = source
        self.prebuffer_frames = int(prebuffer * 50)
        self._frames: deque[bytes] = deque()
        self._max_frames = int(max_buffer * 50)
        self._cond = threading.Condition()
        self._eof = False
        self._closed = False
        self._buffering = True
        self._thread = threading.Thread(target=self._fill, daemon=True)
        self._thread.start()

    def _fill(self):
        while True:
            data = self.source.read()
            with self._cond:
                if self._closed:
                    return
                if not data:
                    self._eof = True
                    self._cond.notify_all()
                    return
                self._frames.append(data)
                self._cond.notify_all()
                while len(self._frames) >= self._max_frames and not self._closed:
                    self._cond.wait()

    def wait_ready(self, timeout: float) -> bool:
        """阻塞直到預緩衝完成或串流結束；回傳是否有可播放的資料。"""
        with self._cond:
            self._cond.wait_for(lambda: self._eof or len(self._frames) >= self.prebuffer_frames, timeout)
            return bool(self._frames)

    def read(self) -> bytes:
        with self._cond:
            if self._buffering:
                # 網路卡住導致緩衝見底：先送靜音，攢到約 1 秒再繼續，避免斷斷續續
                if self._eof or len(self._frames) >= self.prebuffer_frames // 2:
                    self._buffering = False
                else:
                    return self.SILENCE
            if self._frames:
                data = self._frames.popleft()
                self._cond.notify_all()
                return data
            if self._eof:
                return b""
            self._buffering = True
            return self.SILENCE

    def cleanup(self):
        with self._cond:
            self._closed = True
            self._cond.notify_all()
        self.source.cleanup()


# ---------- 本地 LLM 聊天（llama.cpp OpenAI 相容 API） ----------

THINK_RE = re.compile(r"<think>.*?</think>", re.DOTALL)
# 模型偶爾會把工具呼叫或思考標籤當成純文字吐出來，不能顯示給使用者
LEAKED_TAGS_RE = re.compile(r"<tool_call>.*?(?:</tool_call>|$)|</?think>", re.DOTALL)


async def build_chat_messages(bot: commands.Bot, message: discord.Message) -> list[dict]:
    """把頻道最近的訊息轉成 OpenAI 格式；機器人自己的訊息是 assistant，其他人是 user。"""
    history = [m async for m in message.channel.history(limit=CHAT_HISTORY, before=message)]
    history.reverse()
    history.append(message)

    me = message.guild.me if message.guild else bot.user
    system = CHAT_SYSTEM_PROMPT.format(name=me.display_name)
    system += f"\n現在時間：{datetime.now().astimezone():%Y-%m-%d %H:%M (%Z)}。"
    if message.guild:
        system += "\n" + CHAT_TOOL_PROMPT
    if bot.mcp.tools:
        system += "\n" + CHAT_MCP_PROMPT
    messages = [{"role": "system", "content": system}]
    for m in history:
        text = m.clean_content.strip()
        if not text:
            continue
        if m.author.id == bot.user.id:
            messages.append({"role": "assistant", "content": text})
        else:
            messages.append({"role": "user", "content": f"{m.author.display_name}：{text}"})
    return messages


async def chat_completion(session: aiohttp.ClientSession, messages: list[dict], tools: list[dict] | None = None) -> dict:
    """回傳 assistant message（可能含 tool_calls）。"""
    headers = {"Authorization": f"Bearer {LLAMA_API_KEY}"} if LLAMA_API_KEY else {}
    payload = {
        "messages": messages,
        "max_tokens": 1024,
        "chat_template_kwargs": {"enable_thinking": False},  # Qwen3 關掉思考模式，回得比較快
    }
    if tools:
        payload["tools"] = tools
    async with session.post(f"{LLAMA_API_URL}/chat/completions", json=payload, headers=headers) as resp:
        resp.raise_for_status()
        data = await resp.json()
    return data["choices"][0]["message"]


def clean_reply(text: str | None) -> str:
    return LEAKED_TAGS_RE.sub("", THINK_RE.sub("", text or "")).strip()


def split_message(text: str, limit: int = 2000) -> list[str]:
    """Discord 單則訊息上限 2000 字，盡量在換行處切開。"""
    chunks = []
    while len(text) > limit:
        cut = text.rfind("\n", 0, limit)
        if cut <= 0:
            cut = limit
        chunks.append(text[:cut])
        text = text[cut:].lstrip("\n")
    if text:
        chunks.append(text)
    return chunks


# ---------- MCP 工具（網路搜尋、計算機…） ----------

class MCPManager:
    """連接 mcp_servers.json 裡的 MCP server，把它們的工具轉成 OpenAI 格式給聊天用。
    MCP SDK 的連線必須在同一個 task 裡開啟和關閉，所以由一個常駐 task 持有所有連線。"""

    def __init__(self, config_path: str):
        self.config_path = config_path
        self.tools: list[dict] = []
        self._sessions: dict[str, ClientSession] = {}   # 工具名稱 → 提供它的 server
        self._ready = asyncio.Event()
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None

    async def start(self):
        if not self.config_path or not os.path.isfile(self.config_path):
            log.info("找不到 MCP 設定檔 %s，不載入 MCP 工具", self.config_path)
            return
        self._task = asyncio.create_task(self._run())
        await self._ready.wait()

    async def close(self):
        if self._task:
            self._stop.set()
            try:
                await asyncio.wait_for(self._task, 10)
            except (asyncio.TimeoutError, Exception):  # noqa: BLE001
                pass

    def has(self, name: str) -> bool:
        return name in self._sessions

    async def _run(self):
        try:
            with open(self.config_path, encoding="utf-8") as f:
                servers = json.load(f).get("mcpServers", {})
            async with AsyncExitStack() as stack:
                for name, cfg in servers.items():
                    try:
                        async with asyncio.timeout(180):   # 第一次啟動 npm 類 server 要下載套件
                            await self._connect(stack, name, cfg)
                    except Exception as e:  # noqa: BLE001
                        log.warning("MCP server %s 啟動失敗：%r", name, e)
                self._ready.set()
                await self._stop.wait()
        except Exception:  # noqa: BLE001
            log.exception("MCP 管理器錯誤")
        finally:
            self._ready.set()

    async def _connect(self, stack: AsyncExitStack, name: str, cfg: dict):
        # 只給預設的安全環境變數，不把 Discord token 等機密傳給第三方 server
        env = {**get_default_environment(), **cfg.get("env", {})}
        params = StdioServerParameters(command=cfg["command"], args=cfg.get("args", []), env=env)
        read, write = await stack.enter_async_context(stdio_client(params))
        session = await stack.enter_async_context(ClientSession(read, write))
        await session.initialize()

        allowed = cfg.get("tools")   # 可選：只開放部分工具
        loaded = []
        for tool in (await session.list_tools()).tools:
            if allowed and tool.name not in allowed:
                continue
            if tool.name in self._sessions:
                log.warning("MCP 工具名稱重複，略過 %s/%s", name, tool.name)
                continue
            self._sessions[tool.name] = session
            self.tools.append({"type": "function", "function": {
                "name": tool.name,
                "description": tool.description or "",
                "parameters": tool.inputSchema or {"type": "object", "properties": {}},
            }})
            loaded.append(tool.name)
        log.info("MCP server %s 已連線，工具：%s", name, ", ".join(loaded))

    async def call(self, name: str, args: dict) -> str:
        try:
            result = await asyncio.wait_for(self._sessions[name].call_tool(name, args), 60)
        except Exception as e:  # noqa: BLE001
            log.warning("MCP 工具 %s 失敗：%r", name, e)
            return f"工具執行失敗：{e!r}"
        text = "\n".join(c.text for c in result.content if getattr(c, "type", "") == "text")
        if result.isError:
            text = f"工具執行失敗：{text}"
        if len(text) > MCP_RESULT_LIMIT:
            text = text[:MCP_RESULT_LIMIT] + "\n…（內容過長已截斷）"
        return text or "（沒有結果）"


# ---------- 每個伺服器一個播放器 ----------

class GuildPlayer:
    def __init__(self, bot: commands.Bot, guild: discord.Guild):
        self.bot = bot
        self.guild = guild
        self.queue: deque[Track] = deque()
        self.current: Track | None = None
        self.volume = 0.5
        self.text_channel: discord.abc.Messageable | None = None
        self._added = asyncio.Event()
        self._finished = asyncio.Event()
        self._task: asyncio.Task | None = None

    def add(self, tracks: list[Track]):
        self.queue.extend(tracks)
        self._added.set()
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._loop())

    async def notify(self, msg: str):
        if self.text_channel:
            try:
                await self.text_channel.send(msg)
            except discord.HTTPException:
                pass

    def _after(self, err: Exception | None):
        if err:
            log.error("播放錯誤：%s", err)
        self.bot.loop.call_soon_threadsafe(self._finished.set)

    async def _loop(self):
        while True:
            # 等待佇列有東西，閒置太久就離開
            while not self.queue:
                self._added.clear()
                try:
                    await asyncio.wait_for(self._added.wait(), timeout=IDLE_TIMEOUT)
                except asyncio.TimeoutError:
                    await self.disconnect()
                    await self.notify("💤 閒置太久，先離開語音頻道囉")
                    return

            vc = self.guild.voice_client
            if vc is None or not vc.is_connected():
                self.queue.clear()
                return

            track = self.queue.popleft()
            try:
                proc = open_stream(track.url)
            except OSError as e:
                log.error("無法啟動 yt-dlp：%s", e)
                await self.notify(f"⚠️ 無法播放 **{track.title}**，跳過")
                continue

            buffered = BufferedPCMAudio(discord.FFmpegPCMAudio(proc.stdout, pipe=True, **FFMPEG_OPTS))
            if not await asyncio.to_thread(buffered.wait_ready, 30):
                buffered.cleanup()
                if proc.poll() is None:
                    proc.kill()
                await asyncio.to_thread(proc.wait)
                await self.notify(f"⚠️ 無法播放 **{track.title}**，跳過")
                continue
            vc = self.guild.voice_client
            if vc is None or not vc.is_connected():
                buffered.cleanup()
                proc.kill()
                await asyncio.to_thread(proc.wait)
                self.queue.clear()
                return

            source = discord.PCMVolumeTransformer(buffered, volume=self.volume)
            self._finished.clear()
            self.current = track
            vc.play(source, after=self._after)
            await self.notify(f"🎵 正在播放：**{track.title}** `[{track.fmt_duration()}]`（{track.requester} 點播）")
            await self._finished.wait()
            self.current = None
            # 跳過或播完後確保 yt-dlp 也結束
            if proc.poll() is None:
                proc.kill()
            await asyncio.to_thread(proc.wait)

    async def disconnect(self):
        self.queue.clear()
        self.current = None
        vc = self.guild.voice_client
        if vc:
            await vc.disconnect(force=False)


# ---------- Bot 本體 ----------

class MusicBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = LLAMA_API_URL != ""  # 聊天要讀訊息內容，需在開發者後台開啟 Message Content Intent
        super().__init__(command_prefix=commands.when_mentioned, intents=intents)
        self.players: dict[int, GuildPlayer] = {}
        self.http_session: aiohttp.ClientSession | None = None
        self.mcp = MCPManager(MCP_CONFIG)

    def player(self, guild: discord.Guild) -> GuildPlayer:
        if guild.id not in self.players:
            self.players[guild.id] = GuildPlayer(self, guild)
        return self.players[guild.id]

    async def setup_hook(self):
        if LLAMA_API_URL:
            self.http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=180))
            await self.mcp.start()
            log.info("聊天功能已啟用：%s", LLAMA_API_URL)
        if GUILD_ID:
            guild = discord.Object(id=int(GUILD_ID))
            self.tree.copy_global_to(guild=guild)
            # 清空全域指令（移除舊程式留下的殘留指令），只保留伺服器指令
            self.tree.clear_commands(guild=None)
            await self.tree.sync()
            await self.tree.sync(guild=guild)
            log.info("已清空全域指令，Slash 指令已同步到伺服器 %s", GUILD_ID)
        else:
            await self.tree.sync()
            log.info("Slash 指令已全域同步（可能要等一陣子才出現）")

    async def close(self):
        await self.mcp.close()
        if self.http_session:
            await self.http_session.close()
        await super().close()

    async def on_ready(self):
        log.info("已登入：%s (%s)", self.user, self.user.id)

    async def on_message(self, message: discord.Message):
        # 不呼叫 process_commands：這個機器人只用 slash 指令
        if message.author.bot or not self.http_session:
            return
        replied_to_me = (
            message.reference is not None
            and isinstance(message.reference.resolved, discord.Message)
            and message.reference.resolved.author.id == self.user.id
        )
        if not (self.user in message.mentions or replied_to_me):
            return

        async with message.channel.typing():
            try:
                reply = await chat_reply(self, message)
            except (aiohttp.ClientError, asyncio.TimeoutError, KeyError) as e:
                log.warning("聊天失敗：%r", e)
                return await message.reply("⚠️ AI 暫時連不上，等等再試試", mention_author=False)

        chunks = split_message(reply) or ["……（AI 沒有回應）"]
        await message.reply(chunks[0], mention_author=False)
        for chunk in chunks[1:]:
            await message.channel.send(chunk)

    async def on_voice_state_update(self, member, before, after):
        # 語音頻道只剩機器人時自動離開
        vc = member.guild.voice_client
        if vc and vc.channel and not any(not m.bot for m in vc.channel.members):
            await self.player(member.guild).disconnect()


bot = MusicBot()


def _vc(interaction: discord.Interaction) -> discord.VoiceClient | None:
    return interaction.guild.voice_client if interaction.guild else None


class PlayError(Exception):
    """可以直接顯示給使用者的點歌錯誤"""


async def enqueue(member: discord.Member, text_channel: discord.abc.Messageable, query: str) -> list[Track]:
    """進入點歌者所在的語音頻道並把搜尋結果加入佇列；slash 指令和聊天點歌共用。"""
    voice = getattr(member, "voice", None)
    if not voice or not voice.channel:
        raise PlayError("你要先進語音頻道 🎧")

    vc = member.guild.voice_client
    if vc is None:
        await voice.channel.connect(self_deaf=True)
    elif vc.channel != voice.channel:
        await vc.move_to(voice.channel)

    try:
        tracks = await asyncio.to_thread(search_tracks, query, member.display_name)
    except Exception as e:  # noqa: BLE001
        log.warning("搜尋失敗 %s：%s", query, e)
        raise PlayError("⚠️ 解析失敗，可能是 yt-dlp 需要更新或影片無法播放") from e

    if not tracks:
        raise PlayError("找不到結果 🤔")

    player = bot.player(member.guild)
    player.text_channel = text_channel
    player.add(tracks)
    return tracks


def format_queue(player: GuildPlayer) -> str:
    lines = []
    if player.current:
        lines.append(f"🎵 **正在播放：** {player.current.title} `[{player.current.fmt_duration()}]`")
    for i, t in enumerate(list(player.queue)[:10], 1):
        lines.append(f"`{i}.` {t.title} `[{t.fmt_duration()}]`")
    if len(player.queue) > 10:
        lines.append(f"…還有 {len(player.queue) - 10} 首")
    return "\n".join(lines) or "佇列是空的"


# ---------- 聊天中的工具呼叫 ----------

CHAT_TOOLS = [
    {"type": "function", "function": {
        "name": "play_music",
        "description": "在語音頻道點一首歌（加入播放佇列）",
        "parameters": {"type": "object", "properties": {
            "query": {"type": "string", "description": "YouTube 搜尋關鍵字（歌手 + 歌名）或 YouTube 網址"},
        }, "required": ["query"]},
    }},
    {"type": "function", "function": {
        "name": "skip_song",
        "description": "跳過目前正在播放的歌",
        "parameters": {"type": "object", "properties": {}},
    }},
    {"type": "function", "function": {
        "name": "get_queue",
        "description": "查看目前正在播放的歌和待播佇列",
        "parameters": {"type": "object", "properties": {}},
    }},
]


async def run_tool(name: str, args: dict, message: discord.Message) -> str:
    """執行模型要求的工具，回傳給模型看的結果文字。"""
    log.info("聊天工具 %s %s（%s）", name, args, message.author)
    guild = message.guild
    if name == "play_music":
        voice = getattr(message.author, "voice", None)
        if not voice or not voice.channel:
            return f"點歌失敗：{message.author.display_name} 不在任何語音頻道，請他先加入語音頻道再點歌"
        try:
            tracks = await enqueue(message.author, message.channel, str(args.get("query", "")))
        except PlayError as e:
            return f"點歌失敗：{e}"
        if len(tracks) == 1:
            return f"已加入佇列：{tracks[0].title}"
        return f"已加入 {len(tracks)} 首歌"
    if name == "skip_song":
        vc = guild.voice_client
        if not vc or not (vc.is_playing() or vc.is_paused()):
            return "現在沒有在播放"
        vc.stop()
        return "已跳過"
    if name == "get_queue":
        return format_queue(bot.player(guild))
    if bot.mcp.has(name):
        return await bot.mcp.call(name, args)
    return f"未知的工具：{name}"


async def chat_reply(bot: MusicBot, message: discord.Message) -> str:
    """呼叫 LLM；如果它要求工具就執行並把結果餵回去，直到它給出文字回覆。"""
    messages = await build_chat_messages(bot, message)
    for round_ in range(CHAT_MAX_TOOL_ROUNDS + 1):
        if round_ < CHAT_MAX_TOOL_ROUNDS:
            # 音樂工具只在伺服器裡有用；MCP 工具私訊也能用
            tools = (CHAT_TOOLS if message.guild else []) + bot.mcp.tools or None
        else:
            # 最後一輪不給工具，並明確要它收尾，否則它會把工具呼叫當文字吐出來
            tools = None
            messages.append({"role": "user", "content": "（系統提示：工具使用次數已達上限，請根據目前取得的資訊直接回覆。）"})
        reply = await chat_completion(bot.http_session, messages, tools)
        calls = reply.get("tool_calls")
        if not calls:
            return clean_reply(reply.get("content"))
        messages.append({"role": "assistant", "content": reply.get("content") or "", "tool_calls": calls})
        for call in calls:
            try:
                args = json.loads(call["function"].get("arguments") or "{}")
            except json.JSONDecodeError:
                args = {}
            result = await run_tool(call["function"]["name"], args, message)
            messages.append({"role": "tool", "tool_call_id": call.get("id", ""), "content": result})
    return ""


@bot.tree.command(name="play", description="播放 YouTube 網址、播放清單或搜尋關鍵字")
@app_commands.describe(query="YouTube 網址或關鍵字")
async def play(interaction: discord.Interaction, query: str):
    voice = getattr(interaction.user, "voice", None)
    if not voice or not voice.channel:
        return await interaction.response.send_message("你要先進語音頻道 🎧", ephemeral=True)

    await interaction.response.defer(thinking=True)
    try:
        tracks = await enqueue(interaction.user, interaction.channel, query)
    except PlayError as e:
        return await interaction.followup.send(str(e))

    if len(tracks) == 1:
        await interaction.followup.send(f"➕ 已加入佇列：**{tracks[0].title}**")
    else:
        await interaction.followup.send(f"➕ 已加入 {len(tracks)} 首歌")


@bot.tree.command(name="skip", description="跳過目前歌曲")
async def skip(interaction: discord.Interaction):
    vc = _vc(interaction)
    if not vc or not (vc.is_playing() or vc.is_paused()):
        return await interaction.response.send_message("現在沒有在播放", ephemeral=True)
    vc.stop()
    await interaction.response.send_message("⏭️ 已跳過")


@bot.tree.command(name="pause", description="暫停")
async def pause(interaction: discord.Interaction):
    vc = _vc(interaction)
    if vc and vc.is_playing():
        vc.pause()
        return await interaction.response.send_message("⏸️ 已暫停")
    await interaction.response.send_message("現在沒有在播放", ephemeral=True)


@bot.tree.command(name="resume", description="繼續播放")
async def resume(interaction: discord.Interaction):
    vc = _vc(interaction)
    if vc and vc.is_paused():
        vc.resume()
        return await interaction.response.send_message("▶️ 繼續播放")
    await interaction.response.send_message("目前沒有暫停", ephemeral=True)


@bot.tree.command(name="stop", description="停止播放並清空佇列")
async def stop(interaction: discord.Interaction):
    player = bot.player(interaction.guild)
    player.queue.clear()
    vc = _vc(interaction)
    if vc:
        vc.stop()
    await interaction.response.send_message("⏹️ 已停止並清空佇列")


@bot.tree.command(name="leave", description="離開語音頻道")
async def leave(interaction: discord.Interaction):
    if not _vc(interaction):
        return await interaction.response.send_message("我不在語音頻道裡", ephemeral=True)
    await bot.player(interaction.guild).disconnect()
    await interaction.response.send_message("👋 掰掰")


@bot.tree.command(name="queue", description="查看播放佇列")
async def queue(interaction: discord.Interaction):
    await interaction.response.send_message(format_queue(bot.player(interaction.guild)))


@bot.tree.command(name="volume", description="調整音量（0–100）")
@app_commands.describe(level="音量 0–100")
async def volume(interaction: discord.Interaction, level: app_commands.Range[int, 0, 100]):
    player = bot.player(interaction.guild)
    player.volume = level / 100
    vc = _vc(interaction)
    if vc and isinstance(vc.source, discord.PCMVolumeTransformer):
        vc.source.volume = player.volume
    await interaction.response.send_message(f"🔊 音量：{level}%")


if __name__ == "__main__":
    bot.run(TOKEN, log_handler=None)
