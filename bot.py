"""私人 Discord YouTube 音樂機器人（discord.py + yt-dlp + FFmpeg）"""

import asyncio
import json
import logging
import os
import random
import re
import subprocess
import threading
import time
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

from memory import MemoryStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("musicbot")

TOKEN = os.environ["DISCORD_TOKEN"]
GUILD_ID = os.environ.get("GUILD_ID")            # 填了的話 slash 指令會立即同步到該伺服器
COOKIES = os.environ.get("YTDLP_COOKIES", "/app/cookies.txt")
IDLE_TIMEOUT = int(os.environ.get("IDLE_TIMEOUT", "300"))   # 佇列空了多久後自動離開（秒）
MAX_PLAYLIST = int(os.environ.get("MAX_PLAYLIST", "50"))    # 一次最多加入幾首播放清單歌曲

LLAMA_API_URL = os.environ.get("LLAMA_API_URL", "").rstrip("/")  # 例如 http://192.168.0.66:8000/v1，留空則不啟用聊天
LLAMA_API_KEY = os.environ.get("LLAMA_API_KEY", "")
CHAT_HISTORY = int(os.environ.get("CHAT_HISTORY", "20"))    # 聊天時讀取頻道最近幾則原始訊息，更早的靠頻道摘要
# 訊息裡出現這些名字也會回應（逗號分隔；機器人自己的名稱一律算）
CHAT_NAMES = [n.strip() for n in os.environ.get("CHAT_NAMES", "").split(",") if n.strip()]
CHAT_CHIME_IN = float(os.environ.get("CHAT_CHIME_IN", "0.03"))                 # 沒被叫到時主動插話的機率（0 關閉）
CHAT_CHIME_IN_COOLDOWN = int(os.environ.get("CHAT_CHIME_IN_COOLDOWN", "600"))  # 同頻道主動插話的最短間隔（秒）
CHAT_THINKING = os.environ.get("CHAT_THINKING", "true").lower() in ("1", "true", "yes", "on")  # 回覆前先思考（較慢但較聰明）
# 思考力度：xhigh / medium / low，由模型的 chat template 轉成 system 指示（軟性引導）
CHAT_REASONING_EFFORT = os.environ.get("CHAT_REASONING_EFFORT", "xhigh")
CHAT_THINKING_BUDGET = int(os.environ.get("CHAT_THINKING_BUDGET", "1024"))  # 思考最多幾個 token，到了就強制開始回答
CHAT_ANSWER_TOKENS = 2048  # 思考之外留給回覆本身的 token
CHAT_SYSTEM_PROMPT = os.environ.get("CHAT_SYSTEM_PROMPT") or (
    "你是 Discord 伺服器裡的聊天夥伴兼音樂機器人，名字叫 {name}。"
    "請用繁體中文、口語、簡短地回覆，像朋友聊天一樣，必要時才分段。"
    "對話紀錄中每則使用者訊息的開頭是「發言者名稱：」，回覆時不要加這個前綴。"
)
# 不論是否自訂 CHAT_SYSTEM_PROMPT 都會加上，避免它順口評論上下文裡無關的訊息
CHAT_FOCUS_PROMPT = (
    "只回應最後一則訊息；前面的對話紀錄只是背景參考，用來理解話題，"
    "除非最後一則訊息有提到，否則不要主動評論或回應前面其他人的訊息。"
    "使用者訊息開頭的 [月/日 時:分] 是發送時間，可以用來判斷事情發生多久了；你的回覆不要加時間或名稱前綴。"
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
CHAT_MEMORY_PROMPT = (
    "你有長期記憶。聽到值得長期記住的事就呼叫 remember："
    "scope=user 記在發言者身上（喜好、暱稱、生日、近況），scope=server 記成全伺服器共享（內梗、規則、大事）。"
    "不要記瑣碎或一次性的內容，也不要重複記已經記得的事。"
    "想不起某人或某事時用 recall 搜尋；有人要你忘記某件事時用 forget。"
)
CHAT_REACTION_PROMPT = "你可以用 add_reaction 幫最後一則訊息按表情符號，適合簡單附和或開玩笑時使用。"
CHAT_CHIME_IN_PROMPT = (
    "（系統提示：這次沒有人叫你，你只是在旁邊看到大家聊天。"
    "只有能自然地補充有趣或有幫助的話時才開口，而且要很短；也可以只用 add_reaction 按個表情。"
    "如果沒什麼好說的，只回覆 [PASS]。）"
)
CHAT_MAX_TOOL_ROUNDS = 5   # 一則訊息最多連續呼叫幾輪工具（模型一次只會點一首）

MEMORY_DB = os.environ.get("MEMORY_DB", "/app/data/memory.db")
MEMORY_MAX_PER_SCOPE = 50  # 每個人／伺服器最多記幾條
MEMORY_IN_PROMPT = 20      # 每次聊天自動帶入幾條發言者記憶與伺服器記憶
SUMMARY_TRIGGER = int(os.environ.get("SUMMARY_TRIGGER", "30"))  # 上下文窗口外累積幾則未摘要訊息就壓縮進頻道摘要
SUMMARY_MAX_FETCH = 200    # 一次最多壓縮幾則
SUMMARY_PROMPT = (
    "你負責整理 Discord 頻道的對話摘要。把「先前摘要」和「新的對話」合併成一份新的摘要："
    "用繁體中文條列重點（誰聊了什麼話題、做了什麼決定、有什麼還沒完的事、重要的梗），保留人名，"
    "提到約定或事件時寫上日期（對話每行開頭的 [月/日 時:分] 是發送時間），"
    "去掉寒暄和沒有意義的內容，舊的瑣事可以刪減，總長不超過 400 字。只輸出摘要本身。"
)

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
# 模型模仿對話紀錄格式時，回覆開頭可能多出「[10/26 14:03] 名字：」
LEAKED_PREFIX_RE = re.compile(r"^\[\d{1,2}/\d{1,2} \d{1,2}:\d{2}\]\s*(?:[^：\n]{1,32}：)?")


# 主動插話時模型表示「不說話」的標記；它不一定照規定寫 [PASS]，也可能自己編 [HOLD] 之類的
SILENCE_MARKER_RE = re.compile(r"\[[A-Z_]{3,}\]")


# 舊版串流回覆留在頻道裡的「思考中」訊息與「-# 💭 N 秒」頁尾，讀歷史時要濾掉
THINKING_PLACEHOLDER = "💭 思考中"


def stamp(m: discord.Message) -> str:
    """訊息的發送時間（本地時區），例如 [10/26 14:03]"""
    return f"[{m.created_at.astimezone():%m/%d %H:%M}]"


def history_text(bot: commands.Bot, m: discord.Message) -> str | None:
    """把一則頻道訊息轉成給模型看的文字；機器人自己的訊息去掉舊版的思考中提示與頁尾小字。"""
    text = m.clean_content.strip()
    if m.author.id == bot.user.id:
        if text.startswith(THINKING_PLACEHOLDER) or SILENCE_MARKER_RE.fullmatch(text):
            return None
        text = "\n".join(line for line in text.splitlines() if not line.startswith("-# ")).strip()
    return text or None


def memory_guild_id(message: discord.Message) -> int:
    return message.guild.id if message.guild else 0   # 私訊的記憶存在 guild 0


async def build_chat_messages(bot: commands.Bot, message: discord.Message, chime_in: bool = False) -> list[dict]:
    """組 prompt：人設與規則 + 長期記憶 + 頻道摘要 + 最近的原始訊息。
    機器人自己的訊息是 assistant，其他人是 user。"""
    history = [m async for m in message.channel.history(limit=CHAT_HISTORY, before=message)]
    history.reverse()
    history.append(message)

    me = message.guild.me if message.guild else bot.user
    system = CHAT_SYSTEM_PROMPT.format(name=me.display_name)
    system += "\n" + CHAT_FOCUS_PROMPT
    system += f"\n現在時間：{datetime.now().astimezone():%Y-%m-%d %H:%M (%Z)}。"
    if message.guild and not chime_in:
        system += "\n" + CHAT_TOOL_PROMPT
    if bot.mcp.tools and not chime_in:
        system += "\n" + CHAT_MCP_PROMPT
    if not chime_in:
        system += "\n" + CHAT_MEMORY_PROMPT
    system += "\n" + CHAT_REACTION_PROMPT

    gid = memory_guild_id(message)
    if mine := bot.memory.list_for(gid, message.author.id, MEMORY_IN_PROMPT):
        system += f"\n\n關於 {message.author.display_name} 你記得：\n" + "\n".join(f"- {m.fmt()}" for m in mine)
    if message.guild and (shared := bot.memory.list_for(gid, None, MEMORY_IN_PROMPT)):
        system += "\n\n伺服器共同記憶：\n" + "\n".join(f"- {m.fmt()}" for m in shared)
    if summary := bot.memory.get_summary(message.channel.id):
        system += f"\n\n這個頻道稍早的對話摘要（更早之前的內容）：\n{summary[0]}"

    messages = [{"role": "system", "content": system}]
    for m in history:
        text = history_text(bot, m)
        if not text:
            continue
        if m.author.id == bot.user.id:
            messages.append({"role": "assistant", "content": text})
        else:
            messages.append({"role": "user", "content": f"{stamp(m)} {m.author.display_name}：{text}"})
    if chime_in:
        messages.append({"role": "user", "content": CHAT_CHIME_IN_PROMPT})
    return messages


async def chat_completion(
    session: aiohttp.ClientSession,
    messages: list[dict],
    tools: list[dict] | None = None,
    thinking: bool = CHAT_THINKING,
    answer_tokens: int = CHAT_ANSWER_TOKENS,
) -> dict:
    """回傳 assistant message（可能含 tool_calls）。"""
    headers = {"Authorization": f"Bearer {LLAMA_API_KEY}"} if LLAMA_API_KEY else {}
    payload = {
        "messages": messages,
        "max_tokens": answer_tokens + (CHAT_THINKING_BUDGET if thinking else 0),
        "chat_template_kwargs": {"enable_thinking": thinking, "reasoning_effort": CHAT_REASONING_EFFORT},
        "reasoning_format": "deepseek",   # 思考內容放到 reasoning_content，不混進回覆
    }
    if thinking:
        payload["thinking_budget_tokens"] = CHAT_THINKING_BUDGET   # llama.cpp 會在預算用完時插入 </think> 強制作答
    if tools:
        payload["tools"] = tools
    async with session.post(f"{LLAMA_API_URL}/chat/completions", json=payload, headers=headers) as resp:
        resp.raise_for_status()
        data = await resp.json()
    return data["choices"][0]["message"]


async def summarize_channel(bot: commands.Bot, channel: discord.abc.Messageable):
    """把超出上下文窗口、還沒摘要過的舊訊息併進頻道摘要。"""
    recent = [m async for m in channel.history(limit=CHAT_HISTORY)]
    if len(recent) < CHAT_HISTORY:
        return   # 訊息還不多，窗口就裝得下
    prev = bot.memory.get_summary(channel.id)
    summary, last_id = prev if prev else ("", 0)
    after = discord.Object(id=last_id) if last_id else None
    pending = [m async for m in channel.history(limit=SUMMARY_MAX_FETCH, before=recent[-1], after=after)]
    pending.sort(key=lambda m: m.id)
    lines = [f"{stamp(m)} {m.author.display_name}：{t}" for m in pending if (t := history_text(bot, m))]
    if len(lines) < SUMMARY_TRIGGER:
        return

    messages = [
        {"role": "system", "content": SUMMARY_PROMPT},
        {"role": "user", "content": f"先前摘要：\n{summary or '（無）'}\n\n新的對話：\n" + "\n".join(lines)},
    ]
    reply = await chat_completion(bot.http_session, messages, thinking=False, answer_tokens=1024)
    if new := clean_reply(reply.get("content")):
        bot.memory.set_summary(channel.id, new, pending[-1].id)
        log.info("頻道 %s 摘要已更新（併入 %d 則訊息）", channel, len(lines))


def clean_reply(text: str | None) -> str:
    text = LEAKED_TAGS_RE.sub("", THINK_RE.sub("", text or "")).strip()
    return LEAKED_PREFIX_RE.sub("", text).strip()


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
        self._voice_locks: dict[int, asyncio.Lock] = {}
        self.http_session: aiohttp.ClientSession | None = None
        self.mcp = MCPManager(MCP_CONFIG)
        os.makedirs(os.path.dirname(MEMORY_DB), exist_ok=True)
        self.memory = MemoryStore(MEMORY_DB)
        self.active_chats = 0                     # 正在等 LLM 回覆的對話數，背景摘要會讓位
        self._summarizing: set[int] = set()
        self._last_chime_in: dict[int, float] = {}

    def voice_lock(self, guild: discord.Guild) -> asyncio.Lock:
        return self._voice_locks.setdefault(guild.id, asyncio.Lock())

    def player(self, guild: discord.Guild) -> GuildPlayer:
        if guild.id not in self.players:
            self.players[guild.id] = GuildPlayer(self, guild)
        return self.players[guild.id]

    async def setup_hook(self):
        if LLAMA_API_URL:
            self.http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=600))
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
        if self._is_addressed(message):
            await self._chat(message)
        elif self._should_chime_in(message):
            await self._chime_in(message)

    def _is_addressed(self, message: discord.Message) -> bool:
        """私訊、被 @、被回覆，或訊息裡叫了機器人的名字。"""
        if message.guild is None or self.user in message.mentions:
            return True
        ref = message.reference
        if ref is not None and isinstance(ref.resolved, discord.Message) and ref.resolved.author.id == self.user.id:
            return True
        names = {message.guild.me.display_name, self.user.name, *CHAT_NAMES}
        content = message.content.lower()
        return any(n.lower() in content for n in names)

    def _should_chime_in(self, message: discord.Message) -> bool:
        if CHAT_CHIME_IN <= 0 or self.active_chats or len(message.content) < 4:
            return False
        if time.monotonic() - self._last_chime_in.get(message.channel.id, -1e9) < CHAT_CHIME_IN_COOLDOWN:
            return False
        return random.random() < CHAT_CHIME_IN

    async def _chat(self, message: discord.Message):
        turn = ChatTurn(message)
        self.active_chats += 1
        try:
            async with message.channel.typing():
                reply = await chat_reply(self, turn)
        except (aiohttp.ClientError, asyncio.TimeoutError, KeyError, json.JSONDecodeError) as e:
            log.warning("聊天失敗：%r", e)
            await message.reply("⚠️ AI 暫時連不上，等等再試試", mention_author=False)
            return
        except Exception:  # noqa: BLE001
            log.exception("聊天發生非預期錯誤")
            await message.reply("⚠️ 出了點問題，等等再試試", mention_author=False)
            return
        finally:
            self.active_chats -= 1

        if reply or not turn.reacted:   # 只按了表情就不用再多一則訊息
            chunks = split_message(reply) or ["……（AI 沒有回應）"]
            await message.reply(chunks[0], mention_author=False)
            for chunk in chunks[1:]:
                await message.channel.send(chunk)
        self.schedule_summary(message.channel)

    async def _chime_in(self, message: discord.Message):
        self._last_chime_in[message.channel.id] = time.monotonic()
        turn = ChatTurn(message, chime_in=True)
        self.active_chats += 1
        try:
            reply = await chat_reply(self, turn)
        except (aiohttp.ClientError, asyncio.TimeoutError, KeyError, json.JSONDecodeError) as e:
            log.warning("主動插話失敗：%r", e)
            return
        finally:
            self.active_chats -= 1
        reply = SILENCE_MARKER_RE.sub("", reply).strip()
        log.info("主動插話（%s）：%s", message.channel, reply or ("按了表情" if turn.reacted else "略過"))
        if reply:
            for chunk in split_message(reply):
                await message.channel.send(chunk)

    def schedule_summary(self, channel: discord.abc.Messageable):
        if channel.id in self._summarizing:
            return
        self._summarizing.add(channel.id)
        asyncio.create_task(self._summarize(channel))

    async def _summarize(self, channel: discord.abc.Messageable):
        try:
            while self.active_chats:   # 只有一個 slot，有人在等回覆時先讓位
                await asyncio.sleep(5)
            await summarize_channel(self, channel)
        except Exception:  # noqa: BLE001
            log.exception("頻道摘要失敗")
        finally:
            self._summarizing.discard(channel.id)

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

    # 同時有兩個人點歌時，避免兩邊都去連線而撞上「Already connected」
    async with bot.voice_lock(member.guild):
        vc = member.guild.voice_client
        if vc is not None and not vc.is_connected():
            await vc.disconnect(force=True)   # 斷線後殘留的連線物件，清掉重連
            vc = None
        try:
            if vc is None:
                await voice.channel.connect(self_deaf=True, timeout=20)
            elif vc.channel != voice.channel:
                await vc.move_to(voice.channel)
        except (asyncio.TimeoutError, discord.ClientException) as e:
            log.warning("連接語音頻道失敗：%r", e)
            raise PlayError("⚠️ 連不上語音頻道，等一下再試試") from e

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

@dataclass
class ChatTurn:
    """一次聊天回覆的狀態"""
    message: discord.Message           # 觸發回覆的訊息
    chime_in: bool = False
    reacted: bool = False


def _tool(name: str, description: str, properties: dict | None = None, required: list[str] | None = None) -> dict:
    return {"type": "function", "function": {
        "name": name,
        "description": description,
        "parameters": {"type": "object", "properties": properties or {}, "required": required or []},
    }}


MUSIC_TOOLS = [
    _tool("play_music", "在語音頻道點一首歌（加入播放佇列）",
          {"query": {"type": "string", "description": "YouTube 搜尋關鍵字（歌手 + 歌名）或 YouTube 網址"}}, ["query"]),
    _tool("skip_song", "跳過目前正在播放的歌"),
    _tool("get_queue", "查看目前正在播放的歌和待播佇列"),
]
MEMORY_TOOLS = [
    _tool("remember", "把值得長期記住的事存進記憶",
          {"content": {"type": "string", "description": "要記住的內容，寫成一句完整的話，例如「喜歡五月天」"},
           "scope": {"type": "string", "enum": ["user", "server"],
                     "description": "user：記在發言者身上；server：全伺服器共享"}},
          ["content", "scope"]),
    _tool("recall", "搜尋記憶（包含伺服器裡其他人的記憶）",
          {"query": {"type": "string", "description": "關鍵字或人名，多個用空白分隔"}}, ["query"]),
    _tool("forget", "刪除一條記憶",
          {"memory_id": {"type": "integer", "description": "記憶編號（#後面的數字）"}}, ["memory_id"]),
]
REACTION_TOOL = _tool("add_reaction", "幫最後一則訊息按一個表情符號",
                      {"emoji": {"type": "string", "description": "一個 Unicode emoji，例如 😂"}}, ["emoji"])


def tools_for(turn: ChatTurn) -> list[dict]:
    if turn.chime_in:
        return [REACTION_TOOL]   # 主動插話時不准點歌、記東西或上網
    tools = (MUSIC_TOOLS if turn.message.guild else []) + MEMORY_TOOLS + [REACTION_TOOL]
    return tools + bot.mcp.tools


async def run_tool(name: str, args: dict, turn: ChatTurn) -> str:
    """執行模型要求的工具，回傳給模型看的結果文字。出錯時也回傳文字，讓模型照實告訴使用者。"""
    log.info("聊天工具 %s %s（%s）", name, args, turn.message.author)
    try:
        result = await _run_tool(name, args, turn)
    except Exception as e:  # noqa: BLE001
        log.exception("聊天工具 %s 執行失敗", name)
        return f"工具執行失敗：{e!r}"
    log.info("聊天工具 %s 結果：%s", name, result[:200].replace("\n", " "))
    return result


async def _run_tool(name: str, args: dict, turn: ChatTurn) -> str:
    message = turn.message
    guild = message.guild
    gid = memory_guild_id(message)
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
    if name == "remember":
        content = str(args.get("content", "")).strip()[:200]
        if not content:
            return "記憶內容是空的"
        shared = args.get("scope") == "server" and guild is not None
        user_id = None if shared else message.author.id
        if bot.memory.count(gid, user_id) >= MEMORY_MAX_PER_SCOPE:
            return "記憶已滿，請先用 forget 刪掉不重要的再記"
        mid = bot.memory.add(gid, user_id, None if shared else message.author.display_name, content, message.author.id)
        return f"已記住 #{mid}（{'伺服器共同記憶' if shared else message.author.display_name}）：{content}"
    if name == "recall":
        found = bot.memory.search(gid, str(args.get("query", "")).split())
        if not found:
            return "沒有找到相關記憶"
        return "\n".join(f"#{m.id} [{m.user_name or '伺服器'}] {m.content}" for m in found)
    if name == "forget":
        try:
            mem = bot.memory.get(gid, int(args.get("memory_id")))
        except (TypeError, ValueError):
            mem = None
        if mem is None:
            return "找不到這條記憶"
        if mem.user_id not in (None, message.author.id):
            return f"這是 {mem.user_name} 的個人記憶，只有本人可以要求刪除"
        bot.memory.delete(gid, mem.id)
        return f"已忘記 #{mem.id}：{mem.content}"
    if name == "add_reaction":
        try:
            await message.add_reaction(str(args.get("emoji", "")).strip())
        except discord.HTTPException:
            return "無法使用這個表情，請換一個標準 emoji"
        turn.reacted = True
        return "已按表情"
    if bot.mcp.has(name):
        return await bot.mcp.call(name, args)
    return f"未知的工具：{name}"


async def chat_reply(bot: MusicBot, turn: ChatTurn) -> str:
    """呼叫 LLM；如果它要求工具就執行並把結果餵回去，直到它給出文字回覆。"""
    messages = await build_chat_messages(bot, turn.message, turn.chime_in)
    for round_ in range(CHAT_MAX_TOOL_ROUNDS + 1):
        if round_ < CHAT_MAX_TOOL_ROUNDS:
            tools = tools_for(turn)
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
            result = await run_tool(call["function"]["name"], args, turn)
            messages.append({"role": "tool", "tool_call_id": call.get("id", ""), "content": result})
        if turn.chime_in and turn.reacted:
            return ""   # 主動插話按完表情就結束，不用再花一輪 LLM 想要不要說話
    return ""


# ---------- /memory 指令 ----------

memory_group = app_commands.Group(name="memory", description="查看或管理機器人的長期記憶")


@memory_group.command(name="list", description="查看機器人記得的關於你的事，以及伺服器共同記憶")
async def memory_list(interaction: discord.Interaction):
    gid = interaction.guild.id if interaction.guild else 0
    mine = bot.memory.list_for(gid, interaction.user.id)
    lines = ["**關於你：**"] + [f"`#{m.id}` {m.content}" for m in mine] if mine else ["關於你還沒有任何記憶"]
    if interaction.guild:
        shared = bot.memory.list_for(gid, None)
        lines += ["", "**伺服器共同記憶：**"] + ([f"`#{m.id}` {m.content}" for m in shared] or ["（無）"])
    await interaction.response.send_message("\n".join(lines)[:2000], ephemeral=True)


@memory_group.command(name="forget", description="刪除一條記憶（自己的，或伺服器共同記憶）")
@app_commands.describe(memory_id="記憶編號（/memory list 裡 # 後面的數字）")
async def memory_forget(interaction: discord.Interaction, memory_id: int):
    gid = interaction.guild.id if interaction.guild else 0
    mem = bot.memory.get(gid, memory_id)
    if mem is None:
        return await interaction.response.send_message("找不到這條記憶", ephemeral=True)
    if mem.user_id is not None and mem.user_id != interaction.user.id:
        return await interaction.response.send_message("只能刪除自己的記憶", ephemeral=True)
    bot.memory.delete(gid, memory_id)
    await interaction.response.send_message(f"🗑️ 已刪除 #{memory_id}：{mem.content}", ephemeral=True)


@memory_group.command(name="summary", description="查看這個頻道目前的對話摘要")
async def memory_summary(interaction: discord.Interaction):
    summary = bot.memory.get_summary(interaction.channel_id)
    text = f"**頻道摘要：**\n{summary[0]}" if summary else "這個頻道還沒有摘要（訊息累積夠多後會自動產生）"
    await interaction.response.send_message(text[:2000], ephemeral=True)


@memory_group.command(name="reset_summary", description="清除這個頻道的對話摘要")
async def memory_reset_summary(interaction: discord.Interaction):
    bot.memory.clear_summary(interaction.channel_id)
    await interaction.response.send_message("🧹 已清除這個頻道的摘要", ephemeral=True)


bot.tree.add_command(memory_group)


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
