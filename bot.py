"""私人 Discord YouTube 音樂機器人（discord.py + yt-dlp + FFmpeg）"""

import asyncio
import logging
import os
import subprocess
from collections import deque
from dataclasses import dataclass

import discord
import yt_dlp
from discord import app_commands
from discord.ext import commands

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("musicbot")

TOKEN = os.environ["DISCORD_TOKEN"]
GUILD_ID = os.environ.get("GUILD_ID")            # 填了的話 slash 指令會立即同步到該伺服器
COOKIES = os.environ.get("YTDLP_COOKIES", "/app/cookies.txt")
IDLE_TIMEOUT = int(os.environ.get("IDLE_TIMEOUT", "300"))   # 佇列空了多久後自動離開（秒）
MAX_PLAYLIST = int(os.environ.get("MAX_PLAYLIST", "50"))    # 一次最多加入幾首播放清單歌曲

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

            source = discord.PCMVolumeTransformer(
                discord.FFmpegPCMAudio(proc.stdout, pipe=True, **FFMPEG_OPTS), volume=self.volume
            )
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
        super().__init__(command_prefix=commands.when_mentioned, intents=discord.Intents.default())
        self.players: dict[int, GuildPlayer] = {}

    def player(self, guild: discord.Guild) -> GuildPlayer:
        if guild.id not in self.players:
            self.players[guild.id] = GuildPlayer(self, guild)
        return self.players[guild.id]

    async def setup_hook(self):
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

    async def on_ready(self):
        log.info("已登入：%s (%s)", self.user, self.user.id)

    async def on_voice_state_update(self, member, before, after):
        # 語音頻道只剩機器人時自動離開
        vc = member.guild.voice_client
        if vc and vc.channel and not any(not m.bot for m in vc.channel.members):
            await self.player(member.guild).disconnect()


bot = MusicBot()


def _vc(interaction: discord.Interaction) -> discord.VoiceClient | None:
    return interaction.guild.voice_client if interaction.guild else None


@bot.tree.command(name="play", description="播放 YouTube 網址、播放清單或搜尋關鍵字")
@app_commands.describe(query="YouTube 網址或關鍵字")
async def play(interaction: discord.Interaction, query: str):
    voice = getattr(interaction.user, "voice", None)
    if not voice or not voice.channel:
        return await interaction.response.send_message("你要先進語音頻道 🎧", ephemeral=True)

    await interaction.response.defer(thinking=True)

    vc = _vc(interaction)
    if vc is None:
        await voice.channel.connect(self_deaf=True)
    elif vc.channel != voice.channel:
        await vc.move_to(voice.channel)

    try:
        tracks = await asyncio.to_thread(search_tracks, query, interaction.user.display_name)
    except Exception as e:  # noqa: BLE001
        log.warning("搜尋失敗 %s：%s", query, e)
        return await interaction.followup.send("⚠️ 解析失敗，可能是 yt-dlp 需要更新或影片無法播放")

    if not tracks:
        return await interaction.followup.send("找不到結果 🤔")

    player = bot.player(interaction.guild)
    player.text_channel = interaction.channel
    player.add(tracks)

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
    player = bot.player(interaction.guild)
    lines = []
    if player.current:
        lines.append(f"🎵 **正在播放：** {player.current.title} `[{player.current.fmt_duration()}]`")
    for i, t in enumerate(list(player.queue)[:10], 1):
        lines.append(f"`{i}.` {t.title} `[{t.fmt_duration()}]`")
    if len(player.queue) > 10:
        lines.append(f"…還有 {len(player.queue) - 10} 首")
    await interaction.response.send_message("\n".join(lines) or "佇列是空的")


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
