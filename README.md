# 私人 Discord YouTube 音樂機器人

discord.py 2.7（支援 DAVE 語音加密）+ yt-dlp + FFmpeg + Deno，用 Docker 跑。

## 1. 建立 Discord Bot

1. 到 <https://discord.com/developers/applications> → **New Application**
2. 左側 **Bot**：
   - **Reset Token** → 複製 Token（只會顯示一次）
   - 把 **Public Bot** 關掉，只有你能邀請它
   - 要用聊天功能的話，打開 **Message Content Intent**（其他 Privileged Intents 不用開）
3. 左側 **OAuth2 → URL Generator**：
   - Scopes：`bot`、`applications.commands`
   - Bot Permissions：`Connect`、`Speak`、`Send Messages`、`View Channels`、`Read Message History`
   - 打開產生的網址，把機器人邀請到你的伺服器

## 2. 部署到 Ubuntu Server

```bash
cd musicbot
cp .env.example .env
nano .env            # 填 DISCORD_TOKEN 和 GUILD_ID
docker compose up -d --build
docker compose logs -f
```

看到「已登入」和「Slash 指令已同步」就完成了。

## 3. 指令

| 指令 | 說明 |
|---|---|
| `/play <網址或關鍵字>` | 播放影片、播放清單或搜尋 |
| `/skip` | 跳過 |
| `/pause` / `/resume` | 暫停／繼續 |
| `/stop` | 停止並清空佇列 |
| `/queue` | 查看佇列 |
| `/volume <0-100>` | 音量 |
| `/leave` | 離開語音頻道 |

### 聊天

在 `.env` 設定 `LLAMA_API_URL`（llama.cpp 等 OpenAI 相容 API）後，**@機器人** 或 **回覆機器人的訊息** 就能聊天。
機器人會讀取該頻道最近 `CHAT_HISTORY` 則訊息當上下文，所以能接上大家正在聊的話題。

聊天中也能直接叫它點歌、切歌、問現在在播什麼（例如「@機器人 放首適合讀書的歌」），點播者要在語音頻道裡。
這需要 llama.cpp 用 `--jinja` 啟動以支援 tool calling。

## 4. 疑難排解

- **突然不能播**：通常是 YouTube 改版，`docker compose restart` 會自動更新 yt-dlp；還是不行就等 yt-dlp 出新版。
- **出現「Sign in to confirm you're not a bot」**：用瀏覽器擴充套件（如 Get cookies.txt LOCALLY）匯出 YouTube 的 `cookies.txt`，放到 `musicbot/` 資料夾，取消 `docker-compose.yml` 裡 volumes 的註解後重啟。建議用小帳號的 cookies，不要用主帳號。
- **連不上語音頻道**：確認 discord.py 是 2.7 以上（`docker compose exec musicbot python -m discord --version`，會顯示 davey 版本）。
