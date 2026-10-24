FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# yt-dlp 解析 YouTube 需要外部 JS runtime，官方推薦 Deno
COPY --from=denoland/deno:bin /deno /usr/local/bin/deno

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY bot.py .

# 每次啟動先更新 yt-dlp（YouTube 常改版，舊版很快就會壞）
CMD ["sh", "-c", "pip install -q -U --no-cache-dir 'yt-dlp[default]' && exec python bot.py"]
