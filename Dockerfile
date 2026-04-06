FROM python:3.11-slim

# System deps for yt-dlp ffmpeg
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# Create temp dir
RUN mkdir -p /tmp/hanime_bot

CMD ["python", "main.py"]
