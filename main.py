"""
Hanime Downloader Bot - Personal Use Only
Host on Railway via GitHub
Scrapes hanime.tv using BeautifulSoup + cloudscraper
Sends video to user + optional data storage group
Auto-deletes from server after upload
"""

import os
import re
import json
import base64
import asyncio
import logging
import functools
import time
from pathlib import Path
from urllib.parse import quote, urljoin

import cloudscraper
from bs4 import BeautifulSoup
from telethon import TelegramClient, events, Button
from telethon.tl.types import DocumentAttributeVideo

try:
    import yt_dlp
    YTDLP_AVAILABLE = True
except ImportError:
    YTDLP_AVAILABLE = False

# ═══════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("HanimeBot")

# ═══════════════════════════════════════════════════
#  ENVIRONMENT CONFIG
# ═══════════════════════════════════════════════════
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_ID = int(os.environ.get("ADMIN_ID", 0))
DATA_GROUP_ID = os.environ.get("DATA_GROUP_ID", "")  # optional

if not all([API_ID, API_HASH, BOT_TOKEN, ADMIN_ID]):
    logger.critical("Missing required env vars: API_ID, API_HASH, BOT_TOKEN, ADMIN_ID")
    raise SystemExit(1)

if DATA_GROUP_ID:
    DATA_GROUP_ID = int(DATA_GROUP_ID)
    logger.info(f"Data storage group enabled: {DATA_GROUP_ID}")

# ═══════════════════════════════════════════════════
#  CONSTANTS
# ═══════════════════════════════════════════════════
BASE_URL = "https://hanime.tv"
TEMP_DIR = Path("/tmp/hanime_bot")
TEMP_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://hanime.tv/",
    "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}

# ── cloudscraper session ──
scraper = cloudscraper.create_scraper(
    browser={
        "browser": "chrome",
        "platform": "windows",
        "mobile": False,
        "desktop": True,
    },
    delay=10,
)

# ── active downloads tracker (for cancel) ──
active_tasks: dict[int, asyncio.Task] = {}

# ═══════════════════════════════════════════════════
#  TELETHON CLIENT
# ═══════════════════════════════════════════════════
bot = TelegramClient("hanime_session", API_ID, API_HASH)

# ═══════════════════════════════════════════════════
#  UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════

def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


def progress_bar(current: int, total: int, length: int = 25) -> str:
    if total <= 0:
        return f"[{'░' * length}] 0%"
    pct = min(100.0, (current / total) * 100)
    filled = int((pct / 100) * length)
    bar = "█" * filled + "░" * (length - filled)
    return f"[{bar}] {pct:.1f}%"


def human_bytes(size: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


async def run_sync(func, *args, **kwargs):
    """Run blocking function in thread pool."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None, functools.partial(func, *args, **kwargs)
    )


def slug_to_b64(slug: str) -> str:
    return base64.b64encode(slug.encode()).decode().rstrip("=")


def b64_to_slug(b64: str) -> str:
    # Add padding back
    padding = 4 - len(b64) % 4
    if padding != 4:
        b64 += "=" * padding
    return base64.b64decode(b64).decode()


def clean_temp():
    """Remove all files in temp dir."""
    for f in TEMP_DIR.iterdir():
        try:
            f.unlink(missing_ok=True)
        except Exception:
            pass


# ═══════════════════════════════════════════════════
#  SCRAPER FUNCTIONS  (BeautifulSoup based)
# ═══════════════════════════════════════════════════

def _fetch(url: str) -> str:
    """Fetch page HTML via cloudscraper (blocking)."""
    r = scraper.get(url, headers=HEADERS, timeout=40)
    r.raise_for_status()
    return r.text


async def fetch_html(url: str) -> str:
    return await run_sync(_fetch, url)


def _parse_search_results(html: str) -> list[dict]:
    """Parse search/browse page HTML → list of hentai entries."""
    soup = BeautifulSoup(html, "lxml")
    results = []
    seen = set()

    # Try multiple selectors for video cards
    for sel in (
        "a[href*='/videos/hentai/']",
        ".video-card-container a",
        ".browse-card a",
    ):
        cards = soup.select(sel)
        if cards:
            break

    for card in cards:
        href = card.get("href", "")
        if "/videos/hentai/" not in href:
            continue
        slug = href.split("/videos/hentai/")[-1].split("?")[0].split("#")[0].strip("/")
        if not slug or slug in seen:
            continue
        seen.add(slug)

        # Title
        title = ""
        for tsel in (
            ".video-title",
            ".title",
            ".name",
            "h1", "h2", "h3", "h4",
            "span.title",
            ".video-card-title",
        ):
            el = card.select_one(tsel)
            if el and el.text.strip():
                title = el.text.strip()
                break
        if not title:
            title = card.get("title", "")
        if not title:
            title = slug.replace("-", " ").title()

        # Thumbnail
        thumb = ""
        img = card.select_one("img")
        if img:
            thumb = img.get("src") or img.get("data-src") or img.get("data-lazy-src") or ""

        results.append({
            "slug": slug,
            "title": title,
            "thumbnail": thumb,
            "url": f"{BASE_URL}/videos/hentai/{slug}",
        })

    return results


def _extract_video_urls(html: str, slug: str) -> dict:
    """
    Extract download URLs from the /downloads/ page.
    Returns {"360p": "url", "480p": "url", ...}
    Tries multiple strategies.
    """
    urls: dict[str, str] = {}

    # ── Strategy 1: __NEXT_DATA__ JSON ──
    next_match = re.search(
        r'<script\s+id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        html, re.DOTALL | re.IGNORECASE,
    )
    if next_match:
        try:
            data = json.loads(next_match.group(1))
            raw = json.dumps(data)
            for q in ("360p", "480p"):
                # Look for URLs near quality labels
                pattern = rf'"(?:url|src|stream|download)"\s*:\s*"(https?://[^"]*{q}[^"]*)"'
                m = re.search(pattern, raw, re.IGNORECASE)
                if m:
                    urls[q] = m.group(1)
        except Exception:
            pass

    # ── Strategy 2: Any JSON blob with quality+url ──
    if not urls:
        json_blobs = re.findall(r'\{[^{}]*?(?:360p|480p)[^{}]*?\}', html, re.DOTALL)
        for blob in json_blobs:
            try:
                obj = json.loads(blob)
                for key, val in obj.items():
                    if isinstance(val, str) and val.startswith("http"):
                        kl = key.lower()
                        vl = val.lower()
                        if "360" in kl or "360" in vl:
                            urls.setdefault("360p", val)
                        elif "480" in kl or "480" in vl:
                            urls.setdefault("480p", val)
            except Exception:
                pass

    # ── Strategy 3: <a> tags with quality text ──
    soup = BeautifulSoup(html, "lxml")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True).lower()
        for q in ("360p", "480p"):
            if q in href.lower() or q in text:
                full = href if href.startswith("http") else urljoin(BASE_URL, href)
                urls.setdefault(q, full)

    # ── Strategy 4: Regex for MP4 URLs ──
    if not urls:
        mp4s = re.findall(r'(https?://[^\s"\'<>\\]+\.mp4[^\s"\'<>\\]*)', html)
        for u in mp4s:
            ul = u.lower()
            if "360" in ul:
                urls.setdefault("360p", u)
            elif "480" in ul:
                urls.setdefault("480p", u)

    # ── Strategy 5: Regex for any URL with quality suffix ──
    if not urls:
        pattern = r'(https?://[^\s"\'<>\\]+?(?:360p|480p)[^\s"\'<>\\]*)'
        found = re.findall(pattern, html, re.IGNORECASE)
        for u in found:
            ul = u.lower()
            # Clean trailing junk
            u = re.sub(r'[\\,;)\]}]+$', '', u)
            if "360" in ul:
                urls.setdefault("360p", u)
            elif "480" in ul:
                urls.setdefault("480p", u)

    # ── Strategy 6: Look for m3u8 playlist URLs ──
    m3u8s = re.findall(r'(https?://[^\s"\'<>\\]+\.m3u8[^\s"\'<>\\]*)', html)
    for u in m3u8s:
        u = re.sub(r'[\\,;)\]}]+$', '', u)
        urls.setdefault("m3u8", u)

    # ── Strategy 7: video/source tags ──
    for src_el in soup.find_all(["source", "video"], src=True):
        src = src_el["src"]
        sl = src.lower()
        for q in ("360p", "480p"):
            if q in sl:
                urls.setdefault(q, src)

    return urls


async def search_hanime(query: str) -> list[dict]:
    """Search hanime.tv for hentai matching query."""
    try:
        html = await fetch_html(f"{BASE_URL}/search?q={quote(query)}")
        results = _parse_search_results(html)
        return results[:20]
    except Exception as e:
        logger.error(f"Search error: {e}")
        return []


async def browse_hanime(page: int = 1) -> tuple[list[dict], int | None]:
    """Browse hanime.tv catalog. Returns (results, next_page_or_None)."""
    try:
        html = await fetch_html(f"{BASE_URL}/browse/all?page={page}")
        results = _parse_search_results(html)

        # Check for next page
        soup = BeautifulSoup(html, "lxml")
        next_page = None
        for sel in ("a.next", ".pagination-next", "[rel='next']", "a:contains('Next')"):
            el = soup.select_one(sel)
            if el:
                try:
                    href = el["href"]
                    m = re.search(r'page=(\d+)', href)
                    if m:
                        next_page = int(m.group(1))
                    else:
                        next_page = page + 1
                    break
                except Exception:
                    next_page = page + 1
                    break

        return results, next_page
    except Exception as e:
        logger.error(f"Browse error: {e}")
        return [], None


async def get_download_links(slug: str) -> dict:
    """Get video download URLs for a slug."""
    try:
        b64 = slug_to_b64(slug)
        html = await fetch_html(f"{BASE_URL}/downloads/{b64}")
        urls = _extract_video_urls(html, slug)
        return urls
    except Exception as e:
        logger.error(f"Download links error for {slug}: {e}")
        return {}


async def get_video_info(slug: str) -> dict:
    """Get video info from the video page."""
    try:
        html = await fetch_html(f"{BASE_URL}/videos/hentai/{slug}")
        soup = BeautifulSoup(html, "lxml")

        title = ""
        for sel in ("h1.title", "h1", ".video-title", "title"):
            el = soup.select_one(sel)
            if el and el.text.strip():
                title = el.text.strip()
                break
        if not title:
            title = slug.replace("-", " ").title()

        # Try to get description
        desc = ""
        desc_el = soup.select_one(".description, .video-description, .summary")
        if desc_el:
            desc = desc_el.text.strip()[:300]

        # Thumbnail
        thumb = ""
        og_img = soup.select_one('meta[property="og:image"]')
        if og_img:
            thumb = og_img.get("content", "")
        if not thumb:
            img = soup.select_one("video poster, .video-player img")
            if img:
                thumb = img.get("poster") or img.get("src") or ""

        return {"title": title, "description": desc, "thumbnail": thumb}
    except Exception as e:
        logger.error(f"Video info error for {slug}: {e}")
        return {"title": slug.replace("-", " ").title(), "description": "", "thumbnail": ""}


# ═══════════════════════════════════════════════════
#  DOWNLOAD / UPLOAD FUNCTIONS
# ═══════════════════════════════════════════════════

async def download_file(
    url: str,
    filepath: Path,
    progress_msg=None,
    task_key: int = 0,
) -> bool:
    """Download a file with real-time progress. Returns True on success."""
    try:
        r = await run_sync(scraper.get, url, headers=HEADERS, stream=True, timeout=120)
        r.raise_for_status()

        total = int(r.headers.get("content-length", 0))
        downloaded = 0
        last_edit = 0.0

        with open(filepath, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 64):
                # Check cancellation
                if task_key and task_key in active_tasks and active_tasks[task_key].cancelled():
                    f.close()
                    filepath.unlink(missing_ok=True)
                    return False

                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)

                    now = time.time()
                    if progress_msg and total > 0 and (now - last_edit) > 1.5:
                        last_edit = now
                        bar = progress_bar(downloaded, total)
                        try:
                            await progress_msg.edit(
                                f"⬇️ **Downloading...**\n\n"
                                f"`{bar}`\n\n"
                                f"📦 {human_bytes(downloaded)} / {human_bytes(total)}"
                            )
                        except Exception:
                            pass

        return filepath.exists() and filepath.stat().st_size > 0
    except Exception as e:
        logger.error(f"Download failed: {e}")
        filepath.unlink(missing_ok=True)
        return False


async def download_with_ytdlp(
    url: str,
    filepath: Path,
    quality: str,
    progress_msg=None,
    task_key: int = 0,
) -> bool:
    """Download using yt-dlp as fallback."""
    if not YTDLP_AVAILABLE:
        return False

    height = quality.rstrip("p")
    format_str = f"bestvideo[height<={height}]+bestaudio/best[height<={height}]/worst"

    ydl_opts = {
        "format": format_str,
        "outtmpl": str(filepath),
        "quiet": True,
        "no_warnings": True,
        "noprogress": False,
        "merge_output_format": "mp4",
        "postprocessors": [{"key": "FFmpegMerger", "prefer_ffmpeg": True}],
    }

    def _dl():
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])

    try:
        await run_sync(_dl)
        return filepath.exists() and filepath.stat().st_size > 0
    except Exception as e:
        logger.error(f"yt-dlp download failed: {e}")
        filepath.unlink(missing_ok=True)
        return False


async def upload_video(
    client: TelegramClient,
    chat_id: int,
    filepath: Path,
    caption: str,
    progress_msg=None,
    task_key: int = 0,
) -> bool:
    """Upload video to Telegram with progress callback."""
    file_size = filepath.stat().st_size
    last_edit = 0.0

    async def on_progress(current, total):
        nonlocal last_edit
        now = time.time()
        if (now - last_edit) > 1.5:
            last_edit = now
            bar = progress_bar(current, total)
            try:
                await progress_msg.edit(
                    f"⬆️ **Uploading...**\n\n"
                    f"`{bar}`\n\n"
                    f"📦 {human_bytes(current)} / {human_bytes(total)}"
                )
            except Exception:
                pass

    try:
        await client.send_file(
            chat_id,
            str(filepath),
            caption=caption,
            supports_streaming=True,
            progress_callback=on_progress,
        )
        return True
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        return False


# ═══════════════════════════════════════════════════
#  MASTER DOWNLOAD HANDLER
# ═══════════════════════════════════════════════════

async def process_download(
    slug: str,
    quality: str,
    event,  # CallbackQuery event
):
    """Full pipeline: get links → download → upload → cleanup."""
    info = await get_video_info(slug)
    title = info["title"]

    task_key = event.sender_id
    progress_msg = await event.edit(
        f"⏳ **Fetching download links...**\n\n"
        f"🎬 {title}\n"
        f"📹 Quality: {quality}\n\n"
        f"Please wait..."
    )

    # ── Step 1: Get download links ──
    links = await get_download_links(slug)

    video_url = links.get(quality)

    # If requested quality not found, try any available
    if not video_url and links:
        available = [k for k in links if k != "m3u8"]
        if available:
            video_url = links[available[0]]
            quality = available[0]

    # ── Step 2: Download ──
    filepath = TEMP_DIR / f"{slug}_{quality}.mp4"
    filepath.unlink(missing_ok=True)

    if video_url and not video_url.endswith(".m3u8"):
        # Direct download
        ok = await download_file(video_url, filepath, progress_msg, task_key)
    elif video_url and video_url.endswith(".m3u8"):
        # HLS - use yt-dlp
        await progress_msg.edit(
            f"⬇️ **Downloading (HLS)...**\n\n"
            f"🎬 {title}\n"
            f"📹 Quality: {quality}\n\n"
            f"Using yt-dlp for HLS stream..."
        )
        ok = await download_with_ytdlp(video_url, filepath, quality, progress_msg, task_key)
    else:
        # Fallback: try yt-dlp with video page URL
        video_page_url = f"{BASE_URL}/videos/hentai/{slug}"
        await progress_msg.edit(
            f"⬇️ **Downloading (fallback)...**\n\n"
            f"🎬 {title}\n"
            f"📹 Quality: {quality}\n\n"
            f"Using yt-dlp fallback..."
        )
        ok = await download_with_ytdlp(video_page_url, filepath, quality, progress_msg, task_key)

    if not ok:
        filepath.unlink(missing_ok=True)
        await progress_msg.edit(
            f"❌ **Download Failed**\n\n"
            f"🎬 {title}\n"
            f"📹 Quality: {quality}\n\n"
            f"Possible reasons:\n"
            f"• Video unavailable\n"
            f"• Quality not available\n"
            f"• Cloudflare blocked request\n"
            f"• Network timeout\n\n"
            f"💡 Try a different quality.",
            buttons=[Button.inline("🔙 Home", data="back_home")],
        )
        return

    # ── Step 3: Prepare caption ──
    file_size = filepath.stat().st_size
    caption = (
        f"🎬 **{title}**\n"
        f"📹 Quality: {quality}\n"
        f"📦 Size: {human_bytes(file_size)}\n"
        f"🔗 [Source]({BASE_URL}/videos/hentai/{slug})\n\n"
        f"🤖 Hanime Downloader Bot"
    )

    # ── Step 4: Upload to user ──
    await progress_msg.edit(
        f"⬆️ **Uploading...**\n\n"
        f"🎬 {title}\n"
        f"📹 Quality: {quality}\n"
        f"📦 Size: {human_bytes(file_size)}\n\n"
        f"`{progress_bar(0, file_size)}`"
    )

    user_ok = await upload_video(bot, event.chat_id, filepath, caption, progress_msg, task_key)

    # ── Step 5: Upload to data group (if configured) ──
    if user_ok and DATA_GROUP_ID:
        group_caption = (
            f"💾 **Data Storage**\n\n"
            f"🎬 {title}\n"
            f"📹 {quality} • {human_bytes(file_size)}\n"
            f"🔗 [Source]({BASE_URL}/videos/hentai/{slug})"
        )
        try:
            await bot.send_file(
                DATA_GROUP_ID,
                str(filepath),
                caption=group_caption,
                supports_streaming=True,
            )
            logger.info(f"Also sent to data group {DATA_GROUP_ID}")
        except Exception as e:
            logger.error(f"Failed to send to data group: {e}")

    # ── Step 6: Cleanup ──
    try:
        filepath.unlink(missing_ok=True)
        logger.info(f"Cleaned up: {filepath}")
    except Exception:
        pass

    # ── Step 7: Final message ──
    if user_ok:
        try:
            await progress_msg.delete()
        except Exception:
            try:
                await progress_msg.edit(
                    f"✅ **Done!**\n\n🎬 {title}\n📹 {quality} • {human_bytes(file_size)}",
                    buttons=[Button.inline("🔙 Home", data="back_home")],
                )
            except Exception:
                pass
    else:
        filepath.unlink(missing_ok=True)
        try:
            await progress_msg.edit(
                f"❌ **Upload Failed**\n\n"
                f"🎬 {title}\n"
                f"📦 {human_bytes(file_size)}\n\n"
                f"File might be too large for Telegram.\n"
                f"Max: 2 GB for bots.",
                buttons=[Button.inline("🔙 Home", data="back_home")],
            )
        except Exception:
            pass


# ═══════════════════════════════════════════════════
#  COMMAND HANDLERS
# ═══════════════════════════════════════════════════

@bot.on(events.NewMessage(pattern="/start"))
async def cmd_start(event):
    if not is_admin(event.sender_id):
        return
    await event.reply(
        "👋 **Welcome to Hanime Downloader Bot!**\n\n"
        "🔒 *Personal Use Only*\n\n"
        "📌 **Quick Actions:**\n"
        "• Type any hentai name to search\n"
        "• Use commands below for specific actions\n\n"
        "⚡ **Commands:**\n"
        "├ `/help` — All commands & info\n"
        "├ `/search <name>` — Search hentai\n"
        "├ `/fetchall` — Browse full catalog\n"
        "├ `/dl <slug>` — Direct download by slug\n"
        "└ `/cancel` — Cancel ongoing download",
        buttons=[
            [Button.inline("📖 Help", data="help")],
            [Button.inline("🔍 Search", data="search_prompt"),
             Button.inline("📂 Browse All", data="browse_1")],
        ],
    )


@bot.on(events.NewMessage(pattern="/help"))
async def cmd_help(event):
    if not is_admin(event.sender_id):
        return
    await event.reply(
        "📘 **Bot Help**\n\n"
        "━━━ 🔍 SEARCH & DOWNLOAD ━━━\n\n"
        "`/search <query>`\n"
        "   Search for any hentai\n"
        "   Example: `/search sister breeder`\n\n"
        "`/fetchall`\n"
        "   Browse all available hentai\n"
        "   Paginated list with navigation\n\n"
        "`/dl <slug>`\n"
        "   Direct download by slug\n"
        "   Example: `/dl sister-breeder-3`\n\n"
        "`/cancel`\n"
        "   Cancel any ongoing download\n\n"
        "━━━ 💡 HOW IT WORKS ━━━\n\n"
        "1️⃣ Search or browse hentai\n"
        "2️⃣ Tap on a result to select\n"
        "3️⃣ Choose 360p or 480p quality\n"
        "4️⃣ Bot downloads with live progress\n"
        "5️⃣ Video sent to you + data group\n"
        "6️⃣ Auto-deleted from server\n\n"
        "━━━ 🔒 INFO ━━━\n\n"
        "• Admin-only bot\n"
        "• Real-time progress bars\n"
        "• Auto server cleanup\n"
        "• BeautifulSoup + cloudscraper",
        buttons=[Button.inline("🔙 Home", data="back_home")],
    )


@bot.on(events.NewMessage(pattern="/fetchall"))
async def cmd_fetchall(event):
    if not is_admin(event.sender_id):
        return
    msg = await event.reply("📂 **Fetching catalog...**\n\n⏳ Loading page 1...")
    results, next_page = await browse_hanime(page=1)

    if not results:
        await msg.edit("❌ Could not fetch catalog. Try again later.")
        return

    text = f"📂 **Hentai Catalog** — Page 1\n━━━━━━━━━━━━━━━━━━\n\n"
    buttons = []
    row = []

    for i, item in enumerate(results[:16], 1):
        t = item["title"][:32] + "…" if len(item["title"]) > 32 else item["title"]
        row.append(Button.inline(f"{i}. {t}", data=f"sel_{item['slug']}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    if next_page:
        buttons.append([Button.inline("➡️ Next Page", data=f"browse_{next_page}")])
    buttons.append([Button.inline("🔙 Home", data="back_home")])

    await msg.edit(text, buttons=buttons)


@bot.on(events.NewMessage(pattern=r"/search\s+(.+)"))
async def cmd_search(event):
    if not is_admin(event.sender_id):
        return
    query = event.pattern_match.group(1).strip()
    msg = await event.reply(f"🔍 **Searching:** `{query}`\n\n⏳ Please wait...")

    results = await search_hanime(query)

    if not results:
        await msg.edit(f"❌ No results for `{query}`\n\nTry different keywords.",
                       buttons=[Button.inline("🔙 Home", data="back_home")])
        return

    text = f"🔍 **Results for:** `{query}`\n━━━━━━━━━━━━━━━━━━\n**Found {len(results)} results**\n\n"
    buttons = []
    row = []

    for i, item in enumerate(results, 1):
        t = item["title"][:32] + "…" if len(item["title"]) > 32 else item["title"]
        row.append(Button.inline(f"{i}. {t}", data=f"sel_{item['slug']}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    buttons.append([Button.inline("🔙 Home", data="back_home")])
    await msg.edit(text, buttons=buttons)


@bot.on(events.NewMessage(pattern=r"/dl\s+(.+)"))
async def cmd_dl(event):
    if not is_admin(event.sender_id):
        return
    slug = event.pattern_match.group(1).strip()
    title = slug.replace("-", " ").title()

    await event.reply(
        f"🎬 **{title}**\n\n🔗 Slug: `{slug}`\n\n"
        f"Select quality:",
        buttons=[
            [Button.inline("📹 360p", data=f"dl_360p_{slug}"),
             Button.inline("📹 480p", data=f"dl_480p_{slug}")],
            [Button.inline("❌ Cancel", data="cancel")],
        ],
    )


@bot.on(events.NewMessage(pattern="/cancel"))
async def cmd_cancel(event):
    if not is_admin(event.sender_id):
        return
    uid = event.sender_id
    if uid in active_tasks and not active_tasks[uid].done():
        active_tasks[uid].cancel()
        del active_tasks[uid]
        await event.reply("🛑 **Download cancelled.**")
    else:
        await event.reply("ℹ️ No active download to cancel.")


# ── Plain text → auto search ──
@bot.on(events.NewMessage)
async def text_search(event):
    if not is_admin(event.sender_id):
        return
    text = event.text or ""
    if text.startswith("/"):
        return
    text = text.strip()
    if not text or len(text) < 2:
        return

    msg = await event.reply(f"🔍 **Searching:** `{text}`\n\n⏳ Please wait...")
    results = await search_hanime(text)

    if not results:
        await msg.edit(f"❌ No results for `{text}`\n\nTry different keywords.",
                       buttons=[Button.inline("🔙 Home", data="back_home")])
        return

    resp_text = f"🔍 **Results for:** `{text}`\n━━━━━━━━━━━━━━━━━━\n**Found {len(results)} results**\n\n"
    buttons = []
    row = []

    for i, item in enumerate(results, 1):
        t = item["title"][:32] + "…" if len(item["title"]) > 32 else item["title"]
        row.append(Button.inline(f"{i}. {t}", data=f"sel_{item['slug']}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    buttons.append([Button.inline("🔙 Home", data="back_home")])
    await msg.edit(resp_text, buttons=buttons)


# ═══════════════════════════════════════════════════
#  CALLBACK QUERY HANDLERS
# ═══════════════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"back_home"))
async def cb_home(event):
    if not is_admin(event.sender_id):
        await event.answer("⚠️ Unauthorized", alert=True)
        return
    await event.edit(
        "👋 **Hanime Downloader Bot**\n\n📌 **Quick Actions:**",
        buttons=[
            [Button.inline("📖 Help", data="help")],
            [Button.inline("🔍 Search", data="search_prompt"),
             Button.inline("📂 Browse All", data="browse_1")],
        ],
    )


@bot.on(events.CallbackQuery(data=b"help"))
async def cb_help(event):
    if not is_admin(event.sender_id):
        return
    await event.edit(
        "📘 **Bot Help**\n\n"
        "━━━ 🔍 COMMANDS ━━━\n\n"
        "`/search <query>` — Search hentai\n"
        "`/fetchall` — Browse catalog\n"
        "`/dl <slug>` — Direct download\n"
        "`/cancel` — Cancel download\n\n"
        "━━━ 💡 USAGE ━━━\n\n"
        "1️⃣ Search or browse\n"
        "2️⃣ Select episode\n"
        "3️⃣ Pick quality (360p/480p)\n"
        "4️⃣ Auto download & upload\n"
        "5️⃣ Server auto-cleanup\n\n"
        "🔒 Admin Only Bot",
        buttons=[Button.inline("🔙 Home", data="back_home")],
    )


@bot.on(events.CallbackQuery(data=b"search_prompt"))
async def cb_search_prompt(event):
    if not is_admin(event.sender_id):
        return
    await event.edit(
        "🔍 **Type a hentai name in chat**\n\n"
        "Just send the name as a message and\n"
        "I'll search it automatically!",
        buttons=[Button.inline("🔙 Home", data="back_home")],
    )


@bot.on(events.CallbackQuery(data=b"cancel"))
async def cb_cancel(event):
    if not is_admin(event.sender_id):
        return
    uid = event.sender_id
    if uid in active_tasks and not active_tasks[uid].done():
        active_tasks[uid].cancel()
        del active_tasks[uid]
        await event.answer("🛑 Cancelled!", alert=True)
        await event.edit("🛑 **Download cancelled.**",
                         buttons=[Button.inline("🔙 Home", data="back_home")])
    else:
        await event.answer("Nothing to cancel", alert=False)
        await event.edit("ℹ️ No active download.",
                         buttons=[Button.inline("🔙 Home", data="back_home")])


@bot.on(events.CallbackQuery(data=re.compile(rb"browse_(\d+)")))
async def cb_browse(event):
    if not is_admin(event.sender_id):
        return
    page = int(event.pattern_match.group(1))
    await event.answer("⏳ Loading...")
    msg = await event.edit(f"📂 **Fetching page {page}...**\n\n⏳ Please wait...")

    results, next_page = await browse_hanime(page=page)

    if not results:
        await msg.edit("❌ Could not fetch this page.",
                       buttons=[Button.inline("🔙 Home", data="back_home")])
        return

    text = f"📂 **Hentai Catalog** — Page {page}\n━━━━━━━━━━━━━━━━━━\n\n"
    buttons = []
    row = []

    for i, item in enumerate(results[:16], 1):
        t = item["title"][:32] + "…" if len(item["title"]) > 32 else item["title"]
        row.append(Button.inline(f"{i}. {t}", data=f"sel_{item['slug']}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    nav = []
    if page > 1:
        nav.append(Button.inline("⬅️ Prev", data=f"browse_{page - 1}"))
    if next_page:
        nav.append(Button.inline("➡️ Next", data=f"browse_{next_page}"))
    if nav:
        buttons.append(nav)

    buttons.append([Button.inline("🔙 Home", data="back_home")])
    await msg.edit(text, buttons=buttons)


@bot.on(events.CallbackQuery(data=re.compile(rb"sel_(.+)")))
async def cb_select(event):
    if not is_admin(event.sender_id):
        return
    slug = event.pattern_match.group(1).decode()
    title = slug.replace("-", " ").title()

    await event.edit(
        f"🎬 **{title}**\n\n🔗 Slug: `{slug}`\n\n"
        f"━━━ Select Quality ━━━",
        buttons=[
            [Button.inline("📹 360p", data=f"dl_360p_{slug}"),
             Button.inline("📹 480p", data=f"dl_480p_{slug}")],
            [Button.inline("❌ Cancel", data="cancel")],
        ],
    )


@bot.on(events.CallbackQuery(data=re.compile(rb"dl_(360p|480p)_(.+)")))
async def cb_download(event):
    if not is_admin(event.sender_id):
        return
    quality = event.pattern_match.group(1).decode()
    slug = event.pattern_match.group(2).decode()
    await event.answer("🚀 Starting...")

    # Create background task so callback doesn't timeout
    task = asyncio.create_task(process_download(slug, quality, event))
    active_tasks[event.sender_id] = task

    try:
        await task
    except asyncio.CancelledError:
        logger.info(f"Download cancelled for {slug}")
    except Exception as e:
        logger.error(f"Download task error: {e}")
    finally:
        active_tasks.pop(event.sender_id, None)


# ═══════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ═══════════════════════════════════════════════════

async def main():
    clean_temp()
    logger.info("=" * 50)
    logger.info(f"  Hanime Downloader Bot")
    logger.info(f"  Admin ID: {ADMIN_ID}")
    logger.info(f"  Data Group: {DATA_GROUP_ID or 'Not set'}")
    logger.info(f"  yt-dlp: {'Available' if YTDLP_AVAILABLE else 'Not installed'}")
    logger.info("=" * 50)

    await bot.start(bot_token=BOT_TOKEN)
    me = await bot.get_me()
    logger.info(f"Bot @{me.username} is running!")

    await bot.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
