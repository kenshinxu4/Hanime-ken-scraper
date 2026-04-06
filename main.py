"""
Hanime Downloader Bot v2 — API-First Approach
Fix: hanime.tv is Next.js SPA, HTML scraping doesn't work.
Now uses actual API endpoints + yt-dlp fallback.
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
log = logging.getLogger("HanimeBot")

# ═══════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_ID = int(os.environ.get("ADMIN_ID", 0))
DATA_GROUP_ID = os.environ.get("DATA_GROUP_ID", "")

if not all([API_ID, API_HASH, BOT_TOKEN, ADMIN_ID]):
    log.critical("Missing env vars: API_ID, API_HASH, BOT_TOKEN, ADMIN_ID")
    raise SystemExit(1)

if DATA_GROUP_ID:
    DATA_GROUP_ID = int(DATA_GROUP_ID)

BASE_URL = "https://hanime.tv"
TEMP_DIR = Path("/tmp/hanime_bot")
TEMP_DIR.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════
#  CLOUDSCRAPER SESSION (with cookie persistence)
# ═══════════════════════════════════════════════════
scraper = cloudscraper.create_scraper(
    browser={"browser": "chrome", "platform": "windows", "mobile": False, "desktop": True},
    delay=10,
)
scraper.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://hanime.tv/",
    "Origin": "https://hanime.tv",
    "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
})

active_tasks: dict[int, asyncio.Task] = {}

# ═══════════════════════════════════════════════════
#  TELETHON
# ═══════════════════════════════════════════════════
bot = TelegramClient("hanime_session", API_ID, API_HASH)

# ═══════════════════════════════════════════════════
#  UTILS
# ═══════════════════════════════════════════════════
def is_admin(uid: int) -> bool:
    return uid == ADMIN_ID

def progress_bar(cur: int, tot: int, length: int = 25) -> str:
    if tot <= 0:
        return f"[{'░' * length}] 0%"
    pct = min(100.0, (cur / tot) * 100)
    filled = int((pct / 100) * length)
    return f"[{'█' * filled}{'░' * (length - filled)}] {pct:.1f}%"

def human_bytes(size: int) -> str:
    for u in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.1f} {u}"
        size /= 1024
    return f"{size:.1f} TB"

async def run_sync(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, functools.partial(func, *args, **kwargs))

def slug_to_b64(slug: str) -> str:
    return base64.b64encode(slug.encode()).decode().rstrip("=")

def b64_to_slug(b64: str) -> str:
    pad = 4 - len(b64) % 4
    if pad != 4:
        b64 += "=" * pad
    return base64.b64decode(b64).decode()

# ═══════════════════════════════════════════════════
#  SESSION INIT — get cookies first
# ═══════════════════════════════════════════════════
def init_session():
    """Load main page once to get Cloudflare cookies."""
    try:
        scraper.get(BASE_URL, timeout=40)
        log.info("✅ Session initialized, cookies obtained")
    except Exception as e:
        log.warning(f"⚠️ Session init warning: {e}")

# ═══════════════════════════════════════════════════
#  API LAYER — Search & Browse
# ═══════════════════════════════════════════════════

def _normalize_video(v: dict) -> dict | None:
    """Convert various API response formats to standard dict."""
    if not isinstance(v, dict):
        return None
    slug = v.get("slug", "") or v.get("hentai_slug", "") or v.get("video_slug", "")
    if not slug:
        return None
    name = (
        v.get("name", "") or v.get("title", "") or v.get("video_name", "")
        or v.get("hentai_name", "") or slug.replace("-", " ").title()
    )
    cover = (
        v.get("cover_url", "") or v.get("thumbnail", "") or v.get("poster_url", "")
        or v.get("homepage_thumbnail_url", "") or v.get("img_url", "")
        or v.get("cover", "") or ""
    )
    return {
        "slug": slug,
        "title": name.strip(),
        "thumbnail": cover,
        "url": f"{BASE_URL}/videos/hentai/{slug}",
    }


def _extract_videos_from_json(data) -> list[dict]:
    """Try to find video list in any JSON structure."""
    results = []
    if isinstance(data, dict):
        # Try common keys
        for key in ("hentai_videos", "videos", "results", "data", "items", "list"):
            val = data.get(key)
            if isinstance(val, list):
                for v in val:
                    nv = _normalize_video(v)
                    if nv:
                        results.append(nv)
                if results:
                    return results
        # Try pageProps
        pp = data.get("props", {}).get("pageProps", {})
        if isinstance(pp, dict):
            for key in ("hentai_videos", "videos", "results", "data"):
                val = pp.get(key)
                if isinstance(val, list):
                    for v in val:
                        nv = _normalize_video(v)
                        if nv:
                            results.append(nv)
                    if results:
                        return results
            # Single video
            hv = pp.get("hentai_video", {})
            if isinstance(hv, dict):
                nv = _normalize_video(hv)
                if nv:
                    results.append(nv)
    elif isinstance(data, list):
        for v in data:
            nv = _normalize_video(v)
            if nv:
                results.append(nv)
    return results


def _api_search(query: str) -> list[dict]:
    """Search hanime via multiple API endpoints."""
    endpoints = [
        (f"{BASE_URL}/api/v8/search", {"q": query, "tags": ""}),
        (f"{BASE_URL}/api/v8/search", {"q": query}),
        (f"{BASE_URL}/search/api/v1/search", {"q": query}),
        (f"{BASE_URL}/api/search", {"q": query}),
    ]

    for url, params in endpoints:
        try:
            r = scraper.get(url, params=params, timeout=30)
            if r.status_code != 200:
                continue
            ct = r.headers.get("content-type", "")
            if "json" in ct:
                data = r.json()
            else:
                # Try parsing as JSON anyway
                try:
                    data = r.json()
                except Exception:
                    # Not JSON — try extracting from HTML
                    log.debug(f"Endpoint {url} returned HTML, trying extraction...")
                    videos = _extract_videos_from_html(r.text)
                    if videos:
                        return videos
                    continue

            videos = _extract_videos_from_json(data)
            if videos:
                log.info(f"✅ Search '{query}' found {len(videos)} results via {url}")
                return videos
        except Exception as e:
            log.debug(f"Search endpoint {url} failed: {e}")
            continue

    # Fallback: HTML scraping with multiple page types
    log.info("API search failed, trying HTML fallback...")
    for path in (f"/search?q={quote(query)}", f"/browse?search={quote(query)}"):
        try:
            r = scraper.get(f"{BASE_URL}{path}", timeout=30)
            videos = _extract_videos_from_html(r.text)
            if videos:
                log.info(f"✅ Search '{query}' found {len(videos)} results via HTML fallback")
                return videos
        except Exception:
            continue

    return []


def _extract_videos_from_html(html: str) -> list[dict]:
    """Last resort: extract video links from HTML."""
    results = []
    seen = set()

    # Strategy 1: __NEXT_DATA__
    nd_match = re.search(
        r'<script\s+id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        html, re.DOTALL | re.IGNORECASE,
    )
    if nd_match:
        try:
            nd = json.loads(nd_match.group(1))
            vids = _extract_videos_from_json(nd)
            results.extend(vids)
        except Exception:
            pass

    if results:
        return results

    # Strategy 2: Find all /videos/hentai/ links
    soup = BeautifulSoup(html, "lxml")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/videos/hentai/" not in href:
            continue
        slug = href.split("/videos/hentai/")[-1].split("?")[0].split("#")[0].strip("/")
        if not slug or slug in seen:
            continue
        seen.add(slug)

        title = ""
        for sel in (".video-title", ".title", ".name", "h1", "h2", "h3", "span.title"):
            el = a.select_one(sel)
            if el and el.text.strip():
                title = el.text.strip()
                break
        if not title:
            title = a.get("title", "") or slug.replace("-", " ").title()

        thumb = ""
        img = a.select_one("img")
        if img:
            thumb = img.get("src") or img.get("data-src") or ""

        results.append({
            "slug": slug,
            "title": title,
            "thumbnail": thumb,
            "url": f"{BASE_URL}/videos/hentai/{slug}",
        })

    return results


def _api_browse(page: int = 1) -> tuple[list[dict], int | None]:
    """Browse catalog via API endpoints."""
    endpoints = [
        (f"{BASE_URL}/api/v8/browse", {"tags": "all", "page": page, "order": "newest", "broadband": ""}),
        (f"{BASE_URL}/api/v8/browse", {"tags": "all", "page": page, "order": "newest"}),
        (f"{BASE_URL}/api/v8/browse", {"tags": "all", "page": page}),
        (f"{BASE_URL}/api/browse", {"page": page, "order": "newest"}),
    ]

    for url, params in endpoints:
        try:
            r = scraper.get(url, params=params, timeout=30)
            if r.status_code != 200:
                continue
            ct = r.headers.get("content-type", "")
            if "json" in ct:
                data = r.json()
            else:
                try:
                    data = r.json()
                except Exception:
                    videos = _extract_videos_from_html(r.text)
                    if videos:
                        return videos, (page + 1 if len(videos) >= 20 else None)
                    continue

            videos = _extract_videos_from_json(data)
            if videos:
                next_page = None
                if isinstance(data, dict):
                    np = data.get("next_page")
                    if np:
                        try:
                            next_page = int(np)
                        except (ValueError, TypeError):
                            next_page = page + 1
                    elif len(videos) >= 20:
                        next_page = page + 1
                log.info(f"✅ Browse page {page}: {len(videos)} results via {url}")
                return videos, next_page
        except Exception as e:
            log.debug(f"Browse endpoint {url} failed: {e}")
            continue

    # HTML fallback
    log.info("API browse failed, trying HTML fallback...")
    try:
        r = scraper.get(f"{BASE_URL}/browse/all?page={page}", timeout=30)
        videos = _extract_videos_from_html(r.text)
        if videos:
            return videos, (page + 1 if len(videos) >= 20 else None)
    except Exception:
        pass

    return [], None


async def search_hanime(query: str) -> list[dict]:
    return await run_sync(_api_search, query)


async def browse_hanime(page: int = 1) -> tuple[list[dict], int | None]:
    return await run_sync(_api_browse, page)


# ═══════════════════════════════════════════════════
#  DOWNLOAD LINK EXTRACTION
# ═══════════════════════════════════════════════════

def _find_urls_recursive(obj, depth=0) -> list[str]:
    """Recursively find video URLs in any JSON structure."""
    if depth > 20:
        return []
    urls = []
    if isinstance(obj, str):
        if obj.startswith("http") and any(x in obj.lower() for x in (".mp4", ".m3u8", "stream", "download", "cdn")):
            urls.append(obj)
    elif isinstance(obj, dict):
        for k, v in obj.items():
            kl = k.lower()
            vl = v.lower() if isinstance(v, str) else ""
            if isinstance(v, str) and v.startswith("http"):
                if any(x in kl or x in vl for x in ("360", "480", "720", "1080", "quality", "stream", "download", "mp4", "m3u8")):
                    urls.append(v)
            urls.extend(_find_urls_recursive(v, depth + 1))
    elif isinstance(obj, list):
        for item in obj:
            urls.extend(_find_urls_recursive(item, depth + 1))
    return urls


def _classify_url(url: str) -> str | None:
    """Return quality label based on URL content, or None."""
    ul = url.lower()
    if "m3u8" in ul:
        return "m3u8"
    if "1080" in ul:
        return "1080p"
    if "720" in ul:
        return "720p"
    if "480" in ul:
        return "480p"
    if "360" in ul:
        return "360p"
    if ".mp4" in ul:
        return "mp4"
    return None


def _extract_download_urls_from_html(html: str) -> dict[str, str]:
    """Extract download URLs from HTML using ALL possible strategies."""
    urls: dict[str, str] = {}

    # ── Strategy 1: Parse ALL JSON blobs in the page ──
    # __NEXT_DATA__
    nd = re.search(r'<script\s+id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL | re.IGNORECASE)
    if nd:
        try:
            data = json.loads(nd.group(1))
            for u in _find_urls_recursive(data):
                q = _classify_url(u)
                if q:
                    urls.setdefault(q, u)
        except Exception:
            pass

    # Other script tags with JSON
    for sm in re.finditer(r'<script[^>]*>(.*?)</script>', html, re.DOTALL):
        txt = sm.group(1).strip()
        if txt.startswith(("{", "[")):
            try:
                data = json.loads(txt)
                for u in _find_urls_recursive(data):
                    q = _classify_url(u)
                    if q:
                        urls.setdefault(q, u)
            except Exception:
                pass

    # ── Strategy 2: Raw regex for MP4 URLs ──
    for m in re.finditer(r'(https?://[^\s"\'<>\\,\)}\]]+\.mp4[^\s"\'<>\\,\)}\]]*)', html):
        u = m.group(1).rstrip("\\,;)}]")
        q = _classify_url(u)
        if q:
            urls.setdefault(q, u)

    # ── Strategy 3: Regex for URLs containing quality strings ──
    for m in re.finditer(r'(https?://[^\s"\'<>\\,\)}\]]+?(?:360p|480p|720p|1080p)[^\s"\'<>\\,\)}\]]*)', html, re.IGNORECASE):
        u = m.group(1).rstrip("\\,;)}]")
        q = _classify_url(u)
        if q:
            urls.setdefault(q, u)

    # ── Strategy 4: M3U8 URLs ──
    for m in re.finditer(r'(https?://[^\s"\'<>\\,\)}\]]+\.m3u8[^\s"\'<>\\,\)}\]]*)', html):
        u = m.group(1).rstrip("\\,;)}]")
        urls.setdefault("m3u8", u)

    # ── Strategy 5: <a> download links ──
    soup = BeautifulSoup(html, "lxml")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True).lower()
        for q in ("360p", "480p", "720p", "1080p"):
            if q in href.lower() or q in text:
                full = href if href.startswith("http") else urljoin(BASE_URL, href)
                urls.setdefault(q, full)

    # ── Strategy 6: <source> / <video> tags ──
    for el in soup.find_all(["source", "video"], src=True):
        src = el["src"]
        q = _classify_url(src)
        if q:
            urls.setdefault(q, src)

    # ── Strategy 7: data-* attributes on elements ──
    for el in soup.find_all(attrs=True):
        for attr_name, attr_val in el.attrs.items():
            if isinstance(attr_val, str) and attr_val.startswith("http") and any(x in attr_val.lower() for x in (".mp4", ".m3u8")):
                q = _classify_url(attr_val)
                if q:
                    urls.setdefault(q, attr_val)

    return urls


def _get_download_links(slug: str) -> dict[str, str]:
    """Master download link fetcher — tries multiple sources."""
    all_urls: dict[str, str] = {}

    # ── Source 1: Download page (/downloads/BASE64) ──
    b64 = slug_to_b64(slug)
    try:
        r = scraper.get(
            f"{BASE_URL}/downloads/{b64}",
            headers={**dict(scraper.headers), "Accept": "text/html,application/xhtml+xml,*/*"},
            timeout=40,
        )
        urls = _extract_download_urls_from_html(r.text)
        all_urls.update(urls)
        if any(k in all_urls for k in ("360p", "480p")):
            log.info(f"✅ Download links from /downloads/ page: {list(all_urls.keys())}")
            return all_urls
    except Exception as e:
        log.warning(f"Download page error: {e}")

    # ── Source 2: Video page (/videos/hentai/SLUG) ──
    try:
        r = scraper.get(
            f"{BASE_URL}/videos/hentai/{slug}",
            headers={**dict(scraper.headers), "Accept": "text/html,application/xhtml+xml,*/*"},
            timeout=40,
        )
        urls = _extract_download_urls_from_html(r.text)
        all_urls.update(urls)
        if any(k in all_urls for k in ("360p", "480p", "m3u8")):
            log.info(f"✅ Download links from video page: {list(all_urls.keys())}")
            return all_urls
    except Exception as e:
        log.warning(f"Video page error: {e}")

    # ── Source 3: API endpoints for video ──
    for ep in (
        f"{BASE_URL}/api/v8/video/{slug}",
        f"{BASE_URL}/api/video/{slug}",
    ):
        try:
            r = scraper.get(ep, timeout=30)
            if r.status_code == 200:
                try:
                    data = r.json()
                    for u in _find_urls_recursive(data):
                        q = _classify_url(u)
                        if q:
                            all_urls.setdefault(q, u)
                except Exception:
                    pass
        except Exception:
            pass

    if any(k in all_urls for k in ("360p", "480p", "m3u8")):
        log.info(f"✅ Download links from API: {list(all_urls.keys())}")
        return all_urls

    log.warning(f"⚠️ No direct download links found for {slug}, will try yt-dlp")
    return all_urls


async def get_download_links(slug: str) -> dict[str, str]:
    return await run_sync(_get_download_links, slug)


async def get_video_info(slug: str) -> dict:
    def _get():
        title = slug.replace("-", " ").title()
        desc = ""
        thumb = ""
        try:
            r = scraper.get(
                f"{BASE_URL}/videos/hentai/{slug}",
                headers={**dict(scraper.headers), "Accept": "text/html,application/xhtml+xml,*/*"},
                timeout=40,
            )
            html = r.text

            # Try __NEXT_DATA__
            nd = re.search(r'<script\s+id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL | re.IGNORECASE)
            if nd:
                try:
                    ndata = json.loads(nd.group(1))
                    # Navigate to hentai_video
                    hv = ndata
                    for key in ("props", "pageProps"):
                        if isinstance(hv, dict):
                            hv = hv.get(key, hv)
                    if isinstance(hv, dict):
                        hv = hv.get("hentai_video", hv)
                    if isinstance(hv, dict):
                        title = hv.get("name", "") or hv.get("title", "") or title
                        desc = str(hv.get("description", ""))[:500]
                        thumb = (
                            hv.get("cover_url", "") or hv.get("thumbnail", "")
                            or hv.get("poster_url", "") or hv.get("img_url", "") or thumb
                        )
                except Exception:
                    pass

            if not thumb:
                soup = BeautifulSoup(html, "lxml")
                og = soup.select_one('meta[property="og:image"]')
                if og:
                    thumb = og.get("content", "")
        except Exception as e:
            log.error(f"Video info error: {e}")

        return {"title": title, "description": desc, "thumbnail": thumb}
    return await run_sync(_get)


# ═══════════════════════════════════════════════════
#  DOWNLOAD / UPLOAD
# ═══════════════════════════════════════════════════

async def download_file(url: str, filepath: Path, progress_msg=None, task_key: int = 0) -> bool:
    try:
        r = await run_sync(
            scraper.get, url,
            {"headers": HEADERS_JSON if 'json' in scraper.headers.get('accept','').lower() else dict(scraper.headers), "stream": True, "timeout": 180}
        )
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        downloaded = 0
        last_edit = 0.0

        with open(filepath, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 64):
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
                        try:
                            await progress_msg.edit(
                                f"⬇️ **Downloading...**\n\n"
                                f"`{progress_bar(downloaded, total)}`\n\n"
                                f"📦 {human_bytes(downloaded)} / {human_bytes(total)}"
                            )
                        except Exception:
                            pass
        return filepath.exists() and filepath.stat().st_size > 0
    except Exception as e:
        log.error(f"Download failed: {e}")
        filepath.unlink(missing_ok=True)
        return False


async def download_with_ytdlp(url: str, filepath: Path, quality: str, progress_msg=None, task_key: int = 0) -> bool:
    if not YTDLP_AVAILABLE:
        return False
    height = quality.rstrip("p")
    ydl_opts = {
        "format": f"bestvideo[height<={height}]+bestaudio/best[height<={height}]/worst",
        "outtmpl": str(filepath),
        "quiet": True,
        "no_warnings": True,
        "merge_output_format": "mp4",
        "postprocessors": [{"key": "FFmpegMerger", "prefer_ffmpeg": True}],
        "noprogress": False,
        "progress_hooks": [],
    }

    # Progress hook
    last_update = [0.0]
    def hook(d):
        if d.get("status") == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate", 0) or 0
            cur = d.get("downloaded_bytes", 0)
            now = time.time()
            if now - last_update[0] > 2.0:
                last_update[0] = now
                if total > 0:
                    # This runs in a thread, can't edit message directly
                    # We'll rely on the progress_msg periodic update in the caller
                    pass
    ydl_opts["progress_hooks"] = [hook]

    def _dl():
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])

    try:
        await run_sync(_dl)
        return filepath.exists() and filepath.stat().st_size > 0
    except Exception as e:
        log.error(f"yt-dlp failed: {e}")
        filepath.unlink(missing_ok=True)
        return False


async def upload_video(client, chat_id, filepath, caption, progress_msg=None, task_key=0) -> bool:
    file_size = filepath.stat().st_size
    last_edit = [0.0]

    async def on_progress(current, total):
        now = time.time()
        if now - last_edit[0] > 1.5:
            last_edit[0] = now
            try:
                await progress_msg.edit(
                    f"⬆️ **Uploading...**\n\n"
                    f"`{progress_bar(current, total)}`\n\n"
                    f"📦 {human_bytes(current)} / {human_bytes(total)}"
                )
            except Exception:
                pass

    try:
        await client.send_file(
            chat_id, str(filepath),
            caption=caption,
            supports_streaming=True,
            progress_callback=on_progress,
        )
        return True
    except Exception as e:
        log.error(f"Upload failed: {e}")
        return False


# ═══════════════════════════════════════════════════
#  MASTER DOWNLOAD PIPELINE
# ═══════════════════════════════════════════════════

async def process_download(slug: str, quality: str, event):
    info = await get_video_info(slug)
    title = info["title"]
    task_key = event.sender_id

    pmsg = await event.edit(
        f"⏳ **Fetching download links...**\n\n"
        f"🎬 {title}\n📹 Quality: {quality}"
    )

    links = await get_download_links(slug)
    video_url = links.get(quality)

    # Fallback quality
    if not video_url:
        for fallback_q in ("360p", "480p", "720p", "1080p", "m3u8", "mp4"):
            if fallback_q in links:
                video_url = links[fallback_q]
                quality = fallback_q
                break

    filepath = TEMP_DIR / f"{slug}_{quality}.mp4"
    filepath.unlink(missing_ok=True)

    ok = False

    # ── Try direct download ──
    if video_url and not video_url.endswith(".m3u8"):
        await pmsg.edit(
            f"⬇️ **Downloading...**\n\n🎬 {title}\n📹 {quality}\n`{progress_bar(0, 100)}`"
        )
        ok = await download_file(video_url, filepath, pmsg, task_key)

    # ── Try yt-dlp for HLS ──
    if not ok and video_url and video_url.endswith(".m3u8"):
        await pmsg.edit(
            f"⬇️ **Downloading (HLS stream)...**\n\n🎬 {title}\n📹 {quality}\nUsing yt-dlp..."
        )
        ok = await download_with_ytdlp(video_url, filepath, quality, pmsg, task_key)

    # ── Try yt-dlp with video page URL as ultimate fallback ──
    if not ok:
        await pmsg.edit(
            f"⬇️ **Downloading (yt-dlp fallback)...**\n\n🎬 {title}\n📹 {quality}\n"
            f"Trying video page URL..."
        )
        ok = await download_with_ytdlp(
            f"{BASE_URL}/videos/hentai/{slug}", filepath, quality, pmsg, task_key
        )

    if not ok:
        filepath.unlink(missing_ok=True)
        await pmsg.edit(
            f"❌ **Download Failed**\n\n"
            f"🎬 {title}\n📹 {quality}\n\n"
            f"Reasons:\n"
            f"• Video unavailable or geo-blocked\n"
            f"• Quality not available\n"
            f"• Cloudflare blocked\n"
            f"• yt-dlp not installed\n\n"
            f"💡 Try different quality or check logs.",
            buttons=[Button.inline("🔙 Home", data="back_home")],
        )
        return

    # ── Upload ──
    file_size = filepath.stat().st_size
    caption = (
        f"🎬 **{title}**\n"
        f"📹 Quality: {quality}\n"
        f"📦 Size: {human_bytes(file_size)}\n"
        f"🔗 [Source]({BASE_URL}/videos/hentai/{slug})\n\n"
        f"🤖 Hanime Bot"
    )

    await pmsg.edit(
        f"⬆️ **Uploading...**\n\n🎬 {title}\n📹 {quality}\n📦 {human_bytes(file_size)}\n"
        f"`{progress_bar(0, file_size)}`"
    )

    user_ok = await upload_video(bot, event.chat_id, filepath, caption, pmsg, task_key)

    # ── Data group ──
    if user_ok and DATA_GROUP_ID:
        gcap = (
            f"💾 **Data Storage**\n\n"
            f"🎬 {title}\n"
            f"📹 {quality} • {human_bytes(file_size)}\n"
            f"🔗 [Source]({BASE_URL}/videos/hentai/{slug})"
        )
        try:
            await bot.send_file(DATA_GROUP_ID, str(filepath), caption=gcap, supports_streaming=True)
        except Exception as e:
            log.error(f"Data group send failed: {e}")

    # ── Cleanup ──
    try:
        filepath.unlink(missing_ok=True)
    except Exception:
        pass

    if user_ok:
        try:
            await pmsg.delete()
        except Exception:
            pass
    else:
        filepath.unlink(missing_ok=True)
        try:
            await pmsg.edit(
                f"❌ **Upload Failed**\n\n🎬 {title}\n📦 {human_bytes(file_size)}\n\n"
                f"Max bot upload: 2 GB",
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
        "👋 **Hanime Downloader Bot**\n\n"
        "🔒 *Personal Use Only*\n\n"
        "📌 **How to use:**\n"
        "Just type any hentai name — I'll search automatically!\n\n"
        "⚡ **Commands:**\n"
        "├ `/help` — All commands\n"
        "├ `/search <name>` — Search\n"
        "├ `/fetchall` — Browse catalog\n"
        "├ `/dl <slug>` — Direct download\n"
        "└ `/cancel` — Cancel download\n"
        "├ `/debug` — Test API connection",
        buttons=[
            [Button.inline("📖 Help", data="help")],
            [Button.inline("🔍 Search", data="search_prompt"),
             Button.inline("📂 Browse All", data="browse_1")],
            [Button.inline("🧪 Debug API", data="debug")],
        ],
    )


@bot.on(events.NewMessage(pattern="/help"))
async def cmd_help(event):
    if not is_admin(event.sender_id):
        return
    await event.reply(
        "📘 **Help**\n\n"
        "`/search <query>` — Search hentai\n"
        "`/fetchall` — Browse full catalog\n"
        "`/dl <slug>` — Download by slug\n"
        "`/cancel` — Cancel download\n"
        "`/debug` — Test if API works\n\n"
        "💡 **Tip:** Just type a name directly,\n"
        "no need for /search command!",
        buttons=[Button.inline("🔙 Home", data="back_home")],
    )


@bot.on(events.NewMessage(pattern="/debug"))
async def cmd_debug(event):
    if not is_admin(event.sender_id):
        return
    msg = await event.reply("🧪 **Testing API connection...**")
    try:
        # Test 1: Main page
        r1 = await run_sync(scraper.get, BASE_URL, {"timeout": 20})
        s1 = "✅" if r1.status_code == 200 else f"❌ {r1.status_code}"

        # Test 2: Search API
        r2 = await run_sync(scraper.get, f"{BASE_URL}/api/v8/search", {"params": {"q": "test"}, "timeout": 20})
        s2 = "✅" if r2.status_code == 200 else f"❌ {r2.status_code}"
        body2 = ""
        try:
            d = r2.json()
            body2 = f"\n`{json.dumps(d)[:200]}`"
        except Exception:
            body2 = f"\nResponse: `{r2.text[:150]}`"

        # Test 3: Browse API
        r3 = await run_sync(scraper.get, f"{BASE_URL}/api/v8/browse", {"params": {"tags": "all", "page": 1}, "timeout": 20})
        s3 = "✅" if r3.status_code == 200 else f"❌ {r3.status_code}"

        # Test 4: yt-dlp
        s4 = "✅ Installed" if YTDLP_AVAILABLE else "❌ Not installed"

        await msg.edit(
            f"🧪 **API Debug Report**\n\n"
            f"🌐 Main page: {s1}\n"
            f"🔍 Search API: {s2}{body2}\n"
            f"📂 Browse API: {s3}\n"
            f"📦 yt-dlp: {s4}\n\n"
            f"🍪 Cookies: {len(scraper.cookies)} cookies",
            buttons=[Button.inline("🔙 Home", data="back_home")],
        )
    except Exception as e:
        await msg.edit(f"❌ Debug failed: `{e}`",
                       buttons=[Button.inline("🔙 Home", data="back_home")])


@bot.on(events.NewMessage(pattern="/fetchall"))
async def cmd_fetchall(event):
    if not is_admin(event.sender_id):
        return
    msg = await event.reply("📂 **Fetching catalog...**\n\n⏳ Page 1 loading...")
    results, next_page = await browse_hanime(page=1)
    if not results:
        await msg.edit(
            "❌ **Could not fetch catalog.**\n\n"
            "Possible reasons:\n"
            "• Cloudflare blocking\n"
            "• API changed\n\n"
            "👉 Use `/debug` to check API status.",
            buttons=[Button.inline("🔙 Home", data="back_home")],
        )
        return

    text = f"📂 **Hentai Catalog** — Page 1\n━━━━━━━━━━━━━━━━━━\n**{len(results)} results**\n\n"
    buttons = []
    row = []
    for i, item in enumerate(results[:16], 1):
        t = item["title"][:30] + "…" if len(item["title"]) > 30 else item["title"]
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
        await msg.edit(
            f"❌ **No results for:** `{query}`\n\n"
            "👉 Use `/debug` to check if API works.\n"
            "👉 Try different keywords.",
            buttons=[Button.inline("🔙 Home", data="back_home")],
        )
        return

    text = f"🔍 **Results:** `{query}`\n━━━━━━━━━━━━━━━━━━\n**{len(results)} found**\n\n"
    buttons = []
    row = []
    for i, item in enumerate(results, 1):
        t = item["title"][:30] + "…" if len(item["title"]) > 30 else item["title"]
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
        f"🎬 **{title}**\n🔗 `{slug}`\n\nSelect quality:",
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
        await event.reply("🛑 **Cancelled.**")
    else:
        await event.reply("ℹ️ No active download.")


# ── Plain text = auto search ──
@bot.on(events.NewMessage)
async def text_search(event):
    if not is_admin(event.sender_id):
        return
    text = (event.text or "").strip()
    if not text or text.startswith("/") or len(text) < 2:
        return
    msg = await event.reply(f"🔍 **Searching:** `{text}`\n\n⏳ Please wait...")
    results = await search_hanime(text)
    if not results:
        await msg.edit(
            f"❌ **No results for:** `{text}`\n\nTry different keywords.",
            buttons=[Button.inline("🔙 Home", data="back_home")],
        )
        return
    resp = f"🔍 **Results:** `{text}`\n━━━━━━━━━━━━━━━━━━\n**{len(results)} found**\n\n"
    buttons = []
    row = []
    for i, item in enumerate(results, 1):
        t = item["title"][:30] + "…" if len(item["title"]) > 30 else item["title"]
        row.append(Button.inline(f"{i}. {t}", data=f"sel_{item['slug']}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([Button.inline("🔙 Home", data="back_home")])
    await msg.edit(resp, buttons=buttons)


# ═══════════════════════════════════════════════════
#  CALLBACK HANDLERS
# ═══════════════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"back_home"))
async def cb_home(event):
    if not is_admin(event.sender_id):
        await event.answer("⚠️ Unauthorized", alert=True)
        return
    await event.edit(
        "👋 **Hanime Bot**\n\n📌 Type a name or use buttons:",
        buttons=[
            [Button.inline("📖 Help", data="help")],
            [Button.inline("🔍 Search", data="search_prompt"),
             Button.inline("📂 Browse All", data="browse_1")],
            [Button.inline("🧪 Debug API", data="debug")],
        ],
    )


@bot.on(events.CallbackQuery(data=b"help"))
async def cb_help(event):
    if not is_admin(event.sender_id):
        return
    await event.edit(
        "📘 **Help**\n\n"
        "`/search <query>` — Search\n"
        "`/fetchall` — Browse catalog\n"
        "`/dl <slug>` — Direct download\n"
        "`/cancel` — Cancel\n"
        "`/debug` — Test API\n\n"
        "💡 Just type a name directly!",
        buttons=[Button.inline("🔙 Home", data="back_home")],
    )


@bot.on(events.CallbackQuery(data=b"search_prompt"))
async def cb_search_prompt(event):
    if not is_admin(event.sender_id):
        return
    await event.edit(
        "🔍 **Type a hentai name in chat**\n\nJust send it as a message!",
        buttons=[Button.inline("🔙 Home", data="back_home")],
    )


@bot.on(events.CallbackQuery(data=b"debug"))
async def cb_debug(event):
    if not is_admin(event.sender_id):
        return
    await event.answer("Opening debug...")
    uid = event.sender_id
    async with bot.conversation(await event.get_chat(), timeout=5) as conv:
        await conv.send_message("/debug")


@bot.on(events.CallbackQuery(data=b"cancel"))
async def cb_cancel(event):
    if not is_admin(event.sender_id):
        return
    uid = event.sender_id
    if uid in active_tasks and not active_tasks[uid].done():
        active_tasks[uid].cancel()
        del active_tasks[uid]
        await event.answer("🛑 Cancelled!", alert=True)
        await event.edit("🛑 **Cancelled.**",
                         buttons=[Button.inline("🔙 Home", data="back_home")])
    else:
        await event.answer("Nothing to cancel")
        await event.edit("ℹ️ No active download.",
                         buttons=[Button.inline("🔙 Home", data="back_home")])


@bot.on(events.CallbackQuery(data=re.compile(rb"browse_(\d+)")))
async def cb_browse(event):
    if not is_admin(event.sender_id):
        return
    page = int(event.pattern_match.group(1))
    await event.answer("⏳ Loading...")
    msg = await event.edit(f"📂 **Loading page {page}...**")
    results, next_page = await browse_hanime(page=page)
    if not results:
        await msg.edit("❌ Could not load this page.",
                       buttons=[Button.inline("🔙 Home", data="back_home")])
        return
    text = f"📂 **Catalog** — Page {page}\n━━━━━━━━━━━━━━━━━━\n**{len(results)} results**\n\n"
    buttons = []
    row = []
    for i, item in enumerate(results[:16], 1):
        t = item["title"][:30] + "…" if len(item["title"]) > 30 else item["title"]
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
        f"🎬 **{title}**\n🔗 `{slug}`\n\n━━━ Select Quality ━━━",
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
    task = asyncio.create_task(process_download(slug, quality, event))
    active_tasks[event.sender_id] = task
    try:
        await task
    except asyncio.CancelledError:
        log.info(f"Cancelled: {slug}")
    except Exception as e:
        log.error(f"Task error: {e}")
    finally:
        active_tasks.pop(event.sender_id, None)


# ═══════════════════════════════════════════════════
#  START
# ═══════════════════════════════════════════════════

async def main():
    # Clean temp
    for f in TEMP_DIR.iterdir():
        try:
            f.unlink(missing_ok=True)
        except Exception:
            pass

    # Init session (get cookies)
    init_session()

    log.info("=" * 50)
    log.info(f"  Hanime Bot v2 (API-First)")
    log.info(f"  Admin: {ADMIN_ID}")
    log.info(f"  Data Group: {DATA_GROUP_ID or 'Not set'}")
    log.info(f"  yt-dlp: {'✅' if YTDLP_AVAILABLE else '❌'}")
    log.info("=" * 50)

    await bot.start(bot_token=BOT_TOKEN)
    me = await bot.get_me()
    log.info(f"Bot @{me.username} running!")
    await bot.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
