import asyncio
import httpx
import re
import os
import json
import time
import html
import math
import gc
import logging
import shutil
from urllib.parse import urlencode, urljoin
from selectolax.parser import HTMLParser
from datetime import datetime
from pathlib import Path
from io import BytesIO
from pyrogram import Client, filters
from pyrogram.types import Update, Message
from flask import Flask
import threading

# Health check app
app = Flask(__name__)

@app.route('/health')
def health():
    return 'OK'

def run_flask():
    app.run(host='0.0.0.0', port=int(os.getenv("PORT", 8080)))

threading.Thread(target=run_flask, daemon=True).start()

# ───────────────────────────────
# ⚙️ CONFIG
# ───────────────────────────────
BASE_URL = "https://desifakes.com"
INITIAL_SEARCH_ID = "46509052"
ORDER = "date"
NEWER_THAN = "2019"
OLDER_THAN = "2025"
TIMEOUT = 10.0
DELAY_BETWEEN_REQUESTS = 0.2
TEMP_MEDIA_FILE = "Scraping/tempMedia.json"
MAX_CONCURRENT_WORKERS = 5  # Reduced for memory
MAX_RETRIES = 3
RETRY_DELAY = 2

VALID_EXTS = ["jpg", "jpeg", "png", "gif", "webp", "mp4", "mov", "avi", "mkv", "webm"]
EXCLUDE_PATTERNS = ["/data/avatars/", "/data/assets/", "/data/addonflare/"]

# HTML Gallery Config
OUTPUT_FILE = "Scraping/final_full_gallery.html"
MAX_FILE_SIZE_MB = 100
MAX_PAGINATION_RANGE = 100

# Upload Config
UPLOAD_FILE = "Scraping/final_full_gallery.html"
MAX_MB = 100
HOSTS = [
    {"name":"HTML Hosting","url":"https://html-hosting.tirev71676.workers.dev/api/upload","field":"file"},
    {"name":"Litterbox","url":"https://litterbox.catbox.moe/resources/internals/api.php","field":"fileToUpload","data":{"reqtype":"fileupload","time":"72h"}},
    {"name":"Catbox","url":"https://catbox.moe/user/api.php","field":"fileToUpload","data":{"reqtype":"fileupload"}}
]

API_ID = int(os.getenv("API_ID", 24536446))
API_HASH = os.getenv("API_HASH", "baee9dd189e1fd1daf0fb7239f7ae704")
BOT_TOKEN = os.getenv("BOT_TOKEN", "8097154751:AAGdE2IBcRElV1w_zHVwGu3N_utMkOyMpn0")

bot = Client("bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# ───────────────────────────────
# 🧩 LOGGING SETUP
# ───────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def log_memory():
    try:
        import psutil
        mem = psutil.Process().memory_info().rss / 1024 / 1024
        logger.info(f"Memory usage: {mem:.2f} MB")
    except ImportError:
        logger.info("psutil not available for memory tracking")

# ───────────────────────────────
# 🧩 UTILITIES
# ───────────────────────────────
def extract_search_id(url: str):
    match = re.search(r"/search/(\d+)/", url)
    return match.group(1) if match else None

def build_search_url(search_id, query, newer_than, older_than, page=None, older_than_ts=None, title_only=0):
    base_url = f"{BASE_URL}/search/{search_id}/"
    params = {"q": query, "o": ORDER}
    if older_than_ts:
        params["c[older_than]"] = older_than_ts
    else:
        params["c[newer_than]"] = f"{newer_than}-01-01"
        params["c[older_than]"] = f"{older_than}-12-31"
    if title_only == 1:
        params["c[title_only]"] = 1
    if page:
        params["page"] = page
    return f"{base_url}?{urlencode(params)}"

def find_view_older_link(html_str: str, title_only: int = 0):
    tree = HTMLParser(html_str)
    link_node = tree.css_first("div.block-footer a")
    if not link_node or not link_node.attributes.get("href"):
        return None
    href = link_node.attributes["href"]
    match = re.search(r"/search/(\d+)/older.*?before=(\d+).*?[&?]q=([^&]+)", href)
    if not match:
        return None
    sid, before, q = match.groups()
    if title_only == 1:
        return f"{BASE_URL}/search/{sid}/?q={q}&c[older_than]={before}&o=date&c[title_only]=1"
    return f"{BASE_URL}/search/{sid}/?q={q}&c[older_than]={before}&o=date"

def get_total_pages(html_str: str):
    tree = HTMLParser(html_str)
    nav = tree.css_first("ul.pageNav-main")
    if not nav:
        return 1
    pages = [int(a.text(strip=True)) for a in nav.css("li.pageNav-page a") if a.text(strip=True).isdigit()]
    return max(pages) if pages else 1

def extract_threads(html_str: str):
    tree = HTMLParser(html_str)
    threads = []
    for a in tree.css("a[href]"):
        href = a.attributes.get("href", "")
        if "threads/" in href and not href.startswith("#") and "page-" not in href:
            full_link = urljoin(BASE_URL, href)
            if full_link not in threads:
                threads.append(full_link)
    return threads

# ───────────────────────────────
# 🌐 FETCH
# ───────────────────────────────
async def fetch_page(client, url: str):
    try:
        r = await client.get(url, follow_redirects=True, timeout=TIMEOUT)
        return {"ok": r.status_code == 200, "html": r.text, "final_url": str(r.url)}
    except Exception as e:
        logger.error(f"Fetch error for {url}: {e}")
        return {"ok": False, "html": "", "final_url": url}

# ───────────────────────────────
# 📦 ARTICLE PROCESSOR WITH RETRIES
# ───────────────────────────────
async def make_request(client: httpx.AsyncClient, url: str, retries=MAX_RETRIES) -> str:
    for attempt in range(1, retries + 1):
        try:
            resp = await client.get(url, follow_redirects=True, timeout=TIMEOUT)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Attempt {attempt} failed for {url}: {e}")
            if attempt < retries:
                await asyncio.sleep(RETRY_DELAY)
            else:
                logger.error(f"All retries failed for {url}")
                return ""

def article_matches_patterns(article, patterns):
    try:
        article_text = article.text(separator=" ").strip().lower()
    except Exception:
        article_text = (article.html or "").lower()
    
    for pat in patterns:
        if pat.search(article_text):
            return True
    
    for el in article.css("*"):
        try:
            el_text = el.text(separator=" ").strip().lower()
            for pat in patterns:
                if pat.search(el_text):
                    return True
        except Exception:
            pass
    return False

async def process_thread(client: httpx.AsyncClient, post_url, patterns, semaphore):
    async with semaphore:
        html_str = await make_request(client, post_url)
        if not html_str:
            return []
        
        tree = HTMLParser(html_str)
        articles = tree.css("article.message--post")
        matched = []
        
        for article in articles:
            post_id = article.attributes.get("data-content", "").replace("post-", "") or "unknown"
            if post_id == "unknown":
                continue
            
            is_match = article_matches_patterns(article, patterns)
            thread_match = re.search(r"/threads/([^/]+)\.(\d+)/?", post_url)
            
            if thread_match:
                slug = thread_match.group(1)
                tid = thread_match.group(2)
                post_url_full = f"{BASE_URL}/threads/{slug}.{tid}/post-{post_id}"
            else:
                post_url_full = post_url
            
            date_tag = article.css_first("time.u-dt")
            post_date = datetime.now().strftime("%Y-%m-%d")
            if date_tag and "datetime" in date_tag.attributes:
                try:
                    post_date = datetime.strptime(date_tag.attributes["datetime"], "%Y-%m-%dT%H:%M:%S%z").strftime("%Y-%m-%d")
                except:
                    pass
            
            matched.append({
                "url": post_url_full,
                "post_id": post_id,
                "matched": is_match,
                "post_date": post_date,
                "article_html": article.html
            })
        
        return [a for a in matched if a["matched"]] or matched

async def process_threads_concurrent(thread_urls, patterns):
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_WORKERS)
    async with httpx.AsyncClient() as client:
        tasks = [process_thread(client, url, patterns, semaphore) for url in thread_urls]
        results = await asyncio.gather(*tasks)
    return [item for sublist in results for item in sublist]

# ───────────────────────────────
# 🎬 MEDIA EXTRACTION
# ───────────────────────────────
def extract_media_from_html(raw_html: str):
    if not raw_html:
        return []
    
    html_content = html.unescape(raw_html)
    tree = HTMLParser(html_content)
    urls = set()
    
    for node in tree.css("*[src]"):
        src = node.attributes.get("src", "").strip()
        if src:
            if "/vh/dli?" in src:
                src = src.replace("/vh/dli?", "/vh/dl?")
            urls.add(src)
    
    for node in tree.css("*[data-src]"):
        ds = node.attributes.get("data-src", "").strip()
        if ds:
            urls.add(ds)
    
    for node in tree.css("*[data-video]"):
        dv = node.attributes.get("data-video", "").strip()
        if dv:
            urls.add(dv)
    
    for node in tree.css("video, video source"):
        src = node.attributes.get("src", "").strip()
        if src:
            urls.add(src)
    
    for node in tree.css("*[style]"):
        style = node.attributes.get("style") or ""
        for m in re.findall(r'url\((.*?)\)', style):
            m = m.strip('"\' ')
            if m:
                urls.add(m)
    
    for match in re.findall(r'https?://[^\s"\'<>]+', html_content):
        urls.add(match.strip())
    
    media_urls = []
    for u in urls:
        if u:
            low = u.lower()
            if ("encoded$" in low and ".mp4" in low) or any(f".{ext}" in low for ext in VALID_EXTS):
                full_url = urljoin(BASE_URL, u) if u.startswith("/") else u
                media_urls.append(full_url)
    
    return list(dict.fromkeys(media_urls))

def filter_media(media_list, seen_global):
    filtered = []
    seen_local = set()
    for url in media_list:
        if any(bad in url for bad in EXCLUDE_PATTERNS):
            continue
        if url not in seen_local and url not in seen_global:
            seen_local.add(url)
            seen_global.add(url)
            filtered.append(url)
    return filtered

# ───────────────────────────────
# 📄 HTML GENERATOR
# ───────────────────────────────
def create_html(media_by_date_per_username, usernames, start_year, end_year):
    usernames_str = ", ".join(usernames)
    title = f"{usernames_str} - Media Gallery"
    logger.info(f"Generating HTML for usernames: {usernames_str}")

    # Prepare mediaData as a Python dictionary for JSON serialization
    media_data = {}
    total_items = 0
    media_counts = {}
    # Add type counts for all media
    total_type_counts = {'images': 0, 'videos': 0, 'gifs': 0}
    
    for username in usernames:
        media_by_date = media_by_date_per_username[username]
        media_list = []
        count = 0
        user_type_counts = {'images': 0, 'videos': 0, 'gifs': 0}
        
        for media_type in ['images', 'videos', 'gifs']:
            for date in sorted(media_by_date[media_type].keys(), reverse=True):
                for item in media_by_date[media_type][date]:
                    if not item.startswith(('http://', 'https://')):
                        logger.warning(f"Skipping invalid URL for {username}: {item}")
                        continue
                    try:
                        # Ensure URL is properly escaped
                        safe_src = html.escape(item).replace('"', '\\"').replace('\n', '')
                        media_list.append({
                            'type': media_type,
                            'src': safe_src,
                            'date': date
                        })
                        count += 1
                        user_type_counts[media_type] += 1
                        total_type_counts[media_type] += 1
                    except Exception as e:
                        logger.error(f"Failed to process media item for {username}: {item}, error: {str(e)}")
                        continue

        media_list = sorted(media_list, key=lambda x: x['date'], reverse=True)
        safe_username = username.replace(' ', '_')
        media_data[safe_username] = media_list
        media_counts[username] = count
        total_items += count

    if total_items == 0:
        logger.warning(f"No media items found for {usernames_str}")
        return None

    # Check HTML size to prevent truncation
    estimated_size = sum(len(str(item)) for user_items in media_data.values() for item in user_items) / (1024 * 1024)
    if estimated_size > MAX_FILE_SIZE_MB:
        logger.warning(f"Estimated HTML size {estimated_size:.2f} MB exceeds limit of {MAX_FILE_SIZE_MB} MB")
        return None

    # Serialize mediaData to JSON to ensure valid structure
    try:
        media_data_json = json.dumps(media_data, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Failed to serialize mediaData to JSON: {str(e)}")
        return None

    # Calculate default itemsPerPage
    default_items_per_page = max(1, math.ceil(total_items / MAX_PAGINATION_RANGE))

    html_fragments = [f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>{html.escape(title)}</title>
  <style>
    body {{ background-color: #000; font-family: Arial, sans-serif; margin: 0; padding: 20px; color: white; }}
    h1 {{ text-align: center; margin-bottom: 20px; font-size: 24px; }}
    .button-container {{ text-align: center; margin-bottom: 20px; display: flex; flex-wrap: wrap; justify-content: center; gap: 10px; }}
    .filter-button {{ padding: 14px 26px; margin: 6px; font-size: 18px; border-radius: 8px; border: none; background-color: #333; color: white; cursor: pointer; transition: background-color 0.3s; }}
    .filter-button:hover {{ background-color: #555; }}
    .filter-button.active {{ background-color: #007bff; }}
    .number-input {{ padding: 12px; font-size: 18px; width: 80px; border-radius: 8px; border: none; background-color: #333; color: white; }}
    .media-type-select {{ padding: 12px; font-size: 18px; border-radius: 8px; border: none; background-color: #333; color: white; }}
    .pagination {{ text-align: center; margin: 20px 0; }}
    .pagination-button {{ padding: 14px 24px; margin: 0 6px; font-size: 18px; border-radius: 8px; border: none; background-color: #333; color: white; cursor: pointer; transition: background-color 0.3s, transform 0.2s; }}
    .pagination-button:hover {{ background-color: #555; transform: scale(1.05); }}
    .pagination-button.active {{ background-color: #007bff; font-weight: bold; border: 2px solid #0056b3; }}
    .pagination-button:disabled {{ background-color: #555; cursor: not-allowed; opacity: 0.6; }}
    .masonry {{ display: flex; justify-content: center; gap: 10px; min-height: 100px; }}
    .column {{ flex: 1; display: flex; flex-direction: column; gap: 10px; }}
    .column img, .column video {{ width: 100%; border-radius: 5px; display: block; }}
    .column video {{ background-color: #111; }}
    .column video[title*="Video file"] {{ 
      position: relative; 
      cursor: pointer;
    }}
    .column video[title*="Video file"]::after {{ 
      content: "🎬"; 
      position: absolute; 
      top: 5px; 
      right: 5px; 
      background-color: rgba(0,0,0,0.7); 
      color: white; 
      padding: 2px 5px; 
      border-radius: 3px; 
      font-size: 12px;
    }}
    @media (max-width: 768px) {{ 
      .masonry {{ flex-direction: column; }} 
      .filter-button {{ padding: 8px 15px; font-size: 14px; }} 
      .number-input, .media-type-select {{ width: 100px; font-size: 14px; }} 
      .pagination-button {{ padding: 6px 10px; font-size: 12px; }}
    }}
  </style>
</head>
<body>
  <div class="button-container">
    <select id="mediaType" class="media-type-select">
      <option value="all" selected>All ({total_items})</option>
      <option value="images">Images ({total_type_counts['images']})</option>
      <option value="videos">Videos ({total_type_counts['videos']})</option>
      <option value="gifs">Gifs ({total_type_counts['gifs']})</option>
    </select>
    <div id="itemsPerUserContainer">
      <input type="number" id="itemsPerUser" class="number-input" min="1" value="2" placeholder="Items per user">
    </div>
    <input type="number" id="itemsPerPage" class="number-input" min="1" value="{default_items_per_page}" placeholder="Items per page">
    <button class="filter-button active" data-usernames="" data-original-text="All">All ({total_items})</button>
    {"".join(
    f'<button class="filter-button" data-usernames="{html.escape(username.replace(" ", "_"))}" '
    f'data-original-text="{html.escape(username)} ({media_counts[username]})">'
    f'{html.escape(username)} ({media_counts[username]})</button>'
    for username in usernames)}
  </div>
  <div class="pagination" id="pagination"></div>
  <div class="masonry" id="masonry"></div>
  <script>
    const mediaData = {media_data_json};
    const usernames = {json.dumps([username.replace(' ', '_') for username in usernames])};
    const masonry = document.getElementById("masonry");
    const pagination = document.getElementById("pagination");
    const buttons = document.querySelectorAll('.filter-button');
    const mediaTypeSelect = document.getElementById('mediaType');
    const itemsPerUserInput = document.getElementById('itemsPerUser');
    const itemsPerPageInput = document.getElementById('itemsPerPage');
    let selectedUsername = '';
    window.currentPage = 1;

    function updateButtonLabels() {{
      buttons.forEach(button => {{
        const originalText = button.getAttribute('data-original-text');
        button.textContent = originalText;
      }});
    }}

    function generateVideoThumbnail(videoElement) {{
      videoElement.addEventListener('loadedmetadata', function handleLoadedMetadata() {{
        try {{
          videoElement.currentTime = 0.1;
        }} catch (e) {{
          console.error('Error setting currentTime:', e);
        }}
        videoElement.removeEventListener('loadedmetadata', handleLoadedMetadata);
      }}, {{ once: true }});

      videoElement.addEventListener('seeked', function handleSeeked() {{
        try {{
          const playPromise = videoElement.play();
          if (playPromise !== undefined) {{
            playPromise.then(() => {{
              setTimeout(() => {{
                videoElement.pause();
              }}, 100);
            }}).catch(e => {{
              console.error('Play error:', e);
              videoElement.pause();
            }});
          }} else {{
            videoElement.play();
            setTimeout(() => {{
              videoElement.pause();
            }}, 100);
          }}
        }} catch (e) {{
          console.error('Error in play/pause:', e);
        }}
        videoElement.removeEventListener('seeked', handleSeeked);
      }}, {{ once: true }});
    }}

    function getOrderedMedia(mediaType, itemsPerUser, itemsPerPage, page) {{
      try {{
        let allMedia = [];
        if (selectedUsername === '') {{
          let maxRounds = 0;
          const mediaByUser = {{}};
          usernames.forEach(username => {{
            let userMedia = mediaData[username] || [];
            if (mediaType !== 'all') {{
              userMedia = userMedia.filter(item => item.type === mediaType);
            }}
            userMedia = userMedia.sort((a, b) => new Date(b.date) - new Date(a.date));
            mediaByUser[username] = userMedia;
            maxRounds = Math.max(maxRounds, Math.ceil(userMedia.length / itemsPerUser));
          }});
          for (let round = 0; round < maxRounds; round++) {{
            usernames.forEach(username => {{
              const start = round * itemsPerUser;
              const end = start + itemsPerUser;
              allMedia = allMedia.concat(mediaByUser[username].slice(start, end));
            }});
          }}
          allMedia = allMedia.filter(item => item);
        }} else {{
          let userMedia = mediaData[selectedUsername] || [];
          if (mediaType !== 'all') {{
            userMedia = userMedia.filter(item => item.type === mediaType);
          }}
          allMedia = userMedia.sort((a, b) => new Date(b.date) - new Date(a.date));
        }}
        const start = (page - 1) * itemsPerPage;
        const end = start + itemsPerPage;
        console.log('getOrderedMedia:', {{ mediaType, itemsPerUser, itemsPerPage, page, start, end, total: allMedia.length }});
        return {{ media: allMedia.slice(start, end), total: allMedia.length }};
      }} catch (e) {{
        console.error('Error in getOrderedMedia:', e);
        return {{ media: [], total: 0 }};
      }}
    }}

    function updatePagination(totalItems, itemsPerPage, currentPage) {{
      try {{
        pagination.innerHTML = '';
        const totalPages = Math.ceil(totalItems / itemsPerPage);
        if (totalPages <= 1) {{
          console.log('updatePagination: Only one page, no pagination needed');
          return;
        }}

        console.log('updatePagination:', {{ totalItems, itemsPerPage, currentPage: window.currentPage, totalPages }});

        const maxButtons = 5;
        let startPage = Math.max(1, window.currentPage - Math.floor(maxButtons / 2));
        let endPage = Math.min(totalPages, startPage + maxButtons - 1);
        if (endPage - startPage + 1 < maxButtons) {{
          startPage = Math.max(1, endPage - maxButtons + 1);
        }}

        const prevButton = document.createElement('button');
        prevButton.className = 'pagination-button';
        prevButton.textContent = 'Previous';
        prevButton.disabled = window.currentPage === 1;
        prevButton.addEventListener('click', () => {{
          if (window.currentPage > 1) {{
            window.currentPage--;
            console.log('Previous button clicked, new currentPage:', window.currentPage);
            renderMedia();
          }}
        }});
        pagination.appendChild(prevButton);

        for (let i = startPage; i <= endPage; i++) {{
          const pageButton = document.createElement('button');
          pageButton.className = 'pagination-button' + (i === window.currentPage ? ' active' : '');
          pageButton.textContent = i;
          pageButton.addEventListener('click', (function(pageNumber) {{
            return function() {{
              window.currentPage = pageNumber;
              console.log('Page button clicked, new currentPage:', window.currentPage);
              renderMedia();
            }};
          }})(i));
          pagination.appendChild(pageButton);
        }}

        const nextButton = document.createElement('button');
        nextButton.className = 'pagination-button';
        nextButton.textContent = 'Next';
        nextButton.disabled = window.currentPage === totalPages;
        nextButton.addEventListener('click', () => {{
          if (window.currentPage < totalPages) {{
            window.currentPage++;
            console.log('Next button clicked, new currentPage:', window.currentPage);
            renderMedia();
          }}
        }});
        pagination.appendChild(nextButton);
      }} catch (e) {{
        console.error('Error in updatePagination:', e);
      }}
    }}

    function renderMedia() {{
      try {{
        masonry.innerHTML = '';
        const mediaType = mediaTypeSelect.value;
        const itemsPerUser = parseInt(itemsPerUserInput.value) || 2;
        const itemsPerPage = parseInt(itemsPerPageInput.value) || {default_items_per_page};
        const result = getOrderedMedia(mediaType, itemsPerUser, itemsPerPage, window.currentPage);
        const allMedia = result.media;
        const totalItems = result.total;
        console.log('renderMedia:', {{ mediaType, itemsPerUser, itemsPerPage, currentPage: window.currentPage, mediaCount: allMedia.length, totalItems }});
        updatePagination(totalItems, itemsPerPage, window.currentPage);

        const columnsCount = 3;
        const columns = [];
        for (let i = 0; i < columnsCount; i++) {{
          const col = document.createElement("div");
          col.className = "column";
          masonry.appendChild(col);
          columns.push(col);
        }}

        const totalRows = Math.ceil(allMedia.length / columnsCount);
        for (let row = 0; row < totalRows; row++) {{
          for (let col = 0; col < columnsCount; col++) {{
            const actualCol = row % 2 === 0 ? col : columnsCount - 1 - col;
            const index = row * columnsCount + col;
            if (index < allMedia.length) {{
              let element;
              if (allMedia[index].type === "videos") {{
                element = document.createElement("video");
                element.src = allMedia[index].src;
                element.controls = true;
                element.alt = "Video";
                element.loading = "lazy";
                element.preload = "metadata";
                element.playsInline = true;
                element.onerror = () => {{
                  console.error('Failed to load video:', allMedia[index].src);
                  element.remove();
                }};
                element.addEventListener('loadedmetadata', () => {{
                  generateVideoThumbnail(element);
                }}, {{ once: true }});
              }} else {{
                element = document.createElement("img");
                element.src = allMedia[index].src;
                element.alt = allMedia[index].type.charAt(0).toUpperCase() + allMedia[index].type.slice(1);
                element.loading = "lazy";
                element.onerror = () => {{
                  console.error('Failed to load image:', allMedia[index].src);
                  element.remove();
                }};
              }}
              columns[actualCol].appendChild(element);
            }}
          }}
        }}
        window.scrollTo({{ top: 0, behavior: "smooth" }});
      }} catch (e) {{
        console.error('Error in renderMedia:', e);
        masonry.innerHTML = '<p style="color: red;">Error loading media. Please check console for details.</p>';
      }}
    }}

    buttons.forEach(button => {{
      button.addEventListener('click', () => {{
        try {{
          const username = button.getAttribute('data-usernames');
          if (button.classList.contains('active')) {{
            return;
          }}
          buttons.forEach(btn => btn.classList.remove('active'));
          button.classList.add('active');
          selectedUsername = username;
          window.currentPage = 1;
          console.log('Filter button clicked, selectedUsername:', username, 'currentPage:', window.currentPage);
          updateButtonLabels();
          renderMedia();
        }} catch (e) {{
          console.error('Error in button click handler:', e);
        }}
      }});
    }});

    mediaTypeSelect.addEventListener('change', () => {{
      try {{
        window.currentPage = 1;
        console.log('Media type changed, resetting currentPage to 1');
        renderMedia();
      }} catch (e) {{
        console.error('Error in mediaTypeSelect change handler:', e);
      }}
    }});

    itemsPerUserInput.addEventListener('input', () => {{
      try {{
        window.currentPage = 1;
        console.log('Items per user changed, resetting currentPage to 1');
        renderMedia();
      }} catch (e) {{
        console.error('Error in itemsPerUserInput input handler:', e);
      }}
    }});

    itemsPerPageInput.addEventListener('input', () => {{
      try {{
        window.currentPage = 1;
        console.log('Items per page changed, resetting currentPage to 1');
        renderMedia();
      }} catch (e) {{
        console.error('Error in itemsPerPageInput input handler:', e);
      }}
    }});

    document.addEventListener('play', function(e) {{
      const videos = document.querySelectorAll("video");
      videos.forEach(video => {{
        if (video !== e.target) {{
          video.pause();
        }}
      }});
    }}, true);

    try {{
      updateButtonLabels();
      renderMedia();
    }} catch (e) {{
      console.error('Initial render failed:', e);
      masonry.innerHTML = '<p style="color: red;">Error loading media. Please check console for details.</p>';
    }}
  </script>
</body>
</html>"""]
    

    html_content = "".join(html_fragments)
    logger.info(f"Generated HTML with {total_items} items, size: {len(html_content) / (1024 * 1024):.2f} MB")
    
    # Clean up temporary data to free memory
    try:
        html_fragments.clear()
        del html_fragments
        media_data.clear()
        del media_data
        media_counts.clear()
        del media_counts
        gc.collect()  # Force garbage collection after HTML generation
        logger.info("Memory cleaned up after HTML generation")
    except Exception as cleanup_error:
        logger.error(f"Error during HTML generation cleanup: {str(cleanup_error)}")
        gc.collect()  # Still attempt garbage collection
    
    return html_content

# ───────────────────────────────
# � UPLOAD FUNCTIONS
# ───────────────────────────────
async def upload_file(client, host, data):
    buf = BytesIO(data)
    files = {host["field"]:(UPLOAD_FILE, buf, "text/html")}
    try:
        r = await client.post(host["url"], files=files, data=host.get("data", {}), timeout=30.0)
        if r.status_code in (200,201):
            if host["name"]=="HTML Hosting":
                j = r.json()
                if j.get("success") and j.get("url"):
                    return (host["name"], j["url"])
                else:
                    return (host["name"], f"Error: {j.get('error','Unknown')}")
            else:
                t = r.text.strip()
                if t.startswith("https://"):
                    if host["name"]=="Litterbox" and "files.catbox.moe" in t:
                        t = "https://litterbox.catbox.moe/"+t.split("/")[-1]
                    return (host["name"], t)
                return (host["name"], f"Invalid response: {t[:100]}")
        return (host["name"], f"HTTP {r.status_code}")
    except Exception as e:
        return (host["name"], f"Exception: {e}")

# ───────────────────────────────
# PROGRESS BAR
# ───────────────────────────────
def generate_bar(percentage):
    filled = int(percentage / 10)
    empty = 10 - filled
    return "●" * filled + "○" * (empty // 2) + "◌" * (empty - empty // 2)

# ───────────────────────────────
# PROCESS USER
# ───────────────────────────────
async def process_user(user, title_only, user_idx, total_users, progress_msg, last_edit):
    logger.info(f"Processing user: {user}")
    
    search_display = "+".join(user.split())
    tokens = [t for t in re.split(r"[,\s]+", user) if t]
    phrase = " ".join(tokens)
    PATTERNS = []
    if phrase:
        PATTERNS.append(re.compile(r"\b" + re.escape(phrase) + r"\b", re.IGNORECASE))
    for tok in tokens:
        PATTERNS.append(re.compile(r"\b" + re.escape(tok) + r"\b", re.IGNORECASE))
    
    os.makedirs("Scraping", exist_ok=True)
    temp_media_file = TEMP_MEDIA_FILE
    
    # Update progress: start
    progress = (user_idx / total_users) * 100
    bar = generate_bar(progress)
    msg = f"completed {user_idx}/{total_users}\n{bar} {progress:.2f}%\nprocess current username: {user}"
    now = time.time()
    if now - last_edit[0] > 3:
        await progress_msg.edit(msg)
        last_edit[0] = now
    
    start_url = build_search_url(INITIAL_SEARCH_ID, search_display, NEWER_THAN, OLDER_THAN, title_only=title_only)
    batch_num = 1
    current_url = start_url
    
    async with httpx.AsyncClient() as client:
        while current_url:
            resp = await fetch_page(client, current_url)
            if not resp["ok"]:
                logger.error(f"Failed batch start URL {current_url}")
                break
            
            search_id = extract_search_id(resp["final_url"]) or INITIAL_SEARCH_ID
            total_pages = get_total_pages(resp["html"])
            logger.info(f"Batch {batch_num}: {total_pages} pages, search_id {search_id}")
            
            for page_num in range(1, total_pages + 1):
                match = re.search(r"c\[older_than]=(\d+)", current_url)
                older_than_ts = match.group(1) if match else None
                page_url = build_search_url(search_id, search_display, NEWER_THAN, OLDER_THAN, page_num, 
                                           None if batch_num == 1 else older_than_ts, title_only)
                result = await fetch_page(client, page_url)
                if not result["ok"]:
                    logger.error(f"Failed page {page_num}")
                    continue
                
                threads = extract_threads(result["html"])
                if not threads:
                    continue
                
                # Process articles
                articles = await process_threads_concurrent(threads, PATTERNS)
                
                # Extract media
                media_entries = []
                for article in articles:
                    html_data = article.get("article_html", "")
                    media_urls = extract_media_from_html(html_data)
                    media_urls = filter_media(media_urls, set())  # local dedup per page
                    if media_urls:
                        media_entries.append({
                            "username": user,
                            "post_date": article.get("post_date", ""),
                            "media": media_urls
                        })
                
                # Append to tempMedia.json
                if media_entries:
                    with open(temp_media_file, "a", encoding="utf-8") as f:
                        for entry in media_entries:
                            json.dump(entry, f, ensure_ascii=False)
                            f.write("\n")
                
                # Clear memory
                del threads, articles, media_entries
                gc.collect()
                log_memory()
                await asyncio.sleep(DELAY_BETWEEN_REQUESTS)
            
            # Next batch
            next_url = find_view_older_link(result["html"], title_only)
            if not next_url:
                break
            current_url = next_url
            batch_num += 1
    
    # Update progress
    progress += 100 / total_users
    bar = generate_bar(progress)
    msg = f"completed {user_idx+1}/{total_users}\n{bar} {progress:.2f}%\nprocess current username: {user} - completed"
    now = time.time()
    if now - last_edit[0] > 3:
        await progress_msg.edit(msg)
        last_edit[0] = now

# ───────────────────────────────
# BOT HANDLER
# ───────────────────────────────
@bot.on_message(filters.text & filters.private)
async def handle_message(client: Client, message: Message):
    text = message.text.strip()
    match = re.match(r"(.+?)\s+(\d)$", text)
    if not match:
        await message.reply("Invalid format. Use: usernames separated by comma, then 0 or 1 for title_only")
        return
    
    usernames_part = match.group(1)
    title_only = int(match.group(2))
    usernames = [u.strip() for u in usernames_part.split(',') if u.strip()]
    if not usernames:
        await message.reply("No usernames provided")
        return
    
    total_users = len(usernames)
    last_edit = [0]
    
    # Send initial progress message
    progress_msg = await message.reply("Starting processing...")
    
    for user_idx, user in enumerate(usernames):
        await process_user(user, title_only, user_idx, total_users, progress_msg, last_edit)
    
    # Final progress
    progress = 100
    bar = generate_bar(progress)
    msg = f"completed {total_users}/{total_users}\n{bar} {progress:.2f}%\nDeduplicating and generating final gallery..."
    await progress_msg.edit(msg)
    
    # Deduplicate
    logger.info("Deduplicating media")
    log_memory()
    seen_urls = set()
    deduped = []
    
    if os.path.exists(TEMP_MEDIA_FILE):
        with open(TEMP_MEDIA_FILE, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                entry = json.loads(line.strip())
                new_media = []
                for url in entry.get("media", []):
                    if url not in seen_urls:
                        seen_urls.add(url)
                        new_media.append(url)
                if new_media:
                    entry["media"] = new_media
                    deduped.append(entry)
    
    logger.info(f"Deduped to {len(deduped)} entries, {len(seen_urls)} unique URLs")
    log_memory()
    
    # Build media_by_date_per_username
    media_by_date_per_username = {}
    for entry in deduped:
        user = entry["username"]
        if user not in media_by_date_per_username:
            media_by_date_per_username[user] = {"images": {}, "videos": {}, "gifs": {}}
        date = entry.get("post_date", "")
        if not date:
            continue
        for url in entry.get("media", []):
            if 'vh/dl?url' in url:
                typ = 'videos'
            elif 'vh/dli?' in url:
                typ = 'images'
            else:
                if '.mp4' in url.lower():
                    typ = 'videos'
                elif '.gif' in url.lower():
                    typ = 'gifs'
                else:
                    typ = 'images'
            if date not in media_by_date_per_username[user][typ]:
                media_by_date_per_username[user][typ][date] = []
            media_by_date_per_username[user][typ][date].append(url)
    
    # Generate HTML
    html_content = create_html(media_by_date_per_username, usernames, 2019, 2025)
    
    total_items = sum(len(media_list) for user_media in media_by_date_per_username.values() for media_type in user_media.values() for media_list in media_type.values())
    
    if html_content:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            f.write(html_content)
        
        # Upload
        with open(OUTPUT_FILE, "rb") as f:
            data = f.read()
        async with httpx.AsyncClient() as client:
            tasks = [upload_file(client, h, data) for h in HOSTS]
            results = await asyncio.gather(*tasks)
        
        links = []
        for name, res in results:
            status = "✅" if res.startswith("https://") else "❌"
            links.append(f"{status} {name}: {res}")
        
        caption = f"Total Media in Html: {total_items}\n───────────────────────────────────\n📤 Uploading Final HTML to Hosting Services\n" + "\n".join(links)
        
        await message.reply_document(OUTPUT_FILE, caption=caption)
        
        # Delete progress message after sending the final gallery
        await progress_msg.delete()
        
        # Clean up
        try:
            if os.path.exists(TEMP_MEDIA_FILE):
                os.remove(TEMP_MEDIA_FILE)
            shutil.rmtree("Scraping")
        except Exception as e:
            logger.error(f"Cleanup error: {e}")
    else:
        await message.reply("Failed to generate HTML")

# ───────────────────────────────
# MAIN
# ───────────────────────────────
if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    bot.run()
