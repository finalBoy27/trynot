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
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TaskProgressColumn
from datetime import datetime
from pathlib import Path
from io import BytesIO
from pyrogram import Client, filters
from pyrogram.types import Message
from flask import request, Flask
import threading

# ───────────────────────────────
# FLASK SETUP
# ───────────────────────────────
app = Flask(__name__)
webhook_queue = asyncio.Queue()

@app.route('/health')
def health():
    return 'OK', 200

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        update_dict = request.get_json()
        if update_dict:
            # Just acknowledge receipt, don't process here
            return 'OK', 200
        return 'Invalid JSON', 400
    except Exception as e:
        print(f"Webhook error: {e}")
        return 'Error', 500

def run_flask():
    app.run(host='0.0.0.0', port=int(os.getenv("PORT", 8080)), debug=False, use_reloader=False)

# ───────────────────────────────
# ⚙️ CONFIG
# ───────────────────────────────
BASE_URL = "https://desifakes.com"
INITIAL_SEARCH_ID = "46509052"
ORDER = "date"
NEWER_THAN = "2019"
OLDER_THAN = "2025"
TIMEOUT = 13.0
DELAY_BETWEEN_REQUESTS = 0.3
THREADS_DIR = "Scraping/Threads"
ARTICLES_DIR = "Scraping/Articles"
MEDIA_DIR = "Scraping/Media"
MAX_CONCURRENT_WORKERS = 12
MAX_RETRIES = 4
RETRY_DELAY = 1.5

VALID_EXTS = ["jpg", "jpeg", "png", "gif", "webp", "mp4", "mov", "avi", "mkv", "webm"]
EXCLUDE_PATTERNS = ["/data/avatars/", "/data/assets/", "/data/addonflare/"]

OUTPUT_FILE = "Scraping/final_full_gallery.html"
HTML_DIR = "Scraping/Html"
MAX_FILE_SIZE_MB = 100
MAX_PAGINATION_RANGE = 100

UPLOAD_FILE = "Scraping/final_full_gallery.html"
MAX_MB = 100
HOSTS = [
    {"name":"HTML Hosting","url":"https://html-hosting.tirev71676.workers.dev/api/upload","field":"file"},
    {"name":"Litterbox","url":"https://litterbox.catbox.moe/resources/internals/api.php","field":"fileToUpload","data":{"reqtype":"fileupload","time":"72h"}},
    {"name":"Catbox","url":"https://catbox.moe/user/api.php","field":"fileToUpload","data":{"reqtype":"fileupload"}}
]

API_ID = int(os.getenv("API_ID", "24536446"))
API_HASH = os.getenv("API_HASH", "baee9dd189e1fd1daf0fb7239f7ae704")
BOT_TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN environment variable not set")

# Initialize Pyrogram client
bot = Client("bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

console = Console()

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
    except Exception:
        return {"ok": False, "html": "", "final_url": url}

# ───────────────────────────────
# 📦 THREAD COLLECTOR
# ───────────────────────────────
async def process_batch(client, batch_num, start_url, query, title_only):
    console.rule(f"[bold green]📦 Batch #{batch_num}: Collecting Threads[/bold green]")
    resp = await fetch_page(client, start_url)
    if not resp["ok"]:
        console.print(f"[red]❌ Failed batch start URL[/red]")
        return None, None

    search_id = extract_search_id(resp["final_url"]) or INITIAL_SEARCH_ID
    total_pages = get_total_pages(resp["html"])
    console.print(f"[bold cyan]✓[/bold cyan] Found {total_pages} pages | Search ID: {search_id}\n")

    batch_data = {}
    table = Table(title=f"Batch #{batch_num}")
    table.add_column("Page", justify="right", style="cyan")
    table.add_column("Threads", justify="center", style="yellow")

    for page_num in range(1, total_pages + 1):
        match = re.search(r"c\[older_than]=(\d+)", start_url)
        older_than_ts = match.group(1) if match else None
        page_url = build_search_url(search_id, query, NEWER_THAN, OLDER_THAN, page_num, 
                                   None if batch_num == 1 else older_than_ts, title_only)
        result = await fetch_page(client, page_url)
        threads = extract_threads(result["html"]) if result["ok"] else []
        batch_data[f"page_{page_num}"] = threads
        table.add_row(str(page_num), str(len(threads)))
        await asyncio.sleep(DELAY_BETWEEN_REQUESTS)

    console.print(table)

    next_batch_url = find_view_older_link(result["html"], title_only)
    if next_batch_url:
        console.print(f"\n[green]→ Older results found![/green]\n")
    return batch_data, next_batch_url

async def collect_threads(query, title_only, threads_dir):
    os.makedirs(threads_dir, exist_ok=True)
    start_url = build_search_url(INITIAL_SEARCH_ID, query, NEWER_THAN, OLDER_THAN, title_only=title_only)
    batch_num = 1
    current_url = start_url
    
    async with httpx.AsyncClient() as client:
        while current_url:
            batch_data, next_url = await process_batch(client, batch_num, current_url, query, title_only)
            if not batch_data:
                break
            
            file_name = os.path.join(threads_dir, f"batch_{batch_num:02d}_desifakes_threads.json")
            with open(file_name, "w", encoding="utf-8") as f:
                json.dump(batch_data, f, indent=2, ensure_ascii=False)
            
            total = sum(len(v) for v in batch_data.values())
            console.print(f"[green]✓ Batch #{batch_num}:[/green] {total} threads → {file_name}\n")
            
            if not next_url:
                console.print("[yellow]✓ All threads collected[/yellow]\n")
                break
            current_url = next_url
            batch_num += 1

# ───────────────────────────────
# 📺 ARTICLE COLLECTOR
# ───────────────────────────────
async def make_request(client: httpx.AsyncClient, url: str, retries=MAX_RETRIES) -> str:
    for attempt in range(1, retries + 1):
        try:
            resp = await client.get(url, follow_redirects=True, timeout=TIMEOUT)
            resp.raise_for_status()
            return resp.text
        except Exception:
            if attempt < retries:
                await asyncio.sleep(RETRY_DELAY)
            else:
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

async def process_articles_batch(batch_num, articles_file, media_dir):
    console.rule(f"[bold cyan]🎬 Batch #{batch_num}: Extracting Media[/bold cyan]")
    
    try:
        with open(articles_file, "r", encoding="utf-8") as f:
            articles = json.load(f)
    except Exception as e:
        console.print(f"[red]❌ Error reading {articles_file}: {e}[/red]")
        return
    
    articles.sort(key=lambda x: datetime.strptime(x.get("post_date", "1900-01-01"), "%Y-%m-%d"), reverse=True)
    
    all_results = []
    all_media = set()
    no_media_posts = []
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold cyan]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TextColumn("[bold green]{task.completed}/{task.total}"),
        console=console,
        transient=True
    ) as progress:
        task = progress.add_task("Processing posts...", total=len(articles))
        
        for entry in articles:
            html_data = entry.get("article_html", "")
            media_urls = extract_media_from_html(html_data)
            media_urls = filter_media(media_urls, all_media)
            
            post_id = entry.get("post_id") or "unknown"
            
            if not media_urls:
                no_media_posts.append(entry.get("url", "(unknown)"))
            
            all_results.append({
                "url": entry.get("url", ""),
                "post_id": post_id,
                "post_date": entry.get("post_date", ""),
                "media_count": len(media_urls),
                "media": media_urls
            })
            
            progress.update(task, advance=1)
    
    all_results.sort(key=lambda x: datetime.strptime(x.get("post_date", "1900-01-01"), "%Y-%m-%d"), reverse=True)
    
    os.makedirs(media_dir, exist_ok=True)
    media_output = os.path.join(media_dir, f"batch_{batch_num:02d}_desifakes_media.json")
    
    with open(media_output, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)
    
    console.print(f"[green]✓ Media extracted:[/green] {len(all_results)} posts → {media_output}")
    console.print(f"[yellow]⚠ No media:[/yellow] {len(no_media_posts)} posts\n")

# ───────────────────────────────
# 📄 HTML GENERATOR (TRUNCATED - use original)
# ───────────────────────────────
def create_html(media_by_date_per_username, usernames, start_year, end_year):
    # Use your existing create_html function here
    # (kept from original code)
    return "<html><!-- HTML content --></html>"

# ───────────────────────────────
# 📤 UPLOAD FUNCTIONS
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
# 🤖 BOT HANDLER - ONLY POLLING MODE
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
    
    await message.reply(f"Starting scraping for: {', '.join(usernames)}")
    # Your processing logic here

# ───────────────────────────────
# MAIN ENTRY POINT
# ───────────────────────────────
async def main():
    # Start Flask in a thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    console.print("[green]✓ Flask server started[/green]")
    
    # Start Pyrogram bot in polling mode
    async with bot:
        console.print("[green]✓ Bot started in polling mode[/green]")
        await bot.idle()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("[yellow]Bot stopped[/yellow]")
