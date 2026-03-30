#!/usr/bin/env python3
"""Fresh Web Crawler V4 - Actually crawls the internet ourselves.
Uses seed URLs from multiple sources, follows links BFS, extracts text with trafilatura.
Uploads large chunks (~1GB compressed) to OpenTransformer/web-crawl-2026."""
import os, sys, json, gzip, time, hashlib, random, re, traceback
from collections import deque
from urllib.parse import urlparse, urljoin
import requests
from bs4 import BeautifulSoup
import trafilatura
from huggingface_hub import HfApi, login

HF_TOKEN = "HF_TOKEN_REDACTED"
TARGET_REPO = "OpenTransformer/web-crawl-2026"
OUTPUT_DIR = "/workspace/scraped_data"
STATE_FILE = "/workspace/crawl_state_v4.json"
CHUNK_TARGET_BYTES = 1_200_000_000  # ~1.2GB uncompressed per chunk
TIMEOUT = 15
MIN_TEXT_LEN = 200
MAX_TEXT_LEN = 200_000
MAX_URLS_PER_DOMAIN = 500
CRAWL_DELAY = 0.3

os.makedirs(OUTPUT_DIR, exist_ok=True)
login(token=HF_TOKEN, add_to_git_credential=False)
api = HfApi(token=HF_TOKEN)

USER_AGENTS = [
    "Mozilla/5.0 (compatible; OpenTransformerBot/1.0; +https://huggingface.co/OpenTransformer)",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

SEED_DOMAINS = [
    "https://www.reuters.com", "https://apnews.com", "https://www.bbc.com/news",
    "https://www.theguardian.com", "https://www.npr.org", "https://arstechnica.com",
    "https://news.ycombinator.com", "https://lobste.rs", "https://dev.to",
    "https://stackoverflow.blog", "https://www.infoq.com",
    "https://www.scientificamerican.com", "https://www.nature.com/news",
    "https://phys.org", "https://www.quantamagazine.org",
    "https://ocw.mit.edu", "https://plato.stanford.edu",
    "https://en.wikipedia.org/wiki/Special:Random",
    "https://en.wikisource.org", "https://www.gutenberg.org",
    "https://www.usa.gov", "https://www.gov.uk",
    "https://data.gov", "https://www.loc.gov",
    "https://www.smithsonianmag.com", "https://nautil.us",
    "https://longreads.com", "https://theconversation.com",
    "https://fs.blog", "https://waitbutwhy.com",
    "https://www.lesswrong.com", "https://marginalrevolution.com",
    "https://www.wired.com", "https://www.theatlantic.com",
    "https://www.newyorker.com", "https://www.economist.com",
    "https://www.pbs.org", "https://www.vox.com",
    "https://www.propublica.org", "https://fivethirtyeight.com",
    "https://www.technologyreview.com", "https://spectrum.ieee.org",
    "https://www.livescience.com", "https://www.space.com",
    "https://www.sciencedaily.com", "https://www.psychologytoday.com",
    "https://news.mit.edu", "https://www.cam.ac.uk/news",
    "https://hai.stanford.edu/news", "https://www.ox.ac.uk/news",
    "https://www.brookings.edu", "https://www.rand.org",
    "https://www.cfr.org", "https://carnegieendowment.org",
]

BLOCKED_EXTENSIONS = {".pdf", ".jpg", ".jpeg", ".png", ".gif", ".svg", ".mp3", ".mp4",
                       ".avi", ".mov", ".zip", ".tar", ".gz", ".exe", ".dmg", ".iso",
                       ".css", ".js", ".woff", ".woff2", ".ttf", ".eot", ".ico", ".webp"}
BLOCKED_PATTERNS = re.compile(
    r"(login|signup|signin|register|cart|checkout|payment|admin|wp-admin|"
    r"facebook\.com|twitter\.com|instagram\.com|tiktok\.com|"
    r"youtube\.com/watch|amazon\.|ebay\.|\.pdf$|/tag/|/category/|"
    r"/page/\d+|/feed/|/rss|/atom|#comment|/reply|/share)", re.I)


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"chunk_num": 0, "total_docs": 0, "total_bytes_uploaded": 0,
            "seen_hashes": [], "domain_counts": {}}


def save_state(state):
    if len(state.get("seen_hashes", [])) > 500000:
        state["seen_hashes"] = state["seen_hashes"][-200000:]
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def is_valid_url(url):
    try:
        p = urlparse(url)
        if p.scheme not in ("http", "https"):
            return False
        if not p.netloc or "." not in p.netloc:
            return False
        ext = os.path.splitext(p.path)[1].lower()
        if ext in BLOCKED_EXTENSIONS:
            return False
        if BLOCKED_PATTERNS.search(url):
            return False
        return True
    except Exception:
        return False


def extract_links(html, base_url):
    links = []
    try:
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith("#") or href.startswith("javascript:"):
                continue
            full = urljoin(base_url, href)
            full = full.split("#")[0]
            if is_valid_url(full):
                links.append(full)
    except Exception:
        pass
    return links


def content_hash(text):
    return hashlib.md5(text[:500].encode("utf-8", errors="ignore")).hexdigest()


def fetch_page(url, session):
    try:
        resp = session.get(url, timeout=TIMEOUT, allow_redirects=True,
                          headers={"User-Agent": random.choice(USER_AGENTS),
                                   "Accept": "text/html,application/xhtml+xml",
                                   "Accept-Language": "en-US,en;q=0.9"})
        if resp.status_code != 200:
            return None, None
        ct = resp.headers.get("content-type", "")
        if "text/html" not in ct and "xhtml" not in ct:
            return None, None
        return resp.text, resp.url
    except Exception:
        return None, None


def extract_text(html, url):
    try:
        text = trafilatura.extract(html, include_comments=False, include_tables=True,
                                    no_fallback=False, favor_precision=False, url=url)
        if text and len(text) >= MIN_TEXT_LEN:
            return text[:MAX_TEXT_LEN]
    except Exception:
        pass
    return None


def get_random_wikipedia_urls(n=100):
    urls = []
    try:
        resp = requests.get("https://en.wikipedia.org/w/api.php",
                           params={"action": "query", "list": "random",
                                   "rnnamespace": 0, "rnlimit": min(n, 500), "format": "json"},
                           timeout=10)
        data = resp.json()
        for page in data.get("query", {}).get("random", []):
            title = page["title"].replace(" ", "_")
            urls.append("https://en.wikipedia.org/wiki/" + title)
    except Exception:
        pass
    return urls


def get_hn_urls(n=30):
    urls = []
    try:
        resp = requests.get("https://hacker-news.firebaseio.com/v0/topstories.json", timeout=10)
        ids = resp.json()[:n]
        for sid in ids:
            try:
                item = requests.get(
                    "https://hacker-news.firebaseio.com/v0/item/%d.json" % sid, timeout=5).json()
                if item and item.get("url"):
                    urls.append(item["url"])
            except Exception:
                continue
    except Exception:
        pass
    return urls


def get_lobsters_urls(n=25):
    urls = []
    try:
        resp = requests.get("https://lobste.rs/hottest.json", timeout=10)
        for item in resp.json()[:n]:
            if item.get("url"):
                urls.append(item["url"])
    except Exception:
        pass
    return urls


def upload_chunk(filepath, remote_name):
    fsize = os.path.getsize(filepath) / (1024 * 1024)
    print("  Uploading %s (%.1f MB)..." % (remote_name, fsize), flush=True)
    for attempt in range(5):
        try:
            api.upload_file(
                path_or_fileobj=filepath,
                path_in_repo="data/" + remote_name,
                repo_id=TARGET_REPO,
                repo_type="dataset",
            )
            print("  Uploaded %s (%.1f MB)" % (remote_name, fsize), flush=True)
            return True
        except Exception as e:
            print("  Upload attempt %d failed: %s" % (attempt + 1, e), flush=True)
            time.sleep(30 * (attempt + 1))
    return False


def main():
    print("=" * 60, flush=True)
    print("Fresh Web Crawler V4", flush=True)
    print("Target: %s" % TARGET_REPO, flush=True)
    print("Chunk target: ~%.1fGB uncompressed" % (CHUNK_TARGET_BYTES / 1e9), flush=True)
    print("=" * 60, flush=True)

    state = load_state()
    seen_hashes = set(state.get("seen_hashes", []))
    domain_counts = state.get("domain_counts", {})
    chunk_num = state.get("chunk_num", 0)
    total_docs = state.get("total_docs", 0)

    session = requests.Session()
    session.headers.update({"User-Agent": random.choice(USER_AGENTS)})

    while True:
        # Build URL queue from seeds + discovered links
        url_queue = deque()
        discovered = set()

        # Add seed URLs
        seeds = list(SEED_DOMAINS)
        random.shuffle(seeds)
        for seed in seeds:
            url_queue.append(seed)
            discovered.add(seed)

        # Add random Wikipedia articles
        for u in get_random_wikipedia_urls(200):
            if u not in discovered:
                url_queue.append(u)
                discovered.add(u)

        # Add HN + Lobsters links
        for u in get_hn_urls(30) + get_lobsters_urls(25):
            if u not in discovered:
                url_queue.append(u)
                discovered.add(u)

        chunk_name = "crawl_v4_chunk%04d.jsonl.gz" % chunk_num
        chunk_path = os.path.join(OUTPUT_DIR, chunk_name)
        f = gzip.open(chunk_path, "wt", encoding="utf-8")
        chunk_bytes = 0
        chunk_docs = 0
        chunk_start = time.time()

        print("\nStarting chunk %d..." % chunk_num, flush=True)

        pages_tried = 0
        last_status = time.time()

        while chunk_bytes < CHUNK_TARGET_BYTES and url_queue:
            url = url_queue.popleft()
            domain = urlparse(url).netloc

            # Rate limit per domain
            dc = domain_counts.get(domain, 0)
            if dc >= MAX_URLS_PER_DOMAIN:
                continue

            pages_tried += 1
            html, final_url = fetch_page(url, session)
            if not html:
                time.sleep(0.05)
                continue

            # Extract text
            text = extract_text(html, final_url or url)
            if not text:
                time.sleep(0.05)
                continue

            # Dedup
            h = content_hash(text)
            if h in seen_hashes:
                continue
            seen_hashes.add(h)

            # Write doc
            doc = json.dumps({
                "text": text,
                "url": final_url or url,
                "domain": domain,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "source": "crawl_v4",
            }, ensure_ascii=False)
            f.write(doc + "\n")
            doc_bytes = len(doc.encode("utf-8"))
            chunk_bytes += doc_bytes
            chunk_docs += 1
            total_docs += 1
            domain_counts[domain] = dc + 1

            # Extract and queue new links
            new_links = extract_links(html, final_url or url)
            random.shuffle(new_links)
            for link in new_links[:30]:
                if link not in discovered and len(url_queue) < 200000:
                    url_queue.append(link)
                    discovered.add(link)

            # Status every 60s
            now = time.time()
            if now - last_status > 60:
                rate = chunk_docs / max(1, now - chunk_start)
                elapsed_min = (now - chunk_start) / 60
                print("  Chunk %d: %d docs, %.0fMB, %d queued, %.1f docs/s, "
                      "tried %d pages, %.0f min elapsed" %
                      (chunk_num, chunk_docs, chunk_bytes / 1e6,
                       len(url_queue), rate, pages_tried, elapsed_min), flush=True)
                last_status = now

            time.sleep(CRAWL_DELAY)

        f.close()

        if chunk_docs == 0:
            print("  No docs in chunk, cleaning up", flush=True)
            try:
                os.remove(chunk_path)
            except OSError:
                pass
            print("  Waiting 5 min before retrying with fresh seeds...", flush=True)
            time.sleep(300)
            continue

        elapsed = time.time() - chunk_start
        compressed_size = os.path.getsize(chunk_path)
        print("  Chunk %d complete: %d docs, %.0fMB uncompressed, %.0fMB compressed, %.0f min" %
              (chunk_num, chunk_docs, chunk_bytes / 1e6, compressed_size / 1e6, elapsed / 60), flush=True)

        # Upload
        if upload_chunk(chunk_path, chunk_name):
            try:
                os.remove(chunk_path)
            except OSError:
                pass
            chunk_num += 1
            state["chunk_num"] = chunk_num
            state["total_docs"] = total_docs
            state["total_bytes_uploaded"] = state.get("total_bytes_uploaded", 0) + compressed_size
            state["seen_hashes"] = list(seen_hashes)
            state["domain_counts"] = domain_counts
            save_state(state)
            print("  Total: %d docs, %d chunks uploaded" % (total_docs, chunk_num), flush=True)
        else:
            print("  Upload failed, will retry", flush=True)
            try:
                os.remove(chunk_path)
            except OSError:
                pass

        # Brief pause between chunks
        time.sleep(10)


if __name__ == "__main__":
    main()
