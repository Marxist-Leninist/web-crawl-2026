use dashmap::DashMap;
use flate2::write::GzEncoder;
use flate2::Compression;
use md5::{Digest, Md5};
use rand::seq::SliceRandom;
use rand::Rng;
use regex::Regex;
use reqwest::Client;
use scraper::{Html, Selector};
use serde::Serialize;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore};
use url::Url;

const NUM_WORKERS: usize = 500;
const FETCH_TIMEOUT: Duration = Duration::from_secs(8);
const MIN_TEXT_LEN: usize = 200;
const MAX_TEXT_LEN: usize = 200_000;
const MAX_URLS_PER_DOMAIN: u32 = 1000;
const CHUNK_TARGET_BYTES: i64 = 1_200_000_000;
const MAX_QUEUE_SIZE: usize = 5_000_000;
const STATUS_INTERVAL: Duration = Duration::from_secs(30);
const HF_TOKEN: &str = "YOUR_HF_TOKEN_HERE";
const TARGET_REPO: &str = "OpenTransformer/web-crawl-2026";
const OUTPUT_DIR: &str = "/workspace/scraped_data_rust";

static USER_AGENTS: &[&str] = &[
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
];

static BLOCKED_EXTENSIONS: &[&str] = &[
    ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".svg", ".mp3", ".mp4", ".avi",
    ".mov", ".zip", ".tar", ".gz", ".exe", ".dmg", ".css", ".js", ".woff",
    ".woff2", ".ttf", ".ico", ".webp", ".bmp", ".doc", ".docx", ".xls",
    ".xlsx", ".ppt", ".pptx", ".iso", ".rar", ".7z", ".apk", ".deb", ".rpm",
];

#[derive(Serialize)]
struct Document {
    text: String,
    url: String,
    domain: String,
    timestamp: String,
    source: String,
}

struct URLQueue {
    queue: Mutex<Vec<String>>,
}

impl URLQueue {
    fn new() -> Self {
        Self {
            queue: Mutex::new(Vec::with_capacity(100_000)),
        }
    }

    async fn push(&self, url: String) -> bool {
        let mut q = self.queue.lock().await;
        if q.len() >= MAX_QUEUE_SIZE {
            return false;
        }
        q.push(url);
        true
    }

    async fn push_bulk(&self, urls: Vec<String>) -> usize {
        let mut q = self.queue.lock().await;
        let mut added = 0;
        for url in urls {
            if q.len() >= MAX_QUEUE_SIZE {
                break;
            }
            q.push(url);
            added += 1;
        }
        added
    }

    async fn pop_batch(&self, n: usize) -> Vec<String> {
        let mut q = self.queue.lock().await;
        let drain_count = n.min(q.len());
        if drain_count == 0 {
            return Vec::new();
        }
        let start = q.len() - drain_count;
        q.drain(start..).collect()
    }

    async fn len(&self) -> usize {
        self.queue.lock().await.len()
    }

    async fn shuffle(&self) {
        let mut q = self.queue.lock().await;
        let mut rng = rand::thread_rng();
        q.shuffle(&mut rng);
    }
}

fn is_valid_url(raw: &str) -> bool {
    let u = match Url::parse(raw) {
        Ok(u) => u,
        Err(_) => return false,
    };
    match u.scheme() {
        "http" | "https" => {}
        _ => return false,
    }
    let host = match u.host_str() {
        Some(h) => h,
        None => return false,
    };
    if !host.contains('.') {
        return false;
    }
    let path_lower = u.path().to_lowercase();
    for ext in BLOCKED_EXTENSIONS {
        if path_lower.ends_with(ext) {
            return false;
        }
    }
    let full_lower = raw.to_lowercase();
    if BLOCKED_PATTERN.is_match(&full_lower) {
        return false;
    }
    true
}

fn get_domain(raw: &str) -> String {
    Url::parse(raw)
        .ok()
        .and_then(|u| u.host_str().map(|h| h.to_string()))
        .unwrap_or_default()
}

fn content_hash(text: &str) -> String {
    let mut end = text.len().min(500);
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    let mut hasher = Md5::new();
    hasher.update(text[..end].as_bytes());
    format!("{:x}", hasher.finalize())
}

fn extract_text_from_html(html_str: &str) -> String {
    let document = Html::parse_document(html_str);
    let body_sel = Selector::parse("article, main, [role=main], body").unwrap();

    let mut text_parts: Vec<String> = Vec::new();

    // Try to find main content area first, fallback to whole document
    let root = document.select(&body_sel).next();
    if let Some(element) = root {
        for text_node in element.text() {
            let trimmed = text_node.trim();
            if !trimmed.is_empty() {
                text_parts.push(trimmed.to_string());
            }
        }
    } else {
        for text_node in document.root_element().text() {
            let trimmed = text_node.trim();
            if !trimmed.is_empty() {
                text_parts.push(trimmed.to_string());
            }
        }
    }

    let mut result = text_parts.join(" ");

    // Collapse whitespace
    let ws_re = Regex::new(r"\s+").unwrap();
    result = ws_re.replace_all(&result, " ").trim().to_string();

    // Remove any remaining HTML entities
    result = result
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&nbsp;", " ");

    result
}

fn extract_links(html_str: &str, base_url: &str) -> Vec<String> {
    let base = match Url::parse(base_url) {
        Ok(u) => u,
        Err(_) => return Vec::new(),
    };
    let document = Html::parse_document(html_str);
    let a_sel = Selector::parse("a[href]").unwrap();
    let mut links = Vec::new();

    for element in document.select(&a_sel) {
        if let Some(href) = element.value().attr("href") {
            let href = href.trim();
            if href.starts_with('#') || href.starts_with("javascript:") || href.starts_with("mailto:") || href.starts_with("tel:") {
                continue;
            }
            if let Ok(resolved) = base.join(href) {
                let mut full = resolved.to_string();
                // Strip fragment
                if let Some(pos) = full.find('#') {
                    full.truncate(pos);
                }
                if is_valid_url(&full) {
                    links.push(full);
                }
            }
        }
    }
    links
}

async fn fetch_page(client: &Client, url: &str) -> Result<(String, String), String> {
    let ua = {
        let mut rng = rand::thread_rng();
        USER_AGENTS[rng.gen_range(0..USER_AGENTS.len())]
    };

    let resp = client
        .get(url)
        .header("User-Agent", ua)
        .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
        .header("Accept-Language", "en-US,en;q=0.9")
        .header("Accept-Encoding", "gzip, deflate, br")
        .send()
        .await
        .map_err(|e| e.to_string())?;

    let status = resp.status();
    if !status.is_success() {
        return Err(format!("status {}", status));
    }

    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !ct.contains("text/html") && !ct.contains("xhtml") {
        return Err(format!("not html: {}", ct));
    }

    let final_url = resp.url().to_string();

    // Limit body to 2MB
    let body_bytes = resp.bytes().await.map_err(|e| e.to_string())?;
    if body_bytes.len() > 2 * 1024 * 1024 {
        let body = String::from_utf8_lossy(&body_bytes[..2 * 1024 * 1024]).to_string();
        return Ok((body, final_url));
    }
    let body = String::from_utf8_lossy(&body_bytes).to_string();
    Ok((body, final_url))
}

async fn download_commoncrawl_urls(queue: &URLQueue, target_count: usize) -> usize {
    println!("[seed] Downloading Common Crawl URL index...");
    let client = Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .unwrap();

    let queries = vec![
        "*.com/*", "*.org/*", "*.net/*", "*.edu/*", "*.gov/*",
        "*.co.uk/*", "*.io/*", "*.info/*", "*.us/*", "*.ca/*",
        "*.au/*", "*.de/*", "*.fr/*",
    ];

    let mut added = 0usize;
    for q in queries {
        if added >= target_count {
            break;
        }
        let api_url = format!(
            "https://index.commoncrawl.org/CC-MAIN-2024-10-index?url={}&output=json&limit=15000&fl=url&filter=languages:eng",
            urlencoding::encode(q)
        );
        let resp = match client.get(&api_url).send().await {
            Ok(r) => r,
            Err(e) => {
                println!("[seed] CC query failed for {}: {}", q, e);
                continue;
            }
        };
        let text = match resp.text().await {
            Ok(t) => t,
            Err(_) => continue,
        };
        let mut batch = Vec::with_capacity(10_000);
        for line in text.lines() {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(line) {
                if let Some(url) = val.get("url").and_then(|u| u.as_str()) {
                    if is_valid_url(url) {
                        batch.push(url.to_string());
                        if batch.len() >= 10_000 {
                            added += queue.push_bulk(batch).await;
                            batch = Vec::with_capacity(10_000);
                        }
                    }
                }
            }
        }
        if !batch.is_empty() {
            added += queue.push_bulk(batch).await;
        }
        println!("[seed] CC {}: total queued so far: {}", q, queue.len().await);
    }
    added
}

async fn download_wikipedia_urls(queue: &URLQueue, count: usize) -> usize {
    println!("[seed] Fetching {} Wikipedia URLs...", count);
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();

    let mut added = 0;
    let mut failures = 0;
    while added < count && failures < 5 {
        let remaining = (count - added).min(500);
        let url = format!(
            "https://en.wikipedia.org/w/api.php?action=query&list=random&rnnamespace=0&rnlimit={}&format=json",
            remaining
        );
        let resp = match client.get(&url).send().await {
            Ok(r) => r,
            Err(_) => {
                failures += 1;
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        let data: serde_json::Value = match resp.json().await {
            Ok(d) => d,
            Err(_) => {
                failures += 1;
                continue;
            }
        };
        let random = match data["query"]["random"].as_array() {
            Some(a) => a,
            None => {
                failures += 1;
                continue;
            }
        };
        let batch: Vec<String> = random
            .iter()
            .filter_map(|p| p["title"].as_str())
            .map(|t| format!("https://en.wikipedia.org/wiki/{}", t.replace(' ', "_")))
            .collect();
        let n = queue.push_bulk(batch).await;
        added += n;
        if n == 0 {
            failures += 1;
        } else {
            failures = 0;
        }
        println!("[seed] Wikipedia: {} URLs added (total {})", n, added);
    }
    added
}

async fn download_sitemap_urls(queue: &URLQueue) -> usize {
    println!("[seed] Fetching sitemaps from major sites...");
    let client = Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();

    let sitemaps = vec![
        "https://www.reuters.com/arc/outboundfeeds/sitemap-index/?outputType=xml",
        "https://www.bbc.com/sitemaps/https-sitemap-com-news-1.xml",
        "https://www.theguardian.com/sitemaps/news.xml",
        "https://www.npr.org/sitemap.xml",
        "https://arstechnica.com/sitemap.xml",
        "https://www.wired.com/sitemap.xml",
        "https://www.theatlantic.com/sitemap.xml",
        "https://www.nature.com/sitemap.xml",
        "https://phys.org/sitemap.xml",
        "https://www.scientificamerican.com/sitemap.xml",
        "https://www.smithsonianmag.com/sitemap.xml",
        "https://www.britannica.com/sitemap.xml",
        "https://www.healthline.com/sitemap.xml",
        "https://www.investopedia.com/sitemap.xml",
        "https://www.geeksforgeeks.org/sitemap.xml",
        "https://realpython.com/sitemap.xml",
        "https://www.freecodecamp.org/news/sitemap.xml",
        "https://hackernoon.com/sitemap.xml",
        "https://dev.to/sitemap.xml",
        "https://www.history.com/sitemap.xml",
        "https://www.livescience.com/sitemap.xml",
        "https://www.space.com/sitemap.xml",
        "https://www.sciencedaily.com/sitemap.xml",
        "https://www.psychologytoday.com/sitemap.xml",
        "https://www.mayoclinic.org/sitemap.xml",
        "https://medlineplus.gov/sitemap.xml",
        "https://plato.stanford.edu/sitemap.xml",
        "https://www.brookings.edu/sitemap.xml",
        "https://www.rand.org/sitemap.xml",
        "https://stackoverflow.com/sitemap.xml",
        "https://docs.python.org/3/sitemap.xml",
        "https://docs.rs/sitemap.xml",
        "https://www.rust-lang.org/sitemap.xml",
        "https://go.dev/sitemap.xml",
    ];

    let loc_re = Regex::new(r"<loc>([^<]+)</loc>").unwrap();
    let mut added = 0;

    for sitemap_url in sitemaps {
        let resp = match client.get(sitemap_url).send().await {
            Ok(r) => r,
            Err(_) => continue,
        };
        let body = match resp.text().await {
            Ok(t) => t,
            Err(_) => continue,
        };
        let batch: Vec<String> = loc_re
            .captures_iter(&body)
            .filter_map(|c| {
                let u = c[1].to_string();
                if is_valid_url(&u) && !u.ends_with(".xml") && !u.ends_with(".xml.gz") {
                    Some(u)
                } else {
                    None
                }
            })
            .collect();
        let n = queue.push_bulk(batch).await;
        if n > 0 {
            println!("[seed] Sitemap {}: +{} URLs", get_domain(sitemap_url), n);
        }
        added += n;
    }
    println!("[seed] Sitemaps total: {} URLs added", added);
    added
}

async fn download_hn_urls(queue: &URLQueue) -> usize {
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    let mut urls = Vec::new();
    for endpoint in &["topstories", "newstories", "beststories"] {
        let api = format!("https://hacker-news.firebaseio.com/v0/{}.json", endpoint);
        let resp = match client.get(&api).send().await {
            Ok(r) => r,
            Err(_) => continue,
        };
        let ids: Vec<u64> = match resp.json::<Vec<u64>>().await {
            Ok(v) => v,
            Err(_) => continue,
        };
        // Fetch items concurrently in batches
        let sem = Arc::new(Semaphore::new(20));
        let mut handles = Vec::new();
        for &id in ids.iter().take(100) {
            let client = client.clone();
            let sem = sem.clone();
            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire().await.ok()?;
                let url = format!("https://hacker-news.firebaseio.com/v0/item/{}.json", id);
                let resp = client.get(&url).send().await.ok()?;
                let item: serde_json::Value = resp.json().await.ok()?;
                item["url"].as_str().map(|s| s.to_string())
            }));
        }
        for h in handles {
            if let Ok(Some(url)) = h.await {
                if !url.is_empty() {
                    urls.push(url);
                }
            }
        }
    }
    let n = queue.push_bulk(urls).await;
    println!("[seed] HN: {} URLs added", n);
    n
}

fn upload_chunk(filepath: &str, remote_name: &str) -> bool {
    let script = format!(
        r#"
from huggingface_hub import HfApi
api = HfApi(token="{}")
api.upload_file(path_or_fileobj="{}", path_in_repo="data/{}", repo_id="{}", repo_type="dataset")
print("OK")
"#,
        HF_TOKEN, filepath, remote_name, TARGET_REPO
    );
    let output = Command::new("python3").arg("-c").arg(&script).output();
    match output {
        Ok(o) => {
            if o.status.success() {
                println!("  Uploaded {}", remote_name);
                true
            } else {
                println!(
                    "  Upload failed: {}",
                    String::from_utf8_lossy(&o.stderr)
                );
                false
            }
        }
        Err(e) => {
            println!("  Upload error: {}", e);
            false
        }
    }
}

lazy_static::lazy_static! {
    static ref BLOCKED_PATTERN: Regex = Regex::new(
        r"(?i)(login|signup|signin|register|cart|checkout|payment|admin|wp-admin|facebook\.com|twitter\.com|instagram\.com|tiktok\.com|linkedin\.com|youtube\.com/watch|amazon\.|ebay\.|\.pdf$|/tag/|/category/|/page/\d+|/feed/|/rss|/atom|#comment|/reply|/share|mailto:|tel:)"
    ).unwrap();
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    println!("============================================================");
    println!("Rust Web Crawler v1 — Async high-throughput crawler");
    println!("Target: {}", TARGET_REPO);
    println!(
        "Workers: {}, Chunk target: ~{:.1}GB",
        NUM_WORKERS,
        CHUNK_TARGET_BYTES as f64 / 1e9
    );
    println!("============================================================");

    std::fs::create_dir_all(OUTPUT_DIR).ok();

    let client = Client::builder()
        .timeout(FETCH_TIMEOUT)
        .pool_max_idle_per_host(20)
        .pool_idle_timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::limited(5))
        .danger_accept_invalid_certs(true)
        .tcp_keepalive(Duration::from_secs(15))
        .connect_timeout(Duration::from_secs(5))
        .build()
        .expect("Failed to build HTTP client");

    let seen_hashes: Arc<DashMap<String, ()>> = Arc::new(DashMap::new());
    let domain_counts: Arc<DashMap<String, u32>> = Arc::new(DashMap::new());

    // Find next chunk number
    let mut chunk_num: u32 = 0;
    loop {
        let name = format!("crawl_rust_chunk{:04}.jsonl.gz", chunk_num);
        let path = Path::new(OUTPUT_DIR).join(&name);
        if !path.exists() {
            break;
        }
        chunk_num += 1;
    }

    // === PHASE 1: SEED LOADING ===
    let queue = Arc::new(URLQueue::new());
    println!("\n=== PHASE 1: Loading seed URLs ===");
    let t0 = Instant::now();

    // Try loading from pre-generated mega seed file first
    let seed_file = "/workspace/mega_seeds.txt";
    if Path::new(seed_file).exists() {
        println!("[seed] Loading from {}...", seed_file);
        let content = std::fs::read_to_string(seed_file).unwrap_or_default();
        let mut batch = Vec::with_capacity(10_000);
        let mut loaded = 0usize;
        for line in content.lines() {
            let url = line.trim();
            if !url.is_empty() && is_valid_url(url) {
                batch.push(url.to_string());
                if batch.len() >= 10_000 {
                    loaded += queue.push_bulk(batch).await;
                    batch = Vec::with_capacity(10_000);
                }
            }
        }
        if !batch.is_empty() {
            loaded += queue.push_bulk(batch).await;
        }
        println!("[seed] Loaded {} URLs from seed file", loaded);
    }

    // Supplement with live sources if needed
    if queue.len().await < 100_000 {
        download_commoncrawl_urls(&queue, 4_000_000).await;
    }
    download_wikipedia_urls(&queue, 20_000).await;
    download_sitemap_urls(&queue).await;
    download_hn_urls(&queue).await;

    println!(
        "\n=== PHASE 1 complete: {} seed URLs loaded in {:.0}s ===\n",
        queue.len().await,
        t0.elapsed().as_secs_f64()
    );

    queue.shuffle().await;

    // === PHASE 2: CRAWL ===
    println!("=== PHASE 2: Crawling ===");

    // Background queue refiller — keeps queue topped up without blocking crawl
    let refill_queue = queue.clone();
    let refilling = Arc::new(AtomicU64::new(0)); // 1 = refill in progress
    let refilling_bg = refilling.clone();
    let _refill_handle = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let qlen = refill_queue.len().await;
            if qlen < 50_000 && refilling_bg.compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                println!("[bg-refill] Queue at {} — refilling...", qlen);
                download_commoncrawl_urls(&refill_queue, 500_000).await;
                download_wikipedia_urls(&refill_queue, 10_000).await;
                download_sitemap_urls(&refill_queue).await;
                download_hn_urls(&refill_queue).await;
                println!("[bg-refill] Done, queue now at {}", refill_queue.len().await);
                refilling_bg.store(0, Ordering::SeqCst);
            }
        }
    });

    loop {
        if queue.len().await == 0 {
            println!("Queue empty, waiting for background refiller...");
            // Trigger refill if not already running
            for _ in 0..30 {
                tokio::time::sleep(Duration::from_secs(2)).await;
                if queue.len().await > 0 {
                    break;
                }
            }
            if queue.len().await == 0 {
                println!("Still empty after 60s. Force-seeding...");
                download_wikipedia_urls(&queue, 10_000).await;
            }
            continue;
        }

        let chunk_name = format!("crawl_rust_chunk{:04}.jsonl.gz", chunk_num);
        let chunk_path_str = format!("{}/{}", OUTPUT_DIR, chunk_name);
        let chunk_path = PathBuf::from(&chunk_path_str);

        let file = std::fs::File::create(&chunk_path).expect("Cannot create chunk file");
        let gz_writer = Arc::new(Mutex::new(GzEncoder::new(file, Compression::fast())));

        let chunk_bytes = Arc::new(AtomicI64::new(0));
        let chunk_docs = Arc::new(AtomicI64::new(0));
        let pages_tried = Arc::new(AtomicU64::new(0));
        let chunk_start = Instant::now();

        println!(
            "\nStarting chunk {} ({} URLs queued)...",
            chunk_num,
            queue.len().await
        );

        // Status printer task
        let cb = chunk_bytes.clone();
        let cd = chunk_docs.clone();
        let pt = pages_tried.clone();
        let ql = queue.clone();
        let cn = chunk_num;
        let status_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(STATUS_INTERVAL);
            loop {
                interval.tick().await;
                let docs = cd.load(Ordering::Relaxed);
                let bytes = cb.load(Ordering::Relaxed);
                let tried = pt.load(Ordering::Relaxed);
                let elapsed = chunk_start.elapsed().as_secs_f64();
                let rate = docs as f64 / elapsed.max(1.0);
                println!(
                    "  Chunk {}: {} docs, {:.0}MB, {} queued, {:.1} docs/s, tried {}, {:.0}s elapsed",
                    cn,
                    docs,
                    bytes as f64 / 1e6,
                    ql.len().await,
                    rate,
                    tried,
                    elapsed
                );
                if bytes >= CHUNK_TARGET_BYTES {
                    break;
                }
            }
        });

        // Semaphore for concurrency control
        let semaphore = Arc::new(Semaphore::new(NUM_WORKERS));
        let mut task_handles = Vec::new();

        // Feed URLs until chunk is full
        while chunk_bytes.load(Ordering::Relaxed) < CHUNK_TARGET_BYTES {
            let batch = queue.pop_batch(100).await;
            if batch.is_empty() {
                // Don't block — just wait briefly for background refiller
                tokio::time::sleep(Duration::from_millis(500)).await;
                if queue.len().await == 0 {
                    // Try a small inline refill
                    download_wikipedia_urls(&queue, 2_000).await;
                }
                continue;
            }

            for url in batch {
                let permit = semaphore.clone().acquire_owned().await.unwrap();
                let client = client.clone();
                let seen_hashes = seen_hashes.clone();
                let domain_counts = domain_counts.clone();
                let gz_writer = gz_writer.clone();
                let chunk_bytes = chunk_bytes.clone();
                let chunk_docs = chunk_docs.clone();
                let pages_tried = pages_tried.clone();
                let queue = queue.clone();

                let handle = tokio::spawn(async move {
                    let _permit = permit;
                    pages_tried.fetch_add(1, Ordering::Relaxed);

                    let domain = get_domain(&url);
                    if let Some(count) = domain_counts.get(&domain) {
                        if *count >= MAX_URLS_PER_DOMAIN {
                            return;
                        }
                    }

                    let (body, final_url) = match fetch_page(&client, &url).await {
                        Ok((b, u)) => (b, u),
                        Err(_) => return,
                    };

                    let text = extract_text_from_html(&body);
                    if text.len() < MIN_TEXT_LEN {
                        return;
                    }
                    let text = if text.len() > MAX_TEXT_LEN {
                        // Find char boundary at or before MAX_TEXT_LEN
                        let mut end = MAX_TEXT_LEN;
                        while end > 0 && !text.is_char_boundary(end) {
                            end -= 1;
                        }
                        text[..end].to_string()
                    } else {
                        text
                    };

                    let hash = content_hash(&text);
                    if seen_hashes.contains_key(&hash) {
                        return;
                    }
                    seen_hashes.insert(hash, ());

                    let doc = Document {
                        text,
                        url: final_url.clone(),
                        domain: domain.clone(),
                        timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                        source: "crawl_rust_v1".to_string(),
                    };
                    let json_bytes = match serde_json::to_vec(&doc) {
                        Ok(b) => b,
                        Err(_) => return,
                    };

                    {
                        let mut writer = gz_writer.lock().await;
                        let _ = writer.write_all(&json_bytes);
                        let _ = writer.write_all(b"\n");
                    }
                    chunk_bytes.fetch_add(json_bytes.len() as i64, Ordering::Relaxed);
                    chunk_docs.fetch_add(1, Ordering::Relaxed);

                    domain_counts
                        .entry(domain)
                        .and_modify(|c| *c += 1)
                        .or_insert(1);

                    // Extract and enqueue discovered links
                    let mut links = extract_links(&body, &final_url);
                    {
                        let mut rng = rand::thread_rng();
                        links.shuffle(&mut rng);
                    }
                    links.truncate(50);
                    if !links.is_empty() {
                        queue.push_bulk(links).await;
                    }
                });
                task_handles.push(handle);

                // Clean up completed handles periodically
                if task_handles.len() > 2000 {
                    let mut new_handles = Vec::with_capacity(1000);
                    for h in task_handles.drain(..) {
                        if !h.is_finished() {
                            new_handles.push(h);
                        }
                    }
                    task_handles = new_handles;
                }
            }
        }

        // Wait for all remaining tasks
        for h in task_handles {
            let _ = h.await;
        }

        status_handle.abort();

        // Finalize chunk
        let writer = Arc::try_unwrap(gz_writer)
            .expect("All tasks should be done");
        let writer = writer.into_inner();
        let _ = writer.finish();

        let docs = chunk_docs.load(Ordering::Relaxed);
        let bytes = chunk_bytes.load(Ordering::Relaxed);
        let elapsed = chunk_start.elapsed().as_secs_f64();

        if docs == 0 {
            println!("  No docs in chunk, cleaning up");
            let _ = std::fs::remove_file(&chunk_path);
            tokio::time::sleep(Duration::from_secs(300)).await;
            continue;
        }

        let compressed_size = std::fs::metadata(&chunk_path)
            .map(|m| m.len())
            .unwrap_or(0);
        println!(
            "  Chunk {} complete: {} docs, {:.0}MB raw, {:.0}MB compressed, {:.0}s, {:.1} docs/s",
            chunk_num,
            docs,
            bytes as f64 / 1e6,
            compressed_size as f64 / 1e6,
            elapsed,
            docs as f64 / elapsed
        );

        if upload_chunk(&chunk_path_str, &chunk_name) {
            let _ = std::fs::remove_file(&chunk_path);
            chunk_num += 1;
            println!("  Total chunks uploaded: {}", chunk_num);
        } else {
            println!("  Upload failed, will retry chunk");
            let _ = std::fs::remove_file(&chunk_path);
        }
    }
}
