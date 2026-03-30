package main

import (
	"bufio"
	"compress/gzip"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/html"
)

const (
	NumWorkers       = 150
	FetchTimeout     = 10 * time.Second
	MinTextLen       = 200
	MaxTextLen       = 200000
	MaxURLsPerDomain = 1000
	ChunkTargetBytes = 1_200_000_000
	MaxQueueSize     = 5_000_000
	StatusInterval   = 30 * time.Second
	HFToken          = "YOUR_HF_TOKEN_HERE"
	TargetRepo       = "OpenTransformer/web-crawl-2026"
	OutputDir        = "/workspace/scraped_data_go"
	SeedCacheDir     = "/workspace/seed_cache"
)

var blockedExtensions = map[string]bool{
	".pdf": true, ".jpg": true, ".jpeg": true, ".png": true, ".gif": true,
	".svg": true, ".mp3": true, ".mp4": true, ".avi": true, ".mov": true,
	".zip": true, ".tar": true, ".gz": true, ".exe": true, ".dmg": true,
	".css": true, ".js": true, ".woff": true, ".woff2": true, ".ttf": true,
	".ico": true, ".webp": true, ".bmp": true, ".doc": true, ".docx": true,
	".xls": true, ".xlsx": true, ".ppt": true, ".pptx": true, ".iso": true,
}

var blockedPattern = regexp.MustCompile(
	`(?i)(login|signup|signin|register|cart|checkout|payment|admin|wp-admin|` +
		`facebook\.com|twitter\.com|instagram\.com|tiktok\.com|linkedin\.com|` +
		`youtube\.com/watch|amazon\.|ebay\.|\.pdf$|/tag/|/category/|` +
		`/page/\d+|/feed/|/rss|/atom|#comment|/reply|/share|mailto:|tel:)`)

var tagStripper = regexp.MustCompile(`<script[^>]*>[\s\S]*?</script>|<style[^>]*>[\s\S]*?</style>|<[^>]+>`)
var whitespaceCollapse = regexp.MustCompile(`\s+`)

var userAgents = []string{
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
	"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

type Document struct {
	Text      string `json:"text"`
	URL       string `json:"url"`
	Domain    string `json:"domain"`
	Timestamp string `json:"timestamp"`
	Source    string `json:"source"`
}

type URLQueue struct {
	mu    sync.Mutex
	queue []string
	// No seen map — saves memory. Duplicate URLs are harmless since
	// we deduplicate by content hash anyway.
}

func NewURLQueue() *URLQueue {
	return &URLQueue{
		queue: make([]string, 0, 100000),
	}
}

func (q *URLQueue) Push(u string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.queue) >= MaxQueueSize {
		return false
	}
	q.queue = append(q.queue, u)
	return true
}

func (q *URLQueue) PushBulk(urls []string) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	added := 0
	for _, u := range urls {
		if len(q.queue) < MaxQueueSize {
			q.queue = append(q.queue, u)
			added++
		}
	}
	return added
}

func (q *URLQueue) Pop() (string, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.queue) == 0 {
		return "", false
	}
	u := q.queue[0]
	q.queue = q.queue[1:]
	return u, true
}

func (q *URLQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.queue)
}

func (q *URLQueue) Shuffle() {
	q.mu.Lock()
	defer q.mu.Unlock()
	rand.Shuffle(len(q.queue), func(i, j int) {
		q.queue[i], q.queue[j] = q.queue[j], q.queue[i]
	})
}

func isValidURL(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return false
	}
	if u.Host == "" || !strings.Contains(u.Host, ".") {
		return false
	}
	ext := strings.ToLower(filepath.Ext(u.Path))
	if blockedExtensions[ext] {
		return false
	}
	if blockedPattern.MatchString(rawURL) {
		return false
	}
	return true
}

func getDomain(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return u.Host
}

func contentHash(text string) string {
	end := len(text)
	if end > 500 {
		end = 500
	}
	h := md5.Sum([]byte(text[:end]))
	return hex.EncodeToString(h[:])
}

func extractText(body string) string {
	text := tagStripper.ReplaceAllString(body, " ")
	text = strings.ReplaceAll(text, "&amp;", "&")
	text = strings.ReplaceAll(text, "&lt;", "<")
	text = strings.ReplaceAll(text, "&gt;", ">")
	text = strings.ReplaceAll(text, "&quot;", "\"")
	text = strings.ReplaceAll(text, "&#39;", "'")
	text = strings.ReplaceAll(text, "&nbsp;", " ")
	text = whitespaceCollapse.ReplaceAllString(text, " ")
	text = strings.TrimSpace(text)
	return text
}

func extractLinks(body, baseURL string) []string {
	var links []string
	base, err := url.Parse(baseURL)
	if err != nil {
		return links
	}
	tokenizer := html.NewTokenizer(strings.NewReader(body))
	for {
		tt := tokenizer.Next()
		if tt == html.ErrorToken {
			break
		}
		if tt == html.StartTagToken || tt == html.SelfClosingTagToken {
			t := tokenizer.Token()
			if t.Data == "a" {
				for _, attr := range t.Attr {
					if attr.Key == "href" {
						href := strings.TrimSpace(attr.Val)
						if strings.HasPrefix(href, "#") || strings.HasPrefix(href, "javascript:") {
							continue
						}
						ref, err := url.Parse(href)
						if err != nil {
							continue
						}
						resolved := base.ResolveReference(ref)
						resolved.Fragment = ""
						full := resolved.String()
						if isValidURL(full) {
							links = append(links, full)
						}
					}
				}
			}
		}
	}
	return links
}

func fetchPage(client *http.Client, rawURL string) (body string, finalURL string, err error) {
	req, err := http.NewRequest("GET", rawURL, nil)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("User-Agent", userAgents[rand.Intn(len(userAgents))])
	req.Header.Set("Accept", "text/html,application/xhtml+xml")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	resp, err := client.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		io.Copy(io.Discard, resp.Body)
		return "", "", fmt.Errorf("status %d", resp.StatusCode)
	}

	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "text/html") && !strings.Contains(ct, "xhtml") {
		io.Copy(io.Discard, resp.Body)
		return "", "", fmt.Errorf("not html: %s", ct)
	}

	limited := io.LimitReader(resp.Body, 2*1024*1024)
	data, err := io.ReadAll(limited)
	if err != nil {
		return "", "", err
	}

	return string(data), resp.Request.URL.String(), nil
}

// ============================================================
// BULK SEED LOADING — downloads millions of URLs before crawling
// ============================================================

// downloadCommonCrawlURLs fetches URL lists from Common Crawl's columnar index.
// CC publishes .gz files listing all crawled URLs per segment.
func downloadCommonCrawlURLs(queue *URLQueue, targetCount int) int {
	fmt.Println("[seed] Downloading Common Crawl URL index...")
	os.MkdirAll(SeedCacheDir, 0755)

	// CC-MAIN-2024-10 cluster.idx lists pages by URL prefix
	// We download individual WARC path listings and extract URLs
	// Using the cdx-api to get URL samples across many domains
	client := &http.Client{Timeout: 60 * time.Second}

	added := 0
	// Query the CC index API for URLs matching common TLDs
	// Each query returns up to 15000 results
	queries := []string{
		"*.com/*", "*.org/*", "*.net/*", "*.edu/*", "*.gov/*",
		"*.co.uk/*", "*.io/*", "*.info/*", "*.us/*", "*.ca/*",
		"*.au/*", "*.de/*", "*.fr/*",
	}

	for _, q := range queries {
		if added >= targetCount {
			break
		}
		apiURL := fmt.Sprintf(
			"https://index.commoncrawl.org/CC-MAIN-2024-10-index?url=%s&output=json&limit=15000&fl=url&filter=languages:eng",
			url.QueryEscape(q))

		resp, err := client.Get(apiURL)
		if err != nil {
			fmt.Printf("[seed] CC query failed for %s: %v\n", q, err)
			continue
		}

		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
		batch := make([]string, 0, 10000)
		for scanner.Scan() {
			line := scanner.Text()
			var rec struct {
				URL string `json:"url"`
			}
			if json.Unmarshal([]byte(line), &rec) == nil && rec.URL != "" && isValidURL(rec.URL) {
				batch = append(batch, rec.URL)
				if len(batch) >= 10000 {
					added += queue.PushBulk(batch)
					batch = batch[:0]
				}
			}
		}
		resp.Body.Close()
		if len(batch) > 0 {
			added += queue.PushBulk(batch)
		}
		fmt.Printf("[seed] CC %s: total queued so far: %d\n", q, queue.Len())
	}
	return added
}

// downloadWikipediaURLs bulk-fetches Wikipedia article URLs via the API
func downloadWikipediaURLs(queue *URLQueue, count int) int {
	fmt.Printf("[seed] Fetching %d Wikipedia URLs...\n", count)
	client := &http.Client{Timeout: 30 * time.Second}
	added := 0
	batchSize := 500
	failures := 0
	for added < count && failures < 5 {
		remaining := count - added
		if remaining > batchSize {
			remaining = batchSize
		}
		resp, err := client.Get(fmt.Sprintf(
			"https://en.wikipedia.org/w/api.php?action=query&list=random&rnnamespace=0&rnlimit=%d&format=json",
			remaining))
		if err != nil {
			failures++
			time.Sleep(2 * time.Second)
			continue
		}
		var data struct {
			Query struct {
				Random []struct {
					Title string `json:"title"`
				} `json:"random"`
			} `json:"query"`
		}
		json.NewDecoder(resp.Body).Decode(&data)
		resp.Body.Close()

		if len(data.Query.Random) == 0 {
			failures++
			continue
		}

		batch := make([]string, 0, len(data.Query.Random))
		for _, p := range data.Query.Random {
			title := strings.ReplaceAll(p.Title, " ", "_")
			batch = append(batch, "https://en.wikipedia.org/wiki/"+title)
		}
		n := queue.PushBulk(batch)
		added += n
		if n == 0 {
			failures++
		} else {
			failures = 0
		}
		fmt.Printf("[seed] Wikipedia: %d URLs added (total %d)\n", n, added)
	}
	return added
}

// downloadSitemapURLs fetches sitemap.xml from major sites and extracts URLs
func downloadSitemapURLs(queue *URLQueue) int {
	fmt.Println("[seed] Fetching sitemaps from major sites...")
	client := &http.Client{Timeout: 15 * time.Second}

	sitemapSites := []string{
		"https://www.reuters.com/arc/outboundfeeds/sitemap-index/?outputType=xml",
		"https://www.bbc.com/sitemaps/https-sitemap-com-news-1.xml",
		"https://www.theguardian.com/sitemaps/news.xml",
		"https://www.npr.org/sitemap.xml",
		"https://arstechnica.com/sitemap.xml",
		"https://www.wired.com/sitemap.xml",
		"https://www.theatlantic.com/sitemap.xml",
		"https://www.vox.com/sitemaps/entries/1",
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
	}

	added := 0
	locRe := regexp.MustCompile(`<loc>([^<]+)</loc>`)

	for _, sitemapURL := range sitemapSites {
		resp, err := client.Get(sitemapURL)
		if err != nil {
			continue
		}
		// Only read top-level sitemap (2MB max) — skip sub-sitemaps to save memory
		body, err := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
		resp.Body.Close()
		if err != nil {
			continue
		}

		matches := locRe.FindAllSubmatch(body, -1)
		batch := make([]string, 0, len(matches))
		for _, m := range matches {
			u := string(m[1])
			if isValidURL(u) && !strings.HasSuffix(u, ".xml") && !strings.HasSuffix(u, ".xml.gz") {
				batch = append(batch, u)
			}
		}
		n := queue.PushBulk(batch)
		added += n
		if n > 0 {
			fmt.Printf("[seed] Sitemap %s: +%d URLs\n", getDomain(sitemapURL), n)
		}
	}
	fmt.Printf("[seed] Sitemaps total: %d URLs added\n", added)
	return added
}

func getHNURLs(n int) []string {
	var urls []string
	client := &http.Client{Timeout: 10 * time.Second}
	for _, endpoint := range []string{"topstories", "newstories", "beststories"} {
		resp, err := client.Get("https://hacker-news.firebaseio.com/v0/" + endpoint + ".json")
		if err != nil {
			continue
		}
		var ids []int
		json.NewDecoder(resp.Body).Decode(&ids)
		resp.Body.Close()
		if len(ids) > n {
			ids = ids[:n]
		}
		for _, id := range ids {
			itemResp, err := client.Get(fmt.Sprintf("https://hacker-news.firebaseio.com/v0/item/%d.json", id))
			if err != nil {
				continue
			}
			var item struct {
				URL string `json:"url"`
			}
			json.NewDecoder(itemResp.Body).Decode(&item)
			itemResp.Body.Close()
			if item.URL != "" {
				urls = append(urls, item.URL)
			}
		}
	}
	return urls
}

func uploadChunk(filepath, remoteName string) bool {
	cmd := exec.Command("python3", "-c", fmt.Sprintf(`
from huggingface_hub import HfApi
api = HfApi(token="%s")
api.upload_file(path_or_fileobj="%s", path_in_repo="data/%s", repo_id="%s", repo_type="dataset")
print("OK")
`, HFToken, filepath, remoteName, TargetRepo))
	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("  Upload failed: %s\n%s\n", err, string(out))
		return false
	}
	fmt.Printf("  Uploaded %s\n", remoteName)
	return true
}

func main() {
	fmt.Println("============================================================")
	fmt.Println("Go Web Crawler v2 — Bulk-seeded high-throughput crawler")
	fmt.Printf("Target: %s\n", TargetRepo)
	fmt.Printf("Workers: %d, Chunk target: ~%.1fGB\n", NumWorkers, float64(ChunkTargetBytes)/1e9)
	fmt.Println("============================================================")

	os.MkdirAll(OutputDir, 0755)

	// Use connection pooling with higher limits
	transport := &http.Transport{
		MaxIdleConns:        300,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     30 * time.Second,
	}
	client := &http.Client{
		Timeout:   FetchTimeout,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return fmt.Errorf("too many redirects")
			}
			return nil
		},
	}

	seenHashes := sync.Map{}
	domainCounts := sync.Map{}

	chunkNum := 0
	for {
		name := fmt.Sprintf("crawl_go_chunk%04d.jsonl.gz", chunkNum)
		if _, err := os.Stat(filepath.Join(OutputDir, name)); os.IsNotExist(err) {
			break
		}
		chunkNum++
	}

	// ============================================================
	// PHASE 1: BULK SEED LOADING — fill queue with millions of URLs
	// ============================================================
	queue := NewURLQueue()

	fmt.Println("\n=== PHASE 1: Loading seed URLs ===")
	t0 := time.Now()

	// 1. Common Crawl index — millions of known-good English URLs
	downloadCommonCrawlURLs(queue, 2_000_000)

	// 2. Wikipedia — 10k random articles (high quality, lots of outlinks)
	downloadWikipediaURLs(queue, 10000)

	// 3. Sitemaps from major content sites
	downloadSitemapURLs(queue)

	// 4. HN links
	for _, u := range getHNURLs(100) {
		queue.Push(u)
	}

	elapsed := time.Since(t0)
	fmt.Printf("\n=== PHASE 1 complete: %d seed URLs loaded in %.0f seconds ===\n\n", queue.Len(), elapsed.Seconds())

	// Shuffle the queue for domain diversity
	queue.Shuffle()

	// ============================================================
	// PHASE 2: CRAWL — process URLs and discover new ones
	// ============================================================
	fmt.Println("=== PHASE 2: Crawling ===")

	for {
		if queue.Len() == 0 {
			fmt.Println("Queue exhausted. Reloading seeds...")
			downloadCommonCrawlURLs(queue, 500_000)
			downloadWikipediaURLs(queue, 5000)
			downloadSitemapURLs(queue)
			if queue.Len() == 0 {
				fmt.Println("Cannot refill queue. Waiting 5 min...")
				time.Sleep(5 * time.Minute)
				continue
			}
		}

		chunkName := fmt.Sprintf("crawl_go_chunk%04d.jsonl.gz", chunkNum)
		chunkPath := filepath.Join(OutputDir, chunkName)

		f, err := os.Create(chunkPath)
		if err != nil {
			fmt.Printf("Error creating chunk: %v\n", err)
			time.Sleep(30 * time.Second)
			continue
		}
		gzw := gzip.NewWriter(f)

		var writeMu sync.Mutex
		var chunkBytes int64
		var chunkDocs int64
		var pagesTried int64
		chunkStart := time.Now()

		fmt.Printf("\nStarting chunk %d (%d URLs queued)...\n", chunkNum, queue.Len())

		// Status printer
		stopStatus := make(chan struct{})
		go func() {
			ticker := time.NewTicker(StatusInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					docs := atomic.LoadInt64(&chunkDocs)
					bytes := atomic.LoadInt64(&chunkBytes)
					tried := atomic.LoadInt64(&pagesTried)
					elapsed := time.Since(chunkStart).Seconds()
					rate := float64(docs) / elapsed
					fmt.Printf("  Chunk %d: %d docs, %.0fMB, %d queued, %.1f docs/s, tried %d, %.0fs elapsed\n",
						chunkNum, docs, float64(bytes)/1e6, queue.Len(), rate, tried, elapsed)
				case <-stopStatus:
					return
				}
			}
		}()

		// Worker pool
		urlChan := make(chan string, NumWorkers*4)
		var wg sync.WaitGroup

		for i := 0; i < NumWorkers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for rawURL := range urlChan {
					atomic.AddInt64(&pagesTried, 1)

					domain := getDomain(rawURL)
					if v, ok := domainCounts.Load(domain); ok && v.(int) >= MaxURLsPerDomain {
						continue
					}

					body, finalURL, err := fetchPage(client, rawURL)
					if err != nil {
						continue
					}

					text := extractText(body)
					if len(text) < MinTextLen {
						continue
					}
					if len(text) > MaxTextLen {
						text = text[:MaxTextLen]
					}

					h := contentHash(text)
					if _, loaded := seenHashes.LoadOrStore(h, true); loaded {
						continue
					}

					doc := Document{
						Text:      text,
						URL:       finalURL,
						Domain:    domain,
						Timestamp: time.Now().UTC().Format("2006-01-02T15:04:05Z"),
						Source:    "crawl_go_v2",
					}
					jsonBytes, err := json.Marshal(doc)
					if err != nil {
						continue
					}

					writeMu.Lock()
					gzw.Write(jsonBytes)
					gzw.Write([]byte("\n"))
					atomic.AddInt64(&chunkBytes, int64(len(jsonBytes)))
					atomic.AddInt64(&chunkDocs, 1)
					writeMu.Unlock()

					if v, ok := domainCounts.Load(domain); ok {
						domainCounts.Store(domain, v.(int)+1)
					} else {
						domainCounts.Store(domain, 1)
					}

					// Discover new links from crawled pages
					links := extractLinks(body, finalURL)
					rand.Shuffle(len(links), func(i, j int) { links[i], links[j] = links[j], links[i] })
					maxLinks := 50
					if len(links) > maxLinks {
						links = links[:maxLinks]
					}
					queue.PushBulk(links)
				}
			}()
		}

		// Feed URLs to workers
		for atomic.LoadInt64(&chunkBytes) < ChunkTargetBytes {
			u, ok := queue.Pop()
			if !ok {
				// Queue depleted mid-chunk, refill inline
				fmt.Println("  Queue low, fetching more Wikipedia URLs...")
				downloadWikipediaURLs(queue, 5000)
				if queue.Len() == 0 {
					break
				}
				continue
			}
			urlChan <- u
		}
		close(urlChan)
		wg.Wait()
		close(stopStatus)

		gzw.Close()
		f.Close()

		docs := atomic.LoadInt64(&chunkDocs)
		bytes := atomic.LoadInt64(&chunkBytes)
		chunkElapsed := time.Since(chunkStart).Seconds()

		if docs == 0 {
			fmt.Println("  No docs in chunk, cleaning up")
			os.Remove(chunkPath)
			time.Sleep(5 * time.Minute)
			continue
		}

		fi, _ := os.Stat(chunkPath)
		compressedSize := fi.Size()
		fmt.Printf("  Chunk %d complete: %d docs, %.0fMB raw, %.0fMB compressed, %.0fs, %.1f docs/s\n",
			chunkNum, docs, float64(bytes)/1e6, float64(compressedSize)/1e6,
			chunkElapsed, float64(docs)/chunkElapsed)

		if uploadChunk(chunkPath, chunkName) {
			os.Remove(chunkPath)
			chunkNum++
			fmt.Printf("  Total chunks uploaded: %d\n", chunkNum)
		} else {
			fmt.Println("  Upload failed, will retry chunk")
			os.Remove(chunkPath)
		}
	}
}
