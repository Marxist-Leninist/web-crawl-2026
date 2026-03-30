from huggingface_hub import HfApi
import tempfile, os

api = HfApi(token='HF_TOKEN_REDACTED')
REPO = 'OpenTransformer/web-crawl-2026'

# Upload main.rs
print('Uploading main.rs...')
api.upload_file(
    path_or_fileobj='/workspace/rust_crawler/src/main.rs',
    path_in_repo='crawler/rust/src/main.rs',
    repo_id=REPO,
    repo_type='dataset',
    commit_message='Add Rust web crawler source (v3, 150-300 docs/s)'
)

# Upload Cargo.toml
print('Uploading Cargo.toml...')
api.upload_file(
    path_or_fileobj='/workspace/rust_crawler/Cargo.toml',
    path_in_repo='crawler/rust/Cargo.toml',
    repo_id=REPO,
    repo_type='dataset',
    commit_message='Add Rust crawler Cargo.toml'
)

# Create and upload README
readme = '''---
license: apache-2.0
task_categories:
  - text-generation
language:
  - en
tags:
  - web-crawl
  - pretraining
  - nlp
  - text-corpus
pretty_name: Web Crawl 2026
size_categories:
  - 10B<n<100B
---

# Web Crawl 2026

A large-scale web crawl dataset for language model pretraining, collected by the OpenTransformer project.

## Dataset Description

This dataset contains text extracted from web pages crawled directly from the internet using custom high-throughput crawlers. All data is freshly scraped — **not** re-uploaded from existing datasets like FineWeb or C4.

### Data Format

Each record is a JSON line with fields:
-  — extracted text content (200–200,000 chars)
-  — source URL
-  — source domain
-  — crawl timestamp (ISO 8601)
-  — crawler identifier (, , )

### Collection Methods

Three crawlers run in parallel on a Vast.ai GPU box (Titan Xp, /usr/bin/bash.06/hr):

| Crawler | Language | Throughput | 1.2GB Chunk Time | Architecture |
|---------|----------|------------|-------------------|-------------|
| **crawl_rust** | Rust | **150–300 docs/s** | **5–6 min** | 500 async workers, tokio |
| crawl_go | Go | 11 docs/s | ~2 hrs | 150 goroutines |
| crawl_v5.py | Python | 0.8 docs/s | ~25 hrs | 20 async workers |

The Rust crawler is **27x faster than Go** and **375x faster than Python**.

### Rust Crawler Architecture

Source: 

**Key design decisions:**
- **500 concurrent async workers** via tokio + semaphore-based backpressure
- **Background queue refiller** — seed fetching runs in a separate task, never blocks crawling
- **Pre-generated seed file** — 593K URLs from Common Crawl index (12 crawl versions × 20 TLD patterns)
- **Link discovery** — extracts up to 50 links per crawled page, shuffled for domain diversity
- **Content dedup** — MD5 hash of first 500 chars, stored in DashMap (lock-free concurrent hashmap)
- **Domain throttling** — max 1000 pages per domain to ensure diversity
- **Streaming gzip** — writes compressed JSONL chunks (~1.2GB raw → ~350MB compressed)
- **Auto-upload** — each completed chunk is uploaded to HuggingFace Hub via Python subprocess

**Seed sources:**
1. Common Crawl URL index (CC-MAIN-2024-10 through CC-MAIN-2025-08)
2. Wikipedia random articles API (20K articles)
3. Sitemaps from 34 major sites (Reuters, BBC, Nature, StackOverflow, etc.)
4. Hacker News top/new/best stories

**Performance on Titan Xp box (/usr/bin/bash.06/hr):**
- Phase 1: 562K seeds loaded in 28 seconds
- Phase 2: 150–300 docs/s sustained throughput
- ~1.2GB chunk every 5–6 minutes
- ~12–15 GB/hour of raw crawled text
- Cost: ~/usr/bin/bash.004 per GB of crawled text

### Building & Running


  stable-x86_64-pc-windows-gnu installed - (timeout reading rustc version)


Rust is installed now. Great!

To get started you may need to restart your current shell.
This would reload its PATH environment variable to include
Cargo's bin directory (%USERPROFILE%\.cargo\bin).

### Dependencies

- Rust 1.75+
- Python 3 with  (for upload)
-  hardcoded (or modify to use env var)

### Quality Filtering

- HTML text extraction via  crate (article/main/body selectors)
- Minimum 200 chars, maximum 200K chars
- Content-type filtering (only text/html)
- URL filtering: blocks social media, login pages, media files, admin pages
- Deduplication via MD5 content hash

## Intended Use

Pretraining data for the AGILLM-3 language model (698M params, joint AR+SAT architecture).

## License

Apache 2.0
'''

with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False) as f:
    f.write(readme)
    readme_path = f.name

print('Uploading README.md...')
api.upload_file(
    path_or_fileobj=readme_path,
    path_in_repo='README.md',
    repo_id=REPO,
    repo_type='dataset',
    commit_message='Update README with crawler documentation and performance benchmarks'
)
os.unlink(readme_path)
print('All uploads complete!')
