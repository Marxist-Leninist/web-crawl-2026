# Web Crawl 2026

Multi-language web crawling infrastructure for training data collection.

## Crawlers

- **Python** (`crawl_v4.py`, `crawl_v5.py`) — Original Python crawler
- **Go** (`crawler.go`) — High-performance Go rewrite  
- **Rust** (`rust_crawler/src/main.rs`) — Rust rewrite for maximum throughput

## Upload Pipeline

- `auto_upload.py` — Daemon that watches for completed chunks and uploads to HuggingFace
- `combine_and_upload.py` — Merge and upload utility
- `emergency_upload.py` — Force-upload all chunks before instance expiry

## Seed Generation

- `gen_seeds.py` — Initial seed URL generator
- `generate_mega_seeds.py` — Large-scale seed list builder

## Data

Crawl data chunks uploaded to: [OpenTransformer/web-crawl-2026](https://huggingface.co/datasets/OpenTransformer/web-crawl-2026)

## Infrastructure

Runs on vast.ai GPU instances. Crawlers write `.jsonl.gz` chunks which auto-upload to HuggingFace when complete.
