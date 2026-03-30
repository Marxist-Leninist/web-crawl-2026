#!/usr/bin/env python3
"""Download data from HuggingFace datasets and upload to OpenTransformer/web-crawl-2026
V2: replaced broken sources, added looping and resume logic"""
import os
import json
import gzip
import time
import traceback
from datasets import load_dataset
from huggingface_hub import HfApi, login

HF_TOKEN = "HF_TOKEN_REDACTED"
TARGET_REPO = "OpenTransformer/web-crawl-2026"
OUTPUT_DIR = "/workspace/scraped_data"
CHUNK_SIZE = 50000
STATE_FILE = "/workspace/scrape_state.json"

os.makedirs(OUTPUT_DIR, exist_ok=True)
login(token=HF_TOKEN)
api = HfApi(token=HF_TOKEN)

# Sources that work with streaming and don't use deprecated dataset scripts
SOURCES = [
    # FineWeb already done (298 chunks), but FineWeb-Edu is separate
    ("HuggingFaceFW/fineweb-edu", "sample-10BT", "train", "text"),
    ("allenai/c4", "en", "train", "text"),
    ("cerebras/SlimPajama-627B", None, "train", "text"),
    ("uonlp/CulturaX", "en", "train", "text"),
]

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)

def upload_chunk(filepath, remote_name):
    for attempt in range(3):
        try:
            api.upload_file(
                path_or_fileobj=filepath,
                path_in_repo="data/" + remote_name,
                repo_id=TARGET_REPO,
                repo_type="dataset",
            )
            print("  Uploaded " + remote_name, flush=True)
            return True
        except Exception as e:
            print("  Upload attempt %d failed: %s" % (attempt+1, e), flush=True)
            time.sleep(10 * (attempt+1))
    return False

def process_source(name, config, split, text_field):
    sep = "=" * 60
    print("\n" + sep, flush=True)
    print("Source: %s (%s)" % (name, config or "default"), flush=True)
    print(sep, flush=True)

    state = load_state()
    source_tag = name.replace("/", "_")
    if config:
        source_tag += "_" + config.replace("-", "_")
    state_key = source_tag

    # Resume from last chunk
    start_chunk = state.get(state_key, {}).get("next_chunk", 0)
    skip_rows = start_chunk * CHUNK_SIZE
    print("  Resuming from chunk %d (skipping %d rows)" % (start_chunk, skip_rows), flush=True)

    try:
        if config:
            ds = load_dataset(name, config, split=split, streaming=True)
        else:
            ds = load_dataset(name, split=split, streaming=True)
    except Exception as e:
        print("  Failed to load: %s" % e, flush=True)
        return False

    batch = []
    chunk_num = start_chunk
    total_rows = 0
    skipped = 0

    for example in ds:
        if skipped < skip_rows:
            skipped += 1
            if skipped % 500000 == 0:
                print("  Skipping... %d/%d" % (skipped, skip_rows), flush=True)
            continue

        text = example.get(text_field) or example.get("text") or example.get("content") or ""
        if len(text) < 100:
            continue

        row = {
            "text": text,
            "source": name,
            "url": example.get("url", ""),
        }
        batch.append(row)
        total_rows += 1

        if len(batch) >= CHUNK_SIZE:
            chunk_name = "%s_chunk%04d.jsonl.gz" % (source_tag, chunk_num)
            chunk_path = os.path.join(OUTPUT_DIR, chunk_name)

            with gzip.open(chunk_path, "wt", encoding="utf-8") as f:
                for item in batch:
                    f.write(json.dumps(item, ensure_ascii=False) + "\n")

            print("  Chunk %d: %s rows total" % (chunk_num, "{:,}".format(total_rows + skip_rows)), flush=True)

            if upload_chunk(chunk_path, chunk_name):
                os.remove(chunk_path)
                chunk_num += 1
                state[state_key] = {"next_chunk": chunk_num}
                save_state(state)
            else:
                print("  Upload failed, will retry next run", flush=True)
                os.remove(chunk_path)
                return False

            batch = []

    # Final partial batch
    if batch:
        chunk_name = "%s_chunk%04d.jsonl.gz" % (source_tag, chunk_num)
        chunk_path = os.path.join(OUTPUT_DIR, chunk_name)
        with gzip.open(chunk_path, "wt", encoding="utf-8") as f:
            for item in batch:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
        if upload_chunk(chunk_path, chunk_name):
            os.remove(chunk_path)
            chunk_num += 1
            state[state_key] = {"next_chunk": chunk_num, "done": True}
            save_state(state)

    print("  Done: %s rows from %s" % ("{:,}".format(total_rows + skip_rows), name), flush=True)
    return False

if __name__ == "__main__":
    print("Web Crawl Data Collector V2", flush=True)
    print("Target: %s" % TARGET_REPO, flush=True)
    start = time.time()

    for name, config, split, text_field in SOURCES:
        state = load_state()
        source_tag = name.replace("/", "_")
        if config:
            source_tag += "_" + config.replace("-", "_")
        if state.get(source_tag, {}).get("done"):
            print("Skipping %s (already done)" % name, flush=True)
            continue
        try:
            process_source(name, config, split, text_field)
        except Exception as e:
            print("Error processing %s: %s" % (name, e), flush=True)
            traceback.print_exc()
            continue

    elapsed = time.time() - start
    print("\nFinished in %.1f hours" % (elapsed/3600), flush=True)
