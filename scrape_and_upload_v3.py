#!/usr/bin/env python3
"""Download data from HuggingFace datasets and upload to OpenTransformer/web-crawl-2026
V3: Large chunks (1M rows, ~1GB compressed) to reduce number of uploads"""
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
CHUNK_SIZE = 1000000  # 1M rows per chunk (~1GB compressed)
STATE_FILE = "/workspace/scrape_state.json"

os.makedirs(OUTPUT_DIR, exist_ok=True)
login(token=HF_TOKEN)
api = HfApi(token=HF_TOKEN)

SOURCES = [
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
    fsize = os.path.getsize(filepath) / (1024*1024)
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
            print("  Upload attempt %d failed: %s" % (attempt+1, e), flush=True)
            time.sleep(30 * (attempt+1))
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

    start_chunk = state.get(state_key, {}).get("next_chunk_v3", 0)
    skip_rows = state.get(state_key, {}).get("total_rows_v3", 0)
    print("  V3 resuming from chunk %d (skipping %d rows)" % (start_chunk, skip_rows), flush=True)

    try:
        if config:
            ds = load_dataset(name, config, split=split, streaming=True)
        else:
            ds = load_dataset(name, split=split, streaming=True)
    except Exception as e:
        print("  Failed to load: %s" % e, flush=True)
        return

    chunk_num = start_chunk
    total_rows = 0
    skipped = 0
    
    # Stream directly to gzip file to save memory
    chunk_name = "%s_big_chunk%04d.jsonl.gz" % (source_tag, chunk_num)
    chunk_path = os.path.join(OUTPUT_DIR, chunk_name)
    f = gzip.open(chunk_path, "wt", encoding="utf-8")
    rows_in_chunk = 0

    for example in ds:
        if skipped < skip_rows:
            skipped += 1
            if skipped % 1000000 == 0:
                print("  Skipping... %d/%d" % (skipped, skip_rows), flush=True)
            continue

        text = example.get(text_field) or example.get("text") or example.get("content") or ""
        if len(text) < 100:
            continue

        row = json.dumps({
            "text": text,
            "source": name,
            "url": example.get("url", ""),
        }, ensure_ascii=False)
        f.write(row + "\n")
        rows_in_chunk += 1
        total_rows += 1

        if rows_in_chunk % 100000 == 0:
            print("  Chunk %d progress: %dk rows, total: %dk" % (chunk_num, rows_in_chunk//1000, (total_rows+skip_rows)//1000), flush=True)

        if rows_in_chunk >= CHUNK_SIZE:
            f.close()
            print("  Chunk %d complete: %d rows" % (chunk_num, rows_in_chunk), flush=True)

            if upload_chunk(chunk_path, chunk_name):
                os.remove(chunk_path)
                chunk_num += 1
                state[state_key] = state.get(state_key, {})
                state[state_key]["next_chunk_v3"] = chunk_num
                state[state_key]["total_rows_v3"] = total_rows + skip_rows
                save_state(state)
            else:
                print("  Upload failed, will retry next run", flush=True)
                try: os.remove(chunk_path)
                except: pass
                return

            # Start new chunk
            chunk_name = "%s_big_chunk%04d.jsonl.gz" % (source_tag, chunk_num)
            chunk_path = os.path.join(OUTPUT_DIR, chunk_name)
            f = gzip.open(chunk_path, "wt", encoding="utf-8")
            rows_in_chunk = 0

    # Final partial chunk
    f.close()
    if rows_in_chunk > 0:
        print("  Final chunk %d: %d rows" % (chunk_num, rows_in_chunk), flush=True)
        if upload_chunk(chunk_path, chunk_name):
            os.remove(chunk_path)
            chunk_num += 1
            state[state_key] = state.get(state_key, {})
            state[state_key]["next_chunk_v3"] = chunk_num
            state[state_key]["total_rows_v3"] = total_rows + skip_rows
            state[state_key]["done"] = True
            save_state(state)
    else:
        try: os.remove(chunk_path)
        except: pass
        state[state_key] = state.get(state_key, {})
        state[state_key]["done"] = True
        save_state(state)

    print("  Done: %s total rows from %s" % ("{:,}".format(total_rows + skip_rows), name), flush=True)

if __name__ == "__main__":
    print("Web Crawl Data Collector V3 (Large Chunks)", flush=True)
    print("Target: %s" % TARGET_REPO, flush=True)
    print("Chunk size: %d rows" % CHUNK_SIZE, flush=True)
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
