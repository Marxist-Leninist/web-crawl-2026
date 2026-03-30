#!/usr/bin/env python3
"""Auto-upload completed crawl chunks to HuggingFace.
Runs in a loop, checks every 30 min for finished chunks >= 100MB.
A chunk is 'finished' if it hasn't been modified in the last 5 minutes.
"""
import os, time, subprocess, glob, sys
from datetime import datetime

HF_TOKEN = 'HF_TOKEN_REDACTED'
REPO = 'OpenTransformer/web-crawl-2026'
MIN_SIZE_MB = 100
STALE_SECS = 300  # 5 min no modification = finished
CHECK_INTERVAL = 1800  # 30 min

DIRS = {
    '/workspace/scraped_data/': 'data/',
    '/workspace/staging/': 'data/',
    '/workspace/scraped_data_go/': 'data/',
    '/workspace/scraped_data_rust/': 'data/',
}

def log(msg):
    print(f'{datetime.utcnow().isoformat()} {msg}', flush=True)

def upload_file(local_path, hf_path):
    """Upload a single file to HF using huggingface_hub."""
    from huggingface_hub import HfApi
    api = HfApi(token=HF_TOKEN)
    size_mb = os.path.getsize(local_path) / (1024*1024)
    log(f'Uploading {os.path.basename(local_path)} ({size_mb:.0f}MB) -> {hf_path}')
    api.upload_file(
        path_or_fileobj=local_path,
        path_in_repo=hf_path,
        repo_id=REPO,
        repo_type='dataset',
    )
    log(f'Upload complete: {hf_path}')
    return True

def check_and_upload():
    now = time.time()
    uploaded = 0
    for local_dir, hf_prefix in DIRS.items():
        if not os.path.isdir(local_dir):
            continue
        for f in glob.glob(os.path.join(local_dir, '*.jsonl.gz')):
            size = os.path.getsize(f)
            mtime = os.path.getmtime(f)
            age = now - mtime
            size_mb = size / (1024*1024)
            
            # Skip if too small or still being written
            if size_mb < MIN_SIZE_MB:
                log(f'Skip {os.path.basename(f)}: {size_mb:.0f}MB < {MIN_SIZE_MB}MB min')
                continue
            if age < STALE_SECS:
                log(f'Skip {os.path.basename(f)}: still active ({age:.0f}s since mod)')
                continue
            
            # Upload
            hf_path = hf_prefix + os.path.basename(f)
            try:
                upload_file(f, hf_path)
                # Delete after successful upload to save disk
                os.remove(f)
                log(f'Deleted local: {f}')
                uploaded += 1
            except Exception as e:
                log(f'ERROR uploading {f}: {e}')
    return uploaded

if __name__ == '__main__':
    log('Auto-upload daemon started')
    while True:
        try:
            n = check_and_upload()
            log(f'Check complete: {n} files uploaded')
        except Exception as e:
            log(f'ERROR in check cycle: {e}')
        time.sleep(CHECK_INTERVAL)
