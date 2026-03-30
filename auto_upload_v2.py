#!/usr/bin/env python3
"""Auto-upload v2: Fast polling + batched uploads.
- Polls every 30s to catch chunks before crawlers delete them
- Moves completed chunks to staging dir immediately
- Combines staged files and uploads when total >= 500MB
"""
import os, time, glob, shutil, gzip, json
from datetime import datetime

HF_TOKEN = 'HF_TOKEN_REDACTED'
REPO = 'OpenTransformer/web-crawl-2026'
STAGING = '/workspace/staging'
STALE_SECS = 120       # 2 min no modification = finished
POLL_SECS = 30          # check every 30s
UPLOAD_THRESHOLD = 1024 * 1024 * 1024  # 1GB before uploading batch

DIRS = {
    '/workspace/scraped_data/': 'python',
    '/workspace/scraped_data_go/': 'go',
    '/workspace/scraped_data_rust/': 'rust',
}

os.makedirs(STAGING, exist_ok=True)

def log(msg):
    print(f'{datetime.utcnow().isoformat()} {msg}', flush=True)

def staging_size():
    total = 0
    for f in glob.glob(os.path.join(STAGING, '*.jsonl.gz')):
        total += os.path.getsize(f)
    return total

def stage_completed_chunks():
    """Move completed (stale) chunks to staging dir."""
    now = time.time()
    moved = 0
    for local_dir, crawler_name in DIRS.items():
        if not os.path.isdir(local_dir):
            continue
        for f in glob.glob(os.path.join(local_dir, '*.jsonl.gz')):
            size = os.path.getsize(f)
            mtime = os.path.getmtime(f)
            age = now - mtime
            size_mb = size / (1024*1024)
            
            if size < 1024 * 1024:  # skip < 1MB (just started)
                continue
            if age < STALE_SECS:  # still being written
                continue
            
            # Move to staging
            dest = os.path.join(STAGING, f'{crawler_name}_{os.path.basename(f)}')
            try:
                shutil.move(f, dest)
                log(f'Staged {os.path.basename(f)} ({size_mb:.0f}MB) -> {os.path.basename(dest)}')
                moved += 1
            except Exception as e:
                log(f'ERROR staging {f}: {e}')
    return moved

def combine_and_upload():
    """Combine staged files into one big file and upload to HF."""
    files = sorted(glob.glob(os.path.join(STAGING, '*.jsonl.gz')))
    if not files:
        return False
    
    total = sum(os.path.getsize(f) for f in files)
    total_mb = total / (1024*1024)
    
    if total < UPLOAD_THRESHOLD:
        log(f'Staging has {total_mb:.0f}MB ({len(files)} files), waiting for {UPLOAD_THRESHOLD//(1024*1024)}MB threshold')
        return False
    
    # Create combined file with timestamp
    ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    combined_path = f'/workspace/combined_crawl_{ts}.jsonl.gz'
    
    log(f'Combining {len(files)} files ({total_mb:.0f}MB) into {os.path.basename(combined_path)}')
    
    with gzip.open(combined_path, 'wb') as out_f:
        for f in files:
            with gzip.open(f, 'rb') as in_f:
                while True:
                    chunk = in_f.read(8 * 1024 * 1024)  # 8MB chunks
                    if not chunk:
                        break
                    out_f.write(chunk)
    
    combined_size = os.path.getsize(combined_path) / (1024*1024)
    log(f'Combined file: {combined_size:.0f}MB')
    
    # Upload to HF
    try:
        from huggingface_hub import HfApi
        api = HfApi(token=HF_TOKEN)
        hf_path = f'crawl/combined/crawl_batch_{ts}.jsonl.gz'
        log(f'Uploading {combined_size:.0f}MB -> {hf_path}')
        api.upload_file(
            path_or_fileobj=combined_path,
            path_in_repo=hf_path,
            repo_id=REPO,
            repo_type='dataset',
            commit_message=f'Add crawl batch {ts} ({combined_size:.0f}MB, {len(files)} chunks)',
        )
        log(f'Upload complete: {hf_path}')
        
        # Clean up staged files and combined file
        for f in files:
            os.remove(f)
        os.remove(combined_path)
        log(f'Cleaned up {len(files)} staged files + combined file')
        return True
    except Exception as e:
        log(f'ERROR uploading: {e}')
        # Keep files for retry
        if os.path.exists(combined_path):
            os.remove(combined_path)  # remove combined but keep staged
        return False

def also_upload_large_singles():
    """Also directly upload any single chunk that's already >= 500MB."""
    now = time.time()
    for local_dir, crawler_name in DIRS.items():
        if not os.path.isdir(local_dir):
            continue
        for f in glob.glob(os.path.join(local_dir, '*.jsonl.gz')):
            size = os.path.getsize(f)
            mtime = os.path.getmtime(f)
            age = now - mtime
            if size >= UPLOAD_THRESHOLD and age >= STALE_SECS:
                size_mb = size / (1024*1024)
                ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                try:
                    from huggingface_hub import HfApi
                    api = HfApi(token=HF_TOKEN)
                    hf_path = f'crawl/{crawler_name}/{os.path.basename(f)}'
                    log(f'Direct upload {os.path.basename(f)} ({size_mb:.0f}MB) -> {hf_path}')
                    api.upload_file(
                        path_or_fileobj=f,
                        path_in_repo=hf_path,
                        repo_id=REPO,
                        repo_type='dataset',
                        commit_message=f'Add {crawler_name} chunk ({size_mb:.0f}MB)',
                    )
                    log(f'Upload complete: {hf_path}')
                    os.remove(f)
                    log(f'Deleted local: {f}')
                except Exception as e:
                    log(f'ERROR direct upload {f}: {e}')

if __name__ == '__main__':
    log('Auto-upload v2 started (30s poll, 1GB batch threshold)')
    while True:
        try:
            moved = stage_completed_chunks()
            if moved > 0:
                log(f'Staged {moved} chunks, staging total: {staging_size()/(1024*1024):.0f}MB')
            
            also_upload_large_singles()
            combine_and_upload()
        except Exception as e:
            log(f'ERROR in cycle: {e}')
        time.sleep(POLL_SECS)
