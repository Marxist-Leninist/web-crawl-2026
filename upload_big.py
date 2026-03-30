#!/usr/bin/env python3
"""Upload daemon that accumulates crawl chunks until >= 1GB, then combines and uploads."""
import os, time, glob, datetime, json, shutil
from huggingface_hub import HfApi

TOKEN = 'HF_TOKEN_REDACTED'
REPO = 'OpenTransformer/web-crawl-2026'
MIN_BATCH_SIZE = 1 * 1024 * 1024 * 1024  # 1GB minimum before uploading
MAX_AGE_HOURS = 24  # force upload if oldest file > 24h old even if < 1GB
STALE_SEC = 300  # file must be untouched for 5 min to be considered complete
POLL_SEC = 120  # check every 2 minutes
DIRS = ['/workspace/scraped_data_go', '/workspace/scraped_data', '/workspace/staging', '/workspace/scraped_data_rust']
STATE_FILE = '/workspace/upload_big_state.json'

api = HfApi(token=TOKEN)

def log(msg):
    ts = datetime.datetime.utcnow().isoformat()
    print(f'{ts} {msg}', flush=True)

def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except:
        return {'uploaded': [], 'total_uploaded_bytes': 0}

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

def find_ready_files():
    """Find .gz files that are stale (not being written to)."""
    ready = []
    now = time.time()
    state = load_state()
    uploaded_set = set(state.get('uploaded', []))
    for d in DIRS:
        if not os.path.isdir(d):
            continue
        for f in sorted(glob.glob(os.path.join(d, '*.gz'))):
            basename = os.path.basename(f)
            if basename in uploaded_set:
                continue
            try:
                sz = os.path.getsize(f)
                age = now - os.path.getmtime(f)
            except OSError:
                continue
            if sz < 1024 * 1024:  # skip < 1MB
                continue
            if age < STALE_SEC:  # still being written
                continue
            ready.append((f, sz, age))
    return ready

def combine_files(files, output_path):
    """Concatenate gzip files into one."""
    with open(output_path, 'wb') as out:
        for fpath, _, _ in files:
            with open(fpath, 'rb') as inp:
                shutil.copyfileobj(inp, out, length=8*1024*1024)
    return os.path.getsize(output_path)

def main():
    log('Big upload daemon started (1GB batches, 2min poll)')
    while True:
        ready = find_ready_files()
        if not ready:
            log('No ready files, sleeping...')
            time.sleep(POLL_SEC)
            continue

        total_ready = sum(sz for _, sz, _ in ready)
        max_age_h = max(age for _, _, age in ready) / 3600
        log(f'Found {len(ready)} ready files, total {total_ready/(1024*1024):.0f}MB, oldest {max_age_h:.1f}h')

        # Upload if total >= 1GB OR oldest file > 24h
        if total_ready < MIN_BATCH_SIZE and max_age_h < MAX_AGE_HOURS:
            log(f'Waiting for more data ({total_ready/(1024*1024*1024):.2f}GB / 1GB minimum, oldest {max_age_h:.1f}h / {MAX_AGE_HOURS}h max)')
            time.sleep(POLL_SEC)
            continue

        # Combine and upload
        ts = datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        combined_path = f'/workspace/crawl_batch_{ts}.jsonl.gz'
        
        log(f'Combining {len(ready)} files into {combined_path}...')
        combined_size = combine_files(ready, combined_path)
        combined_gb = combined_size / (1024*1024*1024)
        
        remote_name = f'crawl_batch_{ts}_{combined_gb:.1f}GB.jsonl.gz'
        remote_path = f'crawl/combined/{remote_name}'
        
        log(f'Uploading {combined_gb:.2f}GB -> {remote_path}')
        try:
            api.upload_file(
                path_or_fileobj=combined_path,
                path_in_repo=remote_path,
                repo_id=REPO,
                repo_type='dataset',
                commit_message=f'Crawl batch {ts} ({combined_gb:.1f}GB, {len(ready)} chunks, {sum(1 for _ in ready)} files)',
            )
            log(f'Upload complete! Cleaning up local files...')
            
            state = load_state()
            for fpath, sz, _ in ready:
                basename = os.path.basename(fpath)
                state['uploaded'].append(basename)
                state['total_uploaded_bytes'] = state.get('total_uploaded_bytes', 0) + sz
                try:
                    os.remove(fpath)
                    log(f'  Deleted {basename}')
                except:
                    pass
            save_state(state)
            
            # Remove combined file
            try:
                os.remove(combined_path)
            except:
                pass
            
            log(f'Batch upload done! Total uploaded so far: {state[total_uploaded_bytes]/(1024*1024*1024):.2f}GB')
        except Exception as e:
            log(f'Upload FAILED: {e}')
            try:
                os.remove(combined_path)
            except:
                pass
        
        time.sleep(POLL_SEC)

if __name__ == '__main__':
    main()
