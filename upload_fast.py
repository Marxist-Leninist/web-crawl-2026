#!/usr/bin/env python3
import os, time, glob, datetime, json
from huggingface_hub import HfApi

TOKEN = 'HF_TOKEN_REDACTED'
REPO = 'OpenTransformer/web-crawl-2026'
STALE_SEC = 120
POLL_SEC = 30
MIN_SIZE = 50 * 1024 * 1024
DIRS = ['/workspace/scraped_data_go', '/workspace/scraped_data', '/workspace/staging']
UPLOADED_FILE = '/workspace/uploaded_fast.json'
api = HfApi(token=TOKEN)

def log(msg):
    ts = datetime.datetime.utcnow().isoformat()
    print(f'{ts} {msg}', flush=True)

def load_uploaded():
    try:
        with open(UPLOADED_FILE) as f:
            return set(json.load(f))
    except:
        return set()

def save_uploaded(s):
    with open(UPLOADED_FILE, 'w') as f:
        json.dump(list(s), f)

def main():
    log('Fast upload daemon started (30s poll)')
    uploaded = load_uploaded()
    while True:
        now = time.time()
        for d in DIRS:
            if not os.path.isdir(d):
                continue
            for f in glob.glob(os.path.join(d, '*.gz')):
                basename = os.path.basename(f)
                if basename in uploaded:
                    continue
                sz = os.path.getsize(f)
                age = now - os.path.getmtime(f)
                if sz < MIN_SIZE or age < STALE_SEC:
                    continue
                sz_mb = sz / (1024*1024)
                ts = datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                stem = basename.replace('.jsonl.gz', '')
                remote = 'crawl/combined/' + stem + '_' + ts + '.jsonl.gz'
                log('Uploading ' + basename + ' (' + str(int(sz_mb)) + 'MB) -> ' + remote)
                try:
                    api.upload_file(
                        path_or_fileobj=f,
                        path_in_repo=remote,
                        repo_id=REPO,
                        repo_type='dataset',
                        commit_message='Crawl data: ' + basename + ' (' + str(int(sz_mb)) + 'MB)',
                    )
                    log('Upload complete! Deleting local file.')
                    uploaded.add(basename)
                    save_uploaded(uploaded)
                    try:
                        os.remove(f)
                    except:
                        pass
                except Exception as e:
                    log('Upload FAILED: ' + str(e))
        time.sleep(POLL_SEC)

if __name__ == '__main__':
    main()
