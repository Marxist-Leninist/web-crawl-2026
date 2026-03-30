import os, time, glob, datetime
from huggingface_hub import HfApi

TOKEN = "HF_TOKEN_REDACTED"
REPO = "OpenTransformer/web-crawl-2026"
STALE_SEC = 300  # 5 min no writes = done

api = HfApi(token=TOKEN)

def log(msg):
    ts = datetime.datetime.utcnow().isoformat()
    print(f"{ts} {msg}", flush=True)

def find_ready_files():
    ready = []
    now = time.time()
    for d in ["/workspace/staging", "/workspace/scraped_data_go", "/workspace/scraped_data_rust", "/workspace/scraped_data"]:
        for f in glob.glob(os.path.join(d, "*.gz")):
            age = now - os.path.getmtime(f)
            sz = os.path.getsize(f)
            if age > STALE_SEC and sz > 1024*1024:  # stale and >1MB
                ready.append((f, sz, age))
                log(f"  Ready: {f} ({sz/(1024*1024):.0f}MB, {age/3600:.1f}h old)")
    return ready

def upload_file(filepath, size):
    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    basename = os.path.basename(filepath)
    remote = f"crawl/combined/{basename.replace(chr(46)+chr(106),chr(95)+ts+chr(46)+chr(106))}"
    log(f"Uploading {basename} ({size/(1024*1024):.0f}MB) -> {remote}")
    try:
        api.upload_file(
            path_or_fileobj=filepath,
            path_in_repo=remote,
            repo_id=REPO,
            repo_type="dataset",
            commit_message=f"Crawl data: {basename} ({size/(1024*1024):.0f}MB)"
        )
        log(f"Uploaded! Removing {filepath}")
        os.remove(filepath)
        return True
    except Exception as e:
        log(f"Upload failed: {e}")
        return False

def combine_and_upload(files):
    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    combined = f"/workspace/crawl_batch_{ts}.jsonl.gz"
    total = sum(s for _, s, _ in files)
    log(f"Combining {len(files)} files ({total/(1024*1024):.0f}MB)")
    with open(combined, "wb") as out:
        for f, _, _ in files:
            with open(f, "rb") as inp:
                while True:
                    chunk = inp.read(8*1024*1024)
                    if not chunk:
                        break
                    out.write(chunk)
    remote = f"crawl/combined/crawl_batch_{ts}.jsonl.gz"
    final = os.path.getsize(combined)
    log(f"Uploading combined {final/(1024*1024):.0f}MB -> {remote}")
    try:
        api.upload_file(
            path_or_fileobj=combined,
            path_in_repo=remote,
            repo_id=REPO,
            repo_type="dataset",
            commit_message=f"Crawl batch {ts} ({final/(1024*1024):.0f}MB, {len(files)} files)"
        )
        log(f"Uploaded! Cleaning up...")
        for f, _, _ in files:
            os.remove(f)
        os.remove(combined)
        return True
    except Exception as e:
        log(f"Upload failed: {e}")
        if os.path.exists(combined):
            os.remove(combined)
        return False

def main():
    log("Upload daemon v2 starting")
    while True:
        log("Scanning...")
        ready = find_ready_files()
        if not ready:
            log("No files ready, sleeping 30min")
            time.sleep(1800)
            continue
        
        # If any single file >= 100MB, upload individually
        big = [(f, s, a) for f, s, a in ready if s >= 100*1024*1024]
        small = [(f, s, a) for f, s, a in ready if s < 100*1024*1024]
        
        for f, s, a in big:
            upload_file(f, s)
        
        # Combine small files if total >= 100MB, or if any are >12h old (avoid data loss)
        if small:
            total_small = sum(s for _, s, _ in small)
            max_age = max(a for _, _, a in small)
            if total_small >= 100*1024*1024 or max_age > 43200:  # 12 hours
                if len(small) == 1:
                    upload_file(small[0][0], small[0][1])
                else:
                    combine_and_upload(small)
            else:
                log(f"Small files total {total_small/(1024*1024):.0f}MB, newest {max_age/3600:.1f}h old, waiting")
        
        time.sleep(1800)

if __name__ == "__main__":
    main()
