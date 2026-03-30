import subprocess, os, time, glob

REPO = "OpenTransformer/web-crawl-2026"
files = []

# Get all chunks - upload finished ones first, then active ones (copy-then-upload)
for pattern in ["/workspace/scraped_data_rust/*.jsonl.gz", "/workspace/scraped_data_go/*.jsonl.gz", "/workspace/scraped_data/*.jsonl.gz"]:
    files.extend(glob.glob(pattern))

print(f"Found {len(files)} files to upload")

for f in sorted(files, key=os.path.getsize, reverse=True):
    size_mb = os.path.getsize(f) / 1024 / 1024
    basename = os.path.basename(f)
    
    # For active files, copy first so we upload a consistent snapshot
    mtime_age = time.time() - os.path.getmtime(f)
    if mtime_age < 300:  # modified in last 5 min = still active
        print(f"ACTIVE: {basename} ({size_mb:.0f}MB, {mtime_age:.0f}s ago) - copying snapshot...")
        snap = f"/tmp/snap_{basename}"
        os.system(f"cp {f} {snap}")
        upload_path = snap
    else:
        print(f"DONE: {basename} ({size_mb:.0f}MB)")
        upload_path = f
    
    # Determine HF subfolder
    if "rust" in f:
        hf_path = f"data/{basename}"
    elif "go" in f:
        hf_path = f"data/{basename}"
    else:
        hf_path = f"data/{basename}"
    
    print(f"  Uploading {hf_path}...")
    from huggingface_hub import HfApi
    api = HfApi()
    try:
        api.upload_file(
            path_or_fileobj=upload_path,
            path_in_repo=hf_path,
            repo_id=REPO,
            repo_type="dataset",
        )
        print(f"  OK: {hf_path}")
    except Exception as e:
        print(f"  FAIL: {e}")
    
    # Clean up snapshot
    if upload_path.startswith("/tmp/snap_"):
        os.remove(upload_path)

print("ALL DONE")
