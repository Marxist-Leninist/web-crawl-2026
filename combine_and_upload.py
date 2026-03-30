import os, glob, time
from huggingface_hub import HfApi

TOKEN = 'HF_TOKEN_REDACTED'
REPO = 'OpenTransformer/web-crawl-2026'
OUT = '/workspace/crawl_mega_20260323.jsonl.gz'

# Gather all .gz files that haven't been uploaded yet
files_to_combine = []

# Combined crawl files
for f in sorted(glob.glob('/workspace/combined_crawl_*.jsonl.gz')):
    files_to_combine.append(f)

# Go chunks
for f in sorted(glob.glob('/workspace/scraped_data_go/*.jsonl.gz')):
    files_to_combine.append(f)

# Rust staging
for f in sorted(glob.glob('/workspace/staging/*.jsonl.gz')):
    files_to_combine.append(f)

print(f'Found {len(files_to_combine)} files to combine')
for f in files_to_combine:
    sz = os.path.getsize(f) / (1024*1024)
    print(f'  {f}: {sz:.0f}MB')

# Concatenate gz files (gz files can be concatenated directly)
total = 0
with open(OUT, 'wb') as out:
    for f in files_to_combine:
        sz = os.path.getsize(f)
        total += sz
        print(f'Appending {f} ({sz/(1024*1024):.0f}MB)...')
        with open(f, 'rb') as inp:
            while True:
                chunk = inp.read(8*1024*1024)
                if not chunk:
                    break
                out.write(chunk)

total_gb = total / (1024*1024*1024)
final_sz = os.path.getsize(OUT) / (1024*1024*1024)
print(f'Combined {len(files_to_combine)} files, total: {total_gb:.2f}GB, output: {final_sz:.2f}GB')

if final_sz < 0.1:
    print('Output too small, skipping upload')
    exit(1)

# Upload
print(f'Uploading {OUT} to {REPO}...')
api = HfApi(token=TOKEN)
api.upload_file(
    path_or_fileobj=OUT,
    path_in_repo=f'crawl/mega/crawl_mega_20260323.jsonl.gz',
    repo_id=REPO,
    repo_type='dataset',
)
print('Upload complete!')

# Clean up combined files after successful upload
os.remove(OUT)
for f in files_to_combine:
    os.remove(f)
    print(f'Removed {f}')
print('Cleanup done')
