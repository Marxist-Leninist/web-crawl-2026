import requests
import json
import sys

OUTPUT = '/workspace/mega_seeds.txt'
TARGET = 4_000_000

# Multiple CC index crawls for more coverage
CC_INDICES = [
    'CC-MAIN-2024-10',
    'CC-MAIN-2024-18',
    'CC-MAIN-2024-22',
    'CC-MAIN-2024-26',
    'CC-MAIN-2024-30',
    'CC-MAIN-2024-33',
    'CC-MAIN-2024-38',
    'CC-MAIN-2024-42',
    'CC-MAIN-2024-46',
    'CC-MAIN-2024-51',
    'CC-MAIN-2025-05',
    'CC-MAIN-2025-08',
]

QUERIES = [
    '*.com/*', '*.org/*', '*.net/*', '*.edu/*', '*.gov/*',
    '*.co.uk/*', '*.io/*', '*.info/*', '*.us/*', '*.ca/*',
    '*.au/*', '*.de/*', '*.fr/*', '*.nl/*', '*.se/*',
    '*.ch/*', '*.nz/*', '*.ie/*', '*.in/*', '*.jp/*',
]

urls = set()
session = requests.Session()
session.headers['User-Agent'] = 'Mozilla/5.0'

for idx in CC_INDICES:
    if len(urls) >= TARGET:
        break
    for q in QUERIES:
        if len(urls) >= TARGET:
            break
        api = f'https://index.commoncrawl.org/{idx}-index?url={q}&output=json&limit=15000&fl=url&filter=languages:eng'
        try:
            r = session.get(api, timeout=60)
            if r.status_code != 200:
                continue
            for line in r.text.strip().split('\n'):
                try:
                    rec = json.loads(line)
                    u = rec.get('url', '')
                    if u and u.startswith('http'):
                        urls.add(u)
                except:
                    pass
            print(f'[{idx}] {q}: {len(urls)} total URLs so far', flush=True)
        except Exception as e:
            print(f'[{idx}] {q}: FAILED ({e})', flush=True)

print(f'Writing {len(urls)} URLs to {OUTPUT}...', flush=True)
with open(OUTPUT, 'w') as f:
    for u in urls:
        f.write(u + '\n')
print(f'Done. {len(urls)} unique URLs saved.', flush=True)
