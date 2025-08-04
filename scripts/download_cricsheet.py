import os
import requests
import zipfile
import io

# Base CricSheet URLs
URLS = {
    "test": "https://cricsheet.org/downloads/tests_json.zip",
    "odi": "https://cricsheet.org/downloads/odis_json.zip",
    "t20": "https://cricsheet.org/downloads/t20s_json.zip"
}

DATA_DIR = "data/cricsheet"

def download_and_extract(format_type):
    url = URLS[format_type]
    save_dir = os.path.join(DATA_DIR, format_type)
    os.makedirs(save_dir, exist_ok=True)

    print(f"Downloading {format_type.upper()} data from {url}...")
    r = requests.get(url)
    r.raise_for_status()

    print("Extracting files...")
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        z.extractall(save_dir)
    print(f"Done: {save_dir}")

if __name__ == "__main__":
    for fmt in URLS:
        download_and_extract(fmt)
