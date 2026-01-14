import os
import sys
import hashlib
import requests
from pathlib import Path
from PyQt5.QtGui import QPixmap

def get_base_dir() -> Path:
    """Return the base directory depending on run mode."""
    if getattr(sys, 'frozen', False):
        # Running as PyInstaller EXE
        return Path(sys.executable).resolve().parent
    else:
        # Running as normal .py script
        return Path(__file__).resolve().parent

BASE_DIR = get_base_dir()

# Always anchor cache to BASE_DIR
CACHE_DIR = BASE_DIR / "cache"
os.makedirs(CACHE_DIR, exist_ok=True)

def cache_path_for_url(url: str) -> Path:
    """Return deterministic cache path for a given URL."""
    h = hashlib.sha256(url.encode("utf-8")).hexdigest()
    return CACHE_DIR / f"{h}.bin"

def fetch_bytes_capped(url: str, max_bytes: int = 1024 * 1024) -> bytes:
    """Download up to max_bytes from URL, return as bytes."""
    r = requests.get(url, stream=True, timeout=12)
    r.raise_for_status()
    total = 0
    chunks = []
    for chunk in r.iter_content(8192):
        total += len(chunk)
        if total > max_bytes:
            return None
        chunks.append(chunk)
    return b"".join(chunks)

def load_pixmap_cached(url: str) -> QPixmap:
    """Load QPixmap from cache or download and save if missing."""
    if not url:
        return None
    path = cache_path_for_url(url)
    pm = QPixmap()
    if path.is_file():
        if pm.load(str(path)):
            return pm
    try:
        data = fetch_bytes_capped(url, 1024 * 1024)
    except Exception:
        data = None
    if data and pm.loadFromData(data):
        try:
            with open(path, "wb") as f:
                f.write(data)
        except Exception:
            pass
        return pm
    return None
