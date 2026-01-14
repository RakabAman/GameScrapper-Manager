# gui.py
"""
Game Manager GUI Application - Beautified Layout

Main Features:
1. Game database management with CSV/JSON/SQLite import/export
2. Steam metadata scraping and merging
3. Image caching and display with 16:9 aspect ratio
4. Batch operations on selected games
5. PDF/HTML export functionality
6. logger added
7. imprvoed Sanitizer
8. fixed download_all_screenshots cachce update and detection
9. Assets are clickable (urls)
16. Recaching added, Clearing cacghe field for auto redownloading
17. Extended Help menu

Architecture:
- Main window (GameManager) with table view and details panel
- Worker threads for background operations (scraping, image fetching)
- Caching system for images and metadata
- Dialog-based editing and matching
"""

import os
import re
import sys
import json
import csv
import time
import requests
import cache  # your cache.py module
import hashlib
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from utils_sanitize import sanitize_original_title, load_repack_list
from urllib.parse import urlparse

# PyQt5 Imports
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QTableView, QAction, QLabel, QVBoxLayout, QWidget,
    QSplitter, QTextEdit, QLineEdit, QPushButton, QMenu, QDialog,
    QFormLayout, QDialogButtonBox, QMessageBox, QHBoxLayout, QScrollArea,
    QStyledItemDelegate, QInputDialog, QFileDialog, QTextEdit as QTE, QDesktopWidget,
    QSizePolicy, QFrame, QGroupBox, QTabWidget, QProgressBar, QGridLayout,
    QHeaderView, QCheckBox
)
from PyQt5.QtGui import (
    QPixmap, QStandardItemModel, QStandardItem, QColor, QMovie, 
    QDesktopServices, QCursor, QPainter, QFont, QPalette, QBrush,
    QIcon, QFontMetrics
)
from PyQt5.QtCore import (
    Qt, QSortFilterProxyModel, QPoint, QSize, QThread, QObject, 
    pyqtSignal, QTimer, QUrl, QBuffer, QByteArray, QCoreApplication,
    QRect, QMargins
)

from PyQt5.QtMultimedia import QMediaPlayer, QMediaContent
from PyQt5.QtMultimediaWidgets import QVideoWidget

# Local module imports
from import_export import (
    import_csv, import_excel, import_txt,
    save_to_json, load_from_json,
    save_to_sqlite, load_from_sqlite,
    import_file_by_extension, merge_imported_rows,
    game_cache_dir, save_image_bytes, prune_game_cache_dir,
    export_games_to_pdf, export_games_to_html
)
import scraping
import import_export
from match_dialog import MatchDialog

# ============================================================================
# CONSTANTS AND CONFIGURATION
# ============================================================================

STALL_TIMEOUT = 20  # seconds before showing stall warning during batch operations
CACHE_MIN_KB = 10   # Minimum cache size for images (10KB)
CACHE_MAX_KB = 5120 # Maximum cache size for images (5MB)
CHUNK_SIZE = 50     # Number of games to scrape per chunk (moved from scrape_all method)

# ============================================================================
# IMAGE AND MICROTRAILER CONSTANTS
# ============================================================================

MAX_IMAGES_TO_DOWNLOAD = 5   # Maximum screenshots to download per game
MAX_IMAGES_TO_DISPLAY = 5    # Maximum images to display in the viewer for navigation
MAX_MICROTRAILERS = 1        # Always 1 microtrailer per game
MAX_TRAILERS = 3             # Maximum trailer links to show in UI
DEBUG_IMAGES = False         # Set to True to see debug messages
VIDEO_LOOP_ENABLED = True    # Set to True for continuous looping, False for no loop



# ============================================================================
# STYLESHEET CONSTANTS
# ============================================================================

# Modern color palette
PRIMARY_COLOR = "#2c3e50"
SECONDARY_COLOR = "#3498db"
ACCENT_COLOR = "#e74c3c"
SUCCESS_COLOR = "#27ae60"
WARNING_COLOR = "#f39c12"
LIGHT_BG = "#f5f7fa"
DARK_BG = "#34495e"
BORDER_COLOR = "#bdc3c7"
HOVER_COLOR = "#ecf0f1"
SELECTED_COLOR = "#d6eaf8"

# ============================================================================
# CACHE DIRECTORY CONFIGURATION (Simplified)
# ============================================================================

def get_base_dir() -> Path:
    """Get the base directory for the application."""
    if getattr(sys, 'frozen', False):
        # Running as PyInstaller EXE
        return Path(sys.executable).resolve().parent
    else:
        # Running as normal script
        return Path(__file__).resolve().parent

# Get the base directory
BASE_DIR = get_base_dir()

# Default CACHE_DIR to BASE_DIR/cache (ADDED "cache" SUBDIRECTORY)
CACHE_DIR = BASE_DIR / "cache"

# Override with environment variable if set
import os
if "GAME_MANAGER_CACHE_DIR" in os.environ:
    user_cache_dir = Path(os.environ["GAME_MANAGER_CACHE_DIR"])
    if user_cache_dir.is_absolute():
        CACHE_DIR = user_cache_dir
    else:
        CACHE_DIR = BASE_DIR / user_cache_dir

# Set SCRIPT_DIR to CACHE_DIR (this is the key change!)
SCRIPT_DIR = CACHE_DIR

# Update cache module if it exists
if 'cache' in globals() and hasattr(cache, 'CACHE_DIR'):
    cache.CACHE_DIR = CACHE_DIR

# Ensure cache directory exists
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Print for debugging
print(f"[INFO] BASE_DIR: {BASE_DIR}")
print(f"[INFO] CACHE_DIR: {CACHE_DIR}")
print(f"[INFO] SCRIPT_DIR: {SCRIPT_DIR}")

# Ensure cache directory exists
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def _to_relative(path: Path) -> str:
    """Convert absolute path to relative path from SCRIPT_DIR."""
    try:
        return str(path.relative_to(SCRIPT_DIR))
    except ValueError:
        # Path is not relative to SCRIPT_DIR, return absolute path as string
        return str(path)

def _game_cache_dir_for_game(game: dict) -> Path:
    """
    Returns the cache directory path for a specific game.
    
    Strategy:
    1. Use app_id if available for deterministic naming
    2. Fallback to SHA256 hash of title + original_title
    
    Args:
        game: Dictionary containing game data
        
    Returns:
        Path object to the cache directory
    """
    appid = str(game.get("app_id") or "").strip()
    
    if appid:
        sub = f"game_{appid}"
    else:
        # Create deterministic hash from title for games without app_id
        key = (game.get("title") or "") + "|" + (game.get("original_title") or "")
        h = hashlib.sha256(key.encode("utf-8")).hexdigest()[:12]
        sub = f"game_{h}"
    
    cache_dir = CACHE_DIR / sub  # This creates: CACHE_DIR/game_xxxx
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def _save_bytes_to_game_cache(game: dict, url: str, data: bytes) -> Path:
    """
    Save image/video bytes to game-specific cache directory with size validation.
    Returns a relative path for portability.
    
    Enhanced to:
    1. Detect proper file extensions from URL and content-type
    2. Handle microtrailers (GIFs, MP4s, WebMs) specially
    3. Return consistent relative paths
    """
    if not isinstance(data, (bytes, bytearray)):
        raise TypeError(f"Expected bytes for data, got {type(data)}")
    
   
    # Validate size constraints
    min_bytes = CACHE_MIN_KB * 1024
    max_bytes = CACHE_MAX_KB * 1024 if CACHE_MAX_KB else None
    data_len = len(data)
    
    if data_len < min_bytes:
        raise ValueError(f"Data too small ({data_len} bytes < {min_bytes} bytes)")
    if max_bytes and data_len > max_bytes:
        raise ValueError(f"Data too large ({data_len} bytes > {max_bytes} bytes)")
    
    # Determine cache directory
    cache_dir = _game_cache_dir_for_game(game)
    
    # ====================================================================
    # DETERMINE PROPER FILE EXTENSION
    # ====================================================================
    parsed = urlparse(url)
    path = parsed.path.lower()
    
    # Check for microtrailer indicators in URL
    is_microtrailer = any(keyword in url.lower() for keyword in 
                         ['microtrailer', 'trailer', 'video'])
    
    # Determine extension based on URL and content
    ext = ".bin"  # Default fallback
    
    # Check URL for known extensions
    if '.jpg' in path or '.jpeg' in path:
        ext = '.jpg'
    elif '.png' in path:
        ext = '.png'
    elif '.gif' in path:
        ext = '.gif'
        is_microtrailer = True
    elif '.webp' in path:
        ext = '.webp'
    elif '.webm' in path:
        ext = '.webm'
        is_microtrailer = True
    elif '.mp4' in path:
        ext = '.mp4'
        is_microtrailer = True
    
    # If no extension in URL, try to guess from data
    if ext == ".bin":
        # Simple content detection
        if data[:4] == b'\x89PNG':
            ext = '.png'
        elif data[:3] == b'\xff\xd8\xff':
            ext = '.jpg'
        elif data[:6] in [b'GIF87a', b'GIF89a']:
            ext = '.gif'
            is_microtrailer = True
        elif data[:4] == b'RIFF' and data[8:12] == b'WEBP':
            ext = '.webp'
    
    # Generate filename with hash + proper extension
    url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
    filename = f"{url_hash}{ext}"
    target_path = cache_dir / filename
    
    # Skip if already cached
    if target_path.exists():
        # Return relative path from SCRIPT_DIR (not CACHE_DIR)
        try:
            return target_path.relative_to(SCRIPT_DIR)
        except ValueError:
            # Fallback to absolute path if relative fails
            return target_path
    
    # Atomic write with temporary file
    temp_path = target_path.with_suffix(target_path.suffix + ".tmp")
    try:
        temp_path.write_bytes(data)
        temp_path.replace(target_path)
        
        # Show absolute path in console log
        absolute_path = target_path.resolve()
        file_type = "microtrailer" if is_microtrailer else "screenshot"
        print(f"[CACHE] Saved {file_type}: {url} -> {absolute_path} ({data_len} bytes, {ext})")
        
        # Return relative path from SCRIPT_DIR
        try:
            rel_path = target_path.relative_to(SCRIPT_DIR)
            print(f"[CACHE] Relative path: {rel_path}")
            return rel_path
        except ValueError:
            print(f"[WARNING] Could not compute relative path, returning absolute: {target_path}")
            return target_path
        
    except Exception as e:
        # Cleanup on failure
        try:
            if temp_path.exists():
                temp_path.unlink()
        except Exception:
            pass
        raise e

# Application stylesheet
APP_STYLESHEET = f"""
QMainWindow {{
    background-color: {LIGHT_BG};
}}

QWidget {{
    font-family: 'Segoe UI', Arial, sans-serif;
    font-size: 11px;
}}

/* Table Styles */
QTableView {{
    background-color: white;
    border: 1px solid {BORDER_COLOR};
    border-radius: 4px;
    gridline-color: {BORDER_COLOR};
    selection-background-color: {SELECTED_COLOR};
    selection-color: black;
    alternate-background-color: #f9f9f9;
}}

QTableView::item {{
    padding: 4px;
    border-bottom: 1px solid #f0f0f0;
}}

QTableView::item:selected {{
    background-color: {SELECTED_COLOR};
    color: black;
}}

QHeaderView::section {{
    background-color: {PRIMARY_COLOR};
    color: white;
    padding: 6px;
    border: 1px solid {DARK_BG};
    font-weight: bold;
    font-size: 11px;
}}

/* Button Styles */
QPushButton {{
    background-color: {SECONDARY_COLOR};
    color: white;
    border: none;
    border-radius: 4px;
    padding: 6px 12px;
    font-weight: 600;
    min-height: 24px;
}}

QPushButton:hover {{
    background-color: #2980b9;
}}

QPushButton:pressed {{
    background-color: {PRIMARY_COLOR};
}}

QPushButton:disabled {{
    background-color: #95a5a6;
    color: #7f8c8d;
}}

/* Special Buttons */
QPushButton[urgent="true"] {{
    background-color: {ACCENT_COLOR};
}}

QPushButton[urgent="true"]:hover {{
    background-color: #c0392b;
}}

QPushButton[success="true"] {{
    background-color: {SUCCESS_COLOR};
}}

QPushButton[success="true"]:hover {{
    background-color: #229954;
}}

/* Line Edit Styles */
QLineEdit {{
    border: 1px solid {BORDER_COLOR};
    border-radius: 4px;
    padding: 6px;
    background-color: white;
    selection-background-color: {SELECTED_COLOR};
}}

QLineEdit:focus {{
    border: 2px solid {SECONDARY_COLOR};
    padding: 5px;
}}

QLineEdit[error="true"] {{
    border: 2px solid {ACCENT_COLOR};
}}

/* Text Edit Styles */
QTextEdit {{
    border: 1px solid {BORDER_COLOR};
    border-radius: 4px;
    padding: 6px;
    background-color: white;
}}

QTextEdit:focus {{
    border: 2px solid {SECONDARY_COLOR};
    padding: 5px;
}}

/* Group Box Styles */
QGroupBox {{
    font-weight: bold;
    border: 2px solid {BORDER_COLOR};
    border-radius: 6px;
    margin-top: 12px;
    padding-top: 10px;
    background-color: white;
}}

QGroupBox::title {{
    subcontrol-origin: margin;
    left: 10px;
    padding: 0 8px 0 8px;
    color: {PRIMARY_COLOR};
}}

/* Tab Widget Styles */
QTabWidget::pane {{
    border: 1px solid {BORDER_COLOR};
    border-radius: 4px;
    background-color: white;
}}

QTabBar::tab {{
    background-color: #ecf0f1;
    border: 1px solid {BORDER_COLOR};
    border-bottom: none;
    border-top-left-radius: 4px;
    border-top-right-radius: 4px;
    padding: 6px 12px;
    margin-right: 2px;
}}

QTabBar::tab:selected {{
    background-color: white;
    border-bottom: 2px solid {SECONDARY_COLOR};
    font-weight: bold;
}}

QTabBar::tab:hover {{
    background-color: {HOVER_COLOR};
}}

/* Splitter Styles */
QSplitter::handle {{
    background-color: {BORDER_COLOR};
    width: 4px;
    height: 4px;
}}

QSplitter::handle:hover {{
    background-color: {SECONDARY_COLOR};
}}

/* Scroll Area */
QScrollArea {{
    border: none;
    background-color: transparent;
}}

QScrollBar:vertical {{
    border: none;
    background-color: #f0f0f0;
    width: 12px;
    border-radius: 6px;
}}

QScrollBar::handle:vertical {{
    background-color: #c0c0c0;
    border-radius: 6px;
    min-height: 20px;
}}

QScrollBar::handle:vertical:hover {{
    background-color: #a0a0a0;
}}

QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
    border: none;
    background: none;
}}

/* Progress Bar */
QProgressBar {{
    border: 1px solid {BORDER_COLOR};
    border-radius: 4px;
    text-align: center;
    background-color: white;
}}

QProgressBar::chunk {{
    background-color: {SUCCESS_COLOR};
    border-radius: 3px;
}}

/* Status Bar */
QStatusBar {{
    background-color: {PRIMARY_COLOR};
    color: white;
    border-top: 1px solid {DARK_BG};
}}

QStatusBar QLabel {{
    color: white;
    padding: 0 8px;
    border-right: 1px solid rgba(255, 255, 255, 0.2);
}}

/* Menu Bar */
QMenuBar {{
    background-color: {PRIMARY_COLOR};
    color: white;
    border-bottom: 1px solid {DARK_BG};
}}

QMenuBar::item {{
    background-color: transparent;
    padding: 4px 10px;
}}

QMenuBar::item:selected {{
    background-color: {SECONDARY_COLOR};
    border-radius: 2px;
}}

# In the APP_STYLESHEET, update the QMenu section:
QMenu {{
    background-color: white;
    border: 1px solid {BORDER_COLOR};
    border-radius: 4px;
    color: #2c3e50;  # ADD THIS LINE - Set text color to dark
}}

QMenu::item {{
    padding: 6px 24px 6px 20px;
    color: #2c3e50;  # ADD THIS LINE - Ensure item text is dark
}}

QMenu::item:selected {{
    background-color: {SELECTED_COLOR};
    color: #2c3e50;  # Keep text dark when selected
}}

QMenu::separator {{
    height: 1px;
    background-color: {BORDER_COLOR};
    margin: 4px 0;
}}

/* Dialog Styles */
QDialog {{
    background-color: {LIGHT_BG};
}}

QDialogButtonBox {{
    background-color: transparent;
}}

/* Label Styles */
QLabel[title="true"] {{
    font-size: 14px;
    font-weight: bold;
    color: {PRIMARY_COLOR};
    padding: 4px 0;
}}

QLabel[subtitle="true"] {{
    font-size: 12px;
    font-weight: 600;
    color: {DARK_BG};
    padding: 2px 0;
}}

/* Frame Styles */
QFrame[separator="true"] {{
    border: 1px solid {BORDER_COLOR};
    border-radius: 1px;
}}

QFrame[panel="true"] {{
    background-color: white;
    border: 1px solid {BORDER_COLOR};
    border-radius: 6px;
    padding: 8px;
}}
"""

# ============================================================================
# WORKER CLASSES (Background Operations)
# ============================================================================

class ImageFetchWorker(QObject):
    """
    Worker thread for downloading and caching images.
    
    Emits:
        finished(row_index, url, saved_path): When image is successfully cached
        error(row_index, url, error_msg): When fetch fails
    """
    
    finished = pyqtSignal(int, str, str)  # row_index, url, saved_path
    error = pyqtSignal(int, str, str)     # row_index, url, error_msg
    
    def __init__(self, row_index: int, url: str, game: dict = None, parent=None):
        super().__init__(parent)
        self.row_index = row_index
        self.url = (url or "").strip()
        self.game = game or {}
        self.cancelled = False
        
    def run(self):
        """
        Main worker execution - runs in background thread.
        """
        try:
            # Validate URL
            if not self.url:
                self.error.emit(self.row_index, self.url, "Empty URL")
                return
            
            # Normalize URL format
            url = self.url
            if url.startswith("//"):
                url = "https:" + url
            
            # Check if already cached in game data
            if self._is_already_cached(url):
                # Get existing cache path
                cache_path = self._get_existing_cache_path(url)
                if cache_path:
                    print(f"[CACHE HIT] Already in game data: {url}")
                    self.finished.emit(self.row_index, url, str(cache_path))
                    return
            
            # Fetch from network
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) GameScraper/1.0",
                "Accept": "image/webp,image/*,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5"
            }
            
            try:
                with requests.get(url, stream=True, timeout=30, headers=headers) as response:
                    response.raise_for_status()
                    chunks = []
                    for chunk in response.iter_content(chunk_size=8192):
                        if self.cancelled:
                            self.error.emit(self.row_index, self.url, "Cancelled")
                            return
                        if chunk:
                            chunks.append(chunk)
                    data = b"".join(chunks)
            except Exception as e:
                self.error.emit(self.row_index, self.url, f"Download failed: {str(e)}")
                return
            
            if not data:
                self.error.emit(self.row_index, self.url, "No data fetched")
                return
            
            # Save to cache
            try:
                saved_path = _save_bytes_to_game_cache(self.game, url, data)
                
                # Ensure saved_path is a string (Path objects can cause issues in signals)
                if hasattr(saved_path, 'as_posix'):
                    saved_path_str = saved_path.as_posix()
                else:
                    saved_path_str = str(saved_path)
                
                self.finished.emit(self.row_index, url, saved_path_str)
                
            except Exception as e_save:
                self.error.emit(self.row_index, self.url, f"Save failed: {e_save}")
                
        except Exception as e:
            self.error.emit(self.row_index, self.url, str(e))
    
    def _is_already_cached(self, url: str) -> bool:
        """Check if URL is already cached in game data."""
        # Generate hash for this URL
        url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
        
        # Check screenshot cache paths
        screenshot_paths = self.game.get("screenshot_cache_paths", [])
        for path in screenshot_paths:
            if path and url_hash in Path(path).stem:
                return True
        
        # Check microtrailer cache path
        microtrailer_path = self.game.get("microtrailer_cache_path", "")
        if microtrailer_path and url_hash in Path(microtrailer_path).stem:
            return True
        
        return False
    
    def _get_existing_cache_path(self, url: str) -> Optional[Path]:
        """Get existing cache path for URL from game data."""
        url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
        
        # Check screenshot cache paths
        screenshot_paths = self.game.get("screenshot_cache_paths", [])
        for path_str in screenshot_paths:
            if path_str and url_hash in Path(path_str).stem:
                return Path(path_str)
        
        # Check microtrailer cache path
        microtrailer_path = self.game.get("microtrailer_cache_path", "")
        if microtrailer_path and url_hash in Path(microtrailer_path).stem:
            return Path(microtrailer_path)
        
        return None

class ScrapeBatchWorker(QObject):
    """
    Worker for batch scraping multiple games in background.
    
    Emits:
        progress(message): Status updates
        row_started(row_index, total, title): When starting a row
        row_finished(row_index, metadata): When row completes
        finished(total_processed): When batch completes
        error(message): On fatal error
    """
    
    progress = pyqtSignal(str)
    row_started = pyqtSignal(int, int, str)  # row_index, total, title
    row_finished = pyqtSignal(int, dict)     # row_index, metadata/candidates
    finished = pyqtSignal(int)               # total processed
    error = pyqtSignal(str)
    
    def __init__(self, rows_to_process: List[int], games_ref: List[Dict], parent=None):
        super().__init__(parent)
        self.rows = list(rows_to_process)
        self.games_ref = games_ref  # Reference to main games list
        self.cancelled = False
    
    def run(self):
        """
        Process each row sequentially with throttling to avoid rate limiting.
        """
        processed = 0
        total = len(self.rows)
        
        try:
            for idx, row_index in enumerate(self.rows, start=1):
                # Check for cancellation
                if self.cancelled:
                    self.progress.emit("Batch scrape cancelled by user.")
                    break
                
                # Validate row index
                if row_index < 0 or row_index >= len(self.games_ref):
                    continue
                
                game = self.games_ref[row_index]
                title = game.get("title") or game.get("original_title") or ""
                self.row_started.emit(row_index, total, title)
                
                # Skip if already has app_id
                appid = str(game.get("app_id") or "").strip()
                if appid:
                    self.row_finished.emit(row_index, {})
                    processed += 1
                    time.sleep(0.05)  # Small delay for UI responsiveness
                    continue
                
                # FIX: Clean, simple scraping logic without duplicates
                try:
                    print(f"[SCRAPE_WORKER] Scraping row {row_index}: '{title}'")
                    
                    # Call scrape_igdb_then_steam only once
                    meta = scraping.scrape_igdb_then_steam(
                        None,  # igdb_id - will be auto-detected
                        title,
                        auto_accept_score=92,
                        fetch_pcgw_save=False
                    ) or {}
                    
                    print(f"[SCRAPE_WORKER] Row {row_index} returned {len(meta)} metadata fields")
                    
                    # Check what we got back
                    if meta:
                        # If we have candidates, emit as candidates
                        if "__candidates__" in meta:
                            print(f"[SCRAPE_WORKER] Row {row_index} has {len(meta['__candidates__'])} candidates")
                            self.row_finished.emit(row_index, meta)
                        else:
                            # We have actual metadata - check if it's valid
                            has_data = any(meta.get(k) for k in ['app_id', 'developer', 'publisher', 'genres'])
                            if has_data:
                                print(f"[SCRAPE_WORKER] Row {row_index} has valid metadata with keys: {list(meta.keys())}")
                                if meta.get('app_id'):
                                    print(f"[SCRAPE_WORKER]   Found app_id: {meta.get('app_id')}")
                                if meta.get('developer'):
                                    print(f"[SCRAPE_WORKER]   Found developer: {meta.get('developer')}")
                                self.row_finished.emit(row_index, meta)
                            else:
                                # No valid data, treat as empty
                                print(f"[SCRAPE_WORKER] Row {row_index} returned empty/invalid metadata")
                                self.row_finished.emit(row_index, {})
                    else:
                        print(f"[SCRAPE_WORKER] Row {row_index} returned empty metadata")
                        self.row_finished.emit(row_index, {})
                    
                except Exception as e:
                    print(f"[SCRAPE_WORKER] Error scraping row {row_index}: {e}")
                    # Fallback to just getting IGDB candidates
                    try:
                        candidates = scraping.find_candidates_for_title_igdb(title, max_candidates=8)
                        self.row_finished.emit(row_index, {"__candidates__": candidates})
                    except Exception as e2:
                        print(f"[SCRAPE_WORKER] Error getting candidates: {e2}")
                        self.row_finished.emit(row_index, {"__candidates__": []})
                
                processed += 1
                time.sleep(0.12)  # Throttle to avoid overwhelming APIs
                    
        except Exception as e:
            print(f"[SCRAPE_WORKER] Fatal error: {e}")
            self.error.emit(str(e))
        finally:
            print(f"[SCRAPE_WORKER] Finished processing {processed} rows")
            self.finished.emit(processed)

# ============================================================================
# DIALOG CLASSES (BEAUTIFIED)
# ============================================================================

class MultiEditDialog(QDialog):
    """
    Dialog for editing multiple selected games simultaneously.
    
    Fields are optional - leave empty to skip updating that field.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Multi-edit Selected Games")
        self.setMinimumWidth(500)
        self.setStyleSheet(APP_STYLESHEET)
        self._build_ui()
    
    def _build_ui(self):
        """Create dialog layout and widgets."""
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(20, 20, 20, 20)
        main_layout.setSpacing(15)
        
        # Header
        header_label = QLabel("Edit Multiple Games")
        header_label.setProperty("title", True)
        main_layout.addWidget(header_label)
        
        instruction_label = QLabel("Leave fields empty to skip updating them. Changes apply to all selected games.")
        instruction_label.setWordWrap(True)
        instruction_label.setStyleSheet("color: #7f8c8d; font-style: italic;")
        main_layout.addWidget(instruction_label)
        
        # Create scroll area for form
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setFrameShape(QFrame.NoFrame)
        
        form_widget = QWidget()
        form_layout = QFormLayout(form_widget)
        form_layout.setContentsMargins(10, 10, 10, 10)
        form_layout.setSpacing(10)
        
        # Create input fields with tooltips
        self.game_drive = QLineEdit()
        self.game_drive.setPlaceholderText("e.g., D:/Games")
        self.game_drive.setToolTip("Leave empty to skip updating game drive")
        
        self.scene_repack = QLineEdit()
        self.scene_repack.setPlaceholderText("e.g., FitGirl Repack")
        self.scene_repack.setToolTip("Leave empty to skip updating scene/repack")
        
        self.game_modes = QLineEdit()
        self.game_modes.setPlaceholderText("e.g., Single-player, Multiplayer")
        self.game_modes.setToolTip("Leave empty to skip updating game modes")
        
        self.patch_version = QLineEdit()
        self.patch_version.setPlaceholderText("e.g., v1.5.3")
        self.patch_version.setToolTip("Leave empty to skip updating patch version")
        
        self.played = QLineEdit()
        self.played.setPlaceholderText("Yes/No or True/False")
        self.played.setToolTip("Enter Yes/No, True/False, 1/0 or leave empty to skip")
        
        self.save_location = QTE()
        self.save_location.setFixedHeight(100)
        self.save_location.setPlaceholderText("Enter save location path...")
        self.save_location.setToolTip("Leave empty to skip updating save location")
        
        # Add fields to form with better labels
        form_layout.addRow(self._create_form_label("Game Drive:"), self.game_drive)
        form_layout.addRow(self._create_form_label("Scene/Repack:"), self.scene_repack)
        form_layout.addRow(self._create_form_label("Game Modes:"), self.game_modes)
        form_layout.addRow(self._create_form_label("Patch Version:"), self.patch_version)
        form_layout.addRow(self._create_form_label("Played Status:"), self.played)
        form_layout.addRow(self._create_form_label("Save Location:"), self.save_location)
        
        scroll_area.setWidget(form_widget)
        main_layout.addWidget(scroll_area)
        
        # Add buttons with better styling
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        
        apply_btn = QPushButton("Apply Changes")
        apply_btn.setProperty("success", True)
        apply_btn.clicked.connect(self.accept)
        
        button_layout.addWidget(cancel_btn)
        button_layout.addWidget(apply_btn)
        main_layout.addLayout(button_layout)
    
    def _create_form_label(self, text: str) -> QLabel:
        """Create styled form label."""
        label = QLabel(text)
        label.setMinimumWidth(120)
        label.setStyleSheet("font-weight: 600; color: #2c3e50;")
        return label
    
    def result(self) -> dict:
        """
        Parse dialog inputs and return as dictionary.
        
        Returns:
            Dictionary with field names as keys and values (or None if empty)
        """
        played_text = self.played.text().strip().lower()
        played_val = None
        
        # Parse played status
        if played_text in ("yes", "y", "true", "1"):
            played_val = True
        elif played_text in ("no", "n", "false", "0"):
            played_val = False
        
        return {
            "game_drive": self.game_drive.text().strip() or None,
            "scene_repack": self.scene_repack.text().strip() or None,
            "game_modes": self.game_modes.text().strip() or None,
            "patch_version": self.patch_version.text().strip() or None,
            "played": played_val,
            "save_location": self.save_location.toPlainText().strip() or None
        }

class EditDialog(QDialog):
    """
    Dialog for editing a single game's details.
    
    Shows all available fields for comprehensive editing.
    """
    def __init__(self, game: dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Edit Game: {game.get('title', 'Untitled')}")
        self.setMinimumSize(700, 800)
        self.setStyleSheet(APP_STYLESHEET)
        self.game = dict(game)  # Copy to avoid mutating original
        self._build_ui()
    
    def _build_ui(self):
        """Create comprehensive edit form."""
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(15, 15, 15, 15)
        main_layout.setSpacing(15)
        
        # Header
        header_label = QLabel(f"Editing: {self.game.get('title', 'Untitled Game')}")
        header_label.setProperty("title", True)
        main_layout.addWidget(header_label)
        
        # Create tab widget for organized editing
        tab_widget = QTabWidget()
        
        # Basic Info Tab
        basic_tab = QWidget()
        basic_layout = QFormLayout(basic_tab)
        basic_layout.setContentsMargins(20, 20, 20, 20)
        basic_layout.setSpacing(12)
        
        self.title = self._create_line_edit("title", "Game Title")
        self.appid = self._create_line_edit("app_id", "Steam App ID")
        self.release = self._create_line_edit("release_date", "YYYY-MM-DD")
        self.dev = self._create_line_edit("developer", "Developer")
        self.pub = self._create_line_edit("publisher", "Publisher")
        self.genres = self._create_line_edit("genres", "Action, Adventure, RPG")
        self.original_title = self._create_line_edit("original_title", "Original Title")
        self.game_modes = self._create_line_edit("game_modes", "Single-player, Multiplayer")
        
        basic_layout.addRow(self._create_form_label("Title:"), self.title)
        basic_layout.addRow(self._create_form_label("Steam App ID:"), self.appid)
        basic_layout.addRow(self._create_form_label("Release Date:"), self.release)
        basic_layout.addRow(self._create_form_label("Developer:"), self.dev)
        basic_layout.addRow(self._create_form_label("Publisher:"), self.pub)
        basic_layout.addRow(self._create_form_label("Genres:"), self.genres)
        basic_layout.addRow(self._create_form_label("Original Title:"), self.original_title)
        basic_layout.addRow(self._create_form_label("Game Modes:"), self.game_modes)
        
        tab_widget.addTab(basic_tab, "Basic Info")
        
        # Media Tab
        media_tab = QWidget()
        media_layout = QFormLayout(media_tab)
        media_layout.setContentsMargins(20, 20, 20, 20)
        media_layout.setSpacing(12)
        
        self.cover = self._create_line_edit("cover_url", "https://...")
        self.trailer = self._create_line_edit("trailer_webm", "https://...")
        
        media_layout.addRow(self._create_form_label("Cover URL:"), self.cover)
        media_layout.addRow(self._create_form_label("Trailer URL:"), self.trailer)
        
        tab_widget.addTab(media_tab, "Media")
        
        # Links Tab
        links_tab = QWidget()
        links_layout = QFormLayout(links_tab)
        links_layout.setContentsMargins(20, 20, 20, 20)
        links_layout.setSpacing(12)
        
        self.steam = self._create_line_edit("steam_link", "https://store.steampowered.com/app/...")
        self.steamdb = self._create_line_edit("steamdb_link", "https://steamdb.info/app/...")
        self.pcgw = self._create_line_edit("pcgw_link", "https://www.pcgamingwiki.com/wiki/...")
        self.igdb = self._create_line_edit("igdb_link", "https://www.igdb.com/games/...")
        
        links_layout.addRow(self._create_form_label("Steam Link:"), self.steam)
        links_layout.addRow(self._create_form_label("SteamDB Link:"), self.steamdb)
        links_layout.addRow(self._create_form_label("PCGamingWiki:"), self.pcgw)
        links_layout.addRow(self._create_form_label("IGDB Link:"), self.igdb)
        
        tab_widget.addTab(links_tab, "Links")
        
        # Details Tab
        details_tab = QWidget()
        details_layout = QVBoxLayout(details_tab)
        details_layout.setContentsMargins(20, 20, 20, 20)
        details_layout.setSpacing(12)
        
        # Description
        desc_label = QLabel("Description:")
        desc_label.setStyleSheet("font-weight: 600; color: #2c3e50;")
        details_layout.addWidget(desc_label)
        
        self.desc = QTE()
        self.desc.setPlainText(self.game.get("description", ""))
        self.desc.setMinimumHeight(150)
        details_layout.addWidget(self.desc)
        
        # Other details
        other_layout = QFormLayout()
        other_layout.setSpacing(10)
        
        self.save_loc = QTE(self.game.get("save_location", ""))
        self.save_loc.setFixedHeight(80)
        self.game_drive = self._create_line_edit("game_drive", "e.g., D:/Games")
        self.scene_repack = self._create_line_edit("scene_repack", "e.g., FitGirl Repack")
        self.themes = self._create_line_edit("themes", "Fantasy, Sci-fi")
        self.perspective = self._create_line_edit("player_perspective", "First-person, Third-person")
        self.patch_version = self._create_line_edit("patch_version", "v1.5.3")
        
        # Played checkbox
        played_widget = QWidget()
        played_layout = QHBoxLayout(played_widget)
        played_layout.setContentsMargins(0, 0, 0, 0)
        self.played_checkbox = QCheckBox("Mark as played")
        self.played_checkbox.setChecked(self.game.get("played", False))
        played_layout.addWidget(self.played_checkbox)
        played_layout.addStretch()
        
        other_layout.addRow(self._create_form_label("Save Location:"), self.save_loc)
        other_layout.addRow(self._create_form_label("Game Drive:"), self.game_drive)
        other_layout.addRow(self._create_form_label("Scene/Repack:"), self.scene_repack)
        other_layout.addRow(self._create_form_label("Themes:"), self.themes)
        other_layout.addRow(self._create_form_label("Perspective:"), self.perspective)
        other_layout.addRow(self._create_form_label("Patch Version:"), self.patch_version)
        other_layout.addRow(self._create_form_label("Played Status:"), played_widget)
        
        details_layout.addLayout(other_layout)
        
        tab_widget.addTab(details_tab, "Details")
        
        main_layout.addWidget(tab_widget)
        
        # Add action buttons
        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        main_layout.addWidget(buttons)
    
    def _create_line_edit(self, key: str, placeholder: str = "") -> QLineEdit:
        """Create a line edit with current value and placeholder."""
        edit = QLineEdit(str(self.game.get(key, "")))
        edit.setPlaceholderText(placeholder)
        return edit
    
    def _create_form_label(self, text: str) -> QLabel:
        """Create styled form label."""
        label = QLabel(text)
        label.setMinimumWidth(120)
        label.setStyleSheet("font-weight: 600; color: #2c3e50;")
        return label
    
    def result(self) -> dict:
        """
        Get all edited values as dictionary.
        
        Returns:
            Complete game dictionary with updated values
        """
        return {
            "title": self.title.text().strip(),
            "app_id": self.appid.text().strip(),
            "release_date": self.release.text().strip(),
            "developer": self.dev.text().strip(),
            "publisher": self.pub.text().strip(),
            "genres": self.genres.text().strip(),
            "description": self.desc.toPlainText().strip(),
            "cover_url": self.cover.text().strip(),
            "trailer_webm": self.trailer.text().strip(),
            "steam_link": self.steam.text().strip(),
            "steamdb_link": self.steamdb.text().strip(),
            "pcgw_link": self.pcgw.text().strip(),
            "igdb_link": self.igdb.text().strip(),
            "save_location": self.save_loc.toPlainText().strip(),
            "game_drive": self.game_drive.text().strip(),
            "scene_repack": self.scene_repack.text().strip(),
            "game_modes": self.game_modes.text().strip(),
            "original_title": self.original_title.text().strip(),
            "patch_version": self.patch_version.text().strip(),
            "themes": self.themes.text().strip(),
            "player_perspective": self.perspective.text().strip(),
            "played": self.played_checkbox.isChecked()
        }

# ============================================================================
# CUSTOM WIDGET CLASSES (BEAUTIFIED)
# ============================================================================
class AspectRatioWidget(QWidget):
    """
    Container widget that maintains a specific aspect ratio for its child.
    """
    def __init__(self, child_widget: QWidget, parent=None, aspect_w=16, aspect_h=9):
        super().__init__(parent)
        self._child = child_widget
        self._aspect_w = aspect_w
        self._aspect_h = aspect_h
        self._child.setParent(self)
        
        # FIX: Set proper size policy
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        
        # FIX: Create layout without fixed geometry
        self._layout = QVBoxLayout(self)
        self._layout.setContentsMargins(0, 0, 0, 0)
        self._layout.setSpacing(0)
        
        # Add a container widget for the child
        self._child_container = QWidget(self)
        self._child_layout = QVBoxLayout(self._child_container)
        self._child_layout.setContentsMargins(0, 0, 0, 0)
        self._child_layout.addWidget(self._child)
        
        self._layout.addWidget(self._child_container)
        
        # Initial geometry update
        QTimer.singleShot(0, self._update_child_geometry)
    
    def _update_child_geometry(self):
        """
        Calculate and set child geometry to maintain aspect ratio.
        """
        # Get available size
        available_width = max(1, self._child_container.width())
        available_height = max(1, self._child_container.height())
        
        # Calculate target size maintaining aspect ratio
        target_width = available_width
        target_height = int(available_width * self._aspect_h / self._aspect_w)
        
        # Check if height fits
        if target_height > available_height:
            target_height = available_height
            target_width = int(available_height * self._aspect_w / self._aspect_h)
        
        # Center child within container
        x = (available_width - target_width) // 2
        y = (available_height - target_height) // 2
        
        # Set child geometry
        self._child.setGeometry(x, y, target_width, target_height)
    
    def resizeEvent(self, ev):
        """
        Handle resize events to maintain aspect ratio.
        """
        super().resizeEvent(ev)
        self._update_child_geometry()
    
    def showEvent(self, ev):
        """
        Handle show events to initialize geometry.
        """
        super().showEvent(ev)
        QTimer.singleShot(100, self._update_child_geometry)
        
        
class ClickableImageViewer(QLabel):
    """
    Custom QLabel for displaying images WITHOUT click-to-open functionality.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self._url = ""
        self._local_path = ""
        self.setAlignment(Qt.AlignCenter)
        self.setStyleSheet("""
            background-color: #111;
            border-radius: 4px;
        """)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.setCursor(Qt.ArrowCursor)  # Regular arrow cursor, not hand
    
    def set_url(self, url: str, local_path: str = ""):
        """Set URL and local path for reference."""
        self._url = url or ""
        self._local_path = local_path or ""
        # No click tooltip
        self.setToolTip("")
    
    def mousePressEvent(self, ev):
        """Override to do nothing - image is no longer clickable."""
        # Pass event to parent widget
        super().mousePressEvent(ev)
    
    def wheelEvent(self, ev):
        """Delegate wheel events to parent for image navigation."""
        parent = self.parent()
        if hasattr(parent, "on_viewer_wheel"):
            parent.on_viewer_wheel(ev)
        else:
            super().wheelEvent(ev)
    
    def resizeEvent(self, ev):
        """
        Trigger parent to redisplay image with new size constraints.
        """
        parent = self.parent()
        if hasattr(parent, "_display_image") and getattr(parent, "_current_image_index", None) is not None:
            try:
                parent._display_image(parent._current_image_index)
            except Exception:
                pass
        super().resizeEvent(ev)
        
        
class ClickableVideoWidget(QVideoWidget):
    """Clickable video widget that opens network URL when clicked."""
    clicked = pyqtSignal()
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self._url = ""
        self.setCursor(Qt.PointingHandCursor)
        
    def set_url(self, url: str):
        """Set the network URL for this video."""
        self._url = url or ""
        if self._url:
            self.setToolTip(f"Click to open: {self._url}")
        else:
            self.setToolTip("")
    
    def mousePressEvent(self, ev):
        """Open network URL in browser when clicked."""
        if self._url:
            try:
                print(f"[VIDEO CLICK] Opening network URL: {self._url}")
                QDesktopServices.openUrl(QUrl(self._url))
            except Exception as e:
                print(f"[VIDEO CLICK] Error opening URL: {e}")
                try:
                    import webbrowser
                    webbrowser.open(self._url)
                except Exception as e2:
                    print(f"[VIDEO CLICK] Fallback failed: {e2}")
        else:
            super().mousePressEvent(ev)
        
        self.clicked.emit()        

# Add this class definition after the other custom widget classes
class HighlightDelegate(QStyledItemDelegate):
    """Custom delegate for highlighting rows based on game status."""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.parent = parent
        
        # Define VERY LIGHT colors with priority levels
        #self.duplicate_color = QColor(255, 225, 225)      # Very light pink/red for duplicates
        #self.played_color = QColor(235, 255, 235)         # Very light green for played games
        #self.unplayed_color = QColor(235, 245, 255)       # Very light blue for unplayed games
        
                # Even lighter alternative colors (uncomment to use)
        self.duplicate_color = QColor(255, 240, 240)      # Extremely light pink/red
        self.played_color = QColor(245, 255, 245)         # Extremely light green  
        self.unplayed_color = QColor(245, 250, 255)       # Extremely light blue
    
    def paint(self, painter, option, index):
        """
        Custom paint method to apply highlighting with priority.
        """
        # Get the model and game data
        model = index.model()
        
        # Map to source model if using proxy
        if hasattr(self.parent, 'proxy') and isinstance(model, QSortFilterProxyModel):
            source_index = self.parent.proxy.mapToSource(index)
            model = self.parent.model
        else:
            source_index = index
        
        # Get game data
        game = None
        if source_index.isValid():
            row = source_index.row()
            if row < len(self.parent.games):
                game = self.parent.games[row]
        
        # Default painting
        if game:
            # Check for duplicates
            title_val = (game.get("title") or "").strip().lower()
            orig_val = (game.get("original_title") or "").strip().lower()
            steam_val = str(game.get("app_id") or "").strip().lower()
            
            has_title_duplicate = title_val and title_val in getattr(self.parent, '_dup_title_set', set())
            has_original_duplicate = orig_val and orig_val in getattr(self.parent, '_dup_title_set', set())
            has_steam_duplicate = steam_val and steam_val in getattr(self.parent, '_dup_steamid_set', set())
            
            # Determine if this cell should be highlighted as duplicate
            is_duplicate_cell = False
            if self.parent:
                col = source_index.column()
                is_duplicate_cell = (
                    (col == self.parent.COL_TITLE and has_title_duplicate) or
                    (col == self.parent.COL_ORIGINAL and has_original_duplicate) or
                    (col == self.parent.COL_STEAMID and has_steam_duplicate)
                )
            
            # Apply background with priority
            if is_duplicate_cell:
                # Highest priority: duplicate
                painter.fillRect(option.rect, self.duplicate_color)
            elif game.get("played", False):
                # Second priority: played games
                painter.fillRect(option.rect, self.played_color)
            else:
                # Third priority: unplayed games
                painter.fillRect(option.rect, self.unplayed_color)
        
        # Call parent paint to draw text and other elements
        super().paint(painter, option, index)
        
# ============================================================================
# MAIN WINDOW CLASS (BEAUTIFIED)
# ============================================================================

class GameManager(QMainWindow):
    """
    Main application window for Game Manager.
    
    Architecture:
    - Left panel: Game table with filtering
    - Right panel: Details, images, and trailers
    - Status bar: Counters and progress indicators
    - Menu bar: File operations and batch actions
    """
    
    # ============================================================================
    # COLUMN CONSTANTS - Map column indices to game dictionary keys
    # ============================================================================
    
    COL_TITLE = 0
    COL_VERSION = 1
    COL_GAMEDRIVE = 2
    COL_STEAMID = 3
    COL_PLAYED = 4
    COL_GENRES = 5
    COL_GAME_MODES = 6
    COL_RELEASE = 7
    COL_THEMES = 8
    COL_DEV = 9
    COL_PUB = 10
    COL_SCENE = 11
    COL_PERSPECTIVE = 12
    COL_ORIGINAL = 13
    COL_IGDB_ID = 14
    COL_SHORTCUTS = 15
    COL_TRAILER = 16
    COL_STEAMDB = 17
    COL_PCWIKI = 18
    COL_STEAM_LINK = 19
    COL_DESCRIPTION = 20
    COL_IGDB_TRAILERS = 21
    COL_COVER_URL = 22
    COL_MICROTRAILERS = 23
    COL_IMAGE_CACHE_PATHS = 26
    COL_MICROTRAILER_CACHE_PATH = 25
    # Add to column constants (after the existing ones)
    COL_USER_RATING = 29
    COL_SAVE_LOCATION = 27
    
    # Column mapping for data synchronization
    COLUMN_KEYS = {
        0: "title",
        1: "patch_version",   # fallback to original_title_version
        2: "game_drive",
        3: "app_id",
        4: "played",
        5: "genres",
        6: "game_modes",
        7: "release_date",
        8: "themes",
        9: "developer",
        10: "publisher",
        11: "scene_repack",
        12: "player_perspective",
        13: "original_title",
        14: "igdb_id",
        15: "screenshots",
        16: "trailer_webm",
        17: "steamdb_link",
        18: "pcgw_link",
        19: "steam_link",
        20: "description",
        21: "trailers",
        22: "cover_url",
        23: "microtrailers",
        # Add these to COLUMN_KEYS dictionary
        24: "original_title_base",
        25: "original_notes",
        26: "image_cache_paths",
        27: "savegame_location",  # List version
        28: "microtrailer_cache_path",
            # ... existing entries ...
        29: "user_rating",
    }
    
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Game Manager v2.17 (Extended Help menu) By Rakab Aman")
        self.resize(1300, 900)
        self.setStyleSheet(APP_STYLESHEET)
        
        # ========================================================================
        # APPLICATION STATE
        # ========================================================================
        
        self.games: List[Dict] = []  # Main game data storage
        self._threads: List[QThread] = []  # Active background threads
        self._image_threads: List[Tuple[QThread, ImageFetchWorker]] = []  # Image fetch threads
        self._suppress_model_change = False  # Prevent recursive updates
        
        # Caching and filtering state
        self._in_memory_image_cache = {}  # URL -> {pixmap, movie}
        self._image_items: List[Dict] = []  # Current images for display
        self._current_image_index = 0
        self._dup_title_set = set()  # Duplicate titles for highlighting
        self._dup_steamid_set = set()  # Duplicate Steam IDs
        
        # Operation control flags
        self._cancel_current_scrape = False
        self._cancel_batch = False
        
        # Caching and filtering state
        self._in_memory_image_cache = {}  # URL -> {pixmap, movie}
        self._image_items: List[Dict] = []  # Current images for display
        self._current_image_index = 0
        
        # Initialize duplicate sets properly
        self._dup_title_set = set()
        self._dup_steamid_set = set()
        

        # ========================================================================
        # INITIALIZE UI COMPONENTS
        # ========================================================================
        
        self._setup_data_model()
        self._setup_table_view()
        self._setup_top_panel()  # Changed from _setup_filters
        self._setup_buttons()
        self._setup_details_panel()
        self._setup_image_viewer()
        self._setup_trailer_player()
        self._setup_main_layout()
        self._setup_status_bar()
        self._manual_match_queue = []  # List of (row_index, game, candidates)
        self._manual_match_in_progress = False
        # ========================================================================
        # FINAL SETUP
        # ========================================================================
        
        self.build_menus()
        QCoreApplication.instance().aboutToQuit.connect(self._shutdown_workers)
        
        # Center window on screen
        self.center_window()
    
                # Set custom delegate for highlighting
        self.table.setItemDelegate(HighlightDelegate(self))
    
    def center_window(self):
        """Center the window on the screen."""
        frame_geometry = self.frameGeometry()
        center_point = QDesktopWidget().availableGeometry().center()
        frame_geometry.moveCenter(center_point)
        self.move(frame_geometry.topLeft())
    
    # ============================================================================
    # UI SETUP METHODS (BEAUTIFIED)
    # ============================================================================
    
    def _setup_data_model(self):
        """Initialize the data model with column headers - UPDATED with user rating."""
        self.model = QStandardItemModel(0, 30)  # Changed to 30 columns
        self.model.setHorizontalHeaderLabels([
            "Title", "Version", "Game Drive", "Steam ID", "Played", "Genres",
            "Game modes", "Release date", "Themes", "Developer",
            "Publisher", "Scene/Repack", "Player perspective", "Original title",
            "IGDB ID", "Screenshots", "Trailer (micro)", "SteamDB link",
            "PCGamingWiki link", "Steam link",
            "Description", "IGDB trailers", "Cover URL", "Extra microtrailers",
            "Original Title Base", "Original Notes", "Image Cache Paths", 
            "Savegame Locations", "Microtrailers Cache Paths", "User Rating"  # NEW COLUMN
        ])
        self.model.itemChanged.connect(self.on_model_item_changed)
        
        # Setup proxy model for filtering and sorting
        self.proxy = QSortFilterProxyModel(self)
        self.proxy.setSourceModel(self.model)
        self.proxy.setFilterCaseSensitivity(Qt.CaseInsensitive)
        self.proxy.setSortCaseSensitivity(Qt.CaseInsensitive)
        self.proxy.setFilterKeyColumn(-1)  # Search all columns
    
    def _setup_table_view(self):
        """Configure the main game table."""
        self.table = QTableView()
        self.table.setModel(self.proxy)
        self.table.setSortingEnabled(True)
        self.table.setAlternatingRowColors(False)
        self.table.setSelectionBehavior(QTableView.SelectRows)
        self.table.setSelectionMode(QTableView.ExtendedSelection)
        self.table.verticalHeader().setVisible(False)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.table.horizontalHeader().setDefaultAlignment(Qt.AlignLeft)
        
        # Set custom delegate for highlighting
        self.table.setItemDelegate(HighlightDelegate(self))
        
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.open_context_menu)
        self.table.selectionModel().selectionChanged.connect(
            lambda s, d: self._handle_selection_changed(s, d)
        )
        
        # ==============================================
        # REORDER COLUMNS VISUALLY (CHANGE THESE NUMBERS)
        # ==============================================
        # Create your desired visual order
        # Example: Let's move User Rating to position 1 (after Title)
        # This doesn't change the data model, only how it's displayed
        
        # Define your desired visual order of columns
        visual_column_order = [
            self.COL_TITLE,           # 0: Title
            
            self.COL_VERSION,         # 2: Version
            self.COL_GAMEDRIVE,       # 3: Game Drive
            self.COL_STEAMID,         # 4: Steam ID
            self.COL_IGDB_ID,         # 15: IGDB ID
            self.COL_PLAYED,          # 5: Played
            self.COL_USER_RATING,     # 1: User Rating (MOVED HERE!)
            self.COL_GENRES,          # 6: Genres
            self.COL_GAME_MODES,      # 7: Game modes
            self.COL_RELEASE,         # 8: Release date
            self.COL_THEMES,          # 9: Themes
            self.COL_DEV,             # 10: Developer
            self.COL_PUB,             # 11: Publisher
            self.COL_SCENE,           # 12: Scene/Repack
            self.COL_PERSPECTIVE,     # 13: Player perspective
            self.COL_ORIGINAL,        # 14: Original title
            self.COL_DESCRIPTION,     # 21: Description
            self.COL_SHORTCUTS,       # 16: Screenshots
            self.COL_TRAILER,         # 17: Trailer (micro)
            self.COL_STEAMDB,         # 18: SteamDB link
            self.COL_PCWIKI,          # 19: PCGamingWiki link
            self.COL_STEAM_LINK,      # 20: Steam link
            
            self.COL_IGDB_TRAILERS,   # 22: IGDB trailers
            self.COL_COVER_URL,       # 23: Cover URL
            self.COL_MICROTRAILERS,   # 24: Extra microtrailers
            self.COL_IMAGE_CACHE_PATHS, # 25: Image Cache Paths
            self.COL_SAVE_LOCATION,   # 26: Savegame Locations
            self.COL_MICROTRAILER_CACHE_PATH, # 27: Microtrailers Cache Paths
            # Note: COL_USER_RATING is already at position 1 above
            # Add any other columns that might be missing from your list
        ]
        
        # Apply the visual order
        for visual_index, logical_index in enumerate(visual_column_order):
            current_visual_index = self.table.horizontalHeader().visualIndex(logical_index)
            if current_visual_index != visual_index:
                self.table.horizontalHeader().moveSection(current_visual_index, visual_index)
        
        # Make columns resizable and set initial widths
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        
        # Set specific column widths if desired
        self.table.setColumnWidth(self.COL_TITLE, 200)
        self.table.setColumnWidth(self.COL_USER_RATING, 80)  # User Rating column
        self.table.setColumnWidth(self.COL_VERSION, 80)
        self.table.setColumnWidth(self.COL_PLAYED, 60)

    
    def _setup_buttons(self):
        """Create action buttons with better styling."""
        # Main button container
        button_container = QWidget()
        button_layout = QHBoxLayout(button_container)
        button_layout.setContentsMargins(0, 0, 0, 0)
        button_layout.setSpacing(8)  # Reduced spacing
        
        # Database buttons
        self.open_db_btn = QPushButton("📂 Open DB")
        self.open_db_btn.setToolTip("Load database from JSON/SQLite")
        self.open_db_btn.clicked.connect(self._load_database_combined_dialog)
        self.open_db_btn.setMinimumWidth(100)
        
        self.save_db_btn = QPushButton("💾 Save DB")
        self.save_db_btn.setToolTip("Save database to JSON/SQLite")
        self.save_db_btn.clicked.connect(self._save_database_combined_dialog)
        self.save_db_btn.setMinimumWidth(100)
        
        # Add separator
        separator1 = QLabel("|")
        separator1.setStyleSheet("color: #bdc3c7; font-weight: bold;")
        
        # Import/Export buttons
        self.import_btn = QPushButton("📥 Import")
        self.import_btn.setToolTip("Import games from file (CSV/TXT/Excel)")
        self.import_btn.clicked.connect(self._import_file_combined_dialog)
        self.import_btn.setMinimumWidth(80)
        
        self.export_btn = QPushButton("📤 Export")
        self.export_btn.setToolTip("Export games to PDF/HTML")
        self.export_btn.clicked.connect(self.export_to_pdf_dialog)
        self.export_btn.setMinimumWidth(80)
        
        # Add separator
        separator2 = QLabel("|")
        separator2.setStyleSheet("color: #bdc3c7; font-weight: bold;")
        
        # Scrape button
        self.scrape_btn = QPushButton("🗲 Scrape Metadata")
        self.scrape_btn.setToolTip("Fetch metadata for all games without Steam IDs")
        self.scrape_btn.clicked.connect(lambda: self.scrape_all(auto_accept_score=92))
        self.scrape_btn.setMinimumWidth(140)
        self.scrape_btn.setProperty("success", True)
        
        # Download button
        self.download_all_btn = QPushButton("⬇ Download Resources")
        self.download_all_btn.setToolTip("Download screenshots and microtrailers for all games")
        self.download_all_btn.clicked.connect(self.download_all_screenshots)
        self.download_all_btn.setMinimumWidth(150)
        
        # Cancel button
        self.cancel_scrape_btn = QPushButton("✕ Cancel")
        self.cancel_scrape_btn.setVisible(False)
        self.cancel_scrape_btn.clicked.connect(self.force_cancel_operation)
        self.cancel_scrape_btn.setMinimumWidth(100)
        self.cancel_scrape_btn.setProperty("urgent", True)
        
        # Add buttons to layout
        button_layout.addWidget(self.open_db_btn)
        button_layout.addWidget(self.save_db_btn)
        button_layout.addWidget(separator1)
        button_layout.addWidget(self.import_btn)
        button_layout.addWidget(self.export_btn)
        button_layout.addWidget(separator2)
        button_layout.addWidget(self.scrape_btn)
        button_layout.addWidget(self.download_all_btn)
        button_layout.addWidget(self.cancel_scrape_btn)
        button_layout.addStretch()
        
        self.button_container = button_container
    
    def _setup_details_panel(self):
        """Create the details display area with better organization."""
        # Main details container
        details_container = QWidget()
        details_layout = QVBoxLayout(details_container)
        details_layout.setContentsMargins(0, 0, 0, 0)
        details_layout.setSpacing(10)
        
        # External Links Group (Moved to top)
        links_group = QGroupBox("External Links")
        links_group.setMaximumHeight(80)
        links_layout = QVBoxLayout(links_group)
        links_layout.setContentsMargins(10, 15, 10, 10)
        
        self.links_label = QLabel("")
        self.links_label.setTextFormat(Qt.RichText)
        self.links_label.setOpenExternalLinks(True)
        self.links_label.setStyleSheet("""
            QLabel {
                background-color: white;
                border: 1px solid #ecf0f1;
                border-radius: 4px;
                padding: 6px;
                font-size: 10px;
            }
        """)
        self.links_label.setWordWrap(True)
        links_layout.addWidget(self.links_label)
        
        # Game Info Group (Will expand)
        game_info_group = QGroupBox("Game Information")
        game_info_layout = QVBoxLayout(game_info_group)
        game_info_layout.setContentsMargins(12, 15, 12, 12)
        
        self.details = QTextEdit()
        self.details.setReadOnly(True)
        self.details.setMinimumHeight(200)
        self.details.setStyleSheet("""
            QTextEdit {
                background-color: white;
                border: 1px solid #ecf0f1;
                border-radius: 4px;
                padding: 8px;
                font-size: 11px;
            }
        """)
        game_info_layout.addWidget(self.details)
        
        # Add groups to main layout
        details_layout.addWidget(links_group)
        details_layout.addWidget(game_info_group, 1)  # Give stretch factor of 1 to expand
        
        self.details_container = details_container
                
        # In the _setup_image_viewer method, change how buttons are created and positioned:
            # In the _setup_image_viewer method, ensure the ClickableImageViewer is properly initialized:
    def _setup_image_viewer(self):
        """Create the image display area with navigation - FIXED."""
        # Image viewer widget (using QGroupBox for consistency)
        self.image_box = QGroupBox("Screenshots")
        image_layout = QVBoxLayout(self.image_box)
        image_layout.setContentsMargins(0, 0, 0, 0)
        image_layout.setSpacing(0)
        
        # Create clickable image viewer
        self.viewer = ClickableImageViewer(self)
        self.viewer.setMinimumSize(1, 1)
        
        # Create a container for the viewer that will maintain aspect ratio
        viewer_container = QWidget()
        viewer_container_layout = QVBoxLayout(viewer_container)
        viewer_container_layout.setContentsMargins(0, 0, 0, 0)
        viewer_container_layout.setSpacing(0)
        
        # Wrap viewer in aspect ratio container
        self._viewer_container = AspectRatioWidget(
            self.viewer, parent=viewer_container, aspect_w=16, aspect_h=9
        )
        
        # Allow the aspect ratio widget to resize freely
        self._viewer_container.setMinimumSize(100, 56)
        self._viewer_container.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        
        viewer_container_layout.addWidget(self._viewer_container)
        
        # FIXED: Create navigation overlay as child of viewer_container
        self.nav_container = QWidget(viewer_container)
        self.nav_container.setStyleSheet("background: transparent;")
        # FIXED: Set to False so buttons can receive mouse events
        self.nav_container.setAttribute(Qt.WA_TransparentForMouseEvents, False)  # Changed to False
        self.nav_container.raise_()  # Bring to front
        
        # Create navigation buttons with proper parent
        self.prev_btn = QPushButton("◀", self.nav_container)
        self.prev_btn.setFixedSize(40, 40)
        self.prev_btn.clicked.connect(self.prev_image)
        self.prev_btn.setEnabled(False)
        
        # Style for navigation buttons
        button_style = """
        QPushButton {
            background-color: rgba(0, 0, 0, 180);
            color: white;
            border: none;
            border-radius: 20px;
            font-size: 16px;
            font-weight: bold;
        }
        QPushButton:hover {
            background-color: rgba(0, 0, 0, 220);
        }
        QPushButton:disabled {
            background-color: rgba(0, 0, 0, 80);
            color: #cccccc;
        }
        """
        
        self.prev_btn.setStyleSheet(button_style)
        
        # FIXED: Add "Open Image" button
        self.open_image_btn = QPushButton("🌐", self.nav_container)
        self.open_image_btn.setFixedSize(40, 40)
        self.open_image_btn.clicked.connect(self.open_current_image_url)
        self.open_image_btn.setEnabled(False)
        self.open_image_btn.setStyleSheet(button_style)
        self.open_image_btn.setToolTip("Open current image in browser")
        
        self.next_btn = QPushButton("▶", self.nav_container)
        self.next_btn.setFixedSize(40, 40)
        self.next_btn.clicked.connect(self.next_image)
        self.next_btn.setEnabled(False)
        self.next_btn.setStyleSheet(button_style)
        
        # Image counter
        self.image_counter = QLabel("No images", self.nav_container)
        self.image_counter.setAlignment(Qt.AlignCenter)
        self.image_counter.setStyleSheet("""
            QLabel {
                background-color: rgba(0, 0, 0, 160);
                color: white;
                font-weight: 600;
                font-size: 11px;
                padding: 4px 8px;
                border-radius: 3px;
            }
        """)
        
        # Add the viewer container to the main layout
        image_layout.addWidget(viewer_container, 1)
        
        # Store reference to viewer_container for positioning
        self._viewer_container_widget = viewer_container
        
        # Connect resize event to reposition buttons
        viewer_container.resizeEvent = self._on_viewer_container_resize
        
        # FIXED: Ensure nav_container is on top
        QTimer.singleShot(100, lambda: self.nav_container.raise_())
    
    def _on_viewer_container_resize(self, event):
        """Handle viewer container resize to reposition navigation buttons."""
        # Call original resize handler if it exists
        QWidget.resizeEvent(self._viewer_container_widget, event)
        
        # Reposition buttons after a small delay to ensure geometry is settled
        QTimer.singleShot(50, self._position_navigation_buttons)
    
    # Update the _setup_trailer_player method to use ClickableVideoWidget:
    def _setup_trailer_player(self):
        """Create trailer playback area without control buttons."""
        # Trailer container
        self.trailer_container = QGroupBox("Trailer Player")
        trailer_layout = QVBoxLayout(self.trailer_container)
        trailer_layout.setContentsMargins(2, 5, 2, 2)
        trailer_layout.setSpacing(8)
        
        # Create a centered container for the video/gif widgets
        media_container = QWidget()
        media_layout = QVBoxLayout(media_container)
        media_layout.setContentsMargins(0, 0, 0, 0)
        media_layout.setAlignment(Qt.AlignCenter)
        
        # Video player - REMOVE fixed sizes
        self.video_widget = ClickableVideoWidget()  # Changed to ClickableVideoWidget
        self.video_widget.setMinimumSize(100, 56)  # Much smaller minimum
        self.video_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.video_widget.setStyleSheet("""
            QVideoWidget {
                background-color: #111;
                border-radius: 4px;
            }
        """)
        self.video_widget.hide()
        
        # GIF player - REMOVE fixed sizes (already uses ClickableImageViewer)
        self.trailer_gif_label = ClickableImageViewer()  # Already clickable
        self.trailer_gif_label.setAlignment(Qt.AlignCenter)
        self.trailer_gif_label.setMinimumSize(100, 56)  # Much smaller minimum
        self.trailer_gif_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.trailer_gif_label.setStyleSheet("""
            QLabel {
                background-color: #111;
                border-radius: 4px;
            }
        """)
        self.trailer_gif_label.hide()
        
        # Add widgets to the centered container
        media_layout.addWidget(self.video_widget)
        media_layout.addWidget(self.trailer_gif_label)
        
        # Add the centered media container to the main layout
        trailer_layout.addWidget(media_container, 1)  # Give stretch factor
        
        # Media player setup
        self.media_player = QMediaPlayer(None, QMediaPlayer.VideoSurface)
        self.media_player.setVideoOutput(self.video_widget)
        self.media_player.setMuted(True)
        self.media_player.mediaStatusChanged.connect(self._on_media_status_changed)
        
        # Store the current trailer URL for clicking
        self._current_trailer_url = ""
       
    def _setup_main_layout(self):
        """Arrange all components in the main window."""
        # Create central widget
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # Main layout
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(5, 5, 5, 5)
        main_layout.setSpacing(5)
        
        # Add top panel (stats + filters)
        main_layout.addWidget(self.top_container)
        
        # Middle section: Splitter for table and details
        splitter = QSplitter(Qt.Horizontal)
        
        # Left panel: Table
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setSpacing(5)
        
        # Table with scroll area
        table_scroll = QScrollArea()
        table_scroll.setWidgetResizable(True)
        table_scroll.setWidget(self.table)
        table_scroll.setFrameShape(QFrame.NoFrame)
        
        left_layout.addWidget(table_scroll, 1)
        left_layout.addWidget(self.button_container)
        
        # Right panel: Details and media (using tab widget)
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(5)
        
        # Create tab widget for right panel
        right_tabs = QTabWidget()
        right_tabs.setDocumentMode(True)
        
        # Details tab
        details_scroll = QScrollArea()
        details_scroll.setWidgetResizable(True)
        details_scroll.setWidget(self.details_container)
        details_scroll.setFrameShape(QFrame.NoFrame)
        right_tabs.addTab(details_scroll, "📋 Details")
        
        # Media tab with splitter for resizable sections
        media_tab = QWidget()
        media_layout = QVBoxLayout(media_tab)
        media_layout.setContentsMargins(0, 0, 0, 0)
        media_layout.setSpacing(0)
        
        # Create vertical splitter
        media_splitter = QSplitter(Qt.Vertical)
        media_splitter.setHandleWidth(2)
        media_splitter.setStyleSheet("""
            QSplitter::handle {
                background-color: #bdc3c7;
                border: 1px solid #95a5a6;
            }
            QSplitter::handle:hover {
                background-color: #3498db;
            }
        """)
        
        # Add widgets to splitter
        media_splitter.addWidget(self.image_box)
        media_splitter.addWidget(self.trailer_container)
        
        # FIX: Set initial 50%/50% split (500 pixels each as initial)
        media_splitter.setSizes([500, 500])  # This will be adjusted when window is shown

        # Store splitter reference for later adjustments
        self.media_splitter = media_splitter

        # FIX: Set stretch factors for better resizing
        media_splitter.setStretchFactor(0, 1)  # Image viewer gets 3/4 of space
        media_splitter.setStretchFactor(1, 1)  # Trailer gets 1/4 of space
        
        # Add splitter to layout
        media_layout.addWidget(media_splitter)
        
        # Store splitter reference for later adjustments
        self.media_splitter = media_splitter
        
        right_tabs.addTab(media_tab, "🎬 Media")
        
        right_layout.addWidget(right_tabs)
        
        # Add panels to splitter
        splitter.addWidget(left_panel)
        splitter.addWidget(right_panel)
        
                # Set initial sizes for 50%/50% split
        splitter.setSizes([int(self.width() * 0.55), int(self.width() * 0.45)])
        
        # FIX: Set stretch factors for horizontal splitter
        splitter.setStretchFactor(0, 1)  # Table gets more space initially
        splitter.setStretchFactor(1, 1)  # Details gets less space initially
        
        main_layout.addWidget(splitter, 1)
        
    def _setup_status_bar(self):
        """Setup status bar with counters and progress."""
        # Create status bar widgets
        self.status = QLabel("Ready")
        self.status.setStyleSheet("padding: 0 8px;")
        
        # Progress bar for long operations
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setMaximumWidth(200)
        
        # Stats labels
        self.total_label = QLabel("Total: 0")
        self.played_label = QLabel("Played: 0")
        self.remaining_label = QLabel("Remaining: 0")
        
        # Add widgets to status bar
        self.statusBar().addWidget(self.status, 1)
        self.statusBar().addWidget(self.progress_bar)
        self.statusBar().addPermanentWidget(self.total_label)
        self.statusBar().addPermanentWidget(self.played_label)
        self.statusBar().addPermanentWidget(self.remaining_label)
        
        # Set status bar style
        self.statusBar().setStyleSheet("""
            QStatusBar {
                background-color: #2c3e50;
                color: white;
                border-top: 1px solid #34495e;
            }
            QStatusBar QLabel {
                color: white;
                padding: 0 8px;
                border-right: 1px solid rgba(255, 255, 255, 0.2);
            }
            QStatusBar QProgressBar {
                border: 1px solid rgba(255, 255, 255, 0.3);
                border-radius: 3px;
                text-align: center;
                color: white;
            }
            QStatusBar QProgressBar::chunk {
                background-color: #3498db;
                border-radius: 2px;
            }
        """)

    # In the _setup_top_panel method, replace it with this updated version:
    # Update the _setup_top_panel method to keep single line layout:
    def _setup_top_panel(self):
        """Create top panel with filters on left and stats cards on right in one line."""
        # Main top container
        top_container = QWidget()
        top_layout = QHBoxLayout(top_container)
        top_layout.setContentsMargins(0, 0, 0, 0)
        top_layout.setSpacing(15)  # ADJUSTABLE: Space between left (filters) and right (stats) sections
        
        # ========= LEFT SIDE: Search and Filters =========
        left_container = QWidget()
        left_layout = QVBoxLayout(left_container)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setSpacing(8)
        
        # Search box
        search_widget = QWidget()
        search_layout = QHBoxLayout(search_widget)
        search_layout.setContentsMargins(0, 0, 0, 0)
        search_layout.setSpacing(8)
        
        search_label = QLabel("🔍")
        search_label.setStyleSheet("font-size: 14px;")
        
        self.search = QLineEdit()
        self.search.setPlaceholderText("Search games...")
        self.search.setClearButtonEnabled(True)
        self.search.textChanged.connect(self.on_search_changed)
        self.search.setMinimumWidth(200)  # ADJUSTABLE: Change this value (was 180)
        self.search.setFixedHeight(24)
        
        search_layout.addWidget(search_label)
        search_layout.addWidget(self.search)
        search_layout.addStretch()
        
        # Filter container
        filter_widget = QWidget()
        filter_layout = QHBoxLayout(filter_widget)
        filter_layout.setContentsMargins(0, 0, 0, 0)
        filter_layout.setSpacing(10)
        
        # Genre filter (shorter)
        genre_container = QWidget()
        genre_layout = QHBoxLayout(genre_container)
        genre_layout.setContentsMargins(0, 0, 0, 0)
        genre_layout.setSpacing(5)
        
        genre_label = QLabel("Genre:")
        genre_label.setStyleSheet("font-weight: 600; color: #2c3e50; min-width: 40px;")
        
        self.genre_filter = QLineEdit()
        self.genre_filter.setPlaceholderText("Genre...")
        self.genre_filter.textChanged.connect(self.apply_filters)
        self.genre_filter.setFixedHeight(22)
        self.genre_filter.setMinimumWidth(90)  # ADJUSTABLE: Change this value (was 100)
        
        genre_layout.addWidget(genre_label)
        genre_layout.addWidget(self.genre_filter)
        
        # Drive filter (shorter)
        drive_container = QWidget()
        drive_layout = QHBoxLayout(drive_container)
        drive_layout.setContentsMargins(0, 0, 0, 0)
        drive_layout.setSpacing(5)
        
        drive_label = QLabel("Drive:")
        drive_label.setStyleSheet("font-weight: 600; color: #2c3e50; min-width: 40px;")
        
        self.game_drive_filter = QLineEdit()
        self.game_drive_filter.setPlaceholderText("Drive...")
        self.game_drive_filter.textChanged.connect(self.apply_filters)
        self.game_drive_filter.setFixedHeight(22)
        self.game_drive_filter.setMinimumWidth(90)  # ADJUSTABLE: Change this value (was 100)
        
        drive_layout.addWidget(drive_label)
        drive_layout.addWidget(self.game_drive_filter)
        
        filter_layout.addWidget(genre_container)
        filter_layout.addWidget(drive_container)
        filter_layout.addStretch()
        
        # Add to left layout
        left_layout.addWidget(search_widget)
        left_layout.addWidget(filter_widget)
        
        # ========= RIGHT SIDE: Stats Cards =========
        stats_container = QWidget()
        stats_container.setMaximumHeight(70)  # ADJUSTABLE: Overall container height
        stats_layout = QHBoxLayout(stats_container)
        stats_layout.setContentsMargins(0, 0, 0, 0)
        stats_layout.setSpacing(6)  # ADJUSTABLE: Space between stat cards
        
        # Create stats cards with colors - 6 cards total
        stats_config = [
            {"id": "total", "title": "Total", "color": "#3498db"},
            {"id": "played", "title": "Played", "color": "#27ae60"},
            {"id": "remaining", "title": "Remaining", "color": "#e74c3c"},
            {"id": "cached", "title": "Cached", "color": "#f39c12"},
            {"id": "duplicates", "title": "Duplicates", "color": "#9b59b6"},
            {"id": "unscraped", "title": "Unscraped", "color": "#e67e22"}
        ]
        
        self.stats_cards = {}
        for stat in stats_config:
            card = QWidget()
            card.setMinimumWidth(75)   # ADJUSTABLE: Minimum width of stat card
            card.setMaximumWidth(85)   # ADJUSTABLE: Maximum width of stat card
            card.setStyleSheet(f"""
                QWidget {{
                    background-color: white;
                    border-radius: 3px;
                    border-left: 2px solid {stat['color']};
                    padding: 1px;
                }}
            """)
            
            card_layout = QVBoxLayout(card)
            card_layout.setContentsMargins(5, 3, 5, 3)  # ADJUSTABLE: (left, top, right, bottom)
            card_layout.setSpacing(1)                    # ADJUSTABLE: Space between title and value
            
            # Title label - smaller
            title_label = QLabel(stat["title"])
            title_label.setStyleSheet("""
                QLabel {
                    font-size: 12px;
                    font-weight: bold;
                    color: #7f8c8d;
                    font-weight: 500;
                }
            """)
            title_label.setAlignment(Qt.AlignCenter)
            
            # Value label - smaller
            value_label = QLabel("0")
            value_label.setStyleSheet("""
                QLabel {
                    font-size: 12px;
                    font-weight: bold;
                    color: #2c3e50;
                }
            """)
            value_label.setAlignment(Qt.AlignCenter)
            value_label.setObjectName(f"stat_{stat['id']}")
            
            card_layout.addWidget(title_label)
            card_layout.addWidget(value_label)
            
            stats_layout.addWidget(card)
            self.stats_cards[stat["id"]] = value_label
        
        # Add containers to main layout (LEFT SIDE NOW GETS STRETCH)
        top_layout.addWidget(left_container, 1)  # Left side stretches
        top_layout.addWidget(stats_container)
        
        self.top_container = top_container         
    
    def resizeEvent(self, event):
        """Handle window resize to update media display."""
        super().resizeEvent(event)
        
        # Update image viewer
        if hasattr(self, '_viewer_container'):
            self._viewer_container._update_child_geometry()
        
        # Wait a bit for geometry to settle, then reposition buttons
        QTimer.singleShot(50, self._position_navigation_buttons)
        
        # Update current image if one is displayed
        if hasattr(self, '_current_image_index') and self._current_image_index is not None:
            try:
                self._display_image(self._current_image_index)
            except Exception:
                pass
        
        # Force layout update
        QTimer.singleShot(10, self._force_layout_update)


    # ============================================================================
    # IMAGE NAVIGATION UPDATES
    # ============================================================================
        
    def _force_button_refresh(self):
        """Force refresh of navigation buttons."""
        try:
            # Ensure buttons exist
            if hasattr(self, 'prev_btn'):
                self.prev_btn.show()
            if hasattr(self, 'next_btn'):
                self.next_btn.show()
            if hasattr(self, 'open_image_btn'):
                self.open_image_btn.show()
            if hasattr(self, 'image_counter'):
                self.image_counter.show()
            
            # Update navigation state
            self._update_image_navigation()
            
            # Force repositioning
            QTimer.singleShot(100, self._position_navigation_buttons)
            
            # Force repaint
            if hasattr(self, 'nav_container'):
                self.nav_container.update()
                self.nav_container.repaint()
                    
        except Exception as e:
            print(f"[ERROR] Force button refresh failed: {e}")
            
    def _position_navigation_buttons(self):
        """
        Position navigation buttons over the image viewer - FIXED.
        """
        try:
            # Ensure all components exist
            if not hasattr(self, 'prev_btn') or not self.prev_btn:
                return
            
            if not hasattr(self, '_viewer_container_widget'):
                return
            
            viewer_container = self._viewer_container_widget
            if not viewer_container:
                return
            
            viewer_rect = viewer_container.rect()
            if viewer_rect.isEmpty():
                return
            
            # Position nav_container to cover the entire viewer container
            self.nav_container.setGeometry(viewer_rect)
            self.nav_container.raise_()  # Bring to front
            
            # Calculate button positions
            button_y = viewer_rect.height() - 60  # 60px from bottom
            button_y = max(10, min(button_y, viewer_rect.height() - 50))
            
            # Left button
            self.prev_btn.move(20, button_y)
            self.prev_btn.show()
            self.prev_btn.raise_()
            
            # Center button (open image)
            if hasattr(self, 'open_image_btn') and self.open_image_btn:
                center_x = (viewer_rect.width() - 40) // 2
                self.open_image_btn.move(center_x, button_y)
                self.open_image_btn.show()
                self.open_image_btn.raise_()
            
            # Right button
            self.next_btn.move(viewer_rect.width() - 60, button_y)
            self.next_btn.show()
            self.next_btn.raise_()
            
            # Center counter (top center)
            if hasattr(self, 'image_counter') and self.image_counter:
                counter_width = self.image_counter.sizeHint().width()
                counter_x = (viewer_rect.width() - counter_width) // 2
                self.image_counter.move(counter_x, 10)
                self.image_counter.show()
                self.image_counter.raise_()
            
            # Force update
            self.nav_container.update()
            self.nav_container.raise_()  # Ensure it's on top
            
        except Exception as e:
            print(f"[ERROR] Positioning navigation buttons: {e}")
        
    def _force_layout_update(self):
        """Force layout update to ensure proper widget positioning."""
        try:
            self.update()
            self.repaint()
            QCoreApplication.processEvents()
            
            # Force update of viewer container
            if hasattr(self, '_viewer_container'):
                self._viewer_container.update()
                self._viewer_container._update_child_geometry()
                
            # Reposition buttons
            if hasattr(self, '_position_navigation_buttons'):
                QTimer.singleShot(50, self._position_navigation_buttons)
                
        except Exception as e:
            print(f"[ERROR] Force layout update: {e}")
    
    def next_image(self):
        """Navigate to next image in sequence."""
        if not self._image_items:
            return
        
        if self._current_image_index >= len(self._image_items) - 1:
            self._current_image_index = 0
        else:
            self._current_image_index += 1
        
        self._display_image(self._current_image_index)
        self._update_image_navigation()

    def prev_image(self):
        """Navigate to previous image in sequence."""
        if not self._image_items:
            return
        
        if self._current_image_index <= 0:
            self._current_image_index = len(self._image_items) - 1
        else:
            self._current_image_index -= 1
        
        self._display_image(self._current_image_index)
        self._update_image_navigation()
       
    def open_current_image_url(self):
        """Open the current image's URL in default browser."""
        if not self._image_items or self._current_image_index is None:
            return
        
        if self._current_image_index < 0 or self._current_image_index >= len(self._image_items):
            return
        
        item = self._image_items[self._current_image_index]
        url = item.get("url", "")
        
        if not url:
            print("[OPEN IMAGE] No URL available for current image")
            return
        
        try:
            print(f"[OPEN IMAGE] Opening URL: {url}")
            QDesktopServices.openUrl(QUrl(url))
            self.status.setText(f"Opened image URL in browser")
        except Exception as e:
            print(f"[OPEN IMAGE] Error opening URL: {e}")
            try:
                import webbrowser
                webbrowser.open(url)
                self.status.setText(f"Opened image URL in browser (fallback)")
            except Exception as e2:
                print(f"[OPEN IMAGE] Fallback failed: {e2}")
                self.status.setText("Failed to open image URL")

       
    def _update_image_navigation(self):
        """Update image navigation buttons and counter - ALWAYS SHOW BUTTONS."""
        # ALWAYS SHOW BUTTONS, just enable/disable them
        if hasattr(self, 'prev_btn'):
            self.prev_btn.show()
        if hasattr(self, 'next_btn'):
            self.next_btn.show()
        if hasattr(self, 'open_image_btn'):
            self.open_image_btn.show()
        if hasattr(self, 'image_counter'):
            self.image_counter.show()
        
        if not self._image_items:
            # No images - disable buttons
            if hasattr(self, 'prev_btn'):
                self.prev_btn.setEnabled(False)
            if hasattr(self, 'next_btn'):
                self.next_btn.setEnabled(False)
            if hasattr(self, 'open_image_btn'):
                self.open_image_btn.setEnabled(False)
            if hasattr(self, 'image_counter'):
                self.image_counter.setText("No images")
            return
        
        # We have images
        has_multiple = len(self._image_items) > 1
        
        # Enable/disable based on whether we have multiple images
        if hasattr(self, 'prev_btn'):
            self.prev_btn.setEnabled(has_multiple)
        if hasattr(self, 'next_btn'):
            self.next_btn.setEnabled(has_multiple)
        
        # Open button state is already set in _display_image based on URL availability
        
        # Update counter text
        if hasattr(self, 'image_counter'):
            if len(self._image_items) == 1:
                self.image_counter.setText("1/1")
            elif len(self._image_items) > 1:
                self.image_counter.setText(f"{self._current_image_index + 1}/{len(self._image_items)}")
        
        # Reposition buttons
        QTimer.singleShot(50, self._position_navigation_buttons)
        
        
    def showEvent(self, event):
        """Handle show event to ensure proper layout."""
        super().showEvent(event)
        
        # Force layout update after window is shown
        QTimer.singleShot(200, self._force_layout_update)
        
        # Force button positioning
        QTimer.singleShot(300, self._position_navigation_buttons)
                   
    def open_current_image_url(self):
        """Open the current image's URL in default browser."""
        if not self._image_items or self._current_image_index is None:
            return
        
        if self._current_image_index < 0 or self._current_image_index >= len(self._image_items):
            return
        
        item = self._image_items[self._current_image_index]
        url = item.get("url", "")
        
        if not url:
            # Try to get from parent's current image
            if hasattr(self, '_image_items') and hasattr(self, '_current_image_index'):
                idx = self._current_image_index
                if idx < len(self._image_items):
                    item = self._image_items[idx]
                    url = item.get('url', '')
        
        if url:
            try:
                print(f"[OPEN IMAGE] Opening URL: {url}")
                QDesktopServices.openUrl(QUrl(url))
                self.status.setText(f"Opened image URL in browser")
            except Exception as e:
                print(f"[OPEN IMAGE] Error opening URL: {e}")
                try:
                    import webbrowser
                    webbrowser.open(url)
                    self.status.setText(f"Opened image URL in browser (fallback)")
                except Exception as e2:
                    print(f"[OPEN IMAGE] Fallback failed: {e2}")
                    self.status.setText("Failed to open image URL")
        else:
            print("[OPEN IMAGE] No network URL available to open")
            self.status.setText("No network URL available for this image")        

    # ============================================================================
    # MENU SYSTEM (UPDATED)
    # ============================================================================
    
    def build_menus(self):
        """Build the application menu system with better organization."""
        try:
            menubar = self.menuBar()
            menubar.setVisible(True)
            
            # Clear existing menus
            try:
                menubar.clear()
            except Exception:
                for child in list(menubar.children()):
                    try:
                        child.deleteLater()
                    except Exception:
                        pass
            
            # ====================================================================
            # FILE MENU
            # ====================================================================
            file_menu = menubar.addMenu("📁 File")
            
            # Import section
            import_menu = file_menu.addMenu("📥 Import")
            import_csv_action = QAction("Import CSV/TXT/XLS", self)
            import_csv_action.triggered.connect(lambda: self._import_file_combined_dialog())
            import_menu.addAction(import_csv_action)
            
            import_json_action = QAction("Load Database (JSON/SQLite)", self)
            import_json_action.triggered.connect(lambda: self._load_database_combined_dialog())
            import_menu.addAction(import_json_action)
            
            file_menu.addSeparator()
            
            # Export section
            export_menu = file_menu.addMenu("📤 Export")
            export_json_action = QAction("Save Database (Json/Sqlite)", self)
            export_json_action.triggered.connect(lambda: self._save_database_combined_dialog())
            export_menu.addAction(export_json_action)
            
            export_pdf_action = QAction("Export to PDF/HTML...", self)
            export_pdf_action.triggered.connect(self.export_to_pdf_dialog)
            export_menu.addAction(export_pdf_action)
            
            file_menu.addSeparator()
            
            # Exit
            exit_action = QAction("🚪 Exit", self)
            exit_action.setShortcut("Ctrl+Q")
            exit_action.triggered.connect(self.close)
            file_menu.addAction(exit_action)
            
            # ====================================================================
            # EDIT MENU
            # ====================================================================
            edit_menu = menubar.addMenu("✏️ Edit")
            
            # Add this new action
            sanitize_action = QAction("Sanitize Selected Rows", self)
            sanitize_action.setShortcut("Ctrl+Shift+S")
            sanitize_action.triggered.connect(self.sanitize_selected_rows)
            edit_menu.addAction(sanitize_action)
            
            # Add to the Tools menu after other actions:
            recache_action = QAction("Recache Selected Rows", self)
            recache_action.setShortcut("F7")  # Add keyboard shortcut
            recache_action.triggered.connect(self.recache_selected_rows)
            edit_menu.addAction(recache_action)

           
            edit_game_action = QAction("Edit Selected Game...", self)
            edit_game_action.setShortcut("Ctrl+E")
            edit_game_action.triggered.connect(self.edit_selected_game)
            edit_menu.addAction(edit_game_action)
            edit_menu.addSeparator()
            
            multi_edit_action = QAction("Multi-Edit Selected...", self)
            multi_edit_action.setShortcut("Ctrl+Shift+E")
            multi_edit_action.triggered.connect(self.multi_edit_selected)
            edit_menu.addAction(multi_edit_action)
            
            edit_menu.addSeparator()
            
            mark_played_action = QAction("Mark as Played", self)
            mark_played_action.setShortcut("Ctrl+P")
            mark_played_action.triggered.connect(lambda: self.mark_played_selected(True))
            edit_menu.addAction(mark_played_action)
            
            mark_unplayed_action = QAction("Mark as Unplayed", self)
            mark_unplayed_action.setShortcut("Ctrl+Shift+P")
            mark_unplayed_action.triggered.connect(lambda: self.mark_played_selected(False))
            edit_menu.addAction(mark_unplayed_action)
            
            edit_menu.addSeparator()
            
            delete_action = QAction("🗑 Delete Selected", self)
            delete_action.setShortcut("Del")
            delete_action.triggered.connect(self.delete_selected)
            edit_menu.addAction(delete_action)
            
            # ====================================================================
            # TOOLS MENU
            # ====================================================================
            tools_menu = menubar.addMenu("🛠 Tools")
            
            scrape_action = QAction("Scrape Metadata", self)
            scrape_action.setShortcut("F5")
            scrape_action.triggered.connect(lambda: self.scrape_all(auto_accept_score=92))
            tools_menu.addAction(scrape_action)
            
            download_action = QAction("Download Resources", self)
            download_action.setShortcut("F6")
            download_action.triggered.connect(self.download_all_screenshots)
            tools_menu.addAction(download_action)
            
            tools_menu.addSeparator()
            
            sanitize_action = QAction("Sanitize Titles", self)
            sanitize_action.triggered.connect(self.sanitize_selected_rows)
            tools_menu.addAction(sanitize_action)
            
            test_scrape_action = QAction("Test Scrape Selected", self)
            test_scrape_action.triggered.connect(self.test_scrape_single)
            tools_menu.addAction(test_scrape_action)

            # ====================================================================
            # VIEW MENU - UPDATED
            # ====================================================================
            view_menu = menubar.addMenu("👁 View")
            
            refresh_action = QAction("Refresh View", self)
            refresh_action.setShortcut("F5")
            refresh_action.triggered.connect(self.refresh_model)
            view_menu.addAction(refresh_action)
            
            view_menu.addSeparator()
            
            # Column visibility toggles - UPDATED to show all columns
            show_columns_menu = view_menu.addMenu("Show Columns")
            
            # Get all column names from model
            column_names = []
            for col in range(self.model.columnCount()):
                name = self.model.headerData(col, Qt.Horizontal)
                if name:
                    column_names.append((col, name))
            
            # Create actions for all columns
            for col, name in column_names:
                action = QAction(name, self)
                action.setCheckable(True)
                # Check if column is currently visible (not hidden)
                is_hidden = self.table.isColumnHidden(col)
                action.setChecked(not is_hidden)
                # Connect to toggle function
                action.toggled.connect(lambda checked, c=col: self.table.setColumnHidden(c, not checked))
                show_columns_menu.addAction(action)
            
            # Add "Show All" and "Hide All" actions
            view_menu.addSeparator()
            
            show_all_action = QAction("Show All Columns", self)
            show_all_action.triggered.connect(lambda: self._set_all_columns_visible(True))
            view_menu.addAction(show_all_action)
            
            hide_all_action = QAction("Hide All Columns (Except Title)", self)
            hide_all_action.triggered.connect(lambda: self._set_all_columns_visible(False))
            view_menu.addAction(hide_all_action)
            
            # ====================================================================
            # HELP MENU
            # ====================================================================
            help_menu = menubar.addMenu("❓ Help")
            
            about_action = QAction("About Game Manager", self)
            about_action.triggered.connect(self._show_about_dialog)
            help_menu.addAction(about_action)
            
            docs_action = QAction("Documentation", self)
            docs_action.triggered.connect(self._open_documentation)
            help_menu.addAction(docs_action)
            
            self.status.setText("Menu system initialized")
            
        except Exception as e:
            self.status.setText(f"Menu build error: {e}")
            print("Menu build error:", e)

    def _set_all_columns_visible(self, visible: bool):
        """Show or hide all columns except the title column."""
        for col in range(self.model.columnCount()):
            if col != self.COL_TITLE:  # Keep title column always visible
                self.table.setColumnHidden(col, not visible)
            
    def _toggle_column_visibility(self, column: int, visible: bool):
        """Toggle column visibility in the table."""
        self.table.setColumnHidden(column, not visible)

    def _show_all_columns(self):
        """Show all columns."""
        for col in range(self.model.columnCount()):
            self.table.setColumnHidden(col, False)

    def _hide_all_columns(self):
        """Hide all columns except the title column."""
        for col in range(self.model.columnCount()):
            if col != self.COL_TITLE:  # Keep title column visible
                self.table.setColumnHidden(col, True)
        
    def _show_about_dialog(self):
        """Show simplified about dialog with gaming resource tabs."""
        # Create dialog with tabs
        dialog = QDialog(self)
        dialog.setWindowTitle("About Game Manager & Gaming Resources")
        dialog.setMinimumSize(800, 500)
        dialog.setStyleSheet(APP_STYLESHEET)
        
        # Main layout
        layout = QVBoxLayout(dialog)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)
        
        # Header
        header_label = QLabel("<h2>Game Manager v2.16</h2>")
        header_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(header_label)
        
        # Create tab widget
        tab_widget = QTabWidget()
        
        # ================================================================
        # TAB 1: About
        # ================================================================
        about_tab = QWidget()
        about_layout = QVBoxLayout(about_tab)
        about_layout.setContentsMargins(20, 20, 20, 20)
        about_layout.setSpacing(15)
        
        about_text = """
        <h3>By Rakab Aman</h3>
        <p>A comprehensive game database management tool with metadata scraping capabilities.</p>
        
        <p><b>Main Features:</b></p>
        <ul>
            <li>Import/Export from CSV, JSON, SQLite</li>
            <li>IGDB and Steam metadata scraping</li>
            <li>Image and video caching with 16:9 aspect ratio</li>
            <li>Batch operations on selected games</li>
            <li>PDF/HTML export functionality</li>
            <li>Recaching system for automatic redownloading</li>
            <li>Clickable assets (URLs)</li>
            <li>Game sanitization and duplicate detection</li>
            <li>User rating system</li>
        </ul>
        
        <p><b>Architecture:</b></p>
        <ul>
            <li>Main window with table view and details panel</li>
            <li>Worker threads for background operations</li>
            <li>Caching system for images and metadata</li>
            <li>Dialog-based editing and matching</li>
        </ul>
        
        <p>Built with PyQt5 and Python 3.8+</p>
        """
        
        about_label = QLabel(about_text)
        about_label.setWordWrap(True)
        about_label.setOpenExternalLinks(True)
        about_layout.addWidget(about_label)
        
        about_layout.addStretch()
        
        # ================================================================
        # TAB 2: Download Games
        # ================================================================
        download_tab = QWidget()
        download_layout = QVBoxLayout(download_tab)
        download_layout.setContentsMargins(10, 10, 10, 10)
        
        # Scroll area for download links
        download_scroll = QScrollArea()
        download_scroll.setWidgetResizable(True)
        download_scroll.setFrameShape(QFrame.NoFrame)
        
        download_content = QWidget()
        download_content_layout = QVBoxLayout(download_content)
        download_content_layout.setContentsMargins(5, 5, 5, 5)
        download_content_layout.setSpacing(8)
        
        # Download Games links (from FMHY Gaming Wiki)
        download_links = [
            ("CS.RIN.RU", "https://cs.rin.ru/forum/", 
             "Download / Torrent / Signup / PW: cs.rin.ru / csrin.org / .onion"),
            ("CS.RIN Tools", "https://cs.rin.ru/forum/viewtopic.php?f=29&t=124692", 
             "Search Guide (Important) / Status / Enhancements / Steam Buttons"),
            ("SteamRIP", "https://steamrip.com/", 
             "Download / Pre-Installed / Subreddit / Discord"),
            ("AnkerGames", "https://anakgames.com/", 
             "Download / Pre-Installed / Subreddit / Discord"),
            ("GOG Games", "https://gog-games.com/", 
             "Download / Torrent / GOG Games Only / .onion"),
            ("UnionCrax", "https://unioncrax.biz/", 
             "Download / Pre-Installed / Status / Discord"),
            ("AstralGames", "https://astralgames.net/", 
             "Download / Achievements / Pre-Installed / Discord"),
            ("Online Fix", "https://online-fix.me/", 
             "Download / Torrent / Multiplayer / Signup / PW: online-fix.me / Use Translator / Telegram / Discord"),
            ("SteamUnderground", "https://steamunderground.org/", 
             "Download / Pre-Installed / Discord"),
            ("Ova Games", "https://www.ovagames.com/", 
             "Download / PW: www.ovagames.com / Redirect Bypass Required"),
            ("Torrminatorr", "https://forum.torrminatorr.com/", 
             "Download / Forum / Sign-Up Required"),
            ("Reloaded Steam", "https://reloaded.steam.com/", 
             "Download / Pre-Installed / Discord"),
            ("SteamGG", "https://steamgg.com/", 
             "Download / Pre-Installed / Subreddit / Discord"),
            ("World of PC Games", "https://worldofpcgames.net/", 
             "Download / Pre-Installed / Use Adblock / Site Info / Subreddit"),
            ("Games4U", "https://games4u.com/", 
             "Download / Use Adblock / Sources on DDL Pages"),
            ("CG Games", "https://www.cg-games.net/", 
             "Download"),
            ("GamePCFull", "https://gamepcfull.com/", 
             "Download"),
            ("IRC Games", "https://wiki.fmhy.net/pages/7d088d/", 
             "Download Games via IRC"),
            ("FreeToGame", "https://www.freetogame.com/", 
             "F2P Games / Trackers"),
            ("TendingNow", "https://tendingnow.com/", 
             "F2P Games / Trackers"),
            ("Acid Play", "https://acid-play.com/", 
             "F2P Games / Trackers"),
            # Removed Anti Denuvo Sanctuary from here (moved to Discord tab)
        ]
        
        # Add download links
        for name, url, description in download_links:
            link_text = f'<a href="{url}" style="text-decoration: none; color: #3498db; font-weight: 600;">{name}</a> - {description}'
            link_label = QLabel(link_text)
            link_label.setOpenExternalLinks(True)
            link_label.setTextFormat(Qt.RichText)
            link_label.setWordWrap(True)
            link_label.setStyleSheet("margin: 2px 0; padding: 3px 0; border-bottom: 1px dotted #eee;")
            download_content_layout.addWidget(link_label)
        
        download_content_layout.addStretch()
        download_scroll.setWidget(download_content)
        download_layout.addWidget(download_scroll)
        
        # ================================================================
        # TAB 3: Game Repacks
        # ================================================================
        repacks_tab = QWidget()
        repacks_layout = QVBoxLayout(repacks_tab)
        repacks_layout.setContentsMargins(10, 10, 10, 10)
        
        # Scroll area for repack links
        repacks_scroll = QScrollArea()
        repacks_scroll.setWidgetResizable(True)
        repacks_scroll.setFrameShape(QFrame.NoFrame)
        
        repacks_content = QWidget()
        repacks_content_layout = QVBoxLayout(repacks_content)
        repacks_content_layout.setContentsMargins(5, 5, 5, 5)
        repacks_content_layout.setSpacing(8)
        
        # Game Repacks links (from FMHY Gaming Wiki)
        repack_links = [
            ("FitGirl Repacks", "https://fitgirl-repacks.site/", 
             "Download / Torrent / ROM Repacks / Unofficial Launcher"),
            ("KaOsKrew", "http://kaoskrew.org/", 
             "Download / Torrent / Discord"),
            ("ARMGDDN Browser", "https://armgddn.com/", 
             "Download / Telegram / Discord"),
            ("Gnarly Repacks", "https://gnarly-repacks.site/", 
             "Download / PW: gnarly"),
            ("DODI Repacks", "https://dodi-repacks.site/", 
             "Torrent / Redirect Bypass / Site Warning / Discord"),
            ("Elamigos", "https://www.elamigos-games.com/", 
             "Download"),
            ("FreeGOGPCGames", "https://freegogpcgames.com/", 
             "GOG Games Torrent Uploads / Hash Note"),
            ("Game-Repack", "https://game-repack.site/", 
             "Various game repacks"),
            ("Xatab Repacks", "https://xatab-repack.site/", 
             "Russian repacker with English games"),
            ("TinyRepacks", "https://www.tiny-repacks.win/", 
             "Extremely small repacks"),
            ("CPG Repacks", "https://cpgrepacks.site/", 
             "Canadian repacker"),
            ("RG Mechanics", "https://rg-mechanics.org/", 
             "Russian repacker"),
            ("Repack Games", "https://repack-games.com/", 
             "Multi-language repacks"),
        ]
        
        # Add repack links
        for name, url, description in repack_links:
            link_text = f'<a href="{url}" style="text-decoration: none; color: #e74c3c; font-weight: 600;">{name}</a> - {description}'
            link_label = QLabel(link_text)
            link_label.setOpenExternalLinks(True)
            link_label.setTextFormat(Qt.RichText)
            link_label.setWordWrap(True)
            link_label.setStyleSheet("margin: 2px 0; padding: 3px 0; border-bottom: 1px dotted #eee;")
            repacks_content_layout.addWidget(link_label)
        
        repacks_content_layout.addStretch()
        repacks_scroll.setWidget(repacks_content)
        repacks_layout.addWidget(repacks_scroll)
        
        # ================================================================
        # TAB 4: Discord Communities
        # ================================================================
        discord_tab = QWidget()
        discord_layout = QVBoxLayout(discord_tab)
        discord_layout.setContentsMargins(10, 10, 10, 10)
        
        # Scroll area for Discord links
        discord_scroll = QScrollArea()
        discord_scroll.setWidgetResizable(True)
        discord_scroll.setFrameShape(QFrame.NoFrame)
        
        discord_content = QWidget()
        discord_content_layout = QVBoxLayout(discord_content)
        discord_content_layout.setContentsMargins(5, 5, 5, 5)
        discord_content_layout.setSpacing(8)
        
        # Discord Communities links
        discord_links = [
            ("Gamers Unlimited", "https://discord.gg/MNqtzwq8W", 
             "Gaming community Discord"),
            ("Pubs Lounge", "https://discord.gg/pubslounge", 
             "General gaming and community Discord"),
            ("SteamAutoCrack", "https://discord.gg/Y4xcZ4fD", 
             "Gaming and emulation Discord"),
            ("Nucleus Co-op", "https://discord.gg/distro-nucleusco-op-142649962839277568", 
             "Co-op gaming and distribution Discord"),
            ("Piracy Lords", "https://discord.gg/piracylords", 
             "Gaming piracy community Discord"),
            ("Anti Denuvo Sanctuary", "https://discord.com/invite/anti-denuvo-sanctuary", 
             "Denuvo cracking and anti-DRM community"),
        ]
        
        # Add Discord links
        for name, url, description in discord_links:
            link_text = f'<a href="{url}" style="text-decoration: none; color: #7289da; font-weight: 600;">{name}</a> - {description}'
            link_label = QLabel(link_text)
            link_label.setOpenExternalLinks(True)
            link_label.setTextFormat(Qt.RichText)
            link_label.setWordWrap(True)
            link_label.setStyleSheet("margin: 2px 0; padding: 3px 0; border-bottom: 1px dotted #eee;")
            discord_content_layout.addWidget(link_label)
        
        discord_content_layout.addStretch()
        discord_scroll.setWidget(discord_content)
        discord_layout.addWidget(discord_scroll)
        
        # ================================================================
        # Add tabs to tab widget
        # ================================================================
        tab_widget.addTab(about_tab, "📋 About")
        tab_widget.addTab(download_tab, "⬇️ Download Games")
        tab_widget.addTab(repacks_tab, "🎮 Game Repacks")
        tab_widget.addTab(discord_tab, "💬 Discord")
        
        layout.addWidget(tab_widget, 1)  # Add stretch factor
        
        # ================================================================
        # Button to open full FMHY Gaming Wiki
        # ================================================================
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        
        wiki_button = QPushButton("🌐 Open Complete FMHY Gaming Wiki")
        wiki_button.clicked.connect(lambda: QDesktopServices.openUrl(
            QUrl("https://github.com/fmhy/FMHY/wiki/%F0%9F%8E%AE-Gaming---Emulation")
        ))
        wiki_button.setStyleSheet("""
            QPushButton {
                background-color: #3498db;
                color: white;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: 600;
            }
            QPushButton:hover {
                background-color: #2980b9;
            }
        """)
        
        close_button = QPushButton("Close")
        close_button.clicked.connect(dialog.accept)
        
        button_layout.addWidget(wiki_button)
        button_layout.addWidget(close_button)
        
        layout.addLayout(button_layout)
        
        # Show dialog
        dialog.exec_()
        
    def _open_documentation(self):
        """Open documentation in browser."""
        QDesktopServices.openUrl(QUrl("https://github.com/rakab/game-manager"))
    
    # ============================================================================
    # UPDATED UTILITY METHODS
    # ============================================================================
    
    # Update the update_counters method:
    def update_counters(self):
        """Update all statistics cards including new ones."""
        total = len(self.games)
        played = sum(1 for g in self.games if g.get("played", False))
        remaining = total - played
        
        # Count cached games
        cached = 0
        for g in self.games:
            cache_paths = g.get("image_cache_paths") or []
            if cache_paths:
                for path in cache_paths:
                    if path and isinstance(path, str):
                        abs_path = SCRIPT_DIR / path
                        if abs_path.exists():
                            cached += 1
                            break
        
        # Count duplicate games (games with same title)
        duplicate_games = 0
        title_counts = {}
        for game in self.games:
            title = (game.get("title") or "").strip().lower()
            if title:
                title_counts[title] = title_counts.get(title, 0) + 1
        
        # Games that appear more than once in title_counts are duplicates
        for count in title_counts.values():
            if count > 1:
                duplicate_games += count  # Count all instances of duplicates
        
        # Count unscraped (missing both app_id and igdb_id)
        unscraped = 0
        for game in self.games:
            has_app_id = bool(str(game.get("app_id") or "").strip())
            has_igdb_id = bool(str(game.get("igdb_id") or "").strip())
            if not has_app_id and not has_igdb_id:
                unscraped += 1
        
        print(f"[STATS] Total: {total}, Duplicates: {duplicate_games}, Unscraped: {unscraped}")
        
        # Update stats cards
        if hasattr(self, 'stats_cards'):
            self.stats_cards["total"].setText(str(total))
            self.stats_cards["played"].setText(str(played))
            self.stats_cards["remaining"].setText(str(remaining))
            self.stats_cards["cached"].setText(str(cached))
            self.stats_cards["duplicates"].setText(str(duplicate_games))
            self.stats_cards["unscraped"].setText(str(unscraped))
        
        # Update status bar
        self.total_label.setText(f"Total: {total}")
        self.played_label.setText(f"Played: {played}")
        self.remaining_label.setText(f"Remaining: {remaining}")
        
        # Force UI update
        QCoreApplication.processEvents()
    
    def _show_progress(self, visible: bool, maximum: int = 0, value: int = 0):
        """Show or hide progress bar."""
        self.progress_bar.setVisible(visible)
        if visible:
            self.progress_bar.setMaximum(maximum)
            self.progress_bar.setValue(value)
    
    def _update_progress(self, value: int, text: str = ""):
        """Update progress bar value and status."""
        self.progress_bar.setValue(value)
        if text:
            self.status.setText(text)
            self.progress_bar.setFormat(f"%p% - {text}")
            
  # ============================================================================
    # FIXED IMAGE DISPLAY METHODS
    # ============================================================================
        
    def _fetch_and_display_images(self, row_index: int, urls: List[str]) -> bool:
        """
        Fetch and display images for a specific game row with MAX_IMAGES_TO_DOWNLOAD limit.
        """
        # Reset current image state
        self._image_items = []
        self._current_image_index = 0
        
        # Clear viewer
        try:
            if getattr(self.viewer, "movie", None):
                self.viewer.movie().stop()
            self.viewer.clear()
            self.viewer.set_url("")
        except Exception:
            pass
        
        if not urls:
            print(f"[DEBUG] No image URLs for row {row_index}")
            self.status.setText("No images available")
            self._update_image_navigation()
            return True
        
        print(f"[DEBUG] _fetch_and_display_images called with {len(urls)} URLs")
        
        # Get game data
        game = self.games[row_index] if row_index < len(self.games) else None
        if not game:
            print(f"[ERROR] No game data for row {row_index}")
            return False
        
        # Get cover URL for reference
        cover_url = game.get("cover_url", "")
        if cover_url and cover_url.startswith("//"):
            cover_url = "https:" + cover_url
        
        # ====================================================================
        # PHASE 1: LIMIT URLS TO MAX_IMAGES_TO_DOWNLOAD
        # ====================================================================
        # Determine which URLs to process (cover + up to MAX_IMAGES_TO_DOWNLOAD screenshots)
        urls_to_process = []
        
        # Always include cover if it exists (and is in the URL list)
        if cover_url and cover_url in urls:
            urls_to_process.append(cover_url)
        
        # Add screenshots up to the limit (excluding cover if already added)
        screenshot_count = 0
        for url in urls:
            if url == cover_url:
                continue  # Already added
            
            if screenshot_count < MAX_IMAGES_TO_DOWNLOAD:
                urls_to_process.append(url)
                screenshot_count += 1
            else:
                break
        
        print(f"[IMAGE_LIMIT] Processing {len(urls_to_process)} URLs (limit: cover + {MAX_IMAGES_TO_DOWNLOAD} screenshots)")
        
        # ====================================================================
        # PHASE 2: BUILD COMPLETE IMAGE ITEMS LIST WITH CACHE STATUS
        # ====================================================================
        # Initialize all image items first with URLs
        for url in urls_to_process:
            if not url:
                continue
                
            # Normalize URL
            normalized_url = url
            if url.startswith("//"):
                normalized_url = "https:" + url
            
            # Initialize item as not fetched
            self._image_items.append({
                "url": normalized_url,  # Store URL for click-to-open
                "pixmap": None,
                "movie": None,
                "fetched": False,
                "local_path": None,
                "already_cached": False,
                "is_cover": (normalized_url == cover_url)
            })        
        # ====================================================================
        # PHASE 3: CHECK AND LOAD CACHED IMAGES
        # ====================================================================
        cached_paths = list(game.get("image_cache_paths") or [])
        print(f"[IMAGE_CACHE] Found {len(cached_paths)} cached paths in game data")
        
        loaded_from_cache = 0
        cache_miss_indices = []
        
        # For each cached path, try to match it with our URLs
        for cache_path in cached_paths:
            if not cache_path:
                continue
            
            try:
                # Get absolute path
                abs_path = CACHE_DIR / cache_path
                if not abs_path.exists():
                    print(f"[IMAGE_CACHE] Cached file not found: {abs_path}")
                    continue
                
                # Try to match by URL hash
                matched = False
                for idx, item in enumerate(self._image_items):
                    if item.get("fetched") or item.get("already_cached"):
                        continue
                    
                    url = item["url"]
                    url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
                    
                    # Check if this cache file contains the URL hash
                    if url_hash in str(abs_path.name):
                        print(f"[IMAGE_CACHE] Matched URL {idx} to cache: {abs_path.name}")
                        
                        # Load the image
                        if str(abs_path).lower().endswith('.gif'):
                            # Load as animated GIF
                            movie = QMovie(str(abs_path))
                            movie.setCacheMode(QMovie.CacheAll)
                            if movie.isValid():
                                movie.start()
                                item["movie"] = movie
                                item["fetched"] = True
                                item["already_cached"] = True
                                item["local_path"] = cache_path
                                loaded_from_cache += 1
                                matched = True
                                break
                        else:
                            # Load as static image
                            pixmap = QPixmap()
                            if pixmap.load(str(abs_path)):
                                item["pixmap"] = pixmap
                                item["fetched"] = True
                                item["already_cached"] = True
                                item["local_path"] = cache_path
                                loaded_from_cache += 1
                                matched = True
                                break
                
                if not matched:
                    print(f"[IMAGE_CACHE] Could not match cache file: {abs_path.name}")
                    
            except Exception as e:
                print(f"[IMAGE_CACHE] Error processing cache path {cache_path}: {e}")
        
        # Identify which URLs still need to be fetched
        for idx, item in enumerate(self._image_items):
            if not item.get("fetched"):
                cache_miss_indices.append(idx)
        
        print(f"[IMAGE_CACHE] Loaded {loaded_from_cache} images from cache, {len(cache_miss_indices)} to fetch")
        
        # ====================================================================
        # PHASE 4: DISPLAY FIRST AVAILABLE IMAGE
        # ====================================================================
        first_available = None
        for idx, item in enumerate(self._image_items):
            if item.get("fetched"):
                first_available = idx
                break
        
        if first_available is not None:
            self._current_image_index = first_available
            self._display_image(self._current_image_index)
            self._update_image_navigation()
            
            # Force button update and positioning
            QTimer.singleShot(100, self._force_button_refresh)
            
            # Update status
            if loaded_from_cache == len(self._image_items):
                self.status.setText(f"Loaded all {len(self._image_items)} images from cache")
            elif loaded_from_cache > 0:
                self.status.setText(f"Loaded {loaded_from_cache}/{len(self._image_items)} images from cache")
            else:
                self.status.setText(f"No cached images found, downloading...")
        else:
            self.status.setText("No images available")
            self.image_counter.setText("No images")
            self.prev_btn.setEnabled(False)
            self.next_btn.setEnabled(False)
        
        # ====================================================================
        # PHASE 5: FETCH MISSING IMAGES IN BACKGROUND WITH LIMIT ENFORCEMENT
        # ====================================================================
        if not cache_miss_indices:
            # All images were cached
            return True
        
        # Count how many non-cover images we have already cached/downloaded
        already_downloaded_non_cover = 0
        for item in self._image_items:
            if item.get("fetched") and not item.get("is_cover"):
                already_downloaded_non_cover += 1
        
        # Calculate how many more non-cover images we can download
        remaining_downloads = max(0, MAX_IMAGES_TO_DOWNLOAD - already_downloaded_non_cover)
        
        # Only fetch if we have actual URLs to fetch
        images_to_fetch = 0
        
        # Process cache misses in order, respecting the limit
        for idx in cache_miss_indices:
            if images_to_fetch >= remaining_downloads and idx > 0:  # idx > 0 to always allow cover
                print(f"[IMAGE_LIMIT] Reached download limit ({MAX_IMAGES_TO_DOWNLOAD}), skipping remaining images")
                break
            
            item = self._image_items[idx]
            if item.get("fetched") or item.get("already_cached"):
                continue
            
            url = item["url"]
            if not url:
                continue
            
            # Check if this is the cover
            if item.get("is_cover"):
                # Always try to download cover if not cached
                pass
            else:
                # For non-cover images, check the limit
                if images_to_fetch >= remaining_downloads:
                    print(f"[IMAGE_LIMIT] Skipping non-cover image (limit reached): {url}")
                    continue
            
            # Create worker for this image
            worker = ImageFetchWorker(row_index, url, game)
            thread = QThread(self)
            worker.moveToThread(thread)
            
            # Connect signals
            thread.started.connect(worker.run)
            worker.finished.connect(self._on_image_fetched)
            worker.error.connect(
                lambda r, u, e: self.status.setText(f"Image fetch error {u}: {e}")
            )
            
            # Start thread
            thread.start()
            self._image_threads.append((thread, worker))
            images_to_fetch += 1
            
            print(f"[IMAGE_DOWNLOAD] Queued download for URL {idx} ({'cover' if item.get('is_cover') else 'screenshot'})")
        
        if images_to_fetch > 0:
            self.status.setText(f"Fetching {images_to_fetch} new images...")
            return False  # Some images needed fetching
        
        return loaded_from_cache == len(self._image_items)  # Return True if all were cached
        
        
    def _on_image_fetched(self, row_index: int, url: str, rel_path: str):
        """
        Update model and game dict when an image is cached.
        """
        try:
            # Update the image items list
            for item in self._image_items:
                if item.get("url") == url:
                    item["fetched"] = True
                    item["local_path"] = rel_path
                    
                    # Try to load and display immediately
                    try:
                        abs_path = SCRIPT_DIR / rel_path
                        if abs_path.exists():
                            # Check if it's a GIF
                            if str(abs_path).lower().endswith('.gif'):
                                movie = QMovie(str(abs_path))
                                movie.setCacheMode(QMovie.CacheAll)
                                if movie.isValid():
                                    movie.start()
                                    item["movie"] = movie
                                    # Update display if this is the current image
                                    if self._image_items.index(item) == self._current_image_index:
                                        self._display_image(self._current_image_index)
                            else:
                                pixmap = QPixmap()
                                if pixmap.load(str(abs_path)):
                                    item["pixmap"] = pixmap
                                    # Update display if this is the current image
                                    if self._image_items.index(item) == self._current_image_index:
                                        self._display_image(self._current_image_index)
                    except Exception as e:
                        print(f"[ERROR] Failed to load cached image: {e}")
                    
                    break
            
            # Update game's cache paths
            game = self.games[row_index] if row_index < len(self.games) else None
            if game:
                # Check if this URL is a microtrailer by URL pattern
                is_microtrailer = any(ext in url.lower() for ext in ['.gif', '.webm', '.mp4', 'microtrailer'])
                
                if is_microtrailer:
                    # Store in microtrailer_cache_path field
                    game["microtrailer_cache_path"] = rel_path
                    print(f"[CACHE] Updated microtrailer_cache_path for row {row_index}: {rel_path}")
                    
                    # Also update the model column for microtrailer cache path
                    self.model.setItem(row_index, self.COL_MICROTRAILER_CACHE_PATH, 
                                     QStandardItem(rel_path))
                
                # Update image_cache_paths (for all images)
                paths = game.get("image_cache_paths", [])
                if rel_path not in paths:
                    paths.append(rel_path)
                    # Limit to MAX_IMAGES_TO_DISPLAY
                    game["image_cache_paths"] = paths[:MAX_IMAGES_TO_DISPLAY]
                
                # Update model column for cached image paths
                self.model.setItem(row_index, self.COL_IMAGE_CACHE_PATHS, 
                                 QStandardItem(", ".join(paths)))
            
            # Update status
            cached_count = len([i for i in self._image_items if i.get("fetched")])
            self.status.setText(f"{cached_count}/{len(self._image_items)} images loaded")

        except Exception as e:
            print(f"[ERROR] on_image_fetched handler failed: {e}")

            # In the GameManager class, update the _display_image method:
    def _display_image(self, index: int):
        """
        Display image at specified index.
        """
        #print(f"[DEBUG] _display_image called with index {index}")
        
        # Validate input
        if not self._image_items or index is None or \
           index < 0 or index >= len(self._image_items):
            #print(f"[DEBUG] No image items or invalid index")
            self.viewer.clear()
            self.viewer.set_url("")
            self._update_image_navigation()
            return
        
        item = self._image_items[index]
        url = item.get("url") or ""
        
        print(f"[DEBUG] Displaying item {index}: fetched={item.get('fetched')}, url={url}")
        
        try:
            # Stop previous movie
            current_movie = getattr(self.viewer, "movie", None)
            if current_movie:
                try:
                    current_movie.stop()
                except Exception:
                    pass
            
            # Check what type of media we have
            if item.get("movie") and item["movie"].isValid():
                print(f"[DEBUG] Displaying animated GIF")
                self.viewer.setMovie(item["movie"])
                item["movie"].start()
                self.viewer.set_url(url)  # Set URL for reference only
            elif item.get("pixmap") and not item["pixmap"].isNull():
                print(f"[DEBUG] Displaying static image with URL: {url[:50] if url else 'None'}")
                # Scale pixmap to fit viewer while maintaining aspect ratio
                pixmap = item["pixmap"]
                viewer_size = self.viewer.size()
                
                # Calculate scaled size maintaining aspect ratio
                scaled_pixmap = pixmap.scaled(
                    viewer_size, 
                    Qt.KeepAspectRatio, 
                    Qt.SmoothTransformation
                )
                
                self.viewer.setPixmap(scaled_pixmap)
                self.viewer.set_url(url)  # Set URL for reference only
            else:
                print(f"[DEBUG] No valid media to display")
                self.viewer.clear()
                self.viewer.set_url("")
            
            # Update open button state
            if hasattr(self, 'open_image_btn'):
                if url:
                    self.open_image_btn.setEnabled(True)
                    self.open_image_btn.setToolTip(f"Open in browser: {url}")
                else:
                    self.open_image_btn.setEnabled(False)
                    self.open_image_btn.setToolTip("No network URL available")
            
            # Update navigation buttons
            self._update_image_navigation()
            
        except Exception as e:
            print(f"[ERROR] Image display failed: {e}")
            self.viewer.clear()
            self.viewer.set_url("")
            
            # Disable open button on error
            if hasattr(self, 'open_image_btn'):
                self.open_image_btn.setEnabled(False)
            
            self._update_image_navigation()
        
    # ============================================================================
    # FIXED TRAILER PLAYBACK
    # ============================================================================
        
    # Update the _play_trailer_media method to store the network URL:
    def _play_trailer_media(self, url: str):
        """
        Play trailer URL with debug logging.
        """
        print(f"[DEBUG] _play_trailer_media CALLED with: '{url}'")
        
        # Store the network URL for clicking
        self._current_trailer_url = url
        
        # Set URL for clickable widgets
        self.video_widget.set_url(url)
        self.trailer_gif_label.set_url(url, "")  # Only pass URL, no local path needed

        # check cached_microtrailer first
        rel_path = None
        try:
            rel_path = self.games[self._current_row].get("microtrailer_cache_path")
            if rel_path:
                abs_path = SCRIPT_DIR / rel_path
                if abs_path.exists():
                    media = QMediaContent(QUrl.fromLocalFile(str(abs_path)))
                    self.media_player.setMedia(media)
                    self.media_player.play()
                    return
        except Exception:
            pass

        # stop any previous media
        try:
            self.media_player.stop()
        except Exception as e:
            print(f"[DEBUG] Error stopping media player: {e}")
            
        try:
            if hasattr(self.trailer_gif_label, "movie") and self.trailer_gif_label.movie():
                self.trailer_gif_label.movie().stop()
                self.trailer_gif_label.clear()
        except Exception as e:
            print(f"[DEBUG] Error clearing GIF label: {e}")

        if not url:
            print("[DEBUG] URL is empty, aborting playback.")
            return

        lower = url.lower()
        
        # --- CASE 1: GIF ---
        if lower.endswith(".gif"):
            print("[DEBUG] Detected GIF format.")
            try:
                print(f"[DEBUG] Fetching GIF data from: {url}")
                r = requests.get(url, timeout=8, headers={"User-Agent": "GameScraper/1.0"})
                print(f"[DEBUG] HTTP Status: {r.status_code}, Content Size: {len(r.content)} bytes")
                
                if r.status_code == 200 and r.content:
                    movie = QMovie()
                    movie.setCacheMode(QMovie.CacheAll)
                    movie.setDevice(QBuffer(QByteArray(r.content)))
                    
                    if movie.isValid():
                        print("[DEBUG] GIF is valid. Starting QMovie.")
                        self.trailer_gif_label.setMovie(movie)
                        movie.start()
                        self.video_widget.hide()
                        self.trailer_gif_label.show()
                    else:
                        print("[DEBUG] GIF data downloaded but QMovie says it is invalid.")
                else:
                    print("[DEBUG] Failed to download GIF (Bad status or empty content).")
            except Exception as e:
                print(f"[DEBUG] Exception loading GIF: {e}")
                self.status.setText("Failed to load GIF trailer.")
                
        # --- CASE 2: VIDEO (WebM/MP4) ---
        else:
            print("[DEBUG] Detected VIDEO format (WebM/MP4).")
            try:
                self.trailer_gif_label.hide()
                self.video_widget.show()
                
                qurl = QUrl(url)
                print(f"[DEBUG] Setting QMediaContent with QUrl: {qurl.toString()}")
                
                media = QMediaContent(qurl)
                self.media_player.setMedia(media)
                self.media_player.setMuted(True)  # Always muted
                self.media_player.play()
                
                # Check state after play command
                print(f"[DEBUG] Player State after play(): {self.media_player.state()}")
                print(f"[DEBUG] Player Error string: {self.media_player.errorString()}")
                
            except Exception as e:
                print(f"[DEBUG] Exception setting up QMediaPlayer: {e}")
                self.status.setText("Failed to play trailer.")

    def _on_media_status_changed(self, status):
        """
        Handle media status changes to enable looping when VIDEO_LOOP_ENABLED is True.
        """
        if status == QMediaPlayer.EndOfMedia and VIDEO_LOOP_ENABLED:
            # Restart from beginning for continuous playback
            self.media_player.setPosition(0)
            self.media_player.play() 
    
    # ============================================================================
    # DATA MODEL METHODS
    # ============================================================================
    
    def on_model_item_changed(self, item: QStandardItem):
        """
        Handle changes made directly in the table cells.
        """
        if getattr(self, "_suppress_model_change", False):
            return
        
        try:
            row = item.row()
            col = item.column()
            
            if row < 0 or row >= len(self.games):
                return
            
            game = self.games[row]
            text = item.text().strip()
            
            # Don't overwrite with empty values from accidental clearing
            if not text:
                return
            
            # Map column to game dictionary key
            mapping = {
                self.COL_TITLE: "title",
                self.COL_VERSION: "patch_version",
                self.COL_GAMEDRIVE: "game_drive",
                self.COL_STEAMID: "app_id",
                self.COL_GENRES: "genres",
                self.COL_GAME_MODES: "game_modes",
                self.COL_RELEASE: "release_date",
                self.COL_THEMES: "themes",
                self.COL_DEV: "developer",
                self.COL_PUB: "publisher",
                self.COL_SCENE: "scene_repack",
                self.COL_PERSPECTIVE: "player_perspective",
                self.COL_ORIGINAL: "original_title",
                self.COL_IGDB_ID: "igdb_id",
                self.COL_SHORTCUTS: "screenshots",
                self.COL_TRAILER: "microtrailers",
                self.COL_STEAMDB: "steamdb_link",
                self.COL_PCWIKI: "pcgw_link",
                self.COL_STEAM_LINK: "steam_link",
                self.COL_COVER_URL: "cover_url",
                self.COL_DESCRIPTION: "description",
                self.COL_IGDB_TRAILERS: "trailers",
                self.COL_MICROTRAILERS: "microtrailers_extra",
                self.COL_USER_RATING: "user_rating",  # ADDED USER RATING
            }
            
            key = mapping.get(col)
            if not key:
                return
            
            # Handle list fields (comma-separated)
            if key in ("screenshots", "microtrailers", "trailers", "microtrailers_extra"):
                game[key] = [s.strip() for s in re.split(r",\s*", text) if s.strip()]
            else:
                game[key] = text
                
        except Exception as e:
            print(f"[ERROR] Model change handler: {e}")
    
    # In the recompute_duplicates method, store the duplicate counts for easier access:
    def recompute_duplicates(self):
        """
        Identify duplicate titles and Steam IDs for UI highlighting.
        """
        print("[DUPLICATES] Recomputing duplicates...")
        
        title_counts = {}
        steam_counts = {}
        
        for game_idx, game in enumerate(self.games):
            # Check both title fields
            for field in ("title", "original_title"):
                value = (game.get(field) or "").strip()
                if value:
                    normalized = value.lower()
                    if normalized not in title_counts:
                        title_counts[normalized] = []
                    title_counts[normalized].append((game_idx, field, value))
            
            # Check Steam ID
            steam_id = str(game.get("app_id") or "").strip()
            if steam_id:
                normalized = steam_id.lower()
                if normalized not in steam_counts:
                    steam_counts[normalized] = []
                steam_counts[normalized].append((game_idx, steam_id))
        
        # Store duplicates for highlighting
        self._dup_title_set = {k for k, v in title_counts.items() if len(v) > 1}
        self._dup_steamid_set = {k for k, v in steam_counts.items() if len(v) > 1}
        
        # Also store duplicate counts for stats
        self._duplicate_title_count = len(self._dup_title_set)
        self._duplicate_steamid_count = len(self._dup_steamid_set)
        
        # Debug output
        if self._dup_title_set:
            print(f"[DUPLICATES] Found {len(self._dup_title_set)} duplicate titles")
        if self._dup_steamid_set:
            print(f"[DUPLICATES] Found {len(self._dup_steamid_set)} duplicate Steam IDs")
        if not self._dup_title_set and not self._dup_steamid_set:
            print("[DUPLICATES] No duplicates found")
        
        return len(self._dup_title_set), len(self._dup_steamid_set)
    
    def force_highlight_update(self):
        """
        Force update of all highlighting without rebuilding the entire model.
        Useful when toggling played status.
        """
        try:
            # Recompute duplicates
            self.recompute_duplicates()
            
            # Define colors
            duplicate_color = QColor(255, 230, 200)      # Light orange
            played_color = QColor(220, 255, 220)         # Light green
            unplayed_color = QColor(220, 240, 255)       # Light blue
            
            # Apply highlighting to each row
            for row in range(self.model.rowCount()):
                if row >= len(self.games):
                    continue
                    
                game = self.games[row]
                is_played = game.get("played", False)
                
                # Check for duplicates
                title_val = (game.get("title") or "").strip().lower()
                orig_val = (game.get("original_title") or "").strip().lower()
                steam_val = str(game.get("app_id") or "").strip().lower()
                
                has_title_duplicate = title_val and title_val in self._dup_title_set
                has_original_duplicate = orig_val and orig_val in self._dup_title_set
                has_steam_duplicate = steam_val and steam_val in self._dup_steamid_set
                
                # Determine base row color
                row_color = played_color if is_played else unplayed_color
                
                # Apply colors to each column - FIXED: Create items if they don't exist
                for col in range(self.model.columnCount()):
                    item = self.model.item(row, col)
                    if not item:
                        # Create item if it doesn't exist
                        item = QStandardItem("")
                        self.model.setItem(row, col, item)
                    
                    # Check if this column should have duplicate highlighting
                    is_duplicate_cell = (
                        (col == self.COL_TITLE and has_title_duplicate) or
                        (col == self.COL_ORIGINAL and has_original_duplicate) or
                        (col == self.COL_STEAMID and has_steam_duplicate)
                    )
                    
                    # Apply color based on priority
                    if is_duplicate_cell:
                        item.setBackground(duplicate_color)
                    elif row_color:
                        item.setBackground(row_color)
                    else:
                        # Clear background
                        item.setBackground(QBrush())
            
            # Force UI update
            self.table.viewport().update()
            print(f"[HIGHLIGHT] Force updated highlighting for {self.model.rowCount()} rows")
            
        except Exception as e:
            print(f"[ERROR] Error in force_highlight_update: {e}")
            import traceback
            traceback.print_exc()
       
    def update_table_highlights(self):
        """
        Force the table to update all highlighting.
        This triggers a repaint of the entire table viewport.
        """
        try:
            # Recompute duplicates
            self.recompute_duplicates()
            
            # Force the table to repaint
            self.table.viewport().update()
            
            # Update status
            played_count = sum(1 for g in self.games if g.get("played", False))
            unplayed_count = len(self.games) - played_count
            self.status.setText(f"Highlights: {played_count} played (green), {unplayed_count} unplayed (blue)")
            
        except Exception as e:
            print(f"[ERROR] update_table_highlights failed: {e}")    
    
    # ============================================================================
    # UPDATED METHODS FOR STATISTICS
    # ============================================================================
    
    # Update the update_counters method:
    def update_counters(self):
        """Update all statistics cards including new ones."""
        total = len(self.games)
        played = sum(1 for g in self.games if g.get("played", False))
        remaining = total - played
        
        # Count cached games
        cached = 0
        for g in self.games:
            cache_paths = g.get("image_cache_paths") or []
            if cache_paths:
                for path in cache_paths:
                    if path and isinstance(path, str):
                        abs_path = SCRIPT_DIR / path
                        if abs_path.exists():
                            cached += 1
                            break
        
        # Count duplicate games (games with same title)
        duplicate_games = 0
        title_counts = {}
        for game in self.games:
            title = (game.get("title") or "").strip().lower()
            if title:
                title_counts[title] = title_counts.get(title, 0) + 1
        
        # Games that appear more than once in title_counts are duplicates
        for count in title_counts.values():
            if count > 1:
                duplicate_games += count  # Count all instances of duplicates
        
        # Count unscraped (missing both app_id and igdb_id)
        unscraped = 0
        for game in self.games:
            has_app_id = bool(str(game.get("app_id") or "").strip())
            has_igdb_id = bool(str(game.get("igdb_id") or "").strip())
            if not has_app_id and not has_igdb_id:
                unscraped += 1
        
        print(f"[STATS] Total: {total}, Duplicates: {duplicate_games}, Unscraped: {unscraped}")
        
        # Update stats cards
        if hasattr(self, 'stats_cards'):
            self.stats_cards["total"].setText(str(total))
            self.stats_cards["played"].setText(str(played))
            self.stats_cards["remaining"].setText(str(remaining))
            self.stats_cards["cached"].setText(str(cached))
            self.stats_cards["duplicates"].setText(str(duplicate_games))
            self.stats_cards["unscraped"].setText(str(unscraped))
        
        # Update status bar
        self.total_label.setText(f"Total: {total}")
        self.played_label.setText(f"Played: {played}")
        self.remaining_label.setText(f"Remaining: {remaining}")
        
        # Force UI update
        QCoreApplication.processEvents()
    
    # Also call update_counters after any data changes. Add this to refresh_model:
    def refresh_model(self):
        """
        Simplified refresh that only updates data, not styling.
        Styling is handled by the HighlightDelegate.
        """
        self._suppress_model_change = True
        
        try:
            self.model.blockSignals(True)
            self.model.setRowCount(0)
            
            # Recompute duplicates for delegate to use
            self.recompute_duplicates()
            
            # Rebuild each row
            for game_idx, game in enumerate(self.games):
                row_items = []
                
                # Create items for each column
                for col in range(len(self.COLUMN_KEYS)):
                    key = self.COLUMN_KEYS[col]
                    
                    # Special field handling
                    if key == "patch_version":
                        value = game.get("patch_version", "") or \
                                game.get("original_title_version", "")
                    elif key == "screenshots":
                        value = ", ".join(game.get("screenshots", []) or \
                                         game.get("shortcut_links", []))
                    elif key == "trailers":
                        value = ", ".join(game.get("trailers", []) or \
                                         game.get("videos", []))
                    elif key == "microtrailers":
                        value = ", ".join(game.get("microtrailers", []))
                    elif key == "played":
                        value = ""  # Handled separately as checkbox
                    else:
                        value = game.get(key, "")
                    
                    row_items.append(QStandardItem(str(value)))
                
                # Attach full game data to title cell
                row_items[0].setData(game, Qt.UserRole)
                
                # Setup played checkbox
                row_items[self.COL_PLAYED].setCheckable(True)
                is_played = game.get("played", False)
                row_items[self.COL_PLAYED].setCheckState(
                    Qt.Checked if is_played else Qt.Unchecked
                )
                
                # Make all cells editable except played (which is checkbox)
                for col_idx, item in enumerate(row_items):
                    item.setEditable(col_idx != self.COL_PLAYED)
                
                # Add row to model
                self.model.appendRow(row_items)
            
            self.model.blockSignals(False)
            
        finally:
            self._suppress_model_change = False
        
        # Refresh UI components
        self.proxy.invalidate()
        self.proxy.invalidateFilter()
        self.apply_filters()
        
        # REMOVED: self.table.resizeColumnsToContents()
        # This preserves user-adjusted column widths
        
        # Force repaint to apply highlighting
        self.table.viewport().update()
        
        # UPDATE COUNTERS HERE TOO
        self.update_counters()
        
        # Debug: Show counts
        played_count = sum(1 for g in self.games if g.get("played", False))
        print(f"[HIGHLIGHT] Model refreshed: {len(self.games)} games, {played_count} played")
        print(f"[HIGHLIGHT] Duplicate titles: {len(self._dup_title_set)}")
        print(f"[HIGHLIGHT] Duplicate Steam IDs: {len(self._dup_steamid_set)}")
    
    def force_refresh_model(self):
        """
        Force a complete refresh of the model and UI.
        """
        try:
            # Block signals to prevent recursive updates
            self._suppress_model_change = True
            self.model.blockSignals(True)
            
            # Recompute duplicates
            self.recompute_duplicates()
            
            # Update all rows
            for row in range(self.model.rowCount()):
                # Update title cell with current game data
                if row < len(self.games):
                    game = self.games[row]
                    title_item = self.model.item(row, 0)
                    if title_item:
                        title_item.setData(game, Qt.UserRole)
            
            self.model.blockSignals(False)
            self._suppress_model_change = False
            
            # Invalidate filters
            self.proxy.invalidate()
            self.proxy.invalidateFilter()
            
            # Force table update
            self.table.viewport().update()
            
            # Update counters
            self.update_counters()
            
            # Update current selection if any
            selected_rows = self._selected_source_rows()
            if selected_rows:
                self.show_details_for_source_row(selected_rows[0])
                
        except Exception as e:
            print(f"[ERROR] force_refresh_model failed: {e}")
            # Fall back to normal refresh
            self.refresh_model()
    
    # ============================================================================
    # SCRAPING AND METADATA METHODS
    # ============================================================================
     
    def _process_pending_manual_matches(self, stats: dict):
        """
        Process all pending manual matches after all chunks are done.
        """
        if not hasattr(self, '_pending_manual_matches') or not self._pending_manual_matches:
            # No pending matches, finish scraping
            print("[PENDING_MATCHES] No pending matches, finishing scraping")
            self._finish_scraping(stats)
            return
        
        print(f"[PENDING_MATCHES] Processing {len(self._pending_manual_matches)} pending manual matches")
        self.status.setText(f"Processing {len(self._pending_manual_matches)} manual matches...")
        
        # Process each pending match
        pending_count = len(self._pending_manual_matches)
        processed_count = 0
        
        for row_index, match_info in list(self._pending_manual_matches.items()):
            if match_info.get("processed"):
                continue
                
            game = match_info["game"]
            candidates = match_info["candidates"]
            
            print(f"[PENDING_MATCHES] Opening dialog for row {row_index}")
            
            # Create and show dialog (modal this time)
            dlg = MatchDialog(
                {
                    "title": game.get("title", ""),
                    "original_title": game.get("original_title", ""),
                    "description": game.get("description", "")
                },
                candidates,
                parent=self
            )
            
            # Show dialog (modal to process one at a time)
            result = dlg.exec_()
            
            if result == QDialog.Accepted:
                result_data = dlg.result_dict or {}
                chosen = result_data.get("chosen_candidate") or {}
                chosen_appid = chosen.get("id") or chosen.get("app_id")
                
                if chosen_appid:
                    try:
                        # Extract all necessary data from result_data
                        selected_title = result_data.get('title') or chosen.get('name') or game.get("title", "")
                        selected_igdb_id = result_data.get('igdb_id')
                        selected_app_id = result_data.get('app_id') or result_data.get('steam_id')
                        
                        # Convert "N/A" to None
                        if selected_igdb_id == "N/A" or selected_igdb_id == "":
                            selected_igdb_id = None
                        if selected_app_id == "N/A" or selected_app_id == "":
                            selected_app_id = None
                        
                        print(f"[MANUAL_MATCH] Scraping with:")
                        print(f"  Title: '{selected_title}'")
                        print(f"  IGDB ID: '{selected_igdb_id}'")
                        print(f"  Steam AppID: '{selected_app_id}'")
                        
                        # Call scrape_igdb_then_steam with all parameters
                        meta = scraping.scrape_igdb_then_steam(
                            igdb_id=selected_igdb_id,  # Pass IGDB ID
                            title=selected_title,      # Pass title
                            auto_accept_score=92,
                            fetch_pcgw_save=False,
                            steam_app_id=selected_app_id  # Pass Steam AppID
                        ) or {}
                        
                        print(f"[MANUAL_MATCH] Scraping returned {len(meta)} metadata fields")
                        
                        if meta and "__candidates__" not in meta:
                            self._merge_and_apply_metadata(row_index, meta)
                            stats["successful"] += 1
                            print(f"[MANUAL_MATCH] Successfully processed row {row_index}")
                        else:
                            stats["failed"] += 1
                            print(f"[MANUAL_MATCH] Failed to scrape metadata for row {row_index}")
                    except Exception as e:
                        stats["failed"] += 1
                        print(f"[MANUAL_MATCH] Error processing row {row_index}: {e}")
                else:
                    stats["failed"] += 1
                    print(f"[PENDING_MATCHES] No valid candidate selected for row {row_index}")
            else:
                stats["failed"] += 1
                print(f"[PENDING_MATCHES] Dialog cancelled for row {row_index}")
            
            # Mark as processed
            match_info["processed"] = True
            processed_count += 1
            
            # Update status
            self.status.setText(f"Processed {processed_count}/{pending_count} manual matches...")
            QCoreApplication.processEvents()
        
        # After processing all, finish
        print(f"[PENDING_MATCHES] All manual matches processed, finishing scraping")
        self._finish_scraping(stats)
 
 
    def test_scrape_single(self):
        """Test scraping a single game to debug."""
        rows = self._selected_source_rows()
        if not rows:
            QMessageBox.information(self, "Test", "Select a game first")
            return
        
        row = rows[0]
        game = self.games[row]
        title = game.get("title") or game.get("original_title") or ""
        
        print(f"\n{'='*80}")
        print(f"TESTING SINGLE SCRAPE: Row {row} - '{title}'")
        print(f"Before: app_id={game.get('app_id')}, developer={game.get('developer')}")
        
        try:
            meta = scraping.scrape_igdb_then_steam(
                None,  # igdb_id - will be auto-detected
                title,
                auto_accept_score=92,
                fetch_pcgw_save=False
            ) or {}
            
            print(f"Metadata returned: {list(meta.keys())}")
            print(f"Metadata content: {meta}")
            
            # Apply the metadata
            self._merge_and_apply_metadata(row, meta)
            
            print(f"After: app_id={game.get('app_id')}, developer={game.get('developer')}")
            
        except Exception as e:
            print(f"Error: {e}")
        
        print(f"{'='*80}\n")
 
    # In the GameManager class, replace the existing scrape_all method with this:

    def scrape_all(self, auto_accept_score: int = 92):
        """
        Simplified scraping using existing ScrapeBatchWorker and MatchDialog.
        Processes in small batches to prevent crashes with 1000+ items.
        """
        
        print(f"\n{'='*80}")
        print("SCRAPE_ALL: Starting new scrape session")
        print(f"{'='*80}")
        
        from PyQt5.QtCore import QTimer
        
        print(f"[SCRAPE] Starting scrape_all with {len(self.games)} total games")
        
        if not self.games:
            self.status.setText("No titles to scrape.")
            return
        
        # Find rows that need scraping
        rows_to_process = [
            i for i, g in enumerate(self.games)
            if not str(g.get("app_id") or "").strip()
        ]
        
        print(f"[SCRAPE] Found {len(rows_to_process)} games without Steam IDs")
        
        if not rows_to_process:
            self.status.setText("All rows already have Steam ID.")
            QMessageBox.information(self, "Scrape Complete", 
                                  "All games already have Steam IDs. Nothing to scrape.")
            return
        
        self._scrape_stats = {
            "total": len(rows_to_process),
            "successful": 0,
            "failed": 0,
            "manual_needed": 0,
            "start_time": time.time()
        }

        # Initialize dialog tracking
        if not hasattr(self, '_pending_manual_matches'):
            self._pending_manual_matches = {}  # row_index -> True if waiting for manual match
        if not hasattr(self, '_active_match_dialogs'):
            self._active_match_dialogs = []  # List of currently open dialogs
        
        # UI setup
        self.scrape_btn.setEnabled(False)
        self.cancel_scrape_btn.setVisible(True)
        self._cancel_current_scrape = False
        
        # Process in smaller chunks (50 at a time for stability)
        total_chunks = (len(rows_to_process) + CHUNK_SIZE - 1) // CHUNK_SIZE
        
        # Store all chunks
        self._remaining_chunks = []
        for i in range(0, len(rows_to_process), CHUNK_SIZE):
            chunk = rows_to_process[i:i + CHUNK_SIZE]
            self._remaining_chunks.append(chunk)
        
        print(f"[SCRAPE] Created {len(self._remaining_chunks)} chunks of size {CHUNK_SIZE}")
        print(f"[SCRAPE] Chunks: {self._remaining_chunks}")
        
        # Start with first chunk
        if self._remaining_chunks:
            self._start_scrape_chunk(self._remaining_chunks.pop(0), self._scrape_stats, 
                                   auto_accept_score, total_chunks)
        else:
            # This shouldn't happen but handle it
            print("[SCRAPE] No chunks to process, finishing immediately")
            self._finish_scraping(self._scrape_stats)

    def _start_scrape_chunk(self, chunk: list, stats: dict, auto_accept_score: int, total_chunks: int):
        """
        Start scraping a chunk of rows using ScrapeBatchWorker.
        """
        print(f"[SCRAPE_CHUNK] Starting chunk with {len(chunk)} rows: {chunk[:5]}...")
        
        # Check for cancellation
        if self._cancel_current_scrape:
            print("[SCRAPE_CHUNK] Cancellation requested, skipping chunk")
            self._finish_scraping(stats)
            return
        
        if not chunk:
            print("[SCRAPE_CHUNK] Empty chunk, skipping")
            self._finish_scraping(stats)
            return
        
        # Calculate current chunk number
        current_chunk = total_chunks - len(self._remaining_chunks)
        
        # Update status
        self.status.setText(
            f"Scraping chunk {current_chunk}/{total_chunks} "
            f"({len(chunk)} games)..."
        )
        
        # Create worker
        worker = ScrapeBatchWorker(chunk, self.games)
        thread = QThread(self)
        worker.moveToThread(thread)
        
        # Setup stall timer (for progress updates only, not for timeout)
        stall_timer = QTimer(self)
        stall_timer.setInterval(STALL_TIMEOUT * 1000)
        stall_timer.setSingleShot(True)
        
        # Signal handlers
        def on_row_started(row_index, total, title):
            stall_timer.start()
            if self._cancel_current_scrape:
                worker.cancelled = True
            
            print(f"[SCRAPE_ROW] Starting row {row_index}/{total}: {title[:50]}")
            
            # Minimal status update (every 10 rows)
            if row_index % 10 == 0:
                self.status.setText(f"Chunk {current_chunk}/{total_chunks}: {row_index+1}/{len(chunk)} - {title[:40]}")
        
        def on_row_finished(row_index, metadata):
            stall_timer.start()
            
            if "__candidates__" in metadata:
                print(f"[SCRAPE_ROW] Row {row_index} needs manual match ({len(metadata['__candidates__'])} candidates)")
                
                # Mark as pending manual match
                if not hasattr(self, '_pending_manual_matches'):
                    self._pending_manual_matches = {}
                self._pending_manual_matches[row_index] = {
                    "game": self.games[row_index],
                    "candidates": metadata["__candidates__"],
                    "processed": False
                }
                
                print(f"[SCRAPE_ROW] Marked row {row_index} for manual match")
                
            elif metadata:  # Has metadata (successful scrape)
                try:
                    print(f"[SCRAPE_ROW] Processing successful scrape for row {row_index}: {list(metadata.keys())}")
                    self._merge_and_apply_metadata(row_index, metadata)
                    stats["successful"] += 1
                    print(f"[SCRAPE_ROW] Successfully merged metadata for row {row_index}")
                except Exception as e:
                    stats["failed"] += 1
                    print(f"[SCRAPE_ROW] Failed to merge metadata for row {row_index}: {e}")
            else:
                # Empty metadata - queue for manual match
                print(f"[SCRAPE_ROW] Row {row_index} returned empty metadata - queuing for manual match")
                
                if not hasattr(self, '_pending_manual_matches'):
                    self._pending_manual_matches = {}
                self._pending_manual_matches[row_index] = {
                    "game": self.games[row_index],
                    "candidates": [],
                    "processed": False
                }
                
                print(f"[SCRAPE_ROW] Queued row {row_index} for manual match")
                
        def on_finished(total_processed):
            stall_timer.stop()
            print(f"[SCRAPE_CHUNK] Chunk {current_chunk}/{total_chunks} finished, processed {total_processed} rows")
            print(f"[SCRAPE_CHUNK] Stats: successful={stats['successful']}, failed={stats['failed']}, manual={stats['manual_needed']}")
            
            # Clean up thread
            thread.quit()
            thread.wait(1000)
            
            # Process next chunk if available
            if self._remaining_chunks and not self._cancel_current_scrape:
                next_chunk = self._remaining_chunks.pop(0)
                print(f"[SCRAPE_CHUNK] Starting next chunk ({len(self._remaining_chunks)} remaining)")
                # Small delay between chunks
                QTimer.singleShot(500, lambda: self._start_scrape_chunk(
                    next_chunk, stats, auto_accept_score, total_chunks
                ))
            else:
                # No more chunks, check if we have pending manual matches
                has_pending_matches = hasattr(self, '_pending_manual_matches') and self._pending_manual_matches
                print(f"[SCRAPE_CHUNK] No more chunks, checking pending manual matches: {has_pending_matches}")
                
                if has_pending_matches:
                    print(f"[SCRAPE_CHUNK] Found {len(self._pending_manual_matches)} pending manual matches, processing them...")
                    # Process pending manual matches
                    QTimer.singleShot(500, lambda: self._process_pending_manual_matches(stats))
                else:
                    print(f"[SCRAPE_CHUNK] No pending manual matches, finishing scraping")
                    self._finish_scraping(stats)
                    
        def on_error(error_message):
            stall_timer.stop()
            print(f"[SCRAPE_CHUNK] Error in chunk {current_chunk}: {error_message}")
            self.status.setText(f"Error: {error_message}")
            
            # Continue with next chunk if possible
            if self._remaining_chunks and not self._cancel_current_scrape:
                next_chunk = self._remaining_chunks.pop(0)
                QTimer.singleShot(500, lambda: self._start_scrape_chunk(
                    next_chunk, stats, auto_accept_score, total_chunks
                ))
            else:
                self._finish_scraping(stats)
        
        def on_stall():
            print(f"[SCRAPE_CHUNK] Chunk {current_chunk} appears stalled")
            self.status.setText("Processing appears slow... Click Cancel if needed.")
        
        def cancel_handler():
            print(f"[SCRAPE_CHUNK] Cancellation requested for chunk {current_chunk}")
            worker.cancelled = True
            self._cancel_current_scrape = True
            self.status.setText("Cancelling...")
            # Force immediate UI update
            self.scrape_btn.setEnabled(True)
            self.cancel_scrape_btn.setVisible(False)
            QCoreApplication.processEvents()
            
            # Close all open dialogs
            if hasattr(self, '_active_match_dialogs'):
                for dlg in self._active_match_dialogs[:]:
                    try:
                        dlg.close()
                        dlg.deleteLater()
                    except Exception:
                        pass
                self._active_match_dialogs = []
            
            # Mark remaining manual matches as cancelled
            if stats["manual_needed"] > 0:
                stats["failed"] += stats["manual_needed"]
                stats["manual_needed"] = 0
            
            # Force finish
            self._finish_scraping(stats)
        
        # Connect signals
        worker.row_started.connect(on_row_started)
        worker.row_finished.connect(on_row_finished)
        worker.finished.connect(on_finished)
        worker.error.connect(on_error)
        
        stall_timer.timeout.connect(on_stall)
        self.cancel_scrape_btn.clicked.connect(cancel_handler)
        
        # Start thread
        thread.started.connect(worker.run)
        thread.start()
        
        # Store references
        self._current_batch_worker = worker
        self._current_batch_thread = thread
        self._stall_timer = stall_timer
        
        print(f"[SCRAPE_CHUNK] Chunk {current_chunk} worker started")

    def _finish_scraping(self, stats: dict):
        """
        Clean up and show simple report when scraping finishes.
        """
        print(f"[FINISH_SCRAPING] Starting cleanup. Stats: {stats}")
        # Clean up pending matches tracking
        if hasattr(self, '_pending_manual_matches'):
            del self._pending_manual_matches
        if hasattr(self, '_active_match_dialogs'):
            del self._active_match_dialogs
        # Always restore UI
        try:
            self.scrape_btn.setEnabled(True)
            self.cancel_scrape_btn.setVisible(False)
            print("[FINISH_SCRAPING] UI state restored")
        except Exception as e:
            print(f"[FINISH_SCRAPING] Error restoring UI: {e}")
        
        # Clean up any open match dialogs
        if hasattr(self, '_active_match_dialogs'):
            print(f"[FINISH_SCRAPING] Closing {len(self._active_match_dialogs)} open match dialogs")
            for dlg in self._active_match_dialogs[:]:
                try:
                    dlg.close()
                    dlg.deleteLater()
                except Exception:
                    pass
            self._active_match_dialogs = []
        
        # Refresh model
        print("[FINISH_SCRAPING] Refreshing model...")
        self.refresh_model()
        
        # DEBUG: Save data to check what was scraped
        print("[FINISH_SCRAPING] Saving debug data...")
        self.save_and_check_data("scraping_debug.json")
        
        # Calculate success rate
        success_rate = (stats["successful"] / stats["total"] * 100) if stats["total"] > 0 else 0
        
        # Simple report
        report_msg = f"""
        ╔══════════════════════════════════════════════╗
        ║            SCRAPING COMPLETE                ║
        ╚══════════════════════════════════════════════╝
        
        📊 SUMMARY
        • Total Processed: {stats['total']} games
        • Successful: {stats['successful']}
        • Failed: {stats['failed']}
        • Manual Matches Needed: {stats['manual_needed']}
        • Success Rate: {success_rate:.1f}%
        """
        
        msg_box = QMessageBox(self)
        msg_box.setWindowTitle("Scraping Results")
        msg_box.setText(report_msg)
        msg_box.setIcon(QMessageBox.Information)
        msg_box.exec_()
        
        # Update final status
        if self._cancel_current_scrape:
            final_msg = f"Cancelled - {stats['successful']} successful, {stats['failed']} failed"
        else:
            final_msg = f"Complete: {stats['successful']} successful, {stats['failed']} failed, {stats['manual_needed']} manual"
        
        self.status.setText(final_msg)
        print(f"[FINISH_SCRAPING] {final_msg}")
        
        # Reset flags
        self._cancel_current_scrape = False
        
        # Clear remaining chunks
        if hasattr(self, '_remaining_chunks'):
            print(f"[FINISH_SCRAPING] Clearing {len(self._remaining_chunks)} remaining chunks")
            self._remaining_chunks = []
        
        # Force UI update
        QCoreApplication.processEvents()
        print("[FINISH_SCRAPING] Cleanup complete")
            

    def save_and_check_data(self, filename="debug_check.json"):
        """
        Save current games data to file for debugging.
        """
        import json
        try:
            # Create a simple summary
            summary = []
            for i, game in enumerate(self.games):
                summary.append({
                    "row": i,
                    "title": game.get("title", ""),
                    "app_id": game.get("app_id", ""),
                    "developer": game.get("developer", ""),
                    "publisher": game.get("publisher", ""),
                    "genres": game.get("genres", ""),
                    "has_metadata": any(game.get(k) for k in ["developer", "publisher", "genres", "release_date"])
                })
            
            # Save to file
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)
            
            print(f"[DEBUG] Data saved to {filename}")
            print(f"[DEBUG] Total games: {len(self.games)}")
            
            # Count games with metadata
            with_metadata = sum(1 for g in self.games if any(g.get(k) for k in ["developer", "publisher", "genres", "release_date"]))
            print(f"[DEBUG] Games with metadata: {with_metadata}/{len(self.games)}")
            
            return True
        except Exception as e:
            print(f"[DEBUG] Error saving data: {e}")
            return False


    #===============================================================================
    #Download resources
    #===========================================================================    
# ============================================================================
# DOWNLOAD RESOURCES - IMPROVED VERSION
# ============================================================================

    def download_all_screenshots(self):
        """
        Download screenshots and microtrailers for all games.
        FIRST: Check cache directory for existing assets
        ONLY THEN: Download missing assets
        """
        from PyQt5.QtCore import QCoreApplication
        from PyQt5.QtWidgets import QMessageBox, QProgressDialog
        import time
        import os
        
        if not self.games:
            QMessageBox.information(self, "Download Resources", "No games to process.")
            return
        
        # Create progress dialog
        progress = QProgressDialog(
            "Checking cache and downloading resources...", 
            "Cancel", 
            0, 
            len(self.games), 
            self
        )
        progress.setWindowTitle("Download Resources")
        progress.setWindowModality(Qt.WindowModal)
        progress.setMinimumDuration(500)
        
        # Track statistics
        stats = {
            "total_games": len(self.games),
            "processed_games": 0,
            "screenshots_downloaded": 0,
            "screenshots_existing": 0,
            "screenshots_failed": 0,
            "microtrailers_downloaded": 0,
            "microtrailers_existing": 0,
            "microtrailers_failed": 0,
            "start_time": time.time()
        }
        
        self._cancel_current_scrape = False
        
        # Process each game
        for game_idx, game in enumerate(self.games):
            if self._cancel_current_scrape:
                break
            
            progress.setValue(game_idx)
            progress.setLabelText(f"Processing game {game_idx + 1}/{len(self.games)}")
            QCoreApplication.processEvents()
            
            title = game.get("title") or game.get("original_title") or f"Game {game_idx}"
            print(f"\n[DOWNLOAD] Processing: '{title}'")
            
            # ================================================================
            # STEP 1: SCAN CACHE DIRECTORY FOR EXISTING ASSETS
            # ================================================================
            cache_scan_result = self._scan_cache_directory_for_game(game)
            
            # Update game dict with found cache paths
            if cache_scan_result["screenshot_paths"]:
                game["image_cache_paths"] = cache_scan_result["screenshot_paths"]
            
            if cache_scan_result["microtrailer_path"]:
                game["microtrailer_cache_path"] = cache_scan_result["microtrailer_path"]
            
            stats["screenshots_existing"] += len(cache_scan_result["screenshot_paths"])
            stats["microtrailers_existing"] += (1 if cache_scan_result["microtrailer_path"] else 0)
            
            print(f"[DOWNLOAD] Found in cache: {len(cache_scan_result['screenshot_paths'])} screenshots, "
                  f"{1 if cache_scan_result['microtrailer_path'] else 0} microtrailers")
            
            # ================================================================
            # STEP 2: DOWNLOAD MISSING MICROTRAILER (LIMIT: 1 PER GAME)
            # ================================================================
            if not cache_scan_result["microtrailer_path"]:  # Only download if not found in cache
                microtrailer_result = self._download_missing_microtrailer(game_idx, game)
                
                if microtrailer_result == "downloaded":
                    stats["microtrailers_downloaded"] += 1
                    print(f"[DOWNLOAD] Downloaded microtrailer for '{title}'")
                elif microtrailer_result == "failed":
                    stats["microtrailers_failed"] += 1
                    print(f"[DOWNLOAD] Failed to download microtrailer for '{title}'")
            
            # ================================================================
            # STEP 3: DOWNLOAD MISSING SCREENSHOTS (WITH LIMIT)
            # ================================================================
            # Calculate how many more screenshots we can download up to the limit
            cached_screenshots = len(cache_scan_result["screenshot_paths"])
            max_to_download = MAX_IMAGES_TO_DOWNLOAD - cached_screenshots
            
            if max_to_download > 0:
                screenshots_downloaded = self._download_missing_screenshots(
                    game_idx, game, max_to_download
                )
                stats["screenshots_downloaded"] += screenshots_downloaded
                stats["screenshots_failed"] += max_to_download - screenshots_downloaded
            else:
                print(f"[DOWNLOAD] Screenshot limit reached for '{title}' "
                      f"({cached_screenshots}/{MAX_IMAGES_TO_DOWNLOAD})")
            
            stats["processed_games"] += 1
            
            # Update the model for this game
            self._update_game_cache_fields(game_idx, game)
            
            # Small delay to prevent overwhelming the network
            time.sleep(0.05)
        
        # Cleanup
        progress.close()
        
        # Show summary
        elapsed_time = time.time() - stats["start_time"]
        minutes = int(elapsed_time // 60)
        seconds = int(elapsed_time % 60)
        
        total_screenshots = stats["screenshots_downloaded"] + stats["screenshots_existing"]
        total_microtrailers = stats["microtrailers_downloaded"] + stats["microtrailers_existing"]
        
        summary = f"""
        ╔══════════════════════════════════════════════╗
        ║           DOWNLOAD COMPLETE                  ║
        ╚══════════════════════════════════════════════╝
        
        📊 STATISTICS
        • Games Processed: {stats['processed_games']}/{stats['total_games']}
        • Time: {minutes:02d}:{seconds:02d}
        
        🖼️  SCREENSHOTS (Limit: {MAX_IMAGES_TO_DOWNLOAD} per game)
        • Already in Cache: {stats['screenshots_existing']}
        • Newly Downloaded: {stats['screenshots_downloaded']}
        • Total Available: {total_screenshots}
        • Failed: {stats['screenshots_failed']}
        
        🎬 MICROTRAILERS (Limit: {MAX_MICROTRAILERS} per game)
        • Already in Cache: {stats['microtrailers_existing']}
        • Newly Downloaded: {stats['microtrailers_downloaded']}
        • Total Available: {total_microtrailers}
        • Failed: {stats['microtrailers_failed']}
        """
        
        # Show summary
        msg_box = QMessageBox(self)
        msg_box.setWindowTitle("Download Complete")
        msg_box.setText(summary)
        msg_box.setIcon(QMessageBox.Information)
        msg_box.setMinimumWidth(500)
        msg_box.exec_()
        
        # Refresh the model to show updated cache paths
        self.refresh_model()
        self.status.setText(
            f"Download complete: {stats['screenshots_downloaded']} new screenshots, "
            f"{stats['microtrailers_downloaded']} new microtrailers"
        )

    def _scan_cache_directory_for_game(self, game: dict) -> dict:
        """
        Scan the cache directory for existing assets for this game.
        
        Returns:
            Dictionary with:
            - screenshot_paths: List of existing screenshot cache paths
            - microtrailer_path: Existing microtrailer cache path (if any)
        """
        result = {
            "screenshot_paths": [],
            "microtrailer_path": ""
        }
        
        try:
            # Get game-specific cache directory
            cache_dir = _game_cache_dir_for_game(game)
            
            if not cache_dir.exists():
                print(f"[SCAN CACHE] Cache directory doesn't exist: {cache_dir}")
                return result
            
            # List all files in the cache directory
            all_files = list(cache_dir.iterdir())
            print(f"[SCAN CACHE] Found {len(all_files)} files in {cache_dir}")
            
            # Get game's screenshot URLs to match against
            screenshot_urls = []
            
            # Cover URL
            cover_url = game.get("cover_url")
            if cover_url:
                screenshot_urls.append(cover_url)
            
            # Screenshot URLs
            screenshots = game.get("screenshots") or []
            if isinstance(screenshots, list):
                screenshot_urls.extend(screenshots)
            elif isinstance(screenshots, str):
                parts = [p.strip() for p in screenshots.split(",") if p.strip()]
                screenshot_urls.extend(parts)
            
            # Get microtrailer URLs
            microtrailer_urls = []
            
            # trailer_webm field
            if game.get("trailer_webm"):
                microtrailer_urls.append(game["trailer_webm"])
            
            # microtrailers field
            microtrailers = game.get("microtrailers") or []
            if isinstance(microtrailers, list):
                microtrailer_urls.extend(microtrailers)
            elif isinstance(microtrailers, str):
                parts = [p.strip() for p in microtrailers.split(",") if p.strip()]
                microtrailer_urls.extend(parts)
            
            print(f"[SCAN CACHE] Game has {len(screenshot_urls)} screenshot URLs, {len(microtrailer_urls)} microtrailer URLs")
            
            # Check each file in cache directory
            for file_path in all_files:
                if not file_path.is_file():
                    continue
                
                # Skip temporary files
                if file_path.suffix == ".tmp":
                    continue
                
                # Get file size
                try:
                    file_size = file_path.stat().st_size
                    if file_size < CACHE_MIN_KB * 1024:
                        continue  # File too small
                except Exception:
                    continue
                
                # Get file name without extension for hash matching
                file_stem = file_path.stem
                
                # Check if this file matches any screenshot URL
                for url in screenshot_urls:
                    if not url:
                        continue
                    
                    # Generate URL hash
                    url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
                    
                    # Check if hash is in file name
                    if url_hash in file_stem:
                        # Convert to relative path
                        rel_path = _to_relative(file_path)
                        
                        # Add to screenshot paths if not already there
                        if rel_path not in result["screenshot_paths"]:
                            result["screenshot_paths"].append(rel_path)
                            print(f"[SCAN CACHE] Matched screenshot: {url} -> {file_path.name}")
                        break  # Move to next file
                
                # Check if this file matches any microtrailer URL
                for url in microtrailer_urls:
                    if not url:
                        continue
                    
                    # Generate URL hash
                    url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
                    
                    # Check if hash is in file name
                    if url_hash in file_stem:
                        # Check if it's a video/GIF file
                        if file_path.suffix.lower() in ['.gif', '.webm', '.mp4']:
                            # Convert to relative path
                            rel_path = _to_relative(file_path)
                            
                            # Set microtrailer path
                            if not result["microtrailer_path"]:
                                result["microtrailer_path"] = rel_path
                                print(f"[SCAN CACHE] Matched microtrailer: {url} -> {file_path.name}")
                        break  # Move to next file
            
            print(f"[SCAN CACHE] Results: {len(result['screenshot_paths'])} screenshots, "
                  f"{'1 microtrailer' if result['microtrailer_path'] else '0 microtrailers'}")
            
        except Exception as e:
            print(f"[SCAN CACHE] Error scanning cache: {e}")
        
        return result

    
    def _check_and_update_existing_assets(self, game: dict) -> dict:
        """
        Check for existing cached assets and update game cache paths.
        
        Returns:
            Dictionary with counts of existing screenshots and microtrailers
        """
        existing_assets = {
            "screenshots": 0,
            "microtrailers": 0
        }
        
        valid_cache_paths = []
        
        # ================================================================
        # CHECK EXISTING SCREENSHOT CACHE PATHS
        # ================================================================
        cache_paths = game.get("image_cache_paths", [])
        if cache_paths:
            if isinstance(cache_paths, str):
                # Handle comma-separated string
                cache_paths = [p.strip() for p in cache_paths.split(",") if p.strip()]
            elif not isinstance(cache_paths, list):
                cache_paths = []
        
        # Verify each cache path exists
        for cache_path in cache_paths:
            if not cache_path:
                continue
                
            try:
                # Convert to absolute path
                if isinstance(cache_path, str):
                    if not os.path.isabs(cache_path):
                        # Try to resolve relative path
                        abs_path = SCRIPT_DIR / cache_path
                    else:
                        abs_path = Path(cache_path)
                    
                    # Check if file exists
                    if abs_path.exists() and abs_path.is_file():
                        file_size = abs_path.stat().st_size
                        
                        # Check if file meets minimum size requirement
                        min_bytes = CACHE_MIN_KB * 1024
                        if file_size >= min_bytes:
                            # Add to valid paths (use relative path for storage)
                            rel_path = _to_relative(abs_path)
                            if rel_path not in valid_cache_paths:
                                valid_cache_paths.append(rel_path)
                                existing_assets["screenshots"] += 1
                                print(f"[EXISTING] Found cached screenshot: {rel_path} "
                                      f"({file_size} bytes)")
                            else:
                                print(f"[EXISTING] Duplicate cache path: {rel_path}")
                        else:
                            print(f"[EXISTING] File too small, skipping: {abs_path} "
                                  f"({file_size} bytes < {min_bytes} bytes)")
                    else:
                        print(f"[EXISTING] File doesn't exist or not a file: {abs_path}")
                        
            except Exception as e:
                print(f"[EXISTING] Error checking cache path '{cache_path}': {e}")
        
        # ================================================================
        # CHECK EXISTING MICROTRAILER CACHE PATH
        # ================================================================
        microtrailer_path = game.get("microtrailer_cache_path", "")
        valid_microtrailer_path = ""
        
        if microtrailer_path:
            try:
                # Convert to absolute path
                if isinstance(microtrailer_path, str):
                    if not os.path.isabs(microtrailer_path):
                        abs_path = SCRIPT_DIR / microtrailer_path
                    else:
                        abs_path = Path(microtrailer_path)
                    
                    # Check if file exists
                    if abs_path.exists() and abs_path.is_file():
                        file_size = abs_path.stat().st_size
                        
                        # Check if file meets minimum size requirement
                        min_bytes = CACHE_MIN_KB * 1024
                        if file_size >= min_bytes:
                            valid_microtrailer_path = _to_relative(abs_path)
                            existing_assets["microtrailers"] = 1
                            print(f"[EXISTING] Found cached microtrailer: {valid_microtrailer_path} "
                                  f"({file_size} bytes)")
                            
                            # Add to valid cache paths if not already there
                            if valid_microtrailer_path not in valid_cache_paths:
                                valid_cache_paths.append(valid_microtrailer_path)
                        else:
                            print(f"[EXISTING] Microtrailer file too small: {abs_path} "
                                  f"({file_size} bytes < {min_bytes} bytes)")
                    else:
                        print(f"[EXISTING] Microtrailer file doesn't exist: {abs_path}")
                        
            except Exception as e:
                print(f"[EXISTING] Error checking microtrailer path '{microtrailer_path}': {e}")
        
        # ================================================================
        # UPDATE GAME CACHE FIELDS WITH VERIFIED PATHS
        # ================================================================
        # Limit screenshot paths to MAX_IMAGES_TO_DISPLAY
        display_limit = MAX_IMAGES_TO_DISPLAY
        
        # Update image cache paths
        if valid_cache_paths:
            game["image_cache_paths"] = valid_cache_paths[:display_limit]
            print(f"[UPDATE] Updated image_cache_paths: {len(valid_cache_paths[:display_limit])} paths")
        else:
            game["image_cache_paths"] = []
        
        # Update microtrailer cache path
        if valid_microtrailer_path:
            game["microtrailer_cache_path"] = valid_microtrailer_path
            print(f"[UPDATE] Updated microtrailer_cache_path: {valid_microtrailer_path}")
        elif "microtrailer_cache_path" in game:
            del game["microtrailer_cache_path"]
        
        return existing_assets

    def _download_missing_microtrailer(self, game_idx: int, game: dict) -> str:
        """
        Download missing microtrailer if not already cached.
        
        Returns:
            "downloaded" - Successfully downloaded
            "failed" - Failed to download
            "skipped" - Already cached or no URL
        """
        # Check if we already have a microtrailer (should have been caught by _check_and_update_existing_assets)
        if game.get("microtrailer_cache_path"):
            return "skipped"
        
        # Get microtrailer URLs
        microtrailer_urls = []
        
        # Check trailer_webm field
        if game.get("trailer_webm"):
            microtrailer_urls.append(game["trailer_webm"])
        
        # Check microtrailers field
        microtrailers = game.get("microtrailers") or []
        if isinstance(microtrailers, list):
            microtrailer_urls.extend(microtrailers[:MAX_MICROTRAILERS])
        elif isinstance(microtrailers, str):
            # Try to parse comma-separated list
            parts = [p.strip() for p in microtrailers.split(",") if p.strip()]
            microtrailer_urls.extend(parts[:MAX_MICROTRAILERS])
        
        # Remove duplicates and empty URLs
        microtrailer_urls = [url for url in set(microtrailer_urls) if url]
        
        if not microtrailer_urls:
            print(f"[MISSING] No microtrailer URLs for {game.get('title', 'Unknown')}")
            return "skipped"
        
        # Download the first microtrailer
        for url in microtrailer_urls[:MAX_MICROTRAILERS]:
            try:
                # Normalize URL
                if url.startswith("//"):
                    url = "https:" + url
                
                # Check if already cached (by URL hash)
                url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
                
                # Check existing image cache paths
                existing_paths = game.get("image_cache_paths", [])
                for path in existing_paths:
                    if isinstance(path, str) and url_hash in Path(path).stem:
                        print(f"[MISSING] Microtrailer already in cache (by hash): {url}")
                        return "skipped"
                
                # Download the microtrailer
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) GameScraper/1.0"
                }
                
                response = requests.get(url, timeout=30, headers=headers)
                response.raise_for_status()
                
                if not response.content:
                    print(f"[MISSING] Empty response for microtrailer: {url}")
                    continue
                
                # Check size constraints
                data_len = len(response.content)
                min_bytes = CACHE_MIN_KB * 1024
                max_bytes = CACHE_MAX_KB * 1024 if CACHE_MAX_KB else None
                
                if data_len < min_bytes:
                    print(f"[MISSING] Microtrailer too small: {data_len} bytes < {min_bytes} bytes")
                    continue
                
                if max_bytes and data_len > max_bytes:
                    print(f"[MISSING] Microtrailer too large: {data_len} bytes > {max_bytes} bytes")
                    continue
                
                # Save to cache
                try:
                    saved_path = _save_bytes_to_game_cache(game, url, response.content)
                    
                    # Update game's microtrailer_cache_path
                    if hasattr(saved_path, 'as_posix'):
                        saved_path_str = saved_path.as_posix()
                    else:
                        saved_path_str = str(saved_path)
                    
                    game["microtrailer_cache_path"] = saved_path_str
                    
                    # Also add to image_cache_paths for consistency
                    if "image_cache_paths" not in game:
                        game["image_cache_paths"] = []
                    
                    if saved_path_str not in game["image_cache_paths"]:
                        # Ensure we don't exceed the display limit
                        if len(game["image_cache_paths"]) < MAX_IMAGES_TO_DISPLAY:
                            game["image_cache_paths"].append(saved_path_str)
                        else:
                            # Replace oldest entry if we're at limit
                            game["image_cache_paths"].pop(0)
                            game["image_cache_paths"].append(saved_path_str)
                    
                    print(f"[MISSING] Downloaded microtrailer: {url}")
                    return "downloaded"
                    
                except Exception as e:
                    print(f"[MISSING] Failed to save microtrailer: {e}")
                    continue
                    
            except Exception as e:
                print(f"[MISSING] Failed to download microtrailer {url}: {e}")
                continue
        
        return "failed"

    def _download_missing_screenshots(self, game_idx: int, game: dict, max_to_download: int) -> int:
        """
        Download missing screenshots up to the specified limit.
        
        Args:
            game_idx: Index of the game in the games list
            game: Game dictionary
            max_to_download: Maximum number of screenshots to download
        
        Returns:
            Number of screenshots successfully downloaded
        """
        downloaded_count = 0
        
        # Get screenshot URLs
        screenshot_urls = []
        
        # Check cover_url (always include this first)
        cover_url = game.get("cover_url")
        if cover_url:
            screenshot_urls.append(cover_url)
        
        # Check screenshots field
        screenshots = game.get("screenshots") or []
        if isinstance(screenshots, list):
            screenshot_urls.extend(screenshots)
        elif isinstance(screenshots, str):
            # Try to parse comma-separated list
            parts = [p.strip() for p in screenshots.split(",") if p.strip()]
            screenshot_urls.extend(parts)
        
        # Remove duplicates and empty URLs
        screenshot_urls = [url for url in set(screenshot_urls) if url]
        
        if not screenshot_urls:
            print(f"[MISSING] No screenshot URLs for {game.get('title', 'Unknown')}")
            return 0
        
        # Get existing cache paths to avoid re-downloading
        existing_cache_paths = game.get("image_cache_paths", [])
        existing_hashes = set()
        
        for path in existing_cache_paths:
            if isinstance(path, str):
                # Extract hash from filename
                try:
                    filename = Path(path).stem
                    existing_hashes.add(filename)
                except Exception:
                    pass
        
        # Download screenshots with limit
        for i, url in enumerate(screenshot_urls):
            if downloaded_count >= max_to_download:
                print(f"[MISSING] Reached download limit ({max_to_download}) for {game.get('title', 'Unknown')}")
                break
            
            try:
                # Normalize URL
                if url.startswith("//"):
                    url = "https:" + url
                
                # Check if already cached (by URL hash)
                url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
                
                if url_hash in existing_hashes:
                    print(f"[MISSING] Screenshot already cached (by hash): {url}")
                    continue
                
                # Check if this specific URL is already in cached paths
                is_cached = False
                for cache_path in existing_cache_paths:
                    if isinstance(cache_path, str) and url_hash in cache_path:
                        is_cached = True
                        break
                
                if is_cached:
                    print(f"[MISSING] Screenshot already cached: {url}")
                    continue
                
                # Download the screenshot
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) GameScraper/1.0"
                }
                
                response = requests.get(url, timeout=30, headers=headers)
                response.raise_for_status()
                
                if not response.content:
                    print(f"[MISSING] Empty response for screenshot: {url}")
                    continue
                
                # Check size constraints
                data_len = len(response.content)
                min_bytes = CACHE_MIN_KB * 1024
                max_bytes = CACHE_MAX_KB * 1024 if CACHE_MAX_KB else None
                
                if data_len < min_bytes:
                    print(f"[MISSING] Screenshot too small: {data_len} bytes < {min_bytes} bytes")
                    continue
                
                if max_bytes and data_len > max_bytes:
                    print(f"[MISSING] Screenshot too large: {data_len} bytes > {max_bytes} bytes")
                    continue
                
                # Validate it's actually an image by checking headers
                content_type = response.headers.get('content-type', '').lower()
                if content_type and 'image' not in content_type:
                    print(f"[MISSING] Not an image (content-type: {content_type}): {url}")
                    continue
                
                # Save to cache
                try:
                    saved_path = _save_bytes_to_game_cache(game, url, response.content)
                    
                    # Update game's image_cache_paths
                    if "image_cache_paths" not in game:
                        game["image_cache_paths"] = []
                    
                    if hasattr(saved_path, 'as_posix'):
                        saved_path_str = saved_path.as_posix()
                    else:
                        saved_path_str = str(saved_path)
                    
                    if saved_path_str not in game["image_cache_paths"]:
                        # Ensure we don't exceed the display limit
                        if len(game["image_cache_paths"]) < MAX_IMAGES_TO_DISPLAY:
                            game["image_cache_paths"].append(saved_path_str)
                        else:
                            # Replace oldest entry if we're at limit
                            game["image_cache_paths"].pop(0)
                            game["image_cache_paths"].append(saved_path_str)
                    
                    downloaded_count += 1
                    print(f"[MISSING] Downloaded screenshot {downloaded_count}/{max_to_download}: {url}")
                    
                except Exception as e:
                    print(f"[MISSING] Failed to save screenshot: {e}")
                    continue
                    
            except Exception as e:
                print(f"[MISSING] Failed to download screenshot {url}: {e}")
                continue
        
        return downloaded_count
        
    def _update_game_cache_fields(self, game_idx: int, game: dict):
        """
        Update the model with cache fields for a specific game.
        """
        if game_idx >= self.model.rowCount():
            return
        
        # Update image cache paths column
        cache_paths = game.get("image_cache_paths", [])
        if isinstance(cache_paths, list):
            cache_text = ", ".join([str(p) for p in cache_paths if p])
        else:
            cache_text = str(cache_paths)
        
        self.model.setItem(game_idx, self.COL_IMAGE_CACHE_PATHS, 
                          QStandardItem(cache_text))
        
        # Update microtrailer cache path column
        microtrailer_path = game.get("microtrailer_cache_path", "")
        self.model.setItem(game_idx, self.COL_MICROTRAILER_CACHE_PATH,
                          QStandardItem(str(microtrailer_path)))
        
        # Store updated game data in title cell
        title_item = self.model.item(game_idx, self.COL_TITLE)
        if title_item:
            title_item.setData(game, Qt.UserRole)

        
    def _merge_and_apply_metadata(self, row_index: int, metadata: dict):
        """
        Merge scraped metadata into existing game data - UPDATED for user rating and cache preservation.
        """
        if not metadata or row_index < 0 or row_index >= len(self.games):
            return
        
        game = self.games[row_index]
        title = game.get("title", "Unknown")
        print(f"\n[MERGE] Merging metadata for row {row_index}: {title}")
        print(f"[MERGE] Metadata has keys: {list(metadata.keys())}")
        
        # ====================================================================
        # PRESERVE EXISTING CACHE PATHS
        # ====================================================================
        existing_image_cache_paths = game.get("image_cache_paths", [])
        existing_microtrailer_cache_path = game.get("microtrailer_cache_path", "")
        existing_save_locations = game.get("savegame_locations", []) or game.get("savegame_location", [])
        
        # Track what we update
        updated_fields = []
        
        # ====================================================================
        # FIX: Ensure app_id is properly extracted from various possible sources
        # ====================================================================
        # Check multiple possible sources for app_id/steam_id
        app_id_sources = [
            metadata.get("app_id"),
            metadata.get("steam_app_id"),
            metadata.get("steam_id"),
        ]
        
        app_id = None
        for source in app_id_sources:
            if source:
                if isinstance(source, str) and source.strip():
                    app_id = str(source).strip()
                    break
                elif isinstance(source, (int, float)):
                    app_id = str(int(source))
                    break
        
        # If we found an app_id, make sure it's set in the game dict
        if app_id and app_id != "N/A" and app_id != "0":
            old_app_id = game.get("app_id") or ""
            if old_app_id != app_id:
                game["app_id"] = app_id
                updated_fields.append("app_id")
                print(f"[MERGE] Set app_id from metadata: '{old_app_id}' -> '{app_id}'")
        
        # ====================================================================
        # SIMPLE FIELD MAPPING - just copy the metadata to game dict
        # ====================================================================
        for key, value in metadata.items():
            if key == "__candidates__":
                continue
            
            # Skip app_id fields we already handled above
            if key in ["app_id", "steam_app_id", "steam_id"]:
                continue
            
            # Skip empty values
            if value is None or (isinstance(value, str) and not value.strip()):
                continue
            
            # Handle special cases for list fields
            if key in ["screenshots", "microtrailers", "trailers", "shortcut_links", "videos"]:
                # Convert string to list if needed
                if isinstance(value, str):
                    if "," in value:
                        value = [item.strip() for item in value.split(",") if item.strip()]
                    else:
                        value = [value.strip()] if value.strip() else []
                
                # Ensure it's a list
                if not isinstance(value, list):
                    value = [value]
                
                # Update the field if different
                old_value = game.get(key, [])
                if old_value != value:
                    game[key] = value
                    updated_fields.append(key)
                    print(f"[MERGE] Updated {key}: {len(old_value)} items -> {len(value)} items")
                continue
            
            # For regular fields
            old_value = game.get(key)
            if old_value != value:
                game[key] = value
                updated_fields.append(key)
                print(f"[MERGE] Updated {key}: '{old_value}' -> '{value}'")
        
        # ====================================================================
        # EXTRACT FIRST MICROTRAILER AS trailer_webm IF AVAILABLE
        # ====================================================================
        microtrailers = metadata.get("microtrailers")
        if microtrailers and isinstance(microtrailers, list) and len(microtrailers) > 0:
            first_microtrailer = microtrailers[0]
            if first_microtrailer and not game.get("trailer_webm"):
                game["trailer_webm"] = first_microtrailer
                if "trailer_webm" not in updated_fields:
                    updated_fields.append("trailer_webm")
                print(f"[MERGE] Set first microtrailer as trailer_webm: {first_microtrailer}")
        
        # ====================================================================
        # EXTRACT APP_ID FROM STEAM_LINK IF NOT PRESENT
        # ====================================================================
        if not game.get("app_id") and metadata.get("steam_link"):
            match = re.search(r"/app/(\d+)", metadata["steam_link"])
            if match:
                game["app_id"] = match.group(1)
                if "app_id" not in updated_fields:
                    updated_fields.append("app_id")
                print(f"[MERGE] Extracted app_id from steam_link: {game['app_id']}")
        
        # ====================================================================
        # RESTORE PRESERVED CACHE PATHS
        # ====================================================================
        if existing_image_cache_paths:
            game["image_cache_paths"] = existing_image_cache_paths
            print(f"[MERGE] Restored {len(existing_image_cache_paths)} image cache paths")
        
        if existing_microtrailer_cache_path:
            game["microtrailer_cache_path"] = existing_microtrailer_cache_path
            print(f"[MERGE] Restored microtrailer cache path: {existing_microtrailer_cache_path}")
        
        if existing_save_locations:
            game["savegame_locations"] = existing_save_locations
            print(f"[MERGE] Restored save locations")
        
        # ====================================================================
        # UPDATE THE MODEL DIRECTLY
        # ====================================================================
        if updated_fields:
            print(f"[MERGE] Updated {len(updated_fields)} fields for row {row_index}")
            
            # Map game fields to table columns
            column_map = {
                "title": self.COL_TITLE,
                "app_id": self.COL_STEAMID,
                "developer": self.COL_DEV,
                "publisher": self.COL_PUB,
                "genres": self.COL_GENRES,
                "release_date": self.COL_RELEASE,
                "description": self.COL_DESCRIPTION,
                "cover_url": self.COL_COVER_URL,
                "trailer_webm": self.COL_TRAILER,
                "steam_link": self.COL_STEAM_LINK,
                "steamdb_link": self.COL_STEAMDB,
                "pcgw_link": self.COL_PCWIKI,
                "igdb_link": self.COL_IGDB_ID,
                "themes": self.COL_THEMES,
                "player_perspective": self.COL_PERSPECTIVE,
                "microtrailers": self.COL_MICROTRAILERS,
                "user_rating": self.COL_USER_RATING,
                "image_cache_paths": self.COL_IMAGE_CACHE_PATHS,
                "microtrailer_cache_path": self.COL_MICROTRAILER_CACHE_PATH,
                "savegame_locations": self.COL_SAVE_LOCATION,
            }
            
            # Update table cells
            self._suppress_model_change = True
            for field in updated_fields:
                if field in column_map:
                    col = column_map[field]
                    value = game.get(field, "")
                    
                    # Handle special formatting for different field types
                    display_value = ""
                    
                    if field == "microtrailers":
                        # List of microtrailer URLs
                        if isinstance(value, list):
                            display_value = ", ".join(str(x) for x in value if x)
                        else:
                            display_value = str(value)
                            
                    elif field == "image_cache_paths":
                        # List of cache paths
                        if isinstance(value, list):
                            display_value = ", ".join(str(x) for x in value if x)
                        else:
                            display_value = str(value)
                            
                    elif field == "savegame_locations":
                        # List of save locations
                        if isinstance(value, list):
                            display_value = " | ".join(str(x) for x in value if x)
                        else:
                            display_value = str(value)
                            
                    elif isinstance(value, list):
                        # Generic list handling
                        display_value = ", ".join(str(x) for x in value if x)
                    else:
                        display_value = str(value)
                    
                    # Update the model
                    item = self.model.item(row_index, col)
                    if item:
                        item.setText(display_value)
                    else:
                        item = QStandardItem(display_value)
                        self.model.setItem(row_index, col, item)
            
            # Always update cache path columns if they exist
            if existing_image_cache_paths:
                col = self.COL_IMAGE_CACHE_PATHS
                display_value = ", ".join(str(x) for x in existing_image_cache_paths if x)
                item = self.model.item(row_index, col)
                if item:
                    item.setText(display_value)
                else:
                    self.model.setItem(row_index, col, QStandardItem(display_value))
            
            if existing_microtrailer_cache_path:
                col = self.COL_MICROTRAILER_CACHE_PATH
                item = self.model.item(row_index, col)
                if item:
                    item.setText(existing_microtrailer_cache_path)
                else:
                    self.model.setItem(row_index, col, QStandardItem(existing_microtrailer_cache_path))
            
            self._suppress_model_change = False
            
            # Store game data in title cell
            title_item = self.model.item(row_index, self.COL_TITLE)
            if title_item:
                title_item.setData(game, Qt.UserRole)
            
            print(f"[MERGE] Model updated for row {row_index}")
        else:
            print(f"[MERGE] No fields updated for row {row_index}")
        
        # ====================================================================
        # FORCE UI UPDATE
        # ====================================================================
        # Update the current row selection if this is the selected row
        selected_rows = self._selected_source_rows()
        if row_index in selected_rows:
            QTimer.singleShot(100, lambda: self.show_details_for_source_row(row_index))
        
        # Update counters
        self.update_counters()

    # ============================================================================
    # SIMPLER RECACHE METHOD - CLEAR CACHE FIELDS ONLY
    # ============================================================================

    def recache_selected_rows(self):
        """
        Clear cache fields for selected rows, triggering automatic redownload 
        when assets are viewed.
        """
        rows = self._selected_source_rows()
        if not rows:
            self.status.setText("No rows selected for recaching.")
            return
        
        # Confirm with user
        reply = QMessageBox.question(
            self,
            "Recache Selected Rows",
            f"This will clear cached assets for {len(rows)} selected rows.\n\n"
            "Assets will be automatically redownloaded when you view them.\n"
            "Do you want to continue?",
            QMessageBox.Yes | QMessageBox.No
        )
        
        if reply != QMessageBox.Yes:
            return
        
        # Clear cache fields for each selected row
        cleared_count = 0
        for row in rows:
            if row >= len(self.games):
                continue
            
            game = self.games[row]
            title = game.get("title") or game.get("original_title") or f"Row {row}"
            
            print(f"[RECACHE] Clearing cache fields for row {row}: '{title}'")
            
            # Clear the cache fields
            if "image_cache_paths" in game:
                old_count = len(game.get("image_cache_paths", []))
                del game["image_cache_paths"]
                print(f"[RECACHE] Cleared {old_count} screenshot cache paths")
            
            if "microtrailer_cache_path" in game:
                old_path = game["microtrailer_cache_path"]
                del game["microtrailer_cache_path"]
                print(f"[RECACHE] Cleared microtrailer cache path: {old_path}")
            
            # Also clear from model
            self.model.setItem(row, self.COL_IMAGE_CACHE_PATHS, QStandardItem(""))
            self.model.setItem(row, self.COL_MICROTRAILER_CACHE_PATH, QStandardItem(""))
            
            # Store updated game data
            title_item = self.model.item(row, self.COL_TITLE)
            if title_item:
                title_item.setData(game, Qt.UserRole)
            
            cleared_count += 1
        
        # Show summary
        msg_box = QMessageBox.information(
            self,
            "Cache Cleared",
            f"Cleared cache fields for {cleared_count} selected rows.\n\n"
            "Assets will be redownloaded automatically when you view them."
        )
        
        # Refresh UI
        self.refresh_model()
        
        # If current selection includes cleared rows, refresh details
        selected_rows = self._selected_source_rows()
        if selected_rows and selected_rows[0] < len(self.games):
            current_game = self.games[selected_rows[0]]
            if "image_cache_paths" not in current_game and "microtrailer_cache_path" not in current_game:
                self.status.setText(f"Cache cleared for {cleared_count} rows - Assets will redownload on view")
                
                # Clear the image viewer to indicate no cached images
                self.viewer.clear()
                self.viewer.set_url("")
                self._image_items = []
                self._current_image_index = 0
                self._update_image_navigation()
        
        self.status.setText(f"Cache cleared for {cleared_count} rows - Will redownload on view")


    def clear_selected_cache_only(self):
        """
        Alternative: Just clear cache without any redownload prompts.
        Assets will be fetched on demand.
        """
        rows = self._selected_source_rows()
        if not rows:
            self.status.setText("No rows selected.")
            return
        
        for row in rows:
            if row < len(self.games):
                game = self.games[row]
                game.pop("image_cache_paths", None)
                game.pop("microtrailer_cache_path", None)
                
                # Update model
                self.model.setItem(row, self.COL_IMAGE_CACHE_PATHS, QStandardItem(""))
                self.model.setItem(row, self.COL_MICROTRAILER_CACHE_PATH, QStandardItem(""))
        
        self.refresh_model()
        self.status.setText(f"Cache cleared for {len(rows)} rows")
               
    # ============================================================================
    # SELECTION AND DETAILS METHODS
    # ============================================================================
    
    def _selected_source_rows(self) -> List[int]:
        """
        Get selected row indices in source model (not proxy).
        
        Returns:
            List of row indices in source model
        """
        selected = self.table.selectionModel().selectedRows()
        rows = set()
        
        for proxy_index in selected:
            source_index = self.proxy.mapToSource(proxy_index)
            if source_index.isValid():
                rows.add(source_index.row())
        
        return sorted(rows)
    
    def _handle_selection_changed(self, selected, deselected):
        """Update details when selection changes."""
        rows = self.table.selectionModel().selectedRows()
        if not rows:
            return
        
        proxy_index = rows[0]
        source_index = self.proxy.mapToSource(proxy_index)
        
        if source_index.isValid():
            self.show_details_for_source_row(source_index.row())
    
    def show_details_for_source_row(self, source_row: int):
        # Validate row
        if source_row < 0 or source_row >= self.model.rowCount():
            self.status.setText("Invalid row")
            return
        
        # Get game data
        item = self.model.item(source_row, 0)
        if item is None:
            self.status.setText("No item at row")
            return
        
        game = item.data(Qt.UserRole)
        if not isinstance(game, dict):
            self.status.setText("No game dict stored")
            return
        
        # ========================================================================
        # BUILD DESCRIPTION HTML WITH CONSISTENT FONTS
        # ========================================================================
        
        def escape_html(text: str) -> str:
            """Escape HTML special characters."""
            return (text
                    .replace("&", "&amp;")
                    .replace("<", "&lt;")
                    .replace(">", "&gt;")
                    .replace("\n", "<br>"))
        
        # Get user rating
        user_rating = game.get("user_rating", "")
        rating_html = ""
        if user_rating:
            try:
                rating_num = float(user_rating)
                # Create star rating visualization
                stars = min(5, rating_num / 20)  # Convert 0-100 to 0-5 stars
                full_stars = int(stars)
                half_star = 1 if stars - full_stars >= 0.5 else 0
                empty_stars = 5 - full_stars - half_star
                
                stars_html = "★" * full_stars + "½" * half_star + "☆" * empty_stars
                rating_html = f"""
                    <div style='margin-bottom:10px;'>
                        <span style='font-weight:600; color:#f39c12;'>User Rating: {stars_html}</span>
                        <span style='color:#7f8c8d; font-size:11px; margin-left:10px;'>({user_rating}/100)</span>
                    </div>
                """
            except:
                rating_html = f"""
                    <div style='margin-bottom:10px;'>
                        <span style='font-weight:600; color:#2c3e50;'>User Rating:</span>
                        <span style='color:#7f8c8d; margin-left:5px;'>{user_rating}/100</span>
                    </div>
                """
        
        description = (game.get("description") or "").strip()
        desc_html = f"""
        <div style='margin-bottom:12px; font-size:12px; line-height:1.4; color:#2c3e50;'>
            {rating_html}
            {escape_html(description) or '<i style="color:#7f8c8d;">No description available.</i>'}
        </div>
        """
        
        # ========================================================================
        # BUILD DETAILS TABLE WITH USER RATING
        # ========================================================================
        
        details_fields = [
            ("Title", game.get("title", "")),
            ("User Rating", game.get("user_rating", "")),  # ADDED USER RATING
            ("Version", game.get("patch_version", "") or 
                       game.get("original_title_version", "")),
            ("Game Drive", game.get("game_drive", "")),
            ("Steam ID", game.get("app_id", "")),
            ("Played", "Yes" if game.get("played", False) else "No"),
            ("Genres", game.get("genres", "")),
            ("Game modes", game.get("game_modes", "")),
            ("Release date", game.get("release_date", "")),
            ("Themes", game.get("themes", "")),
            ("Developer", game.get("developer", "")),
            ("Publisher", game.get("publisher", "")),
            ("Scene/Repack", game.get("scene_repack", "")),
            ("Perspective", game.get("player_perspective", "")),
            ("Original title", game.get("original_title", "")),
            ("IGDB ID", game.get("igdb_id", "")),
        ]
        
        # Add savegame locations
        save_keys = [
            "savegame_location", "savegame_locations", "savegame_paths",
            "pcgw_save_location", "pcgw_save_locations", "save_location"
        ]
        
        save_locations = []
        for key in save_keys:
            value = game.get(key)
            if not value:
                continue
            
            if isinstance(value, (list, tuple)):
                save_locations.extend(value)
            elif isinstance(value, str):
                # Handle various delimiters
                for delimiter in ("|", "\n"):
                    if delimiter in value:
                        save_locations.extend(
                            [p.strip() for p in value.split(delimiter) if p.strip()]
                        )
                        break
                else:
                    save_locations.append(value)
        
        # Deduplicate save locations
        unique_saves = []
        seen_saves = set()
        for location in save_locations:
            if location and location not in seen_saves:
                seen_saves.add(location)
                unique_saves.append(location)
        
        details_fields.append(("Savegame locations", unique_saves))
        
        # Build HTML table with consistent styling
        table_rows = []
        for label, value in details_fields:
            if label == "Savegame locations":
                # Format save locations as clickable links
                if not value:
                    cell_html = '<i style="color:#7f8c8d;">None</i>'
                else:
                    links = []
                    for location in value:
                        try:
                            if location.startswith(("http://", "https://")):
                                href = location
                            else:
                                href = QUrl.fromLocalFile(str(location)).toString()
                            
                            display = escape_html(str(location))
                            links.append(
                                f'<div style="margin-bottom:3px; font-size:11px;">'
                                f'<a href="{href}" style="color:#3498db; text-decoration:none;">{display}</a>'
                                f'</div>'
                            )
                        except Exception:
                            links.append(
                                f'<div style="margin-bottom:3px; font-size:11px; color:#2c3e50;">'
                                f'{escape_html(str(location))}'
                                f'</div>'
                            )
                    
                    cell_html = (
                        '<div style="max-height:140px; overflow:auto; padding-right:6px;">'
                        + "".join(links) +
                        '</div>'
                    )
            else:
                cell_html = f'<span style="color:#2c3e50; font-size:11px;">{escape_html(str(value or ""))}</span>'
            
            table_rows.append(f"""
                <tr>
                    <td style='vertical-align:top; padding:5px 8px; 
                        white-space:nowrap; width:140px; font-weight:600; 
                        color:#2c3e50; font-size:11px; border-bottom:1px solid #ecf0f1;'>
                        {label}:
                    </td>
                    <td style='vertical-align:top; padding:5px 8px; 
                        max-width:520px; word-wrap:break-word; font-size:11px;
                        border-bottom:1px solid #ecf0f1;'>
                        {cell_html}
                    </td>
                </tr>
            """)
        
        details_html = f"""
            <table style='border-collapse:collapse; width:100%;'>
                {"".join(table_rows)}
            </table>
        """
        
        # ========================================================================
        # COMBINE AND DISPLAY WITH CONSISTENT FONTS
        # ========================================================================
        
        combined_html = f"""
            <div style='font-family:"Segoe UI", Arial, sans-serif;'>
                <div style='background-color:#f8f9fa; padding:12px; border-radius:4px; margin-bottom:12px;'>
                    <h3 style='color:#2c3e50; margin-top:0; margin-bottom:8px; font-size:14px;'>
                        {escape_html(game.get('title', 'Untitled'))}
                    </h3>
                    {desc_html}
                </div>
                <div style='color:#2c3e50; font-size:12px;'>
                    {details_html}
                </div>
            </div>
        """
        
        self.details.setHtml(combined_html)
        
        # ========================================================================
        # BUILD EXTERNAL LINKS
        # ========================================================================
        
                
        def normalize_url(url: str) -> str:
            """Normalize URL format."""
            if not url:
                return url
            
            url = url.strip()
            if url.startswith("//"):
                return "https:" + url
            return url
        
        links = []
        
        # Store links
        if game.get("steam_link"):
            links.append(f'<a href="{normalize_url(game["steam_link"])}">Steam</a>')
        if game.get("steamdb_link"):
            links.append(f'<a href="{normalize_url(game["steamdb_link"])}">SteamDB</a>')
        if game.get("pcgw_link"):
            links.append(f'<a href="{normalize_url(game["pcgw_link"])}">PCGamingWiki</a>')
        if game.get("igdb_link"):
            links.append(f'<a href="{normalize_url(game["igdb_link"])}">IGDB</a>')
        
        # Trailer links
        trailers_raw = game.get("trailers") or game.get("igdb_trailers") or ""
        trailer_list = []
        
        if isinstance(trailers_raw, (list, tuple)):
            trailer_list = [t for t in trailers_raw if t]
        elif isinstance(trailers_raw, str):
            trailer_list = [
                p.strip() for p in re.split(r"[,\|;\n]+", trailers_raw) 
                if p.strip()
            ]
        
        if trailer_list:
            trailer_links = []
            for i, trailer_url in enumerate(trailer_list[:MAX_TRAILERS], 1):
                normalized_url = normalize_url(trailer_url)
                trailer_links.append(f'<a href="{normalized_url}">[{i}]</a>')
            
            links.append("Trailer " + "".join(trailer_links))
        elif game.get("trailer_webm"):
            # Fallback to single trailer
            links.append(
                'Trailer ' + 
                f'<a href="{normalize_url(game["trailer_webm"])}">[1]</a>'
            )
        
        # Shortcut links
        if game.get("shortcut_links"):
            raw_links = str(game.get("shortcut_links") or "")
            parts = [p.strip() for p in raw_links.split("|") if p.strip()]
            
            for part in parts:
                if "|" in part:
                    label, url = part.split("|", 1)
                    links.append(f'<a href="{normalize_url(url)}">{label}</a>')
                else:
                    links.append(f'<a href="{normalize_url(part)}">Link</a>')
        
        # Display links
        if links:
            links_html = (
                "<div style='font-size:9pt; margin-top:6px;'>" +
                " | ".join(links) +
                "</div>"
            )
        else:
            links_html = (
                "<div style='font-size:9pt; margin-top:6px; color:#666;'>"
                "<i>No external links</i>"
                "</div>"
            )
        
        self.links_label.setText(links_html)

        # ========================================================================
        # DISPLAY IMAGES
        # ========================================================================
        # ========================================================================
        # DISPLAY IMAGES - UPDATED TO USE CACHE FIRST
        # ========================================================================

        image_urls = []
        cached_paths = []

        # Get cached image paths if available
        cached_image_paths = game.get("image_cache_paths", [])
        if cached_image_paths and isinstance(cached_image_paths, (list, tuple)):
            # Filter for valid cached paths
            for cache_path in cached_image_paths:
                if not cache_path:
                    continue
                try:
                    # Convert to absolute path if relative
                    if isinstance(cache_path, str):
                        if not os.path.isabs(cache_path):
                            # Try to resolve relative path
                            abs_path = SCRIPT_DIR / cache_path
                            if abs_path.exists():
                                cached_paths.append(str(abs_path))
                        else:
                            # Check if absolute path exists
                            if os.path.exists(cache_path):
                                cached_paths.append(cache_path)
                except Exception:
                    pass  # Skip invalid paths

        # Get image URLs for fallback AND for click-to-open functionality
        # Cover image (always include if available)
        if game.get("cover_url"):
            image_urls.append(game.get("cover_url"))

        # Screenshots (with limit)
        screenshot_list = game.get("screenshots") or []
        screenshots_to_show = screenshot_list[:MAX_IMAGES_TO_DISPLAY]

        for screenshot in screenshots_to_show:
            if screenshot:
                image_urls.append(screenshot)

        print(f"\n{'='*60}")
        print(f"DEBUG: Image display for {game.get('title', 'Unknown')}")
        print(f"  Row: {source_row}")
        print(f"  Cached paths found: {len(cached_paths)}")
        print(f"  URLs available for click-to-open: {len(image_urls)}")
        if cached_paths:
            print(f"  Using cached paths: {cached_paths[:3]}...")  # Show first 3
        elif image_urls:
            print(f"  Using URLs: {image_urls[:3]}...")
        print(f"{'='*60}\n")

        if cached_paths:
            # Use cached images directly
            try:
                # Display cached images immediately
                self.viewer.set_urls(cached_paths)
                self.viewer.set_current_index(0)
                self.status.setText("Loaded images from cache")
                
                # Update image counter
                total_images = len(cached_paths)
                self.image_counter.setText(f"1/{total_images}")
                
                # Enable/disable navigation buttons
                self.prev_btn.setEnabled(False)
                self.next_btn.setEnabled(total_images > 1)
                
                print(f"[DEBUG] Displaying {len(cached_paths)} cached images")
                
            except Exception as e:
                self.status.setText(f"Failed to load cached images: {e}")
                # Fall back to fetching from URLs
                if image_urls:
                    self._fetch_and_display_images(source_row, image_urls)
                else:
                    self._handle_no_images()
        
        elif image_urls:
            # No cached paths, fetch from URLs
            try:
                self._fetch_and_display_images(source_row, image_urls)
            except Exception as e:
                self.status.setText(f"Failed to fetch images: {e}")
                self._handle_no_images()
        
        else:
            try:
                self.viewer.clear()
                self.viewer.set_url("")
                self.status.setText("No images available")
                self.image_counter.setText("No images")
            except Exception as e:
                print(f"[DEBUG] Error handling no images: {e}")
            
        # ========================================================================
        # PLAY TRAILER - FIXED
        # ========================================================================
        
    # ========================================================================
        # PLAY TRAILER - FIXED to use cached microtrailer
        # ========================================================================
        
        print(f"\n--- [DEBUG] Checking Trailer for Row {source_row} ---")
        
        # 1. First check if we have a cached microtrailer
        cached_microtrailer = game.get("microtrailer_cache_path", "")
        if cached_microtrailer:
            try:
                abs_path = SCRIPT_DIR / cached_microtrailer
                if abs_path.exists():
                    print(f"[DEBUG] Found cached microtrailer: {abs_path}")
                    
                    # First, show the container
                    self.trailer_container.show()
                    
                    # Check if it's a GIF or video
                    if str(abs_path).lower().endswith('.gif'):
                        movie = QMovie(str(abs_path))
                        movie.setCacheMode(QMovie.CacheAll)
                        if movie.isValid():
                            self.trailer_gif_label.setMovie(movie)
                            movie.start()
                            self.video_widget.hide()
                            self.trailer_gif_label.show()
                            print(f"[DEBUG] Playing cached GIF microtrailer")
                            return
                    else:
                        # For video files
                        media = QMediaContent(QUrl.fromLocalFile(str(abs_path)))
                        self.media_player.setMedia(media)
                        self.media_player.setMuted(True)
                        self.media_player.play()
                        self.trailer_gif_label.hide()
                        self.video_widget.show()
                        print(f"[DEBUG] Playing cached video microtrailer")
                        return
            except Exception as e:
                print(f"[DEBUG] Error playing cached microtrailer: {e}")
        
        # 2. Fallback to trailer_webm if no cached microtrailer
        trailer_url = game.get("trailer_webm") or ""

        
        if not trailer_url:
            microtrailers = game.get("microtrailers")
            if microtrailers and isinstance(microtrailers, list) and len(microtrailers) > 0:
                trailer_url = microtrailers[0]
                print(f"[DEBUG] Using first microtrailer: {trailer_url}")

        # 3. Original fallback detection if still empty
        if not trailer_url:
            print("[DEBUG] Primary trailer empty. Starting fallback search...")
            
            candidate_sources = [
                "microtrailers", 
                "trailers", 
                ]
            
            candidates = []
            
            for source_key in candidate_sources:
                raw_value = game.get(source_key)
                # Log what we find in the raw data
                if raw_value:
                    print(f"[DEBUG] Found data in '{source_key}': {raw_value}")
                
                # Handle Lists
                if isinstance(raw_value, (list, tuple)):
                    candidates.extend([str(x) for x in raw_value if x])
                
                # Handle Strings (split by | or newline)
                elif isinstance(raw_value, str) and raw_value.strip():
                    cleaned = raw_value.replace("\n", "|").replace(";", "|")
                    parts = [p.strip() for p in cleaned.split("|") if p.strip()]
                    candidates.extend(parts)

            print(f"[DEBUG] All potential candidates found: {candidates}")

            # Filter candidates for valid video files
            for candidate in candidates:
                low = candidate.lower()
                # Check for common video/gif extensions
                if any(low.endswith(ext) for ext in [".webm", ".mp4", ".gif"]):
                    print(f"[DEBUG] VALID MATCH FOUND: {candidate}")
                    trailer_url = candidate
                    break
                else:
                    print(f"[DEBUG] Skipped non-video candidate: {candidate}")

        # 4. Final attempt to play
        if trailer_url:
            print(f"[DEBUG] Attempting to play URL: '{trailer_url}'")
            try:
                # First, show the container
                self.trailer_container.show()
                
                # Play the media
                self._play_trailer_media(trailer_url)
                
                # Set up a timer to check if video actually started playing
                from PyQt5.QtCore import QTimer
                
                def check_if_playing():
                    # Check media player state (1 = PlayingState)
                    if self.media_player.state() != 1:  # Not playing
                        print(f"[DEBUG] Video failed to start playing, hiding container")
                        self.trailer_container.hide()
                        # Also stop any GIF playback
                        try:
                            if hasattr(self.trailer_gif_label, "movie") and \
                               self.trailer_gif_label.movie():
                                self.trailer_gif_label.movie().stop()
                                self.trailer_gif_label.hide()
                        except Exception:
                            pass
                
                # Check after 2 seconds if video started playing
                QTimer.singleShot(2000, check_if_playing)
                
            except Exception as e:
                print(f"[DEBUG] ERROR calling _play_trailer_media: {e}")
                self.status.setText(f"Trailer playback failed: {e}")
                self.trailer_container.hide()
        else:
            print("[DEBUG] No valid trailer URL found after search.")
            
            # Stop playback cleanup
            try:
                self.media_player.stop()
            except Exception:
                pass
            
            try:
                if hasattr(self.trailer_gif_label, "movie") and \
                   self.trailer_gif_label.movie():
                    self.trailer_gif_label.movie().stop()
                self.trailer_gif_label.clear()
            except Exception:
                pass
            
            try:
                self.video_widget.hide()
                self.trailer_gif_label.hide()
            except Exception:
                pass
            
            # Hide the trailer container when no trailer
            self.trailer_container.hide()
    # ============================================================================
    # SEARCH AND FILTER METHODS
    # ============================================================================
    
    def on_search_changed(self, text: str):
        """
        Handle search text changes.
        
        Args:
            text: Search string (searches all columns)
        """
        self.proxy.setFilterFixedString(text or "")
        self.apply_filters()
    
    def apply_filters(self):
        """
        Apply genre and game drive filters to the table.
        
        Combines search filter with additional column-specific filters.
        """
        genre_filter = (self.genre_filter.text() or "").lower().strip()
        drive_filter = (self.game_drive_filter.text() or "").lower().strip()
        
        # No additional filters - show all rows matching search
        if not genre_filter and not drive_filter:
            for proxy_row in range(self.proxy.rowCount()):
                self.table.setRowHidden(proxy_row, False)
            return
        
        # Apply filters
        for proxy_row in range(self.proxy.rowCount()):
            proxy_index = self.proxy.index(proxy_row, 0)
            source_index = self.proxy.mapToSource(proxy_index)
            
            show_row = True
            
            if source_index.isValid():
                # Get column values
                genre_text = (
                    self.model.data(
                        self.model.index(source_index.row(), self.COL_GENRES)
                    ) or ""
                ).lower()
                
                drive_text = (
                    self.model.data(
                        self.model.index(source_index.row(), self.COL_GAMEDRIVE)
                    ) or ""
                ).lower()
                
                # Apply filters
                if genre_filter and genre_filter not in genre_text:
                    show_row = False
                
                if drive_filter and drive_filter not in drive_text:
                    show_row = False
            else:
                show_row = False
            
            self.table.setRowHidden(proxy_row, not show_row)
    
    # ============================================================================
    # CONTEXT MENU AND SELECTION OPERATIONS
    # ============================================================================
    
    # In the open_context_menu method, add this action:
    def open_context_menu(self, pos: QPoint):
        """Show context menu for selected rows."""
        index = self.table.indexAt(pos)
        menu = QMenu(self)
        
        selected_rows = self._selected_source_rows()
        
        if selected_rows:
            # Add context-sensitive actions
            menu.addAction("Scrape selected game(s)...", self.scrape_selected_games)

            menu.addAction("Recache selected row(s)...", self.recache_selected_rows)  # NEW LINE
            menu.addSeparator()
            menu.addAction("Sanitize selected row(s)...", self.sanitize_selected_rows)
            menu.addAction("Edit selected game...", self.edit_selected_game)
            menu.addAction("Multi-edit selected...", self.multi_edit_selected)
            menu.addAction("Mark selected as Played", 
                          lambda: self.mark_played_selected(True))
            menu.addAction("Mark selected as Unplayed", 
                          lambda: self.mark_played_selected(False))
            menu.addAction("Set Game Drive for selected...", 
                          self.set_game_drive_selected)
            menu.addAction("Clear Save Location for selected", 
                          self.clear_save_location_selected)
            menu.addAction("Delete selected", self.delete_selected)
        else:
            menu.addAction("No selection", lambda: None)
        
        menu.exec_(self.table.viewport().mapToGlobal(pos))

    
    def scrape_selected_games(self):
        """Scrape metadata for selected games."""
        selected = self.table.selectionModel().selectedIndexes()
        if not selected:
            QMessageBox.information(self, "Scrape selected", "No rows selected.")
            return
        
        rows = sorted({
            self.proxy.mapToSource(index).row() 
            for index in selected
        })
        
        for row in rows:
            self.run_match_dialog_for_row(row)
        
        self.refresh_model()
        self.status.setText(f"Processed {len(rows)} selected rows.")
    
    def run_match_dialog_for_row(self, row: int):
        """
        Show match dialog for manual game matching.
        
        Args:
            row: Source model row index
        """
        if row < 0 or row >= len(self.games):
            return
        
        game = self.games[row]
        
        # Prepare data for dialog
        original_item = {
            "title": game.get("title", ""),
            "original_title": game.get("original_title", ""),
            "description": game.get("description", "") or ""
        }
        
        # Get IGDB candidates
        candidates = []
        try:
            if hasattr(scraping, "find_candidates_for_title_igdb"):
                candidates = scraping.find_candidates_for_title_igdb(
                    original_item.get("title") or 
                    original_item.get("original_title") or "",
                    max_candidates=12
                )
        except Exception:
            candidates = []
        
        # Show dialog
        if MatchDialog is None:
            QMessageBox.information(self, "Match dialog unavailable", 
                                   "Match dialog module not found.")
            return
        
        dlg = MatchDialog(original_item, candidates, parent=self)
        result = dlg.exec_()
        
        if result in (QDialog.Accepted, 1, 2):  # Accepted or "Apply & Next"
            result_data = getattr(dlg, "result_dict", None)
            if not isinstance(result_data, dict):
                return
            
            chosen = result_data.get("chosen_candidate") or {}
            overwrite = result_data.get("overwrite", False)
            
            # Extract all necessary data from result_data - IMPROVED EXTRACTION
            selected_title = result_data.get('title') or chosen.get('name') or game.get('title', '')
            
            # Extract IGDB ID from multiple possible sources
            selected_igdb_id = None
            for source in [result_data.get('igdb_id'), chosen.get('id'), chosen.get('igdb_id')]:
                if source and source != "N/A" and str(source).strip():
                    selected_igdb_id = str(source).strip()
                    break
            
            # Extract Steam AppID from multiple possible sources - IMPROVED
            selected_app_id = None
            for source in [
                result_data.get('app_id'), 
                result_data.get('steam_id'), 
                chosen.get('steam_id'),
                chosen.get('app_id')
            ]:
                if source and source != "N/A" and str(source).strip():
                    selected_app_id = str(source).strip()
                    break
            
            print(f"[MATCH_DIALOG] Scraping with:")
            print(f"  Title: '{selected_title}'")
            print(f"  IGDB ID: '{selected_igdb_id}'")
            print(f"  Steam AppID: '{selected_app_id}'")
            
            # Fetch metadata using the new scrape_igdb_then_steam function
            try:
                meta = scraping.scrape_igdb_then_steam(
                    igdb_id=selected_igdb_id,   # Pass IGDB ID (can be None)
                    title=selected_title,       # Pass title
                    auto_accept_score=92,
                    fetch_pcgw_save=False,
                    steam_app_id=selected_app_id  # Pass Steam AppID
                ) or {}
                
                print(f"[MATCH_DIALOG] Metadata returned keys: {list(meta.keys())}")
                
                if meta and "__candidates__" not in meta:
                    # Merge metadata using our improved method
                    self._merge_and_apply_metadata(row, meta)
                    print(f"[MATCH_DIALOG] Successfully scraped and merged metadata")
                else:
                    print(f"[MATCH_DIALOG] No valid metadata returned from scraping")
                    # If no metadata returned, still apply basic fields
                    if selected_title and (overwrite or not game.get("title")):
                        game["title"] = selected_title
                    if selected_app_id and (overwrite or not game.get("app_id")):
                        game["app_id"] = selected_app_id
                        print(f"[MATCH_DIALOG] Set app_id directly: {selected_app_id}")
                    if selected_igdb_id and (overwrite or not game.get("igdb_id")):
                        game["igdb_id"] = selected_igdb_id
            except Exception as e:
                print(f"[MATCH_DIALOG] Error scraping: {e}")
                # Fallback: apply basic fields even if scraping fails
                if selected_title and (overwrite or not game.get("title")):
                    game["title"] = selected_title
                if selected_app_id and (overwrite or not game.get("app_id")):
                    game["app_id"] = selected_app_id
                    print(f"[MATCH_DIALOG] Set app_id in fallback: {selected_app_id}")
                if selected_igdb_id and (overwrite or not game.get("igdb_id")):
                    game["igdb_id"] = selected_igdb_id
            
            # Mark as user-matched
            game["_last_matched_by"] = "user"
            
            # Refresh UI
            self.refresh_model()
            self.status.setText(f"Matched row {row}: {game.get('title','')} (app_id: {game.get('app_id','')})")


    # ============================================================================
    # BATCH OPERATION METHODS
    # ============================================================================
    
# ============================================================================
# NEW: SANITIZE SELECTED ROWS METHOD
# ============================================================================

    def sanitize_selected_rows(self):
        """
        Sanitize selected rows similar to how TXT files are imported.
        Breaks down original_title into components and updates game fields.
        """
        rows = self._selected_source_rows()
        if not rows:
            self.status.setText("No rows selected for sanitization.")
            return
        
        updated_count = 0
        for row in rows:
            if row < 0 or row >= len(self.games):
                continue
            
            game = self.games[row]
            original_title = game.get("original_title") or ""
            
            if not original_title:
                continue  # Skip rows without original_title
            
            print(f"[SANITIZE] Processing row {row}: '{original_title}'")
            
            # Sanitize the original title using the same logic as import_txt
            san = sanitize_original_title(original_title)
            
            # Update game fields from sanitized data
            base_title = san.get("base_title", "")
            version = san.get("version", "")
            repack = san.get("repack", "")
            notes = san.get("notes", "")
            modes = san.get("modes", [])
            
            # Set extracted components
            game["original_title_base"] = base_title
            game["original_title_version"] = version
            game["original_notes"] = notes
            
            # Only update scene_repack if empty and we have repack info
            if not game.get("scene_repack") and repack:
                game["scene_repack"] = repack
                print(f"[SANITIZE] Set scene_repack: {repack}")
            
            # Only update game_modes if empty and we have modes
            if not game.get("game_modes") and modes:
                game["game_modes"] = ", ".join(modes)
                print(f"[SANITIZE] Set game_modes: {', '.join(modes)}")
            
            # Update patch_version from original_title_version if available
            if not game.get("patch_version") and version:
                game["patch_version"] = version
                print(f"[SANITIZE] Set patch_version: {version}")
            
            # Set title to base_title if title is empty or same as original
            current_title = game.get("title", "")
            if (not current_title or current_title == original_title) and base_title:
                game["title"] = base_title
                print(f"[SANITIZE] Updated title: {base_title}")
            
            # Also update the original_title field with cleaned version
            # (sanitize_original_title might return a cleaned string in some implementations)
            if hasattr(san, 'get') and san.get('cleaned_string'):
                game["original_title"] = san['cleaned_string']
            elif base_title and version:
                # Construct a cleaner version
                clean_version = f"{base_title} {version}".strip()
                if clean_version and clean_version != original_title:
                    game["original_title"] = clean_version
            
            updated_count += 1
            
            # Update model for this row
            self._update_model_row(row)
        
        if updated_count:
            self.refresh_model()
            self.status.setText(f"Sanitized {updated_count} selected rows")
            
            # Show summary
            QMessageBox.information(
                self, 
                "Sanitize Complete",
                f"Sanitized {updated_count} selected rows:\n"
                f"• Extracted base titles and versions\n"
                f"• Updated scene/repack info\n"
                f"• Updated game modes\n"
                f"• Updated titles where needed"
            )
        else:
            self.status.setText("No changes made (no valid original_title fields)")

    def _update_model_row(self, row_index: int):
        """
        Update a specific row in the model after sanitization.
        """
        if row_index < 0 or row_index >= len(self.games):
            return
        
        game = self.games[row_index]
        
        # Update the model items for this row
        self._suppress_model_change = True
        
        try:
            # Update title column
            title_item = self.model.item(row_index, self.COL_TITLE)
            if title_item:
                title_item.setText(game.get("title", ""))
                title_item.setData(game, Qt.UserRole)
            
            # Update scene/repack column
            scene_item = self.model.item(row_index, self.COL_SCENE)
            if scene_item:
                scene_item.setText(game.get("scene_repack", ""))
            
            # Update game modes column
            modes_item = self.model.item(row_index, self.COL_GAME_MODES)
            if modes_item:
                modes_item.setText(game.get("game_modes", ""))
            
            # Update version column
            version_item = self.model.item(row_index, self.COL_VERSION)
            if version_item:
                version_item.setText(game.get("patch_version", ""))
            
            # Update original title column
            original_item = self.model.item(row_index, self.COL_ORIGINAL)
            if original_item:
                original_item.setText(game.get("original_title", ""))
            
            # Update original title base (if we have that column)
            if hasattr(self, 'COL_ORIGINAL_BASE'):
                original_base_item = self.model.item(row_index, self.COL_ORIGINAL_BASE)
                if original_base_item:
                    original_base_item.setText(game.get("original_title_base", ""))
            
            # Update original notes (if we have that column)
            if hasattr(self, 'COL_ORIGINAL_NOTES'):
                original_notes_item = self.model.item(row_index, self.COL_ORIGINAL_NOTES)
                if original_notes_item:
                    original_notes_item.setText(game.get("original_notes", ""))
                    
        finally:
            self._suppress_model_change = False
    
    def edit_selected_game(self):
        """Edit the first selected game."""
        rows = self._selected_source_rows()
        if not rows:
            QMessageBox.information(self, "Edit", "Select a game row first.")
            return
        
        self.edit_game_row(rows[0])
    
    def edit_game_row(self, source_row: int):
        """Edit a specific game row."""
        dlg = EditDialog(self.games[source_row], self)
        if dlg.exec_() == QDialog.Accepted:
            updated_data = dlg.result()
            self.games[source_row].update(updated_data)
            self.refresh_model()
            self.status.setText("Game details updated.")
    
    def multi_edit_selected(self):
        """Apply multiple edits to selected games."""
        rows = self._selected_source_rows()
        if not rows:
            self.status.setText("No rows selected.")
            return
        
        dlg = MultiEditDialog(self)
        if dlg.exec_() != QDialog.Accepted:
            return
        
        changes = dlg.result()
        applied = 0
        
        for row in rows:
            game = self.games[row]
            changed = False
            
            for key, value in changes.items():
                if value is None:
                    continue
                
                if key == "played":
                    if game.get("played") != value:
                        game["played"] = value
                        changed = True
                else:
                    if game.get(key, "") != value:
                        game[key] = value
                        changed = True
            
            if changed:
                applied += 1
        
        self.refresh_model()
        self.status.setText(f"Applied multi-edit to {applied} rows.")
    
    def mark_played_selected(self, played: bool):
        """Mark selected games as played or unplayed."""
        rows = self._selected_source_rows()
        if not rows:
            self.status.setText("No rows selected.")
            return
        
        for row in rows:
            self.games[row]["played"] = played
            
            # Update checkbox in model
            self.model.setData(
                self.model.index(row, self.COL_PLAYED),
                Qt.Checked if played else Qt.Unchecked,
                Qt.CheckStateRole
            )
        
        # Update highlighting
        self.update_table_highlights()
        
        # Update counters
        self.update_counters()
        
        self.status.setText(
            f"{len(rows)} rows marked as {'Played' if played else 'Unplayed'}."
        )

    
    def set_game_drive_selected(self):
        """Set game drive for selected games."""
        rows = self._selected_source_rows()
        if not rows:
            self.status.setText("No rows selected.")
            return
        
        drive, ok = QInputDialog.getText(
            self,
            "Set Game Drive",
            "Enter game drive/path to set for selected rows:"
        )
        
        if not ok:
            return
        
        drive = drive.strip()
        for row in rows:
            self.games[row]["game_drive"] = drive
        
        self.refresh_model()
        self.status.setText(f"Set Game Drive for {len(rows)} rows.")
    
    def clear_save_location_selected(self):
        """Clear save location for selected games."""
        rows = self._selected_source_rows()
        if not rows:
            self.status.setText("No rows selected.")
            return
        
        for row in rows:
            self.games[row]["save_location"] = ""
        
        self.refresh_model()
        self.status.setText(f"Cleared save location for {len(rows)} rows.")
    
    def delete_selected(self):
        """Delete selected games with confirmation."""
        rows = self._selected_source_rows()
        if not rows:
            self.status.setText("No rows selected.")
            return
        
        # FIXED: Issue #2 - Use QMessageBox.Yes instead of QDialog.Accepted
        confirm = QMessageBox.question(
            self,
            "Delete selected",
            f"Delete {len(rows)} selected rows? This cannot be undone.",
            QMessageBox.Yes | QMessageBox.No
        )
        
        if confirm != QMessageBox.Yes:  # FIXED: Compare with QMessageBox.Yes
            return
        
        # Delete in reverse order to maintain indices
        for row in sorted(rows, reverse=True):
            try:
                del self.games[row]
            except Exception:
                pass
        
        self.refresh_model()
        self.status.setText(f"Deleted {len(rows)} rows.")
    
    # ============================================================================
    # IMPORT/EXPORT METHODS
    # ============================================================================
    
    def _save_database_combined_dialog(self):
        """Save database dialog supporting JSON and SQLite."""
        try:
            filters = "JSON Files (*.json);;SQLite Database (*.sqlite *.db)"
            path, selected_filter = QFileDialog.getSaveFileName(
                self, "Save Database", "games.json", filters
            )
            
            if not path:
                return
            
            # Ensure file extension
            if "." not in os.path.basename(path):
                if "JSON" in (selected_filter or ""):
                    path = path + ".json"
                else:
                    path = path + ".sqlite"
            
            extension = os.path.splitext(path)[1].lower()
            
            # JSON Save
            if extension == ".json":
                try:
                    # FIXED: Use correct function from import_export
                    error = import_export.save_to_json(path, self.games)
                    if error:
                        raise RuntimeError(error)
                    
                    self.status.setText(
                        f"Saved {len(self.games)} games to {os.path.basename(path)}"
                    )
                    QMessageBox.information(
                        self, "Save JSON",
                        f"Saved {len(self.games)} games to:\n{path}"
                    )
                    
                except Exception as e:
                    QMessageBox.critical(self, "Save JSON failed", f"Failed: {e}")
                    self.status.setText("Save JSON failed")
            
            # SQLite Save
            else:
                try:
                    # FIXED: Corrected argument order
                    error = import_export.save_to_sqlite(path, self.games)
                    if error:
                        raise RuntimeError(error)
                    
                    self.status.setText(
                        f"Saved {len(self.games)} games to {os.path.basename(path)}"
                    )
                    QMessageBox.information(
                        self, "Save SQLite",
                        f"Saved {len(self.games)} games to:\n{path}"
                    )
                    
                except Exception as e:
                    QMessageBox.critical(self, "Save SQLite failed", f"Failed: {e}")
                    self.status.setText("Save SQLite failed")
                    
        except Exception as e:
            QMessageBox.critical(self, "Save error", str(e))
            self.status.setText("Save error")
    
    def _load_database_combined_dialog(self):
        """Load database dialog supporting JSON and SQLite."""
        try:
            filters = "JSON Files (*.json);;SQLite Database (*.sqlite *.db)"
            path, selected_filter = QFileDialog.getOpenFileName(
                self, "Load Database", "", filters
            )
            
            if not path:
                return
            
            extension = os.path.splitext(path)[1].lower()
            loaded_games = None
            error = None
            
            # JSON Load
            if extension == ".json":
                try:
                    # FIXED: Use correct function and handle tuple return
                    loaded_games, error = import_export.load_from_json(path)
                except Exception as e:
                    QMessageBox.critical(self, "Load JSON failed", f"Failed: {e}")
                    self.status.setText("Load JSON failed")
                    return
            
            # SQLite Load
            else:
                try:
                    # FIXED: Use correct function and handle tuple return
                    loaded_games, error = import_export.load_from_sqlite(path)
                except Exception as e:
                    QMessageBox.critical(self, "Load SQLite failed", f"Failed: {e}")
                    self.status.setText("Load SQLite failed")
                    return
            
            # Check for loader errors
            if error:
                QMessageBox.critical(self, "Load failed", f"Loader error: {error}")
                self.status.setText("Load failed")
                return
            
            # Validate loaded data
            if not isinstance(loaded_games, list):
                QMessageBox.critical(
                    self, "Load failed",
                    f"Loaded data is not a list. Type: {type(loaded_games)}"
                )
                self.status.setText("Load failed: invalid data")
                return
            
            # Replace games and refresh
            self.games = list(loaded_games)
            self.refresh_model()
            
            self.status.setText(
                f"Loaded {len(self.games)} games from {os.path.basename(path)}"
            )
            QMessageBox.information(
                self, "Loaded",
                f"Loaded {len(self.games)} games from:\n{path}"
            )
            
        except Exception as e:
            QMessageBox.critical(self, "Load error", str(e))
            self.status.setText("Load error")
    
    def _import_file_combined_dialog(self):
        """Combined import dialog for CSV, TXT, and Excel files."""
        try:
            filters = "CSV Files (*.csv);;Text Files (*.txt);;Excel Files (*.xlsx *.xls)"
            path, selected_filter = QFileDialog.getOpenFileName(
                self, "Import file", "", filters
            )
            
            if not path:
                return
            
            extension = os.path.splitext(path)[1].lower()
            imported_rows = None
            error = None
            
            # CSV Import
            if extension == ".csv":
                try:
                    result = import_csv(path)
                    if isinstance(result, tuple) and len(result) == 2:
                        imported_rows, error = result
                    else:
                        imported_rows = result
                except Exception as e:
                    QMessageBox.critical(self, "Import CSV failed", f"Failed: {e}")
                    self.status.setText("Import CSV failed")
                    return
            
            # TXT Import
            elif extension == ".txt":
                try:
                    result = import_txt(path)
                    if isinstance(result, tuple) and len(result) == 2:
                        imported_rows, error = result
                    else:
                        imported_rows = result
                except Exception as e:
                    QMessageBox.critical(self, "Import TXT failed", f"Failed: {e}")
                    self.status.setText("Import TXT failed")
                    return
            
            # Excel Import
            elif extension in (".xlsx", ".xls"):
                try:
                    result = import_excel(path)
                    if isinstance(result, tuple) and len(result) == 2:
                        imported_rows, error = result
                    else:
                        imported_rows = result
                except Exception as e:
                    QMessageBox.critical(self, "Import Excel failed", f"Failed: {e}")
                    self.status.setText("Import Excel failed")
                    return
            
            else:
                QMessageBox.warning(self, "Unsupported file", 
                                   f"Unsupported extension: {extension}")
                return
            
            # Check for import errors
            if error:
                QMessageBox.critical(self, "Import failed", f"Import error: {error}")
                self.status.setText("Import failed")
                return
            
            # Validate imported data
            if not isinstance(imported_rows, (list, tuple)):
                QMessageBox.critical(
                    self, "Import failed",
                    f"Imported data is not a list. Type: {type(imported_rows)}"
                )
                self.status.setText("Import failed: invalid data")
                return
            
            # Merge imported rows
            before_count = len(self.games)
            merge_imported_rows(self.games, imported_rows, prefer_imported=True)
            after_count = len(self.games)
            
            self.refresh_model()
            
            self.status.setText(
                f"Imported {len(imported_rows)} rows; "
                f"games: {before_count} -> {after_count}"
            )
            
            QMessageBox.information(
                self, "Import complete",
                f"Imported {len(imported_rows)} rows from:\n{os.path.basename(path)}"
            )
            
        except Exception as e:
            QMessageBox.critical(self, "Import error", str(e))
            self.status.setText("Import error")
    
    def export_to_pdf_dialog(self):
        """Export games to PDF or HTML."""
        path, selected_filter = QFileDialog.getSaveFileName(
            self, "Export games to PDF", "games_export.pdf",
            "PDF Files (*.pdf);;HTML Files (*.html)"
        )
        
        if not path:
            return
        
        # HTML Export
        if path.lower().endswith(".html"):
            error = export_games_to_html(
                path, self.games, 
                title="Game Manager Export", 
                open_after=True
            )
            
            if error:
                QMessageBox.critical(self, "Export", f"Failed to export HTML:\n{error}")
            else:
                QMessageBox.information(
                    self, "Export",
                    f"Exported HTML to:\n{path}\nOpen in browser to print to PDF."
                )
            
            return
        
        # Ensure PDF extension
        if not path.lower().endswith(".pdf"):
            path = path + ".pdf"
        
        # PDF Export
        error = export_games_to_pdf(path, self.games, title="Game Manager Export")
        
        if error:
            # Check if error is due to missing PDF library
            if any(phrase in str(error).lower() for phrase in 
                   ["no pdf library", "reportlab", "fpdf"]):
                # Fallback to HTML
                html_path = path[:-4] + ".html"
                html_error = export_games_to_html(
                    html_path, self.games,
                    title="Game Manager Export",
                    open_after=True
                )
                
                if html_error:
                    QMessageBox.critical(
                        self, "Export failed",
                        f"PDF and HTML export failed:\nPDF: {error}\nHTML: {html_error}"
                    )
                else:
                    QMessageBox.information(
                        self, "Export fallback",
                        f"PDF libraries not installed. Exported HTML instead:\n{html_path}\n"
                        f"Open it in a browser and print to PDF."
                    )
            else:
                QMessageBox.critical(self, "Export failed", f"Failed to export PDF:\n{error}")
        else:
            QMessageBox.information(
                self, "Export to PDF",
                f"Exported {len(self.games)} games to:\n{path}"
            )
    
    # ============================================================================
    # CLEANUP AND SHUTDOWN
    # ============================================================================
    
    def force_cancel_operation(self):
        """
        Force cancel all ongoing operations and refresh UI immediately.
        """
        print("[FORCE_CANCEL] Force cancel operation requested")
        
        # Set cancellation flags
        self._cancel_current_scrape = True
        self._cancel_batch = True
        
        # Cancel all image fetch workers
        print(f"[FORCE_CANCEL] Cancelling {len(self._image_threads)} image threads")
        for thread, worker in self._image_threads[:]:
            try:
                worker.cancelled = True
                if thread.isRunning():
                    thread.quit()
                    thread.wait(500)
            except Exception as e:
                print(f"[FORCE_CANCEL] Error cancelling image thread: {e}")
        
        self._image_threads.clear()
        
        # Cancel batch scraping worker if exists
        if hasattr(self, '_current_batch_worker'):
            try:
                print("[FORCE_CANCEL] Cancelling batch worker")
                self._current_batch_worker.cancelled = True
            except Exception as e:
                print(f"[FORCE_CANCEL] Error cancelling batch worker: {e}")
        
        # Cancel batch thread if exists
        if hasattr(self, '_current_batch_thread'):
            try:
                print("[FORCE_CANCEL] Cancelling batch thread")
                if self._current_batch_thread.isRunning():
                    self._current_batch_thread.quit()
                    self._current_batch_thread.wait(500)
            except Exception as e:
                print(f"[FORCE_CANCEL] Error cancelling batch thread: {e}")
        
        # Clear any stall timer
        if hasattr(self, '_stall_timer'):
            try:
                self._stall_timer.stop()
            except Exception:
                pass
        
        # Close all open match dialogs
        if hasattr(self, '_active_match_dialogs'):
            print(f"[FORCE_CANCEL] Closing {len(self._active_match_dialogs)} open match dialogs")
            for dlg in self._active_match_dialogs[:]:
                try:
                    dlg.close()
                    dlg.deleteLater()
                except Exception:
                    pass
            self._active_match_dialogs = []
        
        # Clear remaining chunks
        if hasattr(self, '_remaining_chunks'):
            print(f"[FORCE_CANCEL] Clearing {len(self._remaining_chunks)} remaining chunks")
            self._remaining_chunks = []
        
        # IMMEDIATELY restore UI state (THIS IS CRITICAL)
        print("[FORCE_CANCEL] Restoring UI state")
        self.scrape_btn.setEnabled(True)
        self.cancel_scrape_btn.setVisible(False)
        
        # Force a complete model refresh
        print("[FORCE_CANCEL] Forcing model refresh")
        self.refresh_model()
        
        # Force update of details panel
        try:
            selected_rows = self._selected_source_rows()
            if selected_rows:
                print(f"[FORCE_CANCEL] Updating details for row {selected_rows[0]}")
                self.show_details_for_source_row(selected_rows[0])
        except Exception as e:
            print(f"[FORCE_CANCEL] Error updating details: {e}")
        
        # Clear cancellation flags after cleanup
        self._cancel_current_scrape = False
        self._cancel_batch = False
        
        self.status.setText("Operation cancelled. UI refreshed.")
        print("[FORCE_CANCEL] Operation cancelled and UI refreshed")
        
        # Force event processing
        QCoreApplication.processEvents()    
    
    def _shutdown_workers(self, timeout_ms: int = 1500):
        """
        Gracefully shutdown all background workers.
        
        Strategy:
        1. Set cancellation flags
        2. Request threads to quit
        3. Wait for graceful shutdown
        4. Force terminate if necessary
        
        Args:
            timeout_ms: Maximum wait time for graceful shutdown
        """
        # Set cancellation flags
        self._cancel_current_scrape = True
        self._cancel_batch = True
        
        # Process events
        QCoreApplication.processEvents()
        
        def stop_thread_worker(thread, worker):
            """Stop a thread-worker pair."""
            try:
                # Request cancellation
                if hasattr(worker, "cancelled"):
                    worker.cancelled = True
                
                # Request thread interruption
                if hasattr(thread, "requestInterruption"):
                    thread.requestInterruption()
                
                # Request quit
                thread.quit()
                thread.wait(timeout_ms)
                
                # Force terminate if still running
                if thread.isRunning():
                    thread.terminate()
                    thread.wait(200)
                    
            except Exception:
                pass
        
        # Stop image threads
        for item in getattr(self, "_image_threads", []):
            if isinstance(item, tuple) and len(item) >= 2:
                thread, worker = item[0], item[1]
                stop_thread_worker(thread, worker)
        
        # Stop other thread lists
        for list_name in ("_threads", "_batch_image_threads"):
            for item in getattr(self, list_name, []):
                if isinstance(item, tuple) and len(item) >= 2:
                    thread, worker = item[0], item[1]
                    stop_thread_worker(thread, worker)
        
        # Final event processing
        QCoreApplication.processEvents()
        time.sleep(0.05)
        QCoreApplication.processEvents()
    
    def closeEvent(self, event):
        """
        Handle window close event with worker cleanup.
        
        Args:
            event: Close event
        """
        try:
            self._shutdown_workers()
        except Exception:
            pass
        
        try:
            super().closeEvent(event)
        except Exception:
            event.accept()

# ============================================================================
# APPLICATION ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    """
    Main application entry point.
    
    Creates QApplication, initializes main window, and starts event loop.
    """
    app = QApplication(sys.argv)
    app.setApplicationName("Game Manager")
    app.setOrganizationName("GameScraper")
    app.setStyle('Fusion')  # Use Fusion style for consistent look across platforms
    
    # Apply custom palette
    palette = app.palette()
    palette.setColor(QPalette.Window, QColor(LIGHT_BG))
    palette.setColor(QPalette.WindowText, QColor(PRIMARY_COLOR))
    palette.setColor(QPalette.Base, QColor("#ffffff"))
    palette.setColor(QPalette.AlternateBase, QColor("#f5f7fa"))
    palette.setColor(QPalette.ToolTipBase, QColor(PRIMARY_COLOR))
    palette.setColor(QPalette.ToolTipText, Qt.white)
    palette.setColor(QPalette.Text, QColor("#2c3e50"))
    palette.setColor(QPalette.Button, QColor(SECONDARY_COLOR))
    palette.setColor(QPalette.ButtonText, Qt.white)
    palette.setColor(QPalette.Highlight, QColor(SELECTED_COLOR))
    palette.setColor(QPalette.HighlightedText, Qt.black)
    app.setPalette(palette)
    
    window = GameManager()
    window.show()
    
    sys.exit(app.exec_())