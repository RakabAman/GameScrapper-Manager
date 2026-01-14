# match_dialog.py
"""
MatchDialog - a PyQt5 dialog to present fuzzy candidates and let the user pick one.
Updated: Restored original design with enhanced IGDB and Steam search buttons.
Fixed: Thread destruction error and IGDB search issues.

Usage:
  dlg = MatchDialog(original_item, candidates, parent=window)
  if dlg.exec_() == QDialog.Accepted:
      result = dlg.result_dict
"""

from PyQt5.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QListWidget, QListWidgetItem,
    QLabel, QPushButton, QTextEdit, QLineEdit, QCheckBox, QMessageBox, QWidget,
    QApplication, QGroupBox, QFormLayout, QFrame, QTabWidget
)
from PyQt5.QtGui import QPixmap
from PyQt5.QtCore import Qt, QSize, QCoreApplication, QThread, pyqtSignal, QObject
import webbrowser
import requests
from urllib.parse import quote_plus
import sys
import argparse
import time
import json
from typing import Dict, List, Optional, Any

# Use scraping module for all candidate searches
try:
    import scraping
    HAVE_SCRAPING = True
except Exception as e:
    print(f"Warning: Could not import scraping module: {e}")
    scraping = None
    HAVE_SCRAPING = False


class ImageLoader(QThread):
    """Thread for loading images in background to prevent UI freeze."""
    image_loaded = pyqtSignal(str, QPixmap)
    
    def __init__(self, url: str):
        super().__init__()
        self.url = url
    
    def run(self):
        if not self.url:
            return
        
        try:
            resp = requests.get(self.url, timeout=5)
            if resp.status_code == 200 and resp.content:
                pixmap = QPixmap()
                pixmap.loadFromData(resp.content)
                self.image_loaded.emit(self.url, pixmap)
        except Exception:
            pass
    
    def __del__(self):
        self.quit()
        self.wait()


class MatchDialog(QDialog):
    """
    original_item: dict with keys 'title','original_title','description'
    candidates: list of dicts with keys 'id','name','score','source' (optional other keys)
    Updated for IGDB: 'id' is IGDB ID, 'steam_id' for Steam AppID
    """

    def __init__(self, original_item: dict, candidates: list, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Resolve ambiguous match - IGDB & Steam")
        self.resize(920, 560)
        self.original = original_item or {}
        self.candidates = candidates or []
        self.result_dict = None
        
        # Image cache to avoid reloading
        self.image_cache = {}
        # Track active image loaders
        self.active_image_loaders = []
        
        # Initialize UI
        self.init_ui()
        
        # Select first candidate if present
        if self.igdb_list.count():
            self.igdb_list.setCurrentRow(0)

    def init_ui(self):
        """Initialize the user interface."""
        main_layout = QHBoxLayout()
        
        # Left: candidate list and search controls
        left_widget = QWidget()
        left_widget.setMaximumWidth(400)
        left_layout = QVBoxLayout()
        
        # Search controls group
        search_group = QGroupBox("Search Controls")
        search_layout = QVBoxLayout()
        
        # Horizontal layout for search buttons
        search_buttons_row1 = QHBoxLayout()
        search_buttons_row2 = QHBoxLayout()
        
        # IGDB search buttons
        self.igdb_search_title_btn = QPushButton("Search IGDB by Title")
        self.igdb_search_id_btn = QPushButton("Lookup IGDB by ID")
        
        # Steam search buttons
        self.steam_search_title_btn = QPushButton("Search Steam by Title")
        self.steam_search_id_btn = QPushButton("Lookup Steam by ID")
        
        # Combined search button
        self.search_both_btn = QPushButton("Search Both (IGDB & Steam)")
        self.search_both_btn.setStyleSheet("background-color: #4CAF50; color: white; font-weight: bold;")
        
        search_buttons_row1.addWidget(self.igdb_search_title_btn)
        search_buttons_row1.addWidget(self.igdb_search_id_btn)
        search_buttons_row2.addWidget(self.steam_search_title_btn)
        search_buttons_row2.addWidget(self.steam_search_id_btn)
        
        search_layout.addLayout(search_buttons_row1)
        search_layout.addLayout(search_buttons_row2)
        search_layout.addWidget(self.search_both_btn)
        
        # Manual search input
        search_layout.addWidget(QLabel("Manual Title:"))
        self.manual_title = QLineEdit(self.original.get('title', ''))
        search_layout.addWidget(self.manual_title)
        
        # Manual IDs
        ids_layout = QHBoxLayout()
        ids_layout.addWidget(QLabel("IGDB ID:"))
        self.manual_igdb_id = QLineEdit(self.original.get('igdb_id', ''))
        self.manual_igdb_id.setMaximumWidth(150)
        ids_layout.addWidget(self.manual_igdb_id)
        
        ids_layout.addWidget(QLabel("Steam ID:"))
        self.manual_steam_id = QLineEdit(self.original.get('steam_id', '') or self.original.get('app_id', ''))
        self.manual_steam_id.setMaximumWidth(150)
        ids_layout.addWidget(self.manual_steam_id)
        
        search_layout.addLayout(ids_layout)
        
        search_group.setLayout(search_layout)
        left_layout.addWidget(search_group)
        
        # Candidate lists with tabs
        tabs_widget = QTabWidget()
        
        # IGDB tab
        igdb_tab = QWidget()
        igdb_layout = QVBoxLayout()
        self.igdb_list = QListWidget()
        self.igdb_list.setAlternatingRowColors(True)
        igdb_layout.addWidget(self.igdb_list)
        igdb_tab.setLayout(igdb_layout)
        tabs_widget.addTab(igdb_tab, "IGDB Results")
        
        # Steam tab
        steam_tab = QWidget()
        steam_layout = QVBoxLayout()
        self.steam_list = QListWidget()
        self.steam_list.setAlternatingRowColors(True)
        steam_layout.addWidget(self.steam_list)
        steam_tab.setLayout(steam_layout)
        tabs_widget.addTab(steam_tab, "Steam Results")
        
        left_layout.addWidget(tabs_widget, 1)  # Give it stretch
        
        left_widget.setLayout(left_layout)
        main_layout.addWidget(left_widget)
        
        # Right: preview and manual fields
        right_widget = QWidget()
        right_layout = QVBoxLayout()
        
        # Original item info (removed description as requested)
        self.title_label = QLabel(f"<b>Original:</b> {self.original.get('original_title') or self.original.get('title','')}")
        self.title_label.setWordWrap(True)
        
        # Preview area
        preview_frame = QFrame()
        preview_frame.setFrameStyle(QFrame.Box | QFrame.Raised)
        preview_layout = QHBoxLayout()
        
        self.cover = QLabel()
        self.cover.setFixedSize(QSize(220, 120))
        self.cover.setAlignment(Qt.AlignCenter)
        self.cover.setStyleSheet("border: 1px solid #ccc; background-color: #f0f0f0;")
        self.cover.setText("No cover")
        
        self.meta = QLabel()
        self.meta.setWordWrap(True)
        self.meta.setTextInteractionFlags(Qt.TextBrowserInteraction)
        self.meta.setOpenExternalLinks(True)
        
        preview_layout.addWidget(self.cover)
        preview_layout.addWidget(self.meta)
        preview_frame.setLayout(preview_layout)
        
        # Description preview
        self.desc_preview = QTextEdit()
        self.desc_preview.setReadOnly(True)
        self.desc_preview.setFixedHeight(150)
        
        # Action buttons
        self.apply_btn = QPushButton("Apply Selected")
        self.apply_next_btn = QPushButton("Apply + Next")
        self.skip_btn = QPushButton("Skip")
        self.open_btn = QPushButton("Open Candidate")
        self.overwrite_chk = QCheckBox("Overwrite existing fields")
        
        # Status label
        self.status_label = QLabel("")
        self.status_label.setStyleSheet("color: #666; font-size: 10pt;")
        
        # Add widgets to right layout
        right_layout.addWidget(self.title_label)
        right_layout.addWidget(QLabel("<b>Candidate Preview:</b>"))
        right_layout.addWidget(preview_frame)
        right_layout.addWidget(QLabel("<b>Candidate Description:</b>"))
        right_layout.addWidget(self.desc_preview)
        right_layout.addWidget(self.status_label)
        
        # Buttons row
        btn_row = QHBoxLayout()
        btn_row.addWidget(self.apply_btn)
        btn_row.addWidget(self.apply_next_btn)
        btn_row.addWidget(self.open_btn)
        btn_row.addWidget(self.skip_btn)
        right_layout.addLayout(btn_row)
        right_layout.addWidget(self.overwrite_chk)
        
        right_widget.setLayout(right_layout)
        main_layout.addWidget(right_widget)
        
        self.setLayout(main_layout)
        
        # Connect signals
        self.igdb_list.currentItemChanged.connect(lambda current, previous: self.on_candidate_selected(current, previous, 'igdb'))
        self.steam_list.currentItemChanged.connect(lambda current, previous: self.on_candidate_selected(current, previous, 'steam'))
        self.apply_btn.clicked.connect(self.on_apply)
        self.apply_next_btn.clicked.connect(self.on_apply_next)
        self.skip_btn.clicked.connect(self.reject)
        self.open_btn.clicked.connect(self.on_open_candidate)
        self.igdb_search_title_btn.clicked.connect(self.search_igdb_by_title)
        self.igdb_search_id_btn.clicked.connect(self.lookup_igdb_by_id)
        self.steam_search_title_btn.clicked.connect(self.search_steam_by_title)
        self.steam_search_id_btn.clicked.connect(self.lookup_steam_by_id)
        self.search_both_btn.clicked.connect(self.search_both_by_title)
        
        # Initialize button states
        self.apply_btn.setEnabled(False)
        self.apply_next_btn.setEnabled(False)
        self.open_btn.setEnabled(False)
        
        # Populate initial candidates into appropriate lists
        self._populate_initial_candidates()

    def closeEvent(self, event):
        """Clean up threads when dialog closes."""
        for loader in self.active_image_loaders:
            if loader.isRunning():
                loader.quit()
                loader.wait()
        self.active_image_loaders.clear()
        event.accept()

    def _populate_initial_candidates(self):
        """Separate initial candidates into IGDB and Steam lists."""
        for cand in self.candidates:
            source = cand.get("source", "").lower()
            if 'igdb' in source:
                self._add_candidate_to_list(cand, 'igdb')
            elif 'steam' in source:
                self._add_candidate_to_list(cand, 'steam')
            else:
                # Default to IGDB if source not specified
                self._add_candidate_to_list(cand, 'igdb')

    # -------------------------
    # Candidate list helpers
    # -------------------------
    def _add_candidate_to_list(self, candidate: dict, list_type: str = 'igdb'):
        """Add a single candidate to the appropriate list."""
        name = candidate.get("name") or candidate.get("title") or ""
        source = candidate.get("source") or ""
        score = int(candidate.get("score", 0))
        igdb_id = candidate.get("id") or candidate.get("igdb_id") or ""
        steam_id = candidate.get("steam_id") or candidate.get("steam_app_id") or candidate.get("app_id") or ""
        
        label = f"{name}"
        if igdb_id:
            label += f" [IGDB:{igdb_id}]"
        if steam_id:
            label += f" [Steam:{steam_id}]"
        label += f"  [{source}]  ({score}%)"
        
        # Add rating if available
        if candidate.get("rating_display"):
            label += f" - {candidate['rating_display']}"
        
        it = QListWidgetItem(label)
        it.setData(Qt.UserRole, candidate)
        
        if list_type == 'igdb':
            self.igdb_list.addItem(it)
            # Select if it's the first one
            if self.igdb_list.count() == 1:
                self.igdb_list.setCurrentRow(0)
        else:
            self.steam_list.addItem(it)
            if self.steam_list.count() == 1:
                self.steam_list.setCurrentRow(0)

    # -------------------------
    # Search functions
    # -------------------------
    def search_igdb_by_title(self):
        """Search IGDB by game title."""
        if not HAVE_SCRAPING:
            QMessageBox.warning(self, "Search unavailable", "Scraping module not available.")
            return
        
        title = self.manual_title.text().strip()
        if not title:
            QMessageBox.information(self, "Search", "Please enter a title to search.")
            return
        
        self.status_label.setText("Searching IGDB...")
        old_cursor = self.cursor()
        self.setCursor(Qt.WaitCursor)
        
        try:
            QCoreApplication.processEvents()
            
            # Clear Steam ID when searching IGDB by title
            #self.manual_steam_id.clear()
            
            # Use scraping module to find IGDB candidates
            candidates = scraping.find_candidates_for_title_igdb(title, max_candidates=12)
            
            if not candidates:
                self.status_label.setText("No IGDB candidates found.")
                QMessageBox.information(self, "No results", "No IGDB candidates found for this title.")
                return
            
            # Clear and populate IGDB list
            self.igdb_list.clear()
            for candidate in candidates:
                self._add_candidate_to_list(candidate, 'igdb')
            
            self.status_label.setText(f"Found {len(candidates)} IGDB candidates.")
            
        except Exception as e:
            self.status_label.setText(f"IGDB search failed: {str(e)[:50]}...")
            QMessageBox.warning(self, "Search Error", f"IGDB search failed: {e}")
        finally:
            self.setCursor(old_cursor)
            QCoreApplication.processEvents()

    def lookup_igdb_by_id(self):
        """Lookup specific game by IGDB ID only."""
        if not HAVE_SCRAPING:
            QMessageBox.warning(self, "Lookup unavailable", "Scraping module not available.")
            return
        
        igdb_id = self.manual_igdb_id.text().strip()
        if not igdb_id:
            QMessageBox.information(self, "Lookup", "Please enter an IGDB ID.")
            return
        
        self.status_label.setText("Looking up IGDB ID...")
        old_cursor = self.cursor()
        self.setCursor(Qt.WaitCursor)
        
        try:
            QCoreApplication.processEvents()
            
            # Clear Steam ID when searching IGDB by ID (mutual exclusive)
           # self.manual_steam_id.clear()
            
            # Try to get IGDB data by ID
            candidate_data = None
            
            # Try to use the scraping module's functions based on what's available
            try:
                # Option 1: Check if there's a direct ID lookup function
                if hasattr(scraping, 'get_game_by_igdb_id'):
                    result = scraping.get_game_by_igdb_id(igdb_id)
                    if result and not result.get("__error__"):
                        candidate_data = result
                
                # Option 2: Try the standard igdb_scraper function
                if not candidate_data and hasattr(scraping, 'igdb_scraper'):
                    # Use a descriptive title to trigger a search
                    result = scraping.igdb_scraper(f"id:{igdb_id}", auto_accept_score=0)
                    if "__error__" not in result:
                        # Check if it's a single result or multiple candidates
                        if "__candidates__" in result:
                            # Search for our ID in candidates
                            for cand in result["__candidates__"]:
                                cand_id = str(cand.get("id") or cand.get("igdb_id") or "")
                                if cand_id == igdb_id:
                                    candidate_data = cand
                                    break
                        else:
                            # Single result
                            result_id = str(result.get("id") or result.get("igdb_id") or "")
                            if result_id == igdb_id:
                                candidate_data = result
                
                # Option 3: Try to fetch from IGDB API directly (fallback)
                if not candidate_data:
                    # This is a direct API call that might work if you have IGDB API access
                    candidate_data = self._fetch_igdb_data_directly(igdb_id)
                    
            except Exception as e:
                print(f"Error in IGDB lookup: {e}")
            
            # If we still don't have data, create minimal candidate
            if not candidate_data:
                candidate_data = {
                    "id": igdb_id,
                    "igdb_id": igdb_id,
                    "name": f"IGDB Game ID: {igdb_id}",
                    "score": 100,
                    "source": "igdb_manual_id_only",
                    "description": "Could not fetch metadata. The game may not exist or the scraping module doesn't support direct ID lookups.",
                    "genres": "Unknown",
                    "developer": "Unknown"
                }
            
            # Print scraped metadata to console
            print("\n=== IGDB ID Scraped Metadata ===")
            print(f"IGDB ID: {igdb_id}")
            if candidate_data and "description" in candidate_data and candidate_data["description"]:
                print(f"Name: {candidate_data.get('name', 'Unknown')}")
                print(f"Source: {candidate_data.get('source', 'N/A')}")
                print(f"Has description: {'Yes' if candidate_data.get('description') else 'No'}")
                print(f"Has cover: {'Yes' if candidate_data.get('cover_url') else 'No'}")
            else:
                print("⚠ Minimal metadata only - consider using title search")
            print("=================================\n")
            
            # Clear and populate IGDB list
            self.igdb_list.clear()
            self._add_candidate_to_list(candidate_data, 'igdb')
            self.status_label.setText(f"Found IGDB game: {candidate_data.get('name', 'Unknown')}")
            
        except Exception as e:
            error_msg = str(e)
            self.status_label.setText(f"IGDB lookup failed")
            QMessageBox.warning(self, "Lookup Error", 
                f"IGDB lookup may not be fully supported by the scraping module.\n\n"
                f"Error: {error_msg[:100]}\n\n"
                f"Try using 'Search IGDB by Title' for better results.")
        finally:
            self.setCursor(old_cursor)
            QCoreApplication.processEvents()
    
    def _fetch_igdb_data_directly(self, igdb_id):
        """Fallback method to try direct IGDB API call."""
        try:
            # You would need to have IGDB API credentials set up for this
            # This is just a template - you'd need to implement actual API call
            print(f"[INFO] Attempting direct IGDB API call for ID: {igdb_id}")
            
            # Example structure (you'd need to implement the actual API call):
            # headers = {'Client-ID': 'your_client_id', 'Authorization': 'Bearer your_token'}
            # response = requests.post('https://api.igdb.com/v4/games', 
            #                         headers=headers,
            #                         data=f'fields name,summary,cover.url,genres.name,developers.name; where id = {igdb_id};')
            
            # For now, return None to indicate we can't fetch directly
            return None
            
        except Exception as e:
            print(f"Direct IGDB API call failed: {e}")
            return None

    def search_steam_by_title(self):
        """Search Steam by game title."""
        if not HAVE_SCRAPING:
            QMessageBox.warning(self, "Search unavailable", "Scraping module not available.")
            return
        
        title = self.manual_title.text().strip()
        if not title:
            QMessageBox.information(self, "Search", "Please enter a title to search.")
            return
        
        self.status_label.setText("Searching Steam...")
        old_cursor = self.cursor()
        self.setCursor(Qt.WaitCursor)
        
        try:
            QCoreApplication.processEvents()
            
            # Clear IGDB ID when searching Steam by title
            # self.manual_igdb_id.clear()
            
            # Use scraping module to find Steam candidates
            candidates = scraping.find_candidates_for_title(title, max_candidates=12)
            
            if not candidates:
                self.status_label.setText("No Steam candidates found.")
                QMessageBox.information(self, "No results", "No Steam candidates found for this title.")
                return
            
            # Clear and populate Steam list
            self.steam_list.clear()
            for candidate in candidates:
                # Convert to our format
                formatted_candidate = {
                    "id": candidate.get("id", ""),
                    "steam_id": candidate.get("id", ""),
                    "steam_app_id": candidate.get("id", ""),
                    "name": candidate.get("name", title),
                    "score": candidate.get("score", 0),
                    "source": candidate.get("source", "steam"),
                    "tiny_image": candidate.get("tiny_image", "")
                }
                self._add_candidate_to_list(formatted_candidate, 'steam')
            
            self.status_label.setText(f"Found {len(candidates)} Steam candidates.")
            
        except Exception as e:
            self.status_label.setText(f"Steam search failed: {str(e)[:50]}...")
            QMessageBox.warning(self, "Search Error", f"Steam search failed: {e}")
        finally:
            self.setCursor(old_cursor)
            QCoreApplication.processEvents()

    def lookup_steam_by_id(self):
        """Lookup specific game by Steam AppID only."""
        if not HAVE_SCRAPING:
            QMessageBox.warning(self, "Lookup unavailable", "Scraping module not available.")
            return
        
        steam_id = self.manual_steam_id.text().strip()
        if not steam_id:
            QMessageBox.information(self, "Lookup", "Please enter a Steam AppID.")
            return
        
        self.status_label.setText("Looking up Steam ID...")
        old_cursor = self.cursor()
        self.setCursor(Qt.WaitCursor)
        
        try:
            QCoreApplication.processEvents()
            
            # Clear IGDB ID when searching Steam by ID (mutual exclusive)
            # self.manual_igdb_id.clear()
            
            # Use get_store_metadata with empty title since we're searching by ID only
            result = scraping.get_store_metadata(steam_id, "")
            
            if not result.get("title"):
                self.status_label.setText("Steam lookup failed: Invalid AppID or no data")
                QMessageBox.warning(self, "Lookup Error", "Steam lookup failed: Invalid AppID or no data")
                return
            
            # Create candidate
            candidate = {
                "id": steam_id,
                "steam_id": steam_id,
                "steam_app_id": steam_id,
                "name": result.get("title", f"Steam App {steam_id}"),
                "score": 100,
                "source": "steam_manual_id_only",
                "genres": result.get("genres", ""),
                "developer": result.get("developer", ""),
                "publisher": result.get("publisher", ""),
                "cover_url": result.get("cover_url", ""),
                "description": result.get("description", ""),
                "release_date": result.get("release_date", "")
            }
            
            # Print scraped metadata to console
            print("\n=== Steam ID Scraped Metadata ===")
            print(f"Steam ID: {steam_id}")
            print(f"Full data: {json.dumps(result, indent=2, default=str)}")
            print("==================================\n")
            
            # Clear and populate Steam list with the found candidate
            self.steam_list.clear()
            self._add_candidate_to_list(candidate, 'steam')
            self.status_label.setText(f"Found Steam game: {candidate['name']}")
            
        except Exception as e:
            self.status_label.setText(f"Steam lookup failed: {str(e)[:50]}...")
            QMessageBox.warning(self, "Lookup Error", f"Steam lookup failed: {e}")
        finally:
            self.setCursor(old_cursor)
            QCoreApplication.processEvents()

    def search_both_by_title(self):
        """Search both IGDB and Steam using title only."""
        if not HAVE_SCRAPING:
            QMessageBox.warning(self, "Search unavailable", "Scraping module not available.")
            return
        
        title = self.manual_title.text().strip()
        if not title:
            QMessageBox.information(self, "Search", "Please enter a title to search.")
            return
        
        self.status_label.setText("Searching IGDB and Steam...")
        old_cursor = self.cursor()
        self.setCursor(Qt.WaitCursor)
        
        try:
            QCoreApplication.processEvents()
            
            # Clear both ID fields for combined search
            self.manual_igdb_id.clear()
            self.manual_steam_id.clear()
            
            # Search IGDB
            igdb_candidates = []
            try:
                igdb_candidates = scraping.find_candidates_for_title_igdb(title, max_candidates=6)
            except Exception as e:
                print(f"IGDB search error: {e}")
            
            # Search Steam
            steam_candidates = []
            try:
                steam_raw = scraping.find_candidates_for_title(title, max_candidates=6)
                for candidate in steam_raw:
                    formatted_candidate = {
                        "id": candidate.get("id", ""),
                        "steam_id": candidate.get("id", ""),
                        "steam_app_id": candidate.get("id", ""),
                        "name": candidate.get("name", title),
                        "score": candidate.get("score", 0),
                        "source": candidate.get("source", "steam"),
                        "tiny_image": candidate.get("tiny_image", "")
                    }
                    steam_candidates.append(formatted_candidate)
            except Exception as e:
                print(f"Steam search error: {e}")
            
            # Clear and populate both lists
            self.igdb_list.clear()
            self.steam_list.clear()
            
            for candidate in igdb_candidates:
                self._add_candidate_to_list(candidate, 'igdb')
            
            for candidate in steam_candidates:
                self._add_candidate_to_list(candidate, 'steam')
            
            total_found = len(igdb_candidates) + len(steam_candidates)
            self.status_label.setText(f"Found {len(igdb_candidates)} IGDB + {len(steam_candidates)} Steam = {total_found} total candidates.")
            
        except Exception as e:
            self.status_label.setText(f"Combined search failed: {str(e)[:50]}...")
            QMessageBox.warning(self, "Search Error", f"Combined search failed: {e}")
        finally:
            self.setCursor(old_cursor)
            QCoreApplication.processEvents()

    # -------------------------
    # Candidate selection preview
    # -------------------------
    def on_candidate_selected(self, current, previous, source: str):
        if not current:
            self.cover.clear()
            self.cover.setText("No cover")
            self.meta.setText("")
            self.desc_preview.clear()
            self.apply_btn.setEnabled(False)
            self.apply_next_btn.setEnabled(False)
            self.open_btn.setEnabled(False)
            return
        
        c = current.data(Qt.UserRole)
        
        # Enable action buttons
        self.apply_btn.setEnabled(True)
        self.apply_next_btn.setEnabled(True)
        self.open_btn.setEnabled(True)
        
        # Try to load image from candidate data
        cover_url = c.get('cover_url') or c.get('tiny_image') or ""
        if cover_url and cover_url.startswith(('http://', 'https://')):
            # Check cache first
            if cover_url in self.image_cache:
                pixmap = self.image_cache[cover_url]
                self.set_cover_image(pixmap)
            else:
                # Load asynchronously
                self.cover.setText("Loading...")
                self.load_image_async(cover_url)
        else:
            self.cover.setText("No cover")
            self.cover.setPixmap(QPixmap())

        # Build metadata display with IGDB and Steam info
        igdb_id = c.get('id') or c.get('igdb_id') or ""
        steam_id = c.get('steam_id') or c.get('steam_app_id') or c.get('app_id') or ""
        
        meta_html = f"<b>{c.get('name','')}</b><br>"
        if igdb_id:
            meta_html += f"IGDB ID: {igdb_id}<br>"
        if steam_id:
            meta_html += f"Steam ID: {steam_id}<br>"
        
        # Add additional metadata if available
        if c.get('genres'):
            meta_html += f"Genres: {c.get('genres')}<br>"
        if c.get('developer'):
            meta_html += f"Developer: {c.get('developer')}<br>"
        if c.get('publisher'):
            meta_html += f"Publisher: {c.get('publisher')}<br>"
        if c.get('release_date'):
            meta_html += f"Release: {c.get('release_date')}<br>"
        
        meta_html += f"Score: {int(c.get('score',0))}%<br>Source: {c.get('source','')}"
        
        # Add rating if available
        if c.get('rating_display'):
            meta_html += f"<br>Rating: {c.get('rating_display')}"
        
        # Add links (removed SteamDB and PCGamingWiki as requested)
        links_added = False
        links_html = "<br>Links: "
        
        if igdb_id:
            igdb_link = f"https://www.igdb.com/games/{igdb_id}"
            links_html += f"<a href='{igdb_link}'>IGDB</a>"
            links_added = True
        elif c.get('name'):
            igdb_link = f"https://www.igdb.com/search?query={quote_plus(c.get('name'))}"
            links_html += f"<a href='{igdb_link}'>IGDB Search</a>"
            links_added = True
            
        if steam_id:
            steam_link = f"https://store.steampowered.com/app/{steam_id}"
            if links_added:
                links_html += " | "
            links_html += f"<a href='{steam_link}'>Steam</a>"
            links_added = True
        
        if links_added:
            meta_html += links_html

        self.meta.setText(meta_html)
        
        # Update description preview
        description = c.get('description', '') or c.get('summary', '')
        self.desc_preview.setPlainText(description)
        
        # Update manual fields - IMPORTANT: Don't overwrite IDs from opposite source
        # Update manual fields - IMPORTANT: Don't overwrite IDs from opposite source
        if c.get('name'):
            self.manual_title.setText(c.get('name'))
        
        # Only update IGDB ID field if we're selecting from IGDB list
        if source == 'igdb':
            igdb_id = c.get('id') or c.get('igdb_id') or ""
            self.manual_igdb_id.setText(str(igdb_id) if igdb_id else "N/A")
        
        # Only update Steam ID field if we're selecting from Steam list
        if source == 'steam':
            steam_id = c.get('steam_id') or c.get('steam_app_id') or c.get('app_id') or ""
            self.manual_steam_id.setText(str(steam_id) if steam_id else "N/A")

    def load_image_async(self, url: str):
        """Load image asynchronously in background thread."""
        if not url or url in self.image_cache:
            return
        
        # Clean up any finished loaders
        self.active_image_loaders = [loader for loader in self.active_image_loaders if loader.isRunning()]
        
        # Create and start loader thread
        loader = ImageLoader(url)
        loader.image_loaded.connect(self.on_image_loaded)
        loader.finished.connect(lambda: self.active_image_loaders.remove(loader) if loader in self.active_image_loaders else None)
        self.active_image_loaders.append(loader)
        loader.start()

    def on_image_loaded(self, url: str, pixmap: QPixmap):
        """Handle image loaded signal."""
        # Cache the image
        self.image_cache[url] = pixmap
        
        # Update cover if this is the current candidate's image
        # Check both lists
        current_igdb = self.igdb_list.currentItem()
        current_steam = self.steam_list.currentItem()
        
        current_item = current_igdb or current_steam
        if current_item:
            c = current_item.data(Qt.UserRole)
            current_url = c.get('cover_url') or c.get('tiny_image') or ""
            if current_url == url:
                self.set_cover_image(pixmap)

    def set_cover_image(self, pixmap: QPixmap):
        """Set cover image with proper scaling."""
        if pixmap and not pixmap.isNull():
            scaled_pixmap = pixmap.scaled(
                self.cover.size(),
                Qt.KeepAspectRatio,
                Qt.SmoothTransformation
            )
            self.cover.setPixmap(scaled_pixmap)
            self.cover.setText("")
        else:
            self.cover.setText("No cover")
            self.cover.setPixmap(QPixmap())

    # -------------------------
    # Open candidate in browser
    # -------------------------
    def on_open_candidate(self):
        """Open selected candidate in browser."""
        current_igdb = self.igdb_list.currentItem()
        current_steam = self.steam_list.currentItem()
        
        current_item = current_igdb or current_steam
        if not current_item:
            return
        
        c = current_item.data(Qt.UserRole)
        igdb_id = c.get('id') or c.get('igdb_id') or ""
        steam_id = c.get('steam_id') or c.get('steam_app_id') or c.get('app_id') or ""
        
        # Prioritize IGDB link
        if igdb_id:
            webbrowser.open(f"https://www.igdb.com/games/{igdb_id}")
        elif steam_id:
            webbrowser.open(f"https://store.steampowered.com/app/{steam_id}")
        elif c.get('name'):
            webbrowser.open(f"https://www.igdb.com/search?query={quote_plus(c.get('name'))}")

    # -------------------------
    # Collect result and finish
    # -------------------------
    # In the _collect_result method (around line 843), modify it as follows:
    def _collect_result(self):
        """Get current candidate from either list."""
        current_igdb = self.igdb_list.currentItem()
        current_steam = self.steam_list.currentItem()
        
        current_item = current_igdb or current_steam
        candidate = current_item.data(Qt.UserRole) if current_item else None
        
        overwrite = self.overwrite_chk.isChecked()
        res = {"chosen_candidate": candidate, "applied_by": "user"}
        
        # Determine selected_title: Use IGDB title if available, otherwise original title
        if candidate and candidate.get('name'):
            res['selected_title'] = candidate.get('name')
        else:
            # Fall back to original title from original_item
            original_title = self.original.get('original_title') or self.original.get('title', '')
            res['selected_title'] = original_title if original_title else self.manual_title.text().strip()
        
        # Print selected game output to console
        print("\n=== SELECTED GAME OUTPUT ===")
        print(f"Original title: {self.original.get('title', '')}")
        print(f"Selected candidate: {candidate.get('name', 'Unknown') if candidate else 'None'}")
        print(f"Selected title (for output): {res['selected_title']}")
        
        # Title from manual field or candidate - keep existing logic for the 'title' field
        manual_title = self.manual_title.text().strip()
        if manual_title:
            res['title'] = manual_title
            print(f"Manual title: {manual_title}")
        elif candidate and candidate.get('name'):
            res['title'] = candidate.get('name')
            print(f"Title: {candidate.get('name')}")
        else:
            # Fallback to original if nothing else
            original_title = self.original.get('original_title') or self.original.get('title', '')
            res['title'] = original_title if original_title else "N/A"
            print(f"Title (fallback): {res['title']}")
        
        # IGDB ID - Use "N/A" if not available
        manual_igdb_id = self.manual_igdb_id.text().strip()
        if manual_igdb_id:
            res['igdb_id'] = manual_igdb_id
            print(f"IGDB ID: {manual_igdb_id}")
        elif candidate and candidate.get('id'):
            res['igdb_id'] = candidate.get('id')
            print(f"IGDB ID: {candidate.get('id')}")
        elif candidate and candidate.get('igdb_id'):
            res['igdb_id'] = candidate.get('igdb_id')
            print(f"IGDB ID: {candidate.get('igdb_id')}")
        else:
            res['igdb_id'] = "N/A"
            print(f"IGDB ID: N/A (not available)")
        
        # Steam ID - Use "N/A" if not available
        manual_steam_id = self.manual_steam_id.text().strip()
        if manual_steam_id:
            res['steam_id'] = manual_steam_id
            res['app_id'] = manual_steam_id  # Keep backward compatibility
            print(f"Steam ID: {manual_steam_id}")
        elif candidate and candidate.get('steam_id'):
            res['steam_id'] = candidate.get('steam_id')
            res['app_id'] = candidate.get('steam_id')  # Keep backward compatibility
            print(f"Steam ID: {candidate.get('steam_id')}")
        elif candidate and candidate.get('steam_app_id'):
            res['steam_id'] = candidate.get('steam_app_id')
            res['app_id'] = candidate.get('steam_app_id')  # Keep backward compatibility
            print(f"Steam ID: {candidate.get('steam_app_id')}")
        elif candidate and candidate.get('app_id'):
            res['steam_id'] = candidate.get('app_id')
            res['app_id'] = candidate.get('app_id')  # Keep backward compatibility
            print(f"Steam ID: {candidate.get('app_id')}")
        else:
            res['steam_id'] = "N/A"
            res['app_id'] = "N/A"  # Keep backward compatibility
            print(f"Steam ID: N/A (not available)")
        
        # Additional metadata if available
        if candidate:
            for key in ['genres', 'developer', 'publisher', 'description', 'cover_url', 
                       'release_date', 'rating_display', 'score', 'source']:
                if key in candidate and candidate[key]:
                    res[key] = candidate[key]
                    print(f"{key}: {candidate[key]}")
        
        # Ensure 'genres' has a value (can be empty string from API)
        if 'genres' not in res or not res['genres']:
            res['genres'] = "N/A"
            print("genres: N/A (not available)")
        
        res['overwrite'] = overwrite
        print(f"Overwrite: {overwrite}")
        print("=== END SELECTION ===\n")
        
        return res

    def on_apply(self):
        self.result_dict = self._collect_result()
        self.accept()

    def on_apply_next(self):
        self.result_dict = self._collect_result()
        # return custom code 2 to indicate apply+next
        self.done(2)


# ============================================================================
# CLI Testing Functionality
# ============================================================================

def test_dialog_cli():
    """Test the MatchDialog from command line."""
    parser = argparse.ArgumentParser(description="Test MatchDialog GUI")
    parser.add_argument("title", help="Game title to test with")
    parser.add_argument("--steam-id", help="Optional Steam AppID to include")
    parser.add_argument("--igdb-id", help="Optional IGDB ID to include")
    parser.add_argument("--generate-candidates", action="store_true",
                       help="Generate test candidates from scraping module")
    parser.add_argument("--max-candidates", type=int, default=5,
                       help="Maximum number of candidates to generate")
    
    args = parser.parse_args()
    
    print(f"[+] Testing MatchDialog with:")
    print(f"    Title: {args.title}")
    print(f"    Steam ID: {args.steam_id or 'None'}")
    print(f"    IGDB ID: {args.igdb_id or 'None'}")
    print(f"    Generate candidates: {args.generate_candidates}")
    print(f"    Max candidates: {args.max_candidates}")
    print("-" * 60)
    
    # Create original item
    original_item = {
        'title': args.title,
        'original_title': args.title,
        'description': f"Test description for {args.title}",
        'app_id': args.steam_id or '',
        'steam_id': args.steam_id or '',
        'igdb_id': args.igdb_id or ''
    }
    
    # Create candidates
    candidates = []
    
    if args.generate_candidates and HAVE_SCRAPING:
        print("[+] Generating candidates from scraping module...")
        try:
            # Get IGDB candidates
            igdb_candidates = scraping.find_candidates_for_title_igdb(
                args.title, 
                max_candidates=args.max_candidates
            )
            
            for cand in igdb_candidates:
                candidates.append(cand)
            
            # Get Steam candidates
            steam_candidates = scraping.find_candidates_for_title(
                args.title, 
                max_candidates=args.max_candidates
            )
            
            for cand in steam_candidates:
                # Convert to our format
                formatted_cand = {
                    "id": cand.get("id", ""),
                    "steam_id": cand.get("id", ""),
                    "steam_app_id": cand.get("id", ""),
                    "name": cand.get("name", args.title),
                    "score": cand.get("score", 0),
                    "source": cand.get("source", "steam"),
                    "tiny_image": cand.get("tiny_image", "")
                }
                candidates.append(formatted_cand)
            
            print(f"[+] Generated {len(candidates)} candidates")
            
        except Exception as e:
            print(f"[-] Error generating candidates: {e}")
            # Create dummy candidates as fallback
            candidates = [
                {
                    "id": args.igdb_id or "12345",
                    "name": f"{args.title} (Enhanced Edition)",
                    "score": 95,
                    "source": "test",
                    "steam_id": args.steam_id or "67890",
                    "steam_app_id": args.steam_id or "67890",
                    "igdb_id": args.igdb_id or "12345",
                    "genres": "Action, Adventure",
                    "developer": "Test Developer",
                    "cover_url": ""
                }
            ]
    else:
        # Create some dummy candidates for testing
        candidates = [
            {
                "id": args.igdb_id or "12345",
                "name": f"{args.title} (Enhanced Edition)",
                "score": 95,
                "source": "test",
                "steam_id": args.steam_id or "67890",
                "steam_app_id": args.steam_id or "67890",
                "igdb_id": args.igdb_id or "12345",
                "genres": "Action, Adventure",
                "developer": "Test Developer",
                "cover_url": ""
            },
            {
                "id": "67891",
                "name": f"{args.title} 2",
                "score": 85,
                "source": "test",
                "steam_id": "67891",
                "steam_app_id": "67891",
                "igdb_id": "12346",
                "genres": "RPG",
                "developer": "Another Developer"
            }
        ]
        print("[+] Using test candidates")
    
    # Print candidate summary
    if candidates:
        print(f"\n[+] Candidates to display ({len(candidates)}):")
        for i, cand in enumerate(candidates, 1):
            print(f"  {i}. {cand.get('name')} "
                  f"(Score: {cand.get('score')}%, "
                  f"Steam: {cand.get('steam_id', 'N/A')}, "
                  f"IGDB: {cand.get('igdb_id', 'N/A')})")
    
    print("\n[+] Starting GUI application...")
    print("    Note: Close the dialog to see the result")
    print("    Use the new buttons to search IGDB/Steam by title or ID")
    print("-" * 60)
    
    # Start Qt application
    app = QApplication(sys.argv)
    
    # Create and show dialog
    dialog = MatchDialog(original_item, candidates)
    result = dialog.exec_()
    
    # Clean up threads
    for loader in dialog.active_image_loaders:
        if loader.isRunning():
            loader.quit()
            loader.wait()
    
    # Process result
    if result == QDialog.Accepted:
        print("\n[+] Dialog accepted!")
        print("    Result dictionary:")
        for key, value in dialog.result_dict.items():
            if key == 'chosen_candidate' and value:
                print(f"      {key}:")
                for subkey, subvalue in value.items():
                    print(f"        {subkey}: {subvalue}")
            else:
                print(f"      {key}: {value}")
    elif result == 2:  # Apply + Next
        print("\n[+] Dialog accepted with 'Apply + Next'!")
        print("    Result dictionary:")
        for key, value in dialog.result_dict.items():
            if key == 'chosen_candidate' and value:
                print(f"      {key}:")
                for subkey, subvalue in value.items():
                    print(f"        {subkey}: {subvalue}")
            else:
                print(f"      {key}: {value}")
    else:
        print("\n[+] Dialog rejected or skipped")
    
    print("\n[+] Test completed")
    return 0


# ============================================================================
# Main entry point
# ============================================================================

if __name__ == "__main__":
    # Check if we're being called from command line with arguments
    if len(sys.argv) > 1 and not sys.argv[1].startswith('-'):
        # Run CLI test
        sys.exit(test_dialog_cli())
    else:
        # If no arguments, show usage
        print("MatchDialog - Interactive Game Matching Dialog")
        print("\nUsage for GUI testing:")
        print("  python match_dialog.py \"Game Title\" [options]")
        print("\nOptions:")
        print("  --steam-id APPID          Add specific Steam AppID")
        print("  --igdb-id IGDBID          Add specific IGDB ID")
        print("  --generate-candidates     Generate real candidates using scraping module")
        print("  --max-candidates NUM      Maximum candidates to generate (default: 5)")
        print("\nNew features:")
        print("  • Search IGDB by Title button")
        print("  • Lookup IGDB by ID button (ID only, no title used)")
        print("  • Search Steam by Title button")
        print("  • Lookup Steam by ID button (ID only, no title used)")
        print("  • Search Both (IGDB & Steam) by Title button")
        print("  • Separate IGDB and Steam candidate lists")
        print("  • Console output for selected games")
        print("  • Console output for scraped metadata")
        print("  • Mutual exclusive ID fields when searching by ID")
        print("  • No SteamDB/PCGW links in preview")
        print("\nExamples:")
        print("  python match_dialog.py \"Cyberpunk 2077\" --generate-candidates")
        print("  python match_dialog.py \"The Witcher 3\" --steam-id 292030 --igdb-id 1942")
        print("\nFor GUI integration, import MatchDialog class directly.")