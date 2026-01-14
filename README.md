Game Manager GUI 🎮

**Short Description:**  
A modern PyQt5-based Game Manager application for organizing, scraping, and exporting game metadata with caching, batch operations, and a beautified GUI.

---

## ✨ Features

- **Game Database Management**  
  Import/export games via CSV, JSON, TXT, Excel, or SQLite.

- **Steam & IGDB Metadata Scraping**  
  Automatically fetch and merge metadata from Steam and IGDB.

- **Image & Video Caching**  
  Download screenshots, microtrailers, and trailers with smart caching (size validation, recaching, clearing cache fields).

- **Batch Operations**  
  Perform scraping or edits on multiple selected games simultaneously.

- **Export Options**  
  Export your library to PDF or HTML with styled layouts.

- **Clickable Assets**  
  URLs for assets are clickable directly from the GUI.

- **Extended Help Menu**  
  Built-in help and documentation accessible from the application.

---

## 🖼️ GUI Highlights

- Clean, modern **PyQt5 interface** with custom stylesheets.
- **Table view** for game listings with sorting and filtering.
- **Details panel** for screenshots, trailers, and metadata.
- **Dialog-based editing** for single or multiple games.
- **Background workers** for scraping and image fetching to keep UI responsive.

---

## 📦 Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/game-manager-gui.git
   cd game-manager-gui
