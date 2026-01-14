# scraping.py
"""
Scraper module for GameManager (optimized version)
- Fast Steam AppID discovery via storesearch API
- Candidate fuzzy matching (rapidfuzz if available, fallback to simple heuristic)
- Steam store metadata retrieval (store API) with microtrailers
- IGDB scraping (description, screenshots with 720p quality, player perspective, themes, etc.)
- Steam microtrailer scraping from Steam API JSON
- Designed to be called from GUI worker threads; interactive selection is handled by GUI
"""

from __future__ import annotations
import os
import re
import time
import json
import sys
import requests
from typing import List, Dict, Optional, Any, Callable
from urllib.parse import quote_plus
from bs4 import BeautifulSoup

# Optional: rapidfuzz for better fuzzy scoring
try:
    from rapidfuzz import fuzz  # type: ignore
    HAVE_RAPIDFUZZ = True
except ImportError:
    HAVE_RAPIDFUZZ = False

# -------------------------
# Configuration / constants
# -------------------------
STEAM_SEARCH_API = "https://store.steampowered.com/api/storesearch/?term={q}&cc=US&l=en"
STEAM_STORE_APP_URL = "https://store.steampowered.com/app/{appid}"
STEAMDB_APP_URL = "https://steamdb.info/app/{appid}"
PCGW_SEARCH_TEMPLATE = "https://www.pcgamingwiki.com/w/index.php?search={q}"
IGDB_URL_TEMPLATE = "https://www.igdb.com/games/{slug}?utm_source=SteamDB"

# Performance and reliability settings
HTTP_TIMEOUT = 8.0
HTTP_RETRIES = 2  # Increased for better reliability
SLEEP_BETWEEN_REQUESTS = 0.15  # Slightly reduced for better performance

# IGDB image settings - using 720p for better quality
IGDB_IMAGE_SIZE = "t_720p"
IGDB_IMAGE_BASE_URL = "https://images.igdb.com/igdb/image/upload"

# -------------------------
# IGDB auth (Twitch) config
# -------------------------
IGDB_CLIENT_ID = os.environ.get("IGDB_CLIENT_ID", "3y74unpwlpblo3nwnx44a9fpm7aug7")
IGDB_CLIENT_SECRET = os.environ.get("IGDB_CLIENT_SECRET", )
IGDB_ACCESS_TOKEN = os.environ.get("IGDB_ACCESS_TOKEN", "yqox9e79jt463xt44dyt85525gwkg2")

# Token caching for better performance
_IGDB_TOKEN_CACHE = {"access_token": None, "expires_at": 0}

# -------------------------
# Helper functions
# -------------------------
def _http_get(url: str, params: dict = None, timeout: float = HTTP_TIMEOUT) -> Optional[requests.Response]:
    """
    Perform HTTP GET with retries and timeout.
    Returns Response object on success, None on failure.
    """
    headers = {"User-Agent": "GameScraper/2.0 (compatible; GameManager)"}
    
    for attempt in range(HTTP_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout, headers=headers)
            if r.status_code == 200:
                return r
            elif r.status_code == 429:  # Rate limited
                time.sleep(1.0)  # Wait longer on rate limit
        except (requests.RequestException, requests.Timeout) as e:
            if attempt == HTTP_RETRIES:  # Last attempt failed
                print(f"HTTP GET failed for {url}: {e}")
        time.sleep(0.15 + attempt * 0.1)  # Exponential backoff
    return None


def _slugify(name: str) -> str:
    """Convert game title to URL-friendly slug."""
    if not name:
        return ""
    
    s = name.strip().lower()
    # Remove special apostrophes
    s = re.sub(r"[’'`]", "", s)
    # Replace non-alphanumeric with hyphens
    s = re.sub(r"[^a-z0-9]+", "-", s)
    # Remove multiple hyphens and trim
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s


def _score_name(target: str, candidate: str) -> int:
    """
    Score similarity between two strings (0-100).
    Uses rapidfuzz if available, otherwise simple heuristic.
    """
    if not target or not candidate:
        return 0
    
    if HAVE_RAPIDFUZZ:
        try:
            # Token set ratio is good for comparing game titles
            return int(fuzz.token_set_ratio(target, candidate))
        except Exception:
            pass
    
    # Fallback heuristic
    a = re.sub(r'\s+', ' ', target.lower()).strip()
    b = re.sub(r'\s+', ' ', candidate.lower()).strip()
    
    if not a or not b:
        return 0
    
    # Simple character overlap scoring
    common = sum(1 for ch in a if ch in b)
    return int(100 * common / max(1, max(len(a), len(b))))


def _normalize_image_url(url: str) -> str:
    """
    Normalize image URLs to ensure they start with // or https://
    Handles both IGDB and Steam image URLs.
    """
    if not url:
        return ""
    
    # If URL already starts with http, return as-is
    if url.startswith(('http://', 'https://', '//')):
        return url
    
    # For Steam URLs that might be relative
    if url.startswith('/'):
        return 'https:' + url
    
    # For IGDB image IDs, construct proper URL with 720p size
    if len(url) < 20 and not '.' in url:  # Likely an IGDB image ID
        return f"{IGDB_IMAGE_BASE_URL}/{IGDB_IMAGE_SIZE}/{url}.jpg"
    
    # Default: prepend https:// if missing
    return f"https://{url}" if not url.startswith('//') else f"https:{url}"


# -------------------------
# Steam search functionality
# -------------------------
def _steam_search_api(title: str) -> List[Dict]:
    """Search Steam using their API (fastest method)."""
    q = quote_plus(title)
    url = STEAM_SEARCH_API.format(q=q)
    r = _http_get(url)
    
    if not r:
        return []
    
    try:
        data = r.json()
    except json.JSONDecodeError:
        return []
    
    items = data.get("items") or []
    return [
        {
            "id": str(it.get("id", "")),
            "name": it.get("name", ""),
            "tiny_image": it.get("tiny_image"),
            "source": "steam_api"
        }
        for it in items
    ]


def _steam_search_html(title: str) -> List[Dict]:
    """Fallback Steam search using HTML parsing."""
    url = f"https://store.steampowered.com/search/?term={quote_plus(title)}"
    r = _http_get(url)
    
    if not r:
        return []
    
    soup = BeautifulSoup(r.text, "html.parser")
    candidates = []
    
    for a in soup.select("a.search_result_row"):
        href = a.get("href", "")
        match = re.search(r"/app/(\d+)", href)
        
        if match:
            name_elem = a.select_one(".title")
            name = name_elem.get_text(" ", strip=True) if name_elem else ""
            
            candidates.append({
                "id": match.group(1),
                "name": name,
                "tiny_image": None,
                "source": "steam_html"
            })
    
    return candidates


def find_candidates_for_title(title: str, max_candidates: int = 8) -> List[Dict]:
    """
    Find potential Steam game matches for a given title.
    Returns scored candidates sorted by relevance.
    """
    if not title or len(title.strip()) < 2:
        return []
    
    # Try API first (faster)
    candidates = _steam_search_api(title)
    time.sleep(SLEEP_BETWEEN_REQUESTS)
    
    # Fallback to HTML if API returns nothing
    if not candidates:
        candidates = _steam_search_html(title)
    
    # Score and sort candidates
    scored_candidates = []
    for candidate in candidates[:max_candidates]:
        candidate_name = candidate.get("name", "")
        score = _score_name(title, candidate_name)
        
        scored_candidates.append({
            "id": candidate.get("id"),
            "name": candidate_name,
            "score": score,
            "source": candidate.get("source", "unknown")
        })
    
    # Sort by score (highest first)
    scored_candidates.sort(key=lambda x: x["score"], reverse=True)
    return scored_candidates


def get_app_id_from_title(title: str, auto_accept_score: int = 92) -> Optional[str]:
    """
    Automatically get Steam AppID from title if match score is high enough.
    Returns None if no good match found.
    """
    if not title:
        return None
    
    candidates = _steam_search_api(title)
    time.sleep(SLEEP_BETWEEN_REQUESTS)
    
    if not candidates:
        candidates = _steam_search_html(title)
    
    if not candidates:
        return None
    
    # Find best candidate
    best_candidate = max(
        candidates,
        key=lambda it: _score_name(title, it.get("name", ""))
    )
    best_score = _score_name(title, best_candidate.get("name", ""))
    
    return str(best_candidate.get("id")) if best_score >= auto_accept_score else None


# -------------------------
# Trailer URL processing
# -------------------------
def adaptive_to_microtrailer(url: str, ext: str = "webm") -> str:
    """
    Convert adaptive streaming URL to microtrailer URL.
    Microtrailers are smaller, faster-loading video previews.
    """
    if not url:
        return ""
    
    # Handle query parameters
    if "?" in url:
        base, query = url.split("?", 1)
        query = "?" + query
    else:
        base, query = url, ""
    
    # Replace filename with microtrailer
    parts = base.split("/")
    if parts:
        parts[-1] = f"microtrailer.{ext}"
    
    return "/".join(parts) + query


# -------------------------
# Steam metadata retrieval
# -------------------------
def get_store_metadata(app_id: str, title: str, fetch_pcgw_save: bool = False) -> Dict[str, Any]:
    """
    Fetch comprehensive metadata from Steam store.
    Returns dictionary with game information including microtrailers.
    """
    if not app_id:
        return {
            "title": title or "",
            "release_date": "",
            "developer": "",
            "publisher": "",
            "genres": "",
            "description": "",
            "cover_url": "",
            "trailer_webm": "",
            "screenshots": [],
            "microtrailers": [],
            "steam_link": "",
            "steamdb_link": STEAMDB_APP_URL.format(appid=""),
            "pcgw_link": PCGW_SEARCH_TEMPLATE.format(q=quote_plus(title or "")),
            "igdb_link": IGDB_URL_TEMPLATE.format(slug=_slugify(title or "")),
            "save_location": "",
            "source": "steam"
        }
    
    url = f"https://store.steampowered.com/api/appdetails?appids={app_id}"
    r = _http_get(url)
    
    steamdb_link = STEAMDB_APP_URL.format(appid=app_id)
    microtrailers = []
    trailer_webm = ""
    
    if r:
        try:
            payload = r.json().get(str(app_id), {})
            
            if payload.get("success"):
                info = payload.get("data", {})
                
                # Process video content
                movies = info.get("movies") or []
                
                # Extract microtrailers from adaptive streaming URLs
                for movie in movies:
                    # Try adaptive streaming URLs first
                    dash_url = movie.get("dash_av1") or movie.get("dash_h264") or movie.get("hls_h264")
                    if dash_url:
                        micro_url = adaptive_to_microtrailer(dash_url, ext="webm")
                        if micro_url and micro_url not in microtrailers:
                            microtrailers.append(micro_url)
                
                # Fallback to direct video formats
                for movie in movies:
                    for fmt in ("webm", "mp4"):
                        video_data = movie.get(fmt)
                        if isinstance(video_data, dict):
                            for quality in ("max", "480", "360"):
                                video_url = video_data.get(quality)
                                if video_url and video_url not in microtrailers:
                                    microtrailers.append(video_url)
                
                trailer_webm = microtrailers[0] if microtrailers else ""
                
                # Process screenshots
                screenshots = [
                    _normalize_image_url(s.get("path_full", ""))
                    for s in info.get("screenshots", [])
                    if s.get("path_full")
                ][:10]  # Limit to 10 screenshots
                
                # Process release date
                release_info = info.get("release_date", {})
                if isinstance(release_info, dict):
                    release_date = release_info.get("date", "")
                else:
                    release_date = info.get("release_date", "")
                
                # Get game title (fallback to provided title)
                game_title = info.get("name", "") or title or ""
                
                return {
                    "title": game_title,
                    "release_date": release_date,
                    "developer": ", ".join(info.get("developers", []) or []),
                    "publisher": ", ".join(info.get("publishers", []) or []),
                    "genres": ", ".join([
                        g.get("description", "") 
                        for g in info.get("genres", []) 
                        if g.get("description")
                    ]),
                    "description": info.get("short_description", "") or "",
                    "cover_url": _normalize_image_url(info.get("header_image", "") or ""),
                    "trailer_webm": trailer_webm,
                    "screenshots": screenshots,
                    "microtrailers": microtrailers[:2],  # Keep top 2 for performance
                    "steam_link": STEAM_STORE_APP_URL.format(appid=app_id),
                    "steamdb_link": steamdb_link,
                    "pcgw_link": PCGW_SEARCH_TEMPLATE.format(q=quote_plus(game_title)),
                    "igdb_link": IGDB_URL_TEMPLATE.format(slug=_slugify(game_title)),
                    "save_location": "",
                    "source": "steam",
                    "steam_app_id": app_id
                }
                
        except (json.JSONDecodeError, KeyError, AttributeError) as e:
            print(f"Error parsing Steam metadata for app_id {app_id}: {e}")
    
    # Fallback response if API call fails
    return {
        "title": title or "",
        "release_date": "",
        "developer": "",
        "publisher": "",
        "genres": "",
        "description": "",
        "cover_url": "",
        "trailer_webm": "",
        "screenshots": [],
        "microtrailers": [],
        "steam_link": STEAM_STORE_APP_URL.format(appid=app_id),
        "steamdb_link": steamdb_link,
        "pcgw_link": PCGW_SEARCH_TEMPLATE.format(q=quote_plus(title or "")),
        "igdb_link": IGDB_URL_TEMPLATE.format(slug=_slugify(title or "")),
        "save_location": "",
        "source": "steam",
        "steam_app_id": app_id
    }


# -------------------------
# IGDB API integration
# -------------------------
def _fetch_igdb_token_via_twitch() -> str:
    """
    Fetch fresh IGDB access token from Twitch OAuth.
    Implements caching to avoid unnecessary requests.
    """
    now = int(time.time())
    
    # Return cached token if still valid (with 30-second buffer)
    if (_IGDB_TOKEN_CACHE.get("access_token") and 
        _IGDB_TOKEN_CACHE.get("expires_at", 0) - 30 > now):
        return _IGDB_TOKEN_CACHE["access_token"]
    
    # Need client ID and secret to get new token
    if not IGDB_CLIENT_ID or not IGDB_CLIENT_SECRET:
        print("Warning: IGDB_CLIENT_ID or IGDB_CLIENT_SECRET not set")
        return ""
    
    try:
        r = requests.post(
            "https://id.twitch.tv/oauth2/token",
            data={
                "client_id": IGDB_CLIENT_ID,
                "client_secret": IGDB_CLIENT_SECRET,
                "grant_type": "client_credentials"
            },
            timeout=10
        )
        
        if r.status_code == 200:
            data = r.json()
            token = data.get("access_token", "")
            expires_in = int(data.get("expires_in", 0))
            
            if token:
                _IGDB_TOKEN_CACHE["access_token"] = token
                _IGDB_TOKEN_CACHE["expires_at"] = now + max(30, expires_in)
                return token
    
    except requests.RequestException as e:
        print(f"Error fetching IGDB token: {e}")
    
    return ""


def _get_igdb_token() -> str:
    """Get IGDB API token, using environment variable or fetching new one."""
    # Use environment variable if available
    if IGDB_ACCESS_TOKEN and IGDB_ACCESS_TOKEN != "yqox9e79jt463xt44dyt85525gwkg2":
        return IGDB_ACCESS_TOKEN
    
    # Otherwise fetch via Twitch
    return _fetch_igdb_token_via_twitch()


def _igdb_query(endpoint: str, query: str, timeout: float = 10.0) -> Optional[List[Dict]]:
    """
    Execute query against IGDB API.
    Returns parsed JSON response or None on failure.
    """
    token = _get_igdb_token()
    if not token:
        print("Error: No IGDB access token available")
        return None
    
    headers = {
        "Client-ID": IGDB_CLIENT_ID,
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    
    try:
        r = requests.post(
            f"https://api.igdb.com/v4/{endpoint}",
            data=query.encode("utf-8"),
            headers=headers,
            timeout=timeout
        )
        
        if r.status_code == 200:
            return r.json()
        elif r.status_code == 401:
            print("IGDB token expired, attempting to refresh...")
            # Force token refresh on auth error
            _IGDB_TOKEN_CACHE["access_token"] = None
            return _igdb_query(endpoint, query, timeout)
    
    except requests.RequestException as e:
        print(f"IGDB API request failed: {e}")
    
    return None


def _format_igdb_image_url(image_id: str, size: str = IGDB_IMAGE_SIZE) -> str:
    """
    Convert IGDB image ID to full URL with specified size.
    Example: converts 'co1xyz' to 'https://images.igdb.com/.../t_720p/co1xyz.jpg'
    """
    if not image_id:
        return ""
    
    # Remove file extension if present
    image_id = image_id.split('.')[0]
    
    return f"{IGDB_IMAGE_BASE_URL}/{size}/{image_id}.jpg"


# -------------------------
# Enhanced IGDB scraper with user ratings and proper links - FIXED TO PROCESS METADATA WHEN IGDB ID IS GIVEN
# -------------------------

def find_candidates_for_title_igdb(title: str, max_candidates: int = 8) -> List[Dict]:
    """
    Find potential IGDB game matches for a given title.
    Returns scored candidates sorted by relevance.
    """
    if not title or len(title.strip()) < 2:
        return []
    
    # Search IGDB for the game with rating information
    query = f'''
        search "{title}";
        fields 
            id,
            name,
            summary,
            cover.image_id,
            first_release_date,
            platforms.name,
            genres.name,
            rating,
            rating_count,
            aggregated_rating;
        limit {max_candidates};
    '''
    
    results = _igdb_query("games", query) or []
    
    candidates = []
    for item in results:
        item_id = item.get("id")
        item_name = item.get("name", "")
        
        if not item_id or not item_name:
            continue
        
        # Score the candidate
        score = _score_name(title, item_name)
        
        # Get cover image if available
        cover_url = ""
        cover_image_id = item.get("cover", {}).get("image_id")
        if cover_image_id:
            cover_url = _format_igdb_image_url(cover_image_id)
        
        # Get release year if available
        release_year = ""
        first_release = item.get("first_release_date")
        if first_release:
            try:
                release_year = time.strftime("%Y", time.gmtime(first_release))
            except:
                pass
        
        # Get rating information
        user_rating = item.get("rating")
        critic_rating = item.get("aggregated_rating")
        
        # Create rating display
        rating_display = ""
        if user_rating is not None:
            rating_display = f"⭐ {user_rating:.1f}/100"
            rating_count = item.get("rating_count")
            if rating_count:
                rating_display += f" ({rating_count} ratings)"
        elif critic_rating is not None:
            rating_display = f"🎯 {critic_rating:.1f}/100"
        
        # Get genres
        genres = ", ".join([g.get("name", "") for g in item.get("genres", [])][:3])
        
        candidates.append({
            "id": str(item_id),  # IGDB ID
            "name": item_name,
            "score": score,
            "source": "igdb",
            "tiny_image": cover_url,
            "release_year": release_year,
            "user_rating": user_rating,
            "critic_rating": critic_rating,
            "rating_display": rating_display,
            "genres": genres,
            "description_preview": (item.get("summary", "") or "")[:120] + "..." if item.get("summary") else "",
            "is_igdb": True  # Flag to identify IGDB candidates
        })
    
    # Sort by score (highest first)
    candidates.sort(key=lambda x: x["score"], reverse=True)
    return candidates


def get_igdb_id_from_title(title: str, auto_accept_score: int = 92) -> Optional[str]:
    """
    Automatically get IGDB ID from title if match score is high enough.
    Returns None if no good match found.
    """
    if not title:
        return None
    
    candidates = find_candidates_for_title_igdb(title, max_candidates=5)
    
    if not candidates:
        return None
    
    # Find best candidate
    best_candidate = max(
        candidates,
        key=lambda it: it.get("score", 0)
    )
    best_score = best_candidate.get("score", 0)
    
    # Auto-accept at threshold (default 92%)
    return str(best_candidate.get("id")) if best_score >= auto_accept_score else None


def igdb_scraper(title: str, auto_accept_score: int = 92, igdb_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Enhanced IGDB scraper with user ratings and proper IGDB links.
    Now accepts optional igdb_id parameter for direct ID-based scraping.
    FIXED: When IGDB ID is provided, it processes metadata directly without title matching.
    """
    if not title or len(title.strip()) < 2:
        return {"__error__": "Invalid title provided"}
    
    # If IGDB ID is provided, use direct ID query WITHOUT title matching check
    if igdb_id:
        query = f'''
            fields 
                id,
                name,
                slug,
                genres.name,
                themes.name,
                summary,
                screenshots.image_id,
                cover.image_id,
                player_perspectives.name,
                videos.video_id,
                involved_companies.company.name,
                involved_companies.developer,
                involved_companies.publisher,
                rating,
                rating_count,
                aggregated_rating,
                aggregated_rating_count,
                first_release_date,
                websites.url,
                websites.category;
            where id = {igdb_id};
            limit 1;
        '''
        
        results = _igdb_query("games", query) or []
        
        if not results:
            print(f"No IGDB game found with ID {igdb_id}")
            # Fall back to title search if ID not found
            return igdb_scraper(title, auto_accept_score, igdb_id=None)
        
        exact_match = results[0]
        # When IGDB ID is provided, we trust it and return the data regardless of title match
        print(f"IGDB ID {igdb_id} found: '{exact_match.get('name')}'")
        print(f"Note: Provided title '{title}' may not match IGDB title")
    
    else:
        # Title-based search (original logic)
        query = f'''
            search "{title}";
            fields 
                id,
                name,
                slug,
                genres.name,
                themes.name,
                summary,
                screenshots.image_id,
                cover.image_id,
                player_perspectives.name,
                videos.video_id,
                involved_companies.company.name,
                involved_companies.developer,
                involved_companies.publisher,
                rating,
                rating_count,
                aggregated_rating,
                aggregated_rating_count,
                first_release_date,
                websites.url,
                websites.category;
            limit 5;
        '''
        
        results = _igdb_query("games", query) or []
        
        if not results:
            # No results found, return candidates for selection
            candidates = find_candidates_for_title_igdb(title)
            if candidates:
                return {
                    "__candidates__": candidates,
                    "__action__": "select_igdb_candidate",
                    "title": title,
                    "source": "igdb_candidates"
                }
            return {"__error__": "No results found on IGDB"}
        
        # Find best match (exact title match preferred)
        exact_match = None
        for item in results:
            if item.get("name", "").strip().lower() == title.strip().lower():
                exact_match = item
                break
        
        # If no exact match, check if any candidate meets auto-accept threshold
        if not exact_match and results:
            # Score all results
            scored_results = []
            for item in results:
                item_name = item.get("name", "")
                score = _score_name(title, item_name)
                scored_results.append((score, item))
            
            # Sort by score
            scored_results.sort(key=lambda x: x[0], reverse=True)
            
            # Check if best score meets auto-accept threshold
            best_score, best_item = scored_results[0]
            if best_score >= auto_accept_score:
                exact_match = best_item
            else:
                # Return candidates for selection
                candidates = find_candidates_for_title_igdb(title)
                return {
                    "__candidates__": candidates,
                    "__action__": "select_igdb_candidate",
                    "title": title,
                    "source": "igdb_candidates",
                    "best_score": best_score,
                    "auto_accept_threshold": auto_accept_score
                }
        
        if not exact_match:
            # No good match found, return candidates
            candidates = find_candidates_for_title_igdb(title)
            return {
                "__candidates__": candidates,
                "__action__": "select_igdb_candidate",
                "title": title,
                "source": "igdb_candidates"
            }
    
    # Extract developers and publishers
    developers = []
    publishers = []
    
    for company in exact_match.get("involved_companies", []):
        company_name = company.get("company", {}).get("name", "")
        if company.get("developer"):
            developers.append(company_name)
        if company.get("publisher"):
            publishers.append(company_name)
    
    # Process screenshots with high-quality URLs
    screenshots = []
    for screenshot in exact_match.get("screenshots", []):
        image_id = screenshot.get("image_id")
        if image_id:
            screenshot_url = _format_igdb_image_url(image_id)
            screenshots.append(_normalize_image_url(screenshot_url))
    
    # Get cover image URL
    cover_image_id = exact_match.get("cover", {}).get("image_id")
    cover_url = _format_igdb_image_url(cover_image_id) if cover_image_id else ""
    
    # Process trailers
    trailers = []
    for video in exact_match.get("videos", []):
        video_id = video.get("video_id")
        if video_id:
            trailers.append(f"https://www.youtube.com/watch?v={video_id}")
    
    # Get release date
    release_date = ""
    first_release = exact_match.get("first_release_date")
    if first_release:
        try:
            release_date = time.strftime("%Y-%m-%d", time.gmtime(first_release))
        except:
            release_date = str(first_release)
    
    # Get IGDB slug for proper link
    slug = exact_match.get("slug", _slugify(exact_match.get("name", title)))
    igdb_link = f"https://www.igdb.com/games/{slug}"
    
    # Try to find Steam AppID from IGDB websites
    steam_app_id = None
    steam_link = ""
    
    for site in exact_match.get("websites", []):
        if site.get("category") == 13:  # Steam website
            steam_url = site.get("url", "")
            match = re.search(r'/app/(\d+)', steam_url)
            if match:
                steam_app_id = match.group(1)
                steam_link = STEAM_STORE_APP_URL.format(appid=steam_app_id)
                break
    
    # Get rating information
    user_rating = exact_match.get("rating")
    critic_rating = exact_match.get("aggregated_rating")
    
    # Create rating display
    rating_display = ""
    if user_rating is not None:
        rating_display = f"⭐ {user_rating:.1f}/100"
        rating_count = exact_match.get("rating_count")
        if rating_count:
            rating_display += f" ({rating_count} ratings)"
    elif critic_rating is not None:
        rating_display = f"🎯 {critic_rating:.1f}/100"
        rating_count = exact_match.get("aggregated_rating_count")
        if rating_count:
            rating_display += f" ({rating_count} critic reviews)"
    
    return {
        "source": "igdb",
        "igdb_id": str(exact_match.get("id", "")),
        "title": exact_match.get("name", title),
        "description": exact_match.get("summary", ""),
        "genres": ", ".join([g.get("name", "") for g in exact_match.get("genres", [])]),
        "themes": ", ".join([t.get("name", "") for t in exact_match.get("themes", [])]),
        "player_perspective": ", ".join([
            p.get("name", "") 
            for p in exact_match.get("player_perspectives", [])
        ]),
        "developer": ", ".join(developers),
        "publisher": ", ".join(publishers),
        "cover_url": _normalize_image_url(cover_url),
        "screenshots": screenshots,
        "trailers": trailers,
        "igdb_link": igdb_link,  # Proper IGDB link from IGDB data
        "release_date": release_date,
        "steam_app_id": steam_app_id,
        "steam_link": steam_link,
        "steamdb_link": STEAMDB_APP_URL.format(appid=steam_app_id) if steam_app_id else "",
        "pcgw_link": PCGW_SEARCH_TEMPLATE.format(q=quote_plus(exact_match.get("name", title))),
        "user_rating": user_rating,
        "user_rating_count": exact_match.get("rating_count"),
        "critic_rating": critic_rating,
        "critic_rating_count": exact_match.get("aggregated_rating_count"),
        "rating_display": rating_display,
        "scraped_by_id": igdb_id is not None  # Flag to indicate if scraped by ID
    }


# -------------------------
# Metadata merging utilities
# -------------------------
# Allowed keys from IGDB to merge into final metadata
IGDB_ALLOWED_KEYS = {
    "description",  # Description is allowed from IGDB
    "player_perspective", 
    "igdb_id",
    "themes",
    "genres",
    "screenshots",
    "trailers",
    "cover_url",
    "igdb_link",
    "title",
    "developer",
    "publisher",
    "user_rating",
    "user_rating_count",
    "critic_rating",
    "critic_rating_count",
    "rating_display"
}


def _filter_igdb_allowed(metadata: Dict) -> Dict:
    """Filter IGDB metadata to only include allowed keys."""
    if not isinstance(metadata, dict):
        return {}
    
    return {
        key: metadata[key]
        for key in IGDB_ALLOWED_KEYS
        if key in metadata and metadata[key]
    }

# -------------------------
# Unified merge_metadata function (PREFERS STEAM OVER IGDB)
# -------------------------

def merge_metadata(primary: Dict, secondary: Dict) -> Dict:
    """
    Unified merge function that ALWAYS prefers Steam data over IGDB data.
    Fixed to ensure Steam items always come first in lists.
    """
    # Create a copy of the primary data
    merged = primary.copy()
    
    # Determine which source is Steam and which is IGDB
    def is_likely_steam_data(data: Dict) -> bool:
        """Check if data likely comes from Steam."""
        steam_indicators = [
            "steam_link" in data,
            "steam_app_id" in data,
            "price" in data,
            "steamdb_link" in data,
            "microtrailers" in data
        ]
        return any(steam_indicators)
    
    def is_likely_igdb_data(data: Dict) -> bool:
        """Check if data likely comes from IGDB."""
        igdb_indicators = [
            "igdb_link" in data,
            "igdb_id" in data,
            "critic_rating" in data,
            "themes" in data,
            "player_perspective" in data
        ]
        return any(igdb_indicators)
    
    # Identify which dict is which
    primary_is_steam = is_likely_steam_data(primary)
    secondary_is_steam = is_likely_steam_data(secondary)
    primary_is_igdb = is_likely_igdb_data(primary)
    secondary_is_igdb = is_likely_igdb_data(secondary)
    
    # Determine Steam and IGDB data sources
    steam_data = secondary if secondary_is_steam else (primary if primary_is_steam else {})
    igdb_data = secondary if secondary_is_igdb else (primary if primary_is_igdb else {})
    
    # Merge logic: Always prefer Steam data
    for key, steam_value in steam_data.items():
        if not steam_value:  # Skip empty values
            continue
            
        # SPECIAL CASE: Lists (screenshots, trailers) - Steam items FIRST
        if isinstance(steam_value, list) and isinstance(merged.get(key), list):
            # For screenshots and trailers, combine all items with Steam items FIRST
            merged_list = []
            seen_items = set()
            
            # 1. Add Steam items FIRST (always priority)
            for item in steam_value:
                if item and str(item) not in seen_items:
                    merged_list.append(item)
                    seen_items.add(str(item))
            
            # 2. Then add IGDB items (if available)
            igdb_items = igdb_data.get(key, [])
            if isinstance(igdb_items, list):
                for item in igdb_items:
                    if item and str(item) not in seen_items:
                        merged_list.append(item)
                        seen_items.add(str(item))
            
            merged[key] = merged_list
        
        # SPECIAL CASE: Cover URLs - always prefer Steam
        elif key == "cover_url":
            # Always prefer Steam cover URL if available
            if steam_value:
                merged[key] = steam_value
        
        # SPECIAL CASE: Descriptions - use longer description
        elif key == "description" and isinstance(steam_value, str):
            current_desc = merged.get(key, "")
            if len(steam_value) > len(current_desc):
                merged[key] = steam_value
        
        # SPECIAL CASE: IDs - keep both
        elif key in ["steam_app_id", "igdb_id"]:
            # Always use the non-empty value
            if steam_value:
                merged[key] = steam_value
        
        # DEFAULT: Steam data takes priority over IGDB
        elif key not in merged or not merged[key]:
            # If field doesn't exist or is empty, use Steam value
            merged[key] = steam_value
        elif secondary_is_steam and not primary_is_steam:
            # If secondary is Steam and primary is not, prefer Steam
            merged[key] = steam_value
    
    # Ensure we have both IDs if available in either source
    if "steam_app_id" in steam_data and steam_data["steam_app_id"]:
        merged["steam_app_id"] = steam_data["steam_app_id"]
    elif "steam_app_id" in igdb_data and igdb_data["steam_app_id"]:
        merged["steam_app_id"] = igdb_data["steam_app_id"]
    
    if "igdb_id" in igdb_data and igdb_data["igdb_id"]:
        merged["igdb_id"] = igdb_data["igdb_id"]
    elif "igdb_id" in steam_data and steam_data["igdb_id"]:
        merged["igdb_id"] = steam_data["igdb_id"]
    
    # Add debug info
    merged["_merge_debug"] = {
        "primary_is_steam": primary_is_steam,
        "secondary_is_steam": secondary_is_steam,
        "primary_is_igdb": primary_is_igdb,
        "secondary_is_igdb": secondary_is_igdb
    }
    
    return merged


# -------------------------
# Updated IGDB then Steam scraper (uses merge_metadata) - MODIFIED TO ACCEPT BOTH IDs
# -------------------------

def scrape_igdb_then_steam(
    igdb_id: Optional[str],
    title: str,
    primary_scraper_func: Callable = igdb_scraper,
    auto_accept_score: int = 92,
    fetch_pcgw_save: bool = False,
    steam_app_id: Optional[str] = None  # NEW: Accept Steam AppID parameter
) -> Dict[str, Any]:
    """
    Main scraping function that gets IGDB data first, then Steam data.
    Uses merge_metadata to combine with Steam data preferred.
    Now accepts both IGDB ID and Steam AppID for direct scraping.
    """
    primary_metadata = {}
    
    # Get primary (IGDB) metadata using existing igdb_scraper
    try:
        primary_metadata = primary_scraper_func(
            title,
            auto_accept_score=auto_accept_score,
            igdb_id=igdb_id  # Pass IGDB ID if available
        ) or {}
    except Exception as e:
        print(f"Error getting IGDB primary metadata: {e}")
        primary_metadata = {"__error__": f"IGDB error: {str(e)}"}
    
    # Handle candidates or errors
    if "__candidates__" in primary_metadata:
        return primary_metadata
    
    if "__error__" in primary_metadata:
        return primary_metadata
    
    # Get Steam metadata as secondary using existing get_store_metadata
    steam_metadata = {}
    
    # STRATEGY: Prefer provided Steam AppID, then IGDB's Steam AppID, then title search
    steam_app_id_to_use = None
    
    # 1. First try: Use provided Steam AppID if available
    if steam_app_id:
        steam_app_id_to_use = steam_app_id
        print(f"Using provided Steam AppID: {steam_app_id_to_use}")
    
    # 2. Second try: Get Steam AppID from IGDB metadata (if not already provided)
    elif not steam_app_id_to_use:
        steam_app_id_to_use = primary_metadata.get("steam_app_id")
        if steam_app_id_to_use:
            print(f"Using Steam AppID from IGDB data: {steam_app_id_to_use}")
    
    # 3. Third try: If still no Steam AppID, search by title
    if not steam_app_id_to_use:
        # Use IGDB title for better search accuracy
        search_title = primary_metadata.get("title", title)
        print(f"No Steam AppID found from provided or IGDB data, searching by title '{search_title}'...")
        steam_app_id_to_use = get_app_id_from_title(search_title, auto_accept_score)
        if steam_app_id_to_use:
            print(f"Found Steam AppID by title search: {steam_app_id_to_use}")
    
    # 4. If we have a Steam AppID (from any source), get Steam data
    if steam_app_id_to_use:
        print(f"Using Steam AppID: {steam_app_id_to_use} to fetch Steam data")
        try:
            steam_metadata = get_store_metadata(
                steam_app_id_to_use,
                primary_metadata.get("title", title),
                fetch_pcgw_save=fetch_pcgw_save
            ) or {}
        except Exception as e:
            print(f"Error getting Steam metadata with AppID {steam_app_id_to_use}: {e}")
            steam_metadata = {}
    else:
        print(f"No Steam AppID found for '{title}' from any source")
        steam_metadata = {}
    
    # Use merge_metadata to combine data (Steam will be preferred)
    # Note: For igdb_then_steam, primary is IGDB, secondary is Steam
    merged_metadata = merge_metadata(primary_metadata, steam_metadata)
    
    # Add source information
    merged_metadata["source"] = "igdb_then_steam"
    merged_metadata["primary_source"] = "igdb"
    merged_metadata["secondary_source"] = "steam" if steam_metadata else "none"
    merged_metadata["scraped_at"] = time.time()
    merged_metadata["auto_accept_score_used"] = auto_accept_score
    
    # Debug info about Steam AppID source
    if steam_app_id_to_use:
        if steam_app_id and steam_app_id == steam_app_id_to_use:
            merged_metadata["steam_app_id_source"] = "provided"
        elif primary_metadata.get("steam_app_id") and primary_metadata.get("steam_app_id") == steam_app_id_to_use:
            merged_metadata["steam_app_id_source"] = "igdb"
        else:
            merged_metadata["steam_app_id_source"] = "title_search"
    
    # Debug info about IGDB ID source
    if igdb_id:
        merged_metadata["igdb_id_source"] = "provided"
    elif primary_metadata.get("igdb_id"):
        merged_metadata["igdb_id_source"] = "title_search"
    
    return merged_metadata


# -------------------------
# Updated Steam then IGDB scraper (uses same merge_metadata) - MODIFIED TO ACCEPT BOTH IDs
# -------------------------

def scrape_primary_then_igdb(
    app_id: Optional[str],
    title: str,
    auto_accept_score: int = 92,
    fetch_pcgw_save: bool = False,
    igdb_id: Optional[str] = None  # NEW: Accept IGDB ID parameter
) -> Dict[str, Any]:
    """
    Legacy scraping function that gets Steam data first, then IGDB data.
    Uses the same merge_metadata function for consistency.
    Now accepts both Steam AppID and IGDB ID for direct scraping.
    """
    # Get primary (Steam) metadata
    steam_metadata = {}
    try:
        # Use provided Steam AppID if available, otherwise search by title
        steam_app_id_to_use = app_id
        if not steam_app_id_to_use:
            steam_app_id_to_use = get_app_id_from_title(title, auto_accept_score)
        
        if steam_app_id_to_use:
            steam_metadata = get_store_metadata(
                steam_app_id_to_use,
                title,
                fetch_pcgw_save=fetch_pcgw_save
            ) or {}
        else:
            # Return candidates for selection
            candidates = find_candidates_for_title(title)
            return {
                "__candidates__": candidates,
                "__action__": "select_steam_candidate",
                "title": title,
                "source": "steam_candidates"
            }
    except Exception as e:
        print(f"Error getting Steam primary metadata: {e}")
        steam_metadata = {"__error__": f"Steam error: {str(e)}"}
    
    # Handle candidates or errors
    if "__candidates__" in steam_metadata:
        return steam_metadata
    
    if "__error__" in steam_metadata:
        return steam_metadata
    
    # Get IGDB metadata as secondary
    igdb_metadata = {}
    try:
        # Use IGDB title from Steam data for better accuracy
        igdb_title = steam_metadata.get("title", title)
        igdb_metadata = igdb_scraper(
            igdb_title,
            auto_accept_score=auto_accept_score,
            igdb_id=igdb_id  # Pass IGDB ID if available
        ) or {}
        
    except Exception as e:
        print(f"Error getting IGDB metadata: {e}")
        igdb_metadata = {}
    
    # Use the same merge_metadata function for consistency
    # Note: For steam_then_igdb, primary is Steam, secondary is IGDB
    # But merge_metadata will still prefer Steam data
    merged_metadata = merge_metadata(steam_metadata, igdb_metadata)
    
    # Add source information
    merged_metadata["source"] = "steam_then_igdb"
    merged_metadata["primary_source"] = "steam"
    merged_metadata["secondary_source"] = "igdb" if igdb_metadata else "none"
    merged_metadata["scraped_at"] = time.time()
    merged_metadata["auto_accept_score_used"] = auto_accept_score
    
    # Debug info about ID sources
    if app_id:
        merged_metadata["steam_app_id_source"] = "provided"
    elif steam_metadata.get("steam_app_id"):
        merged_metadata["steam_app_id_source"] = "title_search"
    
    if igdb_id:
        merged_metadata["igdb_id_source"] = "provided"
    elif igdb_metadata.get("igdb_id"):
        merged_metadata["igdb_id_source"] = "title_search"
    
    return merged_metadata


# -------------------------
# Enhanced CLI Testing for all scraper types - UPDATED TO SUPPORT NEW PARAMETERS
# -------------------------
if __name__ == "__main__":
    import argparse
    import json
    
    parser = argparse.ArgumentParser(
        description="Test game scraping functionality - All Modes"
    )
    parser.add_argument(
        "title", 
        help="Game title to scrape (e.g., 'Cyberpunk 2077')"
    )
    parser.add_argument(
        "--app-id", 
        help="Steam AppID (skip lookup if provided)"
    )
    parser.add_argument(
        "--igdb-id", 
        help="IGDB ID (for IGDB modes)"
    )
    parser.add_argument(
        "--auto-accept-score", 
        type=int, 
        default=92,
        help="Minimum score for auto-accepting candidates (0-100, default: 92)"
    )
    parser.add_argument(
        "--mode",
        choices=["steam_first", "igdb_first", "steam_only", "igdb_only", "all"],
        default="all",
        help="Scraping mode: steam_first, igdb_first, steam_only, igdb_only, all=test all modes (default: all)"
    )
    parser.add_argument(
        "--brief",
        action="store_true",
        help="Show brief summary instead of full JSON"
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output full JSON for selected mode"
    )
    parser.add_argument(
        "--show-final",
        action="store_true",
        help="Show the final merged data fields for all modes"
    )
    
    args = parser.parse_args()
    
    def print_mode_header(mode_name: str):
        """Print a formatted header for each mode."""
        print(f"\n{'='*70}")
        print(f" {mode_name.upper()} MODE (Auto-Accept: {args.auto_accept_score}%) ")
        print(f"{'='*70}")
    
    def print_brief_summary(data: Dict, mode_name: str):
        """Print a brief summary of the metadata."""
        print(f"\n📊 {mode_name.upper()} SUMMARY")
        print("-" * 50)
        
        if '__candidates__' in data:
            cands = data['__candidates__']
            print(f"🎯 CANDIDATES FOUND: {len(cands)}")
            print(f"📈 Auto-accept threshold: {args.auto_accept_score}%")
            
            # Show top candidates with scores
            for i, c in enumerate(cands[:5], 1):
                score = c.get('score', 0)
                auto_accept = score >= args.auto_accept_score
                status = "✅ AUTO-ACCEPT" if auto_accept else "❌ BELOW THRESHOLD"
                print(f"\n  {i}. {c['name'][:50]}")
                print(f"     ID: {c['id']} | Score: {score}% | {status}")
                print(f"     Source: {c.get('source', 'unknown')}")
                
                if c.get('rating_display'):
                    print(f"     Rating: {c['rating_display']}")
                if c.get('release_year'):
                    print(f"     Release: {c['release_year']}")
                if c.get('genres'):
                    print(f"     Genres: {c['genres']}")
            
            if len(cands) > 5:
                print(f"\n  ... and {len(cands) - 5} more candidates")
            return
        
        if '__error__' in data:
            print(f"❌ ERROR: {data['__error__']}")
            return
        
        print(f"🎮 Title: {data.get('title', 'N/A')}")
        print(f"🏷️  Source: {data.get('source', 'N/A')}")
        
        # IDs and their sources
        if data.get('steam_app_id'):
            print(f"🔵 Steam AppID: {data['steam_app_id']}")
            if data.get('steam_app_id_source'):
                print(f"   Source: {data['steam_app_id_source']}")
        if data.get('igdb_id'):
            print(f"🟣 IGDB ID: {data['igdb_id']}")
            if data.get('igdb_id_source'):
                print(f"   Source: {data['igdb_id_source']}")
        
        # Companies
        dev = data.get('developer', 'N/A')
        pub = data.get('publisher', 'N/A')
        print(f"🏢 Developer: {dev[:60]}{'...' if len(dev) > 60 else ''}")
        print(f"🏢 Publisher: {pub[:60]}{'...' if len(pub) > 60 else ''}")
        
        # Classification
        genres = data.get('genres', 'N/A')
        print(f"🏷️  Genres: {genres[:60]}{'...' if len(genres) > 60 else ''}")
        if data.get('themes'):
            themes = data.get('themes', '')
            print(f"🎭 Themes: {themes[:60]}{'...' if len(themes) > 60 else ''}")
        
        # Ratings
        if data.get('user_rating') is not None:
            rating = data['user_rating']
            count = data.get('user_rating_count', 0)
            print(f"⭐ User Rating: {rating:.1f}/100 ({count} ratings)")
        elif data.get('critic_rating') is not None:
            print(f"🎯 Critic Rating: {data['critic_rating']:.1f}/100")
        if data.get('rating_display'):
            print(f"📊 Rating Display: {data['rating_display']}")
        
        # Release
        print(f"📅 Release Date: {data.get('release_date', 'N/A')}")
        
        # Media
        print(f"🖼️  Screenshots: {len(data.get('screenshots', []))}")
        print(f"🎬 Trailers: {len(data.get('trailers', []))}")
        print(f"🎥 Microtrailers: {len(data.get('microtrailers', []))}")
        
        # Links
        if data.get('steam_link'):
            steam_link = data['steam_link']
            print(f"🔗 Steam: {steam_link[:80]}...")
        if data.get('igdb_link'):
            print(f"🔗 IGDB: {data['igdb_link']}")
        
        # Cover URL source
        cover_url = data.get('cover_url', '')
        if cover_url:
            if 'steam' in cover_url.lower():
                print(f"📸 Cover: Steam URL")
            elif 'igdb' in cover_url.lower():
                print(f"📸 Cover: IGDB URL")
            else:
                print(f"📸 Cover: Other source")
        
        # Scraping method
        if data.get('scraped_by_id'):
            print(f"🔍 IGDB scraped by ID: Yes")
    
    def print_final_fields(data: Dict, mode_name: str):
        """Print the final merged data fields for a mode."""
        print(f"\n📋 FINAL MERGED DATA FIELDS - {mode_name.upper()}")
        print("-" * 70)
        
        if '__candidates__' in data or '__error__' in data:
            print("No merged data available (candidates or error state)")
            return
        
        # Define field categories
        categories = {
            "Identifiers": ["title", "steam_app_id", "igdb_id", "source", "steam_app_id_source", "igdb_id_source"],
            "Companies": ["developer", "publisher"],
            "Classification": ["genres", "themes", "player_perspective"],
            "Content": ["description", "release_date"],
            "Ratings": ["user_rating", "user_rating_count", "critic_rating", "critic_rating_count", "rating_display"],
            "Media": ["cover_url", "screenshots", "trailers", "microtrailers"],
            "Links": ["steam_link", "steamdb_link", "igdb_link", "pcgw_link"],
            "Debug": ["scraped_by_id", "primary_source", "secondary_source"]
        }
        
        for category, fields in categories.items():
            print(f"\n{category}:")
            for field in fields:
                value = data.get(field)
                if value is None:
                    value_str = "None"
                elif isinstance(value, list):
                    value_str = f"List[{len(value)} items]"
                elif isinstance(value, str) and len(value) > 80:
                    value_str = f"{value[:80]}..."
                else:
                    value_str = str(value)
                
                # Indicate source if possible
                source_indicator = ""
                if field in ["user_rating", "user_rating_count", "critic_rating", "critic_rating_count", "themes", "player_perspective"]:
                    source_indicator = " (IGDB)"
                elif field in ["microtrailers"]:
                    source_indicator = " (Steam)"
                
                print(f"  {field:25} = {value_str}{source_indicator}")
    
    def test_steam_first():
        """Test Steam first, then IGDB mode."""
        print_mode_header("Steam First (Legacy)")
        result = scrape_primary_then_igdb(
            app_id=args.app_id,
            title=args.title,
            auto_accept_score=args.auto_accept_score,
            igdb_id=args.igdb_id  # Pass IGDB ID if available
        )
        if args.json:
            print(json.dumps(result, indent=2, ensure_ascii=False))
        elif args.show_final:
            print_final_fields(result, "steam_first")
        else:
            print_brief_summary(result, "steam_first")
        return result
    
    def test_igdb_first():
        """Test IGDB first, then Steam mode."""
        print_mode_header("IGDB First (New)")
        result = scrape_igdb_then_steam(
            igdb_id=args.igdb_id,
            title=args.title,
            auto_accept_score=args.auto_accept_score,
            steam_app_id=args.app_id  # Pass Steam AppID if available
        )
        if args.json:
            print(json.dumps(result, indent=2, ensure_ascii=False))
        elif args.show_final:
            print_final_fields(result, "igdb_first")
        else:
            print_brief_summary(result, "igdb_first")
        return result
    
    def test_steam_only():
        """Test Steam only mode."""
        print_mode_header("Steam Only")
        if args.app_id:
            result = get_store_metadata(args.app_id, args.title)
        else:
            # Try to find Steam AppID
            steam_id = get_app_id_from_title(args.title, args.auto_accept_score)
            if steam_id:
                result = get_store_metadata(steam_id, args.title)
            else:
                # Return candidates
                candidates = find_candidates_for_title(args.title)
                result = {
                    "__candidates__": candidates,
                    "__action__": "select_steam_candidate",
                    "title": args.title,
                    "source": "steam_candidates"
                }
        if args.json:
            print(json.dumps(result, indent=2, ensure_ascii=False))
        elif args.show_final:
            print_final_fields(result, "steam_only")
        else:
            print_brief_summary(result, "steam_only")
        return result
    
    def test_igdb_only():
        """Test IGDB only mode."""
        print_mode_header("IGDB Only")
        result = igdb_scraper(
            args.title, 
            args.auto_accept_score, 
            igdb_id=args.igdb_id  # Pass IGDB ID if available
        )
        if args.json:
            print(json.dumps(result, indent=2, ensure_ascii=False))
        elif args.show_final:
            print_final_fields(result, "igdb_only")
        else:
            print_brief_summary(result, "igdb_only")
        return result
    
    # Main test execution
    print(f"\n{'#'*80}")
    print(f" GAME SCRAPER TEST SUITE ")
    print(f"{'#'*80}")
    print(f"Title: {args.title}")
    print(f"Steam AppID: {args.app_id or 'Auto-detect'}")
    print(f"IGDB ID: {args.igdb_id or 'Auto-detect'}")
    print(f"Auto-accept threshold: {args.auto_accept_score}%")
    print(f"Mode: {args.mode}")
    print(f"{'#'*80}")
    
    if args.mode == "all":
        # Test all modes
        results = {}
        
        # Test Steam candidates
        print("\n[1] Testing Steam candidates...")
        steam_candidates = find_candidates_for_title(args.title)
        if steam_candidates:
            print(f"Found {len(steam_candidates)} Steam candidates")
            best_steam = max(steam_candidates, key=lambda x: x.get('score', 0))
            print(f"Best Steam candidate: {best_steam['name']} (Score: {best_steam['score']}%, ID: {best_steam['id']})")
        else:
            print("No Steam candidates found")
        
        # Test IGDB candidates
        print("\n[2] Testing IGDB candidates...")
        igdb_candidates = find_candidates_for_title_igdb(args.title)
        if igdb_candidates:
            print(f"Found {len(igdb_candidates)} IGDB candidates")
            best_igdb = max(igdb_candidates, key=lambda x: x.get('score', 0))
            print(f"Best IGDB candidate: {best_igdb['name']} (Score: {best_igdb['score']}%, ID: {best_igdb['id']})")
            if best_igdb.get('rating_display'):
                print(f"  Rating: {best_igdb['rating_display']}")
        else:
            print("No IGDB candidates found")
        
        # Test all modes
        print("\n[3] Testing all scraping modes...")
        results["steam_first"] = test_steam_first()
        results["igdb_first"] = test_igdb_first()
        results["steam_only"] = test_steam_only()
        results["igdb_only"] = test_igdb_only()
        
        # Comparison summary
        print(f"\n{'='*70}")
        print(" COMPARISON SUMMARY ")
        print(f"{'='*70}")
        
        comparison_data = []
        for mode, result in results.items():
            if '__candidates__' in result:
                status = f"Candidates: {len(result['__candidates__'])}"
                has_rating = False
                rating = "N/A"
            elif '__error__' in result:
                status = f"Error: {result['__error__'][:30]}..."
                has_rating = False
                rating = "N/A"
            else:
                status = "✅ Success"
                has_rating = result.get('user_rating') is not None
                rating = f"{result.get('user_rating', 'N/A')}"
                if has_rating and isinstance(rating, (int, float)):
                    rating = f"{rating:.1f}/100"
            
            # Safely handle None values
            igdb_id = result.get('igdb_id', 'N/A')
            if igdb_id is None:
                igdb_id_str = 'N/A'
            else:
                igdb_id_str = str(igdb_id)
            
            steam_id = result.get('steam_app_id', 'N/A')
            if steam_id is None:
                steam_id_str = 'N/A'
            else:
                steam_id_str = str(steam_id)
            
            comparison_data.append({
                "mode": mode,
                "status": status,
                "title": result.get('title', 'N/A')[:30],
                "rating": str(rating) if rating is not None else 'N/A',
                "screenshots": str(len(result.get('screenshots', []))),
                "steam_id": steam_id_str,
                "igdb_id": igdb_id_str,
                "cover_source": "Steam" if 'steam' in str(result.get('cover_url', '')).lower() else "IGDB"
            })
        
        # Print comparison table with safe string formatting
        print(f"\n{'Mode':<15} {'Status':<25} {'Title':<30} {'Rating':<15} {'Screenshots':<12} {'Steam ID':<12} {'IGDB ID':<12} {'Cover'}")
        print("-" * 130)
        for data in comparison_data:
            print(f"{data['mode']:<15} {data['status']:<25} {data['title']:<30} {data['rating']:<15} {data['screenshots']:<12} {data['steam_id']:<12} {data['igdb_id']:<12} {data['cover_source']}")
        
        print(f"\n{'='*70}")
        print(" KEY DIFFERENCES ")
        print(f"{'='*70}")
        print("• steam_first: Legacy mode, Steam priority, accepts both IDs")
        print("• igdb_first: New mode, IGDB priority, accepts both IDs")
        print("• steam_only: Pure Steam data, no ratings, Steam screenshots only")
        print("• igdb_only: Pure IGDB data, includes ratings, accepts IGDB ID")
        print(f"\n• All modes use {args.auto_accept_score}% auto-accept threshold")
        print("• IGDB modes provide proper IGDB links from IGDB data")
        print("• IGDB modes include user and critic ratings when available")
        print("• merge_metadata always prefers Steam data over IGDB data")
        print("• ID-based scraping is preferred when IDs are provided")
        print("• IGDB ID scraping now works even with mismatched titles")
        
        # Show final merged data if requested
        if args.show_final:
            print(f"\n{'='*70}")
            print(" FINAL MERGED DATA FOR ALL MODES ")
            print(f"{'='*70}")
            for mode, result in results.items():
                print_final_fields(result, mode)
        
    else:
        # Test specific mode
        result = None
        if args.mode == "steam_first":
            result = test_steam_first()
        elif args.mode == "igdb_first":
            result = test_igdb_first()
        elif args.mode == "steam_only":
            result = test_steam_only()
        elif args.mode == "igdb_only":
            result = test_igdb_only()
        
        # If JSON output requested for single mode
        if args.json and result:
            print(json.dumps(result, indent=2, ensure_ascii=False))
    
    print(f"\n{'#'*80}")
    print(" TEST COMPLETE ")
    print(f"{'#'*80}")