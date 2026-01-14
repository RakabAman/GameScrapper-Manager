# utils_sanitize.py
"""
Sanitizer utility for messy release titles.

Outputs:
- base_title
- version
- repack
- modes
- cleaned_title
- tokens
- notes

Keeps a small repack list file (repack_list.txt) in the same folder.
"""

import re
from pathlib import Path
from typing import List, Dict, Optional

# Basic regexes
_YEAR_RE = re.compile(r'\b(19|20)\d{2}\b')
# improved version/build regexes
_VERSION_RE = re.compile(r'\b(?:v|version|ver)\s*[:\-]?\s*([0-9]+(?:[._\-][0-9A-Za-z]+)*)\b', re.I)
_SIMPLE_VERSION_RE = re.compile(r'\b(v[0-9]+(?:[._][0-9]+){0,})\b', re.I)
_HOTFIX_RE = re.compile(r'\bhotfix\s*[:\-]?\s*([0-9]+)\b', re.I)
_BUILD_RE = re.compile(r'\bbuild\s*[:\-]?\s*([0-9]+(?:[._\-][0-9]+)*)\b', re.I)
_BUILD_SHORT_RE = re.compile(r'\b(?:b|bld)\s*[:\-]?\s*([0-9]{3,})\b', re.I)
# New: Update detection
_UPDATE_RE = re.compile(r'\bupdate\s*[:\-]?\s*([0-9]+(?:[._\-][0-9]+)*)\b', re.I)

# bracket regex: capture content inside [] or ()
_BRACKET_RE = re.compile(r'[\[\(](.*?)[\]\)]')

# separators: dot, underscore, ASCII hyphen, various dashes, slash, pipe
_SEPARATORS = re.compile(r'[._\-\u2013\u2014–—/|]+')

# Emulator tokens to strip (added)
_EMULATOR_TOKENS = [
    r'\brpcs3\b', r'\bryujinx\b', r'\byuzu\b',
    r'\bcemu\b', r'\bdolphin\b', r'\bpcsx2\b',  # additional common emulators
    r'\bswitch\b', r'\bps3\b', r'\bwiiu\b', r'\bps4\b',  # console indicators
    r'\bemulator\b', r'\bemu\b'
]
_EMULATOR_RE = re.compile('|'.join(_EMULATOR_TOKENS), re.I)

# Edition tokens to strip from base title
_EDITION_TOKENS = [
    r'\bdeluxe\b', r'\bedition\b', r'\bultimate\b', r'\bbundle\b', r'\bpack\b',
    r'\bpremium\b', r'\bremaster(?:ed)?\b', r'\bremake\b', r'\bcomplete\b',
    r'\bgoty\b', r'\bdirector\'s cut\b', r'\banniversary\b', r'\bsuper digital\b',
    r'\bevolved\b', r'\bclassified archives\b', r'\bbonus ost\b', r'\bbonus\b'
]
_EDITION_RE = re.compile('|'.join(_EDITION_TOKENS), re.I)

_MODE_KEYWORDS = {
    "Multiplayer": ["multiplayer", "multi-player", "mp", "online"],
    "CO-OP": ["coop", "co-op", "co op", "cooperative"],
    "Singleplayer": ["singleplayer", "single-player", "sp"]
}

# Default repack list file (one name per line)
DEFAULT_REPACK_FILE = "repack_list.txt"

# Enhanced fallback repacks with more scene/repack groups (added RLD/GOG already present)
FALLBACK_REPACKS = [
    "FitGirl Repack", "DODI Repacks", "GOG", "CODEX", "RELOADED", "SKIDROW",
    "CPY", "PLAZA", "Razor1911", "FLT", "SiMPLEX", "PROPHET", "HOODLUM",
    "KaOs Krew", "TinyRepacks", "M4ckD0ge", "qoob", "JIT",
    "GoldBerg", "EMPRESS", "INSANE", "DOGE", "ANOMALY"
]


def load_repack_list(path: Optional[str] = None) -> List[str]:
    p = Path(path or DEFAULT_REPACK_FILE)
    if p.is_file():
        lines = [ln.strip() for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]
        return lines or FALLBACK_REPACKS
    return FALLBACK_REPACKS


def _find_repack(tokens: List[str], repack_list: List[str]) -> Optional[str]:
    repack_lower = [r.lower() for r in repack_list]
    # First pass: exact matches (case-insensitive)
    for t in tokens:
        tl = t.lower().strip()
        if tl in repack_lower:
            return repack_list[repack_lower.index(tl)]
    
    # Second pass: substring matches (more flexible)
    for t in tokens:
        tl = t.lower().strip()
        for i, r in enumerate(repack_lower):
            if r and r in tl:
                return repack_list[i]
        # Also check if token is a substring of any repack
        for i, r in enumerate(repack_lower):
            if tl and tl in r:
                return repack_list[i]
    
    # Third pass: uppercase tags like "GOG" or "CODEX" 
    for t in tokens:
        if t.upper() in repack_list:
            return t.upper()
    
    # Fourth pass: check the raw token against repack list (case-insensitive)
    for t in tokens:
        for repack in repack_list:
            if repack.lower() in t.lower():
                return repack
            if t.lower() in repack.lower():
                return repack
    
    return None


def _extract_bracket_tokens(s: str) -> List[str]:
    return [m.strip() for m in _BRACKET_RE.findall(s) if m.strip()]


def _extract_version(s: str) -> Optional[str]:
    """
    Robust version/build/extractor:
    - Prefer explicit v/version patterns
    - Then Build patterns
    - Then hotfix
    - Then update patterns
    - Then long numeric sequences (fallback)
    Returns normalized token (e.g., 'v1.2.3', 'Build 2151336', 'Hotfix 2', 'Update 4', 'v20250831_2044-321866')
    """
    if not s:
        return None

    # 1) explicit v/version
    m = _VERSION_RE.search(s)
    if m:
        num = m.group(1).replace(' ', '')
        # always prefix with 'v' for consistency
        return 'v' + num

    # 2) Build patterns
    m2 = _BUILD_RE.search(s)
    if m2:
        return "Build " + m2.group(1).replace(' ', '')
    m2b = _BUILD_SHORT_RE.search(s)
    if m2b:
        return "Build " + m2b.group(1)

    # 3) Hotfix
    m3 = _HOTFIX_RE.search(s)
    if m3:
        return f"Hotfix {m3.group(1)}"

    # 4) Update patterns (new)
    m4 = _UPDATE_RE.search(s)
    if m4:
        update_num = m4.group(1).replace(' ', '')
        return f"Update {update_num}"

    # 5) fallback: long numeric sequences (dates/build ids)
    m5 = re.search(r'\bv?([0-9]{6,}[0-9_\-0-9]*)\b', s)
    if m5:
        return 'v' + m5.group(1)

    return None


def _extract_modes(s: str) -> List[str]:
    found = set()
    low = s.lower()
    for mode, keys in _MODE_KEYWORDS.items():
        for k in keys:
            if k in low:
                found.add(mode)
    if not found:
        return ["Singleplayer"]
    if "Singleplayer" not in found:
        found.add("Singleplayer")
    return sorted(found)


def _clean_text_for_title(s: str) -> str:
    """
    Remove extraneous punctuation, normalize separators to spaces,
    collapse multiple spaces, and title-case the result.
    Keeps ampersand and apostrophes.
    """
    s2 = _SEPARATORS.sub(' ', s)
    s2 = re.sub(r'[\"\"\(\)\[\]\{\}:;,+=<>@#\$%\^&\*~`]', ' ', s2)
    s2 = re.sub(r'\s+', ' ', s2).strip()
    def smart_title(tok: str) -> str:
        if tok.upper() in ("PC", "GOG", "PS4", "PS5", "PS3", "NS", "SNES", "XBOX", "XBOX360", "XBOXONE"):
            return tok.upper()
        if tok == "`n":
            return "`N"
        return tok.capitalize()
    parts = s2.split(' ')
    parts = [smart_title(p) for p in parts if p]
    return ' '.join(parts)


def _strip_editions_and_modes(s: str) -> str:
    s2 = re.sub(r'\+\s*Multiplayer', ' ', s, flags=re.I)
    s2 = re.sub(r'\+\s*CO-OP', ' ', s2, flags=re.I)
    s2 = _EDITION_RE.sub(' ', s2)
    s2 = _EMULATOR_RE.sub(' ', s2)  # Added emulator removal
    s2 = re.sub(r'\b(multiplayer|multi-player|mp|online|coop|co-op|co op|cooperative)\b', ' ', s2, flags=re.I)
    # Remove emulator/console indicators that might not be caught by regex
    s2 = re.sub(r'\b(RPCS3|Ryujinx|Yuzu|Cemu|Dolphin|PCSX2)\b', ' ', s2, flags=re.I)
    return s2


def sanitize_original_title(raw: str, repack_file: Optional[str] = None) -> Dict:
    """
    Parse a messy original title string into structured parts.

    Returns dict:
      {
        "base_title": str,
        "version": str or "",
        "repack": str or "",
        "modes": [ ... ],
        "cleaned_title": str,
        "tokens": [ ... ],
        "notes": str
      }
    """
    repack_list = load_repack_list(repack_file)
    if not raw:
        return {
            "base_title": "",
            "version": "",
            "repack": "",
            "modes": ["Singleplayer"],
            "cleaned_title": "",
            "tokens": [],
            "notes": ""
        }

    s = raw.strip()

    # 1) Extract bracketed tokens first
    bracket_tokens = _extract_bracket_tokens(s)

    # 2) Extract version from whole string and bracket tokens
    version = _extract_version(s)
    if not version:
        for t in bracket_tokens:
            v = _extract_version(t)
            if v:
                version = v
                break

    # 3) Extract repack/scene from bracket tokens or trailing tokens
    tokens = bracket_tokens[:]
    # also consider trailing tokens after separators (dash/colon)
    trailing = re.split(r'[-:]', re.sub(r'[\[\]\(\)]', '', s))
    trailing = [t.strip() for t in trailing if t.strip()]
    if len(trailing) > 1:
        tokens.extend(trailing[1:])  # skip first part (likely title)
    
    # Also check the raw string for repacks not in brackets (like "-GOG" or "-RLD")
    raw_lower = s.lower()
    for repack in repack_list:
        if repack.lower() in raw_lower:
            tokens.append(repack)
            break
    
    repack = _find_repack(tokens, repack_list)

    # 4) Extract modes
    modes = _extract_modes(s)

    # 5) Build base title by removing bracketed parts and version and repack tokens
    s_no_brackets = _BRACKET_RE.sub('', s)

    # Remove version tokens (robustly) - including updates
    if version:
        # escape and allow variants of separators when removing
        ver_esc = re.escape(version)
        # replace hyphen/underscore/space variants of the escaped token
        ver_pattern = ver_esc.replace(r'\-', r'[-_\s]').replace(r'\_', r'[_\-\s]')
        try:
            s_no_brackets = re.sub(r'(?i)' + ver_pattern, '', s_no_brackets)
        except re.error:
            # fallback to simple removal
            s_no_brackets = re.sub(re.escape(version), '', s_no_brackets, flags=re.I)

    # Remove repack token if present
    if repack:
        # Try multiple removal strategies
        repack_esc = re.escape(repack)
        try:
            # Pattern with optional spaces/dashes around the repack
            repack_pattern = r'[\s\-_\[]*' + repack_esc + r'[\s\-_\]]*'
            s_no_brackets = re.sub(repack_pattern, ' ', s_no_brackets, flags=re.I)
        except re.error:
            # fallback to simple removal
            s_no_brackets = re.sub(re.escape(repack), '', s_no_brackets, flags=re.I)
        # Also remove any trailing dash/colon that might be left
        s_no_brackets = re.sub(r'[\-\:]+\s*$', '', s_no_brackets)

    # Strip edition/mode tokens and trailing modifiers
    s_no_brackets = _strip_editions_and_modes(s_no_brackets)
    s_no_brackets = re.sub(r'\s*\+\s*.*$', '', s_no_brackets)
    
    # Remove emulator tokens explicitly (additional pass for safety)
    s_no_brackets = _EMULATOR_RE.sub(' ', s_no_brackets)
    s_no_brackets = re.sub(r'\b(RPCS3|Ryujinx|Yuzu|Switch|PS3|PS4|WiiU)\b', ' ', s_no_brackets, flags=re.I)
    
    # Clean separators and punctuation
    base_candidate = _clean_text_for_title(s_no_brackets)
    # Remove stray version/build/update tokens left
    base_candidate = re.sub(r'\b(v[0-9][\d._\-]*)\b', '', base_candidate, flags=re.I).strip()
    base_candidate = re.sub(r'\b(build\s*[0-9_ \-]+)\b', '', base_candidate, flags=re.I).strip()
    base_candidate = re.sub(r'\b(update\s*[0-9._\-]+)\b', '', base_candidate, flags=re.I).strip()
    # Final collapse
    base_candidate = re.sub(r'\s+', ' ', base_candidate).strip()

    # 6) cleaned_title: remove punctuation and normalize casing
    cleaned_title = _clean_text_for_title(raw)

    # 7) notes: leftover tokens not used
    used = set()
    if version:
        used.add(version.lower())
    if repack:
        used.add(repack.lower())
    leftover = []
    for t in tokens:
        tl = t.lower()
        if any(u in tl for u in used):
            continue
        # skip tokens that are just years
        if _YEAR_RE.search(t):
            continue
        # skip emulator tokens
        if _EMULATOR_RE.search(t):
            continue
        leftover.append(t)
    notes = "; ".join(leftover).strip()

    return {
        "base_title": base_candidate,
        "version": version or "",
        "repack": repack or "",
        "modes": modes,
        "cleaned_title": cleaned_title,
        "tokens": tokens,
        "notes": notes
    }


# Quick test harness
if __name__ == "__main__":
    examples = [
        "100 in 1 Game Collection [FitGirl Repack]",
        "Age of Wonders 4 Premium Edition v1.011.001.110650  [FitGirl Repack]",
        "Alien Rogue Incursion Evolved Edition - Deluxe [FitGirl Repack]",
        "Ambrosia Sky Act One + Bonus OST [FitGirl Repack]",
        "Anima Gate of Memories - I and II Remaster [FitGirl Repack]",
        "Atelier Ryza Secret Trilogy Deluxe Pack [FitGirl Repack]",
        "Atelier Yumia The Alchemist of Memories & the Envisioned Land - Deluxe Edition v1.42 [FitGirl Repack]",
        "Baby Steps Hotfix 2 (26.09.2025) [FitGirl Repack]",
        "Bad Cheese v1.00.035 [FitGirl Repack]",
        "Battleborn Build 2151336 + Reborn Project Mod [FitGirl Repack]",
        "Big Dig Energy [FitGirl Repack]",
        "Bleak.Faith.Forsaken-GOG",
        "Bleak.Faith.Forsaken-RLD",  # Test RLD detection
        "Bleak.Faith.Forsaken-CODEX",  # Test CODEX detection
        "Brew [FitGirl Repack]",
        "Bygone Dreams v1.0.0.4 [FitGirl Repack]",
        "Chip `n Clawz vs. The Brainioids v1.0.22358 [FitGirl Repack]",
        "Commandos Origins - Deluxe Edition & Classified Archives v1.5.0.88858 [FitGirl Repack]",
        "Cronos The New Dawn - Deluxe Edition v20250831_2044-321866  [FitGirl Repack]",
        "Cult of the Lamb The One Who Waits Bundle v1.4.3.588 [FitGirl Repack]",
        "Daemon X Machina Titanic Scion - Super Digital Deluxe Edition v1.2.0 [FitGirl Repack]",
        "Dead Island 2 Ultimate Edition v7.0.0 +  Multiplayer [FitGirl Repack]",
        # Test cases with emulators
        "The Legend of Zelda Breath of the Wild [RPCS3]",
        "Super Mario Odyssey [Yuzu]",
        "Persona 5 Royal [Ryujinx Repack]",
        "Bloodborne [RPCS3 Emulator] v1.09",
        "God of War Ragnarok [PS4 Emulator] [FitGirl Repack]",
        # Test cases with updates
        "Indiana Jones And The Great Circle Update 4",
        "Starfield Update 1.9.51",
        "Cyberpunk 2077 Update 2.1 + Phantom Liberty",
        "The Witcher 3 Update 4.04 [GOG]",
        "Baldur's Gate 3 Update 17 [FitGirl Repack]",
        "Hogwarts Legacy Update 5 Build 1145830"
    ]
    repacks = load_repack_list()
    print("Loaded repacks (sample):", repacks[:12], "...")
    import json
    results = []
    for ex in examples:
        out = sanitize_original_title(ex)
        results.append({"input": ex, "parsed": out})
    print(json.dumps(results, indent=2, ensure_ascii=False))