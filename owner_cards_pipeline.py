#!/usr/bin/env python3
"""
Owner Card Pipeline (Local) — Excel-Safe + Excel-Length-Safe (Final Hardened v27.2)

- OCR scanned PDF owner cards
- Extract owner/name/address + item lines
- Best-effort strike-through detection
- Classify: Property vs Memorial vs Funeral Preneed vs At-Need Funeral
- Compute Prime / Mailing lists
- Export Excel workbook with multiple sheets

Usage:
  python3 owner_cards_pipeline.py --pdf "A (all).pdf" --out "OwnerCards_A_Output.xlsx"

V27.2 Fixes:
- Fixed NameError: Defined 'possible_dups_safe' before saving to Excel.
"""

import argparse
import hashlib
import json
import os
import re
import sys
import unicodedata
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Any

import pandas as pd
from tqdm import tqdm

from pdf2image import convert_from_path
import pytesseract
from pytesseract import Output

import cv2
import numpy as np
from PIL import Image

# -----------------------------
# CONFIG
# -----------------------------

STATE_MAP = {
    "ALABAMA": "AL", "ALASKA": "AK", "ARIZONA": "AZ", "ARKANSAS": "AR", "CALIFORNIA": "CA",
    "COLORADO": "CO", "CONNECTICUT": "CT", "DELAWARE": "DE", "FLORIDA": "FL", "GEORGIA": "GA",
    "HAWAII": "HI", "IDAHO": "ID", "ILLINOIS": "IL", "INDIANA": "IN", "IOWA": "IA",
    "KANSAS": "KS", "KENTUCKY": "KY", "LOUISIANA": "LA", "MAINE": "ME", "MARYLAND": "MD",
    "MASSACHUSETTS": "MA", "MICHIGAN": "MI", "MINNESOTA": "MN", "MISSISSIPPI": "MS",
    "MISSOURI": "MO", "MONTANA": "MT", "NEBRASKA": "NE", "NEVADA": "NV", "NEW HAMPSHIRE": "NH",
    "NEW JERSEY": "NJ", "NEW MEXICO": "NM", "NEW YORK": "NY", "NORTH CAROLINA": "NC",
    "NORTH DAKOTA": "ND", "OHIO": "OH", "OKLAHOMA": "OK", "OREGON": "OR", "PENNSYLVANIA": "PA",
    "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC", "SOUTH DAKOTA": "SD", "TENNESSEE": "TN",
    "TEXAS": "TX", "UTAH": "UT", "VERMONT": "VT", "VIRGINIA": "VA", "WASHINGTON": "WA",
    "WEST VIRGINIA": "WV", "WISCONSIN": "WI", "WYOMING": "WY", "DISTRICT OF COLUMBIA": "DC",
    "TENN": "TN", "TENNESSES": "TN", "TEN": "TN", "TENN.": "TN", "TN.": "TN",
    "TIN": "TN", "IN": "TN" 
}

CITY_BLOCKLIST = [
    "NASHVILLE", "BRENTWOOD", "FRANKLIN", "MADISON", "ANTIOCH", 
    "HERMITAGE", "OLD HICKORY", "GOODLETTSVILLE", "PEGRAM", 
    "CLARKSVILLE", "MURFREESBORO", "LEBANON", "GALLATIN", "FAIRVIEW",
    "WHITE BLUFF", "CENTERVILLE"
]

US_STATE_RE = r"\b(" + "|".join(STATE_MAP.keys()) + r"|" + "|".join(STATE_MAP.values()) + r")\b"
ZIP_RE = r"\b\d{5}(?:-\d{4})?\b"
STREET_START_RE = r"^\d+\s+[A-Za-z0-9]"

NAME_BLACKLIST = [
    r"\btransfer", r"\bsold\s+to", r"\bgiven\s+to", r"\bspaces", 
    r"\bcontract", r"\bsee\s+new", r"\bvoid", 
    r"\bcancel", r"\bdeed", r"\binterment", r"\bitem\s+description",
    r"\bprice\b", r"\bsales\s+date", r"\bused\b",
    r"\bdivorced\b", r"\bdeceased\b", r"\bwidow\b"
]

NAME_NOISE_PATTERNS = [
    r"\btoor\b", r"\bmbo\b", r"\byoh\b", r"\bsbo\b",
    r"^\d+[\s\-A-Z]*\b", r"^[;:\.,\-\*]+", 
    r"\bowner\s*id\b.*", r"\bowner\s*since\b.*",
    r"\d+" 
]

ADDRESS_BLOCKERS = [
    r"\bpo\s*box\b", r"\bbox\b",
    r"\broad\b", r"\brd\b", r"\bstreet\b", r"\bst\b",
    r"\bavenue\b", r"\bave\b", r"\bdrive\b", r"\bdr\b",
    r"\blane\b", r"\bln\b", r"\bcourt\b", r"\bct\b",
    r"\bhighway\b", r"\bhwy\b", r"\bblvd\b", r"\bboulevard\b",
    r"\bparkway\b", r"\bpkwy\b", r"\btrail\b", r"\btrl\b",
    r"\bcircle\b", r"\bcir\b", r"\bplace\b", r"\bpl\b"
]

FUNERAL_PN_PATTERNS = [
    r"\bprecoa\b.*\bpolicy\b", r"\bforethought\b.*\bpolicy\b", r"\bpn\s*insurance\b",
    r"\bpreneed\b.*\bpolicy\b", r"\bpre-need\b.*\bpolicy\b", r"\bfuneral\b.*\bpre-need\b",
    r"\bfuneral\b.*\bprearrange(?:ment|d)?\b", r"\bpolicy\s*#\b",
]

AT_NEED_FUNERAL_PATTERNS = [
    r"\bhh\b.*\ban\b.*\bfuneral\b", r"\ban\b.*\bfuneral\b", r"\bat-need\b.*\bfuneral\b",
    r"\ban\s+funeral\b", r"\bhh\s+an\s+funeral\b",
]

MEMORIAL_PATTERNS = [
    r"\bmemorial\b", r"\bmarker\b", r"\bbronze\b", r"\bgranite\b", r"\btablet\b",
    r"\bplaque\b", r"\bvase\b", r"\bmm\s*marker\b",
]

INTERMENT_SERVICE_PATTERNS = [
    r"\binterment\b", r"\bopening\b", r"\bclosing\b", r"\bo/?c\b", r"\bsetting\b", r"\binstallation\b",
]

TRANSFER_CANCEL_PATTERNS = [
    r"\bcancel(?:led|ed)?\b", r"\bvoid\b", r"\bno\s+longer\b",
]

RIGHTS_NOTATION_RE = re.compile(r"\b(\d+)\s*/\s*(\d+)\b")

CACHE_DIR = "_ocr_cache_v27"

EXCEL_CELL_MAX = 32767
EXCEL_SAFE_MAX = 32000
TRUNC_SUFFIX = " …[TRUNCATED]"

GARDEN_PATTERNS_RAW = [
    r"\bal-mahdi\b", r"\batonement\b", r"\bbell\s*tower\b", r"\bcarillon\b",
    r"\bchapel(?:\s*hill)?\b", r"\bcross\b", r"\beternal\s*life\b", r"\beverlasting\s*life\b",
    r"\bfaith\b", r"\bfountain\b", r"\bgarden\s*of\s*grace\b", r"\bgrace\b",
    r"\bgethsemane\b", r"\bgood\s*shepherd\b", r"\blakeside\b", r"\blakeview\b",
    r"\blast\s*supper\b", r"\bmaus\.?\s*private\s*estates\b", r"\bmountain\s*view\b",
    r"\bossuary\b", r"\bpeace\b", r"\bprayer\b", r"\bprivate\s*estate\b",
    r"\bprivate\s*mausoleum\b", r"\bserenity\b", r"\bsermon\s*on\s*the\s*mount\b",
    r"\bstations\s*of\s*the\s*cross\b", r"\btranquility\b", r"\bunity\b", r"\bwall\s*columbarium\b"
]

PROPERTY_PATTERNS = [
    r"\bspace\b", r"\bsp\.?\b", r"\blot\b", r"\bsection\b", r"\bsec\.?\b",
    r"\bblock\b", r"\bblk\.?\b", r"\bgarden\b", r"\bcrypt\b", r"\blawn\b",
    r"\bgrave\b", r"\bburial\b", r"\bmem\.?\s*gds\.?\b", r"\bmausoleum\b",
    r"\bmaus\.?\b", r"\bniche\b", r"\bcolumbarium\b", r"\bestates?\b",
    r"\bcremation\b", r"\b\d+[:]\w+[:]\d+\b", r"\b\d+[/]\w+[/]\d+\b", r"\b\w+[/]\d+[/]\w+\b"
] + GARDEN_PATTERNS_RAW

# -----------------------------
# Helpers
# -----------------------------

def compile_any(patterns: List[str]) -> List[re.Pattern]:
    return [re.compile(p, re.IGNORECASE) for p in patterns]

def sha1_text(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()

def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def safe_upper(s: str) -> str:
    return normalize_ws(s).upper()

def matches_any(line: str, patterns: List[re.Pattern]) -> bool:
    return any(p.search(line or "") for p in patterns)

# Compile regexes
RE_FUNERAL_PN = compile_any(FUNERAL_PN_PATTERNS)
RE_AT_NEED = compile_any(AT_NEED_FUNERAL_PATTERNS)
RE_MEMORIAL = compile_any(MEMORIAL_PATTERNS)
RE_PROPERTY = compile_any(PROPERTY_PATTERNS)
RE_GARDEN_CHECK = compile_any(GARDEN_PATTERNS_RAW)
RE_INTERMENT = compile_any(INTERMENT_SERVICE_PATTERNS)
RE_XFER = compile_any(TRANSFER_CANCEL_PATTERNS)
RE_NAME_BLACKLIST = compile_any(NAME_BLACKLIST)
RE_NAME_NOISE = compile_any(NAME_NOISE_PATTERNS)
RE_ADDR_BLOCK = compile_any(ADDRESS_BLOCKERS)

def normalize_state(st: str) -> str:
    if not st: return ""
    st_clean = st.upper().replace(".", "").strip()
    return STATE_MAP.get(st_clean, st_clean) 

def extract_zip_state(line: str) -> Tuple[Optional[str], Optional[str]]:
    zipm = re.search(ZIP_RE, line or "")
    statem = re.search(US_STATE_RE, line or "", flags=re.IGNORECASE)
    found_zip = zipm.group(0) if zipm else None
    found_state = None
    if statem:
        raw_state = statem.group(0).upper()
        found_state = normalize_state(raw_state)
    return (found_zip, found_state)

def looks_like_address_line(line: str) -> bool:
    if not line: return False
    z, st = extract_zip_state(line)
    if z and st: return True
    return bool(re.search(
        r"\b(st|street|rd|road|ave|avenue|blvd|dr|drive|ln|lane|ct|court|cir|circle|hwy|highway|pkwy|parkway|trl|trail|pl|place)\b",
        line, re.IGNORECASE
    ))

def split_lines(raw_text: str) -> List[str]:
    raw_text = raw_text.replace("\r\n", "\n").replace("\r", "\n")
    lines = [normalize_ws(x) for x in raw_text.split("\n")]
    return [x for x in lines if x]

def extract_phone(text: str) -> str:
    phones = []
    # Enhanced Regex for dots and spaces
    m_full = re.findall(r"(?:\(?(\d{3})\)?[\s\-\./]?)?(\d{3})[\s\-\./]?(\d{4})", text)
    for area, pre, suf in m_full:
        if not area: area = "615"
        if len(area) == 3 and len(pre) == 3 and len(suf) == 4:
            phones.append(f"({area}) {pre}-{suf}")
    if phones: return phones[0] 
    return ""

# -----------------------------
# IMAGE STRATEGIES
# -----------------------------

def preprocess_standard(pil_img: Image.Image) -> Image.Image:
    img_np = cv2.cvtColor(np.array(pil_img), cv2.COLOR_RGB2BGR)
    gray = cv2.cvtColor(img_np, cv2.COLOR_BGR2GRAY)
    _, binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    return Image.fromarray(binary)

def preprocess_thickening(pil_img: Image.Image) -> Image.Image:
    img_np = cv2.cvtColor(np.array(pil_img), cv2.COLOR_RGB2BGR)
    gray = cv2.cvtColor(img_np, cv2.COLOR_BGR2GRAY)
    binary = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 31, 15)
    kernel = np.ones((2, 2), np.uint8)
    dilated = cv2.erode(binary, kernel, iterations=1)
    return Image.fromarray(dilated)

def preprocess_clahe(pil_img: Image.Image) -> Image.Image:
    img_np = cv2.cvtColor(np.array(pil_img), cv2.COLOR_RGB2BGR)
    lab = cv2.cvtColor(img_np, cv2.COLOR_BGR2LAB)
    l, a, b = cv2.split(lab)
    clahe = cv2.createCLAHE(clipLimit=3.0, tileGridSize=(8, 8))
    cl = clahe.apply(l)
    limg = cv2.merge((cl, a, b))
    enhanced = cv2.cvtColor(cv2.cvtColor(limg, cv2.COLOR_LAB2BGR), cv2.COLOR_BGR2GRAY)
    binary = cv2.adaptiveThreshold(enhanced, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 15, 8)
    return Image.fromarray(binary)

# -----------------------------
# OCR + Parsing helpers
# -----------------------------

def is_gibberish(text: str) -> bool:
    if not text: return True
    if len(text) < 3: return True
    if not any(c.isupper() for c in text): return True
    if not re.search(r'[AEIOUYaeiouy]', text): return True
    words = text.split()
    single_char_words = sum(1 for w in words if len(w) == 1 and w.lower() not in ['a', 'i'])
    if len(words) > 0 and (single_char_words / len(words) > 0.5): return True
    return False

def clean_name_line(line: str, target_char: Optional[str] = None) -> str:
    if not line: return ""

    line_upper = line.upper()
    for city in CITY_BLOCKLIST:
        idx = line_upper.find(city)
        if idx != -1:
            line = line[:idx]
            line_upper = line.upper()

    if "#" in line: line = line.split("#")[0]
    if re.match(r"^\d", line): return "" 

    m_kw = re.search(r"\b(road|rd|street|st|avenue|ave|drive|dr|lane|ln|court|ct|blvd|boulevard|pkwy|parkway|hwy|highway|trl|trail|cir|circle|pl|place|po\s*box|box)\b", line, re.IGNORECASE)
    if m_kw: line = line[:m_kw.start()]

    m_addr_start = re.search(r"\b\d+\s+[A-Za-z]", line)
    if m_addr_start: line = line[:m_addr_start.start()]

    cleaned = line
    for pat in RE_NAME_NOISE:
        cleaned = pat.sub("", cleaned)
    
    if target_char:
        tc = target_char.upper()
        words = cleaned.split()
        cleaned_words = []
        found_start = False
        
        for i, w in enumerate(words):
            w_clean = re.sub(r"^[^a-zA-Z]+", "", w)
            if w_clean.upper().startswith(tc):
                cleaned_words.append(w_clean)
                cleaned_words.extend(words[i+1:])
                found_start = True
                break
            # Fuzzy match BDAIR -> ADAIR
            if len(w_clean) > 3:
                first_char = w_clean[0].upper()
                if first_char in ['B', 'O', '4', '@', 'F'] and tc == 'A':
                    w_corrected = 'A' + w_clean[1:]
                    cleaned_words.append(w_corrected)
                    cleaned_words.extend(words[i+1:])
                    found_start = True
                    break
        
        if found_start:
            cleaned = " ".join(cleaned_words)
        else:
            if is_gibberish(cleaned): return ""

    cleaned = re.sub(r"[^a-zA-Z\s&.,\-]", "", cleaned)
    return normalize_ws(cleaned)

def clean_address_line(line: str) -> str:
    if not line: return ""
    m_owner = re.search(r"\bowner\s*(since|id)", line, re.IGNORECASE)
    if m_owner: line = line[:m_owner.start()]
    m_date = re.search(r"\b\d{1,2}/\d{1,2}/\d{2,4}", line)
    if m_date: line = line[:m_date.start()]
    return normalize_ws(line)

def parse_best_address(lines: List[str]) -> Dict:
    candidates = []
    for i, line in enumerate(lines):
        z, st = extract_zip_state(line)
        if z and st:
            street_candidate = ""
            prev_idx = i - 1
            if prev_idx >= 0:
                street_candidate = lines[prev_idx]
            
            street_candidate = clean_address_line(street_candidate)
            street_candidate = re.sub(r"^[\W_]+", "", street_candidate) 

            score = 50
            if street_candidate and re.search(STREET_START_RE, street_candidate):
                score += 40
            
            if "," in street_candidate: score -= 30
            if len(street_candidate) < 5: score -= 20
                
            candidates.append({
                "Index": prev_idx if prev_idx >= 0 else i,
                "Street": street_candidate,
                "CityStateZip": line,
                "State": st,
                "ZIP": z,
                "Score": score
            })
    if not candidates:
        for i, line in enumerate(lines):
            if looks_like_address_line(line):
                 return {"Index": i, "Street": "", "CityStateZip": line, "State": "", "ZIP": "", "Score": 10, "AddressRaw": line}
        return {"Index": None, "Street": "", "CityStateZip": "", "State": "", "ZIP": "", "Score": 0, "AddressRaw": ""}
    best = sorted(candidates, key=lambda x: x["Score"], reverse=True)[0]
    
    street = best["Street"]
    if best["Score"] <= 0: street = ""

    city = ""
    if best["State"]:
        orig_state_match = re.search(US_STATE_RE, best["CityStateZip"], re.IGNORECASE)
        if orig_state_match:
             state_span = orig_state_match.span()
             city_part = best["CityStateZip"][:state_span[0]]
             city = normalize_ws(city_part).replace(",", "")
    
    return {
        "Index": best["Index"], "Street": street, "City": city,
        "State": best["State"], "ZIP": best["ZIP"], "AddressRaw": f"{street} | {best['CityStateZip']}"
    }

def parse_owner_header(lines: List[str], target_char: Optional[str] = None) -> Tuple[str, str, str, str, Dict]:
    if not lines: return ("", "", "", "", {})
    
    top = lines[:40] 
    addr_info = parse_best_address(top)
    addr_idx = addr_info.get("Index", None)

    clean_header = []
    
    search_lines = top[:addr_idx] if (addr_idx is not None and addr_idx > 0) else top[:5]
    if addr_idx: search_iter = range(len(search_lines) - 1, -1, -1)
    else: search_iter = range(len(search_lines))

    # Standard Search
    for i in search_iter:
        ln = search_lines[i]
        if not ln.strip(): continue
        if matches_any(ln, RE_NAME_BLACKLIST): continue
        ln_cleaned = clean_name_line(ln, target_char)
        if not ln_cleaned: continue 
        if is_gibberish(ln_cleaned): continue
        if addr_idx: clean_header.insert(0, ln_cleaned)
        else: clean_header.append(ln_cleaned)
        if len(clean_header) >= 2: break

    # LAST RESORT SEARCH
    if not clean_header:
        for ln in top[:15]: 
            if matches_any(ln, RE_NAME_BLACKLIST): continue
            if re.search(r"^[A-Z][a-z]+,\s+[A-Z][a-z]+", ln):
                fallback = clean_name_line(ln, target_char)
                if fallback and not is_gibberish(fallback):
                    clean_header.append(fallback)
                    break

    header_text = normalize_ws(" ".join(clean_header))
    header_text = re.sub(r"\b(owner|address|phone|lot|section|space|card)\b[:\-]?", "", header_text, flags=re.IGNORECASE).strip()

    primary = ""
    secondary = ""
    last_name = ""

    if re.search(r"\s&\s|\sand\s", header_text, re.IGNORECASE):
        parts = re.split(r"\s&\s|\sand\s", header_text, flags=re.IGNORECASE)
        parts = [normalize_ws(p) for p in parts if normalize_ws(p)]
        if parts:
            primary = parts[0]
            secondary = parts[1] if len(parts) > 1 else ""
    else:
        primary = header_text

    # V27: If Primary does not start with Target Char, kill it (Flag it)
    if target_char and primary:
        if not primary.upper().startswith(target_char.upper()):
            primary = "[MISSING - CHECK PDF]"

    if "," in primary: last_name = primary.split(",")[0].strip()
    elif " " in primary: last_name = primary.split(" ")[0].strip()
    else: last_name = primary

    return (header_text, primary, secondary, last_name, addr_info)

# -----------------------------
# PROPERTY PARSING
# -----------------------------
def extract_property_details(line: str) -> Dict[str, str]:
    details = {"Garden": "", "Section": "", "Lot": "", "Space": ""}
    if not line: return details
    for p_re in RE_GARDEN_CHECK:
        m = p_re.search(line)
        if m:
            details["Garden"] = normalize_ws(m.group(0))
            break
    m_sec = re.search(r"\b(sec|section|blk|block)\.?\s*([A-Za-z0-9\-]+)", line, re.IGNORECASE)
    if m_sec: details["Section"] = m_sec.group(2)
    m_lot = re.search(r"\b(lot)\.?\s*([A-Za-z0-9\-]+)", line, re.IGNORECASE)
    if m_lot: details["Lot"] = m_lot.group(2)
    m_sp = re.search(r"\b(sp|space|grave)\.?\s*([A-Za-z0-9\-\./]+)", line, re.IGNORECASE)
    if m_sp: details["Space"] = m_sp.group(2)
    if ":" in line:
        parts = [p.strip() for p in line.split(":") if p.strip()]
        for part in parts:
            if not details["Garden"] and any(p_re.search(part) for p_re in RE_GARDEN_CHECK):
                details["Garden"] = part
            elif not details["Space"] and re.match(r"^(sp|space)", part, re.IGNORECASE):
                details["Space"] = re.sub(r"^(sp|space)\.?", "", part, flags=re.IGNORECASE).strip()
            elif not details["Space"] and re.match(r"^\d+(-\d+)*$", part):
                if not details["Lot"]: details["Lot"] = part
                else: details["Space"] = part
            elif not details["Section"] and len(part) <= 2 and part.isalpha():
                details["Section"] = part
    return details

def group_text_lines_from_ocr(data: Dict) -> List[Dict]:
    n = len(data.get("text", []))
    words = []
    for i in range(n):
        txt = data["text"][i]
        if not txt or not str(txt).strip(): continue
        try: conf = int(float(data["conf"][i]))
        except Exception: conf = -1
        if 0 <= conf < 30: continue
        words.append({
            "text": str(txt).strip(), "x": int(data["left"][i]), "y": int(data["top"][i]),
            "w": int(data["width"][i]), "h": int(data["height"][i]), "line_num": int(data["line_num"][i]),
            "block_num": int(data["block_num"][i]), "par_num": int(data["par_num"][i]), "conf": conf,
        })
    groups = {}
    for w in words:
        key = (w["block_num"], w["par_num"], w["line_num"])
        groups.setdefault(key, []).append(w)
    lines = []
    for key, ws in groups.items():
        ws = sorted(ws, key=lambda z: z["x"])
        text = normalize_ws(" ".join([w["text"] for w in ws]))
        x1 = min(w["x"] for w in ws); y1 = min(w["y"] for w in ws)
        x2 = max(w["x"] + w["w"] for w in ws); y2 = max(w["y"] + w["h"] for w in ws)
        lines.append({"key": key, "text": text, "bbox": (x1, y1, x2, y2)})
    lines.sort(key=lambda d: (d["bbox"][1], d["bbox"][0]))
    return lines

def detect_horizontal_strikelines(img_bgr: np.ndarray) -> List[Tuple[int, int, int, int]]:
    gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)
    thr = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_MEAN_C, cv2.THRESH_BINARY_INV, 25, 15)
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (35, 1))
    horiz = cv2.morphologyEx(thr, cv2.MORPH_OPEN, kernel, iterations=1)
    edges = cv2.Canny(horiz, 50, 150)
    lines = cv2.HoughLinesP(edges, 1, np.pi / 180, threshold=80, minLineLength=80, maxLineGap=10)
    segs = []
    if lines is None: return segs
    for l in lines[:, 0, :]:
        x1, y1, x2, y2 = map(int, l)
        if abs(y2 - y1) <= 3 and abs(x2 - x1) >= 80:
            segs.append((x1, y1, x2, y2))
    return segs

def line_is_struck(line_bbox: Tuple[int, int, int, int], strike_segs: List[Tuple[int, int, int, int]]) -> bool:
    x1, y1, x2, y2 = line_bbox
    midy = (y1 + y2) // 2
    for sx1, sy1, sx2, sy2 in strike_segs:
        if y1 - 2 <= sy1 <= y2 + 2 and not (sx2 < x1 or sx1 > x2): return True
        if abs(sy1 - midy) <= 3 and not (sx2 < x1 or sx1 > x2): return True
    return False

def standardize_name(name: str) -> str:
    name = normalize_ws(name)
    name = re.sub(r"[^\w\s\-\.'&]", "", name)
    return name

def owner_key(primary: str, secondary: str, addr: Dict) -> str:
    parts = [safe_upper(primary), safe_upper(secondary), safe_upper(addr.get("Street", "")), (addr.get("ZIP", "") or "")]
    return sha1_text("|".join(parts))[:12]

def is_excludable_item(line: str) -> bool:
    return matches_any(line, RE_XFER)

def classify_item(line: str) -> Dict[str, bool]:
    t = line or ""
    flags = {
        "IsProperty": matches_any(t, RE_PROPERTY),
        "IsMemorial": matches_any(t, RE_MEMORIAL),
        "IsFuneralPreneed": matches_any(t, RE_FUNERAL_PN),
        "IsAtNeedFuneral": matches_any(t, RE_AT_NEED),
        "IsIntermentService": matches_any(t, RE_INTERMENT),
        "HasRightsNotation": bool(RIGHTS_NOTATION_RE.search(t)),
    }
    if flags["IsIntermentService"]: flags["IsFuneralPreneed"] = False
    if flags["IsAtNeedFuneral"]: flags["IsFuneralPreneed"] = False
    return flags

def rights_used_total(line: str) -> Optional[Tuple[int, int, str]]:
    m = RIGHTS_NOTATION_RE.search(line or "")
    if not m: return None
    a, b = int(m.group(1)), int(m.group(2))
    return (a, b, f"{a}/{b}")

def total_owners_on_file(primary: str, secondary: str) -> int:
    return 2 if normalize_ws(secondary) else 1

def compute_likely_burials(items: List[Dict]) -> int:
    used_counts: List[int] = []
    for it in items:
        if not it.get("Include", False): continue
        if not it.get("IsProperty", False): continue
        
        txt = (it.get("LineText", "") or "").upper()
        if re.search(r"\bX\b", txt) or re.search(r"\bUSED\b", txt):
            used_counts.append(1)

        ru = it.get("RightsUsed", None)
        if ru is not None:
            try:
                if not pd.isna(ru): used_counts.append(int(ru))
            except Exception: pass
            
    return max(used_counts) if used_counts else 0

# -----------------------------
# MAIN PIPELINE (V27 HYBRID)
# -----------------------------

def process_page_hybrid(pdf_path: str, page_index: int, dpi: int, target_char: Optional[str]) -> Tuple[Dict, List[Dict]]:
    # V27: Run TWO scans. 
    # 1. Standard (for Address)
    # 2. CLAHE (for Faint Names/Property)
    
    os.makedirs(CACHE_DIR, exist_ok=True)
    
    # --- SCAN A: STANDARD ---
    imgs = convert_from_path(pdf_path, dpi=dpi, first_page=page_index + 1, last_page=page_index + 1)
    if not imgs: raise RuntimeError(f"Failed to render page {page_index+1}")
    pil_original = imgs[0].convert("RGB")
    
    pil_std = preprocess_standard(pil_original)
    pil_clahe = preprocess_clahe(pil_original)
    
    # Perform OCR A (Std)
    d_std = pytesseract.image_to_data(pil_std, config='--psm 6', output_type=Output.DICT)
    t_std = pytesseract.image_to_string(pil_std, config='--psm 6')
    
    # Perform OCR B (Clahe)
    d_clahe = pytesseract.image_to_data(pil_clahe, config='--psm 6', output_type=Output.DICT)
    t_clahe = pytesseract.image_to_string(pil_clahe, config='--psm 6')
    
    # Parse A
    lines_a = split_lines(t_std)
    _, p_a, s_a, l_a, addr_a = parse_owner_header(lines_a, target_char)
    phone_a = extract_phone(t_std)
    
    # Parse B
    lines_b = split_lines(t_clahe)
    _, p_b, s_b, l_b, addr_b = parse_owner_header(lines_b, target_char)
    phone_b = extract_phone(t_clahe)
    
    # --- MERGE STRATEGY ---
    
    # Name Priority: Look for valid 'A' name in B (Clahe), then A (Std).
    final_primary = "[MISSING - CHECK PDF]"
    final_secondary = ""
    final_last = ""
    
    # Check B first (better for faint names)
    if p_b and p_b != "[MISSING - CHECK PDF]":
        final_primary, final_secondary, final_last = p_b, s_b, l_b
    elif p_a and p_a != "[MISSING - CHECK PDF]":
        final_primary, final_secondary, final_last = p_a, s_a, l_a
        
    # Address Priority: Look for valid Address in A (Std - usually cleaner), then B.
    final_addr = addr_a
    if not final_addr.get("ZIP"):
        if addr_b.get("ZIP"): final_addr = addr_b
        
    # Phone Priority:
    final_phone = phone_a if phone_a else phone_b
    
    # Item Merge: Combine items from BOTH, removing duplicates by text hash
    raw_lines_a = group_text_lines_from_ocr(d_std)
    raw_lines_b = group_text_lines_from_ocr(d_clahe)
    
    strike_segs_a = detect_horizontal_strikelines(cv2.cvtColor(np.array(pil_std.convert("RGB")), cv2.COLOR_RGB2BGR))
    strike_segs_b = detect_horizontal_strikelines(cv2.cvtColor(np.array(pil_clahe.convert("RGB")), cv2.COLOR_RGB2BGR))
    
    all_items = []
    seen_hashes = set()
    
    # Helper to process lines
    def process_lines(lines, strikes):
        for ln_obj in lines:
            txt = ln_obj["text"]
            if not txt: continue
            
            h = sha1_text(normalize_ws(txt))
            if h in seen_hashes: continue
            
            struck = line_is_struck(ln_obj["bbox"], strikes)
            excludable = is_excludable_item(txt)
            cls = classify_item(txt)
            
            # V27: If it's a Property, extract details
            prop_details = {"Garden":"", "Section":"", "Lot":"", "Space":""}
            if cls["IsProperty"]:
                prop_details = extract_property_details(txt)
            
            looks_item = (cls["IsProperty"] or cls["IsMemorial"] or cls["IsFuneralPreneed"] or cls["IsAtNeedFuneral"] or cls["HasRightsNotation"])
            include = looks_item and (not struck) and (not excludable)
            
            rt = rights_used_total(txt)
            
            all_items.append({
                "LineText": txt,
                "StruckThrough": bool(struck),
                "ExcludedByText": bool(excludable),
                "Include": bool(include),
                "IsProperty": bool(cls["IsProperty"]),
                "IsMemorial": bool(cls["IsMemorial"]),
                "IsFuneralPreneed": bool(cls["IsFuneralPreneed"]),
                "IsAtNeedFuneral": bool(cls["IsAtNeedFuneral"]),
                "IsIntermentService": bool(cls["IsIntermentService"]),
                "RightsNotation": rt[2] if rt else "",
                "RightsUsed": rt[0] if rt else None,
                "RightsTotal": rt[1] if rt else None,
                "Prop_Garden": prop_details["Garden"], 
                "Prop_Section": prop_details["Section"],
                "Prop_Lot": prop_details["Lot"], 
                "Prop_Space": prop_details["Space"]
            })
            seen_hashes.add(h)

    process_lines(raw_lines_b, strike_segs_b) # Prefer CLAHE for faint items
    process_lines(raw_lines_a, strike_segs_a) # Fill gaps with Std
    
    return ({
        "OwnerName_Raw": f"{final_primary} {final_secondary}",
        "PrimaryOwnerName": final_primary,
        "SecondaryOwnerName": final_secondary,
        "LastName": final_last,
        "Phone": final_phone,
        "Street": final_addr.get("Street", ""),
        "City": final_addr.get("City", ""),
        "State": final_addr.get("State", ""),
        "ZIP": final_addr.get("ZIP", ""),
        "AddressRaw": final_addr.get("AddressRaw", ""),
        "RawText": t_std + "\n" + t_clahe, # Save both for debugging
        "RawTextHash": sha1_text(t_std)
    }, all_items)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", required=True, help="Input scanned PDF")
    ap.add_argument("--out", required=True, help="Output Excel path")
    ap.add_argument("--dpi", type=int, default=300, help="OCR render DPI")
    args = ap.parse_args()

    pdf_path = args.pdf
    if not os.path.exists(pdf_path):
        print(f"PDF not found: {pdf_path}", file=sys.stderr)
        sys.exit(1)

    target_char = None
    filename = os.path.basename(pdf_path)
    if filename and filename[0].isalpha():
        target_char = filename[0].upper()
        print(f"[Info] Detected Target Letter from filename: '{target_char}'")

    thumbs = convert_from_path(pdf_path, dpi=50)
    page_count = len(thumbs)
    print(f"[Checkpoint] Detected pages: {page_count}")

    owners_rows = []
    items_rows = []

    for p in tqdm(range(page_count), desc="Hybrid Scan Pages", unit="page"):
        owner_data, items_data = process_page_hybrid(pdf_path, p, args.dpi, target_char)
        
        rec_id = f"A-P{p+1:04d}"
        owner_data["OwnerRecordID"] = rec_id
        owner_data["SourceFile"] = os.path.basename(pdf_path)
        owner_data["PageNumber"] = p + 1
        
        # Key generation
        parts = [
            safe_upper(owner_data["PrimaryOwnerName"]),
            safe_upper(owner_data["SecondaryOwnerName"]),
            safe_upper(owner_data["Street"]),
            (owner_data["ZIP"] or "")
        ]
        owner_data["OwnerGroupKey"] = sha1_text("|".join(parts))[:12]
        
        owners_rows.append(owner_data)
        
        for it in items_data:
            it["OwnerRecordID"] = rec_id
            it["SourceFile"] = os.path.basename(pdf_path)
            it["Page"] = p + 1
            items_rows.append(it)

    owners_df = pd.DataFrame(owners_rows)
    items_df = pd.DataFrame(items_rows)

    dup = owners_df.groupby("RawTextHash").size().reset_index(name="Count")
    dup = dup[dup["Count"] > 1]
    possible_dups = owners_df.merge(dup, on="RawTextHash", how="inner").sort_values(["RawTextHash", "PageNumber"])

    inc = items_df[items_df["Include"] == True].copy()

    def agg_owner(group: pd.DataFrame) -> pd.Series:
        has_property = bool(group["IsProperty"].any())
        has_memorial = bool(group["IsMemorial"].any())
        has_pn = bool(group["IsFuneralPreneed"].any())
        has_an = bool(group["IsAtNeedFuneral"].any())
        
        # Collect evidence lines
        memorial_lines = group[group["IsMemorial"] == True]["LineText"].tolist()
        pn_lines = group[group["IsFuneralPreneed"] == True]["LineText"].tolist()
        an_lines = group[group["IsAtNeedFuneral"] == True]["LineText"].tolist()
        property_lines = group[group["IsProperty"] == True]["LineText"].tolist()

        likely_burials = compute_likely_burials(group.to_dict("records"))
        
        owner_row = owners_df[owners_df["OwnerRecordID"] == group.name].iloc[0]
        total_owners = total_owners_on_file(owner_row["PrimaryOwnerName"], owner_row["SecondaryOwnerName"])
        living_exists = True if (int(likely_burials) < int(total_owners)) else False

        pn_status = "TRUE" if has_pn else "FALSE"
        if total_owners == 2 and has_pn:
            policy_like = [ln for ln in pn_lines if re.search(r"\bpolicy\b", ln, re.IGNORECASE)]
            if len(policy_like) == 1: pn_status = "PARTIAL"

        needs_memorial = bool(has_property and (not has_memorial))
        needs_pn = bool(has_property and (pn_status in ["FALSE", "PARTIAL"]))
        spaces_only_prime = bool(has_property and (not has_memorial) and (pn_status in ["FALSE", "PARTIAL"]) and living_exists)
        survivor_opp = bool((total_owners == 2) and (len(an_lines) == 1) and (pn_status in ["FALSE", "PARTIAL"]) and has_property and living_exists)
        
        return pd.Series({
            "HasProperty": has_property, "HasMemorial": has_memorial, "HasAtNeedFuneral": has_an,
            "HasFuneralPreneedPlanStatus": pn_status, "LikelyBurials": int(likely_burials),
            "TotalOwnersOnFile": int(total_owners), "LivingOwnerExists": bool(living_exists),
            "NeedsMemorial": bool(needs_memorial), "NeedsPNFuneral": bool(needs_pn),
            "SpacesOnly_PRIME": bool(spaces_only_prime), "SurvivorSpouse_Opportunity": bool(survivor_opp),
            "MemorialEvidence": " || ".join(memorial_lines[:3]), "PNFuneralEvidence": " || ".join(pn_lines[:3]),
            "AtNeedEvidence": " || ".join(an_lines[:3]), "PropertyEvidence": " || ".join(property_lines[:3]),
        })

    if len(inc) > 0:
        try: owner_flags = inc.groupby("OwnerRecordID").apply(agg_owner, include_groups=False).reset_index()
        except TypeError: owner_flags = inc.groupby("OwnerRecordID").apply(agg_owner).reset_index()
    else: owner_flags = pd.DataFrame(columns=["OwnerRecordID"])

    owners_master = owners_df.merge(owner_flags, on="OwnerRecordID", how="left")
    
    # Fill defaults
    defaults = {
        "HasProperty": False, "HasMemorial": False, "HasAtNeedFuneral": False,
        "HasFuneralPreneedPlanStatus": "FALSE", "LikelyBurials": 0, "TotalOwnersOnFile": 1,
        "LivingOwnerExists": True, "NeedsMemorial": False, "NeedsPNFuneral": False,
        "SpacesOnly_PRIME": False, "SurvivorSpouse_Opportunity": False,
        "MemorialEvidence": "", "PNFuneralEvidence": "", "AtNeedEvidence": "", "PropertyEvidence": "",
    }
    for col, default in defaults.items():
        if col in owners_master.columns: owners_master[col] = owners_master[col].fillna(default)

    # Filter Lists
    list_memorial = owners_master[(owners_master["HasProperty"] == True) & (owners_master["HasMemorial"] != True) & (owners_master["LivingOwnerExists"] == True)].copy()
    list_pn = owners_master[(owners_master["HasProperty"] == True) & (owners_master["HasFuneralPreneedPlanStatus"].isin(["FALSE", "PARTIAL"])) & (owners_master["LivingOwnerExists"] == True)].copy()
    list_prime = owners_master[owners_master["SpacesOnly_PRIME"] == True].copy()
    list_survivor = owners_master[owners_master["SurvivorSpouse_Opportunity"] == True].copy()

    # Stats
    stats = pd.DataFrame([{
        "GeneratedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "SourceFile": os.path.basename(pdf_path),
        "PagesDetected": page_count, "OwnerRecords": len(owners_master), "PossibleDuplicateScans": 0,
        "Owners_HasProperty": int((owners_master["HasProperty"] == True).sum()),
        "Owners_HasMemorial": int((owners_master["HasMemorial"] == True).sum()),
        "Owners_PN_TRUE": int((owners_master["HasFuneralPreneedPlanStatus"] == "TRUE").sum()),
        "Owners_PN_PARTIAL": int((owners_master["HasFuneralPreneedPlanStatus"] == "PARTIAL").sum()),
        "Owners_PN_FALSE": int((owners_master["HasFuneralPreneedPlanStatus"] == "FALSE").sum()),
        "LIST_Memorial_Letter": len(list_memorial), "LIST_PN_Funeral_Letter": len(list_pn),
        "LIST_SpacesOnly_PRIME": len(list_prime), "LIST_SurvivorSpouse": len(list_survivor),
    }])

    # Safety
    owners_master_safe = make_df_excel_safe(owners_master)
    items_df_safe = make_df_excel_safe(items_df)
    list_memorial_safe = make_df_excel_safe(list_memorial)
    list_pn_safe = make_df_excel_safe(list_pn)
    list_prime_safe = make_df_excel_safe(list_prime)
    list_survivor_safe = make_df_excel_safe(list_survivor)
    possible_dups_safe = make_df_excel_safe(possible_dups)
    stats_safe = make_df_excel_safe(stats)

    out_path = args.out
    tmp_path = out_path + ".tmp.xlsx"
    engine = choose_excel_engine()
    print(f"Writing Excel with engine={engine} to temp file: {tmp_path} ...")

    with pd.ExcelWriter(tmp_path, engine=engine) as xw:
        owners_master_safe.to_excel(xw, index=False, sheet_name="Owners_Master")
        items_df_safe.to_excel(xw, index=False, sheet_name="OwnerItems_Normalized")
        list_memorial_safe.to_excel(xw, index=False, sheet_name="LIST_Memorial_Letter")
        list_pn_safe.to_excel(xw, index=False, sheet_name="LIST_PN_Funeral_Letter")
        list_prime_safe.to_excel(xw, index=False, sheet_name="LIST_SpacesOnly_PRIME")
        list_survivor_safe.to_excel(xw, index=False, sheet_name="LIST_SurvivorSpouse_Opp")
        possible_dups_safe.to_excel(xw, index=False, sheet_name="PossibleDuplicateScans")
        stats_safe.to_excel(xw, index=False, sheet_name="Stats")

    try: import openpyxl; _ = openpyxl.load_workbook(tmp_path, read_only=True)
    except Exception as e:
        print("\n❌ ERROR: The workbook written could not be re-opened for verification."); print(f"Details: {e}"); sys.exit(1)

    try: os.replace(tmp_path, out_path)
    except PermissionError:
        print(f"\n❌ ERROR: Could not overwrite '{out_path}'. It may be open in Excel."); sys.exit(1)

    print("\nDONE ✅"); print(f"Output written to: {out_path}")
    print("Key sheets: Owners_Master, LIST_SpacesOnly_PRIME, LIST_Memorial_Letter, LIST_PN_Funeral_Letter")

if __name__ == "__main__":
    main()
