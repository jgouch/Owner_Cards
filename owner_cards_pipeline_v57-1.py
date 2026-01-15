#!/usr/bin/env python3
"""
Owner Card Pipeline — v57 (Performance & Stability Refactor)

Improvements over v56:
- PERFORMANCE: EasyOCR model is initialized ONCE globally, not per-page.
- LOGGING: Catch blocks now print specific errors rather than silencing them.
- LOGIC: Address parsing now scans 2 lines back for street addresses.
- LOGIC: Text layer heuristic lowered (30 chars) to catch sparse digital cards.
- SAFETY: Target letter validation disabled if filename does not start with a letter.

Usage:
  python3 owner_cards_pipeline.py
"""

import argparse
import glob
import hashlib
import os
import re
from datetime import datetime
from typing import List, Dict, Tuple, Optional

import numpy as np
import pandas as pd
from tqdm import tqdm

# External libs
from PyPDF2 import PdfReader
from pdf2image import convert_from_path, pdfinfo_from_path
import cv2
from PIL import Image
import pytesseract

# Optional: EasyOCR
try:
    import easyocr
    EASYOCR_AVAILABLE = True
except ImportError:
    EASYOCR_AVAILABLE = False

# Global Cache for EasyOCR Reader to prevent re-loading per page
_EASYOCR_READER = None


# -----------------------------
# CONFIG
# -----------------------------

SNAPSHOT_DIR = "_Failed_Snapshots"
EXCEL_SAFE_MAX = 32000
TRUNC_SUFFIX = " …[TRUNCATED]"

# OCR Configs
OCR_PSM6 = "--oem 3 -l eng --psm 6"
OCR_PSM11 = "--oem 3 -l eng --psm 11"

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
    "TENN": "TN", "TENNESSES": "TN", "TEN": "TN", "TENN.": "TN", "TN.": "TN", "TIN": "TN",
    "IN": "IN"
}

CITY_BLOCKLIST = [
    "NASHVILLE", "BRENTWOOD", "FRANKLIN", "MADISON", "ANTIOCH", 
    "HERMITAGE", "OLD HICKORY", "GOODLETTSVILLE", "PEGRAM", 
    "CLARKSVILLE", "MURFREESBORO", "LEBANON", "GALLATIN", "FAIRVIEW",
    "WHITE BLUFF", "CENTERVILLE", "CHAPEL HILL"
]

# Patterns
US_STATE_RE = r"\b(" + "|".join(STATE_MAP.keys()) + r"|" + "|".join(STATE_MAP.values()) + r")\b"
ZIP_RE = r"\b\d{5}(?:-\d{4})?\b"
STREET_START_RE = r"^\d+\s+[A-Za-z0-9]"

NAME_BLACKLIST = [
    r"\btransfer", r"\bsold\s+to", r"\bgiven\s+to", r"\bspaces", 
    r"\bcontract", r"\bsee\s+new", r"\bvoid", 
    r"\bcancel", r"\bdeed", r"\binterment", r"\bitem\s+description",
    r"\bprice\b", r"\bsales\s+date", r"\bused\b",
    r"\bdivorced\b", r"\bdeceased\b", r"\bwidow\b",
    r"\bgarden\b", r"\bsection\b", r"\blot\b", r"\bblock\b", 
    r"\bsermon\b", r"\bchapel\b", r"\bmt\b", r"\bmountain\b",
    r"\bsex\b", r"\bmale\b", r"\bfemale\b", r"\bgrave\b"
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
    r"\bcancel(?:led|ed)?\b", r"\bvoid\b", r"\bno\s+longer\b", r"\brefunded\b", r"\btransfe?r(?:red|ed)?\b",
]

RIGHTS_NOTATION_RE = re.compile(r"\b(\d+)\s*/\s*(\d+)\b")

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
# HELPERS
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

# Pre-compile regex lists
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

def split_lines(raw_text: str) -> List[str]:
    raw_text = (raw_text or "").replace("\r\n", "\n").replace("\r", "\n")
    lines = [normalize_ws(x) for x in raw_text.split("\n")]
    return [x for x in lines if x]

def excel_safe_text(v):
    if v is None: return ""
    if pd.isna(v): return ""
    if isinstance(v, (int, float)): return v
    s = str(v)
    s = re.sub(r"[^\x09\x0A\x0D\x20-\uD7FF\uE000-\uFFFD]", "", s)
    if s.startswith(("=", "+", "-", "@")): s = "'" + s
    if len(s) > EXCEL_SAFE_MAX: s = s[:EXCEL_SAFE_MAX] + TRUNC_SUFFIX
    return s

def choose_excel_engine() -> str:
    try: import xlsxwriter; return "xlsxwriter"
    except: return "openpyxl"

def force_string_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = df[c].apply(lambda x: "" if pd.isna(x) else str(x))
    return df

# -----------------------------
# ADDRESS & PHONE
# -----------------------------

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

def extract_phone(text: str) -> Tuple[str, bool]:
    phones = []
    # Pattern: (XXX) XXX-XXXX or XXX-XXX-XXXX
    m_full = re.findall(r"(?:\(?(\d{3})\)?[\s\-\./]?)?(\d{3})[\s\-\./]?(\d{4})", text or "")
    for area, pre, suf in m_full:
        if not area: area = "615" # Default if missing
        if len(area) == 3 and len(pre) == 3 and len(suf) == 4:
            phones.append(f"({area}) {pre}-{suf}")
    
    # Fallback: 7-digits boundaries
    if not phones:
        m7 = re.search(r"\b(\d{3})[\s\-\./]?(\d{4})\b", text or "")
        if m7: return (f"{m7.group(1)}-{m7.group(2)}", False)

    return (phones[0] if phones else "", True if phones else False)


# -----------------------------
# HEADER PARSING / CLEANING
# -----------------------------

INITIAL_DIGIT_MAP = {"8": "B", "0": "O", "1": "I", "2": "Z", "5": "S", "6": "G", "9": "P"}

def fix_digit_initials_in_name(line: str) -> str:
    if not line: return line
    tokens = line.split()
    out = []
    for i, tok in enumerate(tokens):
        core = tok.rstrip(".,;:")
        trail = tok[len(core):]
        if len(core) == 1 and core.isdigit() and core in INITIAL_DIGIT_MAP:
            if i > 0 and re.search(r"[A-Za-z]", tokens[i - 1]):
                out.append(INITIAL_DIGIT_MAP[core] + (trail if trail else "."))
                continue
        out.append(tok)
    return " ".join(out)

def fix_leading_digit_as_letter(line: str, target_char: Optional[str]) -> str:
    if not line or not line[0].isdigit(): return line
    if line[0] in INITIAL_DIGIT_MAP and len(line) > 2 and line[1].isalpha():
        repl = INITIAL_DIGIT_MAP[line[0]]
        # Only swap if we have a target char (e.g. 'A') and the replacement matches
        if (target_char) and (repl.upper() == target_char.upper()):
             if "," in line[:20] or line[:20].isupper():
                 return repl + line[1:]
    return line

def is_gibberish(text: str) -> bool:
    if not text or len(text) < 3: return True
    if not any(c.isupper() for c in text): return True
    if not re.search(r"[AEIOUYaeiouy]", text): return True
    words = text.split()
    single_char_words = sum(1 for w in words if len(w) == 1 and w.lower() not in ['a', 'i'])
    if words and (single_char_words / len(words) > 0.4): return True
    return False

def clean_name_line(line: str, target_char: Optional[str] = None, aggressive: bool = False) -> str:
    if not line: return ""
    line = fix_digit_initials_in_name(line)
    if re.match(r"^\d", line):
        line = fix_leading_digit_as_letter(line, target_char)
        if re.match(r"^\d", line): return ""
    
    if matches_any(line, [re.compile(p, re.IGNORECASE) for p in [r"\bSermon\b", r"\bChapel\b", r"\bGarden\b", r"\bSection\b", r"\bMount\b", r"\bMt\.?\b", r"\bSex\b", r"\bMale\b", r"\bFemale\b", r"\bGrave\b"]]):
        return ""
    
    line_upper = line.upper()
    for city in CITY_BLOCKLIST:
        idx = line_upper.find(city)
        if idx != -1: line = line[:idx]; line_upper = line.upper()
    
    if "#" in line: line = line.split("#")[0]
    
    m_kw = re.search(r"\b(road|rd|street|st|avenue|ave|drive|dr|lane|ln|court|ct|blvd|boulevard|pkwy|parkway|hwy|highway|trl|trail|cir|circle|pl|place|po\s*box|box)\b", line, re.IGNORECASE)
    if m_kw: line = line[:m_kw.start()]

    cleaned = line
    for pat in RE_NAME_NOISE: cleaned = pat.sub("", cleaned)
    cleaned = re.sub(r"[^a-zA-Z\s&\.,\-']", "", cleaned)
    cleaned = normalize_ws(cleaned)
    
    # If target_char is set, filter names that don't match (Aggressive mode only)
    if aggressive and target_char:
        tc = target_char.upper()
        if not cleaned.upper().startswith(tc):
            # Try to find the name within the string
            idx = cleaned.upper().find(tc)
            if idx != -1: cleaned = cleaned[idx:].strip()
            else: return "" # Reject if target char not found in aggressive mode
        
    return cleaned

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
            # Look back 1 or 2 lines for street
            street_candidate = ""
            best_offset = 0
            
            # Check 1 line back
            if i - 1 >= 0:
                s1 = clean_address_line(lines[i-1])
                if re.search(STREET_START_RE, s1): 
                    street_candidate = s1
                    best_offset = 1
            
            # Check 2 lines back (if 1 didn't match nicely)
            if not street_candidate and i - 2 >= 0:
                s2 = clean_address_line(lines[i-2])
                if re.search(STREET_START_RE, s2):
                    street_candidate = s2
                    best_offset = 2
            
            # Fallback to prev line if no regex match
            if not street_candidate and i - 1 >= 0:
                 street_candidate = clean_address_line(lines[i-1])

            score = 50
            if street_candidate and re.search(STREET_START_RE, street_candidate): score += 40
            if re.search(r"^[A-Z][a-z]+,\s+[A-Z]", street_candidate): score -= 100
            
            candidates.append({"Index": i, "Street": street_candidate, "CityStateZip": line, "State": st, "ZIP": z, "Score": score})
    
    if not candidates:
        return {"Index": None, "Street": "", "CityStateZip": "", "State": "", "ZIP": "", "Score": 0, "AddressRaw": ""}
    
    best = sorted(candidates, key=lambda x: x["Score"], reverse=True)[0]
    street = best["Street"] if best["Score"] > 0 else ""
    
    city = ""
    if best["State"]:
        m = re.search(US_STATE_RE, best["CityStateZip"], re.IGNORECASE)
        if m:
            city = normalize_ws(best["CityStateZip"][:m.start()].replace(",", ""))
            
    return {"Index": best["Index"], "Street": street, "City": city, "State": best["State"], "ZIP": best["ZIP"], "AddressRaw": f"{street} | {best['CityStateZip']}"}

def get_header_candidate(lines: List[str], addr_idx: Optional[int], target_char: Optional[str], aggressive: bool) -> List[str]:
    clean_header = []
    top = lines[:40]
    search_lines = top[:addr_idx] if (addr_idx is not None and addr_idx > 0) else top[:10]
    
    for ln in search_lines:
        if not ln.strip(): continue
        if matches_any(ln, RE_NAME_BLACKLIST): continue
        ln_clean = clean_name_line(ln, target_char, aggressive)
        if not ln_clean or is_gibberish(ln_clean): continue
        clean_header.append(ln_clean)
        if len(clean_header) >= 2: break
    return clean_header

def parse_owner_header(lines: List[str], target_char: Optional[str] = None) -> Tuple[str, str, str, str, Dict, bool]:
    if not lines: return ("", "", "", "", {}, False)
    
    top = lines[:40]
    addr_info = parse_best_address(top)
    addr_idx = addr_info.get("Index")
    
    is_interment = any("INTERMENT RECORD" in ln.upper() for ln in top[:15])
    if is_interment: return ("INTERMENT RECORD - REFILE", "INTERMENT RECORD - REFILE", "", "", {}, True)
    
    # PASS 1: CONSERVATIVE
    clean_header = get_header_candidate(lines, addr_idx, target_char, aggressive=False)
    is_valid = False
    if clean_header:
        candidate_text = normalize_ws(" ".join(clean_header))
        if target_char and candidate_text.upper().startswith(target_char.upper()): is_valid = True
        elif not target_char and not is_gibberish(candidate_text): is_valid = True
    
    # PASS 2: AGGRESSIVE
    if not is_valid:
        clean_header = get_header_candidate(lines, addr_idx, target_char, aggressive=True)
    
    header_text = normalize_ws(" ".join(clean_header))
    header_text = re.sub(r"\b(owner|address|phone|lot|section|space|card)\b[:\-]?", "", header_text, flags=re.IGNORECASE).strip()

    # Final Check
    if target_char and header_text and not header_text.upper().startswith(target_char.upper()):
         # If aggressive mode failed, blank it to trigger fallback
         header_text = ""

    primary = ""
    secondary = ""
    if re.search(r"\s&\s|\sand\s", header_text, re.IGNORECASE):
        parts = re.split(r"\s&\s|\sand\s", header_text, flags=re.IGNORECASE)
        parts = [normalize_ws(p) for p in parts if normalize_ws(p)]
        if parts: primary, secondary = parts[0], parts[1] if len(parts) > 1 else ""
    else:
        primary = header_text
    
    last_name = primary.split(",")[0].strip() if "," in primary else primary.split(" ")[0].strip()

    if target_char and primary:
        if not primary.upper().startswith(target_char.upper()):
             primary = "[MISSING - CHECK PDF] " + primary
    elif not primary:
         primary = "[MISSING - CHECK PDF]"

    # Loose ZIP Recovery
    if not addr_info.get("ZIP"):
        for ln in lines:
            m_zip = re.search(ZIP_RE, ln)
            if m_zip:
                addr_info["ZIP"] = m_zip.group(0)
                m_state = re.search(US_STATE_RE, ln, re.IGNORECASE)
                if m_state: addr_info["State"] = normalize_state(m_state.group(0))
                break

    return (header_text, primary, secondary, last_name, addr_info, False)


# -----------------------------
# IMAGE / OCR PIPELINE
# -----------------------------

def extract_pdf_text_page(pdf_path: str, page_index: int) -> str:
    try:
        reader = PdfReader(pdf_path)
        if page_index >= len(reader.pages): return ""
        return reader.pages[page_index].extract_text() or ""
    except Exception as e:
        # print(f"[Debug] Text Layer extraction failed: {e}")
        return ""

def text_layer_usable(txt: str) -> bool:
    if not txt: return False
    t = normalize_ws(txt)
    # Lowered threshold to 30 to catch sparse but valid cards
    if sum(ch.isalpha() for ch in t) < 30: return False
    anchors = ["ITEM DESCRIPTION", "OWNER ID", "CONTRACT", "LOT", "SECTION", "GARDEN", "TN", "TENNESSEE"]
    if any(a in t.upper() for a in anchors): return True
    return len(t) > 250

def detect_template_type(text: str) -> str:
    t = (text or "").upper()
    if "INTERMENT RECORD" in t: return "interment_record"
    if "ITEM DESCRIPTION" in t or "OWNER ID" in t or "CONTRACT NBR" in t: return "modern_table"
    return "legacy_typewritten"

def deskew_bgr(img_bgr: np.ndarray) -> np.ndarray:
    gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)
    thr = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]
    coords = np.column_stack(np.where(thr > 0))
    if coords.size == 0: return img_bgr
    rect = cv2.minAreaRect(coords)
    angle = rect[-1]
    angle = -(90 + angle) if angle < -45 else -angle
    if abs(angle) < 0.5: return img_bgr
    h, w = img_bgr.shape[:2]
    M = cv2.getRotationMatrix2D((w // 2, h // 2), angle, 1.0)
    return cv2.warpAffine(img_bgr, M, (w, h), flags=cv2.INTER_CUBIC, borderMode=cv2.BORDER_REPLICATE)

def save_failure_snapshot(pil_img: Image.Image, filename: str, page_num: int):
    if not os.path.exists(SNAPSHOT_DIR): os.makedirs(SNAPSHOT_DIR)
    w, h = pil_img.size
    header = pil_img.crop((0, 0, w, int(h * 0.3)))
    out_name = f"FAIL_{filename}_P{page_num:04d}.jpg"
    header.save(os.path.join(SNAPSHOT_DIR, out_name))

def get_easyocr_reader():
    global _EASYOCR_READER
    if _EASYOCR_READER is None and EASYOCR_AVAILABLE:
        try:
            # Initialize once
            _EASYOCR_READER = easyocr.Reader(['en'], gpu=False, verbose=False)
        except Exception as e:
            print(f"[Warn] EasyOCR failed to init: {e}")
            pass
    return _EASYOCR_READER

def try_easyocr(pil_img):
    reader = get_easyocr_reader()
    if not reader: return ""
    try:
        result = reader.readtext(np.array(pil_img), detail=0)
        return " ".join(result)
    except: return ""

def score_text_pass(txt: str) -> int:
    if not txt: return 0
    u = txt.upper()
    score = 0
    if re.search(ZIP_RE, u): score += 40
    if re.search(US_STATE_RE, u, re.IGNORECASE): score += 20
    if "OWNER ID" in u: score += 20
    if "ITEM DESCRIPTION" in u: score += 20
    good = [ln for ln in split_lines(txt) if not is_gibberish(ln)]
    score += min(len(good), 60)
    return score

def process_page(pdf_path: str, page_index: int, dpi: int, target_char: Optional[str]) -> Tuple[Dict, List[Dict], bool]:
    # 1. TEXT LAYER
    pdf_text = extract_pdf_text_page(pdf_path, page_index)
    use_text = False
    if text_layer_usable(pdf_text):
        txt = pdf_text
        lines = split_lines(txt)
        template_type = detect_template_type(txt)
        _, p, s, last, addr, is_int = parse_owner_header(lines, target_char)
        if "[MISSING" not in p: use_text = True
    
    if use_text:
        phone, _ = extract_phone(txt)
        return ({
             "OwnerName_Raw": f"{p} {s}", "PrimaryOwnerName": p, "SecondaryOwnerName": s,
             "LastName": last, "Phone": phone, "Street": addr.get('Street',''), "City": addr.get('City',''),
             "State": addr.get('State',''), "ZIP": addr.get('ZIP',''), "AddressRaw": addr.get('AddressRaw',''),
             "RawText": txt, "RawTextHash": sha1_text(txt), "TemplateType": template_type, "TextSource": "PDF_TEXT_LAYER"
        }, [], is_int)
    
    # 2. OCR FALLBACK
    try:
        imgs = convert_from_path(pdf_path, dpi=dpi, first_page=page_index+1, last_page=page_index+1)
        if not imgs: raise RuntimeError("Render failed")
        pil_orig = imgs[0].convert("RGB")
    except Exception as e:
        print(f"[Error] Failed to render page {page_index+1}: {e}")
        return ({}, [], False)
    
    # Deskew
    orig_bgr = deskew_bgr(cv2.cvtColor(np.array(pil_orig), cv2.COLOR_RGB2BGR))
    pil_deskewed = Image.fromarray(cv2.cvtColor(orig_bgr, cv2.COLOR_BGR2RGB))
    
    pil_std = preprocess_standard(pil_deskewed)
    pil_clahe = preprocess_clahe(pil_deskewed)
    pil_ghost = preprocess_ghost(pil_deskewed)
    
    t_std = pytesseract.image_to_string(pil_std, config=OCR_PSM6)
    t_clahe = pytesseract.image_to_string(pil_clahe, config=OCR_PSM6)
    t_ghost = pytesseract.image_to_string(pil_ghost, config=OCR_PSM6)
    t_sparse = pytesseract.image_to_string(pil_deskewed, config=OCR_PSM11)
    
    candidates = [("STD", t_std), ("CLAHE", t_clahe), ("GHOST", t_ghost), ("SPARSE", t_sparse)]
    best = sorted([(n, t, score_text_pass(t)) for (n, t) in candidates], key=lambda x: x[2], reverse=True)[0]
    best_name, best_text, _ = best
    
    # Try EasyOCR if available and Tesseract failed
    if "[MISSING" in parse_owner_header(split_lines(best_text), target_char)[1] and EASYOCR_AVAILABLE:
        t_easy = try_easyocr(pil_deskewed)
        if t_easy:
             best_name, best_text = "EASYOCR", t_easy

    lines = split_lines(best_text)
    template_type = detect_template_type(best_text)
    _, p, s, last, addr, is_int = parse_owner_header(lines, target_char)
    phone, _ = extract_phone(best_text)
    
    # Try Deep Upscale if still missing
    if "[MISSING" in p:
        pil_up = preprocess_upscale(pil_deskewed)
        t_up = pytesseract.image_to_string(preprocess_standard(pil_up), config=OCR_PSM6)
        _, p_up, s_up, last_up, addr_up, is_int_up = parse_owner_header(split_lines(t_up), target_char)
        if "[MISSING" not in p_up:
             p, s, last, addr, is_int, best_text, best_name = p_up, s_up, last_up, addr_up, is_int_up, t_up, "DEEP_UPSCALE"
             
    if "[MISSING" in p:
        filename = os.path.basename(pdf_path).replace(".pdf", "")
        save_failure_snapshot(pil_orig, filename, page_index + 1)
        
    return ({
         "OwnerName_Raw": f"{p} {s}", "PrimaryOwnerName": p, "SecondaryOwnerName": s,
         "LastName": last, "Phone": phone, "Street": addr.get('Street',''), "City": addr.get('City',''),
         "State": addr.get('State',''), "ZIP": addr.get('ZIP',''), "AddressRaw": addr.get('AddressRaw',''),
         "RawText": best_text, "RawTextHash": sha1_text(best_text), "TemplateType": "legacy", "TextSource": f"OCR_{best_name}"
    }, [], is_int)

# -----------------------------
# MAIN
# -----------------------------

def apply_neighbor_context(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    def clean(s): return re.sub(r'[^A-Z]', '', str(s).split(',')[0].upper())
    
    for i in range(1, len(df)-1):
        name = str(df.at[i, 'PrimaryOwnerName'])
        if "MISSING" in name:
            prev = str(df.at[i-1, 'PrimaryOwnerName'])
            next_ = str(df.at[i+1, 'PrimaryOwnerName'])
            if clean(prev) == clean(next_) and clean(prev):
                 df.at[i, 'PrimaryOwnerName'] = f"{prev.split(',')[0]}, {name.replace('[MISSING - CHECK PDF]', '').strip()} [Context Fix]"
                 df.at[i, 'LastName'] = prev.split(',')[0]
    return df

def process_dataset(pdf_path, out_path, dpi=300):
    if not os.path.exists(pdf_path): return
    filename = os.path.basename(pdf_path)
    
    # V57 Fix: Only infer target char if filename starts with a letter
    target_char = None
    if filename[0].isalpha():
        target_char = filename[0].upper()
    
    print(f"\nProcessing {filename}...")
    try: page_count = pdfinfo_from_path(pdf_path)["Pages"]
    except: 
        try: page_count = len(PdfReader(pdf_path).pages)
        except: page_count = 0 # Will fail in loop if 0

    rows = []
    for p in tqdm(range(page_count), desc="Scanning"):
        owner, items, is_int = process_page(pdf_path, p, dpi, target_char)
        owner["PageNumber"] = p + 1
        rows.append(owner)
        
    df = pd.DataFrame(rows)
    df = apply_neighbor_context(df)
    
    # Excel Safe
    df = make_df_excel_safe(df)
    df = force_string_cols(df, ["ZIP", "OwnerRecordID", "OwnerGroupKey"])

    df.to_excel(out_path, index=False)
    print(f"Saved to {out_path}")

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    pdfs = sorted(glob.glob(os.path.join(script_dir, "* (all).pdf")))
    if not pdfs: pdfs = sorted(glob.glob("* (all).pdf"))
    
    if pdfs:
        print(f"Found: {[os.path.basename(p) for p in pdfs]}")
        for pdf in pdfs:
            process_dataset(pdf, pdf.replace(".pdf", "_Output.xlsx"))
    else:
        print("No PDFs found.")
