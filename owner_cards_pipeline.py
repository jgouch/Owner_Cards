#!/usr/bin/env python3
"""
Owner Card Pipeline (Local) — Excel-Safe + Excel-Length-Safe (Final Hardened v43)

- OCR scanned PDF owner cards
- Extract owner/name/address + item lines
- Best-effort strike-through detection
- Classify: Property vs Memorial vs Funeral Preneed vs At-Need Funeral
- Compute Prime / Mailing lists
- Export Excel workbook with multiple sheets

Usage:
  python3 owner_cards_pipeline.py
  (Automatically finds and processes any "X (all).pdf" in the folder)

V43 Improvements:
- TRUST BUT VERIFY: If the Text Layer (PyPDF2) fails to yield a valid name (starting with Target Letter),
  the script automatically REJECTS the text layer and forces an OCR Fallback.
  (Fixes the regression where garbage text layers were accepted over valid OCR).
- RETAINS: Text Layer wins for clean cards (fixing "ALLEN"), Grid Lock, Safety Net.
"""

import argparse
import hashlib
import glob
import os
import re
import sys
import unicodedata
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Any

import pandas as pd
from tqdm import tqdm

from pdf2image import convert_from_path, pdfinfo_from_path
import pytesseract
from pytesseract import Output

import cv2
import numpy as np
from PIL import Image
from PyPDF2 import PdfReader

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
    "WHITE BLUFF", "CENTERVILLE", "CHAPEL HILL"
]

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

CACHE_DIR = "_ocr_cache_v43"

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

OCR_PSM6 = "--oem 3 -l eng --psm 6"
OCR_PSM11 = "--oem 3 -l eng --psm 11"

# -----------------------------
# REGEX HELPERS
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
    return matches_any(line, RE_ADDR_BLOCK)

def split_lines(raw_text: str) -> List[str]:
    raw_text = (raw_text or "").replace("\r\n", "\n").replace("\r", "\n")
    lines = [normalize_ws(x) for x in raw_text.split("\n")]
    return [x for x in lines if x]

def extract_phone(text: str) -> Tuple[str, bool]:
    phones = []
    m_full = re.findall(r"(?:\(?(\d{3})\)?[\s\-\./]?)?(\d{3})[\s\-\./]?(\d{4})", text or "")
    for area, pre, suf in m_full:
        if not area: area = "615"
        if len(area) == 3 and len(pre) == 3 and len(suf) == 4:
            phones.append(f"({area}) {pre}-{suf}")
    
    # Also check for 7-digit without area code
    m7 = re.search(r"\b(\d{3})[\s\-\./]?(\d{4})\b", text or "")
    if m7 and not phones:
         return (f"{m7.group(1)}-{m7.group(2)}", False)

    return (phones[0] if phones else "", True if phones else False)

def excel_safe_text(v):
    if v is None: return ""
    try:
        if pd.isna(v): return ""
    except Exception: pass
    if isinstance(v, (int, float, np.integer, np.floating)):
        try:
            if np.isinf(v): return ""
        except Exception: pass
        return v
    s = str(v)
    try: s = unicodedata.normalize("NFKD", s)
    except Exception: pass
    s = re.sub(r"[^\x09\x0A\x0D\x20-\uD7FF\uE000-\uFFFD]", "", s)
    s = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", s)
    s_l = s.lstrip()
    if s_l.startswith(("=", "+", "-", "@")): s = "'" + s
    if len(s) > EXCEL_SAFE_MAX: s = s[: (EXCEL_SAFE_MAX - len(TRUNC_SUFFIX))] + TRUNC_SUFFIX
    return s

def make_df_excel_safe(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    for c in df2.columns: df2[c] = df2[c].map(excel_safe_text)
    return df2

def choose_excel_engine() -> str:
    try: import xlsxwriter; return "xlsxwriter"
    except Exception: return "openpyxl"

def force_string_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = df[c].apply(lambda x: "" if x is None or (isinstance(x, float) and np.isnan(x)) else str(x))
    return df

# -----------------------------
# IMAGE STRATEGIES
# -----------------------------

def deskew_bgr(img_bgr: np.ndarray) -> np.ndarray:
    gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)
    thr = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]
    coords = np.column_stack(np.where(thr > 0))
    if coords.size == 0: return img_bgr
    rect = cv2.minAreaRect(coords)
    angle = rect[-1]
    angle = -(90 + angle) if angle < -45 else -angle
    h, w = img_bgr.shape[:2]
    M = cv2.getRotationMatrix2D((w // 2, h // 2), angle, 1.0)
    return cv2.warpAffine(img_bgr, M, (w, h), flags=cv2.INTER_CUBIC, borderMode=cv2.BORDER_REPLICATE)

def ensure_dark_text_on_white(bin_img: np.ndarray) -> np.ndarray:
    return 255 - bin_img if np.mean(bin_img) < 127 else bin_img

def preprocess_standard(pil_img: Image.Image) -> Image.Image:
    img_np = cv2.cvtColor(np.array(pil_img), cv2.COLOR_RGB2BGR)
    gray = cv2.cvtColor(img_np, cv2.COLOR_BGR2GRAY)
    gray = cv2.bilateralFilter(gray, 9, 75, 75)
    binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]
    binary = ensure_dark_text_on_white(binary)
    return Image.fromarray(binary)

def preprocess_clahe(pil_img: Image.Image) -> Image.Image:
    img_np = cv2.cvtColor(np.array(pil_img), cv2.COLOR_RGB2BGR)
    lab = cv2.cvtColor(img_np, cv2.COLOR_BGR2LAB)
    l, a, b = cv2.split(lab)
    clahe = cv2.createCLAHE(clipLimit=3.0, tileGridSize=(8, 8))
    cl = clahe.apply(l)
    limg = cv2.merge((cl, a, b))
    enhanced = cv2.cvtColor(cv2.cvtColor(limg, cv2.COLOR_LAB2BGR), cv2.COLOR_BGR2GRAY)
    bin_img = cv2.adaptiveThreshold(enhanced, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 35, 11)
    bin_img = ensure_dark_text_on_white(bin_img)
    return Image.fromarray(bin_img)

def preprocess_ghost(pil_img: Image.Image) -> Image.Image:
    img_bgr = cv2.cvtColor(np.array(pil_img), cv2.COLOR_RGB2BGR)
    gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)
    clahe = cv2.createCLAHE(clipLimit=2.5, tileGridSize=(8, 8))
    gray = clahe.apply(gray)
    bin_img = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 35, 11)
    bin_img = ensure_dark_text_on_white(bin_img)
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (2, 2))
    bin_img = cv2.dilate(bin_img, kernel, iterations=1)
    return Image.fromarray(bin_img)

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

# -----------------------------
# TEXT LAYER (Adobe OCR)
# -----------------------------

def extract_pdf_text_page(pdf_path: str, page_index: int) -> str:
    try:
        reader = PdfReader(pdf_path)
        if page_index >= len(reader.pages): return ""
        return reader.pages[page_index].extract_text() or ""
    except Exception: return ""

def text_layer_usable(txt: str) -> bool:
    if not txt: return False
    t = normalize_ws(txt)
    if sum(ch.isalpha() for ch in t) < 40: return False
    anchors = ["ITEM DESCRIPTION", "OWNER ID", "CONTRACT", "LOT", "SECTION", "GARDEN", "TN", "TENNESSEE"]
    if any(a in t.upper() for a in anchors): return True
    return len(t) > 250

def detect_template_type(text: str) -> str:
    t = (text or "").upper()
    if "INTERMENT RECORD" in t: return "interment_record"
    if "ITEM DESCRIPTION" in t or "OWNER ID" in t or "CONTRACT NBR" in t: return "modern_table"
    return "legacy_typewritten"

# -----------------------------
# HEADER PARSING / CLEANING
# -----------------------------

INITIAL_DIGIT_MAP = {"8": "B", "0": "O", "1": "I", "2": "Z", "5": "S", "6": "G", "9": "P"}

def fix_digit_initials_in_name(line: str) -> str:
    if not line: return line
    tokens = line.split()
    out = []
    for i, tok in enumerate(tokens):
        core = tok
        trail = ""
        while core and core[-1] in ".,;:":
            trail = core[-1] + trail
            core = core[:-1]
        if len(core) == 1 and core.isdigit() and core in INITIAL_DIGIT_MAP:
            if i > 0 and re.search(r"[A-Za-z]", tokens[i - 1]):
                out.append(INITIAL_DIGIT_MAP[core] + (trail if trail else "."))
                continue
        out.append(tok)
    return " ".join(out)

def fix_leading_digit_as_letter(line: str, target_char: Optional[str]) -> str:
    if not line: return line
    if not line[0].isdigit(): return line
    if line[0] in INITIAL_DIGIT_MAP and len(line) > 2 and line[1].isalpha():
        repl = INITIAL_DIGIT_MAP[line[0]]
        if (not target_char) or (repl.upper() == target_char.upper()):
            if "," in line[:20] or line[:20].isupper():
                return repl + line[1:]
    return line

def repair_word_start(word: str, target_char: str) -> str:
    if not word or not target_char: return word
    t = target_char.upper()
    w = word
    confusables = {
        'B': ['P', 'D', 'R', 'E', '8', '6', '3', '>', 'e', 'h', '|3'],
        'A': ['4', '@', '^', 'R'],
        'D': ['0', 'O', 'Q'],
        'G': ['C', '6'],
        'S': ['5', '$'],
        'Z': ['2']
    }
    if t in confusables:
        first_char = w[0].upper()
        if first_char in confusables[t]:
            if w.startswith('>e') or w.startswith('>E'): return t + w[2:]
            return t + w[1:]
    return w

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
    if matches_any(line, [re.compile(p, re.IGNORECASE) for p in [r"\bSermon\b", r"\bChapel\b", r"\bGarden\b", r"\bSection\b", r"\bMount\b", r"\bMt\.?\b"]]): return ""
    if matches_any(line, [re.compile(p, re.IGNORECASE) for p in [r"\bSex\b", r"\bMale\b", r"\bFemale\b", r"\bGrave\b"]]): return ""

    line_upper = line.upper()
    for city in CITY_BLOCKLIST:
        idx = line_upper.find(city)
        if idx != -1:
            line = line[:idx]
            line_upper = line.upper()
    if "#" in line: line = line.split("#")[0]
    
    m_kw = re.search(r"\b(road|rd|street|st|avenue|ave|drive|dr|lane|ln|court|ct|blvd|boulevard|pkwy|parkway|hwy|highway|trl|trail|cir|circle|pl|place|po\s*box|box)\b", line, re.IGNORECASE)
    if m_kw: line = line[:m_kw.start()]
    
    m_addr_start = re.search(r"\b\d+\s+[A-Za-z]", line)
    if m_addr_start: line = line[:m_addr_start.start()]

    cleaned = line
    for pat in RE_NAME_NOISE: cleaned = pat.sub("", cleaned)
    cleaned = re.sub(r"[^a-zA-Z\s&\.,\-']", "", cleaned)
    cleaned = normalize_ws(cleaned)

    if aggressive and target_char:
        tc = target_char.upper()
        words = cleaned.split()
        cleaned_words = []
        found_start = False
        for i, w in enumerate(words):
            w_repaired = repair_word_start(w, tc)
            w_clean = re.sub(r"^[^a-zA-Z]+", "", w_repaired)
            if w_clean.upper().startswith(tc):
                cleaned_words.append(w_repaired)
                cleaned_words.extend(words[i+1:])
                found_start = True
                break
            if tc in w.upper():
                match_idx = w.upper().find(tc)
                valid_part = w[match_idx:]
                if len(valid_part) > 2:
                    cleaned_words.append(valid_part)
                    cleaned_words.extend(words[i+1:])
                    found_start = True
                    break
        if found_start: cleaned = " ".join(cleaned_words)
        else:
             if is_gibberish(cleaned): return ""
    
    return cleaned

def clean_address_line(line: str) -> str:
    if not line: return ""
    m_owner = re.search(r"\bowner\s*(since|id)", line, re.IGNORECASE)
    if m_owner: line = line[:m_owner.start()]
    m_date = re.search(r"\b\d{1,2}/\d{1,2}/\d{2,4}", line)
    if m_date: line = line[:m_date.start()]
    return normalize_ws(line)

def parse_inline_address_line(line: str) -> Optional[Dict[str, str]]:
    if not line: return None
    z, st = extract_zip_state(line)
    if not (z and st): return None
    if not re.search(STREET_START_RE, line): return None
    state_match = re.search(US_STATE_RE, line, re.IGNORECASE)
    if not state_match: return None
    before_state = normalize_ws(line[:state_match.start()].rstrip(",")).strip()
    if not before_state: return None
    if "," in before_state:
        street_part, city_part = before_state.rsplit(",", 1)
        street = normalize_ws(street_part)
        city = normalize_ws(city_part)
    else:
        addr_match = re.match(r"^(\d+\s+.+?\b(?:road|rd|street|st|avenue|ave|drive|dr|lane|ln|court|ct|blvd|boulevard|pkwy|parkway|hwy|highway|trl|trail|cir|circle|pl|place|po\s*box|box)\.?)\s+(.*)$", before_state, re.IGNORECASE)
        if addr_match:
            street = normalize_ws(addr_match.group(1))
            city = normalize_ws(addr_match.group(2))
        else:
            street = before_state
            city = ""
    return {"Street": street, "City": city, "State": st, "ZIP": z, "CityStateZip": line}

def parse_best_address(lines: List[str]) -> Dict:
    candidates = []
    for i, line in enumerate(lines):
        z, st = extract_zip_state(line)
        if z and st:
            inline = parse_inline_address_line(line)
            if inline:
                candidates.append({"Index": i, "Street": inline["Street"], "CityStateZip": inline["CityStateZip"], "State": inline["State"], "ZIP": inline["ZIP"], "Score": 90 if inline["City"] else 70})
                continue
            prev_idx = i - 1
            street_candidate = lines[prev_idx] if prev_idx >= 0 else ""
            street_candidate = clean_address_line(street_candidate)
            street_candidate = re.sub(r"^[\W_]+", "", street_candidate)
            score = 50
            if street_candidate and re.search(STREET_START_RE, street_candidate): score += 40
            if re.search(r"^[A-Z][a-z]+,\s+[A-Z]", street_candidate): score -= 100 
            elif "," in street_candidate: score -= 30
            if len(street_candidate) < 5: score -= 20
            candidates.append({"Index": prev_idx if prev_idx >= 0 else i, "Street": street_candidate, "CityStateZip": line, "State": st, "ZIP": z, "Score": score})
    if not candidates:
        for i, line in enumerate(lines):
            if looks_like_address_line(line): return {"Index": i, "Street": "", "CityStateZip": line, "State": "", "ZIP": "", "Score": 10, "AddressRaw": line}
        return {"Index": None, "Street": "", "CityStateZip": "", "State": "", "ZIP": "", "Score": 0, "AddressRaw": ""}
    best = sorted(candidates, key=lambda x: x["Score"], reverse=True)[0]
    street = best["Street"] if best["Score"] > 0 else ""
    city = ""
    if best["State"]:
        m = re.search(US_STATE_RE, best["CityStateZip"], re.IGNORECASE)
        if m:
            city_part = best["CityStateZip"][:m.start()]
            city = normalize_ws(city_part).replace(",", "")
    return {"Index": best["Index"], "Street": street, "City": city, "State": best["State"], "ZIP": best["ZIP"], "AddressRaw": f"{street} | {best['CityStateZip']}"}

def get_header_candidate(lines: List[str], addr_idx: Optional[int], target_char: Optional[str], aggressive: bool) -> List[str]:
    clean_header = []
    top = lines[:40]
    search_lines = top[:addr_idx] if (addr_idx is not None and addr_idx > 0) else top[:8]
    has_addr_context = addr_idx is not None and addr_idx > 0
    search_iter = range(len(search_lines) - 1, -1, -1) if has_addr_context else range(len(search_lines))
    for i in search_iter:
        ln = search_lines[i]
        if not ln.strip(): continue
        if matches_any(ln, RE_NAME_BLACKLIST): continue
        ln_clean = clean_name_line(ln, target_char, aggressive)
        if not ln_clean or is_gibberish(ln_clean): continue
        if has_addr_context: clean_header.insert(0, ln_clean)
        else: clean_header.append(ln_clean)
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
    
    # PASS 2: NUCLEAR
    if not is_valid:
        clean_header = get_header_candidate(lines, addr_idx, target_char, aggressive=True)
        if not clean_header:
             for ln in top[:15]: 
                if matches_any(ln, RE_NAME_BLACKLIST): continue
                if re.search(r"^[A-Z][a-z]+,\s+[A-Z][a-z]+", ln):
                    fallback = clean_name_line(ln, target_char, aggressive=True)
                    if fallback and not is_gibberish(fallback):
                        clean_header.append(fallback)
                        break
             if not clean_header:
                for i, ln in enumerate(top):
                    if re.search(r"\b(owner|sold\s+to|transfer|given\s+to|deeded\s+to)\b", ln, re.IGNORECASE):
                        if i + 1 < len(top):
                            fallback = clean_name_line(top[i+1], target_char, aggressive=True)
                            if fallback and not is_gibberish(fallback):
                                clean_header.append(fallback)
                                break
    
    header_text = normalize_ws(" ".join(clean_header))
    header_text = re.sub(r"\b(owner|address|phone|lot|section|space|card)\b[:\-]?", "", header_text, flags=re.IGNORECASE).strip()

    # V43 FIX: Verify final string before accepting it
    # If we are in aggressive mode and it still doesn't start with target, try one last aggressive clean on the combined string
    if target_char and header_text and not header_text.upper().startswith(target_char.upper()):
         header_text = clean_name_line(header_text, target_char, aggressive=True)

    primary = ""
    secondary = ""
    if re.search(r"\s&\s|\sand\s", header_text, re.IGNORECASE):
        parts = re.split(r"\s&\s|\sand\s", header_text, flags=re.IGNORECASE)
        parts = [normalize_ws(p) for p in parts if normalize_ws(p)]
        if parts:
            primary = parts[0]
            secondary = parts[1] if len(parts) > 1 else ""
    else:
        primary = header_text
    
    if "," in primary: last_name = primary.split(",")[0].strip()
    elif " " in primary: last_name = primary.split(" ")[0].strip()
    else: last_name = primary

    # V43 FIX: If still invalid after all that, flag it so caller knows to fallback
    if target_char and primary:
        if not primary.upper().startswith(target_char.upper()):
             primary = "[MISSING - CHECK PDF] " + primary

    return (header_text, primary, secondary, last_name, addr_info, False)

# -----------------------------
# SCORING (fallback)
# -----------------------------

def score_text_pass(txt: str) -> int:
    if not txt: return 0
    u = txt.upper()
    score = 0
    if re.search(ZIP_RE, u): score += 40
    if re.search(US_STATE_RE, u, re.IGNORECASE): score += 20
    if "OWNER ID" in u: score += 20
    if "ITEM DESCRIPTION" in u: score += 20
    if re.search(r"\b(GARDEN|SECTION|LOT|SPACE|CRYPT|MAUS|MAUSOLEUM)\b", u): score += 10
    good_lines = [ln for ln in split_lines(txt) if not is_gibberish(ln)]
    score += min(len(good_lines), 60)
    return score

# -----------------------------
# PAGE PROCESSOR (V43 Hybrid)
# -----------------------------

def process_page(pdf_path: str, page_index: int, dpi: int, target_char: Optional[str]) -> Tuple[Dict, List[Dict], bool]:
    # 1. TEXT LAYER FIRST (TRUST BUT VERIFY)
    pdf_text = extract_pdf_text_page(pdf_path, page_index)
    use_text_layer = False
    
    if text_layer_usable(pdf_text):
        txt = pdf_text
        lines = split_lines(txt)
        template_type = detect_template_type(txt)
        _, p, s, last, addr, is_interment = parse_owner_header(lines, target_char)
        
        # V43 VALIDATION: Does the extracted name look valid?
        # If it's flagged [MISSING], the text layer failed us (it contains garbage or just address).
        if "[MISSING" not in p:
             use_text_layer = True
    
    if use_text_layer:
        phone, phone_has_area = extract_phone(txt)
        items = [] if is_interment else parse_items_from_text(lines, template_type)
        return ({
            "OwnerName_Raw": normalize_ws(f"{p} {s}"),
            "PrimaryOwnerName": p,
            "SecondaryOwnerName": s,
            "LastName": last,
            "Phone": phone,
            "PhoneHasArea": phone_has_area,
            "Street": addr.get("Street", ""),
            "City": addr.get("City", ""),
            "State": addr.get("State", ""),
            "ZIP": addr.get("ZIP", ""),
            "AddressRaw": addr.get("AddressRaw", ""),
            "RawText": txt,
            "RawTextHash": sha1_text(txt),
            "TemplateType": template_type,
            "TextSource": "PDF_TEXT_LAYER",
        }, items, is_interment)

    # 2. FALLBACK TO OCR (If Text Layer was unusable OR invalid)
    imgs = convert_from_path(pdf_path, dpi=dpi, first_page=page_index + 1, last_page=page_index + 1)
    if not imgs: raise RuntimeError(f"Failed to render page {page_index+1}")
    pil_original = imgs[0].convert("RGB")
    
    orig_bgr = cv2.cvtColor(np.array(pil_original), cv2.COLOR_RGB2BGR)
    orig_bgr = deskew_bgr(orig_bgr)
    pil_original = Image.fromarray(cv2.cvtColor(orig_bgr, cv2.COLOR_BGR2RGB))
    
    pil_std = preprocess_standard(pil_original)
    pil_clahe = preprocess_clahe(pil_original)
    pil_ghost = preprocess_ghost(pil_original)
    
    t_std = pytesseract.image_to_string(pil_std, config=OCR_PSM6)
    t_clahe = pytesseract.image_to_string(pil_clahe, config=OCR_PSM6)
    t_ghost = pytesseract.image_to_string(pil_ghost, config=OCR_PSM6)
    t_sparse = pytesseract.image_to_string(pil_original, config=OCR_PSM11)
    
    candidates = [("STD", t_std), ("CLAHE", t_clahe), ("GHOST", t_ghost), ("SPARSE", t_sparse)]
    best_name, best_text, best_score = sorted([(n, t, score_text_pass(t)) for (n, t) in candidates], key=lambda x: x[2], reverse=True)[0]
    
    lines_best = split_lines(best_text)
    template_type = detect_template_type(best_text)
    _, p, s, last, addr, is_interment = parse_owner_header(lines_best, target_char)
    phone, phone_has_area = extract_phone(best_text)
    
    d_std = pytesseract.image_to_data(pil_std, config=OCR_PSM6, output_type=Output.DICT)
    d_clahe = pytesseract.image_to_data(pil_clahe, config=OCR_PSM6, output_type=Output.DICT)
    
    raw_lines_a = group_text_lines_from_ocr(d_std)
    raw_lines_b = group_text_lines_from_ocr(d_clahe)
    strike_segs = detect_horizontal_strikelines(orig_bgr)
    
    all_items = []
    seen = set()
    
    def add_lines(lines_ocr):
        for ln_obj in lines_ocr:
            txt = ln_obj["text"]
            if not txt: continue
            x1, y1, x2, y2 = ln_obj["bbox"]
            key = sha1_text(f"{normalize_ws(txt)}|{round(x1,-1)}|{round(y1,-1)}|{round(x2,-1)}|{round(y2,-1)}")
            if key in seen: continue
            struck = line_is_struck(ln_obj["bbox"], strike_segs)
            all_items.append(item_dict_from_line(txt, struck=struck))
            seen.add(key)
            
    add_lines(raw_lines_b)
    add_lines(raw_lines_a)
    
    combined_raw = "\n".join([t_std, t_clahe, t_ghost, t_sparse])
    
    return ({
        "OwnerName_Raw": normalize_ws(f"{p} {s}"),
        "PrimaryOwnerName": p,
        "SecondaryOwnerName": s,
        "LastName": last,
        "Phone": phone,
        "PhoneHasArea": phone_has_area,
        "Street": addr.get("Street", ""),
        "City": addr.get("City", ""),
        "State": addr.get("State", ""),
        "ZIP": addr.get("ZIP", ""),
        "AddressRaw": addr.get("AddressRaw", ""),
        "RawText": combined_raw,
        "RawTextHash": sha1_text(combined_raw),
        "TemplateType": template_type,
        "TextSource": f"OCR_FALLBACK_{best_name}_S{best_score}",
    }, all_items, is_interment)

# -----------------------------
# NEIGHBOR & DATASET LOGIC
# -----------------------------

def apply_neighbor_context(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for i in range(1, len(df) - 1):
        raw_name = df.at[i, 'PrimaryOwnerName']
        name = "" if pd.isna(raw_name) else str(raw_name)
        if "[MISSING" in name or (len(name) > 0 and not name[0].isalpha()):
            prev_name = str(df.at[i-1, 'PrimaryOwnerName'])
            next_name = str(df.at[i+1, 'PrimaryOwnerName'])
            if "," in prev_name and "," in next_name:
                prev_last = prev_name.split(",")[0].strip().upper()
                next_last = next_name.split(",")[0].strip().upper()
                if prev_last == next_last and prev_last:
                    garbage = name.replace("[MISSING - CHECK PDF]", "").strip()
                    new_name = f"{prev_last}, {garbage} [Context Fix]"
                    df.at[i, 'PrimaryOwnerName'] = new_name
                    df.at[i, 'LastName'] = prev_last
    return df

def process_dataset(pdf_path, out_path, dpi=300):
    if not os.path.exists(pdf_path):
        print(f"Error: {pdf_path} not found.")
        return

    target_char = None
    filename = os.path.basename(pdf_path)
    if filename and filename[0].isalpha():
        target_char = filename[0].upper()
        record_prefix = target_char
    else:
        record_prefix = "A" 
    
    print(f"\n[Info] Processing '{filename}' | Target: '{target_char}' | Prefix: {record_prefix}")

    try:
        info = pdfinfo_from_path(pdf_path)
        page_count = info["Pages"]
        print(f"[Info] Quick page count: {page_count}")
    except Exception:
        print("[Warn] pdfinfo failed, falling back to render...")
        thumbs = convert_from_path(pdf_path, dpi=50)
        page_count = len(thumbs)

    owners_rows = []
    items_rows = []
    interment_rows = [] 

    for p in tqdm(range(page_count), desc=f"Scanning {filename}", unit="page"):
        owner_data, items_data, is_interment = process_page(pdf_path, p, dpi, target_char)
        
        rec_id = f"{record_prefix}-P{p+1:04d}"
        owner_data["OwnerRecordID"] = rec_id
        owner_data["SourceFile"] = filename
        owner_data["PageNumber"] = p + 1
        
        if is_interment:
            interment_rows.append(owner_data)
        else:
            parts = [
                safe_upper(owner_data.get("PrimaryOwnerName", "")),
                safe_upper(owner_data.get("SecondaryOwnerName", "")),
                safe_upper(owner_data.get("Street", "")),
                (owner_data.get("ZIP", "") or "")
            ]
            owner_data["OwnerGroupKey"] = sha1_text("|".join(parts))[:12]
            owners_rows.append(owner_data)
            
            for it in items_data:
                it["OwnerRecordID"] = rec_id
                it["SourceFile"] = filename
                it["Page"] = p + 1
                items_rows.append(it)

    owners_df = pd.DataFrame(owners_rows)
    items_df = pd.DataFrame(items_rows)
    interment_df = pd.DataFrame(interment_rows) 

    owners_df = apply_neighbor_context(owners_df)
    owners_df = force_string_cols(owners_df, ["ZIP", "OwnerRecordID", "OwnerGroupKey"])
    if not interment_df.empty:
        interment_df = force_string_cols(interment_df, ["ZIP", "OwnerRecordID"])

    dup = owners_df.groupby("RawTextHash").size().reset_index(name="Count")
    dup = dup[dup["Count"] > 1]
    possible_dups = owners_df.merge(dup, on="RawTextHash", how="inner").sort_values(["RawTextHash", "PageNumber"])

    inc = items_df[items_df.get("Include", False) == True].copy() if not items_df.empty else pd.DataFrame()

    def agg_owner(group: pd.DataFrame) -> pd.Series:
        has_property = bool(group["IsProperty"].any())
        has_memorial = bool(group["IsMemorial"].any())
        has_pn = bool(group["IsFuneralPreneed"].any())
        has_an = bool(group["IsAtNeedFuneral"].any())
        
        memorial_lines = group[group["IsMemorial"] == True]["LineText"].tolist()
        pn_lines = group[group["IsFuneralPreneed"] == True]["LineText"].tolist()
        an_lines = group[group["IsAtNeedFuneral"] == True]["LineText"].tolist()
        property_lines = group[group["IsProperty"] == True]["LineText"].tolist()

        likely_burials = compute_likely_burials(group.to_dict("records"))
        
        matching_owners = owners_df[owners_df["OwnerRecordID"] == group.name]
        total_owners = total_owners_on_file(
            matching_owners.iloc[0]["PrimaryOwnerName"],
            matching_owners.iloc[0]["SecondaryOwnerName"]
        ) if not matching_owners.empty else 1

        living_exists = True if int(likely_burials) < int(total_owners) else False

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

    if not inc.empty:
        owner_flags = inc.groupby("OwnerRecordID").apply(agg_owner).reset_index()
    else:
        owner_flags = pd.DataFrame(columns=["OwnerRecordID"])

    owners_master = owners_df.merge(owner_flags, on="OwnerRecordID", how="left")
    
    defaults = {
        "HasProperty": False, "HasMemorial": False, "HasAtNeedFuneral": False,
        "HasFuneralPreneedPlanStatus": "FALSE", "LikelyBurials": 0, "TotalOwnersOnFile": 1,
        "LivingOwnerExists": True, "NeedsMemorial": False, "NeedsPNFuneral": False,
        "SpacesOnly_PRIME": False, "SurvivorSpouse_Opportunity": False,
        "MemorialEvidence": "", "PNFuneralEvidence": "", "AtNeedEvidence": "", "PropertyEvidence": "",
    }
    for col, default in defaults.items():
        if col in owners_master.columns: owners_master[col] = owners_master[col].fillna(default)

    if "ZIP" in owners_master.columns:
        owners_master["ZIP"] = owners_master["ZIP"].astype(str)

    list_memorial = owners_master[(owners_master["HasProperty"] == True) & (owners_master["HasMemorial"] != True) & (owners_master["LivingOwnerExists"] == True)].copy()
    list_pn = owners_master[(owners_master["HasProperty"] == True) & (owners_master["HasFuneralPreneedPlanStatus"].isin(["FALSE", "PARTIAL"])) & (owners_master["LivingOwnerExists"] == True)].copy()
    list_prime = owners_master[owners_master["SpacesOnly_PRIME"] == True].copy()
    list_survivor = owners_master[owners_master["SurvivorSpouse_Opportunity"] == True].copy()

    dup_count = len(possible_dups)

    stats = pd.DataFrame([{
        "GeneratedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "SourceFile": filename,
        "PagesDetected": page_count, "OwnerRecords": len(owners_master), 
        "IntermentRecordsFound": len(interment_df), 
        "PossibleDuplicateScans": dup_count, 
        "Owners_HasProperty": int((owners_master["HasProperty"] == True).sum()),
        "Owners_HasMemorial": int((owners_master["HasMemorial"] == True).sum()),
        "Owners_PN_TRUE": int((owners_master["HasFuneralPreneedPlanStatus"] == "TRUE").sum()),
        "Owners_PN_PARTIAL": int((owners_master["HasFuneralPreneedPlanStatus"] == "PARTIAL").sum()),
        "Owners_PN_FALSE": int((owners_master["HasFuneralPreneedPlanStatus"] == "FALSE").sum()),
        "LIST_Memorial_Letter": len(list_memorial), "LIST_PN_Funeral_Letter": len(list_pn),
        "LIST_SpacesOnly_PRIME": len(list_prime), "LIST_SurvivorSpouse": len(list_survivor),
    }])

    owners_master_safe = make_df_excel_safe(owners_master)
    items_df_safe = make_df_excel_safe(items_df) if not items_df.empty else pd.DataFrame()
    list_memorial_safe = make_df_excel_safe(list_memorial)
    list_pn_safe = make_df_excel_safe(list_pn)
    list_prime_safe = make_df_excel_safe(list_prime)
    list_survivor_safe = make_df_excel_safe(list_survivor)
    possible_dups_safe = make_df_excel_safe(possible_dups) if not possible_dups.empty else pd.DataFrame()
    stats_safe = make_df_excel_safe(stats)
    interment_safe = make_df_excel_safe(interment_df) if not interment_df.empty else pd.DataFrame()

    tmp_path = out_path + ".tmp.xlsx"
    engine = choose_excel_engine()
    print(f"Writing Excel to: {out_path} ...")

    with pd.ExcelWriter(tmp_path, engine=engine) as xw:
        owners_master_safe.to_excel(xw, index=False, sheet_name="Owners_Master")
        if not items_df_safe.empty:
            items_df_safe.to_excel(xw, index=False, sheet_name="OwnerItems_Normalized")
        list_memorial_safe.to_excel(xw, index=False, sheet_name="LIST_Memorial_Letter")
        list_pn_safe.to_excel(xw, index=False, sheet_name="LIST_PN_Funeral_Letter")
        list_prime_safe.to_excel(xw, index=False, sheet_name="LIST_SpacesOnly_PRIME")
        list_survivor_safe.to_excel(xw, index=False, sheet_name="LIST_SurvivorSpouse_Opp")
        if not possible_dups_safe.empty:
            possible_dups_safe.to_excel(xw, index=False, sheet_name="PossibleDuplicateScans")
        stats_safe.to_excel(xw, index=False, sheet_name="Stats")
        if not interment_safe.empty:
            interment_safe.to_excel(xw, index=False, sheet_name="LIST_Refile_IntermentRecords")

    try: os.replace(tmp_path, out_path)
    except PermissionError:
        print(f"\n❌ ERROR: Could not overwrite '{out_path}'. It may be open in Excel.")

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    search_pattern = os.path.join(script_dir, "* (all).pdf")
    pdf_files = sorted(glob.glob(search_pattern))
    
    if not pdf_files:
        print(f"❌ No files matching pattern 'X (all).pdf' found in: {script_dir}")
        cwd_files = sorted(glob.glob("* (all).pdf"))
        if cwd_files:
            print(f"⚠️ Found {len(cwd_files)} files in current working directory. Processing those instead.")
            pdf_files = cwd_files
        else:
            return

    print(f"✅ Found {len(pdf_files)} PDF(s) to process: {[os.path.basename(f) for f in pdf_files]}")

    for pdf_path in pdf_files:
        filename = os.path.basename(pdf_path)
        letter = filename.split(' ')[0] 
        output_dir = os.path.dirname(pdf_path)
        out_path = os.path.join(output_dir, f"OwnerCards_{letter}_Output.xlsx")
        process_dataset(pdf_path, out_path, dpi=300)
    
    print("\nALL FILES PROCESSED SUCCESSFULLY ✅")

if __name__ == "__main__":
    main()
