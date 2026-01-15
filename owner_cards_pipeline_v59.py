#!/usr/bin/env python3
"""Owner Card Pipeline — v59 (Py3.14-safe + Accuracy fixes)

This version implements the issues you identified:
1) Strikethrough detection parity: when using PDF text layer, we now run a quick strike-line scan.
   - If strike lines are detected on the page, we OCR the page (data mode) to build item lines with struck flags.
   - Owner/header/address still come from the text layer (fast + accurate), but item inclusion honors strike-through.
2) Single-line address support: parse_best_address now handles inline "street, city ST ZIP" (same line).
3) Phone extensions: PHONE_PATTERN now matches optional extensions (ext/x), and we strip extensions for normalization.

Also includes the Py3.14 regex fix (hyphen placed at end in character class).


Merged from v57 + selected v58 refinements:
- Visual Sniper snapshot default reason set to NAME_MISSING and wording updated.
- Minor docstring clarifications for salvage/OCR helpers.

Run:
  python3 owner_cards_pipeline_v59.py
  or
  python3 owner_cards_pipeline_v59.py --pdf "B (all).pdf" --out "OwnerCards_B_Output.xlsx"

Deps (Mac):
  brew install poppler tesseract
  pip install pandas tqdm pdf2image pytesseract pillow opencv-python PyPDF2 openpyxl xlsxwriter
"""

import argparse
import glob
import hashlib
import os
import re
import unicodedata
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Optional

import numpy as np
import pandas as pd
from tqdm import tqdm

from PyPDF2 import PdfReader
from pdf2image import convert_from_path, pdfinfo_from_path

import cv2
from PIL import Image
import pytesseract
from pytesseract import Output



# -----------------------------
# VISUAL SNIPER HELPERS
# -----------------------------
FAILED_SNAPSHOT_DIR = "_Failed_Snapshots"


def _ensure_dir(p: str):
    try:
        os.makedirs(p, exist_ok=True)
    except Exception:
        pass


def save_failure_snapshot(pil_img: Image.Image, source_pdf: str, page_num: int, reason: str = "NAME_MISSING") -> str:
    """Save a quick header crop (top ~32%) for rapid manual verification."""
    _ensure_dir(FAILED_SNAPSHOT_DIR)
    w, h = pil_img.size
    crop = pil_img.crop((0, 0, w, int(h * 0.32)))
    base = os.path.splitext(os.path.basename(source_pdf))[0]
    safe_reason = re.sub(r"[^A-Za-z0-9_]+", "_", reason)[:40]
    out_name = f"FAIL_{base}_P{page_num:04d}_{safe_reason}.jpg"
    out_path = os.path.join(FAILED_SNAPSHOT_DIR, out_name)
    try:
        crop.save(out_path, quality=92)
    except Exception:
        try:
            crop.save(out_path)
        except Exception:
            return ""
    return out_path


# -----------------------------
# AUTO-RUN HELPERS (drag/drop friendly)
# -----------------------------
def _auto_pick_pdf(script_dir: str) -> str:
    env_pdf = os.getenv('OWNERCARDS_PDF', '').strip()
    if env_pdf and os.path.exists(os.path.expanduser(env_pdf)):
        return os.path.expanduser(env_pdf)
    cwd_pdfs = sorted(glob.glob(os.path.join(os.getcwd(), '*.pdf')))
    if len(cwd_pdfs) == 1:
        return cwd_pdfs[0]
    all_like = [f for f in cwd_pdfs if '(all' in os.path.basename(f).lower()]
    if all_like:
        all_like.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        return all_like[0]
    script_pdfs = sorted(glob.glob(os.path.join(script_dir, '*.pdf')))
    if len(script_pdfs) == 1:
        return script_pdfs[0]
    all_like2 = [f for f in script_pdfs if '(all' in os.path.basename(f).lower()]
    if all_like2:
        all_like2.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        return all_like2[0]
    return ''


def _auto_pick_kraken_model() -> str:
    env_m = os.getenv('OWNERCARDS_KRAKEN_MODEL', '').strip()
    if env_m and os.path.exists(os.path.expanduser(env_m)):
        return os.path.expanduser(env_m)
    base = os.path.expanduser('~/Library/Application Support/htrmopo')
    try:
        mcc, anym = [], []
        for root, _, files in os.walk(base):
            for fn in files:
                if fn.lower().endswith('.mlmodel'):
                    full = os.path.join(root, fn)
                    anym.append(full)
                    if 'mccatmus' in fn.lower():
                        mcc.append(full)
        if mcc:
            mcc.sort(key=lambda x: os.path.getmtime(x), reverse=True)
            return mcc[0]
        if anym:
            anym.sort(key=lambda x: os.path.getmtime(x), reverse=True)
            return anym[0]
    except Exception:
        return ''
    return ''


def _default_out_path(pdf_path: str) -> str:
    base, _ = os.path.splitext(pdf_path)
    return base + '_Output.xlsx'

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
    # common TN typos only (conservative)
    "TENN": "TN", "TENNESSES": "TN", "TEN": "TN", "TENN.": "TN", "TN.": "TN", "TIN": "TN",
    # explicitly keep IN as IN
    "IN": "IN",
}

CITY_BLOCKLIST = [
    "NASHVILLE", "BRENTWOOD", "FRANKLIN", "MADISON", "ANTIOCH",
    "HERMITAGE", "OLD HICKORY", "GOODLETTSVILLE", "PEGRAM",
    "CLARKSVILLE", "MURFREESBORO", "LEBANON", "GALLATIN", "FAIRVIEW",
    "WHITE BLUFF", "CENTERVILLE", "CHAPEL HILL",
]

US_STATE_RE = r"\b(" + "|".join(sorted(set(list(STATE_MAP.keys()) + list(STATE_MAP.values())), key=len, reverse=True)) + r")\b"
ZIP_RE = r"\b\d{5}(?:-\d{4})?\b"
STREET_START_RE = r"^\d+\s+[A-Za-z0-9]"
STREET_SUFFIXES = {
    "ST", "STREET", "AVE", "AVENUE", "RD", "ROAD", "DR", "DRIVE", "LN", "LANE",
    "CT", "COURT", "BLVD", "BOULEVARD", "HWY", "HIGHWAY", "PKWY", "PARKWAY",
    "CIR", "CIRCLE", "PL", "PLACE", "WAY", "TRL", "TRAIL", "TER", "TERRACE",
}
DIRECTIONAL_TOKENS = {"N", "S", "E", "W", "NE", "NW", "SE", "SW"}

NAME_BLACKLIST = [
    r"\btransfer", r"\bsold\s+to", r"\bgiven\s+to", r"\bspaces",
    r"\bcontract", r"\bsee\s+new", r"\bvoid",
    r"\bcancel", r"\bdeed", r"\binterment", r"\bitem\s+description",
    r"\bprice\b", r"\bsales\s+date", r"\bused\b",
    r"\bdivorced\b", r"\bdeceased\b", r"\bwidow\b",
    r"\bgarden\b", r"\bsection\b", r"\blot\b", r"\bblock\b",
    r"\bsermon\b", r"\bchapel\b", r"\bmt\b", r"\bmountain\b",
    r"\bsex\b", r"\bmale\b", r"\bfemale\b", r"\bgrave\b",
]

# Py3.14-safe: hyphen at end of class
NAME_NOISE_PATTERNS = [
    r"\btoor\b", r"\bmbo\b", r"\byoh\b", r"\bsbo\b",
    r"^\d+[\sA-Z-]*\b",
    r"^[;:\\.,\-*]+",
    r"\bowner\s*id\b.*", r"\bowner\s*since\b.*",
    r"\d+",
]

ADDRESS_BLOCKERS = [
    r"\bpo\s*box\b", r"\bbox\b",
    r"\broad\b", r"\brd\b", r"\bstreet\b", r"\bst\b",
    r"\bavenue\b", r"\bave\b", r"\bdrive\b", r"\bdr\b",
    r"\blane\b", r"\bln\b", r"\bcourt\b", r"\bct\b",
    r"\bhighway\b", r"\bhwy\b", r"\bblvd\b", r"\bboulevard\b",
    r"\bparkway\b", r"\bpkwy\b", r"\btrail\b", r"\btrl\b",
    r"\bcircle\b", r"\bcir\b", r"\bplace\b", r"\bpl\b",
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

EXCEL_SAFE_MAX = 32000
TRUNC_SUFFIX = " …[TRUNCATED]"

OCR_PSM6 = "--oem 3 -l eng --psm 6"
OCR_PSM11 = "--oem 3 -l eng --psm 11"


OCR_FILTER_NON_ITEMS = True
# -----------------------------
# HELPERS
# -----------------------------

def sha1_text(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()

def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def safe_upper(s: str) -> str:
    return normalize_ws(s).upper()

def compile_any(patterns: List[str]) -> List[re.Pattern]:
    return [re.compile(p, re.IGNORECASE) for p in patterns]

def matches_any(line: str, patterns: List[re.Pattern]) -> bool:
    return any(p.search(line or "") for p in patterns)

RE_FUNERAL_PN = compile_any(FUNERAL_PN_PATTERNS)
RE_AT_NEED = compile_any(AT_NEED_FUNERAL_PATTERNS)
RE_MEMORIAL = compile_any(MEMORIAL_PATTERNS)
RE_INTERMENT = compile_any(INTERMENT_SERVICE_PATTERNS)
RE_XFER = compile_any(TRANSFER_CANCEL_PATTERNS)
RE_NAME_BLACKLIST = compile_any(NAME_BLACKLIST)
RE_NAME_NOISE = compile_any(NAME_NOISE_PATTERNS)
RE_ADDR_BLOCK = compile_any(ADDRESS_BLOCKERS)
RE_NAME_GEO = compile_any([r"\bSermon\b", r"\bChapel\b", r"\bGarden\b", r"\bSection\b", r"\bMount\b", r"\bMt\.?\b"])
RE_NAME_GENDER = compile_any([r"\bSex\b", r"\bMale\b", r"\bFemale\b", r"\bGrave\b"])


def normalize_state(st: str) -> str:
    if not st:
        return ""
    st_clean = st.upper().replace(".", "").strip()
    return STATE_MAP.get(st_clean, st_clean)

def find_state_match(line: str, zipm: Optional[re.Match]) -> Optional[re.Match]:
    matches = list(re.finditer(US_STATE_RE, line or "", flags=re.IGNORECASE))
    if not matches:
        return None
    if zipm:
        for m in reversed(matches):
            if m.end() <= zipm.start():
                return m
    return matches[-1]

def fix_state_ocr_tokens(line: str) -> str:
    """
    Conservative OCR repair for state tokens.
    Only fixes patterns that are extremely unlikely to be valid US states.
    """
    if not line:
        return line
    # Common OCR: '1N' for 'TN' (digit one)
    line = re.sub(r"\b1N\b", "TN", line, flags=re.IGNORECASE)
    # Optional: lowercase ell 'lN' occasionally appears for TN
    line = re.sub(r"\blN\b", "TN", line, flags=re.IGNORECASE)
    return line

def extract_zip_state(line: str) -> Tuple[Optional[str], Optional[str]]:
    line2 = fix_state_ocr_tokens(line or "")
    zipm = re.search(ZIP_RE, line2)
    statem = find_state_match(line2, zipm)
    found_zip = zipm.group(0) if zipm else None
    found_state = normalize_state(statem.group(0)) if statem else None
    return found_zip, found_state

def looks_like_address_line(line: str) -> bool:
    if not line:
        return False
    z, st = extract_zip_state(line)
    if z and st:
        return True
    return matches_any(line, RE_ADDR_BLOCK)

def split_lines(raw_text: str) -> List[str]:
    raw_text = (raw_text or "").replace("\r\n", "\n").replace("\r", "\n")
    lines = [normalize_ws(x) for x in raw_text.split("\n")]
    return [x for x in lines if x]


# -----------------------------
# INITIAL PRESERVATION
# -----------------------------

INITIAL_DIGIT_MAP = {"8": "B", "0": "O", "1": "I", "2": "Z", "5": "S", "6": "G", "9": "P"}

def fix_digit_initials_in_name(line: str) -> str:
    """Fix digit-as-initial tokens: 'Cynthia 8.' -> 'Cynthia B.'"""
    if not line:
        return line
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
    """Fix leading digit-as-letter only when it matches target char."""
    if not line:
        return line
    if not line[0].isdigit():
        return line
    if line[0] in INITIAL_DIGIT_MAP and len(line) > 2 and line[1].isalpha():
        repl = INITIAL_DIGIT_MAP[line[0]]
        if (not target_char) or (repl.upper() == target_char.upper()):
            if "," in line[:20] or line[:20].isupper():
                return repl + line[1:]
    return line


# -----------------------------
# PHONE (extensions supported)
# -----------------------------

PHONE_PATTERN = re.compile(
    r"(?:(?:\+?1[\s\-\.]?)?\(?\d{3}\)?[\s\-\.]?\d{3}[\s\-\.]?\d{4}|\b\d{3}[\s\-\.]?\d{4}\b)"
    r"(?:\s*(?:ext\.?|x)\s*\d{1,6})?",
    re.IGNORECASE
)

def _digits_only(s: str) -> str:
    return re.sub(r"\D", "", s or "")

def _strip_phone_extension(raw: str) -> str:
    if not raw:
        return ""
    # Strip extensions like 'ext123', 'ext 123', 'x123', 'x 123' (case-insensitive)
    parts = re.split(r"\s*(?:ext\.?|x)\s*\d{1,6}\b", raw, maxsplit=1, flags=re.IGNORECASE)
    return parts[0]

def _normalize_phone_digits(d: str) -> Tuple[str, bool, bool]:
    d = d or ""
    if len(d) == 11 and d.startswith("1"):
        d = d[1:]
    if len(d) == 10:
        return f"({d[0:3]}) {d[3:6]}-{d[6:10]}", True, True
    if len(d) == 7:
        return f"{d[0:3]}-{d[3:7]}", False, True
    return "", False, False

def extract_phone_fields(full_text: str, lines: List[str]) -> Dict[str, object]:
    header_text = "\n".join(lines[:18]) if lines else (full_text or "")
    header_upper = header_text.upper()
    allow_seven_digit = any(token in header_upper for token in ["PHONE", "TEL", "TELEPHONE"])
    matches = [m.group(0) for m in PHONE_PATTERN.finditer(header_text)]
    if not matches:
        matches = [m.group(0) for m in PHONE_PATTERN.finditer(full_text or "")]

    seen = set()
    candidates = []
    for raw in matches:
        raw_main = _strip_phone_extension(raw)
        d = _digits_only(raw_main)
        if len(d) == 7 and not allow_seven_digit:
            continue
        if not d or d in seen:
            continue
        seen.add(d)
        norm, has_area, valid = _normalize_phone_digits(d)
        if valid:
            candidates.append((raw, norm, has_area))

    ten = [c for c in candidates if c[2] is True]
    sev = [c for c in candidates if c[2] is False]

    primary = ("", "", False)
    alt = ("", "", False)
    if ten:
        primary = ten[0]
        rest = [c for c in candidates if c[1] and c[1] != primary[1]]
        if rest:
            alt = rest[0]
    elif sev:
        primary = sev[0]
        rest = [c for c in candidates if c[1] and c[1] != primary[1]]
        if rest:
            alt = rest[0]

    return {
        "PhoneRaw": " | ".join([c[0] for c in candidates[:2]]),
        "Phone": primary[1] if primary[1] else "",
        "PhoneNormalized": primary[1] if primary[1] else "",
        "PhoneAltNormalized": alt[1] if alt[1] else "",
        "PhoneHasAreaCode": bool(primary[2]) if primary[1] else False,
        "PhoneAltHasAreaCode": bool(alt[2]) if alt[1] else False,
        "PhoneValid": bool(primary[1]) if primary[1] else False,
        "PhoneAltValid": bool(alt[1]) if alt[1] else False,
    }


# -----------------------------
# EXCEL SAFETY
# -----------------------------

def excel_safe_text(v):
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass

    if isinstance(v, (int, float, np.integer, np.floating, bool)):
        try:
            if isinstance(v, (float, np.floating)) and np.isinf(v):
                return ""
        except Exception:
            pass
        return v

    s = str(v)
    try:
        s = unicodedata.normalize("NFKD", s)
    except Exception:
        pass

    s = re.sub(r"[^\x09\x0A\x0D\x20-\uD7FF\uE000-\uFFFD]", "", s)
    s = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", s)

    s_l = s.lstrip()
    if s_l.startswith(("=", "+", "-", "@")):
        s = "'" + s

    if len(s) > EXCEL_SAFE_MAX:
        s = s[: (EXCEL_SAFE_MAX - len(TRUNC_SUFFIX))] + TRUNC_SUFFIX
    return s

def make_df_excel_safe(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    for c in df2.columns:
        df2[c] = df2[c].map(excel_safe_text)
    return df2

def choose_excel_engine() -> str:
    try:
        import xlsxwriter  # noqa
        return "xlsxwriter"
    except Exception:
        return "openpyxl"

def force_string_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = df[c].apply(lambda x: "" if x is None or (isinstance(x, float) and np.isnan(x)) else str(x))
    return df


# -----------------------------
# TEXT LAYER
# -----------------------------

def extract_pdf_text_page(pdf_path: str, page_index: int, reader: Optional[PdfReader] = None) -> str:
    try:
        r = reader if reader is not None else PdfReader(pdf_path)
        if page_index >= len(r.pages):
            return ""
        return r.pages[page_index].extract_text() or ""
    except Exception:
        return ""

def text_layer_usable(txt: str) -> bool:
    if not txt:
        return False
    t = normalize_ws(txt)
    if sum(ch.isalpha() for ch in t) < 40:
        return False
    anchors = ["ITEM DESCRIPTION", "OWNER ID", "CONTRACT", "LOT", "SECTION", "GARDEN", "TN", "TENNESSEE"]
    if any(a in t.upper() for a in anchors):
        return True
    return len(t) > 250

def detect_template_type(text: str) -> str:
    t = (text or "").upper()
    if "INTERMENT RECORD" in t:
        return "interment_record"
    if "ITEM DESCRIPTION" in t or "OWNER ID" in t or "CONTRACT NBR" in t:
        return "modern_table"
    return "legacy_typewritten"


# -----------------------------
# OCR / IMAGE
# -----------------------------

def deskew_bgr(img_bgr: np.ndarray) -> np.ndarray:
    gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)
    thr = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]
    coords = np.column_stack(np.where(thr > 0))
    if coords.size == 0:
        return img_bgr
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
    return Image.fromarray(ensure_dark_text_on_white(binary))

def preprocess_clahe(pil_img: Image.Image) -> Image.Image:
    img_np = cv2.cvtColor(np.array(pil_img), cv2.COLOR_RGB2BGR)
    lab = cv2.cvtColor(img_np, cv2.COLOR_BGR2LAB)
    l, a, b = cv2.split(lab)
    clahe = cv2.createCLAHE(clipLimit=3.0, tileGridSize=(8, 8))
    cl = clahe.apply(l)
    limg = cv2.merge((cl, a, b))
    enhanced = cv2.cvtColor(cv2.cvtColor(limg, cv2.COLOR_LAB2BGR), cv2.COLOR_BGR2GRAY)
    bin_img = cv2.adaptiveThreshold(enhanced, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 35, 11)
    return Image.fromarray(ensure_dark_text_on_white(bin_img))

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
        if not txt or not str(txt).strip():
            continue
        try:
            conf = int(float(data["conf"][i]))
        except Exception:
            conf = -1
        if 0 <= conf < 30:
            continue
        words.append({
            "text": str(txt).strip(),
            "x": int(data["left"][i]),
            "y": int(data["top"][i]),
            "w": int(data["width"][i]),
            "h": int(data["height"][i]),
            "line_num": int(data["line_num"][i]),
            "block_num": int(data["block_num"][i]),
            "par_num": int(data["par_num"][i]),
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
    height, width = gray.shape[:2]
    kernel_width = max(35, int(width * 0.03))
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (kernel_width, 1))
    horiz = cv2.morphologyEx(thr, cv2.MORPH_OPEN, kernel, iterations=1)
    edges = cv2.Canny(horiz, 50, 150)
    min_len = max(80, int(width * 0.08))
    lines = cv2.HoughLinesP(edges, 1, np.pi / 180, threshold=80, minLineLength=min_len, maxLineGap=10)
    segs: List[Tuple[int, int, int, int]] = []
    if lines is None:
        return segs
    for l in lines[:, 0, :]:
        x1, y1, x2, y2 = map(int, l)
        if abs(y2 - y1) <= 3 and abs(x2 - x1) >= 80:
            if y1 < int(height * 0.08) or y1 > int(height * 0.95):
                continue
            segs.append((x1, y1, x2, y2))
    return segs

def line_is_struck(line_bbox: Tuple[int, int, int, int], strike_segs: List[Tuple[int, int, int, int]]) -> bool:
    x1, y1, x2, y2 = line_bbox
    midy = (y1 + y2) // 2
    for sx1, sy1, sx2, sy2 in strike_segs:
        if y1 - 2 <= sy1 <= y2 + 2 and not (sx2 < x1 or sx1 > x2):
            return True
        if abs(sy1 - midy) <= 3 and not (sx2 < x1 or sx1 > x2):
            return True
    return False


def render_page(pdf_path: str, page_index: int, dpi: int) -> Optional[Image.Image]:
    imgs = convert_from_path(pdf_path, dpi=dpi, first_page=page_index + 1, last_page=page_index + 1)
    if not imgs:
        return None
    return imgs[0].convert("RGB")


def detect_strike_segs_for_page(pdf_path: str, page_index: int, dpi: int = 150) -> Tuple[List[Tuple[int, int, int, int]], Optional[Image.Image]]:
    """Render once at the requested DPI, deskew, and return (strike_segments, deskewed_PIL_RGB).
    Using the same render for both strike detection and OCR avoids coordinate mismatches.
    """
    pil_img = render_page(pdf_path, page_index, dpi=dpi)
    if pil_img is None:
        return [], None
    img_bgr = cv2.cvtColor(np.array(pil_img), cv2.COLOR_RGB2BGR)
    img_bgr = deskew_bgr(img_bgr)
    segs = detect_horizontal_strikelines(img_bgr)
    pil_deskew = Image.fromarray(cv2.cvtColor(img_bgr, cv2.COLOR_BGR2RGB))
    return segs, pil_deskew

def ocr_items_with_strike_from_image(pil_img: Image.Image, strike_segs: List[Tuple[int, int, int, int]]) -> List[Dict]:
    """OCR a pre-rendered (and ideally deskewed) PIL image in data mode (std+clahe).
    Uses strike_segs in the SAME coordinate space as pil_img to flag struck lines.
    """
    if pil_img is None:
        return []
    pil_std = preprocess_standard(pil_img)
    pil_clahe = preprocess_clahe(pil_img)
    d_std = pytesseract.image_to_data(pil_std, config=OCR_PSM6, output_type=Output.DICT)
    d_clahe = pytesseract.image_to_data(pil_clahe, config=OCR_PSM6, output_type=Output.DICT)
    raw_lines_a = group_text_lines_from_ocr(d_std)
    raw_lines_b = group_text_lines_from_ocr(d_clahe)
    items: List[Dict] = []
    seen = set()

    def maybe_add_line(ln_obj: Dict):
        txt = ln_obj.get("text", "")
        if not txt:
            return
        x1, y1, x2, y2 = ln_obj["bbox"]
        key = sha1_text(f"{normalize_ws(txt)}\n{round(x1,-1)}\n{round(y1,-1)}\n{round(x2,-1)}\n{round(y2,-1)}")
        if key in seen:
            return
        struck = line_is_struck(ln_obj["bbox"], strike_segs)
        it = item_dict_from_line(txt, struck=struck)
        if OCR_FILTER_NON_ITEMS and not (it.get("Include") or it.get("StruckThrough") or it.get("ExcludedByText")):
            seen.add(key)
            return
        items.append(it)
        seen.add(key)

    # Prefer CLAHE first then STD (often cleaner), matching prior behavior
    for ln_obj in raw_lines_b:
        maybe_add_line(ln_obj)
    for ln_obj in raw_lines_a:
        maybe_add_line(ln_obj)
    return items

def ocr_items_with_strike(pdf_path: str, page_index: int, dpi: int, strike_segs: List[Tuple[int, int, int, int]]) -> List[Dict]:
    """Backward-compatible wrapper: render+deskew at DPI, then OCR with strike.
    Prefer calling ocr_items_with_strike_from_image when you already have the rendered PIL.
    """
    pil_img = render_page(pdf_path, page_index, dpi=dpi)
    if pil_img is None:
        return []
    img_bgr = cv2.cvtColor(np.array(pil_img), cv2.COLOR_RGB2BGR)
    img_bgr = deskew_bgr(img_bgr)
    pil_deskew = Image.fromarray(cv2.cvtColor(img_bgr, cv2.COLOR_BGR2RGB))
    return ocr_items_with_strike_from_image(pil_deskew, strike_segs=strike_segs)

def parse_inline_address_line(line: str) -> Optional[Dict[str, str]]:
    """Parse a single-line address containing street + city + state + ZIP."""
    if not line:
        return None
    line2 = fix_state_ocr_tokens(line)
    zipm = re.search(ZIP_RE, line2)
    statem = find_state_match(line2, zipm)
    if not zipm or not statem:
        return None
    if not re.search(STREET_START_RE, line2):
        return None

    z = zipm.group(0)
    st = normalize_state(statem.group(0))
    before_state = normalize_ws(line2[:statem.start()].rstrip(",")).strip()
    if not before_state:
        return None

    # split street/city
    if "," in before_state:
        street_part, city_part = before_state.rsplit(",", 1)
        street = normalize_ws(street_part)
        city = normalize_ws(city_part)
    else:
        # try to split by a known city ending or a street suffix marker
        upper_before = before_state.upper()
        city_match = None
        for city_name in sorted(CITY_BLOCKLIST, key=len, reverse=True):
            if upper_before.endswith(f" {city_name}"):
                city_match = city_name
                break
        if city_match:
            street = normalize_ws(before_state[: -len(city_match)].rstrip())
            city = normalize_ws(city_match)
        else:
            parts = before_state.split()
            suffix_idx = None
            for i in range(len(parts)):
                token = parts[i].rstrip(".").upper()
                if token in STREET_SUFFIXES:
                    suffix_idx = i
            if suffix_idx is not None and suffix_idx + 1 < len(parts):
                street_tokens = parts[: suffix_idx + 1]
                city_tokens = parts[suffix_idx + 1 :]
                if city_tokens and city_tokens[0].upper() in DIRECTIONAL_TOKENS:
                    street_tokens.append(city_tokens[0])
                    city_tokens = city_tokens[1:]
                street = " ".join(street_tokens)
                city = " ".join(city_tokens)
            else:
                if (
                    len(parts) >= 4
                    and parts[-1].isalpha()
                    and parts[-2].isalpha()
                    and parts[-1].upper() not in DIRECTIONAL_TOKENS
                    and parts[-2].upper() not in DIRECTIONAL_TOKENS
                ):
                    street = " ".join(parts[:-2])
                    city = " ".join(parts[-2:])
                elif len(parts) >= 3 and parts[-1].upper() not in DIRECTIONAL_TOKENS:
                    street = " ".join(parts[:-1])
                    city = parts[-1]
                else:
                    street = before_state
                    city = ""

    return {
        "Street": street,
        "City": city,
        "State": st,
        "ZIP": z,
        "CityStateZip": line,
        "Inline": True,
    }


def parse_best_address(lines: List[str]) -> Dict:
    """Pick best address candidate; now supports inline address lines."""
    candidates = []
    for i, line in enumerate(lines):
        z, st = extract_zip_state(line)
        if z and st:
            inline = parse_inline_address_line(line)
            if inline:
                candidates.append({
                    "Index": i,
                    "Street": inline["Street"],
                    "City": inline["City"],
                    "State": inline["State"],
                    "ZIP": inline["ZIP"],
                    "CityStateZip": inline["CityStateZip"],
                    "Score": 95 if inline.get("City") else 85,
                    "Inline": True,
                })
                continue

            prev_idx = i - 1
            street_candidate = lines[prev_idx] if prev_idx >= 0 else ""
            street_candidate = clean_address_line(street_candidate)
            street_candidate = re.sub(r"^[\W_]+", "", street_candidate)

            score = 50
            if street_candidate and re.search(STREET_START_RE, street_candidate):
                score += 40
            if "," in street_candidate:
                score -= 30
            if len(street_candidate) < 5:
                score -= 20

            candidates.append({
                "Index": prev_idx if prev_idx >= 0 else i,
                "Street": street_candidate,
                "City": "",
                "State": st,
                "ZIP": z,
                "CityStateZip": line,
                "Score": score,
            })

    
    # --- OPTIONAL: street-only fallback (when ZIP/state is missing) ---
    # If we couldn't find any ZIP+state anchored candidates, try to at least capture a street line.
    if not candidates:
        TOP_N = 25
        scan = lines[:TOP_N]

        exclude_keywords = (
            "OWNER ID", "OWNER SINCE", "ITEM DESCRIPTION", "CONTRACT", "USED", "SALES DATE", "PRICE",
            "LOT", "SECTION", "SEC", "BLOCK", "SP", "SPACE", "GARDEN", "MAUS", "MAUSOLEUM",
            "INTERMENT", "BURIAL", "DECEASED", "DOD", "DOB"
        )

        street_suffixes = {
            "ST", "ST.", "AVE", "AVE.", "RD", "RD.", "DR", "DR.", "LN", "LN.",
            "CT", "CT.", "BLVD", "BLVD.", "HWY", "PKWY", "CIR", "PL", "WAY", "TRL", "TER"
        }

        best = None  # (score, idx, cleaned)

        for i, line in enumerate(scan):
            ln = normalize_ws(line)
            if not ln:
                continue

            u = ln.upper()
            if any(k in u for k in exclude_keywords):
                continue

            if not re.search(STREET_START_RE, ln):
                continue

            cleaned = clean_address_line(ln)
            if not cleaned:
                continue

            if not any(ch.isalpha() for ch in cleaned):
                continue

            score = 100 - i
            toks = set(re.split(r"\s+", cleaned.upper()))
            if toks.intersection(street_suffixes):
                score += 15
            if matches_any(cleaned, RE_ADDR_BLOCK):
                score += 5
            if "," in cleaned:
                score -= 5

            cand = (score, i, cleaned)
            if (best is None) or (cand[0] > best[0]):
                best = cand

        if best is not None:
            _, idx2, best_street = best
            candidates.append({
                "Index": idx2,
                "Street": best_street,
                "City": "",
                "State": "",
                "ZIP": "",
                "CityStateZip": "",
                "Score": 35,
            })
    if not candidates:
        for i, line in enumerate(lines):
            if looks_like_address_line(line):
                return {"Index": i, "Street": "", "City": "", "State": "", "ZIP": "", "AddressRaw": line}
        return {"Index": None, "Street": "", "City": "", "State": "", "ZIP": "", "AddressRaw": ""}

    best = sorted(candidates, key=lambda x: x["Score"], reverse=True)[0]
    street = best.get("Street", "")
    city = best.get("City", "")
    state = best.get("State", "")
    zipc = best.get("ZIP", "")

    # if city wasn't parsed, derive from CityStateZip
    if not city and best.get("CityStateZip") and state and not best.get("Inline"):
        m = re.search(US_STATE_RE, best["CityStateZip"], re.IGNORECASE)
        if m:
            city_part = best["CityStateZip"][:m.start()]
            city = normalize_ws(city_part).replace(",", "")

    return {
        "Index": best.get("Index"),
        "Street": street,
        "City": city,
        "State": state,
        "ZIP": zipc,
        "AddressRaw": f"{street} | {best.get('CityStateZip','')}".strip(" |"),
    }


# -----------------------------

# ---------------------------------
# REVIEW / SALVAGE + ALT OCR HELPERS
# ---------------------------------
def extract_state_zip_anywhere(text: str) -> Tuple[str, str]:
    """Best-effort: find ZIP and nearby state anywhere in text (used for salvage)."""
    t = fix_state_ocr_tokens(text or "")
    zipm = re.search(ZIP_RE, t)
    if not zipm:
        return "", ""
    left = t[max(0, zipm.start() - 30):zipm.start() + 5]
    statem = re.search(US_STATE_RE, left, re.IGNORECASE)
    st = normalize_state(statem.group(0)) if statem else ""
    return st, zipm.group(0)


def find_comma_name_candidate(lines: List[str]) -> str:
    """Best-effort: find a plausible 'LAST, FIRST' name near the top of OCR fallback pages."""
    best = ""
    best_score = -1
    for ln in (lines or [])[:25]:
        t = normalize_ws(ln)
        if not t or ',' not in t:
            continue
        if matches_any(t, RE_NAME_BLACKLIST):
            continue
        letters = sum(ch.isalpha() for ch in t)
        if letters < 8:
            continue
        bad = sum((not ch.isalnum()) and (ch not in " ,.&'-#") for ch in t)
        score = letters - bad
        if score > best_score:
            best = t
            best_score = score
    return best


def try_ocrmac_text(pil_img: Image.Image) -> str:
    """Last-resort OCR using Apple Vision via ocrmac (macOS only). Returns joined text or ''."""
    try:
        from ocrmac import ocrmac  # type: ignore
    except Exception:
        return ""
    try:
        anns = ocrmac.OCR(pil_img).recognize()
        texts = [a[0] for a in anns if a and isinstance(a, (list, tuple)) and len(a) >= 1 and str(a[0]).strip()]
        return "\n".join(texts)
        # (fixed join)
    except Exception:
        return ""


def try_kraken_text(pil_img: Image.Image, model_path: str = "", kraken_bin: str = "kraken") -> str:
    """Last-resort OCR using Kraken via CLI. Requires kraken CLI + .mlmodel. Returns text or ''."""
    if not model_path:
        return ""
    import tempfile
    import subprocess
    try:
        with tempfile.TemporaryDirectory() as td:
            img_path = Path(td) / "page.png"
            out_path = Path(td) / "out.txt"
            pil_img.save(img_path)
            cmd = [
                kraken_bin, "-i", str(img_path), str(out_path),
                "binarize", "segment", "-bl", "ocr", "-m", model_path
            ]
            subprocess.run(cmd, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if out_path.exists():
                return out_path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""
    return ""

# HEADER PARSING
# -----------------------------

def is_gibberish(text: str) -> bool:
    if not text or len(text) < 3:
        return True
    if not any(c.isupper() for c in text):
        return True
    if not re.search(r"[AEIOUYaeiouy]", text):
        return True
    words = text.split()
    single_char_words = sum(1 for w in words if len(w) == 1 and w.lower() not in ["a", "i"])
    if words and (single_char_words / len(words) > 0.4):
        return True
    return False


def clean_address_line(line: str) -> str:
    if not line:
        return ""
    m_owner = re.search(r"\bowner\s*(since|id)\b", line, re.IGNORECASE)
    if m_owner:
        line = line[:m_owner.start()]
    m_date = re.search(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b", line)
    if m_date:
        line = line[:m_date.start()]
    return normalize_ws(line)


def clean_name_line(line: str, target_char: Optional[str] = None, aggressive: bool = False) -> str:
    if not line:
        return ""

    line = fix_digit_initials_in_name(line)
    if re.match(r"^\d", line):
        line = fix_leading_digit_as_letter(line, target_char)
        if re.match(r"^\d", line):
            return ""

    if matches_any(line, RE_NAME_GEO) or matches_any(line, RE_NAME_GENDER):
        return ""

    u = line.upper()
    for city in CITY_BLOCKLIST:
        idx = u.find(city)
        if idx != -1:
            line = line[:idx]
            break

    if "#" in line:
        line = line.split("#")[0]

    m_kw = re.search(r"\b(road|rd|street|st|avenue|ave|drive|dr|lane|ln|court|ct|blvd|boulevard|pkwy|parkway|hwy|highway|trl|trail|cir|circle|pl|place|po\s*box|box)\b", line, re.IGNORECASE)
    if m_kw:
        line = line[:m_kw.start()]

    m_addr_start = re.search(r"\b\d+\s+[A-Za-z]", line)
    if m_addr_start:
        line = line[:m_addr_start.start()]

    cleaned = line
    for pat in RE_NAME_NOISE:
        cleaned = pat.sub("", cleaned)

    cleaned = re.sub(r"[^a-zA-Z\s&\.,\-']", "", cleaned)
    cleaned = normalize_ws(cleaned)

    if aggressive and target_char and cleaned and not cleaned.upper().startswith(target_char.upper()):
        idx = cleaned.upper().find(target_char.upper())
        if idx != -1:
            cleaned = cleaned[idx:].strip()

    return cleaned


def get_header_candidate(lines: List[str], addr_idx: Optional[int], target_char: Optional[str], aggressive: bool) -> List[str]:
    clean_header: List[str] = []
    top = lines[:40]
    search_lines = top[:addr_idx] if (addr_idx is not None and addr_idx > 0) else top[:10]
    has_addr_context = addr_idx is not None and addr_idx > 0
    search_iter = range(len(search_lines) - 1, -1, -1) if has_addr_context else range(len(search_lines))

    for i in search_iter:
        ln = search_lines[i]
        if not ln.strip():
            continue
        if matches_any(ln, RE_NAME_BLACKLIST):
            continue
        ln_clean = clean_name_line(ln, target_char, aggressive)
        if not ln_clean or is_gibberish(ln_clean):
            continue
        if has_addr_context:
            clean_header.insert(0, ln_clean)
        else:
            clean_header.append(ln_clean)
        if len(clean_header) >= 2:
            break

    return clean_header


def parse_owner_header(lines: List[str], target_char: Optional[str] = None) -> Tuple[str, str, str, str, Dict, bool]:
    if not lines:
        return ("", "", "", "", {}, False)

    top = lines[:40]
    addr_info = parse_best_address(top)
    addr_idx = addr_info.get("Index")

    is_interment = any("INTERMENT RECORD" in ln.upper() for ln in top[:15])
    if is_interment:
        return ("INTERMENT RECORD - REFILE", "INTERMENT RECORD - REFILE", "", "", {}, True)

    clean_header = get_header_candidate(lines, addr_idx, target_char, aggressive=False)
    is_valid = False
    if clean_header:
        candidate = normalize_ws(" ".join(clean_header))
        if target_char and candidate.upper().startswith(target_char.upper()):
            is_valid = True
        elif not target_char and not is_gibberish(candidate):
            is_valid = True

    if not is_valid:
        clean_header = get_header_candidate(lines, addr_idx, target_char, aggressive=True)

    header_text = normalize_ws(" ".join(clean_header))
    header_text = re.sub(r"\b(owner|address|phone|lot|section|space|card)\b[:\-]?", "", header_text, flags=re.IGNORECASE).strip()

    primary, secondary = "", ""
    if re.search(r"\s&\s|\sand\s", header_text, re.IGNORECASE):
        parts = re.split(r"\s&\s|\sand\s", header_text, flags=re.IGNORECASE)
        parts = [normalize_ws(p) for p in parts if normalize_ws(p)]
        if parts:
            primary = parts[0]
            secondary = parts[1] if len(parts) > 1 else ""
    else:
        primary = header_text

    if "," in primary:
        last_name = primary.split(",")[0].strip()
    elif " " in primary:
        last_name = primary.split(" ")[0].strip()
    else:
        last_name = primary

    # Flag missing for trust-but-verify
    if target_char and primary and not primary.upper().startswith(target_char.upper()):
        primary = "[MISSING - CHECK PDF] " + primary

    return (header_text, primary, secondary, last_name, addr_info, False)


# -----------------------------
# ITEM PARSING
# -----------------------------

def is_excludable_item(line: str) -> bool:
    return matches_any(line, RE_XFER)

def classify_item(line: str) -> Dict[str, bool]:
    t = line or ""
    flags = {
        "IsProperty": bool(re.search(r"\b(space|sp\.?|lot|section|sec\.?|block|blk\.?|garden|crypt|lawn|grave|burial|mausoleum|maus\.?|niche|columbarium|estates?)\b", t, re.IGNORECASE)),
        "IsMemorial": matches_any(t, RE_MEMORIAL),
        "IsFuneralPreneed": matches_any(t, RE_FUNERAL_PN),
        "IsAtNeedFuneral": matches_any(t, RE_AT_NEED),
        "IsIntermentService": matches_any(t, RE_INTERMENT),
        "HasRightsNotation": bool(RIGHTS_NOTATION_RE.search(t)),
    }
    if flags["IsIntermentService"]:
        flags["IsFuneralPreneed"] = False
    if flags["IsAtNeedFuneral"]:
        flags["IsFuneralPreneed"] = False
    return flags

def rights_used_total(line: str) -> Optional[Tuple[int, int, str]]:
    m = RIGHTS_NOTATION_RE.search(line or "")
    if not m:
        return None
    a, b = int(m.group(1)), int(m.group(2))
    return (a, b, f"{a}/{b}")

def item_dict_from_line(txt: str, struck: bool = False) -> Dict:
    excludable = is_excludable_item(txt)
    cls = classify_item(txt)
    looks_item = (cls["IsProperty"] or cls["IsMemorial"] or cls["IsFuneralPreneed"] or cls["IsAtNeedFuneral"] or cls["HasRightsNotation"])
    include = looks_item and (not struck) and (not excludable)
    rt = rights_used_total(txt)
    return {
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
    }

def parse_items_from_text(lines: List[str], template_type: str) -> List[Dict]:
    items: List[Dict] = []
    if template_type == "modern_table":
        in_items = False
        for ln in lines:
            u = ln.upper()
            if "ITEM DESCRIPTION" in u:
                in_items = True
                continue
            if not in_items:
                continue
            if "OWNER ID" in u or "OWNER SINCE" in u:
                break
            txt = normalize_ws(ln)
            if not txt:
                continue
            if txt.upper() in {"USED", "USED?", "CONTRACT NBR", "SALES DATE", "PRICE"}:
                continue
            items.append(item_dict_from_line(txt, struck=False))
        return items

    for ln in lines[5:]:
        txt = normalize_ws(ln)
        if not txt:
            continue
        if matches_any(txt, RE_NAME_BLACKLIST):
            continue
        cls = classify_item(txt)
        if cls["IsProperty"] or cls["IsMemorial"] or cls["IsFuneralPreneed"] or cls["IsAtNeedFuneral"] or cls["HasRightsNotation"]:
            items.append(item_dict_from_line(txt, struck=False))
    return items


# -----------------------------
# SCORING
# -----------------------------

def score_text_pass(txt: str) -> int:
    if not txt:
        return 0
    u = txt.upper()
    score = 0
    if re.search(ZIP_RE, u):
        score += 40
    if re.search(US_STATE_RE, u, re.IGNORECASE):
        score += 20
    if "OWNER ID" in u:
        score += 20
    if "ITEM DESCRIPTION" in u:
        score += 20
    good_lines = [ln for ln in split_lines(txt) if not is_gibberish(ln)]
    score += min(len(good_lines), 60)
    return score


# -----------------------------
# PAGE PROCESSOR
# -----------------------------

def process_page(pdf_path: str, page_index: int, dpi: int, target_char: Optional[str], reader: Optional[PdfReader] = None, kraken_model: str = '', kraken_bin: str = 'kraken', alt_ocr_order: str = 'ocrmac_then_kraken') -> Tuple[Dict, List[Dict], bool]:
    """Returns (owner_dict, items_list, is_interment)."""

    # TEXT LAYER FIRST
    pdf_text = extract_pdf_text_page(pdf_path, page_index, reader=reader)
    if text_layer_usable(pdf_text):
        txt = pdf_text
        lines = split_lines(txt)
        template_type = detect_template_type(txt)
        _, p, s, last, addr, is_interment = parse_owner_header(lines, target_char)

        # trust-but-verify
        ok = bool(p) and "[MISSING" not in p
        if ok:
            phone_fields = extract_phone_fields(txt, lines)

            # ---- Strike detection parity ----
            strike_dpi = min(200, max(150, dpi // 2))
            strike_segs, strike_pil = detect_strike_segs_for_page(pdf_path, page_index, dpi=strike_dpi)
            if strike_segs and strike_pil is not None:
                # Build items via OCR using the SAME render used for strike detection (avoids DPI/coordinate mismatch)
                items = [] if is_interment else ocr_items_with_strike_from_image(strike_pil, strike_segs=strike_segs)
            else:
                # fast path: text-layer items (no strike lines detected)
                items = [] if is_interment else parse_items_from_text(lines, template_type)
            owner = {
                "OwnerName_Raw": normalize_ws(f"{p} {s}"),
                "PrimaryOwnerName": p,
                "SecondaryOwnerName": s,
                "LastName": last,
                "Street": addr.get("Street", ""),
                "City": addr.get("City", ""),
                "State": addr.get("State", ""),
                "ZIP": addr.get("ZIP", ""),
                "AddressRaw": addr.get("AddressRaw", ""),
                "RawText": txt,
                "RawTextHash": sha1_text(txt),
                "TemplateType": template_type,
                "TextSource": "PDF_TEXT_LAYER" + ("_WITH_STRIKE_OCR" if strike_segs else ""),
            }
            # NeedsReview: flag rows that should be manually verified
            review_notes: List[str] = []
            needs_review = False
            if "[MISSING" in (p or ""):
                needs_review = True
                review_notes.append('NAME_MISSING')
                try:
                    if strike_pil is not None:
                        save_failure_snapshot(strike_pil, pdf_path, page_index+1, reason='NAME_MISSING')
                    else:
                        img = render_page(pdf_path, page_index, dpi=dpi)
                        if img is not None:
                            save_failure_snapshot(img.convert('RGB'), pdf_path, page_index+1, reason='NAME_MISSING')
                except Exception:
                    pass
            if (not addr.get('State')) or (not addr.get('ZIP')):
                needs_review = True
                review_notes.append('ADDR_INCOMPLETE')
            if strike_segs:
                needs_review = True
                review_notes.append('STRIKE_OCR')
            owner['NeedsReview'] = 'TRUE' if needs_review else 'FALSE'
            owner['NeedsReviewNotes'] = '; '.join(review_notes)
            owner.update(phone_fields)
            return owner, items, is_interment

    # OCR FALLBACK (full)
    pil_original = render_page(pdf_path, page_index, dpi=dpi)
    if pil_original is None:
        raise RuntimeError(f"Failed to render page {page_index+1}")

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

    deep_ocr_applied = False

    alt_ocr_applied = False

    alt_ocr_used = ""

    candidates = [("STD", t_std), ("CLAHE", t_clahe), ("GHOST", t_ghost), ("SPARSE", t_sparse)]
    best_name, best_text, best_score = sorted(
        [(n, t, score_text_pass(t)) for (n, t) in candidates],
        key=lambda x: x[2], reverse=True
    )[0]


    # ---- LAST RESORT: Deep OCR Retry (2x upscale) ----

    if best_score < 35:

        try:

            scale = 2.0

            img_bgr2 = cv2.cvtColor(np.array(pil_original), cv2.COLOR_RGB2BGR)

            img_bgr2 = cv2.resize(img_bgr2, (int(img_bgr2.shape[1]*scale), int(img_bgr2.shape[0]*scale)), interpolation=cv2.INTER_CUBIC)

            pil_2x = Image.fromarray(cv2.cvtColor(img_bgr2, cv2.COLOR_BGR2RGB))

            pil2_std = preprocess_standard(pil_2x)

            pil2_clahe = preprocess_clahe(pil_2x)

            pil2_ghost = preprocess_ghost(pil_2x)

            dt_std = pytesseract.image_to_string(pil2_std, config=OCR_PSM6)

            dt_clahe = pytesseract.image_to_string(pil2_clahe, config=OCR_PSM6)

            dt_ghost = pytesseract.image_to_string(pil2_ghost, config=OCR_PSM6)

            dt_sparse = pytesseract.image_to_string(pil_2x, config=OCR_PSM11)

            deep_candidates = [("DEEP2X_STD", dt_std), ("DEEP2X_CLAHE", dt_clahe), ("DEEP2X_GHOST", dt_ghost), ("DEEP2X_SPARSE", dt_sparse)]

            d_name, d_text, d_score = sorted([(n, t, score_text_pass(t)) for (n, t) in deep_candidates], key=lambda x: x[2], reverse=True)[0]

            if d_score >= best_score + 8:

                best_name, best_text, best_score = d_name, d_text, d_score

                deep_ocr_applied = True

        except Exception:

            pass


    # ---- LAST RESORT: Alternate OCR for handwriting ----

    if best_score < 35:

        try:

            img_bgr2 = cv2.cvtColor(np.array(pil_original), cv2.COLOR_RGB2BGR)

            img_bgr2 = cv2.resize(img_bgr2, (int(img_bgr2.shape[1]*2.0), int(img_bgr2.shape[0]*2.0)), interpolation=cv2.INTER_CUBIC)

            pil_alt = Image.fromarray(cv2.cvtColor(img_bgr2, cv2.COLOR_BGR2RGB))

            alt_text = ''

            if alt_ocr_order == 'none':

                alt_text = ''

            elif alt_ocr_order == 'kraken_then_ocrmac':

                if kraken_model:

                    alt_text = try_kraken_text(pil_alt, model_path=kraken_model, kraken_bin=kraken_bin)

                    if alt_text:

                        alt_ocr_used = 'KRAKEN'

                if not alt_text:

                    alt_text = try_ocrmac_text(pil_alt)

                    if alt_text:

                        alt_ocr_used = 'OCRMAC'

            else:

                alt_text = try_ocrmac_text(pil_alt)

                if alt_text:

                    alt_ocr_used = 'OCRMAC'

                if (not alt_text) and kraken_model:

                    alt_text = try_kraken_text(pil_alt, model_path=kraken_model, kraken_bin=kraken_bin)

                    if alt_text:

                        alt_ocr_used = 'KRAKEN'

            if alt_text:

                a_score = score_text_pass(alt_text)

                if a_score >= best_score + 6:

                    best_name, best_text, best_score = alt_ocr_used, alt_text, a_score

                    alt_ocr_applied = True

        except Exception:

            pass

    lines_best = split_lines(best_text)
    template_type = detect_template_type(best_text)
    _, p, s, last, addr, is_interment = parse_owner_header(lines_best, target_char)
    phone_fields = extract_phone_fields(best_text, lines_best)
    # NeedsReview: OCR fallback is inherently less reliable
    needs_review = True
    review_notes: List[str] = [f"OCR_FALLBACK_{best_name}"]
    if deep_ocr_applied:
        review_notes.append('DEEP_OCR_2X')
    if alt_ocr_applied:
        review_notes.append('ALT_OCR_' + alt_ocr_used)
    # Loose ZIP/State salvage
    if (not addr.get('State')) or (not addr.get('ZIP')):
        st2, z2 = extract_state_zip_anywhere(best_text)
        if st2 or z2:
            addr['State'] = addr.get('State') or st2
            addr['ZIP'] = addr.get('ZIP') or z2
            review_notes.append('ADDR_SALVAGED')
    if (not p) or ('[MISSING' in (p or '')) or is_gibberish(p):
        cand = find_comma_name_candidate(lines_best)
        if cand:
            p = cand
            review_notes.append('NAME_SALVAGED')
    if (not addr.get('State')) or (not addr.get('ZIP')):
        review_notes.append('ADDR_INCOMPLETE')
    if ('[MISSING' in (p or '')):
        review_notes.append('NAME_MISSING')
        try:
            save_failure_snapshot(pil_original, pdf_path, page_index+1, reason='NAME_MISSING')
        except Exception:
            pass

    # items with strike detection
    strike_segs = detect_horizontal_strikelines(orig_bgr)
    items = [] if is_interment else ocr_items_with_strike_from_image(pil_original, strike_segs=strike_segs)

    combined_raw = "\n".join([t_std, t_clahe, t_ghost, t_sparse])

    owner = {
        "OwnerName_Raw": normalize_ws(f"{p} {s}"),
        "PrimaryOwnerName": p,
        "SecondaryOwnerName": s,
        "LastName": last,
        "Street": addr.get("Street", ""),
        "City": addr.get("City", ""),
        "State": addr.get("State", ""),
        "ZIP": addr.get("ZIP", ""),
        "AddressRaw": addr.get("AddressRaw", ""),
        "RawText": combined_raw,
        "RawTextHash": sha1_text(combined_raw),
        "TemplateType": template_type,
        "TextSource": f"OCR_FALLBACK_{best_name}_S{best_score}",
        "NeedsReview": 'TRUE',
        "NeedsReviewNotes": '; '.join(review_notes),
    }
    owner.update(phone_fields)
    return owner, items, is_interment


# -----------------------------
# NEIGHBOR CONTEXT + LISTS
# -----------------------------

def apply_neighbor_context(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for i in range(1, len(df) - 1):
        raw = df.at[i, "PrimaryOwnerName"]
        name = "" if pd.isna(raw) else str(raw)
        if "[MISSING" in name or (name and not name[0].isalpha()):
            prev_name = str(df.at[i - 1, "PrimaryOwnerName"])
            next_name = str(df.at[i + 1, "PrimaryOwnerName"])
            if "," in prev_name and "," in next_name:
                prev_last = prev_name.split(",")[0].strip().upper()
                next_last = next_name.split(",")[0].strip().upper()
                if prev_last == next_last and prev_last:
                    garbage = name.replace("[MISSING - CHECK PDF]", "").strip()
                    df.at[i, "PrimaryOwnerName"] = f"{prev_last}, {garbage} [Context Fix]"
                    df.at[i, "LastName"] = prev_last
    return df

def total_owners_on_file(primary: str, secondary: str) -> int:
    return 2 if normalize_ws(secondary) else 1

def compute_likely_burials(items: List[Dict]) -> int:
    used_counts: List[int] = []
    for it in items:
        if not it.get("Include", False):
            continue
        if not it.get("IsProperty", False):
            continue
        txt = (it.get("LineText", "") or "").upper()
        if re.search(r"\bX\b", txt) or re.search(r"\bUSED\b", txt):
            used_counts.append(1)
        ru = it.get("RightsUsed", None)
        if ru is not None:
            try:
                if not pd.isna(ru):
                    used_counts.append(int(ru))
            except Exception:
                pass
    return max(used_counts) if used_counts else 0


# -----------------------------
# DATASET PROCESSOR
# -----------------------------

def process_dataset(pdf_path: str, out_path: str, dpi: int = 300, kraken_model: str = '', kraken_bin: str = 'kraken', alt_ocr_order: str = 'ocrmac_then_kraken'):
    if not os.path.exists(pdf_path):
        print(f"Error: {pdf_path} not found.")
        return

    filename = os.path.basename(pdf_path)
    target_char = filename[0].upper() if filename and filename[0].isalpha() else None
    record_prefix = target_char if target_char else "A"

    print(f"\n[Info] Processing '{filename}' | Target: '{target_char}' | Prefix: {record_prefix}")

    try:
        reader = PdfReader(pdf_path)
    except Exception:
        reader = None

    try:
        page_count = pdfinfo_from_path(pdf_path)["Pages"]
    except Exception:
        page_count = len(reader.pages) if reader is not None else len(convert_from_path(pdf_path, dpi=50))

    owners_rows: List[Dict] = []
    items_rows: List[Dict] = []
    interment_rows: List[Dict] = []

    for p in tqdm(range(page_count), desc=f"Scanning {filename}", unit="page"):
        owner_data, items_data, is_interment = process_page(pdf_path, p, dpi, target_char, reader=reader, kraken_model=kraken_model, kraken_bin=kraken_bin, alt_ocr_order=alt_ocr_order)

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
                (owner_data.get("ZIP", "") or ""),
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

    if not owners_df.empty:
        owners_df = apply_neighbor_context(owners_df)

    owners_df = force_string_cols(owners_df, ["ZIP", "OwnerRecordID", "OwnerGroupKey"])
    if not interment_df.empty:
        interment_df = force_string_cols(interment_df, ["ZIP", "OwnerRecordID"])

    possible_dups = pd.DataFrame()
    if not owners_df.empty and "RawTextHash" in owners_df.columns:
        dup = owners_df.groupby("RawTextHash").size().reset_index(name="Count")
        dup = dup[dup["Count"] > 1]
        if not dup.empty:
            possible_dups = owners_df.merge(dup, on="RawTextHash", how="inner").sort_values(["RawTextHash", "PageNumber"])

    inc = items_df[items_df.get("Include", False) == True].copy() if not items_df.empty else pd.DataFrame()

    def agg_owner(group: pd.DataFrame) -> pd.Series:
        has_property = bool(group.get("IsProperty", False).any())
        has_memorial = bool(group.get("IsMemorial", False).any())
        has_pn = bool(group.get("IsFuneralPreneed", False).any())
        has_an = bool(group.get("IsAtNeedFuneral", False).any())

        memorial_lines = group[group.get("IsMemorial", False) == True]["LineText"].tolist() if "LineText" in group else []
        pn_lines = group[group.get("IsFuneralPreneed", False) == True]["LineText"].tolist() if "LineText" in group else []
        an_lines = group[group.get("IsAtNeedFuneral", False) == True]["LineText"].tolist() if "LineText" in group else []
        property_lines = group[group.get("IsProperty", False) == True]["LineText"].tolist() if "LineText" in group else []

        likely_burials = compute_likely_burials(group.to_dict("records"))

        matching_owner = owners_df[owners_df["OwnerRecordID"] == group.name]
        if not matching_owner.empty:
            total_owners = total_owners_on_file(matching_owner.iloc[0].get("PrimaryOwnerName", ""), matching_owner.iloc[0].get("SecondaryOwnerName", ""))
        else:
            total_owners = 1

        living_exists = True if int(likely_burials) < int(total_owners) else False

        pn_status = "TRUE" if has_pn else "FALSE"
        if total_owners == 2 and has_pn:
            policy_like = [ln for ln in pn_lines if re.search(r"\bpolicy\b", ln, re.IGNORECASE)]
            if len(policy_like) == 1:
                pn_status = "PARTIAL"

        needs_memorial = bool(has_property and (not has_memorial))
        needs_pn = bool(has_property and (pn_status in ["FALSE", "PARTIAL"]))
        spaces_only_prime = bool(has_property and (not has_memorial) and (pn_status in ["FALSE", "PARTIAL"]) and living_exists)
        survivor_opp = bool((total_owners == 2) and (len(an_lines) == 1) and (pn_status in ["FALSE", "PARTIAL"]) and has_property and living_exists)

        return pd.Series({
            "HasProperty": has_property,
            "HasMemorial": has_memorial,
            "HasAtNeedFuneral": has_an,
            "HasFuneralPreneedPlanStatus": pn_status,
            "LikelyBurials": int(likely_burials),
            "TotalOwnersOnFile": int(total_owners),
            "LivingOwnerExists": bool(living_exists),
            "NeedsMemorial": bool(needs_memorial),
            "NeedsPNFuneral": bool(needs_pn),
            "SpacesOnly_PRIME": bool(spaces_only_prime),
            "SurvivorSpouse_Opportunity": bool(survivor_opp),
            "MemorialEvidence": " || ".join(memorial_lines[:3]),
            "PNFuneralEvidence": " || ".join(pn_lines[:3]),
            "AtNeedEvidence": " || ".join(an_lines[:3]),
            "PropertyEvidence": " || ".join(property_lines[:3]),
        })

    owner_flags = inc.groupby("OwnerRecordID").apply(agg_owner).reset_index() if not inc.empty else pd.DataFrame(columns=["OwnerRecordID"])
    owners_master = owners_df.merge(owner_flags, on="OwnerRecordID", how="left") if not owners_df.empty else pd.DataFrame()

    defaults = {
        "HasProperty": False, "HasMemorial": False, "HasAtNeedFuneral": False,
        "HasFuneralPreneedPlanStatus": "FALSE", "LikelyBurials": 0, "TotalOwnersOnFile": 1,
        "LivingOwnerExists": True, "NeedsMemorial": False, "NeedsPNFuneral": False,
        "SpacesOnly_PRIME": False, "SurvivorSpouse_Opportunity": False,
        "MemorialEvidence": "", "PNFuneralEvidence": "", "AtNeedEvidence": "", "PropertyEvidence": "",
        "NeedsReview": "FALSE", "NeedsReviewNotes": "",
    }
    if not owners_master.empty:
        for col, default in defaults.items():
            if col in owners_master.columns:
                owners_master[col] = owners_master[col].fillna(default)

    # Lists
    if not owners_master.empty:
        list_memorial = owners_master[(owners_master["HasProperty"] == True) & (owners_master["HasMemorial"] != True) & (owners_master["LivingOwnerExists"] == True)].copy()
        list_pn = owners_master[(owners_master["HasProperty"] == True) & (owners_master["HasFuneralPreneedPlanStatus"].isin(["FALSE", "PARTIAL"])) & (owners_master["LivingOwnerExists"] == True)].copy()
        list_prime = owners_master[owners_master["SpacesOnly_PRIME"] == True].copy()
        list_survivor = owners_master[owners_master["SurvivorSpouse_Opportunity"] == True].copy()
    else:
        list_memorial = list_pn = list_prime = list_survivor = pd.DataFrame()

    # PhoneExceptions
    phone_exceptions = pd.DataFrame()
    if not owners_master.empty:
        def has_text(x):
            s = "" if x is None else str(x)
            return bool(s.strip()) and s.strip().lower() != "nan"

        phone_exceptions = owners_master[
            owners_master.get("PhoneRaw", "").apply(has_text) &
            (
                (owners_master.get("PhoneValid", False) == False) |
                (owners_master.get("PhoneNormalized", "").apply(lambda x: not has_text(x))) |
                ((owners_master.get("PhoneAltNormalized", "").apply(has_text)) & (owners_master.get("PhoneAltValid", True) == False))
            )
        ].copy()

    stats = pd.DataFrame([{
        "GeneratedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "SourceFile": filename,
        "PagesDetected": page_count,
        "OwnerRecords": int(len(owners_master)) if not owners_master.empty else 0,
        "IntermentRecordsFound": int(len(interment_df)) if not interment_df.empty else 0,
        "PossibleDuplicateScans": int(len(possible_dups)) if not possible_dups.empty else 0,
        "LIST_Memorial_Letter": int(len(list_memorial)) if not list_memorial.empty else 0,
        "LIST_PN_Funeral_Letter": int(len(list_pn)) if not list_pn.empty else 0,
        "LIST_SpacesOnly_PRIME": int(len(list_prime)) if not list_prime.empty else 0,
        "LIST_SurvivorSpouse": int(len(list_survivor)) if not list_survivor.empty else 0,
        "PhoneExceptions": int(len(phone_exceptions)) if not phone_exceptions.empty else 0,
        "NeedsReviewRows": int((owners_master.get("NeedsReview", "FALSE") == "TRUE").sum()) if not owners_master.empty else 0,
    }])

    owners_master_safe = make_df_excel_safe(owners_master) if not owners_master.empty else pd.DataFrame()
    items_df_safe = make_df_excel_safe(items_df) if not items_df.empty else pd.DataFrame()
    list_memorial_safe = make_df_excel_safe(list_memorial) if not list_memorial.empty else pd.DataFrame()
    list_pn_safe = make_df_excel_safe(list_pn) if not list_pn.empty else pd.DataFrame()
    list_prime_safe = make_df_excel_safe(list_prime) if not list_prime.empty else pd.DataFrame()
    list_survivor_safe = make_df_excel_safe(list_survivor) if not list_survivor.empty else pd.DataFrame()
    possible_dups_safe = make_df_excel_safe(possible_dups) if not possible_dups.empty else pd.DataFrame()
    stats_safe = make_df_excel_safe(stats)
    interment_safe = make_df_excel_safe(interment_df) if not interment_df.empty else pd.DataFrame()
    phone_ex_safe = make_df_excel_safe(phone_exceptions) if not phone_exceptions.empty else pd.DataFrame()

    tmp_path = out_path + ".tmp.xlsx"
    engine = choose_excel_engine()

    with pd.ExcelWriter(tmp_path, engine=engine) as xw:
        owners_master_safe.to_excel(xw, index=False, sheet_name="Owners_Master")
        if not items_df_safe.empty:
            items_df_safe.to_excel(xw, index=False, sheet_name="OwnerItems_Normalized")
        if not list_memorial_safe.empty:
            list_memorial_safe.to_excel(xw, index=False, sheet_name="LIST_Memorial_Letter")
        if not list_pn_safe.empty:
            list_pn_safe.to_excel(xw, index=False, sheet_name="LIST_PN_Funeral_Letter")
        if not list_prime_safe.empty:
            list_prime_safe.to_excel(xw, index=False, sheet_name="LIST_SpacesOnly_PRIME")
        if not list_survivor_safe.empty:
            list_survivor_safe.to_excel(xw, index=False, sheet_name="LIST_SurvivorSpouse_Opp")
        if not possible_dups_safe.empty:
            possible_dups_safe.to_excel(xw, index=False, sheet_name="PossibleDuplicateScans")
        stats_safe.to_excel(xw, index=False, sheet_name="Stats")
        if not interment_safe.empty:
            interment_safe.to_excel(xw, index=False, sheet_name="LIST_Refile_IntermentRecords")
        if not phone_ex_safe.empty:
            phone_ex_safe.to_excel(xw, index=False, sheet_name="PhoneExceptions")

    os.replace(tmp_path, out_path)
    print(f"✅ Wrote: {out_path}")


# -----------------------------
# CLI
# -----------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", help="Input scanned PDF")
    ap.add_argument("--out", help="Output Excel path")
    ap.add_argument("--dpi", type=int, default=300, help="OCR render DPI (fallback only)")
    ap.add_argument("--kraken-model", default="", help="Optional: path to Kraken recognition model (.mlmodel) for handwriting last resort")
    ap.add_argument("--kraken-bin", default="kraken", help="Kraken CLI executable (default: kraken)")
    ap.add_argument("--alt-ocr", default="ocrmac_then_kraken", help="Alternate OCR order (last resort): ocrmac_then_kraken|kraken_then_ocrmac|none")
    args, _ = ap.parse_known_args()
    # --- Drag/drop friendly: positional PDF/env defaults/auto-detect ---
    positional = _[:]
    if (not args.pdf) and positional:
        for tok in positional:
            if isinstance(tok, str) and tok.lower().endswith('.pdf') and os.path.exists(tok):
                args.pdf = tok
                break
    script_dir = os.path.dirname(os.path.abspath(__file__))
    if not args.pdf:
        args.pdf = _auto_pick_pdf(script_dir)
    if args.pdf and (not args.out):
        args.out = _default_out_path(args.pdf)
    if not args.kraken_model:
        args.kraken_model = _auto_pick_kraken_model()


    if args.pdf:
        out = args.out if args.out else args.pdf.replace(".pdf", ".xlsx")
        process_dataset(args.pdf, out, args.dpi, kraken_model=args.kraken_model, kraken_bin=args.kraken_bin, alt_ocr_order=args.alt_ocr)
        return

    script_dir = os.path.dirname(os.path.abspath(__file__))
    pdf_files = sorted(glob.glob(os.path.join(script_dir, "* (all).pdf")))
    if not pdf_files:
        pdf_files = sorted(glob.glob("* (all).pdf"))
        if not pdf_files:
            print("❌ No '* (all).pdf' files found.")
            return

    print(f"✅ Found {len(pdf_files)} PDF(s): {[os.path.basename(f) for f in pdf_files]}")
    for pdf_path in pdf_files:
        filename = os.path.basename(pdf_path)
        letter = filename.split(" ")[0]
        out_path = os.path.join(os.path.dirname(pdf_path), f"OwnerCards_{letter}_Output.xlsx")
        process_dataset(pdf_path, out_path, dpi=args.dpi, kraken_model=args.kraken_model, kraken_bin=args.kraken_bin, alt_ocr_order=args.alt_ocr)


if __name__ == "__main__":
    main()
