
#!/usr/bin/env python3
"""
Owner Card Pipeline — v44 (PASTE-READY)
Text-layer-first + Middle Initial Preservation + PhoneRaw/Normalized + PhoneExceptions

Requirements (Mac):
  brew install poppler tesseract
  pip install pandas tqdm pdf2image pytesseract pillow opencv-python PyPDF2 openpyxl xlsxwriter

Run:
  python3 owner_cards_pipeline.py
  (Auto-processes any "X (all).pdf" in the same folder)
or:
  python3 owner_cards_pipeline.py --pdf "B (all).pdf" --out "OwnerCards_B_Output.xlsx"
"""

import argparse
import glob
import hashlib
import os
import re
import unicodedata
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
    # common TN typos only (kept conservative)
    "TENN": "TN", "TENNESSES": "TN", "TEN": "TN", "TENN.": "TN", "TN.": "TN", "TIN": "TN",
}

CITY_BLOCKLIST = [
    "NASHVILLE", "BRENTWOOD", "FRANKLIN", "MADISON", "ANTIOCH",
    "HERMITAGE", "OLD HICKORY", "GOODLETTSVILLE", "PEGRAM",
    "CLARKSVILLE", "MURFREESBORO", "LEBANON", "GALLATIN", "FAIRVIEW",
    "WHITE BLUFF", "CENTERVILLE", "CHAPEL HILL"
]

US_STATE_RE = r"\b(" + "|".join(sorted(set(list(STATE_MAP.keys()) + list(STATE_MAP.values())), key=len, reverse=True)) + r")\b"
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
    r"\binterment\b", r"\bopening\b", r"\bclosing\b", r"\bo/?c\b",
    r"\bsetting\b", r"\binstallation\b",
]

TRANSFER_CANCEL_PATTERNS = [
    r"\bcancel(?:led|ed)?\b", r"\bvoid\b", r"\bno\s+longer\b",
    r"\brefunded\b", r"\btransfe?r(?:red|ed)?\b",
]

RIGHTS_NOTATION_RE = re.compile(r"\b(\d+)\s*/\s*(\d+)\b")

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
# COMPILED PATTERNS
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


# -----------------------------
# INITIAL PRESERVATION (B->8, etc.)
# -----------------------------

INITIAL_DIGIT_MAP = {"8": "B", "0": "O", "1": "I", "2": "Z", "5": "S", "6": "G", "9": "P"}

def fix_digit_initials_in_name(line: str) -> str:
    """Fix single digit tokens used as initials: 'Cynthia 8.' -> 'Cynthia B.'"""
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
    """Fix '8ABBITT' -> 'BABBITT' only when it maps and matches the target_char."""
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
# TEXT / ADDRESS HELPERS
# -----------------------------

def normalize_state(st: str) -> str:
    if not st:
        return ""
    st_clean = st.upper().replace(".", "").strip()
    return STATE_MAP.get(st_clean, st_clean)

def extract_zip_state(line: str) -> Tuple[Optional[str], Optional[str]]:
    zipm = re.search(ZIP_RE, line or "")
    statem = re.search(US_STATE_RE, line or "", flags=re.IGNORECASE)
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
# PHONE: Raw + Normalized (+ alt) + flags (no fake 615)
# -----------------------------

PHONE_PATTERN = re.compile(
    r"(?:(?:\+?1[\s\-\.])?\(?\d{3}\)?[\s\-\.]?\d{3}[\s\-\.]?\d{4})|\b\d{3}[\s\-\.]?\d{4}\b"
)

def _digits_only(s: str) -> str:
    return re.sub(r"\D", "", s or "")

def _normalize_phone_digits(d: str) -> Tuple[str, bool, bool]:
    """(normalized, has_area, valid) for 10-digit or 7-digit; handles leading 1."""
    d = d or ""
    if len(d) == 11 and d.startswith("1"):
        d = d[1:]
    if len(d) == 10:
        return f"({d[0:3]}) {d[3:6]}-{d[6:10]}", True, True
    if len(d) == 7:
        return f"{d[0:3]}-{d[3:7]}", False, True
    return "", False, False

def extract_phone_fields(full_text: str, lines: List[str]) -> Dict[str, object]:
    """
    Prefer header region first; fallback to full text.
    Returns:
      PhoneRaw, PhoneNormalized, PhoneAltNormalized,
      PhoneHasAreaCode, PhoneAltHasAreaCode,
      PhoneValid, PhoneAltValid
    """
    header_text = "\n".join(lines[:18]) if lines else (full_text or "")
    matches = [m.group(0) for m in PHONE_PATTERN.finditer(header_text)]
    if not matches:
        matches = [m.group(0) for m in PHONE_PATTERN.finditer(full_text or "")]

    seen_digits = set()
    seen_raw = set()
    raw_candidates = []
    candidates = []
    for raw in matches:
        raw_norm = normalize_ws(raw)
        if raw_norm and raw_norm not in seen_raw:
            raw_candidates.append(raw_norm)
            seen_raw.add(raw_norm)
        d = _digits_only(raw)
        if not d or d in seen_digits:
            continue
        seen_digits.add(d)
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

    phone_raw = " | ".join(raw_candidates[:2])

    return {
        "PhoneRaw": phone_raw,
        "Phone": primary[1] if primary[1] else "",              # legacy convenience
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
# PDF TEXT LAYER (Adobe OCR)
# -----------------------------

def extract_pdf_text_page(pdf_path: str, page_index: int, reader: Optional[PdfReader] = None) -> str:
    try:
        pdf_reader = reader if reader is not None else PdfReader(pdf_path)
        if page_index >= len(pdf_reader.pages):
            return ""
        return pdf_reader.pages[page_index].extract_text() or ""
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
# OCR / IMAGE HELPERS (fallback)
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

    groups: Dict[Tuple[int, int, int], List[Dict]] = {}
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
    if lines is None:
        return segs
    for l in lines[:, 0, :]:
        x1, y1, x2, y2 = map(int, l)
        if abs(y2 - y1) <= 3 and abs(x2 - x1) >= 80:
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


# -----------------------------
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
    line = fix_leading_digit_as_letter(line, target_char)

    # Remove known city spillover
    up = line.upper()
    for city in CITY_BLOCKLIST:
        idx = up.find(city)
        if idx != -1:
            line = line[:idx]
            break

    if "#" in line:
        line = line.split("#")[0]

    # Cut at address keywords
    m_kw = re.search(
        r"\b(road|rd|street|st|avenue|ave|drive|dr|lane|ln|court|ct|blvd|boulevard|pkwy|parkway|hwy|highway|trl|trail|cir|circle|pl|place|po\s*box|box)\b",
        line, re.IGNORECASE
    )
    if m_kw:
        line = line[:m_kw.start()]

    # Cut if numeric address begins
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

def parse_best_address(lines: List[str]) -> Dict:
    candidates = []
    for i, line in enumerate(lines):
        z, st = extract_zip_state(line)
        if z and st:
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
    street = best["Street"] if best["Score"] > 0 else ""

    city = ""
    if best["State"]:
        m = re.search(US_STATE_RE, best["CityStateZip"], re.IGNORECASE)
        if m:
            city_part = best["CityStateZip"][:m.start()]
            city = normalize_ws(city_part).replace(",", "")

    return {
        "Index": best["Index"],
        "Street": street,
        "City": city,
        "State": best["State"],
        "ZIP": best["ZIP"],
        "AddressRaw": f"{street} | {best['CityStateZip']}"
    }

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
    if not clean_header:
        clean_header = get_header_candidate(lines, addr_idx, target_char, aggressive=True)

    header_text = normalize_ws(" ".join(clean_header))
    header_text = re.sub(r"\b(owner|address|phone|lot|section|space|card)\b[:\-]?", "", header_text, flags=re.IGNORECASE).strip()

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

    if "," in primary:
        last_name = primary.split(",")[0].strip()
    elif " " in primary:
        last_name = primary.split(" ")[0].strip()
    else:
        last_name = primary

    return (header_text, primary, secondary, last_name, addr_info, False)


# -----------------------------
# ITEM PARSING / CLASSIFICATION
# -----------------------------

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

def extract_property_details(line: str) -> Dict[str, str]:
    details = {"Garden": "", "Section": "", "Lot": "", "Space": ""}
    if not line:
        return details
    for p_re in RE_GARDEN_CHECK:
        m = p_re.search(line)
        if m:
            details["Garden"] = normalize_ws(m.group(0))
            break
    m_sec = re.search(r"\b(sec|section|blk|block)\.?\s*([A-Za-z0-9\-]+)", line, re.IGNORECASE)
    if m_sec:
        details["Section"] = m_sec.group(2)
    m_lot = re.search(r"\b(lot)\.?\s*([A-Za-z0-9\-]+)", line, re.IGNORECASE)
    if m_lot:
        details["Lot"] = m_lot.group(2)
    m_sp = re.search(r"\b(sp|space|grave)\.?\s*([A-Za-z0-9\-\./]+)", line, re.IGNORECASE)
    if m_sp:
        details["Space"] = m_sp.group(2)
    return details

def item_dict_from_line(txt: str, struck: bool = False) -> Dict:
    excludable = is_excludable_item(txt)
    cls = classify_item(txt)
    looks_item = (cls["IsProperty"] or cls["IsMemorial"] or cls["IsFuneralPreneed"] or cls["IsAtNeedFuneral"] or cls["HasRightsNotation"])
    include = looks_item and (not struck) and (not excludable)

    rt = rights_used_total(txt)
    prop_details = {"Garden": "", "Section": "", "Lot": "", "Space": ""}
    if cls["IsProperty"]:
        prop_details = extract_property_details(txt)

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
        "Prop_Garden": prop_details["Garden"],
        "Prop_Section": prop_details["Section"],
        "Prop_Lot": prop_details["Lot"],
        "Prop_Space": prop_details["Space"]
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

    # legacy
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
# OCR PASS SCORING (fallback selection)
# -----------------------------

def score_text_pass(txt: str) -> int:
    if not txt:
        return 0
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
# PAGE PROCESSOR
# -----------------------------

def process_page(
    pdf_path: str,
    page_index: int,
    dpi: int,
    target_char: Optional[str],
    reader: Optional[PdfReader] = None,
) -> Tuple[Dict, List[Dict], bool]:
    # TEXT LAYER FIRST
    pdf_text = extract_pdf_text_page(pdf_path, page_index, reader=reader)
    if text_layer_usable(pdf_text):
        txt = pdf_text
        lines = split_lines(txt)
        template_type = detect_template_type(txt)
        _, p, s, last, addr, is_interment = parse_owner_header(lines, target_char)
        phone_fields = extract_phone_fields(txt, lines)
        items = [] if is_interment else parse_items_from_text(lines, template_type)

        owner_out = {
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
            "TextSource": "PDF_TEXT_LAYER",
        }
        owner_out.update(phone_fields)
        return owner_out, items, is_interment

    # OCR FALLBACK
    imgs = convert_from_path(pdf_path, dpi=dpi, first_page=page_index + 1, last_page=page_index + 1)
    if not imgs:
        raise RuntimeError(f"Failed to render page {page_index+1}")
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
    best_name, best_text, best_score = sorted(
        [(n, t, score_text_pass(t)) for (n, t) in candidates],
        key=lambda x: x[2], reverse=True
    )[0]

    lines_best = split_lines(best_text)
    template_type = detect_template_type(best_text)
    _, p, s, last, addr, is_interment = parse_owner_header(lines_best, target_char)
    phone_fields = extract_phone_fields(best_text, lines_best)

    # build items from OCR boxes (std + clahe), dedupe by bbox+text
    d_std = pytesseract.image_to_data(pil_std, config=OCR_PSM6, output_type=Output.DICT)
    d_clahe = pytesseract.image_to_data(pil_clahe, config=OCR_PSM6, output_type=Output.DICT)
    raw_lines_a = group_text_lines_from_ocr(d_std)
    raw_lines_b = group_text_lines_from_ocr(d_clahe)
    strike_segs = detect_horizontal_strikelines(orig_bgr)

    all_items: List[Dict] = []
    seen = set()

    def add_lines(lines_ocr: List[Dict]):
        for ln_obj in lines_ocr:
            txt_line = ln_obj["text"]
            if not txt_line:
                continue
            x1, y1, x2, y2 = ln_obj["bbox"]
            key = sha1_text(f"{normalize_ws(txt_line)}|{round(x1,-1)}|{round(y1,-1)}|{round(x2,-1)}|{round(y2,-1)}")
            if key in seen:
                continue
            struck = line_is_struck(ln_obj["bbox"], strike_segs)
            all_items.append(item_dict_from_line(txt_line, struck=struck))
            seen.add(key)

    add_lines(raw_lines_b)
    add_lines(raw_lines_a)

    combined_raw = "\n".join([t_std, t_clahe, t_ghost, t_sparse])

    owner_out = {
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
    }
    owner_out.update(phone_fields)
    return owner_out, all_items, is_interment


# -----------------------------
# NEIGHBOR CONTEXT (retained)
# -----------------------------

def apply_neighbor_context(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for i in range(1, len(df) - 1):
        raw_name = df.at[i, "PrimaryOwnerName"]
        name = "" if pd.isna(raw_name) else str(raw_name)
        missing = (not name.strip()) or (name.strip().lower() == "nan")
        if missing or "[MISSING" in name or (len(name) > 0 and not name[0].isalpha()):
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


# -----------------------------
# SUMMARY / LIST LOGIC (retained)
# -----------------------------

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

def process_dataset(pdf_path: str, out_path: str, dpi: int = 300):
    if not os.path.exists(pdf_path):
        print(f"Error: {pdf_path} not found.")
        return

    filename = os.path.basename(pdf_path)
    target_char = filename[0].upper() if filename and filename[0].isalpha() else None
    record_prefix = target_char if target_char else "A"

    print(f"\n[Info] Processing '{filename}' | Target: '{target_char}' | Prefix: {record_prefix}")

    pdf_reader = None
    try:
        pdf_reader = PdfReader(pdf_path)
    except Exception:
        pdf_reader = None

    # page count fast path
    try:
        info = pdfinfo_from_path(pdf_path)
        page_count = info["Pages"]
    except Exception:
        try:
            if pdf_reader is not None:
                page_count = len(pdf_reader.pages)
            else:
                page_count = len(PdfReader(pdf_path).pages)
        except Exception:
            thumbs = convert_from_path(pdf_path, dpi=50)
            page_count = len(thumbs)

    owners_rows = []
    items_rows = []
    interment_rows = []

    for p in tqdm(range(page_count), desc=f"Scanning {filename}", unit="page"):
        owner_data, items_data, is_interment = process_page(pdf_path, p, dpi, target_char, reader=pdf_reader)

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

    if not owners_df.empty:
        owners_df = apply_neighbor_context(owners_df)

    # Force ZIP and IDs to string for Excel fidelity
    owners_df = force_string_cols(owners_df, ["ZIP", "OwnerRecordID", "OwnerGroupKey"])
    if not interment_df.empty:
        interment_df = force_string_cols(interment_df, ["ZIP", "OwnerRecordID"])

    # duplicates by RawTextHash
    possible_dups = pd.DataFrame()
    if not owners_df.empty and "RawTextHash" in owners_df.columns:
        dup = owners_df.groupby("RawTextHash").size().reset_index(name="Count")
        dup = dup[dup["Count"] > 1]
        if not dup.empty:
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
            matching_owners.iloc[0].get("PrimaryOwnerName", ""),
            matching_owners.iloc[0].get("SecondaryOwnerName", "")
        ) if not matching_owners.empty else 1

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

    owner_flags = pd.DataFrame(columns=["OwnerRecordID"])
    if not inc.empty:
        owner_flags = inc.groupby("OwnerRecordID").apply(agg_owner).reset_index()

    owners_master = owners_df.merge(owner_flags, on="OwnerRecordID", how="left") if not owners_df.empty else pd.DataFrame()

    defaults = {
        "HasProperty": False, "HasMemorial": False, "HasAtNeedFuneral": False,
        "HasFuneralPreneedPlanStatus": "FALSE", "LikelyBurials": 0, "TotalOwnersOnFile": 1,
        "LivingOwnerExists": True, "NeedsMemorial": False, "NeedsPNFuneral": False,
        "SpacesOnly_PRIME": False, "SurvivorSpouse_Opportunity": False,
        "MemorialEvidence": "", "PNFuneralEvidence": "", "AtNeedEvidence": "", "PropertyEvidence": "",
    }
    if not owners_master.empty:
        for col, default in defaults.items():
            if col in owners_master.columns:
                owners_master[col] = owners_master[col].fillna(default)

    owners_master = force_string_cols(owners_master, ["ZIP", "OwnerRecordID", "OwnerGroupKey"])

    # Lists
    if not owners_master.empty:
        list_memorial = owners_master[(owners_master["HasProperty"] == True) & (owners_master["HasMemorial"] != True) & (owners_master["LivingOwnerExists"] == True)].copy()
        list_pn = owners_master[(owners_master["HasProperty"] == True) & (owners_master["HasFuneralPreneedPlanStatus"].isin(["FALSE", "PARTIAL"])) & (owners_master["LivingOwnerExists"] == True)].copy()
        list_prime = owners_master[owners_master["SpacesOnly_PRIME"] == True].copy()
        list_survivor = owners_master[owners_master["SurvivorSpouse_Opportunity"] == True].copy()
    else:
        list_memorial = pd.DataFrame()
        list_pn = pd.DataFrame()
        list_prime = pd.DataFrame()
        list_survivor = pd.DataFrame()

    # Phone Exceptions sheet
    phone_exceptions = pd.DataFrame()
    if not owners_master.empty:
        def has_text(x):
            s = "" if x is None else str(x)
            return s.strip() and s.strip().lower() != "nan"

        phone_exceptions = owners_master[
            (owners_master.get("PhoneRaw", "").apply(has_text)) &
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
    }])

    # Excel safe conversion at end
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
    print(f"Writing Excel to: {out_path} ...")

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

    try:
        os.replace(tmp_path, out_path)
    except PermissionError:
        print(f"\n❌ ERROR: Could not overwrite '{out_path}'. It may be open in Excel.")


# -----------------------------
# CLI
# -----------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", help="Input scanned PDF")
    ap.add_argument("--out", help="Output Excel path")
    ap.add_argument("--dpi", type=int, default=300, help="OCR render DPI (fallback only)")
    args, _ = ap.parse_known_args()

    if args.pdf:
        out = args.out if args.out else args.pdf.replace(".pdf", ".xlsx")
        process_dataset(args.pdf, out, args.dpi)
        print("\nDONE ✅")
        return

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

    print(f"✅ Found {len(pdf_files)} PDF(s): {[os.path.basename(f) for f in pdf_files]}")

    for pdf_path in pdf_files:
        filename = os.path.basename(pdf_path)
        letter = filename.split(" ")[0]
        output_dir = os.path.dirname(pdf_path)
        out_path = os.path.join(output_dir, f"OwnerCards_{letter}_Output.xlsx")
        process_dataset(pdf_path, out_path, args.dpi)

    print("\nALL FILES PROCESSED SUCCESSFULLY ✅")


if __name__ == "__main__":
    main()
