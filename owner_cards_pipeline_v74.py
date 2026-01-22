#!/usr/bin/env python3
"""Owner Card Pipeline — v74 (Dimensions Guard + Better FaCTS Matching + Address Notes Split)

This version implements the issues you identified:
1) Strikethrough detection parity: when using PDF text layer, we now run a quick strike-line scan.
   - If strike lines are detected on the page, we OCR the page (data mode) to build item lines with struck flags.
   - Owner/header/address still come from the text layer (fast + accurate), but item inclusion honors strike-through.
2) Single-line address support: parse_best_address now handles inline "street, city ST ZIP" (same line).
3) Phone extensions: PHONE_PATTERN now matches optional extensions (ext/x), and we strip extensions for normalization.

Also includes the Py3.14 regex fix (hyphen placed at end in character class).

Run:
  python3 owner_cards_pipeline_v72.py
  or
  python3 owner_cards_pipeline_v72.py --pdf "B (all).pdf" --out "OwnerCards_B_Output.xlsx"

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
from functools import lru_cache
from datetime import datetime
import difflib
from pathlib import Path
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


DEBUG_LOG = os.getenv("OWNERCARDS_DEBUG", "").strip().lower() in {"1", "true", "yes"}

def _debug_log(msg: str) -> None:
    if DEBUG_LOG:
        print(f"[owner_cards_pipeline] {msg}")



# --- OCR safety: prevent hangs (v70.10) ---
TESS_TIMEOUT_SEC = 25  # hard timeout for pytesseract calls

def _tess_image_to_string(img, config: str):
    """pytesseract.image_to_string with a hard timeout"""
    try:
        return pytesseract.image_to_string(img, config=config, timeout=TESS_TIMEOUT_SEC)
    except Exception:
        return ''

def _tess_image_to_data(img, config: str):
    """pytesseract.image_to_data with a hard timeout"""
    try:
        return pytesseract.image_to_data(
            img,
            config=config,
            output_type=Output.DICT,
            timeout=TESS_TIMEOUT_SEC,
        )
    except Exception:
        return {'text': [], 'conf': [], 'left': [], 'top': [], 'width': [], 'height': [], 'line_num': [], 'block_num': [], 'par_num': []}

def _tess_image_to_osd(img) -> str:
    """pytesseract.image_to_osd with a hard timeout"""
    try:
        return pytesseract.image_to_osd(img, timeout=TESS_TIMEOUT_SEC)
    except Exception:
        return ''

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
    """Default output next to the script/batch folder (not the CWD)."""
    base_name = os.path.splitext(os.path.basename(pdf_path))[0]
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, base_name + '_Output.xlsx')


# -----------------------------
# VISUAL SNIPER HELPERS
# -----------------------------
FAILED_SNAPSHOT_DIR = "_Failed_Snapshots"

# --- Snapshot location: keep with script/batch folder (v70.10) ---
def _set_snapshot_dir_for_script():
    """Force snapshots to the folder containing this .py file (batch folder)."""
    global FAILED_SNAPSHOT_DIR
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        FAILED_SNAPSHOT_DIR = os.path.join(base_dir, '_Failed_Snapshots')
        _ensure_dir(FAILED_SNAPSHOT_DIR)
    except Exception:
        pass


# -----------------------------
# FaCTS SECTION (Garden) VALIDATION
# -----------------------------

@lru_cache(maxsize=2048)
def normalize_section_name(name: str) -> str:
    if name is None:
        return ''
    s = str(name)
    s = unicodedata.normalize('NFKD', s)
    s = s.upper()
    s = s.replace('&', ' AND ')
    # common abbreviations
    s = re.sub(r'\bMT\.?\b', 'MOUNT', s)
    s = re.sub(r'\bGD\.?\b', 'GOOD', s)
    s = re.sub(r'[^A-Z0-9 ]+', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s




# --- v74 helpers: garden abbreviations + blank header detection ---
GARDEN_ABBREV_CANON = {
    'SM': 'SERMON ON THE MOUNT',
    'SOM': 'SERMON ON THE MOUNT',
    'LS': 'LAST SUPPER',
    'AT': 'ATONEMENT',
    'A': 'ATONEMENT',
    'GS': 'GOOD SHEPHERD',
    'EL': 'EVERLASTING LIFE',
    'CH': 'CHAPEL HILL',
    'CHE': 'CHAPEL HILL EAST',
    'CHW': 'CHAPEL HILL WEST',
    'CH-E': 'CHAPEL HILL EAST',
    'CH-W': 'CHAPEL HILL WEST',
}

GARDEN_OCR_FIXES = {
    'GETHSERNANE': 'GETHSEMANE',
    'GETHSERNAME': 'GETHSEMANE',
    'GETHSERNA NE': 'GETHSEMANE',
    'SERM0N': 'SERMON',
    'M0UNT': 'MOUNT',
    'SERMON ON MOUNT': 'SERMON ON THE MOUNT',
    'SERMON ON MT': 'SERMON ON THE MOUNT',
}

def normalize_section_candidate(text: str) -> str:
    base = normalize_section_name(text)
    if not base:
        return base
    for bad, good in GARDEN_OCR_FIXES.items():
        base = base.replace(bad, good)
    return base

def _abbr_from_section(sec_norm: str) -> str:
    if not sec_norm:
        return ''
    stop = {'OF','ON','THE','AND','&','IN'}
    toks = [t for t in sec_norm.split() if t and t not in stop]
    if not toks:
        return ''
    if len(toks) == 1:
        return toks[0][:3]
    return ''.join(t[0] for t in toks)

def build_facts_abbrev_index(facts_sections, facts_index=None):
    idx = facts_index if facts_index is not None else build_facts_section_index(facts_sections or [])
    m = {}
    for sec, nsec in idx:
        ab = _abbr_from_section(nsec)
        if ab:
            m.setdefault(ab, set()).add(sec)
        for k, v in GARDEN_ABBREV_CANON.items():
            if normalize_section_name(v) == nsec:
                m.setdefault(re.sub(r'[^A-Z0-9]', '', k.upper()), set()).add(sec)
    return m

def extract_abbrev_tokens(text: str):
    if not text:
        return []
    u = str(text).upper()
    raw = re.findall(r"\b[A-Z]{1,3}(?:-[A-Z]{1,3})?\b|\b(?:[A-Z]\.){2,4}\b", u)
    toks=[]
    for t in raw:
        t2 = re.sub(r'[^A-Z0-9-]', '', t.replace('.', ''))
        if not t2:
            continue
        if t2 in {'SP','SEC','LOT','BLK','B','C','D','E','W','N','S'}:
            continue
        toks.append(t2)
    seen=set(); out=[]
    for t in toks:
        if t not in seen:
            seen.add(t); out.append(t)
    return out

def is_blank_header(pil_page: Image.Image, header_ratio: float = 0.35) -> tuple[bool, float]:
    """Return (is_blank, dark_pixel_ratio) on header crop."""
    try:
        w, h = pil_page.size
        header = pil_page.crop((0, 0, w, int(h * header_ratio))).convert('L')
        arr = np.array(header)
        dark = (arr < 160).sum()
        ratio = float(dark) / float(max(arr.size, 1))
        return (ratio < 0.0035, ratio)
    except Exception:
        return (False, 0.0)

def load_facts_sections(facts_path: str) -> List[str]:
    """Load unique FaCTS Section values (Garden names) from the inventory export."""
    if not facts_path:
        return []
    if not os.path.exists(facts_path):
        return []
    try:
        df = pd.read_excel(facts_path, engine='openpyxl', sheet_name=0, header=2, usecols=['Section'])
        secs = df['Section'].dropna().astype(str).str.strip()
        secs = [s for s in secs.tolist() if s and s.lower() != 'section']
        # unique preserving order
        seen = set()
        out = []
        for s in secs:
            if s not in seen:
                seen.add(s)
                out.append(s)
        return out
    except Exception:
        return []


def build_facts_section_index(facts_sections: List[str]) -> List[Tuple[str, str]]:
    """Precompute normalized section names for faster matching."""
    out = []
    for sec in facts_sections or []:
        nsec = normalize_section_name(sec)
        if not nsec:
            continue
        out.append((sec, nsec))
    return out



def match_facts_section_from_line(line_text: str, facts_sections: List[str], facts_index: Optional[List[Tuple[str, str]]] = None) -> Tuple[str, float, str]:
    """v74: match FaCTS section from a property line, including garden abbreviations (SM/LS/AT/A)."""
    if not line_text or not facts_sections:
        return ('', 0.0, '')
    if is_dimension_token(line_text):
        return ('', 0.0, 'dimension')

    norm_line = normalize_section_candidate(line_text)
    idx = facts_index if facts_index is not None else build_facts_section_index(facts_sections)
    abbr_index = build_facts_abbrev_index(facts_sections, facts_index=idx)

    for sec, nsec in idx:
        if nsec and nsec in norm_line:
            return (sec, 1.0, 'exact')

    raw_u = str(line_text or '').upper().strip()

    # abbreviation match (unique only)
    try:
        for tok in extract_abbrev_tokens(raw_u):
            key = re.sub(r'[^A-Z0-9]', '', tok)
            cands = list(abbr_index.get(key, set()) or [])
            if len(cands) == 1:
                return (cands[0], 0.86, 'abbrev_unique')
    except Exception:
        pass

    # colon/slash alpha segments
    segs = re.split(r'[:/]', raw_u)
    alpha_segs = []
    for s in segs:
        s2 = re.sub(r'[^A-Z \-]', ' ', s)
        s2 = re.sub(r'\s+', ' ', s2).strip()
        if len(s2) < 3:
            continue
        if s2 in {'LOT','SEC','SECTION','SP','SPACE','BLK','BLOCK','C','D','B','A','N','E','W','S'}:
            continue
        alpha_segs.append(s2)

    alpha_segs = sorted(set(alpha_segs), key=len, reverse=True)
    best = ('', 0.0, '')
    for cand in alpha_segs:
        candn = normalize_section_candidate(cand)
        for sec, nsec in idx:
            if not nsec:
                continue
            r = 0.88 if (candn in nsec or nsec in candn) else difflib.SequenceMatcher(None, candn, nsec).ratio()
            if r > best[1]:
                best = (sec, float(r), 'colon_fuzzy')
        if best[0] and best[1] >= 0.74:
            return best

    # trailing garden token
    m_tail = re.search(r"[:/]\s*([A-Z][A-Z \-]{3,40})\s*$", raw_u)
    if m_tail:
        cand2 = normalize_section_candidate(m_tail.group(1))
        if cand2:
            best_sec, best_ratio = '', 0.0
            for sec, nsec in idx:
                r = 0.86 if (cand2 in nsec or nsec in cand2) else difflib.SequenceMatcher(None, cand2, nsec).ratio()
                if r > best_ratio:
                    best_ratio, best_sec = r, sec
            if best_sec and best_ratio >= 0.72:
                return (best_sec, float(best_ratio), 'tail_fuzzy')

    return ('', 0.0, '')

def enrich_owner_with_review(owner: Dict, items: List[Dict], pdf_path: str, page_num: int, dpi: int, pil_page: Optional[Image.Image] = None, facts_sections: Optional[List[str]] = None, facts_mode: str = '', reader: Optional[PdfReader] = None) -> Dict:
    """Adds NeedsReview + NeedsReviewNotes + snapshot paths; validates property sections against FaCTS."""
    notes: List[str] = []
    # v74: blank/separator pages should not be NeedsReview
    if owner.get('IsBlankSeparator'):
        owner['NeedsReview'] = False
        owner['NeedsReviewNotes'] = 'BLANK_SEPARATOR'
        owner['SnapshotFull'] = ''
        owner['SnapshotHeader'] = ''
        owner['SnapshotItems'] = ''
        owner['SnapshotRotate'] = ''
        owner['SnapshotRotateScore'] = ''
        return owner
    needs_review = False

    prim = str(owner.get('PrimaryOwnerName', '') or '')
    if '[MISSING' in prim.upper():
        needs_review = True
        notes.append('NAME_MISSING')

    # Address completeness
    # v70.10: only flag missing State/ZIP when we have evidence we're looking at a mailing address.
    street = str(owner.get('Street', '') or '')
    city = str(owner.get('City', '') or '')
    addr_raw = str(owner.get('AddressRaw', '') or '')

    addr_evidence = False
    street_ok = bool(street.strip()) and bool(re.search(STREET_START_RE, street))
    city_ok = bool(city.strip())
    raw_ok = bool(addr_raw.strip())
    if street_ok or city_ok or raw_ok:
        blob = normalize_ws(f"{street} {city} {addr_raw}")
        if re.search(ZIP_RE, blob):
            addr_evidence = True
        elif re.search(US_STATE_RE, blob, re.IGNORECASE):
            addr_evidence = True
        elif re.search(r"\bPO\s*BOX\b", blob, re.IGNORECASE):
            addr_evidence = True
        elif re.search(STREET_START_RE, blob):
            addr_evidence = True

    if addr_evidence and (not owner.get('State') or not owner.get('ZIP')):
        # v72: split notes so ZIP-only missing doesn't force review
        missing_state = not bool(owner.get('State'))
        missing_zip = not bool(owner.get('ZIP'))
        if missing_state:
            needs_review = True
            notes.append('ADDR_STATE_MISSING')
        if missing_zip:
            # record, but don't force NeedsReview by itself
            notes.append('ADDR_ZIP_MISSING')

    # Property section validation
    property_lines = []
    for it in (items or []):
        try:
            if it.get('IsProperty') or (it.get('RightsNotation') or ''):
                property_lines.append(it.get('LineText', '') or '')
        except Exception:
            continue

    # v70.10: Only validate FaCTS sections on lines that actually look like they contain garden/lot/sec/space patterns.
    def _facts_candidate_line(ln: str) -> bool:
        if is_dimension_token(ln):
            return False
        if not ln:
            return False
        uln = (ln or '').upper()

        # Strong signals: explicit garden names or structured coordinates
        try:
            if RE_GARDEN_CHECK.search(ln):
                return True
        except Exception:
            pass

        if ln.count(':') >= 2:
            return True

        # Lot/Section references are meaningful even if garden is missing
        if ('LOT' in uln) or ('SEC' in uln) or ('SECTION' in uln):
            return True

        # 'SP'/'SPACE' alone is too weak for FaCTS validation; require another anchor
        has_sp = bool(re.search(r"\bSP\b", uln)) or ('SPACE' in uln)
        if has_sp and (('LOT' in uln) or ('SEC' in uln) or ('SECTION' in uln) or ln.count(':') >= 2):
            return True

        return False

    property_lines_for_validation = [ln for ln in property_lines if _facts_candidate_line(ln)]

    matched_sections = []
    unmatched_lines = []

    if property_lines_for_validation:
        facts_index = build_facts_section_index(facts_sections) if facts_sections else []
        if facts_sections:
            for ln in property_lines_for_validation:
                sec, conf, method = match_facts_section_from_line(ln, facts_sections, facts_index=facts_index)
                if sec:
                    matched_sections.append(sec)
                else:
                    unmatched_lines.append(ln)

            if unmatched_lines and not matched_sections:
                needs_review = True
                # v72: distinguish true no-match vs garden-missing
                garden_missing = []
                for ln in unmatched_lines:
                    uln = (ln or '').upper()
                    if (not RE_GARDEN_CHECK.search(uln)) and (not re.search(r'[:/].*[A-Z]{4,}', uln)):
                        garden_missing.append(ln)
                if garden_missing and len(garden_missing) == len(unmatched_lines):
                    notes.append(f'PROPERTY_SECTION_GARDEN_MISSING_{len(unmatched_lines)}')
                else:
                    notes.append(f'PROPERTY_SECTION_NO_MATCH_{len(unmatched_lines)}')
            elif unmatched_lines:
                notes.append(f'PROPERTY_SECTION_PARTIAL_{len(matched_sections)}of{len(property_lines_for_validation)}')
        else:
            if facts_mode and str(facts_mode).strip().lower() not in ('', 'not_provided', 'not provided'):
                notes.append('FACTS_SECTIONS_NOT_LOADED')

    if matched_sections:
        owner['FaCTS_SectionMatches'] = '; '.join(sorted(set(matched_sections)))
    else:
        owner['FaCTS_SectionMatches'] = ''

    owner['FaCTS_Mode'] = facts_mode or ''
    owner['NeedsReview'] = bool(needs_review)
    owner['NeedsReviewNotes'] = ' | '.join(notes)

    # Snapshots when review is needed (name missing or property mismatch)
    if needs_review:
        try:
            if pil_page is None:
                pil_page = render_page(pdf_path, page_num - 1, dpi=dpi)
            if pil_page is not None:
                paths = save_failure_snapshots(pil_page, pdf_path, page_num, reason=(notes[0] if notes else 'NEEDS_REVIEW'), reader=reader)
                owner['SnapshotFull'] = paths.get('Full', '')
                owner['SnapshotHeader'] = paths.get('Header', '')
                owner['SnapshotItems'] = paths.get('Items', '')
                owner['SnapshotRotate'] = paths.get('Rotate', '')
                owner['SnapshotRotateScore'] = paths.get('RotateScore', '')
        except Exception:
            pass
    else:
        owner['SnapshotFull'] = ''
        owner['SnapshotHeader'] = ''
        owner['SnapshotItems'] = ''
        owner['SnapshotRotate'] = ''
        owner['SnapshotRotateScore'] = ''

    return owner


def _ensure_dir(p: str):
    try:
        os.makedirs(p, exist_ok=True)
    except Exception:
        pass


def score_header_text(txt: str) -> int:
    """Score OCR text for how much it looks like an owner header (name/address/zip/phone)."""
    if not txt:
        return 0
    u = (txt or '').upper()
    score = 0
    # ZIP is a strong signal
    if re.search(ZIP_RE, u):
        score += 50
    # State is a good signal
    if re.search(US_STATE_RE, u, re.IGNORECASE):
        score += 25
    # Phone
    if PHONE_PATTERN.search(txt):
        score += 20
    # A plausible last-name, first-name pattern in first line
    top3 = split_lines(txt)[:3]
    first = top3[0] if top3 else ''
    if re.search(r"^[A-Z][A-Z\'\- ]+,\s*[A-Z]", first):
        score += 25
    # Reward non-gibberish lines
    good_lines = [ln for ln in split_lines(txt) if ln and not is_gibberish(ln)]
    score += min(len(good_lines), 40)
    return score


def _apply_pdf_rotate_metadata(pil_img: Image.Image, source_pdf: str, page_index: int, reader: Optional[PdfReader] = None) -> Image.Image:
    """Apply PDF /Rotate metadata if present so snapshots match viewer orientation."""
    try:
        r = reader if reader is not None else PdfReader(source_pdf)
        rot = int(r.pages[page_index].get("/Rotate") or 0) % 360
        if rot in (90, 180, 270):
            return pil_img.rotate(360 - rot, expand=True)
    except Exception:
        pass
    return pil_img


def _osd_rotate_header(pil_img: Image.Image, header_ratio: float = 0.35) -> tuple[Optional[Image.Image], dict]:
    """
    Try Tesseract OSD on header region only.
    Returns (rotated_image or None if no reliable OSD, meta dict)
    """
    meta = {"method": "osd", "rotate": 0, "conf": 0}
    try:
        w, h = pil_img.size
        header = pil_img.crop((0, 0, w, int(h * header_ratio)))

        # Shadow-corrected header tends to help OSD on yellowed cards
        try:
            header_pp = preprocess_shadow_correct(header)
        except Exception:
            header_pp = header.convert("L")

        osd = _tess_image_to_osd(header_pp)
        m_rot = re.search(r"Rotate:\s*(\d+)", osd)
        m_conf = re.search(r"Orientation confidence:\s*(\d+)", osd)
        rot = int(m_rot.group(1)) if m_rot else 0
        conf = int(m_conf.group(1)) if m_conf else 0
        meta["rotate"] = rot % 360
        meta["conf"] = conf

        # Only trust OSD when confidence is non-trivial
        if rot in (90, 180, 270) and conf >= 5:
            return pil_img.rotate(360 - rot, expand=True), meta
    except Exception:
        pass
    return None, meta


def _auto_rotate_by_header_score(pil_img: Image.Image, header_ratio: float = 0.35) -> tuple[Image.Image, dict]:
    """
    Robust snapshot rotation:
      1) OSD on header if confident
      2) brute-force 0/90/180/270, score header OCR
      3) Only rotate if best is clearly better; else keep 0°
    """
    # 1) OSD attempt
    osd_img, osd_meta = _osd_rotate_header(pil_img, header_ratio=header_ratio)
    if osd_img is not None:
        return osd_img, {"angle": osd_meta["rotate"], "score": "osd", "method": "osd", "osd_conf": osd_meta["conf"]}

    # 2) Brute-force scoring
    cands = []
    for angle in (0, 90, 180, 270):
        try:
            cand = pil_img.rotate(angle, expand=True) if angle else pil_img
            w, h = cand.size
            header = cand.crop((0, 0, w, int(h * header_ratio)))

            try:
                header_pp = preprocess_shadow_correct(header)
            except Exception:
                header_pp = header.convert("L")

            txt = _tess_image_to_string(header_pp, config="--oem 3 -l eng --psm 6")
            sc = score_header_text(txt)
            ratio = w / max(h, 1)
            cands.append((sc, ratio, angle, cand))
        except Exception:
            continue

    if not cands:
        return pil_img, {"angle": 0, "score": 0, "method": "none"}

    cands.sort(key=lambda x: (x[0], x[1], -x[2]), reverse=True)
    best_sc, best_ratio, best_angle, best_img = cands[0]
    second_sc = cands[1][0] if len(cands) > 1 else -999

    # 3) Confidence gating: only rotate if "clearly better"
    MIN_SCORE_TO_ROTATE = 55
    MIN_MARGIN = 8

    if best_angle != 0:
        if best_sc < MIN_SCORE_TO_ROTATE or (best_sc - second_sc) < MIN_MARGIN:
            return pil_img, {"angle": 0, "score": best_sc, "method": "bruteforce_not_confident"}
        return best_img, {"angle": best_angle, "score": best_sc, "method": "bruteforce_confident"}

    return best_img, {"angle": 0, "score": best_sc, "method": "bruteforce_0"}


def save_failure_snapshots(pil_img: Image.Image, source_pdf: str, page_num: int, reason: str = "NEEDS_REVIEW", reader: Optional[PdfReader] = None) -> Dict[str, str]:
    """Save FULL + HEADER + ITEMS snapshots (auto-rotated) for manual verification."""
    _ensure_dir(FAILED_SNAPSHOT_DIR)

    # 0) Apply PDF /Rotate metadata so snapshots match viewer orientation
    page_index = max(0, int(page_num) - 1)
    snap = _apply_pdf_rotate_metadata(pil_img, source_pdf, page_index, reader=reader)

    # 1) Auto-rotate for readability (conservative)
    rot_img, meta = _auto_rotate_by_header_score(snap, header_ratio=0.35)

    base = os.path.splitext(os.path.basename(source_pdf))[0]
    safe_reason = re.sub(r"[^A-Za-z0-9_]+", "_", reason)[:50] if reason else "REVIEW"

    w, h = rot_img.size
    header_h = int(h * 0.35)
    items_y = int(h * 0.33)

    full_name = f"FAIL_{base}_P{page_num:04d}_{safe_reason}_FULL.jpg"
    header_name = f"FAIL_{base}_P{page_num:04d}_{safe_reason}_HEADER.jpg"
    items_name = f"FAIL_{base}_P{page_num:04d}_{safe_reason}_ITEMS.jpg"

    full_path = os.path.join(FAILED_SNAPSHOT_DIR, full_name)
    header_path = os.path.join(FAILED_SNAPSHOT_DIR, header_name)
    items_path = os.path.join(FAILED_SNAPSHOT_DIR, items_name)

    try:
        rot_img.save(full_path, quality=92)
    except Exception:
        rot_img.convert('RGB').save(full_path, quality=92)

    try:
        rot_img.crop((0, 0, w, header_h)).save(header_path, quality=92)
    except Exception:
        rot_img.crop((0, 0, w, header_h)).convert('RGB').save(header_path, quality=92)

    try:
        rot_img.crop((0, items_y, w, h)).save(items_path, quality=92)
    except Exception:
        rot_img.crop((0, items_y, w, h)).convert('RGB').save(items_path, quality=92)

    return {
        "Full": full_path,
        "Header": header_path,
        "Items": items_path,
        "Rotate": str(meta.get('angle', 0)),
        "RotateScore": str(meta.get('score', 0)),
        "Reason": reason or ''
    }


def save_failure_snapshot(pil_img: Image.Image, source_pdf: str, page_num: int, reason: str = "NAME_MISSING") -> str:
    """Backward-compatible: returns the HEADER path."""
    paths = save_failure_snapshots(pil_img, source_pdf, page_num, reason=reason)
    return paths.get('Header', '')
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
STREET_START_RE = r"^\d+\s+(?![xX×]\b)[A-Za-z]"
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
# Region ratios (owner header is consistently in top 25–35% of page)
HEADER_CROP_RATIO = 0.35
ITEMS_START_RATIO = 0.33

# OCR tuning
TESS_LANG = 'eng'
TESS_CONFIG_BASE = '--oem 3 -l eng'

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


# -----------------------------
# -----------------------------------------------------------------
# DIMENSIONS (marker sizes) — normalize and guard against false "property" (v72)
# Examples: 44:X:13 -> 44x13, 24 x 10 -> 24x10, 44×13 -> 44x13
# -----------------------------------------------------------------
DIM_COLON_RE = re.compile(r"\b(\d{1,3})\s*[:/]\s*X\s*[:/]\s*(\d{1,3})\b", re.IGNORECASE)
DIM_X_RE     = re.compile(r"\b(\d{1,3})\s*[x×]\s*(\d{1,3})\b", re.IGNORECASE)

def normalize_dimensions(text: str) -> str:
    """Normalize dimension tokens like 44:X:13 or 44 x 13 to 44x13."""
    if not text:
        return text
    t = text
    t = DIM_COLON_RE.sub(lambda m: f"{m.group(1)}x{m.group(2)}", t)
    t = DIM_X_RE.sub(lambda m: f"{m.group(1)}x{m.group(2)}", t)
    return t

def is_dimension_token(text: str) -> bool:
    if not text:
        return False
    u = str(text)
    return bool(DIM_COLON_RE.search(u) or DIM_X_RE.search(u))

def first_dimension(text: str) -> str:
    """Return first normalized dimension token (e.g., '44x13') if present."""
    if not text:
        return ''
    t = normalize_dimensions(text)
    m = DIM_X_RE.search(t)
    if m:
        return f"{m.group(1)}x{m.group(2)}"
    return ''

# v70.10 PROPERTY HEURISTICS (garden + coordinate OCR fuzz)
# -----------------------------
DEFAULT_GARDENS = [
    "GOOD SHEPHERD",
    "EVERLASTING LIFE",
    "LAST SUPPER",
    "ATONEMENT",
    "PRAYER",
    "GETHSEMANE",
    "CHAPEL HILL",
    "PEACE",
    "FAITH",
    "SERENITY",
    "FOUNTAIN",
    "CROSS",
    "SERMON ON THE MOUNT",
    "LAKEVIEW ESTATES",
    "GRACE",
    "GARDEN OF GRACE",
]

def _build_garden_re(gardens):
    pats = []
    for g in (gardens or []):
        ug = safe_upper(g)
        if not ug:
            continue
        toks = [re.escape(t) for t in ug.split() if t]
        if not toks:
            continue
        pats.append(r"\b" + r"\s+".join(toks) + r"\b")
    if not pats:
        return re.compile(r"$^")
    return re.compile(r"(?:" + "|".join(pats) + r")", re.IGNORECASE)

RE_GARDEN_CHECK = _build_garden_re(DEFAULT_GARDENS)

def _update_garden_regex_from_facts(facts_sections: list):
    """Expand garden regex using DEFAULT_GARDENS plus FaCTS sections (if provided)."""
    global RE_GARDEN_CHECK
    try:
        extra = [s for s in (facts_sections or []) if isinstance(s, str) and s.strip()]
        gardens = list(DEFAULT_GARDENS) + extra
        RE_GARDEN_CHECK = _build_garden_re(gardens)
    except Exception:
        pass


COORD_OCR_FUZZ_RE = re.compile(
    r"""
    (?:\b\d{1,4}\s*[:/]\s*[A-Z0-9]{1,4}\s*[:/]\s*[0-9IL]\b) |   # 73:A:l or 73/A/1
    (?:\b\d{1,4}\s*[A-Z8]\s*[0-9IL]\b) |                         # 73B1 or 7381-ish
    (?:\b\d{1,4}\s*[A-Z8]\b\s*[0-9IL]\b)                         # 73B 1 or 738 1
    """,
    re.IGNORECASE | re.VERBOSE,
)

def heuristic_is_property(line: str) -> bool:
    """
    v70.10: Property if either:
      (A) coordinate/lot-space signature is strong, even without garden text, OR
      (B) garden name present + coordinate-ish signature

    This removes the hard gate that required a garden name from a small list.
    """
    if not line:
        return False

    u = (line or '').upper()
    has_coord = bool(COORD_OCR_FUZZ_RE.search(u))
    has_colon_struct = (u.count(':') + u.count('/')) >= 2
    has_keywords = bool(re.search(r"\b(SP\.?|SPACE|LOT|SEC\.?|SECTION|BLOCK|BLK\.?|GRAVE|CRYPT|LAWN)\b", u))
    has_garden = bool(RE_GARDEN_CHECK.search(u))

    if has_coord and (has_garden or has_keywords or has_colon_struct):
        return True
    if has_colon_struct and (has_keywords or has_garden):
        return True
    if has_coord and has_colon_struct:
        return True

    if not has_garden:
        return False

    toks = re.findall(r"[A-Z0-9]+", u)
    score = 0
    for t in toks:
        has_d = any(ch.isdigit() for ch in t)
        has_a = any(ch.isalpha() for ch in t)
        if has_d and has_a:
            score += 2
        elif has_d and len(t) <= 4:
            score += 1
    return score >= 3

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
    - Fixes extremely common TN OCR issues (1N/lN -> TN)
    - Joins punctuation-split 2-letter abbreviations only when they form a valid state code (e.g., I:N -> IN, T:N -> TN)
    """
    if not line:
        return line

    # Common OCR: '1N' for 'TN' (digit one)
    line = re.sub(r"\b1N\b", "TN", line, flags=re.IGNORECASE)
    # Lowercase ell 'lN' occasionally appears for TN
    line = re.sub(r"\blN\b", "TN", line, flags=re.IGNORECASE)

    # Join punctuation-split state abbrev like I:N or T.N
    # Only if the joined token is a valid 2-letter US state abbreviation.
    try:
        valid = {v.upper() for v in STATE_MAP.values() if isinstance(v, str) and len(v) == 2}

        def _join(mm):
            cand = (mm.group(1) + mm.group(2)).upper()
            return cand if cand in valid else mm.group(0)

        line = re.sub(r"\b([A-Z])\s*[:;\.,]\s*([A-Z])\b", _join, line, flags=re.IGNORECASE)
    except Exception:
        pass

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

def _extract_phone_extension(raw: str) -> str:
    if not raw:
        return ""
    m = re.search(r"\b(?:ext\.?|x)\s*(\d{1,6})\b", raw, flags=re.IGNORECASE)
    return m.group(1) if m else ""

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
    # v65: guard against false 10-digit phones formed by ZIP tail + 7-digit local number
    zips_in_header = re.findall(ZIP_RE, header_text)
    zip_tails = {z[-3:] for z in zips_in_header}
    header_upper = header_text.upper()
    allow_seven_digit = any(token in header_upper for token in ["PHONE", "TEL", "TELEPHONE"])
    matches = [m.group(0) for m in PHONE_PATTERN.finditer(header_text)]
    if not matches:
        matches = [m.group(0) for m in PHONE_PATTERN.finditer(full_text or "")]

    seen = set()
    candidates = []
    for raw in matches:
        raw_main = _strip_phone_extension(raw)
        ext = _extract_phone_extension(raw)
        d = _digits_only(raw_main)
        # v65: detect and fix fused ZIP+phone patterns
        if len(d) == 10 and zip_tails and d[:3] in zip_tails:
            local7 = d[3:]
            try:
                fused = False
                for z in zips_in_header:
                    if not z.endswith(d[:3]):
                        continue
                    if re.search(rf"{re.escape(z)}\s*(?:\n\s*)?{local7[:3]}[\s\-\.]*{local7[3:]}\b", header_text):
                        fused = True
                        break
                if fused:
                    if allow_seven_digit and len(local7) == 7:
                        d = local7
                    else:
                        continue
            except Exception:
                pass
        if len(d) == 7 and not allow_seven_digit:
            continue
        if not d or d in seen:
            continue
        seen.add(d)
        norm, has_area, valid = _normalize_phone_digits(d)
        if valid:
            candidates.append((raw, norm, has_area, ext))

    ten = [c for c in candidates if c[2] is True]
    sev = [c for c in candidates if c[2] is False]

    primary = ("", "", False, "")
    alt = ("", "", False, "")
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
        "PhoneExtension": primary[3] if primary[1] else "",
        "PhoneAltExtension": alt[3] if alt[1] else "",
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



def dewarp_perspective(img_bgr: np.ndarray) -> np.ndarray:
    """Best-effort perspective correction using largest 4-point contour."""
    try:
        gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)
        gray = cv2.GaussianBlur(gray, (5, 5), 0)
        edges = cv2.Canny(gray, 60, 180)
        edges = cv2.dilate(edges, np.ones((3,3), np.uint8), iterations=1)
        cnts, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        if not cnts:
            return img_bgr
        cnts = sorted(cnts, key=cv2.contourArea, reverse=True)[:5]
        h, w = gray.shape[:2]
        for c in cnts:
            area = cv2.contourArea(c)
            if area < 0.25 * w * h:
                continue
            peri = cv2.arcLength(c, True)
            approx = cv2.approxPolyDP(c, 0.02 * peri, True)
            if len(approx) != 4:
                continue
            pts = approx.reshape(4, 2).astype('float32')
            s = pts.sum(axis=1)
            diff = np.diff(pts, axis=1).reshape(-1)
            tl = pts[s.argmin()]
            br = pts[s.argmax()]
            tr = pts[diff.argmin()]
            bl = pts[diff.argmax()]
            src_pts = np.array([tl, tr, br, bl], dtype='float32')
            widthA = np.linalg.norm(br - bl)
            widthB = np.linalg.norm(tr - tl)
            maxW = int(max(widthA, widthB))
            heightA = np.linalg.norm(tr - br)
            heightB = np.linalg.norm(tl - bl)
            maxH = int(max(heightA, heightB))
            if maxW < 100 or maxH < 100:
                continue
            dst_pts = np.array([[0,0],[maxW-1,0],[maxW-1,maxH-1],[0,maxH-1]], dtype='float32')
            M = cv2.getPerspectiveTransform(src_pts, dst_pts)
            warped = cv2.warpPerspective(img_bgr, M, (maxW, maxH), flags=cv2.INTER_CUBIC, borderMode=cv2.BORDER_REPLICATE)
            return warped
        return img_bgr
    except Exception:
        return img_bgr
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
    bin_img = cv2.erode(bin_img, kernel, iterations=1)  # thicken faint ink strokes
    return Image.fromarray(bin_img)


# --- Additional preprocessing for hard scans (illumination correction + sharpening) ---
def preprocess_shadow_correct(pil_img: Image.Image) -> Image.Image:
    """Normalize uneven illumination/shadows to help OCR on faint scans."""
    img_bgr = cv2.cvtColor(np.array(pil_img), cv2.COLOR_RGB2BGR)
    gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)
    bg = cv2.medianBlur(gray, 31)
    bg = np.clip(bg, 1, 255)
    norm = cv2.divide(gray, bg, scale=255)
    norm = cv2.bilateralFilter(norm, 9, 50, 50)
    bin_img = cv2.adaptiveThreshold(norm, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 35, 11)
    bin_img = ensure_dark_text_on_white(bin_img)
    return Image.fromarray(bin_img)


def unsharp_mask(pil_img: Image.Image, amount: float = 1.2, radius: int = 1) -> Image.Image:
    """Simple unsharp mask to strengthen faint strokes without over-bolding."""
    img = np.array(pil_img.convert('L'))
    blur = cv2.GaussianBlur(img, (0, 0), radius)
    sharp = cv2.addWeighted(img, 1.0 + amount, blur, -amount, 0)
    sharp = np.clip(sharp, 0, 255).astype(np.uint8)
    return Image.fromarray(sharp)


def crop_region(pil_img: Image.Image, top: float, bottom: float) -> tuple[Image.Image, int]:
    """Crop a vertical region [top,bottom] fractions. Returns (crop, y_offset_px)."""
    w, h = pil_img.size
    y1 = int(h * top)
    y2 = int(h * bottom)
    y2 = max(y2, y1 + 1)
    return pil_img.crop((0, y1, w, y2)), y1


def crop_header(pil_img: Image.Image) -> tuple[Image.Image, int]:
    return crop_region(pil_img, 0.0, HEADER_CROP_RATIO)


def crop_items(pil_img: Image.Image) -> tuple[Image.Image, int]:
    return crop_region(pil_img, ITEMS_START_RATIO, 1.0)

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
        if 0 <= conf < 15:
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
            segs.append((x1, y1, x2, y2))
    return segs

def line_is_struck(line_bbox: Tuple[int, int, int, int], strike_segs: List[Tuple[int, int, int, int]]) -> bool:
    """
    Overlap-ratio strike logic:
    Treat a line as struck ONLY when a horizontal strike segment covers a meaningful
    portion of the line width. This prevents a small X over one number from canceling
    the entire line.

    Rule:
      - segment must be roughly within the line's vertical band (with small tolerance)
      - horizontal overlap fraction >= 0.45
    """
    if not strike_segs:
        return False

    x1, y1, x2, y2 = line_bbox
    line_w = max(1, x2 - x1)

    # allow a little vertical tolerance
    tol = 3
    for sx1, sy1, sx2, sy2 in strike_segs:
        # Ensure segment is horizontal-ish and within y-band
        sy_mid = (sy1 + sy2) / 2.0
        if not (y1 - tol <= sy_mid <= y2 + tol):
            continue

        # Normalize segment x order
        if sx2 < sx1:
            sx1, sx2 = sx2, sx1

        # Compute overlap
        ov = max(0, min(x2, sx2) - max(x1, sx1))
        # mid-band check to ignore underline-style rules
        h = max(1, y2 - y1)
        mid_top = y1 + int(0.2 * h)
        mid_bot = y2 - int(0.2 * h)
        if not (mid_top <= sy_mid <= mid_bot):
            continue
        if (ov / line_w) >= 0.45:
            return True

    return False


def render_page(pdf_path: str, page_index: int, dpi: int) -> Optional[Image.Image]:
    imgs = convert_from_path(pdf_path, dpi=dpi, first_page=page_index + 1, last_page=page_index + 1)
    if not imgs:
        return None
    return imgs[0].convert("RGB")

# v65: render + deskew helper so strike-segment coordinates can be scaled to full DPI consistently
def render_page_deskew(pdf_path: str, page_index: int, dpi: int) -> Optional[Image.Image]:
    pil_img = render_page(pdf_path, page_index, dpi=dpi)
    if pil_img is None:
        return None
    try:
        img_bgr = cv2.cvtColor(np.array(pil_img), cv2.COLOR_RGB2BGR)
        img_bgr = deskew_bgr(img_bgr)
        return Image.fromarray(cv2.cvtColor(img_bgr, cv2.COLOR_BGR2RGB))
    except Exception:
        return pil_img



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


def ocr_items_with_strike_from_image(pil_img: Image.Image, strike_segs: List[Tuple[int, int, int, int]], y_min_frac: float = ITEMS_START_RATIO) -> List[Dict]:
    if pil_img is None:
        return []
    pil_std = preprocess_standard(pil_img)
    pil_clahe = preprocess_clahe(pil_img)
    try:
        arr = np.array(pil_std.convert('L'))
    except Exception:
        arr = None

    d_std = _tess_image_to_data(pil_std, config=OCR_PSM6)
    d_clahe = _tess_image_to_data(pil_clahe, config=OCR_PSM6)
    raw_lines_a = group_text_lines_from_ocr(d_std)
    raw_lines_b = group_text_lines_from_ocr(d_clahe)

    items=[]
    seen=set()

    def band_strike(bbox):
        if arr is None:
            return False
        x1,y1,x2,y2 = bbox
        w = max(1, x2-x1)
        ymid = int((y1+y2)/2)
        band = arr[max(0, ymid-2):min(arr.shape[0], ymid+3), max(0,x1):min(arr.shape[1], x2)]
        if band.size == 0:
            return False
        dark = (band < 128)
        if dark.mean() < 0.10:
            return False
        col = (dark.mean(axis=0) > 0.40).astype(np.uint8)
        best=0; cur=0
        for v in col:
            if v:
                cur += 1
                best = max(best, cur)
            else:
                cur = 0
        return (best / w) >= 0.55

    def maybe_add(ln_obj):
        txt = ln_obj.get('text','')
        if not txt:
            return
        x1,y1,x2,y2 = ln_obj['bbox']
        try:
            _w,_h = pil_img.size
            if y1 < int(_h * y_min_frac):
                return
        except Exception:
            pass
        key = sha1_text(f"{normalize_ws(txt)}\n{round(x1,-1)}\n{round(y1,-1)}\n{round(x2,-1)}\n{round(y2,-1)}")
        if key in seen:
            return
        struck = line_is_struck(ln_obj['bbox'], strike_segs)
        if (not struck) and band_strike(ln_obj['bbox']):
            struck = True
        it = item_dict_from_line(txt, struck=struck)
        if OCR_FILTER_NON_ITEMS and not (it.get('Include') or it.get('StruckThrough') or it.get('ExcludedByText')):
            seen.add(key)
            return
        items.append(it)
        seen.add(key)

    for ln in raw_lines_b:
        maybe_add(ln)
    for ln in raw_lines_a:
        maybe_add(ln)
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
    # Strip common labels that can block street-start detection
    line2 = re.sub(r"^\s*(address|addr|mailing address)\s*[:\-]\s*", "", line2, flags=re.IGNORECASE)
    line2 = line2.lstrip(" ,")
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
                if len(parts) >= 3 and parts[-1].upper() not in DIRECTIONAL_TOKENS:
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
        "CityStateZip": line2,
        "Inline": True,
    }



def parse_best_address(lines: List[str]) -> Dict:
    candidates = []
    top = lines[:60] if lines else []

    # inline address
    for i, line in enumerate(top):
        z, st = extract_zip_state(line)
        if z and st:
            inline = parse_inline_address_line(line)
            if inline:
                candidates.append({'Index': i, 'Street': inline['Street'], 'City': inline['City'], 'State': inline['State'], 'ZIP': inline['ZIP'], 'CityStateZip': inline['CityStateZip'], 'Score': 95, 'Inline': True})

    for i, line in enumerate(top):
        z, st = extract_zip_state(line)
        if not (z and st):
            continue
        prev1 = clean_address_line(top[i-1] if i-1 >= 0 else '')
        prev2 = clean_address_line(top[i-2] if i-2 >= 0 else '')

        def street_score(s: str) -> int:
            if not s:
                return -999
            u = s.upper()
            sc = 0
            if re.search(r"PO\s*BOX", u):
                sc += 25
            if re.search(r"\d", u):
                sc += 40
            if any(tok in u.split() for tok in ['ST','ST.','AVE','AVE.','RD','RD.','DR','DR.','LN','LN.','CT','CT.','BLVD','HWY','PKWY','CIR','PL','WAY','TRL','TER']):
                sc += 15
            if ',' in s:
                sc -= 30
            if len(s.strip()) < 6:
                sc -= 20
            return sc

        street_cands=[]
        if prev1:
            street_cands.append(prev1)
        if prev2:
            street_cands.append(prev2)
        if prev2 and prev1 and ((len(prev1) < 8) or (not re.search(r"\d", prev1))):
            street_cands.append(normalize_ws(prev2 + ' ' + prev1))

        best_street = ''
        best_sc = -999
        for s in street_cands:
            sc = street_score(s)
            if sc > best_sc:
                best_sc, best_street = sc, s

        candidates.append({'Index': i, 'Street': best_street, 'City': '', 'State': st, 'ZIP': z, 'CityStateZip': line, 'Score': 50 + max(best_sc,0), 'Inline': False})

    if not candidates:
        return {'Index': None, 'Street': '', 'City': '', 'State': '', 'ZIP': '', 'AddressRaw': ''}

    best = sorted(candidates, key=lambda x: x.get('Score',0), reverse=True)[0]
    street = best.get('Street','')
    state = best.get('State','')
    zipc = best.get('ZIP','')
    city = best.get('City','')

    if not city and best.get('CityStateZip') and state and not best.get('Inline'):
        m = re.search(US_STATE_RE, best['CityStateZip'], re.IGNORECASE)
        if m:
            city_part = best['CityStateZip'][:m.start()]
            city = normalize_ws(city_part).replace(',', '')

    address_raw = best.get('CityStateZip','') if best.get('Inline') else (f"{street}\n{best.get('CityStateZip','')}".strip(' \n'))
    return {'Index': best.get('Index'), 'Street': street, 'City': city, 'State': state, 'ZIP': zipc, 'AddressRaw': address_raw}

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


def try_ocrmac_text(pil_img: Image.Image, recognition_level: str = 'accurate', framework: str = 'vision', language_preference: Optional[list] = None) -> str:
    """Last-resort OCR using Apple Vision (ocrmac).

    framework: 'vision' (default) or 'livetext' (macOS Sonoma+).
    recognition_level: 'fast' or 'accurate' (vision only).
    """
    try:
        from ocrmac import ocrmac  # type: ignore
    except Exception:
        return ''
    try:
        kwargs = {}
        if language_preference:
            kwargs['language_preference'] = language_preference
        if framework:
            kwargs['framework'] = framework
        if framework == 'vision':
            kwargs['recognition_level'] = recognition_level
        anns = ocrmac.OCR(pil_img, **kwargs).recognize()
        texts = [a[0] for a in anns if a and isinstance(a, (list, tuple)) and len(a) >= 1 and str(a[0]).strip()]
        return '\n'.join(texts)
    except Exception:
        return ''

def try_kraken_text(pil_img: Image.Image, model_path: str = '', kraken_bin: str = 'kraken', kraken_python: str = '') -> str:
    """Last-resort OCR using Kraken via CLI. Requires kraken CLI + model.

    If kraken_python is provided (pipx/venv python), derive the venv kraken executable from it.
    """
    if not model_path:
        return ''
    import tempfile
    import subprocess
    if kraken_python:
        try:
            kb = Path(kraken_python).expanduser().resolve().parent / 'kraken'
            if kb.exists():
                kraken_bin = str(kb)
        except Exception:
            pass
    try:
        with tempfile.TemporaryDirectory() as td:
            img_path = Path(td) / 'page.png'
            out_path = Path(td) / 'out.txt'
            pil_img.save(img_path)
            cmd = [kraken_bin, '-i', str(img_path), str(out_path), 'binarize', 'segment', '-bl', 'ocr', '-m', model_path]
            subprocess.run(cmd, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if out_path.exists():
                return out_path.read_text(encoding='utf-8', errors='ignore')
    except Exception:
        return ''
    return ''

# HEADER PARSING
# -----------------------------


def is_gibberish(text: str) -> bool:
    if not text:
        return True
    t = str(text).strip()
    if len(t) < 3:
        return True
    sym = sum(1 for ch in t if not ch.isalnum() and ch not in " ,.'-&")
    if sym / max(len(t), 1) > 0.35:
        return True
    alpha = sum(1 for ch in t if ch.isalpha())
    if alpha < 3:
        return True
    if not re.search(r"[AEIOUYaeiouy]", t):
        return True
    words = t.split()
    if words:
        single = sum(1 for w in words if len(w) == 1 and w.lower() not in ['a','i'])
        if single / len(words) > 0.45:
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
    # v70.10: Noise Trimmer - cut off clusters of punctuation/symbols (scribbles/dirt)
    m_noise = re.search(r"[\.,;:\-!\?]{3,}", line)
    if m_noise:
        line = line[:m_noise.start()]

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
    top = lines[:25]
    pool = top[:addr_idx] if (addr_idx is not None and addr_idx > 0) else top

    def score_line(ln: str) -> int:
        if not ln or not ln.strip():
            return -999
        u = ln.upper()
        sc = 0
        if target_char and u.strip().startswith(target_char.upper()):
            sc += 50
        if re.search(r"^[A-Z][A-Z'\- ]+,\s*[A-Z]", u):
            sc += 30
        if re.search(r"\s(&|AND)\s", u):
            sc += 15
        # penalties
        if re.search(ZIP_RE, u):
            sc -= 40
        if re.search(US_STATE_RE, u, re.IGNORECASE):
            sc -= 25
        if re.search(STREET_START_RE, ln):
            sc -= 40
        if matches_any(ln, RE_ADDR_BLOCK):
            sc -= 25
        if re.search(r"(LOT|SEC|SECTION|SP|SPACE|GARDEN|BLOCK|BLK)", u):
            sc -= 60
        try:
            if extract_abbrev_tokens(u):
                sc -= 60
        except Exception:
            pass
        digits = sum(ch.isdigit() for ch in u)
        if digits >= 3:
            sc -= 30
        if matches_any(ln, RE_NAME_BLACKLIST):
            sc -= 80
        if is_gibberish(ln):
            sc -= 50
        alpha = sum(ch.isalpha() for ch in u)
        sc += min(alpha, 25)
        return sc

    scored=[]
    for ln in pool:
        ln_clean = clean_name_line(ln, target_char, aggressive=aggressive)
        if not ln_clean:
            continue
        scored.append((score_line(ln_clean), ln_clean))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [t for sc,t in scored if sc >= 10][:2]

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

# --- Structural / colon-style property detection (v70.10 fix) ---
# These constants/regexes are referenced by classify_item() but were missing,
# causing NameError: COLON_COUNT_PROP_MIN not defined.

# Require at least 2 colons (e.g., "142:C:3" has 2)
COLON_COUNT_PROP_MIN = 2

# OCR-tolerant "structured property" patterns:
# Handles common OCR swaps: 1/l/I, B/8, O/0
# Examples:
#   142:C:3
#   119:8:1   (B -> 8)
#   86:Sp.1-2-3-4:B:ATONEMENT
STRUCT_PROP_RE = re.compile(
    r"""
    (?:\b\d{1,4}\s*[:/]\s*[A-Z0-9IL]{1,4}\s*[:/]\s*[0-9IL]{1,3}\b)       # 73:A:l / 142:C:3
    |(?:\bSP\.?\s*\d{1,2}(?:-\d{1,2}){1,8}\b.*[:/]\s*[A-Z0-9IL]{1,4}\b)  # Sp.1-2-3-4 : B
    |(?:\b\d{1,4}\s*[:/]\s*[A-Z0-9IL]{1,4}\b\s*[:/]\s*\d{1,3}\b)         # 86:B:1
    """,
    re.IGNORECASE | re.VERBOSE
)

# "Lot/Sec/Sp" location family (colon + label variants)
# Examples:
#   Gethsemane:Lot 186:Sec,C:Sp.3 4
#   Last Supper:Lot 107:Sec.D:SP.2
COLON_LOC_RE = re.compile(
    r"""
    (?:\bLOT\.?\s*[: ]\s*\d+\b)
    .*?
    (?:\bSEC(?:TION)?\.?\s*[: ,\.]?\s*[A-Z0-9IL]+\b)
    .*?
    (?:\bSP(?:ACE)?\.?\s*[: \.]?\s*\d+(?:-\d+)?\b)
    """,
    re.IGNORECASE | re.VERBOSE
)

def is_excludable_item(line: str) -> bool:
    return matches_any(line, RE_XFER)


def classify_item(line: str) -> Dict[str, bool]:
    """Classify an item line with OCR-tolerant property detection."""
    t = normalize_dimensions(line or "")
    u = t.upper()

    dim = is_dimension_token(t)
    structural_prop = (not dim) and bool(t.count(":") >= COLON_COUNT_PROP_MIN and (STRUCT_PROP_RE.search(u) or COLON_LOC_RE.search(u)))
    kw_prop = bool(re.search(r"\b(space|sp\.?|lot|section|sec\.?|block|blk\.?|garden|crypt|lawn|grave|burial|mausoleum|maus\.?|niche|columbarium|estates?)\b", t, re.IGNORECASE))
    is_prop = bool(kw_prop or structural_prop or bool(RIGHTS_NOTATION_RE.search(t)) or heuristic_is_property(t))
    if dim:
        is_prop = False

    flags = {
        "IsProperty": is_prop,
        "IsMemorial": matches_any(t, RE_MEMORIAL),
        "IsFuneralPreneed": matches_any(t, RE_FUNERAL_PN),
        "IsAtNeedFuneral": matches_any(t, RE_AT_NEED),
        "IsIntermentService": matches_any(t, RE_INTERMENT),
        "HasRightsNotation": bool(RIGHTS_NOTATION_RE.search(t)),
        "IsDimension": bool(dim),
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
    txt = normalize_dimensions(txt or '')
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
        "IsDimension": bool(cls.get("IsDimension", False)),
        "Dimensions": first_dimension(txt),
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

# --- Header-only OCR ensemble (for rotation + better name/address) ---

def ocr_header_ensemble(pil_page: Image.Image, kraken_model: str = '', kraken_bin: str = 'kraken', kraken_python: str = '', allow_livetext: bool = True, alt_ocr: str = "ocrmac_then_kraken") -> tuple[str, dict]:
    header_img, _ = crop_header(pil_page)

    # blank detect
    try:
        gray = np.array(header_img.convert('L'))
        dark = (gray < 160).sum()
        ratio = float(dark) / float(max(gray.size,1))
        if ratio < 0.0035:
            return '', {'best':'BLANK_HEADER', 'score':0, 'blank_ratio':ratio, 'tesseract':False, 'ocrmac_vision':False, 'ocrmac_livetext':False, 'kraken':False}
    except Exception:
        pass

    # auto-rotate header for OCR
    try:
        cand_scores=[]
        for ang in (0,90,180,270):
            img = header_img.rotate(ang, expand=True) if ang else header_img
            pp = preprocess_shadow_correct(img)
            t = _tess_image_to_string(pp, config="--oem 3 -l eng --psm 6")
            sc = score_header_text(t)
            cand_scores.append((sc, ang, img))
        cand_scores.sort(key=lambda x: x[0], reverse=True)
        header_img = cand_scores[0][2]
    except Exception:
        pass

    scales = [1.0, 2.0, 3.0]
    variants = []
    for s in scales:
        if s != 1.0:
            w,h = header_img.size
            img_s = header_img.resize((int(w*s), int(h*s)), resample=Image.BICUBIC)
        else:
            img_s = header_img
        variants.extend([
            ('STD', preprocess_standard(img_s)),
            ('CLAHE', preprocess_clahe(img_s)),
            ('GHOST', preprocess_ghost(img_s)),
            ('SHADOW', preprocess_shadow_correct(img_s)),
            ('SHARP', unsharp_mask(img_s)),
        ])

    psm_list = [6,4,11]
    best_text=''
    best_name=''
    best_score=-1
    used = {'tesseract': False, 'ocrmac_vision': False, 'ocrmac_livetext': False, 'kraken': False}

    for tag, img_v in variants:
        if best_score >= 95:
            break
        for psm in psm_list:
            cfg = f"{TESS_CONFIG_BASE} --psm {psm}"
            t = _tess_image_to_string(img_v, config=cfg)
            used['tesseract'] = True
            sc = score_header_text(t)
            if sc > best_score:
                best_score = sc
                best_text = t
                best_name = f"TESS_{tag}_PSM{psm}_S{sc}"

    # alt OCR
    alt_mode = (alt_ocr or '').strip().lower()
    allow_alt = alt_mode != 'none'

    def try_ocrmac():
        if not allow_alt:
            return
        t_vision = try_ocrmac_text(header_img, recognition_level='accurate', framework='vision', language_preference=['en-US'])
        if t_vision:
            used['ocrmac_vision'] = True
            sc = score_header_text(t_vision)
            nonlocal_best = sc
            return ('OCRMAC_VISION', t_vision, sc)
        return None

    # keep it simple: if score very low, try ocrmac
    if allow_alt and best_score < 45:
        try:
            t_vision = try_ocrmac_text(header_img, recognition_level='accurate', framework='vision', language_preference=['en-US'])
            if t_vision:
                used['ocrmac_vision'] = True
                sc = score_header_text(t_vision)
                if sc > best_score:
                    best_score = sc
                    best_text = t_vision
                    best_name = f"OCRMAC_VISION_S{sc}"
        except Exception:
            pass

    meta = {'best': best_name, 'score': best_score, **used}
    return best_text, meta

def process_page(pdf_path: str, page_index: int, dpi: int, target_char: Optional[str], reader: Optional[PdfReader] = None, kraken_model: str = '', kraken_bin: str = 'kraken', kraken_python: str = '', allow_livetext: bool = True, facts_sections: Optional[List[str]] = None, facts_mode: str = '', alt_ocr: str = "ocrmac_then_kraken") -> Tuple[Dict, List[Dict], bool]:
    """Returns (owner_dict, items_list, is_interment)."""

    # TEXT LAYER FIRST
    pdf_text = extract_pdf_text_page(pdf_path, page_index, reader=reader)
    if text_layer_usable(pdf_text):
        txt = pdf_text
        lines = split_lines(txt)
        template_type = detect_template_type(txt)
        _, p, s, last, addr, is_interment = parse_owner_header(lines, target_char)

        # trust-but-verify
        ok = bool(p) and "[MISSING" not in p and ((not target_char) or p.strip().upper().startswith(target_char.upper()))
        if ok:
            owner_header_meta = {}
            phone_fields = extract_phone_fields(txt, lines)
            # v65: If PDF text layer is usable but lacks phone data, run header OCR to recover phones
            try:
                if (not phone_fields.get('PhoneValid')) and (not (phone_fields.get('PhoneRaw') or phone_fields.get('Phone') or phone_fields.get('PhoneNormalized'))):
                    pil_h = render_page_deskew(pdf_path, page_index, dpi=min(dpi, 300))
                    if pil_h is not None:
                        htxt, hmeta = ocr_header_ensemble(
                            pil_h,
                            kraken_model=kraken_model,
                            kraken_bin=kraken_bin,
                            kraken_python=kraken_python,
                            allow_livetext=allow_livetext,
                            alt_ocr=alt_ocr,
                        )
                        hlines = split_lines(htxt)
                        p2 = extract_phone_fields(htxt, hlines)
                        if p2.get('PhoneValid') and p2.get('PhoneNormalized'):
                            phone_fields.update(p2)
                        owner_header_meta = hmeta
            except Exception:
                pass

            # ---- Strike detection parity ----
            strike_dpi = min(200, max(150, dpi // 2))
            strike_segs, strike_pil = detect_strike_segs_for_page(pdf_path, page_index, dpi=strike_dpi)
            if strike_segs and strike_pil is not None:
                # v65: detect strikes at lower DPI, but OCR items at full DPI for better accuracy
                full_pil = render_page_deskew(pdf_path, page_index, dpi=dpi)
                if full_pil is not None:
                    try:
                        sw, sh = strike_pil.size
                        fw, fh = full_pil.size
                        fx = fw / max(sw, 1)
                        fy = fh / max(sh, 1)
                        scaled = [(int(x1*fx), int(y1*fy), int(x2*fx), int(y2*fy)) for (x1,y1,x2,y2) in strike_segs]
                    except Exception:
                        scaled = strike_segs
                    items = [] if is_interment else ocr_items_with_strike_from_image(full_pil, strike_segs=scaled)
                else:
                    items = [] if is_interment else ocr_items_with_strike_from_image(strike_pil, strike_segs=strike_segs)
            else:
                items = [] if is_interment else parse_items_from_text(lines, template_type)
            # v70.10: salvage State/ZIP from anywhere on page when text-layer address parse is incomplete
            try:
                if (not addr.get('State')) or (not addr.get('ZIP')):
                    st2, z2 = extract_state_zip_anywhere(txt)
                    if st2 and not addr.get('State'):
                        addr['State'] = st2
                    if z2 and not addr.get('ZIP'):
                        addr['ZIP'] = z2
            except Exception:
                pass
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
                # v65: diagnostics for header OCR (populated when header OCR ran)
                "HeaderOCRBest": (owner_header_meta.get('best','') if isinstance(owner_header_meta, dict) else ''),
                "HeaderOCRScore": (owner_header_meta.get('score','') if isinstance(owner_header_meta, dict) else ''),
                "HeaderOCRUsedTesseract": bool(owner_header_meta.get('tesseract')) if isinstance(owner_header_meta, dict) else False,
                "HeaderOCRUsedOCRmacVision": bool(owner_header_meta.get('ocrmac_vision')) if isinstance(owner_header_meta, dict) else False,
                "HeaderOCRUsedOCRmacLiveText": bool(owner_header_meta.get('ocrmac_livetext')) if isinstance(owner_header_meta, dict) else False,
                "HeaderOCRUsedKraken": bool(owner_header_meta.get('kraken')) if isinstance(owner_header_meta, dict) else False,
            }
            owner.update(phone_fields)
            owner = enrich_owner_with_review(owner, items, pdf_path, page_index+1, dpi=dpi, pil_page=None, facts_sections=facts_sections, facts_mode=facts_mode, reader=reader)
            return owner, items, is_interment

    # OCR FALLBACK (full)
    pil_original = render_page(pdf_path, page_index, dpi=dpi)
    # v74: blank/separator detection
    try:
        is_blank, blank_ratio = is_blank_header(pil_original)
    except Exception:
        is_blank, blank_ratio = (False, 0.0)
    if is_blank:
        owner = {
            'OwnerName_Raw': '[BLANK/SEPARATOR]',
            'PrimaryOwnerName': '[BLANK/SEPARATOR]',
            'SecondaryOwnerName': '',
            'LastName': '',
            'Street': '',
            'City': '',
            'State': '',
            'ZIP': '',
            'AddressRaw': '',
            'RawText': '',
            'RawTextHash': sha1_text(''),
            'TemplateType': 'blank',
            'TextSource': 'BLANK_OR_SEPARATOR',
            'IsBlankSeparator': True,
        }
        return owner, [], False
    if pil_original is None:
        raise RuntimeError(f"Failed to render page {page_index+1}")

    orig_bgr = cv2.cvtColor(np.array(pil_original), cv2.COLOR_RGB2BGR)
    orig_bgr = dewarp_perspective(orig_bgr)
    orig_bgr = deskew_bgr(orig_bgr)
    pil_original = Image.fromarray(cv2.cvtColor(orig_bgr, cv2.COLOR_BGR2RGB))

    # --- v65: Header-targeted OCR ensemble (top region) ---
    header_text, header_meta = ocr_header_ensemble(
        pil_original,
        kraken_model=kraken_model,
        kraken_bin=kraken_bin,
        kraken_python=kraken_python,
        allow_livetext=allow_livetext,
        alt_ocr=alt_ocr,
    )

    pil_std = preprocess_standard(pil_original)
    pil_clahe = preprocess_clahe(pil_original)
    pil_ghost = preprocess_ghost(pil_original)

    t_std = _tess_image_to_string(pil_std, config=OCR_PSM6)
    t_clahe = _tess_image_to_string(pil_clahe, config=OCR_PSM6)
    t_ghost = _tess_image_to_string(pil_ghost, config=OCR_PSM6)
    t_sparse = _tess_image_to_string(pil_original, config=OCR_PSM11)

    candidates = [("STD", t_std), ("CLAHE", t_clahe), ("GHOST", t_ghost), ("SPARSE", t_sparse)]
    best_name, best_text, best_score = sorted(
        [(n, t, score_text_pass(t)) for (n, t) in candidates],
        key=lambda x: x[2], reverse=True
    )[0]

    lines_best = split_lines(header_text if header_text else best_text)
    template_type = detect_template_type(best_text)
    _, p, s, last, addr, is_interment = parse_owner_header(lines_best, target_char)
    phone_fields = extract_phone_fields(header_text if header_text else best_text, lines_best)
    # Visual Sniper: salvage ZIP/State if missing
    if (not addr.get('State')) or (not addr.get('ZIP')):
        st2, z2 = extract_state_zip_anywhere(best_text)
        if st2 or z2:
            addr['State'] = addr.get('State') or st2
            addr['ZIP'] = addr.get('ZIP') or z2
    # items with strike detection
    strike_segs = detect_horizontal_strikelines(orig_bgr)
    items = [] if is_interment else ocr_items_with_strike_from_image(pil_original, strike_segs=strike_segs, y_min_frac=ITEMS_START_RATIO)

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
        # v65: capture which header OCR backend won (useful diagnostics)
        "HeaderOCRBest": header_meta.get('best','') if isinstance(header_meta, dict) else '',
        "HeaderOCRScore": header_meta.get('score','') if isinstance(header_meta, dict) else '',
        "HeaderOCRUsedTesseract": bool(header_meta.get('tesseract')) if isinstance(header_meta, dict) else False,
        "HeaderOCRUsedOCRmacVision": bool(header_meta.get('ocrmac_vision')) if isinstance(header_meta, dict) else False,
        "HeaderOCRUsedOCRmacLiveText": bool(header_meta.get('ocrmac_livetext')) if isinstance(header_meta, dict) else False,
        "HeaderOCRUsedKraken": bool(header_meta.get('kraken')) if isinstance(header_meta, dict) else False,
    }
    owner.update(phone_fields)
    owner = enrich_owner_with_review(owner, items, pdf_path, page_index+1, dpi=dpi, pil_page=pil_original, facts_sections=facts_sections, facts_mode=facts_mode, reader=reader)
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
    # v65 NOTE: We intentionally use MAX across property lines as a conservative estimate of burials on the card.
    # If you need a total across multiple properties, use compute_likely_burials_sum().
    return max(used_counts) if used_counts else 0


# -----------------------------
# DATASET PROCESSOR
# -----------------------------


def compute_likely_burials_sum(items: List[Dict]) -> int:
    """Sum burial indicators across multiple property lines (more aggressive than MAX)."""
    total = 0
    for it in (items or []):
        if not it.get("Include", False):
            continue
        if not it.get("IsProperty", False):
            continue
        txt = (it.get("LineText", "") or "").upper()
        if re.search(r"\bX\b", txt) or re.search(r"\bUSED\b", txt):
            total += 1
            continue
        ru = it.get("RightsUsed", None)
        if ru is not None:
            try:
                if not pd.isna(ru):
                    total += int(ru)
            except Exception:
                pass
    return int(total)

def process_dataset(pdf_path: str, out_path: str, dpi: int = 300, kraken_model: str = '', kraken_bin: str = 'kraken', kraken_python: str = '', allow_livetext: bool = True, facts_path: str = '', alt_ocr: str = "ocrmac_then_kraken"):
    # v65: FaCTS load mode tracking (not_provided vs loaded vs failed)
    facts_mode = 'not_provided'
    facts_sections = []
    if facts_path:
        if not os.path.exists(facts_path):
            facts_mode = 'failed'
        else:
            facts_sections = load_facts_sections(facts_path)
            facts_mode = 'loaded' if facts_sections else 'failed'
            _update_garden_regex_from_facts(facts_sections)
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
        if reader is None:
            print("Error: Unable to read PDF metadata for page count.")
            return
        page_count = len(reader.pages)

    owners_rows: List[Dict] = []
    items_rows: List[Dict] = []
    interment_rows: List[Dict] = []
    blanks_rows: List[Dict] = []  # v74

    for p in tqdm(range(page_count), desc=f"Scanning {filename}", unit="page"):
        owner_data, items_data, is_interment = process_page(
            pdf_path,
            p,
            dpi,
            target_char,
            reader=reader,
            kraken_model=kraken_model,
            kraken_bin=kraken_bin,
            kraken_python=kraken_python,
            allow_livetext=allow_livetext,
            facts_sections=facts_sections,
            facts_mode=facts_mode,
            alt_ocr=alt_ocr,
        )

        rec_id = f"{record_prefix}-P{p+1:04d}"
        owner_data["OwnerRecordID"] = rec_id
        owner_data["SourceFile"] = filename
        owner_data["PageNumber"] = p + 1

        if owner_data.get('IsBlankSeparator'):
            blanks_rows.append(owner_data)
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
    blanks_df = pd.DataFrame(blanks_rows)

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
    all_items = items_df.copy() if not items_df.empty else pd.DataFrame()

    # v70.10 debug: capture lines that look like property candidates but were not classified as property
    debug_rows = []
    if not all_items.empty and 'LineText' in all_items.columns:
        for _, r in all_items.iterrows():
            txt = str(r.get('LineText', '') or '')
            u = txt.upper()
            coord = bool(COORD_OCR_FUZZ_RE.search(u))
            colons = (u.count(':') + u.count('/'))
            kw = bool(re.search(r"\b(SP\.?|SPACE|LOT|SEC\.?|SECTION|BLOCK|BLK\.?|GRAVE|CRYPT|LAWN)\b", u))
            if (coord or colons >= 2 or kw) and (not bool(r.get('IsProperty', False))):
                why = []
                if coord: why.append('coord')
                if colons >= 2: why.append('colon')
                if kw: why.append('kw')
                debug_rows.append({'OwnerRecordID': r.get('OwnerRecordID',''), 'Page': r.get('Page', r.get('PageNumber','')), 'LineText': txt, 'Why': '+'.join(why)})
    debug_df = pd.DataFrame(debug_rows)

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
        likely_burials_sum = compute_likely_burials_sum(group.to_dict("records"))

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
            "LikelyBurialsSum": int(likely_burials_sum),
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

    def agg_owner_any(group: pd.DataFrame) -> pd.Series:
        """Audit-only: property presence even if struck/excluded.

        Robust to missing columns and avoids .any() on a scalar.
        """
        # IsProperty column
        if "IsProperty" in group.columns:
            isprop_series = group["IsProperty"].fillna(False).astype(bool)
            has_isprop = isprop_series.any()
            prop_lines_any = group.loc[isprop_series, "LineText"].tolist() if "LineText" in group.columns else []
        else:
            has_isprop = False
            prop_lines_any = []

        # Rights evidence: RightsNotation string OR RightsUsed/RightsTotal numeric
        has_rights = False
        if "RightsNotation" in group.columns:
            has_rights = group["RightsNotation"].fillna("").astype(str).str.strip().ne("").any()
        if (not has_rights) and ("RightsUsed" in group.columns):
            try:
                has_rights = group["RightsUsed"].notna().any()
            except Exception:
                pass
        if (not has_rights) and ("RightsTotal" in group.columns):
            try:
                has_rights = group["RightsTotal"].notna().any()
            except Exception:
                pass

        has_prop_any = bool(has_isprop or has_rights)
        return pd.Series({
            "HasPropertyAny": bool(has_prop_any),
            "PropertyEvidenceAny": "\\n\\n".join([str(x) for x in prop_lines_any[:3]]),
        })

    try:
        owner_flags = inc.groupby("OwnerRecordID").apply(agg_owner, include_groups=False).reset_index() if not inc.empty else pd.DataFrame(columns=["OwnerRecordID"])
    except TypeError:
        owner_flags = inc.groupby("OwnerRecordID").apply(agg_owner).reset_index() if not inc.empty else pd.DataFrame(columns=["OwnerRecordID"])
    try:
        owner_flags_any = all_items.groupby("OwnerRecordID").apply(agg_owner_any, include_groups=False).reset_index() if (not all_items.empty) else pd.DataFrame(columns=["OwnerRecordID"])
    except TypeError:
        owner_flags_any = all_items.groupby("OwnerRecordID").apply(agg_owner_any).reset_index() if (not all_items.empty) else pd.DataFrame(columns=["OwnerRecordID"])

    owners_master = owners_df.merge(owner_flags, on="OwnerRecordID", how="left") if not owners_df.empty else pd.DataFrame()
    if not owners_master.empty and not owner_flags_any.empty:
        owners_master = owners_master.merge(owner_flags_any, on="OwnerRecordID", how="left")

    defaults = {

        "HasPropertyAny": False,
        "PropertyEvidenceAny": "",
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
        try:
            owners_master = owners_master.infer_objects(copy=False)
        except Exception:
            pass

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
 "BlanksFound": int(len(blanks_df)) if not blanks_df.empty else 0,
        "PossibleDuplicateScans": int(len(possible_dups)) if not possible_dups.empty else 0,
        "LIST_Memorial_Letter": int(len(list_memorial)) if not list_memorial.empty else 0,
        "LIST_PN_Funeral_Letter": int(len(list_pn)) if not list_pn.empty else 0,
        "LIST_SpacesOnly_PRIME": int(len(list_prime)) if not list_prime.empty else 0,
        "LIST_SurvivorSpouse": int(len(list_survivor)) if not list_survivor.empty else 0,
        "PhoneExceptions": int(len(phone_exceptions)) if not phone_exceptions.empty else 0,
        "NeedsReviewRows": int(owners_master["NeedsReview"].fillna(False).map(lambda v: (v is True) or (str(v).strip().upper() in ("TRUE","1","YES","Y"))).sum()) if (not owners_master.empty and "NeedsReview" in owners_master.columns) else 0,
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
        # v65: convenience sheet with only rows that need human review
        try:
            if 'NeedsReview' in owners_master_safe.columns:
                nr = owners_master_safe[owners_master_safe['NeedsReview'] == True]
                if not nr.empty:
                    nr.to_excel(xw, index=False, sheet_name='NeedsReview')
        except Exception:
            pass
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
        if 'debug_df' in locals() and isinstance(debug_df, pd.DataFrame) and (not debug_df.empty):
            make_df_excel_safe(debug_df).to_excel(xw, index=False, sheet_name='PropCand_NotClass')
        if not interment_safe.empty:
            interment_safe.to_excel(xw, index=False, sheet_name="LIST_Refile_IntermentRecords")
        # v74: blanks/separators (excluded from NeedsReview)
        if 'blanks_df' in locals() and isinstance(blanks_df, pd.DataFrame) and not blanks_df.empty:
            make_df_excel_safe(blanks_df).to_excel(xw, index=False, sheet_name='LIST_Blanks_Separators')
        if not phone_ex_safe.empty:
            phone_ex_safe.to_excel(xw, index=False, sheet_name="PhoneExceptions")

    os.replace(tmp_path, out_path)
    print(f"✅ Wrote: {out_path}")


# -----------------------------
# CLI
# -----------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("pdf_positional", nargs="?", help="PDF path (drag-drop)")
    ap.add_argument("--pdf", default="", help="Input scanned PDF (optional if you drag-drop a PDF)")
    ap.add_argument("--out", help="Output Excel path")
    ap.add_argument("--dpi", type=int, default=300, help="OCR render DPI (fallback only)")
    ap.add_argument("--kraken-model", default="", help="Optional: path to Kraken recognition model (.mlmodel) for handwriting last resort")
    ap.add_argument("--kraken-bin", default="kraken", help="Kraken CLI executable (default: kraken)")
    ap.add_argument("--kraken-python", default="/Users/john/.local/pipx/venvs/kraken/bin/python", help="Kraken Py3.11 env python; used to locate venv kraken executable")
    ap.add_argument("--no-livetext", action="store_true", help="Disable ocrmac LiveText backend")
    ap.add_argument("--alt-ocr", default="ocrmac_then_kraken", help="Alternate OCR order (last resort): ocrmac_then_kraken|kraken_then_ocrmac|none")
    ap.add_argument("--facts", default="", help="Optional: FaCTS inventory export (.xlsx) for section validation")
    args, _ = ap.parse_known_args()
    # --- Drag/drop friendly: positional PDF/env defaults/auto-detect ---
    if (not args.pdf) and getattr(args, "pdf_positional", None):
        if isinstance(args.pdf_positional, str) and args.pdf_positional.lower().endswith(".pdf") and os.path.exists(args.pdf_positional):
            args.pdf = args.pdf_positional

    positional = _[:]
    if (not args.pdf) and positional:
        for tok in positional:
            if isinstance(tok, str) and tok.lower().endswith('.pdf') and os.path.exists(tok):
                args.pdf = tok
                break
    script_dir = os.path.dirname(os.path.abspath(__file__))
    _set_snapshot_dir_for_script()
    print(f"[Info] Failed snapshots folder: {FAILED_SNAPSHOT_DIR}")
    if not args.pdf:
        args.pdf = _auto_pick_pdf(script_dir)
    if args.pdf and (not args.out):
        args.out = _default_out_path(args.pdf)
    if not args.kraken_model:
        args.kraken_model = _auto_pick_kraken_model()


    if args.pdf:
        out = args.out if args.out else args.pdf.replace(".pdf", ".xlsx")
        process_dataset(
            args.pdf,
            out,
            args.dpi,
            kraken_model=getattr(args, "kraken_model", ""),
            kraken_bin=getattr(args, "kraken_bin", "kraken"),
            kraken_python=getattr(args, "kraken_python", ""),
            allow_livetext=(not getattr(args, "no_livetext", False)),
            facts_path=args.facts,
            alt_ocr=getattr(args, "alt_ocr", "ocrmac_then_kraken"),
        )
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
        process_dataset(
            pdf_path,
            out_path,
            dpi=args.dpi,
            kraken_model=getattr(args, "kraken_model", ""),
            kraken_bin=getattr(args, "kraken_bin", "kraken"),
            kraken_python=getattr(args, "kraken_python", ""),
            allow_livetext=(not getattr(args, "no_livetext", False)),
            facts_path=args.facts,
            alt_ocr=getattr(args, "alt_ocr", "ocrmac_then_kraken"),
        )


if __name__ == "__main__":
    main()
