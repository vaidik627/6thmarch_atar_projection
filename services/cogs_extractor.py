"""
cogs_extractor.py — COGS and Inventory Component extraction (Tier 2 fallback).

Used when COGS was NOT found in the primary P&L window by gp_extractor.py.

Public API:
    extract_cogs_text(ocr_text: str) -> str   — secondary P&L COGS scan
    extract_inventory_text(ocr_text: str) -> str — Balance Sheet inventory scan
"""

import re
import logging

logger = logging.getLogger(__name__)

# ── COGS section header keywords ──────────────────────────────────────────────
_COGS_SECTION_HEADERS = [
    'cost structure', 'cost analysis', 'direct cost breakdown', 'cost of revenue detail'
]

# ── COGS row labels ───────────────────────────────────────────────────────────
_COGS_ROW_LABELS = [
    'cost of goods sold', 'cogs', 'cost of sales', 'cost of revenue',
    'cost of products sold', 'direct costs', 'cost of services',
    'total cost of goods sold', 'total cost of sales',
]

# ── Balance Sheet section keywords ───────────────────────────────────────────
_BS_KEYWORDS = [
    'balance sheet', 'statement of financial position',
    'current assets', 'total assets',
]

# ── Inventory row labels ──────────────────────────────────────────────────────
_INV_LABELS = [
    'inventory', 'inventories', 'total inventory',
    'finished goods', 'raw materials', 'work in progress', 'wip',
]

# ── Revenue row labels (for COGS sanity check) ────────────────────────────────
_REV_LABELS = ['total revenue', 'net revenue', 'net sales', 'revenue', 'total sales']

# ── GP row labels (for COGS-R2 service business detection) ────────────────────
_GP_LABELS = ['gross profit', 'gross income', 'gross earnings', 'gross margin']

# ── Purchases row labels ──────────────────────────────────────────────────────
_PURCHASE_LABELS = ['purchases', 'cost of purchases', 'inventory purchases']

# ── Numeric regex ─────────────────────────────────────────────────────────────
_NUM_PAT = re.compile(r'[\d,]+(?:\.\d+)?')


def _nums(line: str):
    """Return list of floats from a line."""
    result = []
    for m in _NUM_PAT.finditer(line):
        try:
            result.append(float(m.group().replace(',', '')))
        except ValueError:
            pass
    return result


def _max_num(line: str) -> float:
    ns = _nums(line)
    return max(ns) if ns else 0.0


def _matches(line_lower: str, labels: list) -> bool:
    return any(lbl in line_lower for lbl in labels)


# ─────────────────────────────────────────────────────────────────────────────
# Public function 1: extract_cogs_text
# ─────────────────────────────────────────────────────────────────────────────

def extract_cogs_text(ocr_text: str) -> str:
    """
    Secondary P&L COGS scan — looks for standalone COGS section or service-business format.

    COGS-R1: Scans for cost breakdown section headers.
    COGS-R2: Detects service businesses (Revenue → blank → Gross Profit, no COGS row).
    COGS-R3: Validates COGS < Revenue, COGS > 0, GP+COGS≈Revenue.

    Returns: scoped text block containing COGS context, or empty string if not found.
    """
    try:
        lines = ocr_text.splitlines()
        total = len(lines)

        # COGS-R1: Find standalone COGS section
        anchor_idx = -1
        for i, line in enumerate(lines):
            ll = line.lower().strip()
            if _matches(ll, _COGS_SECTION_HEADERS):
                anchor_idx = i
                break

        # If no section header found, do a direct row scan
        if anchor_idx == -1:
            for i, line in enumerate(lines):
                ll = line.lower().strip()
                if _matches(ll, _COGS_ROW_LABELS) and _nums(line):
                    anchor_idx = i
                    break

        if anchor_idx == -1:
            logger.info("  COGS extractor: no COGS section or row found")
            return ''

        start = max(0, anchor_idx - 10)
        end   = min(total, anchor_idx + 60)
        window_lines = lines[start:end]
        window_text  = '\n'.join(window_lines)
        window_lower = window_text.lower()

        # COGS-R2: Service business detection
        # Look for Revenue → [blank/no COGS] → Gross Profit pattern
        cogs_found = any(_matches(l.lower(), _COGS_ROW_LABELS) and _nums(l) for l in window_lines)
        if not cogs_found:
            # Check if GP is present — COGS derivable from Revenue − GP
            gp_found  = any(_matches(l.lower(), _GP_LABELS) and _nums(l) and '%' not in l for l in window_lines)
            rev_found = any(_matches(l.lower(), _REV_LABELS) and _nums(l) for l in window_lines)
            if gp_found and rev_found:
                logger.info("  COGS extractor (R2): service business format — COGS derivable from GP row")
                note = "\nNOTE: Service business format detected — COGS = Revenue - Gross Profit\n"
                return window_text + note
            logger.info("  COGS extractor: COGS row not confirmed in secondary scan")
            return ''

        # COGS-R3: Validation
        cogs_val = 0.0
        rev_val  = 0.0
        gp_val   = 0.0
        for line in window_lines:
            ll = line.lower()
            if _matches(ll, _COGS_ROW_LABELS) and _nums(line):
                cogs_val = max(_nums(line))
            if _matches(ll, _REV_LABELS) and _nums(line):
                rev_val = max(_nums(line))
            if _matches(ll, _GP_LABELS) and _nums(line) and '%' not in line:
                gp_val = max(_nums(line))

        validation_note = ''
        if rev_val > 0 and cogs_val > rev_val:
            logger.warning(f"  COGS extractor (R3): COGS ({cogs_val:,.0f}) > Revenue ({rev_val:,.0f}) — discarding")
            return ''
        if cogs_val <= 0:
            logger.warning("  COGS extractor (R3): COGS <= 0 — discarding")
            return ''
        if gp_val > 0 and rev_val > 0 and abs(gp_val + cogs_val - rev_val) / rev_val > 0.02:
            validation_note = (
                f"\nWARN: GP({gp_val:,.0f}) + COGS({cogs_val:,.0f}) "
                f"≠ Revenue({rev_val:,.0f}) — prefer GP row, derive COGS\n"
            )

        logger.info(f"  COGS extractor: COGS section found at line {anchor_idx}, COGS={cogs_val:,.0f}")
        return window_text + validation_note

    except Exception as exc:
        logger.warning(f"  COGS extractor: error ({exc}) — returning empty string")
        return ''


# ─────────────────────────────────────────────────────────────────────────────
# Public function 2: extract_inventory_text
# ─────────────────────────────────────────────────────────────────────────────

def extract_inventory_text(ocr_text: str) -> str:
    """
    Balance Sheet inventory scan for Tier 2 fallback.

    INV-R1: Find Balance Sheet section using LAST occurrence of BS keywords.
    INV-R2: Extract ending inventory and purchases rows within 60-line window.
    INV-R3: Note if beginning inventory must be estimated (only 1 year of BS).

    Returns: scoped Balance Sheet text block, or empty string if not found.
    """
    try:
        lines = ocr_text.splitlines()
        total = len(lines)

        # INV-R1: Find LAST occurrence of Balance Sheet keywords
        bs_idx = -1
        for i, line in enumerate(lines):
            ll = line.lower().strip()
            if _matches(ll, _BS_KEYWORDS):
                bs_idx = i  # keep overwriting → last occurrence wins

        if bs_idx == -1:
            logger.info("  Inventory extractor: no Balance Sheet section found")
            return ''

        start = max(0, bs_idx - 5)
        end   = min(total, bs_idx + 60)
        window_lines = lines[start:end]
        window_text  = '\n'.join(window_lines)

        # INV-R2: Check that inventory row exists in window
        inv_found  = any(_matches(l.lower(), _INV_LABELS) and _nums(l) for l in window_lines)
        if not inv_found:
            # Try expanding window
            end2 = min(total, bs_idx + 100)
            window_lines = lines[start:end2]
            window_text  = '\n'.join(window_lines)
            inv_found = any(_matches(l.lower(), _INV_LABELS) and _nums(l) for l in window_lines)

        if not inv_found:
            logger.info("  Inventory extractor: Balance Sheet found but no Inventory row in window")
            return ''

        # INV-R3: Count distinct year columns in window to detect if beginning inv must be estimated
        year_pat = re.compile(r'(?<!\d)(20\d{2}|FY\s?\d{2,4})(?!\d)', re.IGNORECASE)
        year_tokens = set()
        for line in window_lines[:10]:  # look at header lines
            year_tokens.update(year_pat.findall(line))

        note = ''
        if len(year_tokens) <= 1:
            note = "\nNOTE: Only 1 year of Balance Sheet data found — beg_inventory will be estimated as end_inventory × 0.95\n"

        logger.info(
            f"  Inventory extractor: BS section found at line {bs_idx}, "
            f"{len(year_tokens)} year(s) detected"
        )
        return window_text + note

    except Exception as exc:
        logger.warning(f"  Inventory extractor: error ({exc}) — returning empty string")
        return ''
