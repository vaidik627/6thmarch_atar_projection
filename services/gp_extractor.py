"""
gp_extractor.py — Stage 2 of the GP & COGS extraction pipeline.

Scans the already-identified P&L window (from revenue_extractor.py) for
Gross Profit and COGS rows. Python decides scope; LLM only formats.

Public API:
    extract_gp_text(pnl_window: str, fy_years: tuple) -> str

Returns a GP-R5 formatted scoped text block for the focused LLM call.
Returns empty string if GP row not found (triggers fallback chain in app.py).
"""

import re
import logging

logger = logging.getLogger(__name__)

# ── GP row labels (priority order, lower index = higher priority) ─────────────
_GP_LABELS_P1 = ['gross profit', 'gross income', 'gross earnings']
_GP_LABELS_P2 = ['gross margin']          # only when dollar values, not % values
_GP_LABELS_P3 = ['total gross profit', 'net gross profit']
_GP_LABELS_ALL = _GP_LABELS_P1 + _GP_LABELS_P2 + _GP_LABELS_P3

# ── COGS row labels ───────────────────────────────────────────────────────────
_COGS_LABELS = [
    'cost of goods sold', 'cogs', 'cost of sales', 'cost of revenue',
    'cost of products sold', 'direct costs', 'cost of services',
    'total cost of goods sold', 'total cost of sales',
]
_COGS_SKIP = ['operating expenses', 'sg&a', 'sga', 'overhead']

# ── Revenue row labels (for sanity check) ─────────────────────────────────────
_REV_LABELS = ['total revenue', 'net revenue', 'net sales', 'revenue', 'total sales', 'sales']

# ── Numeric value regex ───────────────────────────────────────────────────────
_NUM_PAT = re.compile(r'[\d,]+(?:\.\d+)?')


def _extract_numbers(line: str):
    """Return list of floats from a line."""
    nums = []
    for m in _NUM_PAT.finditer(line):
        try:
            nums.append(float(m.group().replace(',', '')))
        except ValueError:
            pass
    return nums


def _max_number(line: str) -> float:
    return max(_extract_numbers(line)) if _extract_numbers(line) else 0.0


def _is_percent_row(line: str) -> bool:
    """Return True if line appears to be a % row (contains %, 'margin %', etc.)."""
    return '%' in line or 'margin %' in line.lower() or 'gross margin %' in line.lower()


def _matches_label(line_lower: str, labels: list) -> bool:
    return any(lbl in line_lower for lbl in labels)


def extract_gp_text(pnl_window: str, fy_years: tuple) -> str:
    """
    Scan the P&L window for Gross Profit and COGS rows.

    Args:
        pnl_window: scoped text from revenue_extractor (may include === markers) or full OCR text.
        fy_years:   (y1, y2, y3) tuple of detected fiscal year integers.

    Returns:
        GP-R5 formatted scoped text block, or empty string if GP row not found.
    """
    try:
        y1, y2, y3 = fy_years

        # GP-R1: Strip === REVENUE TABLE markers if present to get raw window lines
        raw_text = pnl_window
        if '=== REVENUE TABLE' in raw_text:
            raw_text = re.sub(r'=== REVENUE TABLE.*?===\n?', '', raw_text, flags=re.DOTALL)
            raw_text = raw_text.replace('=== END REVENUE TABLE ===', '').strip()

        lines = raw_text.splitlines()

        # ── Scan lines for GP, COGS, Revenue rows ────────────────────────────
        gp_line   = None
        cogs_line = None
        rev_line  = None

        for line in lines:
            ll = line.lower().strip()

            # Skip percentage rows for GP candidates
            if _is_percent_row(line):
                continue

            # GP-R2/R3/R4: find GP row (priority order), COGS row, Revenue row
            if gp_line is None:
                if _matches_label(ll, _GP_LABELS_P1 + _GP_LABELS_P3):
                    if _extract_numbers(line):
                        gp_line = line
                elif _matches_label(ll, _GP_LABELS_P2):
                    # 'Gross Margin' only accepted when it has numeric values and no % sign
                    if _extract_numbers(line) and not _is_percent_row(line):
                        gp_line = line

            if cogs_line is None:
                # Skip COGS skip labels
                if not any(skip in ll for skip in _COGS_SKIP):
                    if _matches_label(ll, _COGS_LABELS) and _extract_numbers(line):
                        cogs_line = line

            if rev_line is None:
                if _matches_label(ll, _REV_LABELS) and _extract_numbers(line):
                    rev_line = line

        if gp_line is None:
            logger.info("  GP extractor: no GP row found in P&L window — returning empty string")
            return ''

        # GP-R3: Sanity checks
        gp_nums  = _extract_numbers(gp_line)
        rev_nums = _extract_numbers(rev_line) if rev_line else []
        sanity_lines = []

        if gp_nums and rev_nums:
            gp_max  = max(gp_nums)
            rev_max = max(rev_nums)
            if rev_max > 0:
                margin = gp_max / rev_max
                status = 'OK' if gp_max < rev_max else 'WARN: GP > Revenue'
                margin_status = 'OK' if 0.05 <= margin <= 0.95 else f'WARN: margin {margin:.1%} outside 5-95%'
                sanity_lines.append(f"FY{y3}: GP({gp_max:,.0f}) vs Revenue({rev_max:,.0f}) — {status} | Margin: {margin:.1%} — {margin_status}")
            else:
                sanity_lines.append(f"FY{y3}: Revenue not found in window — cannot verify GP < Revenue")
        elif gp_nums:
            gp_max = max(gp_nums)
            if gp_max < 0:
                sanity_lines.append(f"FY{y3}: Negative GP ({gp_max:,.0f}) — flag for review (conf=0.6)")
            else:
                sanity_lines.append(f"FY{y3}: GP({gp_max:,.0f}) — Revenue not in window, cannot verify")

        # GP-R5: Return formatted scoped text block
        cogs_status = 'FOUND' if cogs_line else 'NOT FOUND'
        gp_status   = 'FOUND' if gp_line else 'NOT FOUND'
        rev_status  = 'FOUND' if rev_line else 'NOT FOUND'

        block = (
            f"GROSS PROFIT EXTRACTION — SCOPED TEXT\n"
            f"Year Headers: {y1}A | {y2}A | {y3}A\n"
            f"Revenue Row: {rev_line.strip() if rev_line else 'NOT FOUND'} [{rev_status}]\n"
            f"COGS Row: {cogs_line.strip() if cogs_line else 'NOT FOUND'} [{cogs_status}]\n"
            f"Gross Profit Row: {gp_line.strip()} [{gp_status}]\n"
            f"GP Sanity Check:\n"
        )
        for s in sanity_lines:
            block += f"  {s}\n"
        block += "NOTE: Values in $000s\n"

        logger.info(
            f"  GP extractor: GP row found [{gp_status}], "
            f"COGS [{cogs_status}], Revenue [{rev_status}]"
        )
        return block

    except Exception as exc:
        logger.warning(f"  GP extractor: unexpected error ({exc}) — returning empty string")
        return ''
