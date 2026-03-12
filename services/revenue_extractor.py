"""
revenue_extractor.py — Stage 2 of the 3-stage revenue extraction pipeline.

Python deterministically scopes the correct revenue table section from OCR text
so the focused LLM call (Stage 3) only needs to FORMAT what it receives.

Public API:
    extract_revenue_text(ocr_text: str) -> str

Returns a scoped block containing the 121-line window around the year-header anchor.
Falls back to the original ocr_text if no valid table is found.
"""

import re
import logging

logger = logging.getLogger(__name__)

# ── Accepted revenue row labels (consolidated P&L indicators) ─────────────────
_ACCEPT_LABELS = [
    "total revenue", "net revenue", "net sales", "revenue", "total sales", "sales"
]

# ── Segment/geo keywords that indicate we may only have a breakdown table ─────
_REJECT_LABELS = [
    "americas", "emea", "apac", "international", "domestic", "segment"
]

# ── Year-token regex ──────────────────────────────────────────────────────────
_YEAR_PAT = re.compile(r'(?<!\d)(FY\s?\d{2,4}|CY\s?\d{2,4}|20\d{2}|19\d{2})(?!\d)', re.IGNORECASE)

# ── Numeric value regex (matches 1,234 or 1234 or 1,234.5) ───────────────────
_NUM_PAT = re.compile(r'[\d,]+(?:\.\d+)?')


def _extract_numbers(line: str):
    """Return list of floats parsed from a line."""
    nums = []
    for m in _NUM_PAT.finditer(line):
        try:
            nums.append(float(m.group().replace(',', '')))
        except ValueError:
            pass
    return nums


def _max_number(line: str) -> float:
    """Return the largest number found in a line, or 0."""
    nums = _extract_numbers(line)
    return max(nums) if nums else 0.0


def extract_revenue_text(ocr_text: str) -> str:
    """
    Scope the OCR text to the 121-line window around the primary revenue table.

    Rules implemented:
        R-1: Locate the year-header anchor line (line with most year tokens, min 3).
        R-2: Capture 60 lines above + 60 lines below the anchor.
        R-3: Consolidated table validation — check for ACCEPT/REJECT keywords.
        R-4: Year label classification (informational — window passed as-is to LLM).
        R-5: Revenue row extraction for REJECT guard.
        R-6: Wrap window in ==='s and return.

    Fallback: returns original ocr_text unchanged if no valid table found.
    """
    try:
        lines = ocr_text.splitlines()
        total_lines = len(lines)

        # ── R-1: Find anchor line ─────────────────────────────────────────────
        best_idx = -1
        best_count = 0

        for i, line in enumerate(lines):
            matches = _YEAR_PAT.findall(line)
            count = len(matches)
            if count > best_count or (count == best_count and count >= 3 and i > best_idx):
                best_count = count
                best_idx = i

        if best_count < 3:
            logger.info("  Revenue scoper (R-1): no line with ≥3 year tokens — returning full OCR text")
            return ocr_text

        logger.info(f"  Revenue scoper (R-1): anchor line {best_idx} ({best_count} year tokens): "
                    f"{lines[best_idx].strip()[:80]}")

        # ── R-2: 60-line context window ───────────────────────────────────────
        start = max(0, best_idx - 60)
        end = min(total_lines, best_idx + 61)  # +61 so best_idx is included + 60 below
        window_lines = lines[start:end]

        window_text = '\n'.join(window_lines)
        window_lower = window_text.lower()

        # ── R-3: Consolidated table validation ───────────────────────────────
        has_accept = any(label in window_lower for label in _ACCEPT_LABELS)
        has_reject = any(label in window_lower for label in _REJECT_LABELS)

        if not has_accept:
            logger.info("  Revenue scoper (R-3): no consolidated revenue label in window — returning full OCR text")
            return ocr_text

        if has_reject:
            # Find max revenue value from ACCEPT rows vs REJECT rows.
            # If reject-table revenue dominates, fall back to full text.
            accept_max = 0.0
            reject_max = 0.0
            for wline in window_lines:
                wline_lower = wline.lower()
                if any(lbl in wline_lower for lbl in _ACCEPT_LABELS):
                    v = _max_number(wline)
                    if v > accept_max:
                        accept_max = v
                if any(lbl in wline_lower for lbl in _REJECT_LABELS):
                    v = _max_number(wline)
                    if v > reject_max:
                        reject_max = v

            if reject_max > accept_max > 0:
                logger.info(
                    f"  Revenue scoper (R-3): REJECT table value ({reject_max:,.0f}) > "
                    f"ACCEPT table value ({accept_max:,.0f}) — returning full OCR text"
                )
                return ocr_text
            else:
                logger.info(
                    f"  Revenue scoper (R-3): ACCEPT ({accept_max:,.0f}) ≥ REJECT ({reject_max:,.0f}) — "
                    "proceeding with scoped window"
                )
        else:
            logger.info(f"  Revenue scoper (R-3): consolidated revenue label found, no segment keywords")

        # ── R-6: Return scoped block ──────────────────────────────────────────
        scoped = (
            "=== REVENUE TABLE (scoped by revenue_extractor.py) ===\n"
            + window_text
            + "\n=== END REVENUE TABLE ==="
        )
        logger.info(
            f"  Revenue scoper: scoped to {len(scoped):,} chars "
            f"(lines {start}–{end - 1} of {total_lines})"
        )
        return scoped

    except Exception as exc:
        logger.warning(f"  Revenue scoper: unexpected error ({exc}) — returning full OCR text")
        return ocr_text
