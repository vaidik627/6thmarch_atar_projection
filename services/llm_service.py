
"""
LLM Service — NVIDIA NIM (GPT-OSS-120B) financial field extraction.

Handles large OCR text from 70-100 page PDFs:
1. Smart section extraction: scores text windows by financial keyword density
   and selects the most relevant sections (P&L, balance sheet, EBITDA tables)
2. Two-pass extraction if text is very large:
   - Pass 1: find which windows have the financial tables
   - Pass 2: focused extraction from those windows
3. Retry logic with exponential backoff (3 attempts)
4. JSON schema validation on every response
"""
import os
import json
import re
import time
import logging
import datetime
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

NVIDIA_API_KEY = os.getenv('NVIDIA_API_KEY')
NVIDIA_BASE_URL = os.getenv('NVIDIA_BASE_URL', 'https://integrate.api.nvidia.com/v1')
NVIDIA_MODEL = os.getenv('NVIDIA_MODEL', 'openai/gpt-oss-120b')
EXTRACTIONS_FOLDER = os.getenv('EXTRACTIONS_FOLDER', 'storage/extractions')

# How much text to send per LLM call (chars).
# GPT-OSS-120B supports ~128k tokens ≈ ~500k chars, but staying at 30k
# keeps latency manageable and cost lower.
MAX_CONTEXT_CHARS = 30_000

# Financial keyword patterns used to score windows
FINANCIAL_KEYWORDS = [
    r'\brevenue\b', r'\bnet revenue\b', r'\bnet sales\b', r'\bsales\b',
    r'\bgross profit\b', r'\bgross margin\b',
    r'\bebitda\b', r'\badj.*ebitda\b', r'\badjusted ebitda\b',
    r'\boperating income\b', r'\boperating profit\b',
    r'\bsg&a\b', r'\bselling.*general.*admin\b',
    r'\binterest expense\b', r'\binterest income\b',
    r'\bnet income\b', r'\bnet loss\b',
    r'\btotal assets\b', r'\btotal liabilities\b',
    r'\binventory\b', r'\baccounts receivable\b',
    r'\bterm loan\b', r'\bcredit facility\b', r'\bline of credit\b',
    r'\bfiscal year\b', r'\bfy\d{2,4}\b', r'\b20\d{2}\b',
    r'\$[\d,]+', r'\d{1,3}(?:,\d{3})+',   # dollar amounts / large numbers
]
_KW_PATTERNS = [re.compile(kw, re.IGNORECASE) for kw in FINANCIAL_KEYWORDS]

# Fields the LLM must extract
EXTRACTION_SCHEMA = {
    "company_name": "The name of the company being analyzed",
    "fy_year_1": "The earliest historical fiscal year label (e.g. FY2019 or 2019)",
    "fy_year_2": "The middle historical fiscal year label (e.g. FY2020 or 2020)",
    "fy_year_3": "The most recent historical fiscal year label (e.g. FY2021 or 2021)",
    "revenue_fy1": "TOTAL consolidated company-wide top-line revenue for the EARLIEST fiscal year (fy1, per FISCAL YEAR MAPPING above) in $000s. Pick the SINGLE largest revenue row in the P&L — it must be ≥ every other revenue line in that table. NEVER use segment/division/geographic/product-line sub-totals. Labels: 'Total Revenue', 'Net Revenue', 'Net Sales', 'Revenue', 'Total Sales', 'Sales'.",
    "revenue_fy2": "TOTAL consolidated company-wide top-line revenue for the MIDDLE fiscal year (fy2) in $000s. Same rule — use the largest/topmost revenue figure in the P&L, never a segment or breakdown sub-row.",
    "revenue_fy3": "TOTAL consolidated company-wide top-line revenue for the MOST RECENT fiscal year (fy3) in $000s. Same rule — largest single revenue figure, company-wide consolidated, not a segment.",
    "gross_margin_fy1": "Gross profit/gross margin dollar amount for earliest fiscal year (in $000s). = Revenue minus Cost of Goods Sold.",
    "gross_margin_fy2": "Gross margin for middle fiscal year (in $000s).",
    "gross_margin_fy3": "Gross margin for most recent fiscal year (in $000s).",
    "cogs_fy1": "Cost of Revenue / COGS for earliest fiscal year in $000s. Extract only if gross margin row is absent.",
    "cogs_fy2": "Cost of Revenue / COGS for middle fiscal year in $000s. Extract only if gross margin row is absent.",
    "cogs_fy3": "Cost of Revenue / COGS for most recent fiscal year in $000s. Extract only if gross margin row is absent.",
    "sga_fy1": "Selling, General & Administrative expenses for earliest fiscal year (in $000s).",
    "sga_fy2": "SG&A for middle fiscal year (in $000s).",
    "sga_fy3": "SG&A for most recent fiscal year (in $000s).",
    "interest_expense_fy1": "Interest expense/(income) for earliest fiscal year (in $000s).",
    "interest_expense_fy2": "Interest expense for middle fiscal year (in $000s).",
    "interest_expense_fy3": "Interest expense for most recent fiscal year (in $000s).",
    "adjustments_fy1": "One-time/non-recurring items for earliest fiscal year (in $000s).",
    "adjustments_fy2": "One-time adjustments for middle fiscal year (in $000s).",
    "adjustments_fy3": "One-time adjustments for most recent fiscal year (in $000s).",
    "adj_ebitda_fy1": "Adjusted EBITDA for earliest fiscal year in $000s. Accept ANY label (case-insensitive): 'Adj. EBITDA', 'Adjusted EBITDA', 'Adj EBITDA', 'EBITDA (Adjusted)', 'EBITDA (as adjusted)', 'Normalized EBITDA', 'Recurring EBITDA'. Prefer the row immediately following a 'Total Add-backs' or 'Non-Recurring Adjustments' subtotal. Fallback: derive as operating_income + adjustments (confidence 0.75).",
    "adj_ebitda_fy2": "Adjusted EBITDA for middle fiscal year in $000s. Same label variants and fallback derivation as adj_ebitda_fy1.",
    "adj_ebitda_fy3": "Adjusted EBITDA for most recent fiscal year in $000s. Same label variants and fallback derivation as adj_ebitda_fy1.",
    "net_revenue_collateral": "Net revenue value as collateral/borrowing base — most recent year (in $000s).",
    "inventory_collateral": "Physical inventory book value for collateral (in $000s).",
    "me_equipment_collateral": "Machinery and equipment book value for collateral (in $000s).",
    "building_land_collateral": "GROSS asset value of real estate / building / land in $000s. If a table shows 'Asset Value | Advance Rate | Borrowing Base', return ONLY the Asset Value column — NOT the advance-rate-adjusted borrowing base. If the company is a pure leaseholder or gross value is $0, return 0.",
    "existing_term_loans": "Outstanding principal balance of any existing term loan or cashflow loan (NOT the revolving ABL/revolver) in $000s. Look in: debt/capital structure tables, 'Sources & Uses', 'Financing' sections. Phrases: 'term loan outstanding', 'term facility balance', '$X.XM outstanding'. Return outstanding balance (use original principal if outstanding not stated separately). Labels: 'Term Loans/Cashflow loans', 'Existing Term Debt'.",
    # ── Projection / Forecast data (extract ONLY if document contains explicit forward-looking statements)
    "proj_revenue_y1": "Projected/budgeted TOTAL revenue Year 1 (first future year after most recent historical) in $000s. Return null if document has no explicit projections.",
    "proj_revenue_y2": "Projected total revenue Year 2 in $000s. Null if not in document.",
    "proj_revenue_y3": "Projected total revenue Year 3 in $000s. Null if not in document.",
    "proj_revenue_y4": "Projected total revenue Year 4 in $000s. Null if not in document.",
    "proj_revenue_y5": "Projected total revenue Year 5 in $000s. Null if not in document.",
    "proj_gross_margin_y1": "Projected gross margin Year 1 in $000s. Null if not in document.",
    "proj_gross_margin_y2": "Projected gross margin Year 2 in $000s. Null if not in document.",
    "proj_gross_margin_y3": "Projected gross margin Year 3 in $000s. Null if not in document.",
    "proj_gross_margin_y4": "Projected gross margin Year 4 in $000s. Null if not in document.",
    "proj_gross_margin_y5": "Projected gross margin Year 5 in $000s. Null if not in document.",
    "proj_sga_y1": "Projected SG&A Year 1 in $000s. Null if not in document.",
    "proj_sga_y2": "Projected SG&A Year 2 in $000s. Null if not in document.",
    "proj_sga_y3": "Projected SG&A Year 3 in $000s. Null if not in document.",
    "proj_sga_y4": "Projected SG&A Year 4 in $000s. Null if not in document.",
    "proj_sga_y5": "Projected SG&A Year 5 in $000s. Null if not in document.",
}

SYSTEM_PROMPT = """You are a financial data extraction specialist for Atar Capital private equity deal analysis.
Your only output is a single strict JSON object — no prose, no markdown, no explanation.

RULES (all 10 are non-negotiable):

RULE 1 — NULL OVER GUESS:
  Return null for any field where you cannot find the value with confidence >= 0.70.
  Never estimate, interpolate, or fabricate. Silence is correct. Guessing is a failure.

RULE 2 — MONETARY UNITS:
  All monetary values must be in $000s (thousands of USD).
  Convert: $5.2M → 5200 | $4,800,000 → 4800 | $500K → 500 | $1.2B → 1200000
  Read the unit qualifier (M, K, B, thousands, millions) from the table caption or column note.

RULE 3 — REVENUE = CONSOLIDATED TOP-LINE ONLY:
  Revenue must be the single largest consolidated total for the entire company.
  FORBIDDEN sub-revenue labels (reject even if they appear largest in a sub-section):
    Product Revenue, Service Revenue, Recurring Revenue, Subscription Revenue,
    Professional Services, Segment [X], Region [X], Geographic [X],
    Division [X], Business Unit [X], Brand [X], Channel [X], Category [X]

RULE 4 — OUTPUT FORMAT:
  Each field: { "value": <number|null>, "confidence": <0.0-1.0>, "citation": "<quoted source>" }
  Output the JSON object only. No preamble. No trailing text.

RULE 5 — CONFIDENCE THRESHOLDS:
  >= 0.90 → value explicitly stated, exact label match, correct year column confirmed
  0.70–0.89 → value is probable (reasonable table alignment, inferred from context)
  0.65–0.69 → DERIVED value only (computed from formula — see Rule 6). Return with confidence 0.75.
  < 0.70 → return null — do not guess

RULE 6 — FORMULA DERIVATION (apply when direct extraction fails):
  Gross Margin ($)  = Revenue − COGS            [if COGS row present]
  Operating Income  = Gross Margin − SG&A
  Adj. EBITDA       = Operating Income + Adjustments + Depreciation + Amortisation
  CFADS             = Adj. EBITDA − CAPEX − Working Capital Change − Taxes
  FCCR              = (Adj. EBITDA − CAPEX) / Total Debt Service
  Mark confidence 0.75 for any derived value. Cite the formula used in citation field.

RULE 7 — YEAR ANCHOR (FULL-FORM AND SHORT-FORM LABELS):
  The detected fiscal years are injected into the extraction prompt as fy1, fy2, fy3.
  You MUST match values to columns using ALL of these header variants:
    Full 4-digit : {fy1}, FY{fy1}, FY {fy1}, FY-{fy1}
    Short 2-digit : FY{yy1}, FY'{yy1}, '{yy1}  (where yy1 = last 2 digits of fy1)
    Suffixed      : {fy1}A, {fy1} Actual, {fy1} Audited
  Apply the same pattern for fy2 and fy3.
  If a column header does NOT match any variant above for the target year → set value to null.

RULE 8 — COLUMN ASSIGNMENT BY HEADER MATCH, NOT POSITION:
  When a financial table has 4 or more year columns, do NOT assign by left-to-right index.
  Instead: scan ALL column headers first, identify which column contains fy1/fy2/fy3 labels,
  then read only those columns. Ignore all columns that do not match a detected year.

RULE 9 — IGNORE NON-FISCAL COLUMNS:
  The following column types must be COMPLETELY IGNORED for all field extraction:
    LTM, TTM, NTM, Run-Rate, Annualised, Annualized, Pro Forma, PF, Combined,
    Adjusted (as standalone column header), Budget (when not a target projection year),
    Q1/Q2/Q3/Q4 (quarterly columns).
  Do not assign any value from these columns to any fy1/fy2/fy3 or projection slot.

RULE 10 — RESTATED vs AS-REPORTED:
  If a document contains two columns for the same fiscal year with labels such as
  'As Reported', 'As Filed', 'Restated', 'Revised', 'Adjusted':
  Priority order: Restated > Revised > Adjusted > As Reported > As Filed > unlabelled.
  Always use the highest-priority column. Record BOTH values in the citation field:
  citation: 'Restated: {val_r} | As Reported: {val_ar} — used Restated per Rule 10'
"""


def _build_system_prompt(y1: int, y2: int, y3: int) -> str:
    """Return SYSTEM_PROMPT with actual detected year integers injected into year-anchor rules."""
    yy1 = str(y1)[2:]
    yy2 = str(y2)[2:]
    yy3 = str(y3)[2:]
    return f"""You are a financial data extraction specialist for Atar Capital private equity deal analysis.
Your only output is a single strict JSON object — no prose, no markdown, no explanation.

RULES (all 10 are non-negotiable):

RULE 1 — NULL OVER GUESS:
  Return null for any field where you cannot find the value with confidence >= 0.70.
  Never estimate, interpolate, or fabricate. Silence is correct. Guessing is a failure.

RULE 2 — MONETARY UNITS:
  All monetary values must be in $000s (thousands of USD).
  Convert: $5.2M → 5200 | $4,800,000 → 4800 | $500K → 500 | $1.2B → 1200000
  Read the unit qualifier (M, K, B, thousands, millions) from the table caption or column note.

RULE 3 — REVENUE = CONSOLIDATED TOP-LINE ONLY:
  Revenue must be the single largest consolidated total for the entire company.
  FORBIDDEN sub-revenue labels (reject even if they appear largest in a sub-section):
    Product Revenue, Service Revenue, Recurring Revenue, Subscription Revenue,
    Professional Services, Segment [X], Region [X], Geographic [X],
    Division [X], Business Unit [X], Brand [X], Channel [X], Category [X]

RULE 4 — OUTPUT FORMAT:
  Each field: {{"value": <number|null>, "confidence": <0.0-1.0>, "citation": "<quoted source>"}}
  Output the JSON object only. No preamble. No trailing text.

RULE 5 — CONFIDENCE THRESHOLDS:
  >= 0.90 → value explicitly stated, exact label match, correct year column confirmed
  0.70–0.89 → value is probable (reasonable table alignment, inferred from context)
  0.65–0.69 → DERIVED value only (computed from formula — see Rule 6). Return with confidence 0.75.
  < 0.70 → return null — do not guess

RULE 6 — FORMULA DERIVATION (apply when direct extraction fails):
  Gross Margin ($)  = Revenue − COGS            [if COGS row present]
  Operating Income  = Gross Margin − SG&A
  Adj. EBITDA       = Operating Income + Adjustments + Depreciation + Amortisation
  CFADS             = Adj. EBITDA − CAPEX − Working Capital Change − Taxes
  FCCR              = (Adj. EBITDA − CAPEX) / Total Debt Service
  Mark confidence 0.75 for any derived value. Cite the formula used in citation field.

RULE 7 — YEAR ANCHOR (FULL-FORM AND SHORT-FORM LABELS):
  Detected fiscal years for this document: fy1={y1}, fy2={y2}, fy3={y3}.
  You MUST match values to columns using ALL of these header variants:
    fy1: {y1}, FY{y1}, FY {y1}, FY-{y1}, FY{yy1}, FY'{yy1}, '{yy1}, {y1}A, {y1} Actual, {y1} Audited
    fy2: {y2}, FY{y2}, FY {y2}, FY-{y2}, FY{yy2}, FY'{yy2}, '{yy2}, {y2}A, {y2} Actual, {y2} Audited
    fy3: {y3}, FY{y3}, FY {y3}, FY-{y3}, FY{yy3}, FY'{yy3}, '{yy3}, {y3}A, {y3} Actual, {y3} Audited
  Projection slots (y1..y5) map to calendar years {y3+1}..{y3+5}.
  Column headers with suffix E/F/B/P (e.g. {y3+1}E, FY{y3+1}F) are projections only.
  If a column header does NOT match any variant above for the target year → set value to null.

RULE 8 — COLUMN ASSIGNMENT BY HEADER MATCH, NOT POSITION:
  When a financial table has 4 or more year columns, do NOT assign by left-to-right index.
  Instead: scan ALL column headers first, identify which column contains fy1/fy2/fy3 labels,
  then read only those columns. Ignore all columns that do not match a detected year.

RULE 9 — IGNORE NON-FISCAL COLUMNS:
  The following column types must be COMPLETELY IGNORED for all field extraction:
    LTM, TTM, NTM, Run-Rate, Annualised, Annualized, Pro Forma, PF, Combined,
    Adjusted (as standalone column header), Budget (when not a target projection year),
    Q1/Q2/Q3/Q4 (quarterly columns).
  Do not assign any value from these columns to any fy1/fy2/fy3 or projection slot.

RULE 10 — RESTATED vs AS-REPORTED:
  If a document contains two columns for the same fiscal year with labels such as
  'As Reported', 'As Filed', 'Restated', 'Revised', 'Adjusted':
  Priority order: Restated > Revised > Adjusted > As Reported > As Filed > unlabelled.
  Always use the highest-priority column. Record BOTH values in the citation field:
  citation: 'Restated: {{val_r}} | As Reported: {{val_ar}} — used Restated per Rule 10'

Output ONLY the JSON object. No preamble. No trailing text.
"""


def _score_window(text: str) -> int:
    """Score a text window by number of financial keyword matches."""
    return sum(1 for pat in _KW_PATTERNS if pat.search(text))


def _extract_financial_sections(ocr_text: str, target_chars: int = MAX_CONTEXT_CHARS) -> str:
    """
    Extracts the most financially-relevant sections from OCR text.
    Scoring uses TWO axes:
    1. Keyword density — finance keywords per 500-char window
    2. Numeric table density — lines with 3+ numbers per window
    The window with highest numeric density is ALWAYS force-included.
    This ensures actual P&L tables always enter the output even when
    narrative prose scores higher on keyword density alone.
    """
    if len(ocr_text) <= target_chars:
        return ocr_text  # Small enough — use everything

    WINDOW = 1500       # chars per scoring window
    MAX_CHARS = target_chars

    KEYWORDS = [
        'revenue', 'net revenue', 'gross margin', 'ebitda', 'adj. ebitda',
        'adjusted ebitda', 'sga', 'sg&a', 'operating income', 'interest',
        'depreciation', 'amortization', 'capex', 'cash flow', 'net income',
        'total revenue', 'sales', 'cost of', 'cogs', 'gross profit',
        'income statement', 'profit and loss', 'p&l', 'financial summary',
        'historical', 'fiscal year', 'fy20', 'fy21', 'fy22', 'fy23', 'fy24',
        'projection', 'forecast', 'budget', 'management case',
    ]

    NUM_PATTERN = re.compile(r'\b\d[\d,]*\.?\d*\b')
    text_lower = ocr_text.lower()
    windows = []

    for i in range(0, len(ocr_text), WINDOW):
        chunk = ocr_text[i:i + WINDOW]
        chunk_lower = text_lower[i:i + WINDOW]

        # Axis 1: keyword score
        kw_score = sum(chunk_lower.count(kw) for kw in KEYWORDS)

        # Axis 2: numeric table density — count lines with 3+ numbers
        numeric_score = 0
        lines_in_chunk = chunk.split('\n')
        num_lines_count = sum(
            1 for line in lines_in_chunk
            if len(NUM_PATTERN.findall(line)) >= 1
        )
        for line in lines_in_chunk:
            nums_on_line = NUM_PATTERN.findall(line)
            if len(nums_on_line) >= 3:
                numeric_score += 3   # strong table-row signal
            elif len(nums_on_line) == 2:
                numeric_score += 1   # weak signal
        # Dense block of single-value lines = one-value-per-line OCR table format
        if num_lines_count >= 8:
            numeric_score += 5   # bonus: 8+ numeric lines in window = very likely a P&L table

        combined_score = kw_score + numeric_score
        windows.append((combined_score, numeric_score, i, chunk))

    # Sort by combined score descending
    windows.sort(key=lambda x: x[0], reverse=True)

    # FORCE-INCLUDE: the window with the highest pure numeric density
    # (ensures the actual P&L table is always in the output chars)
    best_numeric_idx = max(range(len(windows)), key=lambda idx: windows[idx][1])
    force_window = windows.pop(best_numeric_idx)

    # Build output: force window first, then top keyword+numeric windows
    selected = [force_window]
    total_chars = len(force_window[3])

    for win in windows:
        if total_chars >= MAX_CHARS:
            break
        # Avoid duplicate if force_window was already in top results
        if win[2] != force_window[2]:
            selected.append(win)
            total_chars += len(win[3])

    # Sort selected windows by original position (preserve reading order)
    selected.sort(key=lambda x: x[2])

    result = '\n'.join(w[3] for w in selected)[:MAX_CHARS]
    logger.info(
        f"Text condensed: {len(ocr_text):,} → {len(result):,} chars "
        f"({len(selected)} windows selected, numeric-density force-included)"
    )
    return result


def _detect_fiscal_years(ocr_text: str) -> tuple[int, int, int]:
    """
    Detect the 3 most recent HISTORICAL fiscal years present in the document.

    Strategy (two-pass):
    1. PRIMARY — table-header lines: lines where ≥2 non-projection years appear together.
    2. SECONDARY — frequency supplement: if fewer than 3 table-header years found, supplement
       with years mentioned ≥2 times anywhere (excluding projection-suffix years).
    HARD CAP: years > current calendar year are always excluded.
    PROJECTION EXCLUSION (two layers):
      Layer 1 — Suffix: years followed by E/F/B/P (Estimate/Forecast/Budget/Projected),
        including OCR-embedded variants like "FY2025F" (single token) and "2025 F" (split).
        Uses digit-boundary (?<!\d) / (?!\d) instead of \b to match years inside "FYxxxx" tokens.
      Layer 2 — Section context: years on lines AFTER a "Management Forecast" / "Budget" /
        "Financial Projections" section header are treated as projections even without suffix.
        This handles OCR stripping the suffix entirely.
    """
    current_year = datetime.datetime.now().year

    # Digit-boundary year pattern: matches "2025" inside "FY2025", "FY2025F", "FY 2025", etc.
    # (?<!\d) = not preceded by digit; (?!\d) = not followed by digit
    year_pat = re.compile(r'(?<!\d)(20\d{2})(?!\d)')

    # Projection-suffix pattern: matches years with E/F/B/P suffix in all OCR formats:
    #   "FY2025F" (single token), "FY 2025F", "FY 2025 F", "2025E", "2025 E"
    # (?![A-Za-z0-9]) after suffix = suffix is not part of a longer word (e.g. "2025FORECAST")
    proj_suffix_pat = re.compile(r'(?<!\d)(20\d{2})\s*[EFBPefbp](?![A-Za-z0-9])')

    # Section-header patterns for projection / historical context (start-of-line anchored)
    proj_section_pat = re.compile(
        r'^\s*(?:management\s+(?:forecast|case|projections?)'
        r'|financial\s+projections?'
        r'|(?:forecast|budget|projections?)\s*$)',
        re.IGNORECASE
    )
    hist_section_pat = re.compile(
        r'^\s*(?:historical\s+financial|historical\s+summary|actual\s+results?|audited)\b',
        re.IGNORECASE
    )

    # Pre-compute all projection-suffix years from entire document (for Pass 2)
    proj_yrs_by_suffix = {int(m.group(1)) for m in proj_suffix_pat.finditer(ocr_text)}

    # Pre-compute confirmed historical years — those with A/Actual/Audited/Restated marker.
    # Used by Guard 2 below to verify that the chosen y3 is genuinely historical,
    # not a projection year that leaked through when the document omits E/F/B/P suffixes.
    _hist_marker_pat = re.compile(
        r'(?<!\d)(20\d{2})\s*[Aa](?![A-Za-z0-9])'           # "FY2024A", "2024A"
        r'|(?<!\d)(20\d{2})\b[^\n]{0,30}?(?:Actual|Audited|Restated)\b',  # "FY2024 Actual"
        re.IGNORECASE
    )
    confirmed_hist_years: set[int] = set()
    for _hm in _hist_marker_pat.finditer(ocr_text):
        _yr_str = _hm.group(1) or _hm.group(2)
        if _yr_str:
            _hyr = int(_yr_str)
            if 2000 <= _hyr <= current_year:
                confirmed_hist_years.add(_hyr)
    logger.info(f"  Confirmed-historical years (A/Actual/Audited markers): {sorted(confirmed_hist_years)}")

    # ── Pass 1: table-header lines (primary signal) ───────────────────────────
    table_counts: dict[int, int] = {}
    in_proj_section = False
    for line in ocr_text.split('\n'):
        # Update section context
        if hist_section_pat.search(line):
            in_proj_section = False
        if proj_section_pat.search(line):
            in_proj_section = True

        suffix_proj_on_line = {int(m.group(1)) for m in proj_suffix_pat.finditer(line)}
        all_yrs_on_line = {
            int(m.group(1)) for m in year_pat.finditer(line)
            if 2000 <= int(m.group(1)) <= current_year
        }
        # Layer 2: if inside a projection section, ALL years on this line are projection years
        excluded = all_yrs_on_line if in_proj_section else suffix_proj_on_line

        yrs = [y for y in all_yrs_on_line if y not in excluded]
        if len(yrs) >= 2:          # Multiple non-projection years on same line = table header
            for y in set(yrs):
                table_counts[y] = table_counts.get(y, 0) + 1

    candidates = sorted(table_counts.keys(), reverse=True)
    logger.info(f"  Table-header years found: {candidates} | proj-by-suffix: {sorted(proj_yrs_by_suffix)}")

    # ── Pass 2: frequency-based supplement (secondary signal) ─────────────────
    if len(candidates) < 3:
        freq: dict[int, int] = {}
        for m in year_pat.finditer(ocr_text):
            y = int(m.group(1))
            if 2000 <= y <= current_year and y not in proj_yrs_by_suffix:
                freq[y] = freq.get(y, 0) + 1
        freq_cands = sorted([y for y, c in freq.items() if c >= 2], reverse=True)
        for y in freq_cands:
            if y not in candidates:
                candidates.append(y)
        logger.info(f"  After frequency supplement: {candidates}")

    # ── Pass 3: last-resort scan (single-mention, recent 7-year window) ───────
    # Triggered when OCR layout puts each year on its own line (e.g. pypdf, some
    # Document AI outputs) so neither table-header detection nor frequency≥2 fire.
    # Scans for ANY non-projection year within (current_year − 7) … current_year.
    # Safely excludes founding-year mentions ("Founded in 2016") that fall outside
    # the 7-year window.  Only runs when candidates is still empty.
    if not candidates:
        recent_window = current_year - 7
        last_resort: set[int] = set()
        for m in year_pat.finditer(ocr_text):
            y = int(m.group(1))
            if recent_window <= y <= current_year and y not in proj_yrs_by_suffix:
                last_resort.add(y)
        candidates = sorted(last_resort, reverse=True)
        logger.info(f"  After last-resort scan (≥1 mention, window {recent_window}-{current_year}): {candidates}")

    # ── Assign fy1/fy2/fy3 — always 3 consecutive years anchored to most recent ──
    # Take the single most recent historical year found in the document, then
    # derive the previous two as y3-1 and y3-2 (always consecutive).
    # E.g. if document's latest historical year = 2024 → fy3=2024, fy2=2023, fy1=2022
    # This prevents non-consecutive gaps (e.g. 2024,2022,2020) and ignores older
    # columns when the document contains more than 3 historical years.
    most_recent_year = candidates[0] if candidates else current_year

    # Guard 1 — y3 must never be a future year (hard-cap at current_year)
    # Handles edge case where a projection table header like "FY2025 | FY2026" appears
    # without E/F suffix and current_year happens to equal one of those years.
    if most_recent_year > current_year:
        fallback_years = sorted([y for y in candidates if y <= current_year], reverse=True)
        most_recent_year = fallback_years[0] if fallback_years else current_year - 1
        logger.info(f"  FY detect Guard 1: future year capped → y3={most_recent_year}")

    # Guard 2 — if y3 ONLY appears with projection suffixes (E/F/B/P) and NEVER with
    # historical markers (A/Actual/Audited/Restated), it is a projection year that leaked
    # through because the document omitted suffixes in some tables or the section-context
    # detector did not trigger.  Walk back to the nearest confirmed-historical year.
    if most_recent_year in proj_yrs_by_suffix and most_recent_year not in confirmed_hist_years:
        logger.info(
            f"  FY detect Guard 2: y3={most_recent_year} is proj-suffix-only "
            f"(confirmed_hist={sorted(confirmed_hist_years)}) — walking back"
        )
        hist_candidates = sorted(
            [y for y in candidates if y not in proj_yrs_by_suffix or y in confirmed_hist_years],
            reverse=True
        )
        if hist_candidates:
            most_recent_year = hist_candidates[0]
        else:
            most_recent_year = most_recent_year - 1  # last resort: step back one year
        logger.info(f"  FY detect Guard 2: corrected y3={most_recent_year}")

    y3 = most_recent_year
    y2 = y3 - 1
    y1 = y3 - 2

    logger.info(f"  Detected fiscal years: fy1={y1}  fy2={y2}  fy3={y3}  (most-recent-anchor={y3}, cap={current_year})")
    return y1, y2, y3


def _build_extraction_prompt(text: str, fy_years: tuple[int, int, int] = None) -> str:
    if fy_years:
        fy1, fy2, fy3 = fy_years
    else:
        cy = datetime.datetime.now().year
        fy1, fy2, fy3 = cy - 2, cy - 1, cy

    proj_y1 = fy3 + 1
    proj_y2 = fy3 + 2
    proj_y3 = fy3 + 3
    proj_y4 = fy3 + 4
    proj_y5 = fy3 + 5

    # SHORT-FORM ALIASES — 2-digit year suffix (e.g. 2022 → '22)
    yy1 = str(fy1)[2:]  # e.g. '22'
    yy2 = str(fy2)[2:]  # e.g. '23'
    yy3 = str(fy3)[2:]  # e.g. '24'

    return f"""You have OCR-extracted text from a Confidential Information Memorandum (CIM).
Extract the financial fields listed below using ALL rules from your system prompt.

════════════════════════════════════════════════════════════
SECTION A — FISCAL YEAR MAPPING [CRITICAL — READ FIRST]
════════════════════════════════════════════════════════════
Three historical fiscal years detected in this document:

  fy1 = {fy1}
    Accept these column headers: {fy1}  FY{fy1}  FY {fy1}  FY-{fy1}
                                  FY{yy1}  FY'{yy1}  '{yy1}
                                  {fy1}A  {fy1} Actual  {fy1} Audited

  fy2 = {fy2}
    Accept these column headers: {fy2}  FY{fy2}  FY {fy2}  FY-{fy2}
                                  FY{yy2}  FY'{yy2}  '{yy2}
                                  {fy2}A  {fy2} Actual  {fy2} Audited

  fy3 = {fy3}
    Accept these column headers: {fy3}  FY{fy3}  FY {fy3}  FY-{fy3}
                                  FY{yy3}  FY'{yy3}  '{yy3}
                                  {fy3}A  {fy3} Actual  {fy3} Audited

Projection years (extract ONLY if document has an explicit forecast table):
  y1 = {proj_y1}  Accept: {proj_y1}  FY{proj_y1}  {proj_y1}E  {proj_y1}F  {proj_y1}B  {proj_y1}P
  y2 = {proj_y2}  Accept: {proj_y2}  FY{proj_y2}  {proj_y2}E  {proj_y2}F
  y3 = {proj_y3}  Accept: {proj_y3}  FY{proj_y3}  {proj_y3}E  {proj_y3}F
  y4 = {proj_y4}  Accept: {proj_y4}  FY{proj_y4}  {proj_y4}E  {proj_y4}F
  y5 = {proj_y5}  Accept: {proj_y5}  FY{proj_y5}  {proj_y5}E  {proj_y5}F

COLUMN ASSIGNMENT RULE — BY HEADER MATCH, NOT POSITION:
  1. Scan the table header row and identify which column header matches fy1/fy2/fy3.
  2. Assign values from the matched column only.
  3. If a column header does not match any of the accepted variants above, skip it.
  4. Do NOT assign by left-to-right index. A 5-column table may have columns for
     FY2020/FY2021/FY2022/FY2023/FY2024 — only use the columns for fy1/fy2/fy3.

COLUMNS TO IGNORE ENTIRELY (do not extract any value from these):
  LTM, TTM, NTM, Run-Rate, Annualised, Annualized, Pro Forma, PF,
  Combined, Adjusted (standalone), Budget (non-projection years),
  Q1, Q2, Q3, Q4, and any quarterly-period column.

RESTATED vs AS-REPORTED (when two columns exist for the same year):
  Use priority: Restated > Revised > Adjusted > As Reported > As Filed > unlabelled.
  Record both values in the citation.

COLLATERAL & DEBT FIELDS — IMPORTANT:
  existing_term_loans: Return the OUTSTANDING principal balance of any existing term loan
    or cashflow loan. Do NOT return the revolving ABL/revolver balance.
    Look in: debt/capital structure tables, 'Sources & Uses' section, 'Financing' prose.
    Phrases: "term loan outstanding", "term facility balance", "$X.XM outstanding".
    If only original principal is stated, use that. Values in $000s.
    Common labels: "Term Loans/Cashflow loans", "Existing Term Debt".
  building_land_collateral: Return the GROSS asset value of real estate/building/land.
    If a table shows "Asset Value | Advance Rate | Borrowing Base", return only
    the Asset Value column. Do NOT return the advance-rate-adjusted borrowing base.
    If the company is a pure leaseholder or gross value is $0, return 0 (not null).

════════════════════════════════════════════════════════════
SECTION B — REVENUE IDENTIFICATION
════════════════════════════════════════════════════════════
Accept these row labels: Revenue, Net Revenue, Total Revenue,
  Net Sales, Total Net Sales, Revenues, Total Revenues,
  Gross Revenue, Total Gross Revenue

FORBIDDEN labels (reject even if value is largest in section):
  Product Revenue, Service Revenue, Recurring Revenue, Subscription Revenue,
  Professional Services, Segment [X], Region [X], Geographic [X],
  Division [X], Business Unit [X], Brand [X], Channel [X], Category [X]

If multiple qualifying rows exist, choose labelled 'Total' or the largest
  consolidated figure. Cite the exact row label used.

════════════════════════════════════════════════════════════
SECTION C — FORMULA DERIVATION CHAIN
════════════════════════════════════════════════════════════
Apply these ONLY when the target field is absent from the document.
Set confidence = 0.75 for all derived values. Cite the formula in citation.

  Gross Margin ($)   = Revenue − COGS
  GM%                = Gross Margin / Revenue
  Operating Income   = Gross Margin − SG&A
  Adj. EBITDA        = Operating Income + Adjustments + Depreciation + Amortisation
  EBITDA%            = Adj. EBITDA / Revenue
  Taxable Income     = Adj. EBITDA − Interest − Depreciation
  Taxes              = Taxable Income × 0.30   (default 30%)
  Net Income         = Taxable Income − Taxes
  CFADS              = Adj. EBITDA − CAPEX − Working Capital Change − Taxes
  DSCR               = CFADS / Total Debt Service
  FCCR               = (Adj. EBITDA − CAPEX) / Total Debt Service

ADJ. EBITDA LABEL MATCHING — check these in order before deriving:
  Accepted row labels (case-insensitive):
    "Adj. EBITDA"  |  "Adjusted EBITDA"  |  "Adj EBITDA"
    "EBITDA (Adjusted)"  |  "EBITDA (as adjusted)"
    "Normalized EBITDA"  |  "Recurring EBITDA"
  Preferred source: the row immediately following a "Total Add-backs" or
    "Non-Recurring Adjustments" subtotal row.
  Fallback derivation (use ONLY when none of the above labels exist):
    adj_ebitda = operating_income + adjustments
    where operating_income = the EBIT / Operating Income / Operating Profit line
    Set confidence = 0.75 for this derived value. Cite the formula.

ADJ. EBITDA CROSS-CHECK (run for every historical year):
  calc = gross_margin − sga + adjustments
  If |stated_ebitda − calc| / calc > 0.05:
    Set confidence = 0.60
    Append to citation: '[DISCREPANCY: stated={{stated}}, calc={{calc}}, delta={{pct}}%]'

════════════════════════════════════════════════════════════
SECTION D — PROJECTION EXTRACTION RULES
════════════════════════════════════════════════════════════
Extract proj_ fields ONLY if the document has an explicit forward-looking table
labelled: Forecast, Projections, Budget, Management Case, Strategic Plan, Outlook.

IMPORTANT DISTINCTION — E/F/B/P SUFFIX COLUMNS:
  For HISTORICAL fields (revenue_fy1/fy2/fy3, gross_margin_fy1/2/3, sga_fy1/2/3,
    interest_expense_fy1/2/3, adjustments_fy1/2/3, adj_ebitda_fy1/2/3):
    DO NOT extract values from columns with E/F/B/P suffix. Those are projection columns.
  For PROJECTION fields (proj_revenue_y1..y5, proj_gross_margin_y1..y5, proj_sga_y1..y5):
    DO extract values from columns with E/F/B/P suffix — these ARE the projection columns.
    proj_revenue_y1 = value in the FIRST projection year column (lowest E/F-suffix year > fy3)
    proj_revenue_y2 = value in the SECOND projection year column, etc.
    Year ordering: y1 = nearest future year ({proj_y1}), y5 = furthest ({proj_y5}).

CONTAMINATION GUARD: If proj_revenue_yN exactly equals revenue_fy3 → set to null.
If no explicit forecast section exists → return null for ALL proj_ fields.
The system will auto-calculate projections from CAGR and historical averages.

════════════════════════════════════════════════════════════
SECTION E — ANTI-HALLUCINATION SELF-CHECK
════════════════════════════════════════════════════════════
Before writing your final JSON, verify each item:
  [ ] Each fy1/fy2/fy3 value came from a column whose header matches an accepted variant
  [ ] No positional assignment used — all assignment by header match
  [ ] No LTM/TTM/Pro-Forma/Quarterly column values used
  [ ] No sub-revenue (segment/division/region) used as revenue
  [ ] No projection-suffix column (E/F/B/P) mapped to historical slot
  [ ] Adj. EBITDA cross-check run; discrepancy noted if >5%
  [ ] All monetary values in $000s
  [ ] All confidence < 0.70 fields set to null
  [ ] Restated column used when dual columns exist for same year
  [ ] Projection contamination guard applied
  [ ] Adj. EBITDA: checked all label variants before using derivation fallback
  [ ] existing_term_loans: returned outstanding balance, NOT the revolving ABL/revolver
  [ ] building_land_collateral: returned GROSS asset value, NOT advance-rate-adjusted borrowing base
  [ ] E/F/B/P suffix columns: NOT mapped to historical slots (fy1/fy2/fy3)
  [ ] E/F/B/P suffix columns: ARE correctly mapped to proj_* slots (y1..y5)

════════════════════════════════════════════════════════════
SECTION F — OUTPUT JSON SCHEMA
════════════════════════════════════════════════════════════
Return ONLY the JSON below. No prose. No markdown. No explanation.
{{
  "company_name":             {{"value": "<string>",  "confidence": 1.0,   "citation": "..." }},
  "fy_year_1":                {{"value": {fy1},       "confidence": 1.0,   "citation": "detected fiscal year" }},
  "fy_year_2":                {{"value": {fy2},       "confidence": 1.0,   "citation": "detected fiscal year" }},
  "fy_year_3":                {{"value": {fy3},       "confidence": 1.0,   "citation": "detected fiscal year" }},
  "revenue_fy1":              {{"value": <num|null>,  "confidence": <0-1>, "citation": "<exact row label | column header matched>" }},
  "revenue_fy2":              {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "revenue_fy3":              {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "gross_margin_fy1":         {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "gross_margin_fy2":         {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "gross_margin_fy3":         {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "cogs_fy1":                 {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "cogs_fy2":                 {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "cogs_fy3":                 {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "sga_fy1":                  {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "sga_fy2":                  {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "sga_fy3":                  {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "interest_expense_fy1":     {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "interest_expense_fy2":     {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "interest_expense_fy3":     {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "adjustments_fy1":          {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "adjustments_fy2":          {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "adjustments_fy3":          {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "adj_ebitda_fy1":           {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "adj_ebitda_fy2":           {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "adj_ebitda_fy3":           {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "net_revenue_collateral":   {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "inventory_collateral":     {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "me_equipment_collateral":  {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "building_land_collateral": {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "existing_term_loans":      {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "proj_revenue_y1":          {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "proj_revenue_y2":          {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "proj_revenue_y3":          {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "proj_revenue_y4":          {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "proj_revenue_y5":          {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "proj_gross_margin_y1":     {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "proj_gross_margin_y2":     {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "proj_gross_margin_y3":     {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "proj_gross_margin_y4":     {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "proj_gross_margin_y5":     {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "proj_sga_y1":              {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "proj_sga_y2":              {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "proj_sga_y3":              {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "proj_sga_y4":              {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "proj_sga_y5":              {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }}
}}

OCR TEXT:
---
{text}
"""


def _call_llm(client: OpenAI, prompt: str, max_retries: int = 3,
              system_prompt: str = None) -> str:
    """Call NVIDIA NIM with retry and exponential backoff."""
    sp = system_prompt if system_prompt is not None else SYSTEM_PROMPT
    last_error = None
    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model=NVIDIA_MODEL,
                messages=[
                    {"role": "system", "content": sp},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.0,
                max_tokens=8192,  # increased from 4096 — 40 fields with citations + reasoning tokens
            )
            choice = response.choices[0]
            finish = choice.finish_reason
            content = choice.message.content or ''
            if finish == 'length':
                logger.warning(f"LLM response TRUNCATED (finish_reason=length, {len(content)} chars) — consider reducing prompt or increasing max_tokens further")
            else:
                logger.info(f"LLM response complete (finish_reason={finish}, {len(content)} chars)")
            return content
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                wait = 2 ** attempt
                logger.warning(f"LLM call attempt {attempt + 1} failed: {e}. Retrying in {wait}s…")
                time.sleep(wait)

    raise RuntimeError(f"LLM call failed after {max_retries} attempts: {last_error}")


def _parse_llm_json(raw: str) -> dict:
    """Robustly parse JSON from LLM response (handles markdown fences, truncation, trailing text)."""
    raw = raw.strip()
    # Strip markdown code fences
    raw = re.sub(r'^```(?:json)?\s*', '', raw, flags=re.MULTILINE)
    raw = re.sub(r'\s*```\s*$', '', raw, flags=re.MULTILINE)
    raw = raw.strip()

    # Try direct parse
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    # Find the outermost JSON object
    start = raw.find('{')
    if start != -1:
        # Walk brackets to find matching close
        depth = 0
        for i, ch in enumerate(raw[start:], start):
            if ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(raw[start:i + 1])
                    except json.JSONDecodeError:
                        break

    # ── Regex fallback: recover complete field entries from truncated JSON ────
    # Handles the case where max_tokens cut the response before the final '}'
    # Matches: "field_name": {"value": V, "confidence": C, "citation": Z}
    _field_re = re.compile(
        r'"(\w+)"\s*:\s*\{\s*'
        r'"value"\s*:\s*(null|-?[\d.]+|"(?:[^"\\]|\\.)*")\s*,\s*'
        r'"confidence"\s*:\s*([\d.]+)'
        r'(?:\s*,\s*"citation"\s*:\s*("(?:[^"\\]|\\.)*"|null))?\s*\}',
        re.DOTALL,
    )
    result = {}
    for m in _field_re.finditer(raw):
        field_name = m.group(1)
        value_raw = m.group(2)
        confidence = float(m.group(3))
        citation_raw = m.group(4) or 'null'

        if value_raw == 'null':
            value = None
        elif value_raw.startswith('"'):
            value = value_raw[1:-1].replace('\\"', '"')
        else:
            try:
                value = float(value_raw)
            except ValueError:
                value = None

        citation = '' if citation_raw == 'null' else citation_raw[1:-1].replace('\\"', '"')
        result[field_name] = {'value': value, 'confidence': confidence, 'citation': citation}

    if result:
        logger.warning(f"Partial JSON recovery via regex fallback — {len(result)} fields extracted from truncated response")
        return result

    logger.error(f"Failed to parse LLM JSON. Response preview: {raw[:300]}")
    return {}


def _merge_extractions(base: dict, overlay: dict) -> dict:
    """
    Merge two extraction dicts. Overlay wins only if confidence is higher.
    """
    merged = dict(base)
    for key, overlay_val in overlay.items():
        if not isinstance(overlay_val, dict):
            continue
        base_val = merged.get(key, {})
        if not isinstance(base_val, dict):
            merged[key] = overlay_val
            continue
        # Keep higher confidence result
        if (overlay_val.get('confidence') or 0) > (base_val.get('confidence') or 0):
            merged[key] = overlay_val
    return merged


def _unpack_extraction(parsed: dict) -> tuple[dict, dict, dict]:
    """
    Unpack a parsed LLM JSON dict into (extracted, confidences, citations).
    Only processes fields present in EXTRACTION_SCHEMA.
    Used by Pass 3 to merge additional results into main extraction dicts.
    """
    _STRING_FIELDS = {'company_name', 'fy_year_1', 'fy_year_2', 'fy_year_3'}
    extracted_out = {}
    confidences_out = {}
    citations_out = {}
    for field_key in EXTRACTION_SCHEMA:
        field_data = parsed.get(field_key)
        if isinstance(field_data, dict):
            raw_val = field_data.get('value')
            if field_key in _STRING_FIELDS:
                extracted_out[field_key] = str(raw_val).strip() if raw_val is not None else None
            else:
                extracted_out[field_key] = _coerce_numeric(raw_val)
            confidences_out[field_key] = float(field_data.get('confidence') or 0.0)
            citations_out[field_key] = str(field_data.get('citation', ''))[:300]
        else:
            extracted_out[field_key] = None
            confidences_out[field_key] = 0.0
            citations_out[field_key] = ''
    return extracted_out, confidences_out, citations_out


def extract_financial_fields(ocr_text: str,
                              session_id: str = None) -> tuple[dict, dict, dict, tuple]:
    """
    Extract financial fields from OCR text using NVIDIA NIM.

    For large documents:
    - Selects the most financially-relevant sections (up to MAX_CONTEXT_CHARS)
    - If text is very large (>2×MAX_CONTEXT_CHARS), does a second pass on
      low-confidence fields using an alternative window

    Returns: (extracted_values, confidence_scores, source_citations, detected_fy_years)
    where detected_fy_years is a (y1, y2, y3) tuple of integers from the document.
    """
    os.makedirs(EXTRACTIONS_FOLDER, exist_ok=True)

    client = OpenAI(base_url=NVIDIA_BASE_URL, api_key=NVIDIA_API_KEY)

    # Detect fiscal years from document (anchored to latest year IN the doc, not system clock)
    fy_years = _detect_fiscal_years(ocr_text)
    y1, y2, y3 = fy_years
    logger.info(f"  LLM extraction: fiscal year anchor → fy1={y1}  fy2={y2}  fy3={y3}")

    # Build year-anchored system prompt with actual detected year integers
    formatted_system_prompt = _build_system_prompt(y1, y2, y3)

    # ── Pass 1: Smart section extraction ─────────────────────────────────────
    logger.info(f"  LLM pass 1: selecting relevant sections from {len(ocr_text):,} chars of OCR text…")
    focused_text = _extract_financial_sections(ocr_text)
    logger.info(f"  LLM pass 1: sending {len(focused_text):,} chars to {NVIDIA_MODEL}…")
    prompt1 = _build_extraction_prompt(focused_text, fy_years)
    raw1 = _call_llm(client, prompt1, system_prompt=formatted_system_prompt)
    parsed1 = _parse_llm_json(raw1)
    found1 = sum(1 for v in parsed1.values() if isinstance(v, dict) and v.get('value') is not None)
    logger.info(f"  LLM pass 1: ✔ response received — {found1}/{len(EXTRACTION_SCHEMA)} fields found")

    # ── Pass 2: Re-extract low-confidence fields from alternate window ────────
    # If original text is large, try a different cut for fields that came back
    # null or with low confidence
    low_conf_fields = [
        k for k in EXTRACTION_SCHEMA
        if k in parsed1 and isinstance(parsed1[k], dict)
        and (parsed1[k].get('value') is None or (parsed1[k].get('confidence') or 0) < 0.65)
    ]
    null_fields = [k for k in EXTRACTION_SCHEMA if k not in parsed1 or not isinstance(parsed1.get(k), dict)]
    needs_retry = low_conf_fields + null_fields

    parsed_final = dict(parsed1)

    if not needs_retry or len(ocr_text) <= MAX_CONTEXT_CHARS:
        logger.info(f"  LLM pass 2: skipped (all fields found or document small enough)")

    if needs_retry and len(ocr_text) > MAX_CONTEXT_CHARS:
        logger.info(f"  LLM pass 2: {len(needs_retry)} fields need retry — {needs_retry}")

        # Use the second half of the document for the alternate window
        mid = len(ocr_text) // 2
        alt_text = _extract_financial_sections(
            ocr_text[mid:],
            target_chars=MAX_CONTEXT_CHARS // 2
        )
        # Build a focused prompt for only the missing fields
        focused_schema = {k: EXTRACTION_SCHEMA[k] for k in needs_retry if k in EXTRACTION_SCHEMA}
        alt_fields_desc = '\n'.join(f'  "{k}": {v}' for k, v in focused_schema.items())
        prompt2 = f"""Extract ONLY the following fields from this document text.
Null if not found. Confidence 0–1. Cite exact text.
FISCAL YEAR MAPPING: fy1={y1} | fy2={y2} | fy3={y3}
REVENUE = TOTAL consolidated top-line only — not segments.

FIELDS:
{alt_fields_desc}

OUTPUT FORMAT:
{{"field_name": {{"value": ..., "confidence": ..., "citation": "..."}}}}

DOCUMENT TEXT:
{alt_text}
"""
        logger.info(f"  LLM pass 2: sending focused prompt to {NVIDIA_MODEL}…")
        raw2 = _call_llm(client, prompt2, system_prompt=formatted_system_prompt)
        parsed2 = _parse_llm_json(raw2)
        found2 = sum(1 for v in parsed2.values() if isinstance(v, dict) and v.get('value') is not None)
        logger.info(f"  LLM pass 2: ✔ {found2} additional fields recovered after merge")
        parsed_final = _merge_extractions(parsed_final, parsed2)

    # ── Unpack into three separate dicts ─────────────────────────────────────
    # These fields are strings — do NOT coerce to numeric
    _STRING_FIELDS = {'company_name', 'fy_year_1', 'fy_year_2', 'fy_year_3'}

    extracted = {}
    confidences = {}
    citations = {}

    for field_key in EXTRACTION_SCHEMA:
        field_data = parsed_final.get(field_key)
        if isinstance(field_data, dict):
            raw_val = field_data.get('value')
            if field_key in _STRING_FIELDS:
                extracted[field_key] = str(raw_val).strip() if raw_val is not None else None
            else:
                extracted[field_key] = _coerce_numeric(raw_val)
            confidences[field_key] = float(field_data.get('confidence') or 0.0)
            citations[field_key] = str(field_data.get('citation', ''))[:300]
        else:
            extracted[field_key] = None
            confidences[field_key] = 0.0
            citations[field_key] = ''

    # ── PASS 3 ─────────────────────────────────────────────────────────────
    # Trigger: revenue_fy3 is still null after Pass 1+2 merge.
    # Strategy: re-run LLM on the top-3 NUMERICALLY DENSE windows from the
    # FULL text (no keyword filter). Guarantees the actual P&L table is found
    # even if keyword scoring missed it or it sat in the middle of the document.
    # ─────────────────────────────────────────────────────────────────────────
    _pass3_triggered = False
    if extracted.get('revenue_fy3') is None:
        _pass3_triggered = True
        _NUM = re.compile(r'\b\d[\d,]*\.?\d*\b')
        _WINDOW = 1500  # larger window — capture full P&L tables
        _all_windows = []
        for _i in range(0, len(ocr_text), _WINDOW):
            _chunk = ocr_text[_i:_i + _WINDOW]
            _num_lines = sum(
                1 for _line in _chunk.split('\n')
                if len(_NUM.findall(_line)) >= 3
            )
            _all_windows.append((_num_lines, _i, _chunk))
        # Top-3 most numerically dense windows
        _all_windows.sort(key=lambda x: x[0], reverse=True)
        _p3_text = '\n'.join(w[2] for w in _all_windows[:3])
        logger.info('[PASS 3] revenue_fy3 null — retrying on top-3 numeric windows')
        try:
            _p3_prompt = _build_extraction_prompt(_p3_text, fy_years)
            _p3_resp = _call_llm(client, _p3_prompt, system_prompt=formatted_system_prompt)
            _p3_json = _parse_llm_json(_p3_resp)
            _p3_ext, _p3_conf, _p3_cite = _unpack_extraction(_p3_json)
            # Merge: Pass 3 wins only where current extracted is null
            for _k, _v in _p3_ext.items():
                if extracted.get(_k) is None and _v is not None:
                    extracted[_k] = _v
                    confidences[_k] = _p3_conf.get(_k, 0.70)
                    citations[_k] = _p3_cite.get(_k, '')
            logger.info(f'[PASS 3] revenue_fy3 after merge: {extracted.get("revenue_fy3")}')
        except Exception as _e:
            logger.warning(f'[PASS 3] failed: {_e}')
    # ── END PASS 3 ───────────────────────────────────────────────────────────

    # ── ENH-1: COGS fallback — derive gross_margin_fyN = revenue_fyN - cogs_fyN when GM is null ──
    for _n in (1, 2, 3):
        if extracted.get(f'gross_margin_fy{_n}') is None:
            rev = extracted.get(f'revenue_fy{_n}')
            cogs = extracted.get(f'cogs_fy{_n}')
            if rev is not None and cogs is not None:
                extracted[f'gross_margin_fy{_n}'] = round(float(rev) - float(cogs), 2)
                confidences[f'gross_margin_fy{_n}'] = min(
                    confidences.get(f'revenue_fy{_n}', 0),
                    confidences.get(f'cogs_fy{_n}', 0)
                )
                citations[f'gross_margin_fy{_n}'] = f"Calculated: revenue({rev}) - COGS({cogs})"
                logger.info(f"  GM fy{_n} derived from COGS: {extracted[f'gross_margin_fy{_n}']}")

    # ── EBITDA cross-check at extraction time ────────────────────────────────
    # If stated Adj.EBITDA differs >5% from (GM - SGA + adj), downgrade confidence
    # to 0.60 and append a discrepancy note to citation.
    for _n in (1, 2, 3):
        doc_ebitda = extracted.get(f'adj_ebitda_fy{_n}')
        gm  = extracted.get(f'gross_margin_fy{_n}')
        sg  = extracted.get(f'sga_fy{_n}')
        adj = extracted.get(f'adjustments_fy{_n}') or 0.0
        if doc_ebitda is not None and gm is not None and sg is not None:
            calc = gm - sg + adj
            if abs(calc) > 0:
                pct_diff = abs(calc - doc_ebitda) / abs(calc)
                if pct_diff > 0.05:
                    confidences[f'adj_ebitda_fy{_n}'] = min(
                        confidences.get(f'adj_ebitda_fy{_n}', 0.60), 0.60
                    )
                    existing = citations.get(f'adj_ebitda_fy{_n}', '')
                    citations[f'adj_ebitda_fy{_n}'] = (
                        existing + f" [DISCREPANCY: stated={doc_ebitda:,.0f}, "
                        f"calculated={calc:,.0f}, delta={pct_diff:.0%}]"
                    ).strip()
                    logger.warning(
                        f"  EBITDA fy{_n} discrepancy: stated={doc_ebitda:,.0f}, "
                        f"calc={calc:,.0f}, diff={pct_diff:.0%}"
                    )

    # ── Data presence flags (used by UI to dim/badge empty historical columns) ─
    for _n in (1, 2, 3):
        extracted[f'_data_present_fy{_n}'] = any(
            extracted.get(f) not in (None, 0, 0.0)
            for f in [f'revenue_fy{_n}', f'gross_margin_fy{_n}', f'sga_fy{_n}']
        )

    # ── Persist ───────────────────────────────────────────────────────────────
    if session_id:
        debug_path = os.path.join(EXTRACTIONS_FOLDER, f"{session_id}_extraction.json")
        debug_payload = {
            'session_id': session_id,
            'detected_fy_years': list(fy_years),
            'ocr_text_total_chars': len(ocr_text),
            'pass1_window_chars': len(focused_text),
            'pass2_triggered': bool(needs_retry and len(ocr_text) > MAX_CONTEXT_CHARS),
            'pass3_triggered': _pass3_triggered,
            'fields_null_after_merge': [
                k for k in ['revenue_fy1', 'revenue_fy2', 'revenue_fy3',
                             'gross_margin_fy3', 'adj_ebitda_fy3', 'sga_fy3']
                if extracted.get(k) is None
            ],
            'revenue_fy1': extracted.get('revenue_fy1'),
            'revenue_fy2': extracted.get('revenue_fy2'),
            'revenue_fy3': extracted.get('revenue_fy3'),
            'extracted': extracted,
            'confidences': confidences,
            'citations': citations,
        }
        with open(debug_path, 'w', encoding='utf-8') as f:
            json.dump(debug_payload, f, indent=2)
        logger.info(f'[DEBUG] Saved extraction log: {debug_path}')
        logger.info(f'[DEBUG] FY years: {fy_years}')
        logger.info(f'[DEBUG] Null fields: {debug_payload["fields_null_after_merge"]}')

    return extracted, confidences, citations, fy_years


def _coerce_numeric(val):
    """
    Try to convert LLM output to a float.
    Handles: "5,200", "$5.2M", "5200000", None, already-numeric.
    """
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return val
    s = str(val).strip()
    if not s or s.lower() in ('null', 'none', 'n/a', '-'):
        return None
    # Strip currency symbols and whitespace
    s = re.sub(r'[$,\s]', '', s)
    # Handle M/K suffixes
    m = re.match(r'^([\d.]+)\s*([MmKkBb]?)$', s)
    if m:
        num = float(m.group(1))
        suffix = m.group(2).upper()
        if suffix == 'M':
            num *= 1_000   # M → thousands
        elif suffix == 'K':
            num /= 1       # K is already ~thousands (pass through)
        elif suffix == 'B':
            num *= 1_000_000  # B → thousands
        return num
    try:
        return float(s)
    except ValueError:
        return None


def fill_missing_projections(extracted: dict) -> tuple[dict, str]:
    """
    Build 5-year projection values (revenue, gross_margin, sga, interest, adjustments, term_loan).

    Priority:
    1. Use LLM-extracted proj_* values if present (document had explicit forecasts)
    2. Auto-calculate from historical averages / growth rates if not

    Returns: (projections_dict, source) where source is 'ocr' or 'calculated'
    """
    def _f(key):
        v = extracted.get(key)
        try:
            return float(v) if v is not None else None
        except (ValueError, TypeError):
            return None

    # Historical values
    rev1, rev2, rev3 = _f('revenue_fy1'), _f('revenue_fy2'), _f('revenue_fy3')
    gm1,  gm2,  gm3  = _f('gross_margin_fy1'), _f('gross_margin_fy2'), _f('gross_margin_fy3')
    sg1,  sg2,  sg3   = _f('sga_fy1'), _f('sga_fy2'), _f('sga_fy3')
    int3 = _f('interest_expense_fy3') or _f('interest_expense_fy2') or _f('interest_expense_fy1')

    # Check if LLM found any OCR projections
    ocr_rev  = [extracted.get(f'proj_revenue_y{i}') for i in range(1, 6)]
    ocr_gm   = [extracted.get(f'proj_gross_margin_y{i}') for i in range(1, 6)]
    ocr_sga  = [extracted.get(f'proj_sga_y{i}') for i in range(1, 6)]

    # De-contamination guard: LLM sometimes assigns historical values to proj_* fields
    # (e.g. "Net Revenue $266 $283 $297 $310" → proj_y1=283 which equals revenue_fy1).
    # Null out any proj value that duplicates a known historical value.
    hist_rev_set = {v for v in [rev1, rev2, rev3] if v is not None}
    hist_gm_set  = {v for v in [gm1, gm2, gm3]   if v is not None}
    hist_sg_set  = {v for v in [sg1, sg2, sg3]    if v is not None}

    def _dedup(ocr_list, hist_set):
        return [None if (_coerce_numeric(v) in hist_set) else v for v in ocr_list]

    ocr_rev = _dedup(ocr_rev, hist_rev_set)
    ocr_gm  = _dedup(ocr_gm,  hist_gm_set)
    ocr_sga = _dedup(ocr_sga, hist_sg_set)

    has_ocr_proj = any(v is not None for v in ocr_rev + ocr_gm + ocr_sga)

    proj = {}
    source = 'ocr' if has_ocr_proj else 'calculated'

    # ── Revenue ──────────────────────────────────────────────────────────────
    if has_ocr_proj and any(v is not None for v in ocr_rev):
        for i in range(1, 6):
            proj[f'revenue_y{i}'] = _coerce_numeric(ocr_rev[i - 1])
        logger.info("  Projections: revenue from OCR document")
    else:
        # ── ROBUST CAGR BASE SELECTION ─────────────────────────────────────
        # Old code: used rev3 directly → silent 0 when null.
        # New code: fallback chain rev3 → rev2 → rev1, use available years only.
        # ─────────────────────────────────────────────────────────────────────
        base_revenue = None
        base_years_back = 0  # how many years before fy3 the base is
        if rev3 and rev3 > 0:
            base_revenue = rev3
            base_years_back = 0
        elif rev2 and rev2 > 0:
            base_revenue = rev2
            base_years_back = 1
            logger.info('[PROJ] revenue_fy3 null — using revenue_fy2 as CAGR base')
        elif rev1 and rev1 > 0:
            base_revenue = rev1
            base_years_back = 2
            logger.info('[PROJ] revenue_fy3+fy2 null — using revenue_fy1 as CAGR base')
        else:
            # All historical revenues are null — cannot calculate projections
            logger.warning('[PROJ] WARNING: all historical revenues null — returning empty projections')
            return {}, 'manual_required'

        # CAGR from all available revenues (oldest to newest)
        avail_revs = [r for r in [rev1, rev2, rev3] if r and r > 0]
        if len(avail_revs) >= 2:
            oldest = avail_revs[0]
            newest = avail_revs[-1]
            n_years = len(avail_revs) - 1
            raw_cagr = (newest / oldest) ** (1 / n_years) - 1
        else:
            raw_cagr = 0.05  # default 5% if only one historical year available

        # Cap CAGR at -20% to +50% (existing rule preserved)
        cagr = max(-0.20, min(0.50, raw_cagr))

        # Project from base_revenue, adjusting for years_back offset.
        # y1 is always fy3+1. If base is fy2, we need 1 extra compound step.
        for yr_idx in range(1, 6):
            steps = yr_idx + base_years_back
            proj[f'revenue_y{yr_idx}'] = round(base_revenue * ((1 + cagr) ** steps), 2)
        logger.info(f"  Projections: revenue auto-calculated at {cagr:.1%} CAGR (base_years_back={base_years_back})")

    # ── Gross Margin ─────────────────────────────────────────────────────────
    if has_ocr_proj and any(v is not None for v in ocr_gm):
        for i in range(1, 6):
            proj[f'gross_margin_y{i}'] = _coerce_numeric(ocr_gm[i - 1])
    else:
        gm_pcts = []
        for rev, gm in [(rev1, gm1), (rev2, gm2), (rev3, gm3)]:
            if rev and gm and rev > 0:
                gm_pcts.append(gm / rev)
        avg_gm_pct = sum(gm_pcts) / len(gm_pcts) if gm_pcts else 0.30
        for i in range(1, 6):
            rev_yi = proj.get(f'revenue_y{i}')
            proj[f'gross_margin_y{i}'] = round(float(rev_yi) * avg_gm_pct, 2) if rev_yi else None
        logger.info(f"  Projections: gross margin auto-calculated at {avg_gm_pct:.1%} avg GM%")

    # ── SG&A ─────────────────────────────────────────────────────────────────
    if has_ocr_proj and any(v is not None for v in ocr_sga):
        for i in range(1, 6):
            proj[f'sga_y{i}'] = _coerce_numeric(ocr_sga[i - 1])
    else:
        sga_pcts = []
        for rev, sg in [(rev1, sg1), (rev2, sg2), (rev3, sg3)]:
            if rev and sg and rev > 0:
                sga_pcts.append(sg / rev)
        avg_sga_pct = sum(sga_pcts) / len(sga_pcts) if sga_pcts else 0.20
        for i in range(1, 6):
            rev_yi = proj.get(f'revenue_y{i}')
            proj[f'sga_y{i}'] = round(float(rev_yi) * avg_sga_pct, 2) if rev_yi else None
        logger.info(f"  Projections: SG&A auto-calculated at {avg_sga_pct:.1%} avg SGA%")

    # ── Interest expense: flat from most recent historical ───────────────────
    for i in range(1, 6):
        proj[f'interest_expense_y{i}'] = round(int3, 2) if int3 else None

    # ── Adjustments: 0 (non-recurring by definition) ─────────────────────────
    for i in range(1, 6):
        proj[f'adjustments_y{i}'] = 0

    # ── Term loan: leave blank for manual input ───────────────────────────────
    for i in range(1, 6):
        proj[f'term_loan_y{i}'] = None

    return proj, source


def verify_llm_connection() -> dict:
    """
    Verify NVIDIA NIM API key and model availability.
    Sends a minimal prompt — does NOT process real data.
    Returns: { 'ok': bool, 'message': str, 'details': dict }
    """
    if not NVIDIA_API_KEY:
        return {
            'ok': False,
            'message': 'NVIDIA_API_KEY not set in .env',
            'details': {},
        }

    try:
        client = OpenAI(base_url=NVIDIA_BASE_URL, api_key=NVIDIA_API_KEY)

        start = time.time()
        response = client.chat.completions.create(
            model=NVIDIA_MODEL,
            messages=[
                {"role": "user", "content": 'Reply with the single word: VERIFIED'},
            ],
            temperature=0.0,
            max_tokens=20,
        )
        elapsed = round(time.time() - start, 2)
        content = response.choices[0].message.content
        reply = content.strip() if content else '(empty — model connected)'

        return {
            'ok': True,
            'message': f'NVIDIA NIM connected. Model responded in {elapsed}s.',
            'details': {
                'model': NVIDIA_MODEL,
                'base_url': NVIDIA_BASE_URL,
                'response': reply,
                'latency_s': elapsed,
                'max_context_chars': MAX_CONTEXT_CHARS,
            },
        }

    except Exception as e:
        return {
            'ok': False,
            'message': f'LLM verification failed — {type(e).__name__}: {str(e)}',
            'details': {
                'model': NVIDIA_MODEL,
                'base_url': NVIDIA_BASE_URL,
            },
        }


def generate_risk_analysis(all_inputs: dict, results: dict) -> list:
    """
    Generate 6 risk factors for a PE deal from extracted financial data.
    Each item: {risk, category, source ('memo'|'general'), confidence, citation}
    Returns [] on failure — caller must handle gracefully.
    """
    client = OpenAI(base_url=NVIDIA_BASE_URL, api_key=NVIDIA_API_KEY)
    company = all_inputs.get('company_name') or 'the company'

    def _fmt(key):
        v = all_inputs.get(key)
        try:
            return f"{float(v):,.0f}" if v is not None else None
        except (TypeError, ValueError):
            return None

    # Build concise financial summary (grounding for 'memo' risks)
    lines = [f"Company: {company}"]
    for n in (1, 2, 3):
        rev = _fmt(f'revenue_fy{n}')
        gm  = _fmt(f'gross_margin_fy{n}')
        sg  = _fmt(f'sga_fy{n}')
        if rev:
            lines.append(f"FY{n}: Revenue=${rev}k" +
                         (f", GM=${gm}k" if gm else '') +
                         (f", SGA=${sg}k" if sg else ''))
    for label, key in [('EBITDA(FY3)', 'adj_ebitda_fy3'), ('Acq. Multiple', 'acquisition_multiple'),
                       ('% Acquired', 'pct_acquired'), ('Exit Multiple', 'exit_multiple')]:
        v = all_inputs.get(key)
        if v is not None:
            lines.append(f"{label}: {v}")
    # Use actual results keys from calculator.py
    if results.get('C29') is not None:
        lines.append(f"MOIC: {results['C29']}x")
    if results.get('C57_fccr') is not None:
        lines.append(f"FCCR: {results['C57_fccr']}")
    dscr_d = results.get('dscr')
    if isinstance(dscr_d, dict) and dscr_d.get('Y1') is not None:
        lines.append(f"DSCR(Y1): {dscr_d['Y1']}x")
    ebitda_d = results.get('adj_ebitda')
    if isinstance(ebitda_d, dict) and ebitda_d.get('Y1') is not None:
        lines.append(f"Adj.EBITDA(Y1): ${ebitda_d['Y1']}k")
    if results.get('C42') is not None:
        lines.append(f"Total Debt Service: ${results['C42']}k")
    summary = "\n".join(lines)

    system_msg = (
        "You are a private equity deal risk analyst. Given a company financial summary, "
        "identify exactly 6 key deal risks.\n"
        "Rules:\n"
        "1. source='memo' for risks directly evidenced by the financial data provided — cite the specific metric\n"
        "2. source='general' for industry/market/structural risks not specific to these numbers\n"
        "3. confidence: 0.80-0.95 for memo risks, 0.60-0.80 for general risks\n"
        "4. category: one of Financial, Operational, Market, Legal, Integration, Leverage\n"
        "5. Return ONLY valid JSON — no markdown, no commentary\n"
        'Schema: {"risks": [{"risk": str, "category": str, "source": "memo"|"general", '
        '"confidence": float, "citation": str}]}'
    )
    prompt = f"Financial Summary:\n{summary}\n\nIdentify the 6 key PE deal risks:"

    for attempt in range(3):
        try:
            t0 = time.time()
            resp = client.chat.completions.create(
                model=NVIDIA_MODEL,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user",   "content": prompt},
                ],
                temperature=0.0,
                max_tokens=2048,
            )
            raw = resp.choices[0].message.content or ''
            logger.info(f"Risk LLM response ({len(raw)} chars): {raw[:300]}")

            # Parse risk JSON directly — handles {"risks":[...]}, plain [...], and markdown fences
            raw_clean = re.sub(r'^```(?:json)?\s*|\s*```\s*$', '', raw.strip(), flags=re.MULTILINE).strip()
            risks = []
            try:
                parsed = json.loads(raw_clean)
                if isinstance(parsed, list):
                    risks = parsed
                elif isinstance(parsed, dict):
                    risks = parsed.get('risks', [])
            except (json.JSONDecodeError, ValueError):
                # Fallback: find first JSON object or array in response
                m_obj = re.search(r'\{[\s\S]*\}', raw_clean)
                m_arr = re.search(r'\[[\s\S]*\]', raw_clean)
                if m_obj:
                    try:
                        d = json.loads(m_obj.group())
                        risks = d.get('risks', []) if isinstance(d, dict) else []
                    except Exception:
                        pass
                if not risks and m_arr:
                    try:
                        risks = json.loads(m_arr.group())
                    except Exception:
                        pass

            logger.info(f"Risk analysis complete — {len(risks)} risks ({time.time()-t0:.1f}s)")
            if not risks:
                logger.warning(f"Risk analysis returned 0 risks — raw response: {raw[:400]}")
            return risks
        except Exception as exc:
            logger.warning(f"Risk analysis attempt {attempt+1} failed: {exc}")
            if attempt < 2:
                time.sleep(2 ** attempt)
    logger.error("Risk analysis failed after 3 attempts — returning empty list")
    return []
