
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
    "revenue_fy1": "TOTAL consolidated top-line revenue for fiscal year fy1 in $000s. Use the TOPMOST revenue row in the P&L (appears BEFORE COGS/Gross Margin). Accept labels: Revenue, Net Revenue, Total Revenue, Sales, Net Sales, Total Sales, Revenues, Operating Revenue. NEVER use segment/region/product/subscription sub-rows. If the row is absent → null. If present → extract the value from the fy1 column (confidence ≥ 0.60).",
    "revenue_fy2": "TOTAL consolidated top-line revenue for fiscal year fy2 in $000s. Same rules as revenue_fy1. Topmost revenue row, fy2 column only. Absent → null.",
    "revenue_fy3": "TOTAL consolidated top-line revenue for fiscal year fy3 (most recent) in $000s. Same rules as revenue_fy1. Topmost revenue row, fy3 column only. Absent → null.",
    "gross_margin_fy1": "Gross profit/gross margin dollar amount for earliest fiscal year (in $000s). = Revenue minus Cost of Goods Sold.",
    "gross_margin_fy2": "Gross margin for middle fiscal year (in $000s).",
    "gross_margin_fy3": "Gross margin for most recent fiscal year (in $000s).",
    "cogs_fy1": "Cost of Revenue / COGS for earliest fiscal year in $000s. ALWAYS extract if row present — used as fallback to derive gross margin when GM row is missing. Accept labels: 'Cost of Goods Sold', 'COGS', 'Cost of Sales', 'Cost of Revenue', 'Direct Costs', 'Cost of Products'.",
    "cogs_fy2": "Cost of Revenue / COGS for middle fiscal year in $000s. ALWAYS extract if row present. Same label variants as cogs_fy1.",
    "cogs_fy3": "Cost of Revenue / COGS for most recent fiscal year in $000s. ALWAYS extract if row present. Same label variants as cogs_fy1.",
    "sga_fy1": "Selling, General & Administrative expenses for earliest fiscal year (in $000s).",
    "sga_fy2": "SG&A for middle fiscal year (in $000s).",
    "sga_fy3": "SG&A for most recent fiscal year (in $000s).",
    "interest_expense_fy1": "Interest expense/(income) for earliest fiscal year (in $000s).",
    "interest_expense_fy2": "Interest expense for middle fiscal year (in $000s).",
    "interest_expense_fy3": "Interest expense for most recent fiscal year (in $000s).",
    "adjustments_fy1": "One-time/non-recurring items for earliest fiscal year (in $000s).",
    "adjustments_fy2": "One-time adjustments for middle fiscal year (in $000s).",
    "adjustments_fy3": "One-time adjustments for most recent fiscal year (in $000s).",
    "adj_ebitda_fy1": "Adjusted EBITDA for earliest fiscal year in $000s. Accept ANY label (case-insensitive): 'Adj. EBITDA', 'Adjusted EBITDA', 'Adj EBITDA', 'EBITDA (Adjusted)', 'EBITDA (as adjusted)', 'Normalized EBITDA', 'Recurring EBITDA', 'EBITDA'. Prefer the row immediately following a 'Total Add-backs' or 'Non-Recurring Adjustments' subtotal. Fallback: derive as operating_income + adjustments (confidence 0.75). CRITICAL: Extract ONLY from the historical fy1 column — NEVER from E/F/P/B projection columns.",
    "adj_ebitda_fy2": "Adjusted EBITDA for middle fiscal year in $000s. Same label variants and fallback as adj_ebitda_fy1. CRITICAL: Extract ONLY from the historical fy2 column — NEVER from E/F/P/B projection columns.",
    "adj_ebitda_fy3": "Adjusted EBITDA for most recent fiscal year in $000s. Same label variants and fallback as adj_ebitda_fy1. CRITICAL: Extract ONLY from the historical fy3 column — NEVER from E/F/P/B projection columns even if they appear in the same row.",
    "net_revenue_collateral": (
        "Accounts Receivable (AR) or Trade Receivables from the BALANCE SHEET — most recent "
        "year (in $000s). MUST come from a row labeled 'Accounts Receivable', 'Trade Receivables', "
        "'AR', 'Net Receivables', 'Receivables, net', 'Net AR', 'Trade AR', 'Receivables' "
        "on the balance sheet (CURRENT ASSETS section). "
        "Do NOT use 'Net Revenue' or 'Revenue' from the P&L — completely different. "
        "If no balance sheet exists in the document, return null."
    ),
    "inventory_collateral": (
        "Physical inventory book value from BALANCE SHEET for collateral (in $000s). "
        "Labels: 'Inventory', 'Inventories', 'Net inventory', 'Inventories, net', "
        "'Finished goods', 'Raw materials and WIP' (use TOTAL inventory line, not sub-components). "
        "Use the BALANCE SHEET value for most recent year. "
        "Do NOT use borrowing base schedule gross value."
    ),
    "me_equipment_collateral": (
        "Machinery and equipment value from FIXED ASSET SCHEDULE for collateral (in $000s). "
        "Accept row labels: 'Machinery & Equipment', 'M&E', 'Equipment', 'Machinery', "
        "'Warehouse Equipment', 'Manufacturing Equipment', 'Production Equipment', "
        "'Plant & Equipment', 'Plant, Property & Equipment', 'Fixed Assets', "
        "'FF&E', 'Furniture Fixtures & Equipment', 'Furniture and Equipment'. "
        "Extract most recent historical year column. Increasing values = Gross Cost — correct."
    ),
    "building_land_collateral": (
        "GROSS asset value of real estate / building / land from FIXED ASSET SCHEDULE (in $000s). "
        "Accept row labels: 'Building', 'Buildings', 'Land', 'Real Estate', 'Property', "
        "'Land & Buildings', 'Buildings & Improvements', 'Building & Land', "
        "'Leasehold Improvements', 'Land & Improvements', 'Building & Improvements'. "
        "Extract most recent historical year column. Constant values = correct Gross Cost. "
        "If a table shows 'Asset Value | Advance Rate | Borrowing Base', return ONLY Asset Value. "
        "If pure leaseholder or gross value is $0, return 0."
    ),
    "existing_term_loans": (
        "Outstanding principal balance of any existing term loan or cashflow loan (NOT revolving "
        "ABL/revolver) in $000s. Look in: debt/capital structure tables, 'Sources & Uses', "
        "'Financing', 'Capitalization', 'Long-term Debt' sections. "
        "Accept labels: 'Term Loan', 'Senior Secured Term Loan', 'TL-A', 'TL-B', "
        "'Bank Debt', 'Senior Debt', 'Term Debt', 'Funded Debt', 'Term Loans/Cashflow loans', "
        "'Existing Term Debt', 'Senior Term Loan', 'First Lien Term Loan', 'Term Facility'. "
        "Return outstanding balance (use original principal if outstanding not stated). "
        "EXCLUDE: revolving credit, line of credit, ABL revolver, seller notes, operating leases."
    ),
    "acquisition_multiple": "EV/EBITDA acquisition multiple for this transaction (a plain number, e.g. 7.5 means 7.5×). Look in: 'Transaction Overview', 'Investment Highlights', 'Proposed Transaction', 'Deal Terms', 'Valuation' sections. Phrases: '×EBITDA', 'multiple of EBITDA', 'acquisition multiple', 'purchase price multiple', 'X.Xx Adj. EBITDA'. Return the number only (not the EBITDA dollar value). Null if not stated.",
    # ── Projection / Forecast data (extract ONLY if document contains explicit forward-looking statements)
    "proj_revenue_y1": "Projected TOTAL top-line revenue Year 1 (first future year after fy3) in $000s. Same row-label rules as historical revenue. Extract ONLY from an explicit forecast/projection table. Null if no such table exists.",
    "proj_revenue_y2": "Projected TOTAL top-line revenue Year 2 in $000s. Same row-label rules. Null if no projection table in document.",
    "proj_revenue_y3": "Projected TOTAL top-line revenue Year 3 in $000s. Same row-label rules. Null if no projection table in document.",
    "proj_revenue_y4": "Projected TOTAL top-line revenue Year 4 in $000s. Same row-label rules. Null if no projection table in document.",
    "proj_revenue_y5": "Projected TOTAL top-line revenue Year 5 in $000s. Same row-label rules. Null if no projection table in document.",
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
    "effective_tax_rate": "Effective income tax rate as a decimal (e.g. 0.25 for 25%). LOOK FOR: 'effective tax rate', 'Tax (X%)' line in P&L, or derive as Tax Expense / EBT. Convert % to decimal: '25%' → 0.25. Use COMBINED effective rate, not statutory 21% federal rate alone. Acceptable range: 0.05 to 0.45. Return null if outside range or not found. Do NOT default to any value.",
    "proj_capex_y1": "Projected capital expenditures Year 1 in $000s. Search: projections table CapEx/Capital Expenditures/PP&E Additions row, LBO model, cash flow investing section. Return POSITIVE numbers. Null if not found.",
    "proj_capex_y2": "Projected capital expenditures Year 2 in $000s. Same rules as proj_capex_y1.",
    "proj_capex_y3": "Projected capital expenditures Year 3 in $000s. Same rules as proj_capex_y1.",
    "proj_capex_y4": "Projected capital expenditures Year 4 in $000s. Same rules as proj_capex_y1.",
    "proj_capex_y5": "Projected capital expenditures Year 5 in $000s. Same rules as proj_capex_y1.",
    "reported_ebitda_fy1": (
        "Reported EBITDA BEFORE adjustments/add-backs for earliest fiscal year in $000s. "
        "Labels: 'EBITDA', 'Operating EBITDA', 'Unadjusted EBITDA', 'EBITDA (Reported)', "
        "'EBITDA before non-recurring'. "
        "This row appears BEFORE the add-backs/adjustments section in the P&L. "
        "This value is SMALLER than Adjusted EBITDA. "
        "Return null if not explicitly present — do NOT confuse with Adjusted EBITDA."
    ),
    "reported_ebitda_fy2": (
        "Reported EBITDA (before adjustments) for middle fiscal year in $000s. "
        "Same rules as reported_ebitda_fy1."
    ),
    "reported_ebitda_fy3": (
        "Reported EBITDA (before adjustments) for most recent fiscal year in $000s. "
        "Same rules as reported_ebitda_fy1."
    ),
    "line_of_credit": (
        "Outstanding balance on revolving credit facility / line of credit / ABL "
        "at most recent balance sheet date in $000s. "
        "Labels: 'Line of Credit', 'Revolver', 'Revolving Credit Facility', 'ABL', 'LOC'. "
        "Return null if not found."
    ),
    "current_lt_debt": (
        "Current portion of long-term debt at most recent balance sheet date in $000s. "
        "Labels: 'Current Portion of LTD', 'Current Maturities of Long-Term Debt', "
        "'Current Portion Long-Term Debt', 'Current Maturities LT Debt'. "
        "Return null if not found."
    ),
}

SYSTEM_PROMPT = """You are a financial data extraction specialist for Atar Capital private equity deal analysis.
Your only output is a single strict JSON object — no prose, no markdown, no explanation.

RULES (all 10 are non-negotiable):

RULE 1 — ACCURACY OVER SILENCE:
  Return your best-found value with accurate confidence score rather than null.
  Return null ONLY if:
    (a) the field is genuinely absent from the document, OR
    (b) you cannot determine which table row or column the value belongs to.
  If you FIND a value in the correct row/column but are uncertain of units or scale,
  return it with confidence 0.60–0.69 rather than null — the system will flag it for review.
  Never fabricate or interpolate — only return values you can point to in the text.

RULE 2 — MONETARY UNITS:
  All monetary values must be in $000s (thousands of USD).
  Convert: $5.2M → 5200 | $4,800,000 → 4800 | $500K → 500 | $1.2B → 1200000
  Read the unit qualifier (M, K, B, thousands, millions) from the table caption or column note.
  UNITS DETECTION: Scan the table title/caption for qualifier before extracting.
  Adjust: millions → ×1000, billions → ×1,000,000, thousands/$000s → use as-is.
  Sanity check: revenue for a mid-market company is typically 1,000–500,000 in $000s.

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
  0.60–0.69 → value found but uncertain about units, scale, or column match — return with this confidence
  < 0.60 → return null — not enough evidence

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

RULE 1 — ACCURACY OVER SILENCE:
  Return your best-found value with accurate confidence score rather than null.
  Return null ONLY if:
    (a) the field is genuinely absent from the document, OR
    (b) you cannot determine which table row or column the value belongs to.
  If you FIND a value in the correct row/column but are uncertain of units or scale,
  return it with confidence 0.60–0.69 rather than null — the system will flag it for review.
  Never fabricate or interpolate — only return values you can point to in the text.

RULE 2 — MONETARY UNITS:
  All monetary values must be in $000s (thousands of USD).
  Convert: $5.2M → 5200 | $4,800,000 → 4800 | $500K → 500 | $1.2B → 1200000
  Read the unit qualifier (M, K, B, thousands, millions) from the table caption or column note.
  UNITS DETECTION: Scan the table title/caption for qualifier before extracting.
  Adjust: millions → ×1000, billions → ×1,000,000, thousands/$000s → use as-is.
  Sanity check: revenue for a mid-market company is typically 1,000–500,000 in $000s.

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
  0.60–0.69 → value found but uncertain about units, scale, or column match — return with this confidence
  < 0.60 → return null — not enough evidence

RULE 6 — FORMULA DERIVATION (apply when direct extraction fails):
  Gross Margin ($)  = Revenue − COGS            [if COGS row present]
  Operating Income  = Gross Margin − SG&A
  Adj. EBITDA       = Operating Income + Adjustments + Depreciation + Amortisation
  CFADS             = Adj. EBITDA − CAPEX − Working Capital Change − Taxes
  FCCR              = (Adj. EBITDA − CAPEX) / Total Debt Service
  Mark confidence 0.75 for any derived value. Cite the formula used in citation field.

RULE 7 — YEAR ANCHOR (FULL-FORM, SHORT-FORM, AND FLOAT-FORMAT LABELS):
  Detected fiscal years for this document: fy1={y1}, fy2={y2}, fy3={y3}.
  You MUST match values to columns using ALL of these header variants:
    fy1: {y1}, FY{y1}, FY {y1}, FY-{y1}, FY{yy1}, FY'{yy1}, '{yy1}, {y1}A, {y1} Actual, {y1} Audited, {y1}.0, {y1}.00
    fy2: {y2}, FY{y2}, FY {y2}, FY-{y2}, FY{yy2}, FY'{yy2}, '{yy2}, {y2}A, {y2} Actual, {y2} Audited, {y2}.0, {y2}.00
    fy3: {y3}, FY{y3}, FY {y3}, FY-{y3}, FY{yy3}, FY'{yy3}, '{yy3}, {y3}A, {y3} Actual, {y3} Audited, {y3}.0, {y3}.00
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


def _extract_financial_sections(ocr_text: str, target_chars: int = MAX_CONTEXT_CHARS, fy_years: tuple = None) -> str:
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

    # Pre-compute the global max-magnitude char position before ANY windows are popped.
    # Used later by Force-Include #4 to find the correct prev-window (column headers).
    # We scan the full OCR text in WINDOW steps — same stride as the window creation loop.
    _PRE_NUM_RE = re.compile(r'\b(\d[\d,]{2,})\b')
    _global_max_val = 0.0
    _global_max_pos = 0  # char offset of the globally max-magnitude window
    for _pre_i in range(0, len(ocr_text), WINDOW):
        _pre_chunk = ocr_text[_pre_i:_pre_i + WINDOW]
        _pre_nums = [float(m.replace(',', '')) for m in _PRE_NUM_RE.findall(_pre_chunk)]
        if _pre_nums:
            _pre_mx = max(_pre_nums)
            if _pre_mx > _global_max_val:
                _global_max_val = _pre_mx
                _global_max_pos = _pre_i
    logger.info(f"  Global max-magnitude pre-scan: char {_global_max_pos} "
                f"(max value: {_global_max_val:,.0f})")

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

    # FORCE-INCLUDE: window with most historical-year label matches
    # Ensures the historical P&L table (fy1/fy2/fy3 columns) is always
    # sent to the LLM even when keyword scoring prioritises the projection section.
    if fy_years and total_chars < MAX_CHARS:
        hist_strs = [str(y) for y in fy_years]  # e.g. ['2021', '2022', '2023']
        best_hist_score = -1
        best_hist_idx = -1
        for _idx, _win in enumerate(windows):
            _match = sum(1 for _yr in hist_strs if _yr in _win[3])
            if _match > best_hist_score:
                best_hist_score = _match
                best_hist_idx = _idx
        if best_hist_idx >= 0 and best_hist_score > 0:
            hist_win = windows.pop(best_hist_idx)
            selected.append(hist_win)
            total_chars += len(hist_win[3])
            logger.info(f"  Historical-year force-include: window at char {hist_win[2]} "
                        f"(matched {best_hist_score}/{len(hist_strs)} year labels)")

    # FORCE-INCLUDE #3: window whose maximum individual number is globally highest.
    # The largest single number in a CIM is overwhelmingly the consolidated revenue.
    # A dense projection table can out-score the P&L on density, but not on magnitude.
    if windows and total_chars < MAX_CHARS:
        _NUM_RE = re.compile(r'\b(\d[\d,]{2,})\b')
        _best_mag_idx = -1
        _best_mag_val = 0.0
        for _idx, _win in enumerate(windows):
            _nums = [float(m.replace(',', '')) for m in _NUM_RE.findall(_win[3])]
            if _nums:
                _mx = max(_nums)
                if _mx > _best_mag_val:
                    _best_mag_val = _mx
                    _best_mag_idx = _idx
        if _best_mag_idx >= 0:
            _mag_win = windows.pop(_best_mag_idx)
            # Skip if already covered by a previously force-included window (within 500 chars)
            _already_covered = any(abs(w[2] - _mag_win[2]) < 500 for w in selected)
            if not _already_covered:
                selected.append(_mag_win)
                total_chars += len(_mag_win[3])
                logger.info(f"  Max-magnitude force-include: window at char {_mag_win[2]} "
                            f"(max value: {_best_mag_val:,.0f})")

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

    # Month-Year column header pattern: "Dec-23A", "Mar-24", "Jun-25E" etc.
    # Converts 2-digit year to full year: Dec-23A → 2023 (historical), Jun-25E → 2025 (proj)
    month_year_pat = re.compile(
        r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-(\d{2})([AaEeFfBbPp])?\b'
    )

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

    # Extended suffix pattern — catches OCR-stripped variants where the
    # suffix character was dropped entirely. Matches years that appear:
    #   - on lines containing the words Forecast/Projection/Budget/Estimate
    #     within 60 chars of the year (e.g. "FY2023 Forecast", "2023 (Budget)")
    #   - or inside explicit section titles like "Five-Year Projections FY2023"
    # These years are treated as projection years even without E/F/B/P character.
    _proj_context_pat = re.compile(
        r'(?<!\d)(20\d{2})(?!\d)[^\n]{0,60}'
        r'(?:Forecast|Projection|Budget|Estimate|Outlook|Plan|Forward)'
        r'|(?:Forecast|Projection|Budget|Estimate|Outlook|Plan|Forward)'
        r'[^\n]{0,60}(?<!\d)(20\d{2})(?!\d)',
        re.IGNORECASE
    )
    # Merge extended projection years into proj_yrs_by_suffix
    for _m in _proj_context_pat.finditer(ocr_text):
        _yr_str = _m.group(1) or _m.group(2)
        if _yr_str:
            _yr = int(_yr_str)
            if 2000 <= _yr <= current_year + 10:
                proj_yrs_by_suffix.add(_yr)
    logger.info(
        f"  proj_yrs_by_suffix after extended pattern: {sorted(proj_yrs_by_suffix)}"
    )

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
            # Allow current_year + 1: CIMs are often released in early calendar year
            # for a fiscal year that just ended (e.g. Dec-25A released in early 2026
            # is a genuine historical year even if current_year = 2025 at processing time).
            if 2000 <= _hyr <= current_year + 1:
                confirmed_hist_years.add(_hyr)
    logger.info(f"  Confirmed-historical years (A/Actual/Audited markers): {sorted(confirmed_hist_years)}")

    # Also scan month-year format headers (Dec-23A, Mar-24A, Jun-25E) —
    # CIPs frequently label P&L columns with month-end dates rather than full years.
    # A suffix → historical; E/F/B/P suffix → projection.
    _month_yr_explicit_a: set[int] = set()  # Dec-25A / Mar-24A — unambiguous historical column headers
    for _mm in month_year_pat.finditer(ocr_text):
        _yr_short = _mm.group(1)
        _suf = (_mm.group(2) or '').upper()
        _yr = 2000 + int(_yr_short)
        if 2000 <= _yr <= current_year + 10:
            if _suf == 'A':
                # Explicit 'A' suffix — highest-reliability historical signal (column header format)
                if _yr <= current_year + 1:
                    _month_yr_explicit_a.add(_yr)
                    confirmed_hist_years.add(_yr)
            elif _suf == '':
                # No suffix — still plausibly historical (e.g. "Dec-24"), add to confirmed but NOT explicit-A
                if _yr <= current_year + 1:
                    confirmed_hist_years.add(_yr)
            elif _suf in ('E', 'F', 'B', 'P'):
                proj_yrs_by_suffix.add(_yr)
    logger.info(f"  Month-year format scan: confirmed_hist={sorted(confirmed_hist_years)}")
    logger.info(f"  Month-year explicit-A (Guard 5 primary): {sorted(_month_yr_explicit_a)}")

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
        # Also detect month-year format headers on this line (Dec-23A, Mar-24, Jun-25E)
        for _mm in month_year_pat.finditer(line):
            _yr_short = _mm.group(1)
            _suf = (_mm.group(2) or '').upper()
            _yr = 2000 + int(_yr_short)
            if 2000 <= _yr <= current_year + 10:
                if _suf in ('E', 'F', 'B', 'P'):
                    suffix_proj_on_line.add(_yr)
                elif _yr <= current_year + 1:
                    # Allow current_year + 1 for A-suffix month-year headers (Dec-25A in 2025)
                    all_yrs_on_line.add(_yr)
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
            if 2000 <= y <= current_year + 1 and y not in proj_yrs_by_suffix:
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
            if recent_window <= y <= current_year + 1 and y not in proj_yrs_by_suffix:
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

    # Guard 1 — y3 must never be a future year beyond current_year + 1.
    # Allows current_year + 1 for documents reporting a just-completed fiscal year
    # (e.g. Dec-25A CIM processed in Q1 2025 where current_year = 2025).
    # Hard-caps anything > current_year + 1 as a genuine future projection year.
    if most_recent_year > current_year + 1:
        fallback_years = sorted([y for y in candidates if y <= current_year + 1], reverse=True)
        most_recent_year = fallback_years[0] if fallback_years else current_year
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
            [y for y in candidates if y <= current_year + 1 and
             (y not in proj_yrs_by_suffix or y in confirmed_hist_years)],
            reverse=True
        )
        if hist_candidates:
            most_recent_year = hist_candidates[0]
        else:
            most_recent_year = most_recent_year - 1  # last resort: step back one year
        logger.info(f"  FY detect Guard 2: corrected y3={most_recent_year}")

    # Guard 3 — co-presence check: if most_recent_year-1 is a STRONGER
    # historical candidate than most_recent_year, prefer it.
    #
    # Trigger conditions (ANY one sufficient):
    #   (a) most_recent_year not in confirmed_hist_years
    #       AND most_recent_year-1 in confirmed_hist_years
    #       (prior year has explicit A/Actual/Audited marker, chosen year does not)
    #   (b) most_recent_year in proj_yrs_by_suffix
    #       AND most_recent_year-1 in table_counts
    #       (chosen year ever appeared with E/F suffix, prior year is a real table header)
    #
    # Both conditions check that (most_recent_year - 1) is actually present
    # as a table-header candidate before walking back — this prevents
    # false walk-back on documents that genuinely have only 1 historical year.
    _prior = most_recent_year - 1
    _guard3_a = (
        most_recent_year not in confirmed_hist_years
        and _prior in confirmed_hist_years
    )
    _guard3_b = (
        most_recent_year in proj_yrs_by_suffix
        and most_recent_year not in confirmed_hist_years  # don't walk back years with confirmed A/Actual markers
        and _prior in table_counts
    )
    if _guard3_a or _guard3_b:
        logger.info(
            f"  FY detect Guard 3: most_recent_year={most_recent_year} "
            f"(guard3_a={_guard3_a}, guard3_b={_guard3_b}) — "
            f"prior year {_prior} is stronger historical candidate → walking back"
        )
        most_recent_year = _prior

    # Guard 4 — sanity check: if y3 is more than 5 years behind current year
    # AND there are more recent confirmed historical years available (e.g. from
    # month-year column headers like Dec-23A), walk forward to use the most
    # recent confirmed year. This prevents old narrative years (founding dates,
    # acquisition years) from being treated as the current fiscal year.
    if most_recent_year < current_year - 5 and confirmed_hist_years:
        recent_confirmed = sorted(
            [y for y in confirmed_hist_years if y <= current_year + 1],
            reverse=True
        )
        if recent_confirmed and recent_confirmed[0] > most_recent_year:
            logger.info(
                f"  FY detect Guard 4: y3={most_recent_year} is >5 years old "
                f"— walking forward to confirmed hist year {recent_confirmed[0]}"
            )
            most_recent_year = recent_confirmed[0]

    # Guard 5 — frequency-bias correction: table-header counting often favours
    # OLDER years because they appear in more historical tables (segment analysis,
    # 5-year trends, management discussion sections).  The most recent fiscal year
    # (e.g. Dec-25A) may only appear in the consolidated P&L and balance sheet,
    # giving it a lower Pass-1 count than e.g. 2024 or 2023.
    #
    # Rule: if confirmed_hist_years (years with explicit A/Actual/Audited markers)
    # contains ANY year MORE RECENT than the currently chosen most_recent_year,
    # always prefer the newest confirmed year.
    #
    # This is NOT document-specific: it correctly handles any CIM where the
    # latest fiscal year is under-represented in table headers compared to
    # prior years with more narrative/segment coverage.
    #
    # Guard 5: two-tier frequency-bias correction
    # Tier 1 (highest confidence): month-year explicit-A headers (e.g. Dec-25A) that are NOT
    #   known projection years.  These column-header patterns cannot appear in prose.
    # Tier 2 (fallback): all confirmed_hist_years filtered by not in proj_yrs_by_suffix.
    # Using proj_yrs_by_suffix filter prevents genuine projection years (e.g. 2026E) that
    #   happen to have appeared near the word "Actual" in prose from polluting the result.
    # Tier 1 does NOT filter by proj_yrs_by_suffix.
    # "Dec-25A" with explicit 'A' suffix is definitive proof of a historical year.
    # A year being in proj_yrs_by_suffix (from prose like "FY2025 plan/budget/forecast")
    # cannot override a structured column header with an explicit audit marker.
    _g5_tier1 = {y for y in _month_yr_explicit_a
                 if y <= current_year + 1}
    _g5_tier2 = {y for y in confirmed_hist_years
                 if y <= current_year + 1 and y not in proj_yrs_by_suffix}
    _g5_candidates = _g5_tier1 if _g5_tier1 else _g5_tier2
    if _g5_candidates:
        newest_confirmed = max(_g5_candidates)
        if newest_confirmed > most_recent_year:
            logger.info(
                f"  FY detect Guard 5 (frequency-bias correction): "
                f"newest={newest_confirmed} > most_recent={most_recent_year} "
                f"— promoting (tier1={sorted(_g5_tier1)}, tier2={sorted(_g5_tier2)})"
            )
            most_recent_year = newest_confirmed

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
UNITS DETECTION — READ BEFORE EXTRACTING ANY VALUES:
  Scan the document for a units qualifier. Check table title, caption, column header footnotes.
  Apply conversion:
    "thousands" / "$000s" / "in 000s"  → values already in $000s → use as-is
    "millions" / "$M"                  → multiply by 1,000
    "billions" / "$B"                  → multiply by 1,000,000
    No qualifier + values look like full dollars (e.g. 4,800,000) → divide by 1,000
  Sanity: mid-market company revenue is typically 1,000–500,000 in $000s.

Three historical fiscal years detected in this document:

  fy1 = {fy1}
    Accept these column headers: {fy1}  FY{fy1}  FY {fy1}  FY-{fy1}
                                  FY{yy1}  FY'{yy1}  '{yy1}
                                  {fy1}A  {fy1} Actual  {fy1} Audited
                                  {fy1}.0  {fy1}.00
                                  Dec-{yy1}A  Dec-{yy1}  Mar-{yy1}A  Jun-{yy1}A  Sep-{yy1}A

  fy2 = {fy2}
    Accept these column headers: {fy2}  FY{fy2}  FY {fy2}  FY-{fy2}
                                  FY{yy2}  FY'{yy2}  '{yy2}
                                  {fy2}A  {fy2} Actual  {fy2} Audited
                                  {fy2}.0  {fy2}.00
                                  Dec-{yy2}A  Dec-{yy2}  Mar-{yy2}A  Jun-{yy2}A  Sep-{yy2}A

  fy3 = {fy3}
    Accept these column headers: {fy3}  FY{fy3}  FY {fy3}  FY-{fy3}
                                  FY{yy3}  FY'{yy3}  '{yy3}
                                  {fy3}A  {fy3} Actual  {fy3} Audited
                                  {fy3}.0  {fy3}.00
                                  Dec-{yy3}A  Dec-{yy3}  Mar-{yy3}A  Jun-{yy3}A  Sep-{yy3}A
    IMPORTANT: if both {fy3}A (Actual/Audited) AND {fy3}E / {fy3}B (Estimate/Budget) columns
    exist for the same year, extract ONLY from the {fy3}A Actual column. Ignore {fy3}E/{fy3}B.
    Never use a Budget or Estimate column for a year that has a confirmed Actual column.

Projection years (extract ONLY if document has an explicit forecast table):
  y1 = {proj_y1}  Accept: {proj_y1}  FY{proj_y1}  {proj_y1}E  {proj_y1}F  {proj_y1}B  {proj_y1}P  {proj_y1}.0
  y2 = {proj_y2}  Accept: {proj_y2}  FY{proj_y2}  {proj_y2}E  {proj_y2}F  {proj_y2}.0
  y3 = {proj_y3}  Accept: {proj_y3}  FY{proj_y3}  {proj_y3}E  {proj_y3}F  {proj_y3}.0
  y4 = {proj_y4}  Accept: {proj_y4}  FY{proj_y4}  {proj_y4}E  {proj_y4}F  {proj_y4}.0
  y5 = {proj_y5}  Accept: {proj_y5}  FY{proj_y5}  {proj_y5}E  {proj_y5}F  {proj_y5}.0

COMBINED OR SEPARATE TABLE — BOTH ARE VALID:
  The financial data may appear as:
    (a) A SINGLE COMBINED TABLE with columns: {fy1} | {fy2} | {fy3} | {proj_y1}E | {proj_y2}E | ...
    (b) TWO SEPARATE TABLES: one historical P&L, one forward-looking projection table
  In EITHER case — extract historical values from columns matching {fy1}/{fy2}/{fy3},
  and proj_ values from columns matching {proj_y1}+.

  PRIORITY ORDER:
  1. Find ANY table row labeled "Revenue"/"Sales"/"Net Revenue"/"Total Revenue" etc.
  2. From that row, read values under column headers matching {fy1}, {fy2}, {fy3}
     (including float variants {fy1}.0, {fy2}.0, {fy3}.0)
  3. Assign those values to revenue_fy1, revenue_fy2, revenue_fy3 respectively
  4. Repeat for gross_margin, sga, interest_expense, adjustments, adj_ebitda rows
  5. THEN extract proj_ values from columns {proj_y1}–{proj_y5} in the SAME or separate table

  CRITICAL: A projection column header like "{proj_y1}" or "{proj_y1}E" does NOT
  invalidate the historical columns in the same table. Extract ALL columns that match.

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

COLUMN FORMAT PRIORITY — WHEN TWO FORMATS EXIST FOR THE SAME YEAR:
  CIMs often present the same year's data in two column-header formats:
    HIGH-TRUST format: Month-Year-A (Dec-{yy1}A, Dec-{yy2}A, Dec-{yy3}A, Mar-{yy1}A, etc.)
      → Period explicitly confirmed as Actual/Audited by the 'A' suffix.
    LOW-TRUST  format: Bare year or FY-year ({fy1}, FY{fy1}, FY-{fy1}, {fy2}, FY{fy2}, etc.)
      → Period label only; column may contain budget, plan, or segment data.
  RULE: If the document contains BOTH formats for the SAME fiscal year, extract
  EXCLUSIVELY from the Month-Year-A (HIGH-TRUST) column. Ignore the bare-year column
  for that year entirely. Apply this rule to ALL three fiscal years (fy1, fy2, fy3).
  EXAMPLE: Table A columns [FY{fy1} | FY{fy2} | FY{fy3}] — values 125,837/99,086/92,452
           Table B columns [Dec-{yy1}A | Dec-{yy2}A | Dec-{yy3}A] — values 160,333/125,837/99,086
    → Use Table B exclusively. revenue_fy1=160,333, revenue_fy2=125,837, revenue_fy3=99,086.
    → Do NOT use Table A values even if Table A appears first or is more prominent.
  This rule supersedes header-matching priority — a Month-Year-A column always wins.
  TWO-COLUMN DEC-NNA CASE — if Dec-{yy1}A is NOT visible but Dec-{yy2}A and
  Dec-{yy3}A ARE visible as column headers in the same table:
    → Extract fy2 values from the Dec-{yy2}A column ({yy2} = {fy2}'s last 2 digits)
    → Extract fy3 values from the Dec-{yy3}A column ({yy3} = {fy3}'s last 2 digits)
    → Return null for ALL fy1 fields — do NOT assign Dec-{yy2}A data to fy1
    → Do NOT use a bare-year column (FY{fy1}) for fy1 when Dec-NNA format is available
    NOTE: "Dec-{yy2}A" contains "{yy2}" not "{yy1}" — it cannot represent fy1={fy1}.

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

--- SECTION: COLLATERAL & BALANCE SHEET EXTRACTION ---

BALANCE SHEET DATE COLUMN RULE (apply to ALL collateral fields below):
  The balance sheet often has MULTIPLE date columns (e.g. Dec-23A, Dec-24A, Dec-25A).
  You MUST identify which column corresponds to the MOST RECENT actual/audited date.
  The most recent actual column is the RIGHTMOST column with an 'A' or 'Actual' suffix.
  EXAMPLE: columns are [Dec-23A | Dec-24A | Dec-25A | Dec-26E]
    → The most recent actual column is Dec-25A (fy3={fy3}).
    → Dec-23A and Dec-24A are older historical columns — DO NOT use these.
    → Dec-26E is a projection column — DO NOT use this.
  VERIFY: The column you extract from must match the accepted fy3 header variants
    listed in Section A (e.g. Dec-{yy3}A or {fy3}A or FY{fy3}).
  If you accidentally extract from an older column (e.g. Dec-23A), the value will
  be significantly larger or different from the most recent year — re-check.

ACCOUNTS RECEIVABLE (net_revenue_collateral):
  Find AR / Trade Receivables / Accounts Receivable on the Balance Sheet.
  CRITICAL: Extract from the MOST RECENT year column (FY{fy3} / Dec-{yy3}A) ONLY.
  REJECT: averages, FY1 columns, FY2 columns, revenue P&L figures, deferred revenue.
  Prefer NET AR (after allowances). Convert to $000s.
  If absent (service company with no AR): return null.

INVENTORY (inventory_collateral):
  Find Inventory / Stock on the Balance Sheet (NOT from a separate sub-schedule or
  collateral schedule).
  CRITICAL: Extract from the MOST RECENT year column (FY{fy3} / Dec-{yy3}A) ONLY.
  Do NOT use FY1 or FY2 column values even if they appear first in the table.
  PERIOD ALIGNMENT: Inventory and Accounts Receivable are both current assets on the same
    balance sheet and share the same column headers. If the balance sheet shows 4 periods
    (Dec-{yy1}A | Dec-{yy2}A | Dec-{yy3}A | one more), the Dec-{yy3}A Inventory is the
    THIRD value in the row, not the last. Count column positions to confirm.
    If you can only see N−1 values in the Inventory row but the AR row has N values for
    the same periods, search adjacent text for the missing Inventory Dec-{yy3}A value before
    falling back to the prior year.
  If service company (no inventory): return 0 with confidence 0.95.
  BALANCE SHEET PRIORITY: The main Balance Sheet / Statement of Financial Position is the
    primary source. It shows a single "Inventory" or "Inventories" line that represents the
    net carrying value. Use THIS value, even if other schedules or sub-sections show a
    different (larger) inventory figure.
  DETAIL SUB-SCHEDULES: If the document has a separate Inventory Detail Schedule that breaks
    inventory into sub-categories (raw materials, WIP, finished goods), it may show a
    DIFFERENT total than the balance sheet. In that case:
    → Use the BALANCE SHEET single-line Inventory value — NOT the detail schedule total.
    → The detail schedule total may include gross values before obsolescence reserves.
  MULTIPLE VALUES: If you see two numeric values that could be inventory for the same period:
    → ALWAYS use the smaller value. The smaller value is the net/book value (after reserves).
    → The larger value is likely gross inventory before write-downs, or from a sub-schedule.
  CROSS-CHECK: inventory should normally be < 30% of revenue_fy3. If your extracted value
    exceeds 30% of revenue_fy3, lower confidence to 0.60 and re-verify you are on the
    correct total-inventory row (not summing sub-categories from multiple years).
  NO-ASSUMPTION RULE: If you cannot find an Inventory row on the balance sheet with
    confidence ≥ 0.80, return null. Do NOT guess or assume — only return a value that
    is explicitly labeled "Inventory" or "Inventories" on its own row.
    NOTE: Inventory and AR may coincidentally have the same numeric value in some documents.
    That is acceptable IF each value was independently read from its own labeled row.
    What is NOT acceptable: copying the AR value to fill in a missing Inventory row.
  BORROWING BASE TABLE WARNING: Some documents contain a separate Borrowing Base or
    Collateral Availability schedule with columns like (Asset | Gross Value | Advance Rate |
    Borrowing Base). Do NOT use the Gross Value column from this table for inventory_collateral
    — that gross value is an inflated lending basis larger than the balance sheet book value.
    ALWAYS prefer the BALANCE SHEET Inventory line for FY{fy3} / Dec-{yy3}A.
    If balance sheet shows $X and borrowing base table shows a different (larger) $Y for the
    same period, use $X (the balance sheet value).

MACHINERY & EQUIPMENT (me_equipment_collateral):
  Find machinery / production equipment / warehouse equipment asset value.
  Extract from the most recent year column (FY{fy3} / Dec-{yy3}A).
  Do NOT use FY1 or FY2 column values.
  ACCEPT ONLY these row labels: "Machinery", "Machinery & Equipment", "M&E",
    "Production Equipment", "Warehouse Equipment", "Manufacturing Equipment",
    "Equipment", "Plant & Equipment", "Plant, Property & Equipment",
    "Fixed Assets", "FF&E", "Furniture Fixtures & Equipment",
    "Furniture and Equipment", "Furniture & Equipment"
  DO NOT extract from rows labeled: "Building", "Buildings", "Land", "Real Estate",
    "Leasehold Improvements", "Land & Building" — those belong to building_land.
  ANTI-SWAP: me_equipment value must come from the M&E/Equipment row, NOT the Building row.
  ROW-LABEL-FIRST RULE: Fixed Asset Schedules typically show ONE value series per row (Gross Cost).
    STEP 1: Find the row whose label matches an M&E label (from ACCEPT list above).
    STEP 2: From that row, extract the MOST RECENT historical year column value
            (Dec-{yy3}A or FY{fy3} — rightmost non-projection column).
    STEP 3: Return that value. Values that INCREASE across years = correct Gross Cost.
            Do NOT skip the row because its values are increasing.
    STEP 4: ONLY if the schedule shows a separate per-asset Net Book Value column (NBV = Cost
            minus Accum. Depr. shown on the same row), prefer NBV over Gross Cost.
            If no per-asset NBV column exists, use Gross Cost.
  ⚠️ DO NOT use value trend (constant vs increasing) to decide which row is M&E.
     Building's Gross Cost is often constant — that does NOT make it M&E Net Book Value.

BUILDING & LAND (building_land_collateral):
  Find real estate / building / land gross asset value.
  Extract from the most recent year column (FY{fy3} / Dec-{yy3}A).
  Do NOT use FY1 or FY2 column values.
  ACCEPT ONLY these row labels: "Building", "Buildings", "Land", "Real Estate",
    "Land & Buildings", "Buildings & Improvements", "Building & Land",
    "Leasehold Improvements", "Land & Improvements", "Building & Improvements", "Property"
  DO NOT extract from rows labeled: "Equipment", "Machinery", "M&E", "Warehouse Equipment",
    "Fixed Assets", "FF&E" — those belong to me_equipment.
  If table shows "Asset Value | Advance Rate | Borrowing Base", return ONLY the
  Asset Value column — NOT the advance-rate-adjusted borrowing base.
  ANTI-SWAP: building value must come from the Building/Land row, NOT the Equipment row.
  ROW-LABEL-FIRST RULE: Identify the Building row by its LABEL, not its value pattern.
    STEP 1: Find the row whose label matches a Building/Land label (from ACCEPT list above).
    STEP 2: From that row, extract the MOST RECENT historical year column value
            (Dec-{yy3}A or FY{fy3} — rightmost non-projection column).
    STEP 3: Return that value. Building values are often CONSTANT across years (no new
            construction) — this is expected and correct. Extract it as-is.
  ⚠️ DO NOT require building_land_collateral >= me_equipment_collateral.
     In warehouse-heavy or equipment-intensive companies, M&E may far exceed Building value.
     Always use the row whose label matches — regardless of relative size.

EXISTING TERM LOANS (existing_term_loans):
  SUM all term loan tranches (TL-A + TL-B + etc.) at most recent balance sheet date.
  ACCEPT labels: "Term Loan", "Senior Secured Term Loan", "TL-A", "TL-B",
    "Bank Debt", "Senior Debt", "Term Debt", "Funded Debt", "First Lien Term Loan",
    "Senior Term Loan", "Term Facility", "Existing Term Debt", "Term Loans/Cashflow loans".
  EXCLUDE: revolving facility, line of credit, ABL revolver, seller notes, operating leases.
  Example: "TL-A $45M + TL-B $35M" → 80000.

COLLATERAL SOURCE PRIORITY:
  Always prefer BALANCE SHEET or FIXED ASSET SCHEDULE values over any borrowing base or
  collateral schedule. Borrowing base schedules contain LENDER-ADJUSTED gross values that
  differ from the book values we need here.
  SOURCE RULES by field:
    → net_revenue_collateral (AR): use Balance Sheet AR value for FY{fy3} / Dec-{yy3}A.
        If document has a 4-column borrowing base table (Asset|Gross Value|Advance Rate|Borrowing Base),
        Col 2 Gross Value is acceptable for AR only.
    → inventory_collateral: use BALANCE SHEET Inventory for FY{fy3} / Dec-{yy3}A.
        Do NOT use the Gross Value column from a 4-column borrowing base schedule — that value
        inflates the book amount for lending purposes and is LARGER than the balance sheet value.
    → me_equipment_collateral: use value from M&E-labeled row in Fixed Asset Schedule.
        If schedule shows per-asset NBV column, prefer NBV. If only Gross Cost per row, use Gross Cost.
        Do NOT use borrowing base schedule.
    → building_land_collateral: use GROSS COST column from Fixed Asset Schedule (original price).
        Do NOT use Net Book Value column. Do NOT use borrowing base schedule.
  ANTI-ROW-SHIFT: every value must come from the SAME LINE as its asset label in the OCR text.

ADJUSTMENTS / ADD-BACKS EXTRACTION:
  Extract the TOTAL row (labeled 'Total Adjustments', 'Total Add-backs',
  'Total Non-Recurring', 'Total Addbacks', etc.) for ALL THREE fiscal years.
  CRITICAL: Do NOT extract from individual component rows (e.g. 'M&A Costs',
  'COVID Relief', 'Legal Settlement'). These rows may be sparse — some years
  may be blank on certain component lines. Only the TOTAL ROW has consistent
  three-column values across fy1/fy2/fy3.
  Validation: each year's adjustment total should be >= 0.
  If the total row for a year is blank, return 0 (not null) for that year.

INTEREST EXPENSE EXTRACTION:
  interest_expense is a STANDALONE single row in the P&L, positioned
  BELOW the Adjusted EBITDA line. It is NOT part of the add-backs block.
  The "Do NOT extract from individual component rows" rule above applies
  ONLY to the adjustments/add-backs block. It does NOT apply here.
  Extract interest_expense_fy1, interest_expense_fy2, interest_expense_fy3
  from ALL THREE fiscal year columns of this single row.
  Accepted labels (case-insensitive):
    'Interest Expense'  |  'Interest expense/(income)'
    'Net Interest'      |  'Interest & Financing Costs'
    'Interest Charges'  |  'Interest on Debt'
  This field is subject to the normal year-column matching rules (Rule 7/8).
  Extract all three years. Return null only if the row is genuinely absent.

ACQUISITION MULTIPLE EXTRACTION (acquisition_multiple):
  WHAT TO FIND: EV/EBITDA for THIS specific transaction only. Single plain number (e.g. 5.4).

  LOOKUP PRIORITY (stop at first match):
    P1. "EV of $X / EBITDA of $Y = Z.Zx" stated in Transaction Structure or Offer Summary.
    P2. Implied: Enterprise Value stated + EBITDA stated for same period → compute EV / EBITDA.
    P3. Label: "Transaction Multiple" or "Purchase Multiple" explicitly for THIS deal.

  REJECT — these are NOT the acquisition multiple:
    ✗ Peer trading comp medians (e.g. "6.3x peer median", "sector comps 7.2x")
    ✗ Precedent transaction medians (e.g. "7.2x median precedent")
    ✗ Exit multiples or LBO scenario exit range multiples
    ✗ Football field valuation range multiples
    ✗ "Implied multiple" from a comparable companies or precedents table

  VALIDATION: Must be between 3.0x and 15.0x. If confidence < 0.75 → return null. No defaults.
  IMPORTANT: Extract the NUMBER ONLY — not the EBITDA dollar value.
    CORRECT: "5.4× Adjusted EBITDA" → return 5.4
    WRONG:   "5.4× Adjusted EBITDA of $95.3M" → do NOT return 95.3

TAX RATE EXTRACTION (effective_tax_rate):
  WHAT TO FIND: Combined effective income tax rate — NOT the statutory federal 21%.
  LOOK FOR (in order):
    1. Row labelled "Income Tax", "Tax Expense", "Provision for Income Taxes" in P&L.
       Derive effective rate: Tax Expense / EBT (Earnings Before Tax).
    2. Explicit label: "Effective Tax Rate: X%" in notes, assumptions, or deal overview.
    3. "Tax (X%)" stated inline in an EBITDA-to-net-income bridge or summary box.
  RULES:
    - Convert % to decimal: "25%" → 0.25.
    - Acceptable range: 0.05 to 0.45. Return null if outside range.
    - Return null if not found — do NOT default to any value.

CAPITAL EXPENDITURE PROJECTIONS (proj_capex_y1..y5):
  SEARCH IN ORDER:
    1. Projections table — row labelled "CapEx", "Capital Expenditures", "PP&E Additions"
       in the same table as proj_revenue_y1..y5.
    2. LBO model / returns analysis — CapEx schedule.
    3. Cash flow projections — investing activities section.
    4. Assumptions: "CapEx as % of Revenue = X%" → compute revenue_yN × X%.
  IMPORTANT: Growth and technology companies show INCREASING CapEx over time.
    A declining schedule that reaches zero (e.g. 64→48→32→16→0) is a template
    artifact, NOT real CapEx data — return null for those years.
  Return POSITIVE numbers in $000s. Only extract if confidence >= 0.70.
  Return null if not found — do NOT fabricate or use a declining template schedule.

CONSOLIDATED vs SEGMENT TABLE DISAMBIGUATION (CRITICAL):
  Many CIPs contain BOTH a consolidated (total company) P&L AND separate segment P&Ls.
  You MUST ALWAYS extract revenue, gross margin, SG&A, EBITDA from the CONSOLIDATED table.
  The consolidated table is typically titled:
    "Consolidated Income Statement", "Summary Financials", "Financial Overview",
    "Historical Financials", "Tale of the Tape", "Selected Financial Data"
  Segment tables are typically titled:
    "[Name] Segment", "Industrial Segment", "Consumer Segment", "Division X", "Business Unit Y"

  IDENTIFICATION RULE: The consolidated table has a "Total Revenue" row whose value
    is GREATER THAN OR EQUAL TO the sum of all segment revenues.
    If you find multiple tables with revenue rows → use the one with the LARGEST revenue.

  SEGMENT TABLE WARNING: Segment tables often appear AFTER the consolidated P&L and
    use the SAME column header years (e.g. Dec-23A, Dec-24A, Dec-25A). The revenue
    row in a segment table looks identical to consolidated revenue but is a fraction
    of the total. ALWAYS verify the table title before extracting revenue from it.
    If the table title contains words like "Industrial", "Consumer", "Segment", "Division",
    "Business Unit", or any named brand → that is a segment table. SKIP IT.

  CROSS-CHECK: After extracting revenue, verify:
    consolidated revenue ≥ any individual segment revenue found elsewhere.
    If not, you extracted from a segment table — re-extract from the consolidated table.

  IGNORE: Segment-level tables entirely for all historical (fy1/fy2/fy3) and
    projection (y1–y5) field extraction.

════════════════════════════════════════════════════════════
SECTION B — REVENUE EXTRACTION ALGORITHM [EXECUTE STEP BY STEP]
════════════════════════════════════════════════════════════

STEP 1 — LOCATE THE P&L TABLE:
  Find the Income Statement / P&L table. Look for these section headings:
    "Income Statement", "Profit & Loss", "P&L", "Financial Summary",
    "Historical Financials", "Revenue Summary", "Consolidated Statements of Operations",
    "Selected Financial Data", "Financial Highlights", "Tale of the Tape"
  If no heading found, look for any table whose first data row is labeled
  "Revenue", "Sales", "Net Revenue", or similar.

STEP 2 — IDENTIFY THE REVENUE ROW:
  The revenue row is the TOPMOST financial data row — it appears BEFORE
  "Cost of Goods Sold" / "COGS" / "Cost of Revenue" / "Gross Margin".

  ACCEPT these row labels (case-insensitive, partial match OK):
    "Revenue"              "Net Revenue"           "Total Revenue"
    "Sales"                "Net Sales"             "Total Sales"
    "Total Net Sales"      "Revenues"              "Total Revenues"
    "Gross Revenue"  "Total Gross Revenue"   "Revenue / Sales"
    "Revenue/Sales"  "Net Revenues"          "Total Net Revenues"
    "Operating Revenue"   "Total Operating Revenue"
    NOTE: "Gross Revenue" = top-line total billings (same as Revenue).
          It does NOT mean "Gross Profit" or "Gross Margin" (those appear AFTER COGS).
          Never assign a row labeled "Gross Profit" to revenue fields.

  REJECT these row labels (NEVER extract — even if value looks largest):
    Any label containing any of these words: Recurring, Product, Service,
    Subscription, License, Maintenance, Professional Services, Segment,
    Region, Geographic, Division, Business Unit, Channel, Category, Brand,
    Subsidiary, International, Domestic, Americas, EMEA, APAC, Asia, Europe,
    Industrial, Consumer, Distribution, Residential, Commercial, Wholesale,
    Retail, OEM, Aftermarket, Direct, Indirect
  Also REJECT any row whose label is a proper noun (a company brand name,
    subsidiary name, or named product line — e.g. "Silpak", "Polytek Industrial",
    "Acme Products"). These are sub-brand revenue rows, not consolidated totals.

  MAGNITUDE CHECK (run BEFORE writing to JSON):
    If revenue_fy3 < 10% of proj_revenue_y1 (when proj_revenue_y1 is already
    extracted) → you selected a segment sub-row. Re-extract from the consolidated
    total table. The consolidated revenue is ALWAYS ≥ any individual segment.
    If no proj_revenue_y1 is available, apply the cross-check below instead.

  MULTIPLE REVENUE ROWS: If two or more rows match ACCEPT labels →
    choose the row with the HIGHEST value (consolidated total ≥ any sub-row).
    Cite the exact label of the row chosen.

  CROSS-TABLE CONSOLIDATION CHECK (run AFTER header-matching):
    If you matched revenue via bare-year columns (FY{fy1}/FY{fy2}/FY{fy3}) AND
    the document ALSO contains Month-Year-A columns (Dec-{yy1}A/Dec-{yy2}A/Dec-{yy3}A)
    with DIFFERENT values for the same fiscal years → DISCARD the bare-year values
    and re-extract using the Month-Year-A columns per Section A COLUMN FORMAT PRIORITY.
    The Month-Year-A table contains audited actuals; the bare-year table may be budget/plan.

  P&L ROW-ORDER SANITY CHECK (run BEFORE writing revenue to JSON):
    In every standard P&L/income statement, rows appear in this fixed order:
      1st row: Net Revenue / Net Sales  ← ALWAYS the largest number, TOPMOST row
      2nd row: Cost of Goods Sold (COGS) [optional]
      3rd row: Gross Profit / Gross Margin  ← ALWAYS less than Revenue
      ...lower rows: SG&A, EBITDA, Net Income
    RULE: revenue_fyN MUST be GREATER than gross_margin_fyN for the same year.
    If your extracted revenue_fyN ≤ gross_margin_fyN, you have read the WRONG row —
    the value you extracted is actually Gross Profit, not Revenue.
    ACTION: find the row labeled "Net Revenue" or "Revenue" ABOVE the Gross Profit row
    and re-extract the correct (larger) revenue value from that row.
    This check applies to all three fiscal years independently.

STEP 3 — EXTRACT COLUMN VALUES:
  PRIORITY 1 — HEADER MATCHING (preferred):
  From the identified revenue row, read values under columns {fy1}, {fy2}, {fy3}
  using ALL accepted header variants listed in Section A (including {fy1}.0 etc.).
    column matching fy1 → revenue_fy1
    column matching fy2 → revenue_fy2
    column matching fy3 → revenue_fy3

  PRIORITY 2 — POSITIONAL FALLBACK (use ONLY when ALL conditions below are met):
  CONDITIONS (all must be true — do NOT use if any condition is false):
    (a) You found the Revenue row via an ACCEPT label
    (b) Header matching returned null for ALL of revenue_fy1, revenue_fy2, revenue_fy3
    (c) proj_revenue_y1 was already assigned via a projection column header
    (d) There are numeric values to the LEFT of the proj_y1 column in the same row
    (e) Those left-side values are LARGER than proj_y1 (historical revenue ≥ nearest future year
        is the expected pattern; if left values are smaller → wrong row or wrong direction)
  If ALL conditions are met, assign positionally:
    value immediately LEFT of proj_y1 position → revenue_fy3
    value two positions LEFT of proj_y1        → revenue_fy2
    value three positions LEFT of proj_y1      → revenue_fy1
  Set confidence = 0.70 and cite "positional assignment — header unmatched".
  EXAMPLE: Row is "160,333 | 125,837 | 99,086 | 92,452 | 88,000 | ..."
    and proj_y1=92,452 was header-matched → revenue_fy3=99,086, revenue_fy2=125,837.
  SAFETY: If the positional value equals any proj_revenue_yN value → SKIP (contamination).
  SCOPE: This positional fallback is REVENUE ROW ONLY. Do NOT apply positional
  inference to gross_margin, SGA, adjustments, or any other P&L rows.

STEP 4 — ABSENT CASE:
  Return null when:
    (a) No row with an ACCEPT label exists anywhere in the document, OR
    (b) The row exists but the target year column is blank or missing.
  Do NOT derive revenue from other fields. Do NOT use a REJECT-label row as fallback.
  Null is the correct answer when revenue is genuinely absent.

STEP 5 — VERIFY BEFORE WRITING JSON:
  [ ] Revenue ≥ Gross Margin for each year (accounting identity — if violated, re-check rows)
  [ ] Revenue row appears BEFORE COGS in the P&L (topmost position)
  [ ] Revenue values are POSITIVE numbers (negative revenue = wrong row selected)
  [ ] Revenue YoY change ≤ ±500% (extreme swing = wrong row or wrong column)
  [ ] Cited row label is from the ACCEPT list

COGS EXTRACTION (always extract — used as gross margin fallback):
  Accept labels: "Cost of Goods Sold", "COGS", "Cost of Sales", "Cost of Revenue",
    "Direct Costs", "Cost of Products", "Cost of Services"
  Extract cogs_fy1/fy2/fy3 for ALL three historical years even if a Gross Margin row exists.
  This allows the system to derive gross_margin = revenue − COGS when the GM row is absent.

PROJECTION REVENUE (proj_revenue_y1–y5):
  Apply STEPS 1–5 to the projection table columns {proj_y1}–{proj_y5}.
  Return null for ALL proj_revenue fields if no explicit forecast/projection table exists.
  Do NOT auto-calculate — the system handles projection calculation separately.

GROSS MARGIN ROW EXTRACTION:
  The Gross Margin (Gross Profit) row appears IMMEDIATELY AFTER the COGS row in the P&L.
  Its dollar value is ALWAYS LESS THAN Revenue (= Revenue − COGS by accounting definition).

  ACCEPT these row labels (case-insensitive):
    "Gross Profit"        "Gross Margin"           "Gross Income"
    "Gross Profit/Loss"   "Gross Profit ($)"       "Gross Profit Margin ($)"
    "Gross Profit Dollars" "GP ($)"                "Gross Margin $"

  IMPORTANT — GM% to GM$ CONVERSION:
    Some CIMs show a "Gross Margin %" row instead of (or in addition to) a dollar row.
    If you find ONLY a percentage (e.g. "Gross Margin %  43.0%  41.1%  39.9%") and no
    dollar row, CONVERT it: Gross Margin $ = Revenue × Gross Margin %.
    Example: Revenue = 99,086 and GM% = 39.9% → GM$ = 99,086 × 0.399 = 39,535.
    Set confidence = 0.85. Cite: "Derived: Revenue × GM% (GM$ row absent)".
    This is preferred over returning null when the % is clearly stated.

  REJECT these as gross_margin row labels (they are Revenue rows, not Margin rows):
    "Gross Revenue"   "Gross Sales"   "Gross Billings"   "Total Gross Revenue"

  CRITICAL ACCOUNTING SANITY CHECK (apply before writing to JSON):
    ✗ If extracted gross_margin for a year >= revenue for the same year → WRONG ROW.
      Return null for that year. The backend will derive it from EBITDA automatically.
    ✗ If revenue is null but gross_margin > adj_ebitda × 8 for that year →
      likely wrong row. Return null — backend will derive it.
    ✓ Correct: gross_margin is always POSITIVE and LESS THAN revenue.

  Apply the SAME rules to proj_gross_margin_y1–y5 (same row, projection columns).
  IMPORTANT: If historical gross_margin returned null due to the sanity check failure
  above, ALSO return null for ALL proj_gross_margin_y1–y5 — the entire row is untrusted.

SG&A / TOTAL OPERATING EXPENSES EXTRACTION (sga_fy1/2/3):
  Extract the TOTAL OPERATING EXPENSES subtotal row, NOT individual sub-items.
  P&L tables often break down operating expenses into lines like:
    Selling expenses, Warehouse & Distribution, G&A, Amortization, R&D, IT, etc.
  You MUST extract the SUBTOTAL row that aggregates all of these.

  CRITICAL — CONSOLIDATED TABLE ONLY:
    Many CIMs include segment-level P&Ls (e.g. "Industrial Segment", "Consumer Segment")
    in SEPARATE tables AFTER the consolidated P&L. These segment tables have their own
    SG&A rows that are MUCH SMALLER than the consolidated total.
    You MUST extract SG&A ONLY from the CONSOLIDATED (company-wide) P&L table.
    If a table title contains "Segment", "Division", "Industrial", "Consumer", or any
    business unit name → it is a segment table. Do NOT use its SG&A values.

  ACCEPT these subtotal row labels (case-insensitive):
    "Total SG&A"              "Total Operating Expenses"    "Total OpEx"
    "SG&A"                    "Selling, General & Admin"    "Total Selling & G&A"
    "Operating Expenses"      "Total Expenses"              "Total G&A"
    "SG&A Expenses"           "Total Overhead"

  REJECT these as sga — they are sub-items, NOT the total:
    "R&D"  "Research & Development"  "Amortization"  "Depreciation"
    "Selling Expenses" (alone)  "G&A" (alone)  "Warehouse" (alone)
    Any single line item that is clearly a sub-component

  VALIDATION: sga should be SIGNIFICANTLY LARGER than any single sub-item.
    If extracted sga matches R&D exactly (e.g. both are 1,102) → you selected
    the wrong row. Re-extract from the TOTAL row. R&D is typically < 10% of total OpEx.
    If sga < 10% of revenue for the same year → likely a segment-level value.
    Re-extract from the consolidated total. Consolidated SG&A is 10%–60% of revenue.

  The sga field should be in range 10%–60% of revenue for most businesses.
  Apply the SAME rules to proj_sga_y1–y5 (same row, projection columns).

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

EBITDA — THREE DISTINCT METRICS (CRITICAL — DO NOT CONFUSE THEM):
  The P&L has THREE separate EBITDA-related line items. Extract each into its own field.

  1. REPORTED EBITDA → reported_ebitda_fy1/fy2/fy3:
     The BASE EBITDA BEFORE any adjustments. Appears in the P&L BEFORE the add-backs block.
     Labels: 'Reported EBITDA', 'EBITDA', 'Operating EBITDA', 'Unadjusted EBITDA',
             'EBITDA (Reported)', 'EBITDA (as reported)', 'Standalone EBITDA', 'Company EBITDA',
             'EBITDA (before adjustments)'
     ⚠️ CAN BE NEGATIVE for distressed or acquisition-stage companies. Store negative values as-is
     (e.g. -4,000 / -1,000 / -21,000). Do NOT zero out or discard negative reported EBITDA.
     Extract ALL THREE years (fy1, fy2, fy3). If the P&L shows both "EBITDA" and "Adj. EBITDA":
       → "EBITDA" row = reported_ebitda (smaller, often negative)
       → "Adj. EBITDA" row = adj_ebitda (larger, includes add-backs)

  2. EBITDA ADJUSTMENTS → adjustments_fy1/fy2/fy3 (already in schema):
     The ADD-BACK AMOUNTS added to Reported EBITDA.
     Labels: 'Total Adjustments', 'Total Add-backs', 'Non-recurring Items'
     These are the SMALLER numbers in the EBITDA bridge section.
     Example magnitude: 405 / 1,797 / 549 (small relative to EBITDA)

  3. ADJUSTED EBITDA → adj_ebitda_fy1/fy2/fy3:
     The FINAL VALUE AFTER ADD-BACKS. Equals Reported EBITDA + Adjustments.
     Labels: 'Adj. EBITDA', 'Adjusted EBITDA', 'Pro Forma EBITDA', 'Normalized EBITDA'
     This is ALWAYS LARGER than Reported EBITDA.
     Example magnitude: 21,632 / 10,918 / 8,581 (large — close to reported EBITDA)

  MANDATORY VALIDATION before writing to JSON:
    ✗ FAIL if adj_ebitda ≈ adjustments (within 20%): you put add-back amounts into
      the Adjusted EBITDA field. This is wrong. Re-find the true Adjusted EBITDA row
      (it appears AFTER the add-backs and is MUCH LARGER than the add-back amounts).
    ✗ FAIL if adj_ebitda < reported_ebitda AND both are positive: impossible — add-backs are positive.
      Exception: reported_ebitda can be negative while adj_ebitda is positive (PF add-backs exceed loss).
    ✓ PASS: adj_ebitda = reported_ebitda + adjustments (approximately)
    ✓ PASS: adj_ebitda significantly LARGER than adjustments alone

ADJ. EBITDA LABEL MATCHING — check these in order before deriving:
  Accepted row labels (case-insensitive):
    "Adj. EBITDA"  |  "Adjusted EBITDA"  |  "Adj EBITDA"
    "EBITDA (Adjusted)"  |  "EBITDA (as adjusted)"
    "Normalized EBITDA"  |  "Recurring EBITDA"
    "PF Adj. EBITDA"  |  "Pro Forma Adj. EBITDA"
    "EBITDA"   ← also accept plain EBITDA label (common in CIMs without formal adjustments)
  Preferred source: the row immediately following a "Total Add-backs" or
    "Non-Recurring Adjustments" subtotal row.
  IMPORTANT: The Adjusted EBITDA row is the FINAL SUBTOTAL in the EBITDA bridge table.
    Its value is LARGER than the plain "EBITDA" row (because add-backs increase it).
    Example structure: EBITDA (8,581) → Add-backs (+886) → Adj. EBITDA (9,467)
    If two rows match (e.g. "EBITDA" and "Adj. EBITDA") → use the LARGER value.
  For the most recent fiscal year (fy3={fy3}), the Adjusted EBITDA is the key
    acquisition basis — extract it from the fy3 column with highest precision.
  Fallback derivation — try these options IN ORDER when direct label match fails:

  Option 1 — EBITDA row present (preferred fallback):
    adj_ebitda = EBITDA + adjustments
    (EBITDA row labels: 'EBITDA', 'Earnings Before Interest Tax D&A', 'EBITDA (unadjusted)')
    Set confidence = 0.80. Cite: "EBITDA row + adjustments"

  Option 2 — Operating income + D&A row both present:
    adj_ebitda = operating_income + depreciation_and_amortisation + adjustments
    (D&A row labels: 'Depreciation', 'Amortisation', 'D&A', 'Depreciation & Amortization')
    Set confidence = 0.75. Cite: "operating_income + D&A + adjustments"

  Option 3 — Operating income only (last resort):
    adj_ebitda = operating_income + adjustments
    where operating_income = the EBIT / Operating Income / Operating Profit line
    WARNING: This formula omits D&A and will understate adj_ebitda.
    Only use if Options 1 and 2 are both impossible.
    Set confidence = 0.65. Cite: "operating_income + adjustments (D&A not found)"

  SCAN ALL SECTIONS: When searching for adj_ebitda_fy1 and adj_ebitda_fy2,
  check not only the main P&L table but also: deal overview, executive
  summary, financial highlights, investment thesis, and any KPI summary box.
  These sections often state historical Adj. EBITDA values inline in prose
  (e.g. "Adjusted EBITDA grew from $8.1M in FY2021 to $9.7M in FY2022").
  Extract ALL three years wherever found with highest confidence match.
  Apply derivation fallback only if no direct value found anywhere.

IMPORTANT: Extract adj_ebitda for ALL THREE fiscal years from the same row.
  The Adjusted EBITDA row appears once in the P&L table with three year columns.
  Return adj_ebitda_fy1, adj_ebitda_fy2, AND adj_ebitda_fy3 — do not return
  only the most recent year. Map each year column by header match (not position):
  fy1 column → adj_ebitda_fy1, fy2 column → adj_ebitda_fy2, fy3 column → adj_ebitda_fy3.

ADJ. EBITDA PROJECTION CONTAMINATION GUARD (critical for multi-year tables):
  Many CIM P&L tables have MORE than 3 columns — they show historical years AND
  projection years side by side in the same row, for example:
    FY2022 | FY2023 | FY2024 | FY2025 | 2026F | 2027F | 2028F
    or: Dec-22A | Dec-23A | Dec-24A | Dec-25A | Y1 | Y2 | Y3
  The projection columns (Y1/Y2/Y3, 2026F/2027F/2028F, or any column to the RIGHT
  of the FY{fy3} / Dec-{yy3}A column) must NEVER be used for adj_ebitda_fy1/fy2/fy3.
  RULE: adj_ebitda_fyN must come ONLY from the column whose header matches {fy1}/{fy2}/{fy3}
    (or Dec-{yy1}A / Dec-{yy2}A / Dec-{yy3}A). Stop reading the row at the fy3 column.
  VERIFICATION: After extracting adj_ebitda_fy3, check that the column you used is
    the LAST historical column (i.e. no further historical year exists to its right).
    If the value you found is from a column to the right of fy3 → it is a projection
    value. Discard it and return null for adj_ebitda_fy3 with confidence 0.0.
  EXAMPLE 1: Row = [15,044 | 21,632 | 10,918 | 8,581 | 9,324 | 13,624 | 19,395]
    with headers [FY22 | FY23 | FY24 | FY25 | Y1 | Y2 | Y3] and fy3=2025:
    → adj_ebitda_fy3 = 8,581 (FY25 column, position 4). NOT 13,624 (Y2, position 6).
  EXAMPLE 2 ($M document): Row = [-4 | -1 | -21 | 23 | 26 | 32 | 42] (values in $M)
    with headers [2020A | 2021A | 2022A | 2023E | 2024P | 2025P | 2026P] and fy3=2022:
    → Reported EBITDA fy3 = -21 × 1000 = -21,000 (2022A column). NOT 23,000 (2023E).
    → If add-backs for 2022A = 28,000: PF Adj. EBITDA = -21,000 + 28,000 = 7,000.
    → Do NOT extract 42,000 (2026P) or 23,000 (2023E) as adj_ebitda_fy3.

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

CONTAMINATION GUARD: If proj_revenue_yN exactly equals any historical revenue
  (revenue_fy1, revenue_fy2, or revenue_fy3) → set to null. This prevents actuals
  from being mis-categorized as projections.
  Example: if revenue_fy3 = 99,086 and proj_revenue_y1 = 99,086 → set proj_revenue_y1 = null.

TEMPORAL GUARD: Projection Year 1 (y1) = {proj_y1} (= fy3 + 1 = {fy3} + 1).
  Do NOT treat any actual/audited year as a projection year. If a column is labeled
  "{fy3}A" or "Dec-{yy3}A" → it is historical (fy3), NOT projection year 1.
  Only columns labeled "{proj_y1}", "{proj_y1}E", "{proj_y1}F", or "{proj_y1}B"
  are valid projection year 1 sources.

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
  [ ] Adj. EBITDA: extracted all THREE years (fy1, fy2, fy3) from the same row — not just fy3
  [ ] existing_term_loans: returned outstanding balance, NOT the revolving ABL/revolver
  [ ] building_land_collateral: returned GROSS asset value, NOT advance-rate-adjusted borrowing base
  [ ] inventory_collateral: extracted from BALANCE SHEET Inventory line — NOT from borrowing base table Gross Value column (which inflates book value)
  [ ] me_equipment_collateral: extracted from a row labeled "Warehouse Equipment"/"M&E"/"Equipment"/"Machinery" — NOT inferred from value trend patterns
  [ ] me_equipment_collateral: value may be INCREASING across years (Gross Cost) — this is correct; do NOT discard it for being increasing
  [ ] adjustments: extracted TOTAL row for all three years, NOT individual component rows
  [ ] interest_expense: standalone row below Adj EBITDA — extracted all THREE years — NOT subject to total-row-only rule
  [ ] inventory_collateral: value confirmed on SAME LINE as 'Inventory' label — no row carry-forward from AR row
  [ ] inventory_collateral: read from its own labeled "Inventory" row — NOT assumed/copied from AR; AR and Inventory may coincidentally match in value, which is OK if each came from its own row
  [ ] collateral year: all collateral values extracted from fy3 / Dec-{yy3}A column — NOT from Dec-{yy1}A or Dec-{yy2}A (older columns)
  [ ] net_revenue_collateral: from "Accounts Receivable" / "AR" / "Trade Receivables" row on BALANCE SHEET — NOT from "Net Revenue" / "Revenue" row on P&L. If no balance sheet → null.
  [ ] adj_ebitda_fy3: column header matches fy3 year — NOT from any E/F/P/B projection column; cross-check: reported_ebitda + adjustments ≈ adj_ebitda (within 5%)
  [ ] E/F/B/P suffix columns: NOT mapped to historical slots (fy1/fy2/fy3)
  [ ] E/F/B/P suffix columns: ARE correctly mapped to proj_* slots (y1..y5)
  [ ] adj_ebitda_fy3: from FY{fy3}/Dec-{yy3}A column ONLY — NOT from Y1/Y2/Y3 or any projection column to its right
  [ ] Revenue row label: from ACCEPT list only — NOT from REJECT list (no segments/regions/products)
  [ ] Revenue row: is the TOPMOST data row in the P&L (appears BEFORE COGS / Gross Margin)
  [ ] Revenue values: all positive numbers (negative = wrong row selected)
  [ ] Revenue values: each year's revenue ≥ that year's gross margin (accounting identity)
  [ ] Projection revenue: extracted ONLY from explicit forecast/projection table — null otherwise
  [ ] Revenue absent: returned null (NOT 0, NOT a guess) when row or column genuinely missing
  [ ] Fiscal years: detected from P&L TABLE HEADERS, not from narrative/history/founding year text
  [ ] Month-year format: "Dec-23A" recognized as FY2023, "Dec-25A" as FY2025
  [ ] Revenue: extracted from the CONSOLIDATED table (largest revenue), not a segment sub-table
  [ ] Consolidated cross-check: extracted revenue >= any individual segment revenue in document
  [ ] SG&A: extracted TOTAL subtotal row — NOT R&D alone, NOT Selling alone, NOT any single sub-item
  [ ] SG&A validation: extracted value significantly > any single sub-component (e.g. R&D, D&A)
  [ ] building_land_collateral: extracted from a row labeled "Building"/"Land"/"Real Estate" — NOT inferred from being constant or larger than M&E
  [ ] Collateral anti-swap: me_equipment citation references M&E/Equipment/Warehouse Equipment label; building_land citation references Building/Land label — labels must not be swapped
  [ ] reported_ebitda: extracted from the EBITDA row BEFORE the add-backs section (smaller than adj_ebitda)
  [ ] adj_ebitda NOT ≈ adjustments: if adj_ebitda ≈ adjustments amount → wrong field, re-extract
  [ ] adj_ebitda >= reported_ebitda: adjusted must be ≥ base EBITDA (add-backs are always positive)
  [ ] line_of_credit: revolving credit/ABL balance from most recent balance sheet — null if not found
  [ ] current_lt_debt: current portion of LTD from most recent balance sheet — null if not found

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
  "proj_sga_y5":              {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "effective_tax_rate":       {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "proj_capex_y1":            {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "proj_capex_y2":            {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "proj_capex_y3":            {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "proj_capex_y4":            {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "proj_capex_y5":            {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "reported_ebitda_fy1":      {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "reported_ebitda_fy2":      {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "reported_ebitda_fy3":      {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "line_of_credit":           {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }},
  "current_lt_debt":          {{"value": <num|null>,  "confidence": <0-1>, "citation": "..." }}
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


_COLLATERAL_FIELDS = (
    'net_revenue_collateral',
    'inventory_collateral',
    'me_equipment_collateral',
    'building_land_collateral',
    'existing_term_loans',
)

# Keywords that score windows for each collateral section
_COLLATERAL_WINDOW_KEYWORDS = [
    # Balance sheet / AR / Inventory
    'accounts receivable', 'trade receivables', 'receivables', 'inventory', 'inventories',
    'balance sheet', 'current assets', 'net ar', 'trade ar',
    # Fixed asset schedule / M&E / Building
    'machinery', 'warehouse equipment', 'equipment', 'fixed assets', 'pp&e', 'net pp&e',
    'building', 'land', 'leasehold', 'fixed asset schedule', 'plant',
    # Debt / term loans
    'term loan', 'senior debt', 'bank debt', 'funded debt', 'credit facility',
    'long-term debt', 'outstanding balance', 'tl-a', 'tl-b', 'capitalization',
]


def _score_window_for_collateral(window: str) -> int:
    """Score a text window for collateral-section relevance."""
    low = window.lower()
    score = sum(1 for kw in _COLLATERAL_WINDOW_KEYWORDS if kw in low)
    # Bonus for numeric density
    num_lines = sum(1 for line in window.splitlines() if any(c.isdigit() for c in line))
    if num_lines >= 5:
        score += 3
    return score


def _extract_collateral_pass(client, ocr_text: str, fy_years: tuple,
                               extracted: dict, confidences: dict, citations: dict) -> None:
    """
    Pass 4: dedicated collateral rescue pass.
    Scans the full OCR text for windows rich in balance-sheet / fixed-asset / debt content,
    then runs a short, focused LLM prompt to extract ONLY the 5 collateral fields.
    Mutates extracted/confidences/citations IN PLACE — only fills fields that are still null.
    """
    # Only run if at least one collateral field is still null
    null_fields = [f for f in _COLLATERAL_FIELDS if extracted.get(f) is None]
    if not null_fields:
        return

    fy1, fy2, fy3 = fy_years
    yy3 = str(fy3)[2:]
    logger.info(f"  Pass 4 (collateral rescue) triggered — null fields: {null_fields}")

    # Score all 1500-char windows and pick top-3
    WINDOW = 1500
    chunks = [ocr_text[i:i + WINDOW] for i in range(0, len(ocr_text), WINDOW)]
    scored = sorted(enumerate(chunks), key=lambda x: _score_window_for_collateral(x[1]), reverse=True)
    top_windows = [chunks[idx] for idx, _ in scored[:3] if _score_window_for_collateral(chunks[idx]) > 0]

    if not top_windows:
        logger.info("  Pass 4: no collateral-relevant windows found — skipping")
        return

    context = '\n\n---\n\n'.join(top_windows)

    prompt = f"""You are extracting collateral and debt values from a private equity CIM.
Most recent historical fiscal year (fy3) = {fy3} (columns: {fy3}A, Dec-{yy3}A, FY{fy3}).

Extract ONLY these 5 fields. Return null if not found — do NOT guess or infer.
All monetary values in $000s. For $M documents multiply by 1000 (e.g. $6.9M → 6900).

FIELDS TO EXTRACT:

1. net_revenue_collateral — Accounts Receivable (AR) from the BALANCE SHEET.
   Labels: "Accounts receivable", "Trade Receivables", "AR", "Net AR", "Receivables, net".
   Source: Current Assets section of balance sheet. Most recent year column.
   NOT from P&L "Net Revenue" or "Revenue" rows — those are income statement items.

2. inventory_collateral — Inventory from the BALANCE SHEET.
   Labels: "Inventory", "Inventories", "Net inventory", "Inventories, net".
   Source: Current Assets section of balance sheet. Use TOTAL inventory line.
   Do NOT use borrowing base schedule gross value.

3. me_equipment_collateral — Machinery & Equipment from FIXED ASSET SCHEDULE.
   Labels: "Machinery & Equipment", "M&E", "Equipment", "Warehouse Equipment",
   "Plant & Equipment", "Fixed Assets", "FF&E", "Manufacturing Equipment".
   Extract the MOST RECENT historical year column value.
   Values increasing across years = Gross Cost — extract it as-is.

4. building_land_collateral — Building / Land from FIXED ASSET SCHEDULE.
   Labels: "Building", "Buildings", "Land", "Real Estate", "Property",
   "Leasehold Improvements", "Land & Buildings".
   Extract the MOST RECENT historical year column value.
   Values constant across years = correct Gross Cost.

5. existing_term_loans — Outstanding term loan balance (NOT revolving credit).
   Labels: "Term Loan", "Senior Secured Term Loan", "TL-A", "TL-B",
   "Bank Debt", "Senior Debt", "Funded Debt", "First Lien Term Loan".
   SUM all tranches. EXCLUDE: revolving credit, ABL, seller notes, operating leases.

Return ONLY this JSON — no prose:
{{
  "net_revenue_collateral":   {{"value": <num|null>, "confidence": <0.0-1.0>, "citation": "<exact label + value from text>"}},
  "inventory_collateral":     {{"value": <num|null>, "confidence": <0.0-1.0>, "citation": "<exact label + value from text>"}},
  "me_equipment_collateral":  {{"value": <num|null>, "confidence": <0.0-1.0>, "citation": "<exact label + value from text>"}},
  "building_land_collateral": {{"value": <num|null>, "confidence": <0.0-1.0>, "citation": "<exact label + value from text>"}},
  "existing_term_loans":      {{"value": <num|null>, "confidence": <0.0-1.0>, "citation": "<exact label + value from text>"}}
}}

DOCUMENT SECTIONS:
{context}"""

    try:
        response_text = _call_llm(client, prompt, system_prompt=(
            "You are a financial data extractor. Return ONLY valid JSON matching the schema. "
            "null means not found — do not guess. Confidence 0.0 if uncertain."
        ))
        parsed = _parse_llm_json(response_text)
        if not isinstance(parsed, dict):
            logger.warning("  Pass 4: LLM returned non-dict — skipping merge")
            return

        # Merge: only fill fields that are currently null
        for _ckey in _COLLATERAL_FIELDS:
            if extracted.get(_ckey) is not None:
                continue  # already has a value — never overwrite
            field_data = parsed.get(_ckey)
            if not isinstance(field_data, dict):
                continue
            raw_val = field_data.get('value')
            p4_val = _coerce_numeric(raw_val)
            p4_conf = float(field_data.get('confidence') or 0.0)
            if p4_val is not None and p4_conf >= 0.60:
                extracted[_ckey] = p4_val
                confidences[_ckey] = p4_conf
                citations[_ckey] = (str(field_data.get('citation', ''))[:300] + ' [Pass4]').strip()
                logger.info(
                    f"  Pass 4 rescue: {_ckey}={p4_val:,.0f} (conf={p4_conf:.2f})"
                )
            else:
                logger.info(
                    f"  Pass 4: {_ckey} still null after rescue "
                    f"(val={raw_val}, conf={p4_conf:.2f})"
                )
    except Exception as _e:
        logger.warning(f"  Pass 4 collateral rescue failed: {_e}")


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
    focused_text = _extract_financial_sections(ocr_text, fy_years=fy_years)
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
        and (parsed1[k].get('value') is None or (parsed1[k].get('confidence') or 0) < 0.60)
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
            target_chars=MAX_CONTEXT_CHARS // 2,
            fy_years=fy_years
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
    _hist_null_count = sum(1 for _k in ['revenue_fy1', 'revenue_fy2', 'revenue_fy3']
                           if extracted.get(_k) is None)
    # Additional trigger: fy2 and fy3 revenues suspiciously close (< 3% apart).
    # Two consecutive historical fiscal years being nearly identical suggests the LLM
    # read from a projection table (where year-over-year estimates are similar) rather
    # than the historical P&L (where real operating results typically differ by > 5%).
    _rev_fy2_raw = extracted.get('revenue_fy2')
    _rev_fy3_raw = extracted.get('revenue_fy3')
    _rev_fy2_fy3_identical = (
        _rev_fy2_raw is not None and _rev_fy3_raw is not None
        and float(_rev_fy2_raw) > 0
        and abs(float(_rev_fy2_raw) - float(_rev_fy3_raw)) / max(float(_rev_fy2_raw), float(_rev_fy3_raw)) < 0.03
    )
    if _rev_fy2_fy3_identical:
        logger.info(f"  Pass 3 triggered: revenue_fy2={_rev_fy2_raw} ≈ revenue_fy3={_rev_fy3_raw} "
                    f"(< 3% apart — likely read from projection table)")
    # Additional trigger: fy1 null but fy2 and fy3 present.
    # Indicates the TWO-COLUMN DEC-NNA case fired correctly — fy2/fy3 are valid but
    # fy1 was not visible. Pass 3 uses numerically-dense windows to recover fy1.
    _fy1_only_null = (
        extracted.get('revenue_fy1') is None
        and extracted.get('revenue_fy2') is not None
        and extracted.get('revenue_fy3') is not None
    )
    if _fy1_only_null:
        logger.info(f"  Pass 3 triggered: revenue_fy1 null, fy2/fy3 present "
                    f"(fy2={extracted.get('revenue_fy2')}, fy3={extracted.get('revenue_fy3')})")
    # Additional trigger: revenue < gross_margin for any year — physically impossible in P&L.
    # Means the LLM extracted Gross Margin as Revenue (wrong row).
    # Symptom seen when window context includes GM rows before Revenue rows.
    _rev_lt_gm = False
    for _n in (1, 2, 3):
        _rev_n = extracted.get(f'revenue_fy{_n}')
        _gm_n = extracted.get(f'gross_margin_fy{_n}')
        if _rev_n is not None and _gm_n is not None and float(_gm_n) > 0:
            if float(_rev_n) < float(_gm_n) * 0.98:  # 2% tolerance for rounding
                _rev_lt_gm = True
                logger.warning(
                    f"  Pass 3 triggered: revenue_fy{_n}={_rev_n} < gross_margin_fy{_n}={_gm_n} "
                    f"— wrong row extracted (GM mistaken for Revenue)")
                break
    if extracted.get('revenue_fy3') is None or _hist_null_count >= 2 or _rev_fy2_fy3_identical or _fy1_only_null or _rev_lt_gm:
        _pass3_triggered = True
        # When triggered by _rev_lt_gm or _rev_fy2_fy3_identical, the existing revenue
        # values are provably wrong (physics violation or statistical anomaly). Clear them
        # so Pass 3 can override with correct values. The Pass 3 merge normally only fills
        # null fields — without clearing, it cannot fix non-null wrong values.
        if _rev_lt_gm or _rev_fy2_fy3_identical:
            for _clear_k in ('revenue_fy1', 'revenue_fy2', 'revenue_fy3'):
                if extracted.get(_clear_k) is not None:
                    logger.info(
                        f"  Clearing {_clear_k}={extracted[_clear_k]} "
                        f"(provably wrong — {'rev<GM' if _rev_lt_gm else 'fy2≈fy3'}) "
                        f"to allow Pass 3 override")
                    extracted[_clear_k] = None
                    confidences[_clear_k] = 0.0
                    citations[_clear_k] = ''
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

    # ── PASS 4: Dedicated Collateral Rescue ──────────────────────────────────
    # Triggers when any collateral field is still null after Pass 1+2+3.
    # Runs a focused short LLM prompt specifically targeting balance sheet,
    # fixed asset schedule, and debt sections. Fills null collateral fields only.
    _extract_collateral_pass(client, ocr_text, fy_years, extracted, confidences, citations)
    # ── END PASS 4 ───────────────────────────────────────────────────────────

    # ── Revenue magnitude sanity check ────────────────────────────────────────
    # If extracted revenue_fy3 is much smaller than proj_revenue_y1 (more than 10x),
    # the LLM likely extracted a segment sub-row instead of consolidated top-line.
    # Flag it with low confidence so the review page shows a yellow/red warning.
    _rev3 = extracted.get('revenue_fy3')
    _proj_y1 = extracted.get('proj_revenue_y1')
    if _rev3 is not None and _proj_y1 is not None and _proj_y1 > 0 and _rev3 > 0:
        if _rev3 < _proj_y1 * 0.10:
            logger.warning(
                f"  REVENUE SEGMENT-MISMATCH: revenue_fy3={_rev3:,.0f} << "
                f"proj_revenue_y1={_proj_y1:,.0f} (ratio={_rev3/_proj_y1:.2%}) "
                f"— likely a segment row was extracted instead of consolidated total"
            )
            for _yn in (1, 2, 3):
                if extracted.get(f'revenue_fy{_yn}') is not None:
                    confidences[f'revenue_fy{_yn}'] = min(
                        confidences.get(f'revenue_fy{_yn}', 0.55), 0.55
                    )
                    citations[f'revenue_fy{_yn}'] = (
                        (citations.get(f'revenue_fy{_yn}') or '') +
                        f" [WARNING: revenue_fy3={_rev3:,.0f} << proj_y1={_proj_y1:,.0f} "
                        f"— possible segment row]"
                    )

    # ── SG&A magnitude sanity check ───────────────────────────────────────────
    # If sga < 10% of revenue for the same year, it may be a segment-level figure.
    # Flag with low confidence so the review page shows a warning.
    for _n in (1, 2, 3):
        _sga = extracted.get(f'sga_fy{_n}')
        _rev = extracted.get(f'revenue_fy{_n}')
        if _sga is not None and _rev is not None and _rev > 0 and _sga > 0:
            _sga_pct = _sga / _rev
            if _sga_pct < 0.05:
                logger.warning(
                    f"  SGA fy{_n} POSSIBLE SEGMENT VALUE: sga={_sga:,.0f} "
                    f"is only {_sga_pct:.1%} of revenue={_rev:,.0f} "
                    f"— may be segment SG&A not consolidated total"
                )
                confidences[f'sga_fy{_n}'] = min(
                    confidences.get(f'sga_fy{_n}', 0.55), 0.55
                )
                citations[f'sga_fy{_n}'] = (
                    (citations.get(f'sga_fy{_n}') or '') +
                    f" [WARNING: sga={_sga:,.0f} is {_sga_pct:.1%} of revenue "
                    f"— possible segment row]"
                )

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

    # ── Cross-table contamination guard ──────────────────────────────────────
    # If gross_margin or sga is <5% of revenue for a company with >$5M revenue,
    # the LLM read these fields from a segment/subsidiary table rather than the
    # consolidated P&L.  Null them out — fill_missing_projections() will
    # auto-calculate from historical averages, which is better than segment garbage.
    # This threshold is general: <5% GM is only possible for pure commodity
    # pass-throughs; <5% SGA is impossible for any real operating company at scale.
    for _n in (1, 2, 3):
        _rev = extracted.get(f'revenue_fy{_n}')
        _gm  = extracted.get(f'gross_margin_fy{_n}')
        _sga = extracted.get(f'sga_fy{_n}')
        if _rev is not None and float(_rev) > 5000:
            if _gm is not None and float(_rev) > 0 and float(_gm) / float(_rev) < 0.05:
                logger.warning(
                    f"  Cross-table guard: gross_margin_fy{_n}={_gm} is <5% of "
                    f"revenue_fy{_n}={_rev} — nulling (wrong-table signal)"
                )
                extracted[f'gross_margin_fy{_n}'] = None
            if _sga is not None and float(_rev) > 0 and float(_sga) / float(_rev) < 0.05:
                logger.warning(
                    f"  Cross-table guard: sga_fy{_n}={_sga} is <5% of "
                    f"revenue_fy{_n}={_rev} — nulling (wrong-table signal)"
                )
                extracted[f'sga_fy{_n}'] = None

    # ── Adj. EBITDA derivation fallback ──────────────────────────────────────
    # If adj_ebitda_fyN is null after all passes but gross_margin + sga are present,
    # derive it so the review page shows a value and C26 in Excel is populated.
    for _n in (1, 2, 3):
        if extracted.get(f'adj_ebitda_fy{_n}') is None:
            gm  = extracted.get(f'gross_margin_fy{_n}')
            sg  = extracted.get(f'sga_fy{_n}')
            adj = extracted.get(f'adjustments_fy{_n}') or 0.0
            if gm is not None and sg is not None:
                derived = round(float(gm) - float(sg) + float(adj), 2)
                extracted[f'adj_ebitda_fy{_n}'] = derived
                confidences[f'adj_ebitda_fy{_n}'] = 0.70
                citations[f'adj_ebitda_fy{_n}'] = (
                    f"Derived: gross_margin({gm}) - sga({sg}) + adjustments({adj})"
                )
                logger.info(f"  Adj.EBITDA fy{_n} derived from P&L: {derived}")

    # ── Revenue ≥ Gross Margin accounting guard ───────────────────────────────
    # If gross_margin > revenue for the same year the value is from the wrong row.
    # Null it so the reverse-derivation block below can re-derive it from EBITDA.
    for _n in (1, 2, 3):
        _rev = extracted.get(f'revenue_fy{_n}')
        _gm  = extracted.get(f'gross_margin_fy{_n}')
        if _rev is not None and _gm is not None and _gm > _rev:
            logger.warning(
                f"  GM fy{_n} IMPOSSIBLE: gm={_gm:,.0f} > revenue={_rev:,.0f} "
                f"— nulling gross_margin (accounting violation)"
            )
            extracted[f'gross_margin_fy{_n}'] = None
            confidences.pop(f'gross_margin_fy{_n}', None)
            citations[f'gross_margin_fy{_n}'] = (
                f"Nulled: extracted {_gm:,.0f} > revenue {_rev:,.0f} (GM > Revenue impossible)"
            )

    # ── EBITDA cross-check at extraction time ────────────────────────────────
    # If stated Adj.EBITDA differs >5% from (GM - SGA + adj), downgrade confidence
    # to 0.60 and append a discrepancy note to citation.
    # IMPORTANT: Only run when adjustments_fyN is explicitly extracted (not null).
    # When adjustments is null, calc = GM - SGA (omits add-backs entirely) → the
    # discrepancy vs stated adj_ebitda (which includes add-backs) is always large
    # and meaningful, causing false confidence downgrades for companies with add-backs.
    for _n in (1, 2, 3):
        doc_ebitda = extracted.get(f'adj_ebitda_fy{_n}')
        gm  = extracted.get(f'gross_margin_fy{_n}')
        sg  = extracted.get(f'sga_fy{_n}')
        adj_raw = extracted.get(f'adjustments_fy{_n}')
        # Skip cross-check if adjustments is unknown — calc would exclude add-backs
        # and always look "wrong" vs. the stated adj_ebitda which includes them.
        if adj_raw is None:
            continue
        adj = float(adj_raw)
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

    # ── Adj. EBITDA / Adjustments confusion guard ─────────────────────────────
    # Root cause (Polytek v13): LLM puts adjustment add-back amounts into adj_ebitda slot.
    # Detection: adj_ebitda ≈ adjustments (within 20%) → likely swapped.
    # Fix: if reported_ebitda available, derive adj_ebitda = reported + abs(adjustments).
    for _n in (1, 2, 3):
        _adj_e  = extracted.get(f'adj_ebitda_fy{_n}')
        _adj_s  = extracted.get(f'adjustments_fy{_n}')
        _rep_e  = extracted.get(f'reported_ebitda_fy{_n}')
        if _adj_e is not None and _adj_s is not None and abs(_adj_s) > 0:
            _ratio = abs(_adj_e) / abs(_adj_s)
            if _ratio < 1.20:   # adj_ebitda ≤ 120% of adjustments = swapped
                if _rep_e is not None and _rep_e > abs(_adj_s):
                    _corrected = round(float(_rep_e) + abs(float(_adj_s)), 2)
                    logger.warning(
                        f"  EBITDA SWAP fy{_n}: adj_ebitda={_adj_e:,.0f} ≈ "
                        f"adjustments={_adj_s:,.0f} (ratio={_ratio:.2f}) — "
                        f"correcting: reported({_rep_e:,.0f}) + adj({abs(_adj_s):,.0f}) = {_corrected:,.0f}"
                    )
                    extracted[f'adj_ebitda_fy{_n}'] = _corrected
                    confidences[f'adj_ebitda_fy{_n}'] = 0.75
                    citations[f'adj_ebitda_fy{_n}'] = (
                        f"Swap-corrected: reported_ebitda({_rep_e:,.0f}) + "
                        f"abs(adjustments)({abs(_adj_s):,.0f}) = {_corrected:,.0f} "
                        f"[original adj_ebitda={_adj_e:,.0f} was ≈ adjustments — confusion fixed]"
                    )
                else:
                    logger.warning(
                        f"  EBITDA SWAP SUSPECTED fy{_n}: adj_ebitda={_adj_e:,.0f} ≈ "
                        f"adjustments={_adj_s:,.0f} — no reported_ebitda to auto-correct"
                    )
                    confidences[f'adj_ebitda_fy{_n}'] = min(
                        confidences.get(f'adj_ebitda_fy{_n}', 0.55), 0.55
                    )
        # Guard: adj_ebitda must be >= reported_ebitda (add-backs are positive)
        if _adj_e is not None and _rep_e is not None and _adj_e < _rep_e * 0.90:
            _fixed = round(float(_rep_e) + abs(float(_adj_s or 0)), 2)
            logger.warning(
                f"  EBITDA SIGN fy{_n}: adj_ebitda={_adj_e:,.0f} < "
                f"reported_ebitda={_rep_e:,.0f} — deriving {_fixed:,.0f}"
            )
            extracted[f'adj_ebitda_fy{_n}'] = _fixed
            confidences[f'adj_ebitda_fy{_n}'] = 0.75
            citations[f'adj_ebitda_fy{_n}'] = (
                f"Sign-corrected: adj({_adj_e:,.0f}) < reported({_rep_e:,.0f}) "
                f"— derived {_fixed:,.0f}"
            )

    # ── Gross Margin reverse derivation (when EBITDA discrepancy is large) ──────
    # If the stated adj_ebitda looks correct but the extracted gross_margin produces
    # a >40% EBITDA gap, the gross_margin row was pulled from the wrong table.
    # Reverse-derive: gross_margin = adj_ebitda + sga - adjustments.
    for _n in (1, 2, 3):
        doc_ebitda = extracted.get(f'adj_ebitda_fy{_n}')
        gm  = extracted.get(f'gross_margin_fy{_n}')
        sg  = extracted.get(f'sga_fy{_n}')
        adj = extracted.get(f'adjustments_fy{_n}') or 0.0
        if doc_ebitda is not None and gm is not None and sg is not None:
            calc = gm - sg + adj
            if abs(calc) > 0:
                pct_diff = abs(calc - doc_ebitda) / abs(calc)
                if pct_diff > 0.40:
                    derived_gm = round(doc_ebitda + sg - adj, 2)
                    if derived_gm > 0:
                        extracted[f'gross_margin_fy{_n}'] = derived_gm
                        confidences[f'gross_margin_fy{_n}'] = 0.70
                        citations[f'gross_margin_fy{_n}'] = (
                            f"Reverse-derived: adj_ebitda({doc_ebitda}) + sga({sg})"
                            f" - adjustments({adj}) = {derived_gm}"
                            f" [original {gm} discarded — {pct_diff:.0%} EBITDA gap]"
                        )
                        logger.info(
                            f"  GM fy{_n} reverse-derived: {gm} → {derived_gm}"
                            f" (EBITDA gap was {pct_diff:.0%})"
                        )

    # ── proj_gross_margin invalidation when hist GM was reverse-derived ──────────
    # If any historical gross_margin was corrected by reverse-derivation, all the
    # proj_gross_margin_y{i} values come from the SAME wrong row and must be discarded.
    # fill_missing_projections() will then use the corrected historical GM% instead.
    _gm_was_corrected = any(
        'Reverse-derived' in (citations.get(f'gross_margin_fy{_n}') or '')
        for _n in (1, 2, 3)
    )
    if _gm_was_corrected:
        _corr_gms = [
            extracted.get(f'gross_margin_fy{_n}')
            for _n in (1, 2, 3)
            if extracted.get(f'gross_margin_fy{_n}') is not None
        ]
        _max_corr_gm = max(_corr_gms) if _corr_gms else 0
        for _i in range(1, 6):
            _pgm = extracted.get(f'proj_gross_margin_y{_i}')
            if _pgm is not None and _max_corr_gm > 0 and _pgm > _max_corr_gm * 3:
                logger.warning(
                    f"  proj_gross_margin_y{_i}={_pgm:,.0f} discarded "
                    f"(>3× corrected hist GM {_max_corr_gm:,.0f} — same wrong row)"
                )
                extracted[f'proj_gross_margin_y{_i}'] = None
                citations[f'proj_gross_margin_y{_i}'] = (
                    f"Nulled: {_pgm:,.0f} >3× corrected hist GM — from same wrong row"
                )

    # ── HARD GUARD: adj_ebitda_fy3 projection contamination (>200% discrepancy) ──
    # If reported_ebitda + adjustments gives a VERY different value, the LLM read
    # from a projection column. Auto-correct to calculated value.
    _stated_ebitda = extracted.get('adj_ebitda_fy3')
    _reported_e3 = extracted.get('reported_ebitda_fy3')
    _adjustments_e3 = extracted.get('adjustments_fy3')
    if (_stated_ebitda is not None and _reported_e3 is not None
            and _adjustments_e3 is not None):
        _calc_ebitda = _reported_e3 + _adjustments_e3
        if _calc_ebitda != 0 and abs(_stated_ebitda - _calc_ebitda) / abs(_calc_ebitda) > 2.0:
            _delta_pct = abs(_stated_ebitda - _calc_ebitda) / abs(_calc_ebitda) * 100
            logger.warning(
                f"  adj_ebitda_fy3 HARD GUARD: stated={_stated_ebitda:,.0f}, "
                f"calculated={_calc_ebitda:,.0f}, delta={_delta_pct:.0f}% > 200%"
                f" — forcing to calculated value (projection contamination)"
            )
            extracted['adj_ebitda_fy3'] = _calc_ebitda
            confidences['adj_ebitda_fy3'] = 0.70
            existing_cite = citations.get('adj_ebitda_fy3', '')
            citations['adj_ebitda_fy3'] = (
                existing_cite + f' [AUTO-CORRECTED: stated {_stated_ebitda:,.0f} was '
                f'projection contamination; using reported({_reported_e3:,.0f}) + '
                f'adjustments({_adjustments_e3:,.0f}) = {_calc_ebitda:,.0f}]'
            ).strip()

    # ── GUARD: AR cannot equal revenue_fy3 exactly (P&L confusion) ────────────
    # If LLM returned revenue as AR, null it out — AR is a balance sheet field.
    _ar_val = extracted.get('net_revenue_collateral')
    _rev3_val = extracted.get('revenue_fy3')
    if _ar_val is not None and _rev3_val is not None and _ar_val == _rev3_val:
        logger.warning(
            f"  net_revenue_collateral GUARD: AR ({_ar_val:,.0f}) = revenue_fy3 "
            f"({_rev3_val:,.0f}) exactly — LLM confused P&L revenue with balance "
            f"sheet AR. Nulling AR."
        )
        extracted['net_revenue_collateral'] = None
        confidences['net_revenue_collateral'] = 0.0
        existing_cite = citations.get('net_revenue_collateral', '')
        citations['net_revenue_collateral'] = (
            existing_cite + ' [NULLED: value identical to revenue_fy3 — likely P&L confusion]'
        ).strip()

    # ── adj_ebitda CONFIDENCE GUARD ──────────────────────────────────────────
    # adj_ebitda_fy3 drives the Term Loans fallback (adj_ebitda × leverage_multiple).
    # A wrong fy3 value directly corrupts Sources. Apply strict 0.70 threshold.
    # adj_ebitda_fy1/fy2 only affect historical display — standard 0.60 threshold applies.
    for _n in (1, 2, 3):
        _key = f'adj_ebitda_fy{_n}'
        _threshold = 0.70 if _n == 3 else 0.60  # strict threshold only for fy3
        if extracted.get(_key) is not None and confidences.get(_key, 0) < _threshold:
            logger.warning(
                f"  adj_ebitda_fy{_n} CONFIDENCE GUARD: confidence="
                f"{confidences.get(_key, 0):.2f} < {_threshold} — nulling uncertain value "
                f"({extracted[_key]:,.0f})"
                + ("; calculator will derive from reported+adjustments." if _n == 3 else ".")
            )
            extracted[_key] = None
            confidences[_key] = 0.0

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
    # Null out any proj value that duplicates (or nearly matches) a known historical value.
    # Near-match tolerance: 1% — catches rounding artifacts (e.g. 99086 vs 99086.0).
    hist_rev_set = {v for v in [rev1, rev2, rev3] if v is not None}
    hist_gm_set  = {v for v in [gm1, gm2, gm3]   if v is not None}
    hist_sg_set  = {v for v in [sg1, sg2, sg3]    if v is not None}

    def _is_near_hist(val, hist_set, tol=0.01):
        """Return True if val is within tol% of any historical value."""
        if val is None:
            return False
        for h in hist_set:
            if h and h > 0 and abs(val - h) / h <= tol:
                return True
        return False

    def _dedup(ocr_list, hist_set):
        result = []
        for v in ocr_list:
            num = _coerce_numeric(v)
            if num is not None and _is_near_hist(num, hist_set):
                logger.info(f'  [DEDUP] Projection value {num} matches a historical value — nulling (contamination guard)')
                result.append(None)
            else:
                result.append(v)
        return result

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
        base_revenue = None
        base_years_back = 0
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
            logger.warning('[PROJ] WARNING: all historical revenues null — returning empty projections')
            return {}, 'manual_required'
        avail_revs = [r for r in [rev1, rev2, rev3] if r and r > 0]
        if len(avail_revs) >= 2:
            oldest = avail_revs[0]
            newest = avail_revs[-1]
            n_years = len(avail_revs) - 1
            raw_cagr = (newest / oldest) ** (1 / n_years) - 1
        else:
            raw_cagr = 0.05
        cagr = max(-0.20, min(0.50, raw_cagr))
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

    # ── Term loan: straight-line amortization over 5 years ───────────────────
    # If existing_term_loans is known, auto-populate declining balance (Y1=4/5, Y2=3/5… Y5=0)
    # so Excel row 32 (L–P) reflects a realistic debt paydown schedule.
    # If unknown, leave blank for manual input.
    _term_loan_bal = _f('existing_term_loans')
    if _term_loan_bal and _term_loan_bal > 0:
        _annual_repay = round(_term_loan_bal / 5, 2)
        for i in range(1, 6):
            remaining = round(_term_loan_bal - _annual_repay * i, 2)
            proj[f'term_loan_y{i}'] = max(remaining, 0)
        logger.info(f"  Projections: term loan straight-line amortization "
                    f"(balance={_term_loan_bal:,.0f}, annual repay={_annual_repay:,.0f})")
    else:
        for i in range(1, 6):
            proj[f'term_loan_y{i}'] = None

    # ── Y4/Y5 GM + SGA derivation when still null ────────────────────────────────
    # Covers the case where document provides Y1-Y3 projections but not Y4/Y5.
    # Derive from trailing average % using best available projection + historical data.
    def _avg_pct(nums, denoms):
        ratios = [n / d for n, d in zip(nums, denoms)
                  if n is not None and d is not None and d > 0]
        return sum(ratios) / len(ratios) if ratios else None

    # Prefer OCR Y1-Y3 for the trailing %; fall back to historical FY1-3
    _gm_num  = [proj.get(f'gross_margin_y{i}') or extracted.get(f'gross_margin_fy{i}')
                for i in range(1, 4)]
    _rev_den = [proj.get(f'revenue_y{i}') or extracted.get(f'revenue_fy{i}')
                for i in range(1, 4)]
    _sg_num  = [proj.get(f'sga_y{i}') or extracted.get(f'sga_fy{i}')
                for i in range(1, 4)]

    _avg_gm_pct  = _avg_pct(_gm_num, _rev_den)
    _avg_sga_pct = _avg_pct(_sg_num, _rev_den)

    for _i in (4, 5):
        _rev_yi = proj.get(f'revenue_y{_i}')
        if not _rev_yi or _rev_yi <= 0:
            continue
        if proj.get(f'gross_margin_y{_i}') is None and _avg_gm_pct is not None:
            proj[f'gross_margin_y{_i}'] = round(_rev_yi * _avg_gm_pct)
            logger.info(f"  [PROJ] gross_margin_y{_i} derived: "
                        f"{_rev_yi:,.0f} × {_avg_gm_pct:.1%} = {proj[f'gross_margin_y{_i}']:,.0f}")
        if proj.get(f'sga_y{_i}') is None and _avg_sga_pct is not None:
            proj[f'sga_y{_i}'] = round(_rev_yi * _avg_sga_pct)
            logger.info(f"  [PROJ] sga_y{_i} derived: "
                        f"{_rev_yi:,.0f} × {_avg_sga_pct:.1%} = {proj[f'sga_y{_i}']:,.0f}")

    # ── CapEx projections: pass through from OCR extraction ──────────────────
    # No CAGR fallback — if not in document, leave null (better than a wrong default).
    for _i in range(1, 6):
        _cap = extracted.get(f'proj_capex_y{_i}')
        if _cap is not None:
            proj[f'capex_y{_i}'] = _coerce_numeric(_cap)

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
