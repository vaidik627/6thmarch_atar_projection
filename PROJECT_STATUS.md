# Atar Capital — Prebid Analysis System
## Project Status & Architecture Reference
**Last Updated:** 2026-03-11 (session 5 — Sources section collateral extraction fixes)
> Give this file to Claude (web or API) so it can understand the full project and make accurate modifications.

---

## 1. What This Project Does

A Flask web application for Atar Capital's private equity deal team. It:
1. Accepts a **PDF deal memo / CIM** (up to 100 pages)
2. Runs **Google Document AI OCR** to extract text
3. Sends OCR text to **NVIDIA NIM (GPT-OSS-120B)** to extract ~40 financial fields
4. Shows a **Review page** where the analyst verifies and corrects extracted values
5. Runs a **full calculation engine** replicating 45+ Excel formulas from Atar's Prebid V31 template
6. Shows an **Analysis page** with Sources & Uses, financial projections (3 historical + 5 forecast years), validation checks, and AI-generated risk analysis
7. **Exports a filled Excel** (Prebid V31 Template.xlsx) with all inputs pre-filled and formula cells protected

---

## 2. Tech Stack

| Component | Technology |
|---|---|
| Backend | Flask 3.0.3 (Python 3.13), port 5000 |
| OCR | Google Document AI (project: gothic-context-477515-s3, processor: 3cf93b5e642f6323) |
| LLM | NVIDIA NIM `openai/gpt-oss-120b` via OpenAI-compatible API at integrate.api.nvidia.com |
| Frontend | Bootstrap 5.3 + Bootstrap Icons + Vanilla JS |
| State | Flask session only — no database |
| PDF splitting | pypdf (splits into 14-page chunks for Document AI 15-page limit) |
| Excel | openpyxl — fills `Prebid V31  Template.xlsx` |
| Config | python-dotenv (.env file — never read or commit) |

---

## 3. Project File Structure

```
project_root/
├── app.py                          # Flask routes + template filters
├── CLAUDE.md                       # Claude Code instructions (DO NOT MODIFY)
├── PROJECT_STATUS.md               # This file
├── Prebid V31  Template.xlsx       # Excel template (DO NOT MODIFY)
├── requirements.txt                # Pinned dependencies (DO NOT MODIFY)
├── .env                            # Credentials (NEVER read or commit)
├── uploads/                        # Temp PDF uploads
├── storage/extractions/            # JSON debug logs per session
├── services/
│   ├── __init__.py
│   ├── ocr_service.py              # Google Doc AI — stable, do not touch
│   ├── llm_service.py              # LLM extraction, FY detection, projections, risk
│   ├── calculator.py               # All 45+ Excel formula replicas
│   ├── validator.py                # Anti-hallucination validation rules
│   └── excel_export.py            # Fills Excel template, returns BytesIO
├── templates/
│   ├── index.html                  # Upload page (stable, do not touch)
│   ├── review.html                 # Step 2: review extracted + manual inputs
│   └── analysis.html              # Step 3: full analysis display
└── static/
    ├── css/style.css               # All custom styles
    └── js/main.js                  # Client-side JS (stable, do not touch)
```

---

## 4. File-by-File Details

### `app.py` — Flask Routes

| Route | Method | Purpose |
|---|---|---|
| `/` | GET | Upload page |
| `/upload` | POST | OCR → LLM → Validate → Projections → session → redirect /review |
| `/review` | GET | Show review form with extracted fields |
| `/calculate` | POST | Validate manual inputs → run calculations → risk analysis → redirect /analysis |
| `/analysis` | GET | Show full analysis |
| `/export` | GET | Generate and download filled Excel |
| `/reset` | GET | Clear session |
| `/verify/ocr` | GET | Live OCR credentials check (JSON) |
| `/verify/llm` | GET | Live NVIDIA NIM ping (JSON) |

**Helpers registered in app.py:**
- `fmt_num` — Jinja filter: float with comma + 2 decimal places (`5200.00`)
- `format_number` — Jinja filter: integer with commas (`5,200`)
- `safe_float(val)` — converts any form string to `float|None`; strips commas; returns `None` for empty/null/blank (never returns `0` for a missing value)
- `_STRING_FORM_FIELDS` — set of keys that stay as strings: `{'company_name', 'proj_source'}`

**`/calculate` form data flow:**
```python
form_data_raw = request.form.to_dict()      # all strings
form_data = {k: safe_float(v) for k, v in form_data_raw.items()
             if k not in _STRING_FORM_FIELDS}  # typed: float|None
# Smart merge: only non-None form values override extracted LLM values.
# Empty form fields keep the original extracted value.
all_inputs = dict(session['extracted'])
for k, v in form_data.items():
    if v is not None:
        all_inputs[k] = v
```

**Session keys stored during `/upload`:**
```python
session['session_id']          # 8-char UUID prefix
session['extracted']           # dict of ~40 extracted fields + _data_present flags
session['confidences']         # dict field → float 0.0–1.0
session['citations']           # dict field → quoted source text
session['validation_flags']    # dict field → {status, message}
session['pdf_filename']        # original filename
session['projections']         # 5-year projection dict (revenue_y1..y5, etc.)
session['proj_source']         # 'ocr' or 'calculated'
session['detected_fy_years']   # [y1, y2, y3] integers from document
```

---

### `services/llm_service.py` — LLM Extraction

**Key functions:**

#### `_detect_fiscal_years(ocr_text) → (y1, y2, y3)`
Detects 3 consecutive historical fiscal years from the document.
- **Pass 1 (PRIMARY)**: Table-header lines where ≥2 non-projection years appear together
- **Pass 2 (SECONDARY)**: Frequency ≥2 mentions, excludes projection-suffix years
- **Pass 3 (LAST RESORT)**: Single-mention scan within a 7-year window of current year (catches OCR layouts where years appear on separate lines)
- **HARD CAP**: years > current calendar year excluded
- **Projection exclusion**: years with E/F/B/P suffix (e.g. FY2025E, 2024P, 2026F) are excluded
- **Final assignment**: Always consecutive — `y3 = most_recent`, `y2 = y3-1`, `y1 = y3-2`
  - Example: document has FY2020–FY2024 → fy3=2024, fy2=2023, fy1=2022

#### `_build_extraction_prompt(text, fy_years) → str`
Builds the per-call extraction prompt. Structured into 6 sections (A–F):
- **A — FISCAL YEAR MAPPING**: Injects y1/y2/y3 with full+short-form aliases (`FY{yy}`, `FY'{yy}`, `'{yy}`, `{y}A`, `{y} Actual`); 4-step COLUMN ASSIGNMENT RULE (header-match, not positional); COLUMNS TO IGNORE list (LTM, TTM, etc.); RESTATED priority order
- **B — REVENUE IDENTIFICATION**: Forbidden sub-revenue labels, selection rule (highest value wins), position rule (appears before COGS)
- **C — FORMULA DERIVATION CHAIN**: adj_ebitda = GM − SGA + adjustments; EBITDA cross-check
- **D — PROJECTION EXTRACTION RULES**: Temporal guard — projection years must be > fy3
- **E — ANTI-HALLUCINATION SELF-CHECK**: 10 items (includes: no positional assignment, no LTM/TTM, restated check, contamination guard)
- **F — OUTPUT JSON SCHEMA**: Hardcoded with detected year integers as `fy_year_1/2/3` values

#### `_extract_financial_sections(ocr_text, target_chars) → str`
Condenses OCR text to most financially-relevant sections (up to 30k chars).
- **WINDOW = 1500 chars** per scoring chunk (captures full P&L table rows)
- **Dual-axis scoring:**
  - Axis 1 — keyword density: count of finance keywords per window
  - Axis 2 — numeric table density: lines with 3+ numbers → +3 each; 2 numbers → +1 each
  - Dense-block bonus: if ≥8 lines with any number in the window → +5 (catches one-value-per-line OCR format)
- **Force-include**: window with highest pure numeric score is always included (ensures actual P&L table is present even when narrative prose scores higher)
- Combined score = kw_score + numeric_score; windows ranked descending

#### `_unpack_extraction(parsed) → (extracted, confidences, citations)`
Helper that unpacks a parsed LLM JSON dict into three separate dicts.
Used by Pass 3 to merge additional results into the main extraction dicts.
Only processes fields present in `EXTRACTION_SCHEMA`.

#### `extract_financial_fields(ocr_text, session_id) → (extracted, confidences, citations, fy_years)`
Main extraction function:
- Runs `_detect_fiscal_years()`
- Builds year-anchored system prompt via `_build_system_prompt(y1, y2, y3)`
- **Pass 1**: condenses OCR to 30k chars via `_extract_financial_sections()`, calls LLM
- **Pass 2**: if fields null/low-confidence AND text > 30k chars, re-runs on second half of doc
- Merges passes (higher confidence wins)
- **Pass 3**: if `revenue_fy3` is still null after Pass 1+2 — scans full OCR in 1500-char windows ranked by numeric density, takes top-3, calls LLM again; merges conservatively (Pass 3 fills null slots only)
- Unpacks JSON into `extracted`, `confidences`, `citations` dicts
- **ENH-1**: Derives `gross_margin_fyN = revenue_fyN - cogs_fyN` if GM is null but COGS present
- **EBITDA cross-check**: if stated adj_ebitda differs >5% from (GM − SGA + adj), downgrades confidence to 0.60 and appends discrepancy note to citation
- Sets `_data_present_fy1/2/3` flags (True if any of revenue/GM/SGA non-null/non-zero)
- Saves enhanced debug JSON to `storage/extractions/{session_id}_extraction.json`
  - Includes: `detected_fy_years`, `pass2_triggered`, `pass3_triggered`, `fields_null_after_merge`, per-year revenue snapshots
- **BUG-CONFIDENCE-NULL fix**: `_merge_extractions` and confidence unpacking use `(val.get('confidence') or 0)` — handles `"confidence": null` from LLM

#### `fill_missing_projections(extracted) → (projections_dict, source)`
Builds Y1–Y5 projections:
1. Uses `proj_revenue_y1..y5` from LLM if document had explicit forecasts
2. De-contamination guard: nulls any proj value that exactly matches a historical value (catches LLM assigning historical to projection fields)
3. Auto-calculates from CAGR/averages if no OCR projections found
- **Robust CAGR base**: fallback chain `rev3 → rev2 → rev1`; tracks `base_years_back` offset to correctly compound from whichever year is available; returns `{}, 'manual_required'` if all three are null
- Revenue CAGR capped at -20% to +50%
- GM%: average of historical
- SGA%: average of historical
- Interest: flat from most recent historical year
- Adjustments: 0 (non-recurring by definition)
- Term loan: blank (user inputs manually)

#### `generate_risk_analysis(all_inputs, results) → list`
Generates 6 PE deal risks from financial summary.
Each risk: `{risk, category, source ('memo'|'general'), confidence, citation}`
Categories: Financial, Operational, Market, Legal, Integration, Leverage

**EXTRACTION_SCHEMA** — Fields the LLM extracts:
```
company_name, fy_year_1/2/3,
revenue_fy1/2/3, gross_margin_fy1/2/3, cogs_fy1/2/3,
sga_fy1/2/3, interest_expense_fy1/2/3, adjustments_fy1/2/3,
adj_ebitda_fy1/2/3,
net_revenue_collateral, inventory_collateral,
me_equipment_collateral, building_land_collateral, existing_term_loans,
proj_revenue_y1..y5, proj_gross_margin_y1..y5, proj_sga_y1..y5
```
All monetary values in **$000s** (thousands USD). LLM converts $M → ×1000, $B → ×1,000,000.

**SYSTEM_PROMPT** — 10 rules (static constant + `_build_system_prompt(y1,y2,y3)` f-string version with actual year integers injected):
1. NULL OVER GUESS — confidence < threshold → return null
2. MONETARY UNITS — all values in $000s
3. REVENUE = CONSOLIDATED TOP-LINE ONLY — not segments or divisions
4. OUTPUT FORMAT — JSON only, no commentary
5. CONFIDENCE THRESHOLDS — <0.70 → null; 0.70–0.89 → probable; ≥0.90 → explicit
6. FORMULA DERIVATION CHAIN — derive adj_ebitda from GM − SGA + adjustments
7. YEAR ANCHOR — accept full-form (FY2024, FY 2024, FY-2024) AND short-form (FY24, FY'24, '24, 2024A, 2024 Actual, 2024 Audited)
8. COLUMN ASSIGNMENT BY HEADER MATCH, NOT POSITION — 4-step: scan headers, match by year label, assign per field, flag ambiguity
9. IGNORE NON-FISCAL COLUMNS — LTM, TTM, NTM, Run-Rate, Pro Forma, PF, Combined, Q1–Q4 are not historical fiscal years
10. RESTATED vs AS-REPORTED priority — Restated > Revised > Adjusted > As Reported > As Filed

---

### `services/calculator.py` — Calculation Engine

Replicates Prebid V31 Template.xlsx formulas. All values in $000s.

**Numeric helpers at top of file:**
- `_f(val, default=0.0)` — safe float; returns `default` for missing/empty; **strips commas** so `'23,500'` → `23500.0` (not `0`)
- `safe_num(val, default=None)` — like `_f()` but returns `None` for missing (not `0`); use when absence should propagate rather than silently zero out

**Input keys consumed** (from `all_inputs = {**extracted, **form_data}`):

| Category | Keys |
|---|---|
| Sources collateral | `net_revenue_collateral`, `inventory_collateral`, `me_equipment_collateral`, `building_land_collateral`, `existing_term_loans`, `seller_note`, `earnout`, `equity_roll_from_seller` |
| Advance rates | `net_revenue_multiplier` (0.75), `inventory_multiplier` (0.70), `me_equipment_multiplier` (0), `building_land_multiplier` (0.50) |
| Deal terms | `transaction_fees_total`, `working_capital_change` (7), `cfads_factor` (1), `acquisition_multiple` (7.0), `pct_acquired` (1.0) |
| Rate params | `capex_pct_availability` (0.30), `depreciation_rate` (0.045), `mgmt_ltip_rate` (0.055), `atar_ownership_rate` (0.05), `return_of_equity_years` (3), `atar_repayment_years` (4), `lp_pct` (0.03), `preferred_pct` (0.05), `fccr_rate` (0.08), `remaining_cash_pct` (0.75) |
| Transaction fees | `debt_sourcing_rate` (0.0075), `lawyers_rate` (0.0075), `qof_e_diligence`, `tax_fee`, `rw_insurance`, `atar_bonuses_senior`, `atar_bonuses_junior`, `project_other` |
| Historical P&L | `revenue_fy1/2/3`, `gross_margin_fy1/2/3`, `sga_fy1/2/3`, `interest_expense_fy1/2/3`, `adjustments_fy1/2/3` |
| Projections | `revenue_y1..y5`, `gross_margin_y1..y5`, `sga_y1..y5`, `adjustments_y1..y5`, `interest_expense_y1..y5`, `term_loan_y1..y5` |

**Key results dict keys:**
```python
# Transaction Fees
tf_debt_sourcing, tf_lawyers, tf_qofe, tf_tax, tf_rw,
tf_senior, tf_junior, tf_project, tf_total

# Sources (computed)
E7 (net revenue × multiplier), E8 (inventory × mult), E9 (M&E),
E11 (building), E12 (term loans), E14 (seller note), E16 (earnout), E18 (equity roll)
total_sources

# Uses (computed)
E30 (debt service), E31 (interest), E32 (ABL/tx fees), E34 (total uses)
balance_check (= E34 - total_sources, should be near 0)

# LP MOIC Summary
C29 (MOIC), C41 (Mgmt LTIP), C42 (Total Debt Service),
C43, C44 (LP), C45 (GP), C47 (Preferred), C48 (Total),
C49 (Revolver balance), C52 (FCCR $), C53_left, C54_left, C55_left,
C57_fccr (FCCR Ratio — target ≥ 1.0)

# Projections (dicts with keys FY1/FY2/FY3/Y1..Y5)
revenue, growth_rate, gross_margin, gm_pct, sga,
operating_income, adjustments, adj_ebitda, ebitda_pct,
interest_expense, depreciation, availability, capex, total_uses_proj,
abl_proj, term_proj, ev_at_exit, plus_cash, lp_gp_split,
earnout_payments, dscr (target ≥ 1.2 green, ≥ 1.0 yellow, < 1.0 red)
running_balance
```

---

### `services/validator.py` — Anti-Hallucination Validation

Runs after extraction. Returns `validation_flags` dict: `field → {status, message}`.

**Checks performed:**
- Revenue: range $100K–$10B, confidence thresholds
- Gross Margin: must be ≤ revenue, positive
- SG&A: must be positive
- Interest: must be 0 or positive
- YoY revenue growth: flags if outside -50% to +500%
- ENH-2: Adj. EBITDA cross-check — flags >5% discrepancy between LLM-stated and calculated (GM - SGA + adj)

**Confidence thresholds:**
- ≥ 0.85: High (green badge)
- 0.70–0.84: Medium (yellow badge, "review recommended")
- < 0.70: Low (red badge, "please verify")

**None-safety:** all `confidences.get(key)` reads use `or 0.0` pattern — handles the case where LLM returns `"confidence": null` (key exists with `None` value, so `.get(key, 0.0)` default is NOT used). `get_confidence_class()` guards against `None` input.

---

### `services/excel_export.py` — Excel Export

Loads `Prebid V31  Template.xlsx`, fills ~70 INPUT cells, leaves formula cells untouched.

**Cell mapping:**
| Cell | Content |
|---|---|
| A1 | `Project {company_name}` |
| C6 | Today's date |
| I4/J4/K4 | FY year labels (fy1/fy2/fy3) |
| C7–C18 | Collateral values (sources) |
| D7–D11 | Advance rate multipliers |
| C26 | EBITDA (adj_ebitda_fy3 if available, else GM-SGA+adj) |
| C27 | Acquisition multiple |
| C28 | % acquired |
| C40 | Exit multiple (fixed at 6) |
| H26–H46 | Rate parameters |
| A44/A47/A52/A54 | LP%, preferred%, FCCR rate, remaining cash% |
| I7:K21 | Historical P&L (FY1/2/3) — uses `_fopt()` (skips write if null, avoids DIV/0) |
| L7:P32 | Projections (Y1–Y5) — uses `_fopt()` |
| Transaction Fees tab | D6, D8, E10–E15 |

**Sheet protection:** Input cells marked `locked=False`, Sheet1 protected (formula cells read-only but navigable). No password — accident prevention only.

---

### `templates/review.html` — Step 2 Review Page

**Jinja2 macro** `render_extracted_field(field_key, ...)`:
- Shows confidence badge (green/yellow/red/manual)
- Editable `<input>` pre-filled with extracted value
- Validation flag message below input
- Citation snippet from LLM

**Sections:**
1. Extracted Historical P&L — company name, FY labels, revenue/GM/SGA/interest/adjustments
2. Collateral & Sources — net revenue collateral (defaults to revenue_fy3), inventory, M&E, building, term loans
3. 5-Year Projections — revenue/GM/SGA/interest/adjustments/term loan for Y1–Y5, with OCR/Calculated badge
4. Deal Terms — acquisition_multiple (default 7.0), pct_acquired (default 1.0)
5. Manual Inputs — Sources section (seller note, earnout, equity roll + advance rates), Uses (debt service, working capital, CFADS), Transaction Fees, Rate Parameters

---

### `templates/analysis.html` — Step 3 Analysis Page

**Sections:**
1. Nav bar — New Analysis, Export Excel, Print
2. Header badges — FCCR pass/fail, Sources=Uses balance check
3. Key Metrics row — MOIC, Total Uses, Mgmt LTIP, Total Debt Svc, Revolver Balance, FCCR Ratio
4. Sources & Uses tables
5. Transaction Fees detail
6. LP MOIC Summary
7. **Tale of the Tape** — full 8-column projection table (FY1/2/3 + Y1–Y5), rows: Revenue, Growth%, GM, GM%, SGA, Operating Income, Adjustments, Adj.EBITDA, EBITDA%, Interest, Depreciation, Availability, CAPEX, ABL, Term Repayment, EV at Exit, +Cash, LP/GP Split, Earnout Payments, Running Balance, DSCR
8. Validation Summary — 6 pass/fail checks
9. Risk Analysis — 6 LLM-generated risk factors table (category badge, memo/general source, confidence bar, citation)

---

## 5. Data Flow (End-to-End)

```
PDF Upload
    │
    ▼
Google Document AI OCR
    │ (14-page chunks → concatenated text with [PAGES x-y of N] markers)
    ▼
_detect_fiscal_years(ocr_text)
    │ → (y1, y2, y3) — 3 consecutive historical years from document
    ▼
_extract_financial_sections(ocr_text)
    │ → 30k chars via dual-axis scoring (keyword density + numeric table density)
    │   Force-includes window with highest numeric score (WINDOW=1500)
    ▼
NVIDIA NIM LLM (Pass 1)
    │ → JSON with ~40 fields {value, confidence, citation}
    ▼
[Pass 2 if needed: low-conf fields re-extracted from second half of doc]
    ▼
[Pass 3 if revenue_fy3 still null: top-3 numerically-dense windows from full OCR]
    ▼
_parse_llm_json() → regex fallback if JSON truncated
    ▼
Unpack → extracted{}, confidences{}, citations{}
    │
    ├─ ENH-1: COGS fallback (GM = rev - COGS if GM null)
    ├─ EBITDA cross-check (confidence downgrade if >5% discrepancy)
    ├─ Set _data_present_fy1/2/3 flags
    └─ Save enhanced debug JSON to storage/extractions/
    ▼
validate_extracted_fields() → validation_flags{}
    ▼
fill_missing_projections() → projections{}, proj_source
    ▼
[User reviews, corrects, submits form]
    ▼
validate_manual_inputs(form_data) → manual_errors{}
    ▼
all_inputs = {**extracted, **form_data}
    ▼
run_calculations(all_inputs) → results{}
    ▼
generate_risk_analysis(all_inputs, results) → risk_analysis[]
    ▼
Render analysis.html
    │
    └─ [Optional] generate_excel() → download filled Prebid V31 Template.xlsx
```

---

## 6. All Monetary Values

- **All amounts in $000s (thousands of USD)**
- `$5.2M` → `5200` | `$4,800,000` → `4800` | `$500K` → `500` | `$1B` → `1000000`
- LLM converts on extraction; calculator and UI assume $000s throughout

---

## 7. Fiscal Year Detection Rules

The `_detect_fiscal_years()` function uses 3-pass detection:

1. **Primary** — table-header lines (≥2 years on same line = confirmed column headers)
2. **Secondary** — frequency ≥2 anywhere in document (excludes projection-suffix years: E/F/B/P)
3. **Last resort** — any non-projection year within 7 years of current calendar year (handles OCR where each year is on its own line)

**Always returns 3 consecutive years:**
- `y3 = most_recent_found`, `y2 = y3-1`, `y1 = y3-2`
- Example: doc has FY2020–FY2024 → **fy1=2022, fy2=2023, fy3=2024** (ignores older columns)

**Projection exclusion:**
- Years with suffix E/F/B/P (e.g. `FY2025E`, `2024P`, `FY2026F`)
- Years appearing inside a "Management Forecast" / "Budget" / "Financial Projections" section header

---

## 8. Key Business Rules (Do Not Break)

1. **Revenue = largest consolidated top-line** — never segment/division/geographic sub-totals
2. **GM% can be 0** (pass-through model) — never break on this
3. **SGA can be 0** (service company) — code must not fail
4. **adj_ebitda can be negative** — never use negative as PE valuation basis in Excel
5. **Never use `if val` for numeric checks** — use `if val is not None` (0 is valid)
6. **3 historical years may not all be populated** — handle missing FY1 or FY2 gracefully
7. **Confidence < 0.70** → LLM returns null, never guesses

---

## 9. Run Instructions

```bash
cd "c:\Users\vaidi\Desktop\Antigravity_claude_project_atar"
python app.py
```
App runs at **http://localhost:5000**

---

## 10. Allowed Modifications

| File | What it controls |
|---|---|
| `services/llm_service.py` | LLM schema, prompts, FY detection, projection logic |
| `services/calculator.py` | Excel formula replicas |
| `services/validator.py` | Validation rules |
| `app.py` | Routes, session management, step logging |
| `templates/review.html` | Review page, form fields |
| `templates/analysis.html` | Analysis output display |
| `services/excel_export.py` | Excel template cell mapping |
| `static/css/style.css` | Visual styles only |

**Never modify:** `services/ocr_service.py`, `templates/index.html`, `requirements.txt`, `.env`, `static/js/main.js`, `Prebid V31  Template.xlsx`

---

## 11. Completed Feature List

- [x] Google Document AI OCR with 14-page chunking + retry
- [x] NVIDIA NIM LLM extraction with 2-pass + regex JSON fallback
- [x] Fiscal year detection — table-header anchored, 3-pass, always consecutive years
- [x] Revenue extraction hardening — forbidden sub-revenues, selection/position rules
- [x] COGS fallback — derives GM = revenue - COGS when GM row absent
- [x] Adj. EBITDA extraction + cross-check validation
- [x] 5-year projections — OCR-extracted if document has forecasts, else CAGR auto-calculated
- [x] Projection de-contamination guard — nulls proj values matching historical values
- [x] Anti-hallucination validation (10 rules, confidence thresholds)
- [x] Full calculation engine — 45+ Prebid V31 formulas replicated
- [x] Review page — editable extracted fields with confidence badges + citations
- [x] Analysis page — full Tale of the Tape, Sources/Uses, LP MOIC Summary, validation checks
- [x] Risk Analysis — 6 LLM-generated PE deal risks with memo/general classification
- [x] Excel export — fills Prebid V31 Template.xlsx with ~70 input cells, formula cells protected
- [x] Excel scenario modeling — input cells unlocked, formulas auto-recalculate
- [x] Deal terms — acquisition_multiple and pct_acquired on review form
- [x] AR collateral default — net_revenue_collateral pre-fills with revenue_fy3
- [x] Transaction date in Excel — uses today's date
- [x] Step-by-step terminal logging for all stages
- [x] Live OCR/LLM connection verify endpoints
- [x] LLM prompt robustness v2 — 4 real-world PDF layout gaps (G-1 short-form year labels, G-2 4+ column tables, G-3 LTM/TTM columns, G-4 Restated vs As-Reported)
- [x] SYSTEM_PROMPT expanded from 6 → 10 rules; `_build_system_prompt()` updated with `yy1/yy2/yy3` 2-digit aliases
- [x] Watchdog fix — `use_reloader=False` in `app.py` prevents Flask double-start on Windows
- [x] BUG-CONFIDENCE-NULL (B-1) — `TypeError: '>' not supported between float and NoneType` fixed in `_merge_extractions`, `low_conf_fields` check, and confidence unpacking (`llm_service.py`); confidence reads in validator also made None-safe (`validator.py`)
- [x] BUG-ALL-ZEROS (B-2) — Analysis page showing all zeros fixed: (1) `safe_float()` added to `app.py` — strips commas, never returns `0` for empty fields; (2) `/calculate` form merge changed to smart overlay — empty form fields no longer override extracted LLM values; (3) `_f()` in `calculator.py` updated to strip commas so `'23,500'` correctly converts to `23500.0`; (4) `safe_num()` added to `calculator.py` (`app.py`, `calculator.py`, `validator.py`)
- [x] LLM-V3-EXTRACTION — `_extract_financial_sections()` replaced with dual-axis scoring (keyword density + numeric table density); force-includes window with highest numeric score; `_unpack_extraction()` helper added; Pass 3 inserted into `extract_financial_fields()` (triggers when `revenue_fy3` null, re-runs LLM on top-3 numeric windows); CAGR base in `fill_missing_projections()` uses `rev3→rev2→rev1` fallback chain with `base_years_back` offset; debug JSON enhanced with `pass3_triggered`, `fields_null_after_merge`, per-year revenue snapshots (`llm_service.py`)
- [x] LLM-V3-HOTFIXES — (1) `generate_risk_analysis()` `NameError: client` fixed — added `client = OpenAI(...)` as first line; (2) `WINDOW` increased from `500` to `1500` chars; (3) dense-block numeric bonus added — if ≥8 lines with any number in window → `+5` to `numeric_score`; (4) all 9 `print()` calls replaced with `logger.info()` / `logger.warning()` (`llm_service.py`)
- [x] SOURCES-COLLATERAL-FIX (session 2026-03-11) — 8 prompt/code fixes to make Sources section extract correctly from real CIM PDFs. See CLAUDE.md SOURCES-FIX-SESSION-2026-03-11 for full details. Key fixes: M&E multiplier default 0→0.50; leverage_multiple input (default 3.5) + Term Loans fallback formula; ADJ. EBITDA PROJECTION CONTAMINATION GUARD (7-column table guard with Polytek example); COLLATERAL SOURCE PRIORITY (Balance Sheet > Borrowing Base Table for all fields); VALUE TREND rule for M&E (increasing=Gross Cost, constant=NBV); CONSTANT PERIOD VALUES rule for Building (constant across periods=Gross Cost, increasing=M&E row); Inventory NO-ASSUMPTION RULE refined (removed false "different values" constraint that caused LLM to reject correct inventory value matching AR).

---

## 13. Sources Section — Architecture Reference

### What the Sources section represents (ABL/acquisition financing structure)
The Sources section in Prebid V31 Template shows how the acquisition is financed.
Each line is a collateral asset multiplied by an advance rate = lending capacity.

### Field-by-field extraction and calculation

| Sources Line | Form Field | LLM Field | How Calculated | Correct Value (Polytek) |
|---|---|---|---|---|
| Net Revenue | `net_revenue_collateral` × `net_revenue_multiplier` (0.75) | `net_revenue_collateral` = AR from Balance Sheet | Extracted | 6,878 × 0.75 = 5,158.50 |
| Inventory | `inventory_collateral` × `inventory_multiplier` (0.70) | `inventory_collateral` = Inventory from Balance Sheet | Extracted | 6,878 × 0.70 = 4,814.60 |
| M&E Equipment | `me_equipment_collateral` × `me_equipment_multiplier` (0.50) | `me_equipment_collateral` = Net Book Value from Fixed Asset Schedule | Extracted | 3,250 × 0.50 = 1,625.00 |
| Building & Land | `building_land_collateral` × `building_land_multiplier` (0.50) | `building_land_collateral` = Gross Cost from Fixed Asset Schedule | Extracted | 14,067 × 0.50 = 7,033.50 |
| Term Loans / Cashflow | `existing_term_loans` OR fallback | `existing_term_loans` from debt schedule | Extracted or calculated | null → 8,581 × 3.5 = 30,033.50 |
| Seller Note | `seller_note` | None | Manual input | 0.00 |
| Earnout | `earnout` | None | Manual input | 0.00 |
| Equity Roll From Seller | `equity_roll_from_seller` | None | Manual input | 0.00 |

### Fixed Asset Schedule — How to read it correctly
A CIM Fixed Asset Schedule shows historical cost, accumulated depreciation, and net book value:
```
Asset               | Gross Cost | Accum. Depr. | Net Book Value
Warehouse Equipment |    14,634  |   (11,384)   |       3,250
Building            |    14,067  |      (xxx)   |       3,250
```
**Rule for M&E**: Extract NET BOOK VALUE (= Cost − Accum. Depr.) — realistic liquidation proxy.
  - Identification signal: NBV is CONSTANT or DECREASING across periods (equipment is depreciated).
  - The INCREASING set of values across periods (14,067→14,431→14,634) = Gross Cost — ignore.

**Rule for Building**: Extract GROSS COST (= original purchase price) — real estate doesn't depreciate in market value.
  - Identification signal: Building Gross Cost is CONSTANT across periods (same every year).
  - If values on "Building" row are INCREASING → you are on the M&E row by mistake.

### Term Loans fallback formula (calculator.py)
```python
_existing_tl = inputs.get('existing_term_loans')
if _existing_tl is not None and _f(_existing_tl) > 0:
    C12 = _f(_existing_tl)
else:
    _leverage_mult = _f(inputs.get('leverage_multiple', 3.5))
    _adj_ebitda3 = _f(inputs.get('adj_ebitda_fy3', 0))
    C12 = round(_adj_ebitda3 * _leverage_mult, 2) if _adj_ebitda3 > 0 else 0.0
E12 = C12
```

### Adj. EBITDA Projection Contamination (7-column tables)
Some CIM P&L tables show BOTH historical and projection years in the same row:
```
FY22 | FY23 | FY24 | FY25 | Y1   | Y2   | Y3
15,044 | 21,632 | 10,918 | 8,581 | 9,324 | 13,624 | 19,395
```
Rule: `adj_ebitda_fy3` must come from FY25 (position 4 = 8,581), NOT Y2 (position 6 = 13,624).
The prompt includes an explicit example with these exact Polytek values as a general illustration.

### Anti-hallucination trap: Inventory = AR value
In Polytek, AR = 6,878 AND Inventory = 6,878 (same value coincidentally).
A previous anti-hallucination rule said "AR and Inventory almost always have DIFFERENT values" — this caused the LLM to REJECT the correct inventory (6,878) because it matched AR, then pick 14,494 instead.
**Fix**: The rule now says "may coincidentally have the same value — acceptable if each was independently confirmed from its own labeled row."

---

## 12. Known Behaviours / Edge Cases

- **LLM non-determinism**: Even at `temperature=0`, NVIDIA NIM can return slightly different results across runs. This is normal — the review page exists to let analysts correct any wrong values.
- **Collateral extraction variability**: M&E and Building extractions are the most LLM-sensitive fields due to Fixed Asset Schedule ambiguity. The value-trend rules (M&E=constant/decreasing, Building=constant across periods) are more reliable than column-header matching (OCR often strips headers). If wrong values appear, check the extraction JSON citations — they reveal exactly which number the LLM read and from where.
- **SGA null on simple PDFs**: Documents without an explicit SG&A row will have null SGA. This is correct — no SGA row means operating income = gross margin, which produces valid EBITDA.
- **OCR quality varies**: Google Document AI handles tables well but complex multi-column layouts may produce less-structured text. The `_extract_financial_sections()` scoring ensures the most financially-dense sections are prioritised.
- **Large PDFs (>30 pages)**: Text is condensed to 30k chars using dual-axis window scoring (keyword density + numeric table density). Pass 2 runs on the second half if any fields are null. Pass 3 runs on top-3 numerically-dense windows if `revenue_fy3` is still null.
- **Session expiry**: Flask session is cookie-based. If the user closes the browser or the session expires, they must re-upload the PDF.
- **Excel DIV/0 protection**: `_fopt()` helper in `excel_export.py` skips writing null/zero values to cells that are divisors in template formulas (e.g. L7 is a divisor in L8=(L7-K7)/K7).
- **Pre-existing SyntaxWarning**: `(?<!\d)` in a docstring in `llm_service.py` triggers a harmless Python SyntaxWarning on import. Does not affect functionality.
- **PDFs tested**: Project Chimera, Project Network (CIP), Project Manta Ray, Project Palm, Project Smores (CIM 2026).
- **Form merge order**: `/calculate` uses `{**extracted}` then overlays only non-None form values. Empty/blank form fields keep the LLM-extracted value. This means analysts can submit the review form without clearing any pre-filled field accidentally.
- **Comma-formatted inputs**: `_f()` and `safe_float()` both strip commas. If an analyst types `23,500` anywhere in the form it converts correctly.
- **B-2 silent zeros**: Previously `_f('23,500')` crashed on comma → returned `0.0` silently. This was the primary all-zeros root cause on first-pass submissions.
