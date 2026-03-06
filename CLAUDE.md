# Atar Capital — Prebid Analysis System

---

## ⚠️ STRICT MODIFICATION RULE

**This file is the ONLY source of instructions.**
When the user writes a task in the `## PENDING UPGRADES / TASKS` section below:
- Read it carefully
- Modify **only the files explicitly named in the task** (or listed in the allowed table)
- Do **not** touch any other file, fix unrelated things, refactor surrounding code, or add unrequested features
- After completing the task, move it from `PENDING` to `COMPLETED`

---

## ALLOWED FILES TO MODIFY

Only these files may be changed. Do NOT touch anything else without explicit user instruction.

| File | What it controls |
|---|---|
| `services/llm_service.py` | LLM extraction schema, prompts, projection logic, fiscal year detection, field parsing |
| `services/calculator.py` | Excel formula replication (all 45+ formulas) |
| `services/validator.py` | Anti-hallucination validation rules |
| `app.py` | Flask routes, step logging, session management |
| `templates/review.html` | Review page, extracted fields display, manual input form |
| `templates/analysis.html` | Final analysis output display |
| `services/excel_export.py` | Excel template export (fills Prebid V31 Template.xlsx) |
| `static/css/style.css` | Visual styles only |

## DO NOT MODIFY — EVER (unless user explicitly names the file)

| File | Reason |
|---|---|
| `services/ocr_service.py` | OCR chunking is stable and working correctly |
| `templates/index.html` | Upload page is complete |
| `requirements.txt` | Dependencies are pinned |
| `.env` | Credentials — never read or modify |
| `static/js/main.js` | Client-side JS is stable |

---

## PENDING UPGRADES / TASKS

> Write new tasks here. Claude will read this section, execute only what is written, and move each task to COMPLETED when done.

---

## COMPLETED TASKS

- [x] LLM-PROMPT-FIX4: 4 targeted fixes to `services/llm_service.py` only. (FIX-1) `_detect_fiscal_years()` hard-cap guard: added `confirmed_hist_years` pre-computation (years appearing with A/Actual/Audited/Restated markers); Guard 1 walks back from future years (`> current_year`); Guard 2 walks back when `most_recent_year` is proj-suffix-only and absent from `confirmed_hist_years` — prevents 2025E/2026E leaking as detected historical years. (FIX-2) Adj. EBITDA label matching: expanded `EXTRACTION_SCHEMA` adj_ebitda descriptions to list all label variants ("Adj. EBITDA", "Adjusted EBITDA", "Adj EBITDA", "EBITDA (Adjusted)", "EBITDA (as adjusted)", "Normalized EBITDA", "Recurring EBITDA"); added ADJ. EBITDA LABEL MATCHING block to Section C of `_build_extraction_prompt()` with priority: explicit label → post-add-backs row → fallback derivation (operating_income + adjustments, confidence 0.75). (FIX-3) E-suffix distinction: added IMPORTANT DISTINCTION block to Section D making explicit that E/F/B/P suffix columns must NOT be mapped to historical fy1/fy2/fy3 slots but MUST be mapped to proj_y1..y5 slots; proj_revenue_y1 = first E/F-suffix year column > fy3. (FIX-4) Term loan & collateral guidance: updated `EXTRACTION_SCHEMA` descriptions for `existing_term_loans` (outstanding balance, not revolver) and `building_land_collateral` (gross asset value, not advance-rate-adjusted borrowing base); added explicit COLLATERAL & DEBT FIELDS block to Section A of `_build_extraction_prompt()`; extended Section E anti-hallucination checklist with 5 new check items. No other files changed.

- [x] BUG-SESSION-OVERFLOW: /analysis showing previous PDF's data — root cause: Flask 4KB cookie limit silently drops session writes when results dict is too large. Fix (`app.py` only): (1) Added `import json`, `import datetime`, `make_response` to imports; (2) Added `RESULTS_FOLDER = os.path.join('storage', 'results')` constant; (3) Added `_save_results(session_id, results, risk_analysis, all_inputs)` — serialises all three dicts to `storage/results/{session_id}_results.json` via `json.dump(..., default=str)`; (4) Added `_load_results(session_id) → (results, risk_analysis, all_inputs)` — reads JSON file, returns empty defaults on FileNotFoundError/JSONDecodeError; (5) `/calculate` now calls `_save_results()` and sets `session['calc_complete']` + `session['calc_timestamp']` instead of `session['results']`/`session['risk_analysis']`/`session['all_inputs']`; (6) `/analysis` reads `sid = session.get('session_id')`, calls `_load_results(sid)`, redirects to `/review` if no file found, wraps `render_template` in `make_response` with `Cache-Control: no-store` / `Pragma: no-cache` / `Expires: 0` headers; (7) `/export` also updated to call `_load_results(sid)` instead of reading from session (since `all_inputs` no longer lives in cookie).

- [x] LLM-V3-EXTRACTION: Window Extraction + Projection Engine Fix — `services/llm_service.py` only. 4 targeted changes: (FIX-1) Replaced `_extract_financial_sections()` with dual-axis scoring — keyword density + numeric table density (lines with 3+ numbers score +3 each); window with highest pure numeric score is always force-included, ensuring P&L tables reach the LLM even when narrative prose scores higher. (FIX-2) Added `_unpack_extraction()` helper function; inserted Pass 3 into `extract_financial_fields()` immediately before ENH-1 COGS comment — triggers when `revenue_fy3` is null after Pass 1+2, re-runs LLM on top-3 numerically-dense 1500-char windows from full OCR text (no keyword filter); merges: Pass 3 wins only where current extracted value is null. (FIX-3) Replaced CAGR base block in `fill_missing_projections()` with robust fallback chain rev3→rev2→rev1; tracks `base_years_back` offset and adjusts compound steps accordingly; if all three historical revenues are null returns `{}, 'manual_required'` instead of silently zeroing. (FIX-4) Enhanced debug JSON: replaced `json.dump({...})` save block with `debug_payload` dict containing `detected_fy_years`, `pass3_triggered`, `fields_null_after_merge`, per-year revenue values; all diagnostics use `logger` calls. Adapter notes applied: `_call_llm(client, prompt, system_prompt=...)` signature used correctly; `_unpack_extraction()` added as named helper rather than inline. No other files changed.

- [x] LLM-V3-HOTFIXES: 4 follow-up fixes to `services/llm_service.py` only. (1) Fixed `NameError: client is not defined` in `generate_risk_analysis()` — added `client = OpenAI(base_url=NVIDIA_BASE_URL, api_key=NVIDIA_API_KEY)` as first line of function body. (2) Increased `_extract_financial_sections()` window size from `WINDOW=500` to `WINDOW=1500` — captures full P&L table rows that span more than 500 chars. (3) Added dense-block numeric bonus to window scoring — counts lines with ≥1 number per chunk; if `num_lines_count >= 8`, adds `+5` bonus to `numeric_score`; catches one-value-per-line OCR table format (vertical P&L layout). (4) Replaced all 9 `print()` calls added in LLM-V3 with proper `logger.info()` / `logger.warning()` calls — Pass 3 lines, [PROJ] fallback lines, and [DEBUG] log lines all now flow through the logging framework. No other files changed.

- [x] PROMPT-ROBUST-V2: LLM prompt robustness v2.0 — closed 4 real-world PDF layout gaps in `llm_service.py` only. (G-1) Short-form year labels: added `yy1/yy2/yy3 = str(fyN)[2:]` to `_build_extraction_prompt()` and `_build_system_prompt()`; Section A now lists `FY{yy1}`, `FY'{yy1}`, `'{yy1}` aliases for each year; Rule 7 in both SYSTEM_PROMPT and `_build_system_prompt()` documents all variants. (G-2) 4+ column tables: removed positional TABLE READING ORDER rule; replaced with header-match COLUMN ASSIGNMENT RULE (4-step) in Section A and Rule 8. (G-3) LTM/TTM columns: added COLUMNS TO IGNORE ENTIRELY block to Section A and Rule 9 (LTM, TTM, NTM, Run-Rate, Pro Forma, PF, Combined, quarterly). (G-4) Restated vs As-Reported: added RESTATED vs AS-REPORTED priority block to Section A and Rule 10 (Restated > Revised > Adjusted > As Reported > As Filed). Static `SYSTEM_PROMPT` constant expanded from 6 to 10 rules; `_build_system_prompt()` updated to match with year integers injected; Section E anti-hallucination checklist expanded from 6 to 10 items (added: no positional assignment, no LTM/TTM, restated check, contamination guard). No other files changed. (`llm_service.py`)

- [x] PROMPT-OPT: LLM prompt optimisation — dynamic year anchoring & anti-hallucination. (A) Added `_build_system_prompt(y1, y2, y3)` function that injects detected year integers into SYSTEM_PROMPT Rule 6 (YEAR ANCHOR ENFORCEMENT), replacing the static `SYSTEM_PROMPT` constant for extraction calls; (B) Updated `_call_llm()` to accept optional `system_prompt` parameter; (C) Replaced `_build_extraction_prompt()` entirely with comprehensive f-string: FISCAL YEAR MAPPING with all column header variants (FY{y}A, "{y} Actual"), projection year aliases tied to fy3+1..fy3+5, TABLE READING ORDER (left→right), COLUMN COUNT CHECK (2-col → fy2/fy3 only), FORMULA DERIVATION CHAIN, ADJ. EBITDA CROSS-CHECK, PROJECTION EXTRACTION RULES + CONTAMINATION GUARD, ANTI-HALLUCINATION CHECKLIST, hardcoded JSON schema with fy_year_1/2/3 as detected integers; (D) Added EBITDA cross-check block in `extract_financial_fields()` — downgrade confidence to 0.60 and append discrepancy note to citation when stated vs calculated EBITDA differs >5%; (E) Updated `analysis.html` projection column headers from hardcoded "Year 1-5" to dynamic "FY{fy3+1}"-"FY{fy3+5}" using `detected_fy_years[2]`. (`llm_service.py`, `analysis.html`)

- [x] FY-DETECT: Fiscal year detection — always 3 consecutive years anchored to most recent historical year in PDF. (A) `_detect_fiscal_years()` assignment now always forces `y3=most_recent, y2=y3-1, y1=y3-2` regardless of how many historical years exist; (B) Pass 3 last-resort scan added: when table-header detection AND frequency≥2 both fail (single-column-per-line OCR layout), scans for any non-projection year within a 7-year window of current year; (C) `_build_extraction_prompt()` FISCAL YEAR MAPPING block updated to explicitly restrict LLM to ONLY the 3 detected year columns and ignore older columns. Tested on `synthetic_cip_test_02.pdf` (4 historical years FY2021-FY2024) and `synthetic_cip_test_03.pdf` (5 historical years FY2020-FY2024) — both correctly return fy1=2022, fy2=2023, fy3=2024. (`llm_service.py`)

- [x] Fix Jinja2 macro undefined error in `review.html` — moved `render_extracted_field` macro to top of file
- [x] Add step-by-step terminal logging for OCR, LLM, and validation steps
- [x] Revenue extraction: enforce TOTAL consolidated revenue, detect fiscal years from document (not system clock)
- [x] Add 5-year projections: extract from OCR if available, else auto-calculate from historical CAGR/averages
- [x] Optimized extraction prompt: short SYSTEM_PROMPT, dedicated `_detect_fiscal_years()`, explicit year-to-key mapping in prompt
- [x] BUG-1: Fixed string fields (`company_name`, `fy_year_*`) null — skip `_coerce_numeric` for string fields
- [x] BUG-2/3: Fixed projection contamination — de-duplication guard in `fill_missing_projections()`
- [x] MISSING-4: Detected FY integers stored in session and passed to templates as fallback
- [x] MISSING-5: Added temporal guard in extraction prompt — projections must be AFTER fy3
- [x] Dynamic fiscal year detection — table-header anchored algorithm, `_data_present_fyN` flags, UI dimming
- [x] Revenue extraction hardening — FORBIDDEN sub-revenues in prompt, SELECTION/POSITION rules
- [x] Excel export — `services/excel_export.py` fills Prebid V31 Template with ~70 INPUT cells, `/export` route in app.py, button in analysis.html
- [x] BUG-6: Fixed LLM extraction returning all nulls — root cause was `max_tokens=4096` truncating the JSON response when reasoning model consumed extra tokens. Fix: (1) increased `max_tokens` to 8192, (2) added regex-based fallback in `_parse_llm_json()` to recover complete fields from truncated JSON, (3) added `finish_reason` logging to detect future truncation
- [x] ENH-1: COGS fallback — `cogs_fy1/2/3` added to schema; if `gross_margin_fyN` is null and `cogs_fyN` non-null, derives GM = revenue − COGS (`llm_service.py`)
- [x] ENH-2: Adj. EBITDA extraction — `adj_ebitda_fy1/2/3` added to schema; cross-check in `validate_extracted_fields()` flags >5% discrepancy between calculated and document-stated EBITDA (`llm_service.py`, `validator.py`)
- [x] ENH-3: Company name editable — added `<input name="company_name">` to review form so user can correct wrong extracted name (`review.html`)
- [x] ENH-4: AR collateral smart default — `net_revenue_collateral` pre-filled with `revenue_fy3` when null, so Total Sources is non-zero by default (`review.html`)
- [x] ENH-5: Transaction date in Excel export — `ws['C6'] = datetime.date.today()` replaces static 2021-11-30 template date (`excel_export.py`)
- [x] FIX-1: Excel Uses section C26/C27/C28 wrong keys — C26 now = derived EBITDA (GM_fy3−SGA_fy3+adj, prefer `adj_ebitda_fy3`), C27 = `acquisition_multiple`, C28 = `pct_acquired` (`excel_export.py`)
- [x] FIX-2: REVERTED — original E10-E15 mapping was correct; FIX-2's E11-E16 shift caused double-count with template default at E10 (`excel_export.py`)
- [x] FIX-3: Added `acquisition_multiple` (×EBITDA, default 7.0) and `pct_acquired` (default 1.0) fields to Deal Terms section of review form (`review.html`)
- [x] FIX-4: Robust EBITDA calc — (A) `if gm3` replaces `if (gm3 and sga3)` so SGA=0 companies work; (B) `doc_ebitda > 0` guard prevents negative EBITDA from being used as PE valuation basis (`excel_export.py`)
- [x] ENH-6: Force Excel auto-recalculation — `wb.calculation.calcMode = 'auto'` + `fullCalcOnLoad = True` added before save; changing C27 (multiple) in exported Excel now auto-updates Purchase Price, IRR, MOIC, exit valuation (`excel_export.py`)
- [x] ENH-8: Risk Analysis section — LLM-generated 6 risk factors shown below Validation Summary on analysis page; each risk has category badge, source badge (Memo=document-grounded / General=industry knowledge), confidence progress bar, and citation; `generate_risk_analysis()` added to `llm_service.py`, called in `/calculate` route, stored in session (`llm_service.py`, `app.py`, `analysis.html`)
- [x] BUG-EXCEL-DIV0: Fixed DIV/0 in projection formula cells — root cause was `_f()` returning `0.0` for missing values, overwriting template defaults (L7-P7=100000 etc.) and causing `L11=L10/L7`, `L8=(L7-K7)/K7`, `L19=L18/L7` to divide by zero. Fix: added `_fopt()` helper that returns `None` for absent values; all historical (I-K) and projection (L-P) financial cells now use conditional write pattern — cell is only written when value is non-null/non-empty. Rate parameters, deal terms, and transaction fees still use `_f()` with hardcoded defaults (`excel_export.py`)
- [x] BUG-RISK-JSON: Fixed Risk Analysis not showing on UI — improved JSON parsing in `generate_risk_analysis()`: now handles `{"risks":[...]}`, plain `[...]` array, markdown fences, and partial JSON; added detailed terminal logging (raw LLM response preview, risk count, 0-risk warning) for diagnosing future failures (`llm_service.py`)
- [x] BUG-RISK-KEYS: Fixed Risk Analysis returning empty — root cause was wrong `results` dict key lookups (`moic`, `irr`, `fccr` don't exist; correct keys are `C29`=MOIC, `C57_fccr`=FCCR, `dscr`=dict with Y1..Y5, `adj_ebitda`=dict). Summary now correctly includes MOIC/FCCR/DSCR/Adj.EBITDA/Total Debt Service from actual calculator output, giving the LLM enough grounded data to generate meaningful risks (`llm_service.py`)
- [x] ENH-9: Excel scenario modeling — exported workbook now has cell-level protection: all ~40 input cells (sources, uses, deal terms, rate params, historical P&L, Y1-Y5 projections) set `locked=False`; Sheet1 protected so formula cells are read-only but navigable; Transaction Fees tab fully locked (structural). User can change any assumption directly in Excel and formulas recalculate — no need to return to web UI. No password set (accident-prevention only; user can unprotect if needed) (`excel_export.py`)

---

## Project Rules (DO NOT CHANGE THESE)

### Fiscal Year Detection (Table-Header Anchored)
- Use `_detect_fiscal_years(ocr_text)` in `llm_service.py`
- **PRIMARY signal**: lines where ≥2 years appear together = table column headers = confirmed data years
  - `"   FY2025    FY2026"` on one line → both are data years ✓
  - `"as of March 2024, the company..."` → only 1 year → NOT a table header ✗
- **SECONDARY**: frequency ≥2 mentions, used to supplement only if fewer than 3 table-header years found
- **HARD CAP**: years > current calendar year excluded (future projections, not historical data)
- If fewer than 3 years found after both passes, infer backward from most recent
- Detected years injected into LLM prompt for precise column matching
- After extraction: `_data_present_fy1/2/3` flags added to `extracted` dict (True if revenue/GM/SGA non-null/non-zero for that year)
  - These flags flow to UI: `analysis.html` dims empty historical columns; `review.html` shows "No data found" badge
  - `_data_present_*` keys use underscore prefix = internal flags, not shown as extraction fields

### Revenue Extraction
- Always extract the **TOTAL company-wide consolidated top-line revenue** — the SINGLE LARGEST revenue figure in the P&L
- **FORBIDDEN sub-revenues (NEVER extract these)**:
  - Geographic breakdowns: Americas, EMEA, APAC, Asia, Europe, North America, International
  - Product/service type rows: Hardware, Software, Services, Subscription, License, Maintenance
  - Division/segment/channel rows: Segment, Division, Business Unit, B2B, B2C, Wholesale, Retail
  - Any row named after a specific product, brand, or subsidiary
- **SELECTION RULE**: if multiple revenue rows exist → pick the one with the HIGHEST value. The total is always ≥ every sub-row.
- **POSITION RULE**: in a standard P&L, the top-line revenue row appears BEFORE "Cost of Goods Sold" / "COGS"
- Common valid labels: "Total Revenue", "Net Revenue", "Net Sales", "Revenue", "Total Sales", "Sales"
- Column matching: use detected year values (e.g. 2025, 2026) to match P&L columns exactly
- Same 3-year rule applies to ALL financial fields: gross margin, SG&A, interest, adjustments
- These rules are enforced in: `SYSTEM_PROMPT` (rule 3), `REVENUE IDENTIFICATION` block in `_build_extraction_prompt()`, and revenue field descriptions in `EXTRACTION_SCHEMA`

### Projections
- **First**: extract projection years (Y1–Y5) from OCR if document has explicit forecasts/budgets
- **If not found**: auto-calculate using `fill_missing_projections()` in `llm_service.py`:
  - Revenue: historical CAGR applied forward (capped -20% to +50%)
  - Gross Margin: average GM% × projected revenue
  - SG&A: average SG&A% × projected revenue
  - Interest expense: flat from most recent historical year
  - Adjustments: default 0
  - Term loan: blank for manual input
- Projection source (`'ocr'` or `'calculated'`) shown as badge on review page

### Anti-Hallucination Rules
- LLM confidence < 0.70 → return `null`, never a guess
- All monetary values in **$000s** (thousands of USD)
- Validation runs on every extraction before showing review page

### Monetary Conversions
- `$5.2M` → `5200` | `$4,800,000` → `4800` | `$500K` → `500` | `$1B` → `1000000`

### Flexibility / Defensive Coding
- Never assume all 3 historical years are populated — handle missing FY1, FY2, or FY3 gracefully
- Never use Python truthiness (`if val`) to check numeric fields — 0 is a valid financial value
  - Use `if val is not None` for presence checks
  - Use `if val and val > 0` only when zero/negative is genuinely invalid (e.g. EBITDA for PE valuation)
- SGA can legitimately be 0 (service companies, pass-through models) — code must not break
- `adj_ebitda_fyN` can be negative (loss-making company) — never use a negative value as PE valuation basis
- All new logic must handle: (a) missing field, (b) zero value, (c) negative value, (d) very large value
- Do not add document-specific constants or thresholds — keep all parameters configurable via `all_inputs`

---

## Prompt Architecture (services/llm_service.py)

### SYSTEM_PROMPT
- 6 rules only — do NOT make it verbose
- Key rules: null-if-missing, $000s conversion, revenue=consolidated total, JSON-only, confidence thresholds

### `_detect_fiscal_years(ocr_text)`
- Regex scans all `20xx` 4-digit years in document
- Years mentioned ≥2 times are real fiscal years (not noise)
- Returns `(y1, y2, y3)` anchored to document's latest year, NOT system clock

### `_build_extraction_prompt(text, fy_years)`
- Injects detected `(y1, y2, y3)` as FISCAL YEAR MAPPING block at top
- Contains focused REVENUE IDENTIFICATION block
- Do NOT make the prompt longer — every added line reduces extraction quality

---

## Run Command
```
python app.py
```
App runs at http://localhost:5000

## Tech Stack
- Flask 3.0.3 (Python) — port 5000, debug mode
- Google Document AI — OCR (14-page chunks via pypdf)
- NVIDIA NIM `openai/gpt-oss-120b` — field extraction
- Bootstrap 5.3 — UI
- No database — Flask session only
