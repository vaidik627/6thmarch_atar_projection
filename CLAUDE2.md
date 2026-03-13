# Atar Capital — CLAUDE2 Instruction File
# Purpose: Prevent hallucinations and scope-creep during modifications.
# This file is the authoritative override for any session where CLAUDE2.md is referenced.

---

## ⚠️ PRIME DIRECTIVE

**Read this file completely before touching any code.**

Rules in this file OVERRIDE CLAUDE.md where they conflict.
When given a task:
1. Identify EXACTLY which file(s) the task requires changing
2. Read those files in full before editing
3. Make ONLY the change described — nothing else
4. Do NOT fix unrelated issues you notice while reading
5. Do NOT refactor, rename, reformat, or "clean up" surrounding code
6. Do NOT add comments, docstrings, logging, or error handling unless the task explicitly asks for it

---

## ALLOWED FILES TO MODIFY

> Update this table when a new task is assigned. Remove or update rows when tasks are completed.

| File | What may be changed |
|---|---|
| `services/llm_service.py` | LLM extraction schema, prompts, projection logic, fiscal year detection, field parsing, recommendation logic |
| `services/calculator.py` | Excel formula replication |
| `services/validator.py` | Anti-hallucination validation rules |
| `app.py` | Flask routes, session management, results persistence |
| `templates/review.html` | Review page fields and deal inputs |
| `templates/analysis.html` | Analysis output display |
| `services/excel_export.py` | Excel template export |
| `static/css/style.css` | Visual styles only |

---

## DO NOT MODIFY — EVER

| File | Reason |
|---|---|
| `services/ocr_service.py` | OCR pipeline is stable — do not touch |
| `templates/index.html` | Upload page is complete (Deal Value field added and finalised 2026-03-13) |
| `requirements.txt` | Dependencies are pinned — do not add or change versions |
| `.env` | Credentials — never read, never modify, never print |
| `static/js/main.js` | Client-side JS is stable |
| `Prebid V31  Template.xlsx` | Excel template — read-only reference, never overwrite |

---

## PENDING TASKS

> User writes tasks here. Execute exactly what is written. Move to COMPLETED when done.
> Format: TASK-ID: description. Files: list. Expected result: what changes.

*(none)*

---

## GROUND TRUTH VALUES (Do Not Contradict These)

These are confirmed correct extraction values from the Polytek CIM (session 38a03c5f).
When writing or debugging extraction logic, DO NOT produce code that would yield different values.

| Field | Correct Value | Source | Status |
|---|---|---|---|
| `revenue_fy1` | 99,086 | P&L FY2023 | ✅ Active extraction |
| `revenue_fy2` | 92,452 | P&L FY2024 | ✅ Active extraction |
| `revenue_fy3` | 96,100 | P&L FY2025 | ✅ Active extraction |
| `gross_margin_fy1` | 50,723 | P&L FY2023 | ✅ Active extraction |
| `gross_margin_fy2` | 45,474 | P&L FY2024 | ✅ Active extraction |
| `gross_margin_fy3` | 36,999 | P&L FY2025 | ✅ Active extraction |
| `sga_fy1` | 32,847 | P&L FY2023 | ✅ Active extraction (re-enabled 2026-03-13) |
| `sga_fy2` | 31,559 | P&L FY2024 | ✅ Active extraction (re-enabled 2026-03-13) |
| `sga_fy3` | 28,812 | P&L FY2025 | ✅ Active extraction (re-enabled 2026-03-13) |
| `adjustments_fy1` | 3,959 | Add-backs total FY2023 | ✅ Active extraction |
| `adjustments_fy2` | 5,088 | Add-backs total FY2024 | ❌ BUG-ADJ-FY2: reads 2,438 |
| `adjustments_fy3` | 886 | Add-backs total FY2025 | ❌ BUG-ADJ-FY3: reads 394 |
| `adj_ebitda_fy3` | 8,581 | FY2025 column ONLY — NOT Y1/Y2 projection columns | ❌ BUG-ADJ-EBITDA-FY3: reads null |
| `net_revenue_collateral` | 6,878 | AR from Balance Sheet Dec-25A | ✅ Active extraction |
| `inventory_collateral` | 6,878 or 6,147 | Balance Sheet Dec-25A (NOT 14,494 borrowing base) | ❌ BUG-INV: reads 14,494 |
| `me_equipment_collateral` | 14,634 | Fixed Asset Schedule — Warehouse Equipment row, Dec-25A | ✅ Active extraction |
| `building_land_collateral` | 3,250 | Fixed Asset Schedule — Building row, Dec-25A | ❌ BUG-BLDG: M&E / Building swap |
| `detected_fy_years` | [2023, 2024, 2025] | Table-header anchored | ✅ Active extraction |

---

## KNOWN ACTIVE BUGS (as of 2026-03-13)

List bugs that are confirmed but not yet fixed. Reference these when writing tasks.

| Bug ID | Field | Current Wrong Value | Correct Value | Root Cause |
|---|---|---|---|---|
| BUG-ADJ-FY2 | `adjustments_fy2` | 2,438 | 5,088 | LLM reading wrong column (year-shift) |
| BUG-ADJ-FY3 | `adjustments_fy3` | 394 | 886 | LLM reading wrong column (year-shift) |
| BUG-ADJ-EBITDA-FY3 | `adj_ebitda_fy3` | null | 8,581 | Confidence guard nulling it OR projection contamination |
| BUG-INV | `inventory_collateral` | 14,494 | 6,878 | LLM reading borrowing base table instead of Balance Sheet |
| BUG-BLDG | `building_land_collateral` | 14,634 | 3,250 | M&E / Building swap still occurring |

---

## CURRENT FEATURE STATUS (as of 2026-03-13)

| Feature | Status | Notes |
|---|---|---|
| OCR (Google Document AI) | ✅ Working | 14-page chunk pipeline, stable |
| LLM extraction (NVIDIA NIM) | ✅ Working | 3-pass extraction with focused window |
| Fiscal year detection | ✅ Working | Table-header anchored, 3-guard system |
| Revenue extraction | ✅ Working | Consolidated total enforced, FORBIDDEN sub-revenues |
| Gross Margin extraction | ✅ Working | COGS fallback if GM null |
| SG&A extraction | ✅ Active extraction | LLM + contamination guard (60% threshold) + GM−EBITDA fallback; OCR-first → CAGR projections |
| Adjustments extraction | ⚠️ Partial | FY1 correct; FY2/FY3 year-shifted (see bugs above) |
| Adj. EBITDA extraction | ⚠️ Partial | FY3 often null due to confidence guard / contamination |
| Collateral — AR | ✅ Working | Balance Sheet only, AR=revenue guard |
| Collateral — Inventory | ❌ Bug | Reads borrowing base (14,494) instead of BS (6,878) |
| Collateral — M&E | ⚠️ Unreliable | Row-label-first rule in place; still swapping with Building |
| Collateral — Building | ⚠️ Unreliable | Row-label-first rule in place; still swapping with M&E |
| 5-year projections | ✅ Working | OCR-first, CAGR fallback; SGA: OCR-first → 2-yr CAGR → sentinel 0 |
| Risk Analysis | ✅ Working | LLM-generated 6 risk factors with badges + confidence bars |
| Deal Recommendation | ✅ Working | Python-threshold verdict (BUY/HOLD/NOT TO BUY) + LLM rationale |
| Deal Value input | ✅ Working | Captured at upload, editable on review, fed to recommendation |
| Excel export | ✅ Working | ~70 input cells, scenario modelling, cell-level protection |
| Results persistence | ✅ Working | Disk-based JSON (4-tuple); Flask session only holds session_id |

---

## ANTI-HALLUCINATION CONSTRAINTS FOR CODE CHANGES

These rules apply to every code change, regardless of task scope.

### Python Numeric Safety
- NEVER use `if val:` to check a numeric field — `0` is a valid financial value
- ALWAYS use `if val is not None:` for presence checks
- ALWAYS use `if val is not None and val > 0:` when zero is genuinely invalid
- `safe_float()` and `_f()` already strip commas — do NOT add redundant `.replace(',', '')`

### LLM Prompt Rules
- Do NOT add new prompt sections without removing an equivalent amount of existing text
- Every new rule added to the prompt must have a concrete example (show wrong → correct)
- Do NOT repeat instructions already stated elsewhere in the prompt — duplication confuses the LLM
- All monetary examples in the prompt must use $000s units (e.g. 8,581 not 8.581M)
- Year variables: always use `{fy1}`, `{fy2}`, `{fy3}` — never hardcode year integers in prompt strings
- Do NOT add general advice ("be careful", "double-check") — only concrete extraction rules

### SG&A Contamination Guard (DO NOT WEAKEN THRESHOLDS)
- Guard 1: if `sga_fyN >= revenue_fyN` → null (SGA can never equal or exceed revenue)
- Guard 2: if `sga_fyN / revenue_fyN > 0.60` → null (cross-table COGS contamination signal)
- Threshold is 0.60, NOT 0.30 — Polytek SGA% is 30–34% which is normal for manufacturing/distribution
- GM−EBITDA fallback: if SGA null after guards AND gross_margin + adj_ebitda both present → `sga = gm − ebitda`
- Sentinel: if still null after fallback → set to 0 (Excel formulae require numeric)
- Projections: OCR-first → 2-yr CAGR (fy1→fy3) → earlier hist base → sentinel 0

### Collateral Extraction (DO NOT CHANGE THESE RULES)
- M&E: ROW-LABEL-FIRST — find "Warehouse Equipment"/"M&E"/"Equipment" row, extract most recent year
- Building: ROW-LABEL-FIRST — find "Building"/"Land"/"Real Estate" row, extract most recent year
- Inventory: Balance Sheet only — single "Inventory"/"Inventories" line, most recent year
- AR: Balance Sheet only — "Accounts Receivable" line, most recent year. If AR = revenue_fy3 exactly → null
- Do NOT use: Borrowing Base table, sub-schedule totals, or gross-before-reserves values

### Fiscal Year Anchoring
- `fy3` = most recent HISTORICAL year in document (not current calendar year)
- `fy3` can NEVER be a projection year (E/F/B/P suffix)
- adj_ebitda_fyN must come from the column whose header = fyN exactly
- Projection columns (Y1/Y2/Y3 or E/F suffix) must NEVER be mapped to fy1/fy2/fy3 keys

### Calculator / Excel
- All values passed to `calculator.py` and `excel_export.py` are in $000s — never convert again
- `_fopt()` must be used for any cell that could be a divisor in an Excel formula
- `_f()` with default=0.0 is only safe for non-divisor input cells

### Deal Recommendation
- Verdict is ALWAYS Python-computed from actual MOIC/FCCR/DSCR numbers — never let LLM decide verdict
- LLM only writes a 1-sentence rationale citing the actual numbers
- Thresholds: NOT TO BUY if FCCR<1.0 OR DSCR(Y1)<1.0 OR MOIC<1.0; BUY if score≥3/4; HOLD otherwise
- BUY score criteria: MOIC≥2.5×, FCCR≥1.10, DSCR≥1.20, implied EV/EBITDA≤10×

---

## WHAT "DONE" MEANS

A task is complete ONLY when:
1. The specific wrong value is corrected to the ground truth value shown above
2. No other field values changed as a side effect
3. No unrelated code was modified
4. The task is moved from PENDING to COMPLETED in this file

---

## COMPLETED TASKS

- [x] SGA-REIMPLEMENTATION-2026-03-13: Re-implemented SG&A extraction in `services/llm_service.py` only, per spec SGA_Extraction_Implementation_Spec.pdf.
  - **Change 1 — EXTRACTION_SCHEMA**: Added `sga_fy1/2/3` (default 0) and `proj_sga_y1..y5` (default 0) keys after `gross_margin_fy3`.
  - **Change 2 — `_build_extraction_prompt()`**: (2a) Added full SG&A EXTRACTION block (ROW SELECTION RULE, CONSOLIDATED TABLE RULE, COLUMN ASSIGNMENT RULE, SIZE CHECK at 60% threshold, ABSENT SG&A rule) between Gross Margin and EBITDA sections. (2b) Added `sga_fy1/2/3` to JSON output schema after `gross_margin_fy3`. (2c) Added 2 SGA items to Section E checklist (consolidated P&L only; ratio ≤ 0.60).
  - **Change 3 — `_extract_revenue_fields_focused()`**: (3a) Added SGA extraction rule line to Rules block. (3b) Added `sga_fy1/2/3` conf/cite fields to Return JSON schema. (3c) Added `'sga_fy1', 'sga_fy2', 'sga_fy3'` to `_revenue_override_keys` so focused-pass SGA overrides main-pass.
  - **Change 4 — `extract_financial_fields()`**: Replaced force-zero block with contamination guard + GM−EBITDA fallback: Guard 1 (SGA≥revenue → null); Guard 2 (SGA>60% of revenue → null); Calculated fallback (GM−EBITDA when SGA absent and both present); Sentinel 0 if still null.
  - **Change 5 — `fill_missing_projections()`**: Restored `sg1/sg2/sg3`, `ocr_sga`, `hist_sg_set`, `_dedup` call, `ocr_sga` in `has_ocr_proj`; replaced force-zero with OCR-first → 2-yr CAGR (fy1→fy3) → earlier base → sentinel 0 priority chain; restored Y4/Y5 `_sg_num`/`_avg_sga_pct` derivation block.
  - **Restore items**: `sga_fy3` restored to `fields_null_after_merge` debug list; `sga_fy{_n}` restored to `_data_present_fyN` flag checks.
  - **Threshold decision**: 60% used (not 30% from spec §4) — Polytek SGA% is 30–34%, so 30% would null all values; 60% catches COGS row misread (≈60–80%) while passing all Polytek values.

- [x] SGA-REMOVAL-2026-03-13: Removed all SG&A extraction logic from `services/llm_service.py` only. All SGA fields now return zero — no dummy/demo data.
  - **EXTRACTION_SCHEMA**: Removed `sga_fy1`, `sga_fy2`, `sga_fy3`, `proj_sga_y1..y5` keys entirely.
  - **`_build_extraction_prompt()`**: Removed SG&A EXTRACTION block, SGA ROW SELECTION RULE block, SGA SIZE CHECK block; removed `sga_fy1/2/3` and `proj_sga_y1-y5` from JSON output schema in prompt; removed 2 SGA items from Section E checklist.
  - **`_extract_revenue_fields_focused()`**: Removed SGA from prompt description and JSON return schema.
  - **`extract_financial_fields()`**: Removed `sga_fy1/2/3` from `_revenue_override_keys`; replaced SGA magnitude sanity check with force-zero block: `for _n in (1,2,3): extracted[f'sga_fy{_n}'] = 0`; removed SGA from cross-table contamination guard; removed `sga_fy{_n}` from `_data_present_fyN` flags; removed `sga_fy3` from `fields_null_after_merge` debug list.
  - **`fill_missing_projections()`**: Removed `sg1, sg2, sg3` historical reads; removed `ocr_sga` OCR list; removed `hist_sg_set`; removed `ocr_sga = _dedup(...)` call; removed `ocr_sga` from `has_ocr_proj` condition; replaced SGA projection calc block with `for i in range(1,6): proj[f'sga_y{i}'] = 0`; removed `_sg_num`, `_avg_sga_pct`, and Y4/Y5 SGA derivation.
  - **DO NOT MODIFY**: `ocr_service.py`, `calculator.py`, `validator.py`, `excel_export.py`, `templates/`, `app.py`, `requirements.txt`, `.env`, `main.js`.

- [x] DEAL-VALUE-RECOMMENDATION-2026-03-13: Added Deal Value input field and Buy/Hold/Not to Buy recommendation.
  - **index.html**: Added `deal_value` number input ($000s) above "Company PDF" section inside upload form.
  - **app.py**: (1) `deal_value = safe_float(request.form.get('deal_value'))` captured in `/upload` before `try`; stored in `session['deal_value']`. (2) `_save_results()` gains `recommendation=None` param, saves it in payload. (3) `_load_results()` returns 4-tuple: `(results, risk_analysis, all_inputs, recommendation)` — ALL callers updated. (4) `/calculate` calls `generate_deal_recommendation(all_inputs, results, deal_value)` after risk analysis; stores `all_inputs['deal_value'] = deal_value` so it survives to results JSON. (5) `/analysis` unpacks 4-tuple, passes `recommendation=recommendation` to template. (6) `/export` unpacks 4-tuple with `_rec` discard.
  - **review.html**: `deal_value` input field added to Deal Terms section (editable, pre-filled from session).
  - **llm_service.py**: `generate_deal_recommendation(all_inputs, results, deal_value) → dict` added after `generate_risk_analysis()`. Verdict is **Python-computed** from thresholds (not LLM): NOT TO BUY if FCCR<1.0 or DSCR<1.0 or MOIC<1.0; BUY if score≥3/4 (MOIC≥2.5x, FCCR≥1.10, DSCR≥1.20, implied multiple≤10x); HOLD otherwise. LLM writes only a 1-sentence rationale citing actual numbers. Returns `{verdict, confidence, rationale, metrics, deal_value, deal_breakers}`.
  - **analysis.html**: Deal Recommendation card appended after Risk Analysis section. Shows: large verdict badge (green=BUY, yellow=HOLD, red=NOT TO BUY/SELL), confidence bar, rationale text, deal-breaker badges, metric tiles color-coded green/yellow/red.

- [x] ADJ-EBITDA-DEEPER-FIX-2026-03-11: Fixed adj_ebitda_fy3 contamination (66K→231K Term Loans on Manta Ray). (FIX-A) adj_ebitda CONFIDENCE GUARD: confidence < 0.70 → nulled. (FIX-B) `calculator.py` Term Loans fallback derives adj_ebitda from reported_ebitda_fy3 + adjustments_fy3 when null/0. (FIX-C) Strengthened "Reported EBITDA" prompt block with label variants, negative-value allowance, all-three-years requirement.

- [x] DYNAMIC-SOURCES-FIX-2026-03-11: 6 fixes to make Sources extraction work for ANY CIM. AR field → "Accounts Receivable from BALANCE SHEET"; adj_ebitda schema → "NEVER from E/F/P/B projection columns"; HARD GUARD: adj_ebitda_fy3 >200% vs (reported+adj) → auto-correct; removed ENH-4 AR smart default; Python guard AR=revenue_fy3 → null; 2 new Section E checklist items.

- [x] COLLATERAL-ROW-LABEL-FIX-2026-03-11: Fixed M&E/Building swap. Replaced VALUE PATTERN RULE with ROW-LABEL-FIRST RULE for both M&E and Building blocks. Updated Section E checklist.

- [x] SOURCES-FIX-SESSION-2026-03-11: 7 fixes across `llm_service.py`, `calculator.py`, `review.html`. M&E multiplier 0→0.50; `leverage_multiple` field added (default 3.5); Term Loans fallback formula; ADJ. EBITDA CONTAMINATION GUARD; COLLATERAL SOURCE PRIORITY block; VALUE TREND rule for M&E; CONSTANT PERIOD VALUES rule for Building; Inventory NO-ASSUMPTION RULE.

- [x] FY-DETECT-FIX2: 2 fixes to `_detect_fiscal_years()`. Guard 3 co-presence check (OCR-stripped E suffix). Extended `_proj_context_pat` regex for keyword co-occurrence detection.

- [x] LLM-EXTRACTION-FIX6: INTEREST EXPENSE EXTRACTION block; Adj. EBITDA 3-option cascade fallback; ANTI-ROW-SHIFT RULE in collateral; 2 new Section E items.

- [x] LLM-EXTRACTION-FIX5: COLLATERAL EXTRACTION RULE (4-col table); ADJUSTMENTS total-row-only rule; ADJ. EBITDA all-three-years instruction.

- [x] LLM-PROMPT-FIX4: `_detect_fiscal_years()` hard-cap guard (confirmed_hist_years); Adj. EBITDA label variants; E-suffix distinction block (Section D); collateral & debt field guidance + 5 Section E items.

- [x] BUG-SESSION-OVERFLOW: Disk-based results persistence (`storage/results/{sid}_results.json`). `_save_results()` / `_load_results()` added to `app.py`. `/analysis` + `/export` use file not session. Cache-Control: no-store headers.

- [x] LLM-V3-EXTRACTION: Dual-axis window scoring (keyword + numeric density); Pass 3 fallback (revenue_fy3 null trigger); robust CAGR fallback chain rev3→rev2→rev1; enhanced debug JSON.

- [x] LLM-V3-HOTFIXES: `client` NameError in `generate_risk_analysis()`; WINDOW 500→1500; dense-block +5 numeric bonus; 9 `print()` → `logger` calls.

- [x] PROMPT-ROBUST-V2: Short-form year labels (yy1/yy2/yy3); 4+ column COLUMN ASSIGNMENT RULE; COLUMNS TO IGNORE (LTM/TTM/NTM); RESTATED vs AS-REPORTED priority. SYSTEM_PROMPT 6→10 rules.

- [x] PROMPT-OPT: `_build_system_prompt(y1,y2,y3)` dynamic year injection; `_call_llm()` optional `system_prompt` param; `_build_extraction_prompt()` full f-string rewrite; EBITDA cross-check; analysis.html dynamic projection headers.

- [x] FY-DETECT: `_detect_fiscal_years()` forced y3=most_recent; Pass 3 last-resort scan; FISCAL YEAR MAPPING restricted to 3 detected years only.

- [x] ENH-9: Excel cell-level protection — ~40 input cells `locked=False`; Sheet1 protected; Transaction Fees tab fully locked.

- [x] BUG-RISK-KEYS: Fixed Risk Analysis empty — wrong `results` dict keys corrected (`C29`=MOIC, `C57_fccr`=FCCR, `dscr` dict, `adj_ebitda` dict).

- [x] BUG-RISK-JSON: Fixed Risk Analysis not showing — improved JSON parsing (handles `{"risks":[]}`, plain array, markdown fences, partial JSON).

- [x] ENH-8: Risk Analysis section — LLM-generated 6 risk factors, `generate_risk_analysis()` added to `llm_service.py`, called in `/calculate`, stored in session.

- [x] BUG-EXCEL-DIV0: `_fopt()` helper returns `None` for absent values; conditional write for all historical + projection financial cells.

- [x] ENH-6: Excel auto-recalculation — `calcMode='auto'` + `fullCalcOnLoad=True`.

- [x] FIX-4: Robust EBITDA calc — `if gm3` replaces `if (gm3 and sga3)`; `doc_ebitda > 0` guard.

- [x] FIX-3: `acquisition_multiple` (×7.0 default) and `pct_acquired` (default 1.0) added to review form Deal Terms.

- [x] FIX-1: Excel Uses section C26/C27/C28 keys corrected.

- [x] ENH-5: Transaction date → `datetime.date.today()` in excel_export.

- [x] ENH-3: Company name editable input on review form.

- [x] ENH-2: `adj_ebitda_fy1/2/3` added to schema; >5% discrepancy cross-check in `validator.py`.

- [x] ENH-1: COGS fallback — GM = revenue − COGS when gross_margin null.

- [x] BUG-6: `max_tokens` 4096→8192; regex fallback in `_parse_llm_json()` for truncated JSON; `finish_reason` logging.

- [x] BUG-SESSION-OVERFLOW (initial): Flask 4KB cookie → disk-based results persistence.

- [x] Various early fixes: Jinja2 macro error, step logging, revenue extraction, projections engine, FY detection, FORBIDDEN sub-revenues, Excel export (~70 cells), BUG-1 string fields, BUG-2/3 projection contamination.

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
- Disk-based results persistence (`storage/results/`) — no database
