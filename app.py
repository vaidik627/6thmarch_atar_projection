import os
import uuid
import json
import datetime
import logging
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify, send_file, make_response
from werkzeug.utils import secure_filename
from dotenv import load_dotenv
from openai import OpenAI

from services.ocr_service import extract_text_from_pdf, verify_ocr_connection
from services.llm_service import extract_financial_fields, verify_llm_connection, fill_missing_projections, generate_risk_analysis
from services.calculator import run_calculations
from services.validator import validate_extracted_fields, validate_manual_inputs
from services.excel_export import generate_excel

load_dotenv()

# ── Logging setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  [%(name)s]  %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger('app')

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'atar-docanalyzer-secret-2026')
app.config['MAX_CONTENT_LENGTH'] = int(os.getenv('MAX_FILE_SIZE_MB', 50)) * 1024 * 1024

UPLOAD_FOLDER = os.getenv('UPLOAD_FOLDER', 'uploads')
RESULTS_FOLDER = os.path.join('storage', 'results')
ALLOWED_EXTENSIONS = {'pdf'}

# ── Template filters ──────────────────────────────────────────────────────────
@app.template_filter('fmt_num')
def fmt_num(value):
    """Format a number with commas and 2 decimal places."""
    try:
        v = float(value)
        return f'{v:,.2f}'
    except (TypeError, ValueError):
        return str(value)

@app.template_filter('format_number')
def format_number_filter(value):
    """Format integer with commas."""
    try:
        return f'{int(value):,}'
    except (TypeError, ValueError):
        return str(value)


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def _save_results(session_id: str, results: dict, risk_analysis: list, all_inputs: dict = None) -> None:
    """Persist calculation results to disk — avoids Flask 4KB cookie overflow."""
    os.makedirs(RESULTS_FOLDER, exist_ok=True)
    payload = {
        'session_id': session_id,
        'results': results,
        'risk_analysis': risk_analysis,
        'all_inputs': all_inputs or {},
    }
    path = os.path.join(RESULTS_FOLDER, f"{session_id}_results.json")
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)


def _load_results(session_id: str) -> tuple:
    """Load calculation results from disk. Returns (results, risk_analysis, all_inputs) or ({}, [], {})."""
    path = os.path.join(RESULTS_FOLDER, f"{session_id}_results.json")
    try:
        with open(path, 'r', encoding='utf-8') as f:
            payload = json.load(f)
        return payload.get('results', {}), payload.get('risk_analysis', []), payload.get('all_inputs', {})
    except (FileNotFoundError, json.JSONDecodeError):
        return {}, [], {}


# Fields that must remain strings after form submission (not converted to float)
_STRING_FORM_FIELDS = {'company_name', 'proj_source'}


def safe_float(val):
    """
    Safely convert any form value to float or None.
    Handles: None, '', 'None', 'null', whitespace, comma-formatted numbers.
    Returns None for missing/blank — never returns 0 for a missing value.
    0.0 is only returned when the input is literally '0' or 0.
    """
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if s == '' or s.lower() in ('none', 'null', 'n/a', '-'):
        return None
    try:
        return float(s.replace(',', ''))
    except (ValueError, TypeError):
        return None


@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload():
    if 'pdf_file' not in request.files:
        flash('No file selected.', 'danger')
        return redirect(url_for('index'))

    file = request.files['pdf_file']
    if file.filename == '':
        flash('No file selected.', 'danger')
        return redirect(url_for('index'))

    if not allowed_file(file.filename):
        flash('Only PDF files are allowed.', 'danger')
        return redirect(url_for('index'))

    filename = secure_filename(file.filename)
    session_id = str(uuid.uuid4())[:8]
    save_path = os.path.join(UPLOAD_FOLDER, f"{session_id}_{filename}")
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    file.save(save_path)

    try:
        logger.info("=" * 60)
        logger.info(f"NEW REQUEST  session={session_id}  file={filename}")
        logger.info("=" * 60)

        # Step 1: OCR
        logger.info("STEP 1/3 ▶ OCR — sending PDF to Google Document AI…")
        t0 = __import__('time').time()
        ocr_text = extract_text_from_pdf(save_path, session_id)
        logger.info(f"STEP 1/3 ✔ OCR complete — {len(ocr_text):,} chars extracted  ({__import__('time').time()-t0:.1f}s)")

        # Step 2: LLM field extraction
        logger.info("STEP 2/3 ▶ LLM — sending OCR text to NVIDIA NIM for field extraction…")
        t1 = __import__('time').time()
        extracted, confidences, citations, detected_fy_years = extract_financial_fields(ocr_text, session_id)
        found = sum(1 for v in extracted.values() if v is not None)
        logger.info(f"STEP 2/3 ✔ LLM complete — {found}/{len(extracted)} fields extracted  ({__import__('time').time()-t1:.1f}s)")

        # Step 3: Validate extracted fields
        logger.info("STEP 3/3 ▶ Validation — running anti-hallucination checks…")
        validation_flags = validate_extracted_fields(extracted, confidences)
        warnings = sum(1 for f in validation_flags.values() if isinstance(f, dict) and f.get('status') in ('warning', 'error'))
        logger.info(f"STEP 3/3 ✔ Validation complete — {warnings} flag(s) raised")

        # Build 5-year projections (from OCR if available, else auto-calculated)
        logger.info("PROJECTIONS ▶ Building 5-year projections…")
        projections, proj_source = fill_missing_projections(extracted)
        logger.info(f"PROJECTIONS ✔ Source: {proj_source} — {sum(1 for v in projections.values() if v is not None)} values populated")

        # Inject deal-term defaults not provided by LLM extraction (Jira: default 5x, D7=75%)
        if extracted.get('acquisition_multiple') is None:
            extracted['acquisition_multiple'] = 5.0
        if extracted.get('net_revenue_multiplier') is None:
            extracted['net_revenue_multiplier'] = 0.75

        # ── GP Fallback Chain (Tiers 1–4) ────────────────────────────────────
        # Runs after extract_financial_fields() — fills any remaining null GP fields.
        # Uses gross_margin_fyN as the GP key (codebase naming convention).
        for _yr in ('fy1', 'fy2', 'fy3'):
            _gp_key   = f'gross_margin_{_yr}'
            _cogs_key = f'cogs_{_yr}'
            _rev_key  = f'revenue_{_yr}'
            # Tier 1: GP = Revenue − COGS
            if (extracted.get(_gp_key) is None
                    and extracted.get(_cogs_key) is not None
                    and extracted.get(_rev_key) is not None):
                _calc_gp = extracted[_rev_key] - extracted[_cogs_key]
                if _calc_gp > 0:
                    extracted[_gp_key] = round(_calc_gp, 2)
                    extracted.setdefault('gp_source', 'GP=Sales-COGS')
                    logger.info(f"  GP Tier 1: {_gp_key}={_calc_gp:,.0f} (Revenue-COGS)")
                else:
                    validation_flags[_gp_key] = {
                        'status': 'warning',
                        'message': f'Calculated GP is negative ({_calc_gp:,.0f}). Review COGS.'
                    }

        # Tier 2: COGS from inventory components (only when GP and COGS both still null)
        _needs_inv = any(
            extracted.get(f'gross_margin_{yr}') is None
            and extracted.get(f'cogs_{yr}') is None
            for yr in ('fy1', 'fy2', 'fy3')
        )
        if _needs_inv:
            from services.cogs_extractor import extract_inventory_text
            from services.llm_service import _extract_inventory_fields_focused
            _inv_fy_years = tuple(session.get('detected_fy_years', [0, 0, 0]))
            _inv_client = OpenAI(
                base_url=os.getenv('NVIDIA_BASE_URL', 'https://integrate.api.nvidia.com/v1'),
                api_key=os.getenv('NVIDIA_API_KEY')
            )
            _scoped_inv = extract_inventory_text(ocr_text)
            _inv_json = (
                _extract_inventory_fields_focused(_inv_client, _scoped_inv, _inv_fy_years)
                if _scoped_inv else None
            )
            if _inv_json:
                for _k, _v in _inv_json.items():
                    if _v is not None:
                        extracted[_k] = _v

            for _yr, _prev in (('fy1', 'fy0'), ('fy2', 'fy1'), ('fy3', 'fy2')):
                _gp_key   = f'gross_margin_{_yr}'
                _cogs_key = f'cogs_{_yr}'
                _rev_key  = f'revenue_{_yr}'
                if extracted.get(_gp_key) is not None:
                    continue
                _end_inv   = extracted.get(f'end_inventory_{_yr}')
                _beg_inv   = extracted.get(f'end_inventory_{_prev}')
                _purchases = extracted.get(f'purchases_{_yr}')
                if _beg_inv is None and _end_inv is not None:
                    _beg_inv = round(_end_inv * 0.95, 2)
                    extracted.setdefault('gp_source', 'COGS=Inventory Calc (estimated beg. inventory)')
                else:
                    extracted.setdefault('gp_source', 'COGS=Inventory Calc')
                if _purchases is None:
                    continue
                if _beg_inv is not None and _end_inv is not None:
                    _cogs_calc = _beg_inv + _purchases - _end_inv
                    if _cogs_calc > 0:
                        extracted[_cogs_key] = round(_cogs_calc, 2)
                        if extracted.get(_rev_key) is not None:
                            extracted[_gp_key] = round(extracted[_rev_key] - _cogs_calc, 2)
                            logger.info(f"  GP Tier 2: {_gp_key}={extracted[_gp_key]:,.0f} (inv calc)")

        # Tier 4: GP Margin Average (last resort)
        _known_margins = []
        for _yr in ('fy1', 'fy2', 'fy3'):
            _gp  = extracted.get(f'gross_margin_{_yr}')
            _rev = extracted.get(f'revenue_{_yr}')
            if _gp is not None and _rev and _rev > 0:
                _known_margins.append(_gp / _rev)
        if _known_margins:
            _avg_margin = max(0.05, min(sum(_known_margins) / len(_known_margins), 0.95))
            for _yr in ('fy1', 'fy2', 'fy3'):
                if extracted.get(f'gross_margin_{_yr}') is None:
                    _rev = extracted.get(f'revenue_{_yr}')
                    if _rev:
                        extracted[f'gross_margin_{_yr}'] = round(_rev * _avg_margin, 2)
                        extracted.setdefault('gp_source', 'GP Margin Average')
                        logger.info(f"  GP Tier 4: gross_margin_{_yr}={extracted[f'gross_margin_{_yr}']:,.0f}")
        else:
            for _yr in ('fy1', 'fy2', 'fy3'):
                if extracted.get(f'gross_margin_{_yr}') is None:
                    validation_flags[f'gross_margin_{_yr}'] = {
                        'status': 'error',
                        'message': 'Gross Profit could not be extracted or calculated. Manual entry required.'
                    }

        # Projection fallback: apply avg GP margin to proj_revenue_y1..y5
        if not extracted.get('proj_gp_y1'):
            _proj_margins = []
            for _yr in ('fy1', 'fy2', 'fy3'):
                _gp  = extracted.get(f'gross_margin_{_yr}')
                _rev = extracted.get(f'revenue_{_yr}')
                if _gp is not None and _rev and _rev > 0:
                    _proj_margins.append(_gp / _rev)
            if _proj_margins:
                _avg_proj_m = max(0.05, min(sum(_proj_margins) / len(_proj_margins), 0.95))
                for _i in range(1, 6):
                    _proj_rev = extracted.get(f'proj_revenue_y{_i}')
                    if _proj_rev:
                        extracted[f'proj_gp_y{_i}'] = round(_proj_rev * _avg_proj_m, 2)

        # Derive GP margin % for all historical years
        for _yr in ('fy1', 'fy2', 'fy3'):
            _gp  = extracted.get(f'gross_margin_{_yr}')
            _rev = extracted.get(f'revenue_{_yr}')
            if _gp is not None and _rev and _rev > 0:
                extracted[f'gp_margin_{_yr}'] = round((_gp / _rev) * 100, 2)

        # Ensure gp_source is always set
        if not extracted.get('gp_source'):
            _any_gp = any(extracted.get(f'gross_margin_{yr}') is not None for yr in ('fy1', 'fy2', 'fy3'))
            extracted['gp_source'] = 'CIM Extracted' if _any_gp else 'Not Available'
        # ── END GP Fallback Chain ─────────────────────────────────────────────

        # Store in session
        session['session_id'] = session_id
        session['extracted'] = extracted
        session['confidences'] = confidences
        session['citations'] = citations
        session['validation_flags'] = validation_flags
        session['pdf_filename'] = filename
        session['projections'] = projections
        session['proj_source'] = proj_source
        session['detected_fy_years'] = list(detected_fy_years)  # [y1, y2, y3] integers

        return redirect(url_for('review'))

    except Exception as e:
        flash(f'Processing error: {str(e)}', 'danger')
        return redirect(url_for('index'))


@app.route('/review', methods=['GET'])
def review():
    if 'extracted' not in session:
        flash('Please upload a PDF first.', 'warning')
        return redirect(url_for('index'))

    return render_template(
        'review.html',
        extracted=session.get('extracted', {}),
        confidences=session.get('confidences', {}),
        citations=session.get('citations', {}),
        validation_flags=session.get('validation_flags', {}),
        pdf_filename=session.get('pdf_filename', ''),
        proj_defaults=session.get('projections', {}),
        proj_source=session.get('proj_source', 'calculated'),
    )


@app.route('/calculate', methods=['POST'])
def calculate():
    if 'extracted' not in session:
        flash('Session expired. Please upload again.', 'warning')
        return redirect(url_for('index'))

    form_data_raw = request.form.to_dict()

    # Convert every form value from string to float|None before merging.
    # safe_float() handles: empty string → None, comma-formatted '23,500' → 23500.0,
    # 'null'/'None' → None. String fields (company_name) are preserved as-is.
    form_data = {}
    for key, val in form_data_raw.items():
        if key in _STRING_FORM_FIELDS:
            form_data[key] = val.strip() if val.strip() else None
        else:
            form_data[key] = safe_float(val)

    # Validate manual inputs
    manual_errors = validate_manual_inputs(form_data)
    if manual_errors:
        flash('Please fix the highlighted errors.', 'danger')
        return render_template(
            'review.html',
            extracted=session.get('extracted', {}),
            confidences=session.get('confidences', {}),
            citations=session.get('citations', {}),
            validation_flags=session.get('validation_flags', {}),
            pdf_filename=session.get('pdf_filename', ''),
            proj_defaults=session.get('projections', {}),
            proj_source=session.get('proj_source', 'calculated'),
            detected_fy_years=session.get('detected_fy_years', []),
            manual_errors=manual_errors,
            form_data=form_data,
        )

    # Merge extracted + manual inputs.
    # Start from extracted (all floats/None from LLM), then overlay form values.
    # Only non-None form values override extracted — empty form fields keep extracted value.
    all_inputs = dict(session.get('extracted', {}))
    for k, v in form_data.items():
        if v is not None:
            all_inputs[k] = v

    # Run full calculation engine
    logger.info("CALCULATE ▶ Running full Excel formula engine…")
    results = run_calculations(all_inputs)
    logger.info("CALCULATE ✔ Calculations complete")

    # Generate risk analysis
    logger.info("RISK ▶ Generating risk analysis…")
    risk_analysis = generate_risk_analysis(all_inputs, results)
    logger.info(f"RISK ✔ {len(risk_analysis)} risk factor(s) identified — redirecting to analysis")

    _save_results(session['session_id'], results, risk_analysis, all_inputs)
    session['calc_complete'] = True
    session['calc_timestamp'] = datetime.datetime.utcnow().isoformat()

    return redirect(url_for('analysis'))


@app.route('/analysis', methods=['GET'])
def analysis():
    sid = session.get('session_id')
    if not sid:
        return redirect(url_for('index'))
    results, risk_analysis, all_inputs = _load_results(sid)
    if not results:
        # No results file found — redirect back to review
        return redirect(url_for('review'))

    response = make_response(render_template(
        'analysis.html',
        results=results,
        all_inputs=all_inputs,
        pdf_filename=session.get('pdf_filename', ''),
        detected_fy_years=session.get('detected_fy_years', []),
        risk_analysis=risk_analysis,
    ))
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


@app.route('/export', methods=['GET'])
def export():
    sid = session.get('session_id')
    if not sid:
        flash('No analysis available. Please start over.', 'warning')
        return redirect(url_for('index'))
    results, _risk, all_inputs = _load_results(sid)
    if not results:
        flash('No analysis available. Please start over.', 'warning')
        return redirect(url_for('index'))

    detected_fy_years = session.get('detected_fy_years', [])

    buf = generate_excel(all_inputs, results, detected_fy_years)

    company = all_inputs.get('company_name') or 'Company'
    safe_name = ''.join(c for c in company if c.isalnum() or c in ' _-')
    filename = f'Prebid Analysis - {safe_name}.xlsx'

    return send_file(buf, as_attachment=True, download_name=filename,
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


@app.route('/reset', methods=['GET'])
def reset():
    session.clear()
    return redirect(url_for('index'))


@app.route('/verify/ocr', methods=['GET'])
def verify_ocr():
    """Real-time OCR credentials + API connectivity check."""
    result = verify_ocr_connection()
    return jsonify(result)


@app.route('/verify/llm', methods=['GET'])
def verify_llm():
    """Real-time NVIDIA NIM API key + model connectivity check."""
    result = verify_llm_connection()
    return jsonify(result)


if __name__ == '__main__':
    debug = os.getenv('FLASK_DEBUG', 'True').lower() == 'true'
    app.run(debug=debug, port=5000, use_reloader=False)
