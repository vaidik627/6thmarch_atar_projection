"""
Anti-hallucination validation layer.
Validates LLM extracted fields and manual inputs before calculation.
"""

# Confidence thresholds
HIGH_CONFIDENCE = 0.85
MEDIUM_CONFIDENCE = 0.70

# Reasonable financial ranges (in thousands USD)
REVENUE_MIN = 100          # $100K
REVENUE_MAX = 10_000_000   # $10B
MARGIN_MIN = 0
MARGIN_MAX = 100           # percentage
GROWTH_MIN = -50           # -50%
GROWTH_MAX = 500           # +500%


def validate_extracted_fields(extracted: dict, confidences: dict) -> dict:
    """
    Validate LLM-extracted fields.
    Returns a dict of field -> { 'status': 'ok'|'warning'|'error', 'message': str }
    """
    flags = {}

    # Revenue checks
    for key in ['revenue_fy1', 'revenue_fy2', 'revenue_fy3']:
        val = _num(extracted.get(key))
        conf = confidences.get(key) or 0.0
        flags[key] = _check_field(key, val, conf, REVENUE_MIN, REVENUE_MAX, required=False, label='Revenue')

    # Gross Margin must be positive and ≤ revenue
    for i, gm_key in enumerate(['gross_margin_fy1', 'gross_margin_fy2', 'gross_margin_fy3']):
        rev_key = ['revenue_fy1', 'revenue_fy2', 'revenue_fy3'][i]
        gm_val = _num(extracted.get(gm_key))
        rev_val = _num(extracted.get(rev_key))
        conf = confidences.get(gm_key, 0.0)
        flag = _check_field(gm_key, gm_val, conf, 0, REVENUE_MAX, required=False, label='Gross Margin')
        if flag['status'] == 'ok' and rev_val is not None and gm_val is not None:
            if gm_val > rev_val:
                flag = {'status': 'error', 'message': f'Gross Margin ({gm_val:,.0f}) exceeds Revenue ({rev_val:,.0f}) — impossible value.'}
        flags[gm_key] = flag

    # SG&A must be positive
    for key in ['sga_fy1', 'sga_fy2', 'sga_fy3']:
        val = _num(extracted.get(key))
        conf = confidences.get(key) or 0.0
        flags[key] = _check_field(key, val, conf, 0, REVENUE_MAX, required=False, label='SG&A')

    # Interest expense: can be 0 or positive
    for key in ['interest_expense_fy1', 'interest_expense_fy2', 'interest_expense_fy3']:
        val = _num(extracted.get(key))
        conf = confidences.get(key) or 0.0
        flags[key] = _check_field(key, val, conf, 0, REVENUE_MAX, required=False, label='Interest Expense')

    # Collateral values: must be positive
    for key, label in [
        ('net_revenue_collateral', 'Net Revenue Collateral'),
        ('inventory_collateral', 'Inventory Collateral'),
        ('me_equipment_collateral', 'M&E Equipment Collateral'),
        ('building_land_collateral', 'Building & Land Collateral'),
        ('existing_term_loans', 'Existing Term Loans'),
    ]:
        val = _num(extracted.get(key))
        conf = confidences.get(key) or 0.0
        flags[key] = _check_field(key, val, conf, 0, REVENUE_MAX, required=False, label=label)

    # YoY revenue trend check
    rev1 = _num(extracted.get('revenue_fy1'))
    rev2 = _num(extracted.get('revenue_fy2'))
    rev3 = _num(extracted.get('revenue_fy3'))
    if all(v is not None for v in [rev1, rev2, rev3]):
        if rev1 > 0 and rev2 > 0:
            growth = (rev2 - rev1) / rev1 * 100
            if not (GROWTH_MIN <= growth <= GROWTH_MAX):
                flags['revenue_fy2']['status'] = 'warning'
                flags['revenue_fy2']['message'] = f'Revenue growth FY1→FY2 of {growth:.1f}% is unusually high. Please verify.'
        if rev2 > 0 and rev3 > 0:
            growth = (rev3 - rev2) / rev2 * 100
            if not (GROWTH_MIN <= growth <= GROWTH_MAX):
                flags['revenue_fy3']['status'] = 'warning'
                flags['revenue_fy3']['message'] = f'Revenue growth FY2→FY3 of {growth:.1f}% is unusually high. Please verify.'

    # ENH-2: Adj. EBITDA cross-check — flag if calculated EBITDA differs >5% from document-stated
    for n in (1, 2, 3):
        doc_ebitda = _num(extracted.get(f'adj_ebitda_fy{n}'))
        gm = _num(extracted.get(f'gross_margin_fy{n}'))
        sg = _num(extracted.get(f'sga_fy{n}'))
        adj = _num(extracted.get(f'adjustments_fy{n}')) or 0.0
        if doc_ebitda and gm is not None and sg is not None:
            calc = gm - sg + adj
            pct_diff = abs(calc - doc_ebitda) / abs(doc_ebitda)
            if pct_diff > 0.05:
                flags[f'adj_ebitda_fy{n}'] = {
                    'status': 'warning',
                    'message': f'Calculated EBITDA ({calc:,.0f}) differs {pct_diff:.0%} from document-stated ({doc_ebitda:,.0f}). Check GM or SG&A.',
                }

    return flags


def validate_manual_inputs(form_data: dict) -> dict:
    """
    Validate manually-entered fields from the review form.
    Returns a dict of field -> error message (empty dict = no errors).
    """
    errors = {}

    def require_positive(key, label):
        val = _num(form_data.get(key))
        if val is None:
            pass  # Optional fields allowed to be blank
        elif val < 0:
            errors[key] = f'{label} must be a positive number.'

    def require_rate(key, label, min_val=0, max_val=1):
        val = _num(form_data.get(key))
        if val is None:
            pass
        elif not (min_val <= val <= max_val):
            errors[key] = f'{label} must be between {min_val} and {max_val}.'

    def require_positive_int(key, label):
        val = _num(form_data.get(key))
        if val is None:
            pass
        elif val <= 0:
            errors[key] = f'{label} must be a positive number.'

    # Advance rates (0-1)
    require_rate('net_revenue_multiplier', 'Net Revenue Advance Rate')
    require_rate('inventory_multiplier', 'Inventory Advance Rate')
    require_rate('building_land_multiplier', 'Building & Land Advance Rate')

    # Positive amounts
    require_positive('transaction_fees_total', 'Transaction Fees')
    require_positive('working_capital_change', 'Working Capital Change')

    # Rates
    require_rate('capex_pct_availability', 'CAPEX % of Availability', 0, 1)
    require_rate('depreciation_rate', 'Depreciation Rate', 0, 0.5)
    require_rate('mgmt_ltip_rate', 'Mgmt LTIP Rate', 0, 0.5)
    require_rate('atar_ownership_rate', 'Atar Ownership Rate', 0, 0.5)
    require_rate('lp_pct', 'LP %', 0, 0.5)
    require_rate('preferred_pct', 'Preferred %', 0, 0.5)
    require_rate('fccr_rate', 'FCCR Rate', 0, 1)
    require_rate('remaining_cash_pct', 'Remaining Cash %', 0, 1)

    # Years must be positive
    require_positive_int('return_of_equity_years', 'Return of Equity Years')
    require_positive_int('atar_repayment_years', 'Atar Repayment Years')

    return errors


def get_confidence_class(confidence) -> str:
    """Return Bootstrap badge class based on confidence score."""
    conf = confidence or 0.0
    if conf >= HIGH_CONFIDENCE:
        return 'success'
    elif conf >= MEDIUM_CONFIDENCE:
        return 'warning'
    else:
        return 'danger'


def get_flag_class(flag: dict) -> str:
    """Return Bootstrap alert class based on validation flag."""
    status = flag.get('status', 'ok')
    return {'ok': 'success', 'warning': 'warning', 'error': 'danger'}.get(status, 'secondary')


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _num(val):
    """Safe conversion to float, returns None if not convertible."""
    if val is None or val == '' or val == 'null':
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _check_field(key, val, conf, min_val, max_val, required, label):
    """Build a flag dict for a single field."""
    if val is None:
        if required:
            return {'status': 'error', 'message': f'{label} is required but was not found in the document.'}
        return {'status': 'ok', 'message': 'Not found in document — enter manually.'}

    if val < min_val or val > max_val:
        return {'status': 'error', 'message': f'{label} value {val:,.0f} is outside expected range ({min_val:,.0f}–{max_val:,.0f}). Please verify.'}

    if conf < MEDIUM_CONFIDENCE:
        return {'status': 'error', 'message': f'Low confidence extraction ({conf:.0%}). Please verify this value.'}
    elif conf < HIGH_CONFIDENCE:
        return {'status': 'warning', 'message': f'Medium confidence ({conf:.0%}). Review recommended.'}

    return {'status': 'ok', 'message': f'Extracted with high confidence ({conf:.0%}).'}
