"""
Anti-hallucination validation layer.
Validates LLM extracted fields and manual inputs before calculation.
"""

# Confidence thresholds
HIGH_CONFIDENCE = 0.85
MEDIUM_CONFIDENCE = 0.60

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

    # ── V-GP-1 through V-GP-7: Gross Profit validation checks ────────────────
    for _n in (1, 2, 3):
        _gp   = _num(extracted.get(f'gross_margin_fy{_n}'))
        _rev  = _num(extracted.get(f'revenue_fy{_n}'))
        _cogs = _num(extracted.get(f'cogs_fy{_n}'))

        # V-GP-1: GP < Revenue (CRITICAL)
        if _gp is not None and _rev is not None and _rev > 0 and _gp > _rev:
            flags[f'gross_margin_fy{_n}'] = {
                'status': 'error',
                'message': f'GP ({_gp:,.0f}) exceeds Revenue ({_rev:,.0f}). Extraction error — verify row.'
            }

        # V-GP-2: GP > 0
        elif _gp is not None and _gp < 0:
            flags[f'gross_margin_fy{_n}'] = {
                'status': 'warning',
                'message': f'Negative GP ({_gp:,.0f}) for FY{_n}. Review COGS extraction.'
            }

        # V-GP-3: GP margin 5%–95%
        elif _gp is not None and _rev is not None and _rev > 0:
            _margin = _gp / _rev
            if not (0.05 <= _margin <= 0.95):
                flags.setdefault(f'gross_margin_fy{_n}', {
                    'status': 'warning',
                    'message': f'GP margin {_margin:.1%} outside 5–95% band for FY{_n}. Review.'
                })

        # V-GP-4: GP + COGS ≈ Revenue (within 2%)
        if _gp is not None and _cogs is not None and _rev is not None and _rev > 0:
            _diff = abs(_gp + _cogs - _rev) / _rev
            if _diff > 0.02:
                flags[f'cogs_fy{_n}'] = {
                    'status': 'warning',
                    'message': f'GP + COGS ({_gp + _cogs:,.0f}) ≠ Revenue ({_rev:,.0f}) — {_diff:.1%} gap.'
                }

    # V-GP-5: Year-over-year GP margin consistency (>20pp swing)
    _margins_hist = []
    for _n in (1, 2, 3):
        _gp  = _num(extracted.get(f'gross_margin_fy{_n}'))
        _rev = _num(extracted.get(f'revenue_fy{_n}'))
        _margins_hist.append(_gp / _rev if (_gp is not None and _rev and _rev > 0) else None)
    for _i in range(len(_margins_hist) - 1):
        _m1, _m2 = _margins_hist[_i], _margins_hist[_i + 1]
        if _m1 is not None and _m2 is not None and abs(_m2 - _m1) > 0.20:
            flags[f'gross_margin_fy{_i + 2}'] = {
                'status': 'warning',
                'message': f'GP margin swung {abs(_m2 - _m1):.1%} year-over-year. Possible table error.'
            }

    # V-GP-6: gp_source must be set
    if not extracted.get('gp_source'):
        flags['gp_source'] = {
            'status': 'error',
            'message': 'gp_source not set after GP pipeline. Internal error.'
        }

    # V-GP-7: Proj GP < Proj Revenue
    for _i in range(1, 6):
        _pgp  = _num(extracted.get(f'proj_gp_y{_i}'))
        _prev = _num(extracted.get(f'proj_revenue_y{_i}'))
        if _pgp is not None and _prev is not None and _prev > 0 and _pgp > _prev:
            flags[f'proj_gp_y{_i}'] = {
                'status': 'error',
                'message': f'Projected GP ({_pgp:,.0f}) exceeds Proj Revenue ({_prev:,.0f}) Y{_i}.'
            }
    # ── END V-GP checks ───────────────────────────────────────────────────────

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
