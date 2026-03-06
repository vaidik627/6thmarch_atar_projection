"""
Replicates all formulas from Prebid V31 Template.xlsx
All monetary values are in thousands of USD.
"""


def _f(val, default=0.0):
    """Safe float conversion. Returns default (0.0) for missing/invalid values.
    Strips commas so '23,500' converts correctly instead of returning 0."""
    if val is None:
        return default
    if isinstance(val, (int, float)):
        v = float(val)
        return v if v == v else default  # NaN check
    s = str(val).strip()
    if s == '' or s.lower() in ('none', 'null'):
        return default
    try:
        return float(s.replace(',', ''))
    except (ValueError, TypeError):
        return default


def safe_num(val, default=None):
    """Like _f() but returns None (not 0) for missing/empty values.
    Use when a missing input should propagate as None rather than silently become 0."""
    if val is None:
        return default
    if isinstance(val, (int, float)):
        v = float(val)
        return v if v == v else default  # NaN check
    s = str(val).strip()
    if s == '' or s.lower() in ('none', 'null', 'n/a', '-'):
        return default
    try:
        return float(s.replace(',', ''))
    except (ValueError, TypeError):
        return default


def run_calculations(inputs: dict) -> dict:
    """
    Run the full Atar Capital prebid analysis calculation engine.
    Inputs dict contains both extracted and manually-entered fields.
    Returns a results dict with all computed values for display.
    """

    # ─────────────────────────────────────────────
    # TRANSACTION FEES SHEET
    # ─────────────────────────────────────────────
    debt_sourcing_rate = _f(inputs.get('debt_sourcing_rate', 0.0075))
    lawyers_rate = _f(inputs.get('lawyers_rate', 0.0075))
    qof_e_diligence = _f(inputs.get('qof_e_diligence', 0))
    tax_fee = _f(inputs.get('tax_fee', 0))
    rw_insurance = _f(inputs.get('rw_insurance', 0))
    atar_bonuses_senior = _f(inputs.get('atar_bonuses_senior', 0))
    atar_bonuses_junior = _f(inputs.get('atar_bonuses_junior', 0))
    project_other = _f(inputs.get('project_other', 0))

    # ─────────────────────────────────────────────
    # SOURCES (Left side, Column C = raw values, D = multiplier, E = computed)
    # ─────────────────────────────────────────────
    C7 = _f(inputs.get('net_revenue_collateral', 0))   # Net Revenue (collateral)
    D7 = _f(inputs.get('net_revenue_multiplier', 0.75))
    E7 = C7 * D7  # Net Revenue source amount

    C8 = _f(inputs.get('inventory_collateral', 0))     # Inventory (collateral)
    D8 = _f(inputs.get('inventory_multiplier', 0.70))
    E8 = C8 * D8  # Inventory source amount

    C9 = _f(inputs.get('me_equipment_collateral', 0))  # M&E Equipment
    D9 = _f(inputs.get('me_equipment_multiplier', 0))
    E9 = C9 * D9

    C11 = _f(inputs.get('building_land_collateral', 0))  # Building & Land
    D11 = _f(inputs.get('building_land_multiplier', 0.50))
    E11 = C11 * D11

    C12 = _f(inputs.get('existing_term_loans', 0))    # Term Loans/Cashflow loans
    E12 = C12

    C14 = _f(inputs.get('seller_note', 0))             # Seller Note
    E14 = C14

    C16 = _f(inputs.get('earnout', 0))                 # Earnout
    E16 = C16

    C18 = _f(inputs.get('equity_roll_from_seller', 0)) # Equity Roll From Seller
    E18 = C18

    # Transaction fees cross-references (from Transaction Fees sheet)
    # E6 = debt_sourcing_rate * SUM(E7:E9)
    tf_debt_sourcing = debt_sourcing_rate * (E7 + E8 + E9)
    # E8_tf = lawyers_rate * SUM(E11:E12)
    tf_lawyers = lawyers_rate * (E11 + E12)
    # E17 = SUM of all transaction fee items
    tf_total = tf_debt_sourcing + tf_lawyers + qof_e_diligence + tax_fee + rw_insurance + atar_bonuses_senior + atar_bonuses_junior + project_other

    # ─────────────────────────────────────────────
    # USES (Left side)
    # ─────────────────────────────────────────────
    C26 = _f(inputs.get('transaction_fees_total', 0))  # Transaction Fees
    C27 = _f(inputs.get('working_capital_change', 7))  # Working Capital Change
    C28 = _f(inputs.get('cfads_factor', 1))            # Cashflow Available for Debt Service factor

    # DEBT SERVICE = C26 * C27 * C28
    E30 = C26 * C27 * C28

    # ─────────────────────────────────────────────
    # PROJECTIONS - Right side (FY19, FY20, FY21, Y1..Y5)
    # ─────────────────────────────────────────────
    # Historical years (FY19=I, FY20=J, FY21=K)
    I7  = _f(inputs.get('revenue_fy1', 0))
    J7  = _f(inputs.get('revenue_fy2', 0))
    K7  = _f(inputs.get('revenue_fy3', 0))
    # Year 1-5 projections
    L7  = _f(inputs.get('revenue_y1', 0))
    M7  = _f(inputs.get('revenue_y2', 0))
    N7  = _f(inputs.get('revenue_y3', 0))
    O7  = _f(inputs.get('revenue_y4', 0))
    P7  = _f(inputs.get('revenue_y5', 0))

    # Growth Rates
    def growth(curr, prev):
        return (curr - prev) / prev if prev != 0 else 0.0

    J8  = growth(J7, I7)
    K8  = growth(K7, J7)
    L8  = growth(L7, K7)
    M8  = growth(M7, L7)
    N8  = growth(N7, M7)
    O8  = growth(O7, N7)
    P8  = growth(P7, O7)

    # Gross Margin
    I10 = _f(inputs.get('gross_margin_fy1', 0))
    J10 = _f(inputs.get('gross_margin_fy2', 0))
    K10 = _f(inputs.get('gross_margin_fy3', 0))
    L10 = _f(inputs.get('gross_margin_y1', 0))
    M10 = _f(inputs.get('gross_margin_y2', 0))
    N10 = _f(inputs.get('gross_margin_y3', 0))
    O10 = _f(inputs.get('gross_margin_y4', 0))
    P10 = _f(inputs.get('gross_margin_y5', 0))

    def gm_pct(gm, rev):
        return gm / rev if rev != 0 else 0.0

    I11 = gm_pct(I10, I7)
    J11 = gm_pct(J10, J7)
    K11 = gm_pct(K10, K7)
    L11 = gm_pct(L10, L7)
    M11 = gm_pct(M10, M7)
    N11 = gm_pct(N10, N7)
    O11 = gm_pct(O10, O7)
    P11 = gm_pct(P10, P7)

    # SG&A
    I13 = _f(inputs.get('sga_fy1', 0))
    J13 = _f(inputs.get('sga_fy2', 0))
    K13 = _f(inputs.get('sga_fy3', 0))
    L13 = _f(inputs.get('sga_y1', 0))
    M13 = _f(inputs.get('sga_y2', 0))
    N13 = _f(inputs.get('sga_y3', 0))
    O13 = _f(inputs.get('sga_y4', 0))
    P13 = _f(inputs.get('sga_y5', 0))

    # Operating Income = Gross Margin - SG&A
    I15 = I10 - I13
    J15 = J10 - J13
    K15 = K10 - K13
    L15 = L10 - L13
    M15 = M10 - M13
    N15 = N10 - N13
    O15 = O10 - O13
    P15 = P10 - P13

    # 1X Expenses / Adjustments (non-recurring)
    I17 = _f(inputs.get('adjustments_fy1', 0))
    J17 = _f(inputs.get('adjustments_fy2', 0))
    K17 = _f(inputs.get('adjustments_fy3', 0))
    L17 = _f(inputs.get('adjustments_y1', 0))
    M17 = _f(inputs.get('adjustments_y2', 0))
    N17 = _f(inputs.get('adjustments_y3', 0))
    O17 = _f(inputs.get('adjustments_y4', 0))
    P17 = _f(inputs.get('adjustments_y5', 0))

    # Adj. EBITDA = Operating Income + 1X Adjustments
    # Historical: SUM(I15:I17), Projections: L15 + L17
    I18 = I15 + I17
    J18 = J15 + J17
    K18 = K15 + K17
    L18 = L15 + L17
    M18 = M15 + M17
    N18 = N15 + N17
    O18 = O15 + O17
    P18 = P15 + P17

    def ebitda_pct(ebitda, rev):
        return ebitda / rev if rev != 0 else 0.0

    I19 = ebitda_pct(I18, I7)
    J19 = ebitda_pct(J18, J7)
    K19 = ebitda_pct(K18, K7)
    L19 = ebitda_pct(L18, L7)
    M19 = ebitda_pct(M18, M7)
    N19 = ebitda_pct(N18, N7)
    O19 = ebitda_pct(O18, O7)
    P19 = ebitda_pct(P18, P7)

    # Interest expense/(income)
    I21 = _f(inputs.get('interest_expense_fy1', 0))
    J21 = _f(inputs.get('interest_expense_fy2', 0))
    K21 = _f(inputs.get('interest_expense_fy3', 0))
    L21 = _f(inputs.get('interest_expense_y1', 0))
    M21 = _f(inputs.get('interest_expense_y2', 0))
    N21 = _f(inputs.get('interest_expense_y3', 0))
    O21 = _f(inputs.get('interest_expense_y4', 0))
    P21 = _f(inputs.get('interest_expense_y5', 0))

    # Depreciation/amortization rate parameters
    H39 = _f(inputs.get('depreciation_rate', 0.045))
    H40 = _f(inputs.get('mgmt_ltip_rate', 0.055))
    H41 = _f(inputs.get('atar_ownership_rate', 0.05))
    H45 = _f(inputs.get('return_of_equity_years', 3))
    H46 = _f(inputs.get('atar_repayment_years', 4))

    # Depreciation expense per year (L22 = -L42, which we compute below)
    # L39 = -SUM(E7:E9) * H39
    L39 = -(E7 + E8 + E9) * H39
    # Subsequent years use rolling average of previous year's book value (approximate)
    # From Excel: M39 = -AVERAGE(L57) * H39, etc. — simplified as fixed proportion
    # We replicate the actual Excel logic:
    # The Excel uses AVERAGE(L57)*H39 which is running inventory avg;
    # for simplicity we apply the same rate to each year's projected revenue base
    def depreciation_year(prev_revenue, prev_depr, rate):
        return -prev_revenue * rate

    M39 = -(L7 * H39)
    N39 = -(M7 * H39)
    O39 = -(N7 * H39)
    P39 = -(O7 * H39)

    # L40 = -SUM(E11:E12) * H40 (Mgmt LTIP: building+term loans)
    # Corrected below after L45 is computed; placeholder here
    L40 = -(E11 + E12) * H40

    # L41 = -E14 * H41 (Atar Ownership: Seller Note)
    L41 = -E14 * H41

    # CAPEX parameters
    H26 = _f(inputs.get('capex_pct_availability', 0.30))

    # ABL (Interest) line for projections = Adj. EBITDA
    L31 = L18
    M31 = M18
    N31 = N18
    O31 = O18
    P31 = P18

    # Term loan repayments (manually entered, typically negative)
    L32 = _f(inputs.get('term_loan_y1', 0))
    M32 = _f(inputs.get('term_loan_y2', 0))
    N32 = _f(inputs.get('term_loan_y3', 0))
    O32 = _f(inputs.get('term_loan_y4', 0))
    P32 = _f(inputs.get('term_loan_y5', 0))

    # Availability (CFADS) = EBITDA - Interest - Depreciation - Other
    # L24 = L18 - SUM(L21:L23) where L22 = depreciation, L23 = empty in template
    L22 = -L39  # Depreciation (positive value, negated later)
    M22 = -M39
    N22 = -N39
    O22 = -O39
    P22 = -P39

    L24 = L18 - (L21 + L22)
    M24 = M18 - (M21 + M22)
    N24 = N18 - (N21 + N22)
    O24 = O18 - (O21 + O22)
    P24 = P18 - (P21 + P22)

    # CAPEX = IF(H26 * -Availability < 0, H26 * -Availability, 0)
    def calc_capex(avail, rate):
        v = rate * (-avail)
        return v if v < 0 else 0.0

    L26 = calc_capex(L24, H26)
    M26 = calc_capex(M24, H26)
    N26 = calc_capex(N24, H26)
    O26 = calc_capex(O24, H26)
    P26 = calc_capex(P24, H26)

    # Total Uses (projection) = Availability + CAPEX
    L27 = L24 + L26
    M27 = M24 + M26
    N27 = N24 + N26
    O27 = O24 + O26
    P27 = P24 + P26

    # EV At Exit (L33)
    # L33 = SUM(E7:E8) * -L8 * 0.2
    L33 = (E7 + E8) * (-L8) * 0.2
    M33 = (L33 * (1 + M8) - L33) * 0.2
    N33 = (M33 * (1 + N8) - M33) * 0.2
    O33 = (N33 * (1 + O8) - N33) * 0.2
    P33 = (O33 * (1 + P8) - O33) * 0.2

    # Exit Multiple row = CAPEX (L34 = L26)
    L34 = L26
    M34 = M26
    N34 = N26
    O34 = O26
    P34 = P26

    # + Cash = SUM(ABL + Term + EV + CAPEX)
    L35 = L31 + L32 + L33 + L34
    M35 = M31 + M32 + M33 + M34
    N35 = N31 + N32 + N33 + N34
    O35 = O31 + O32 + O33 + O34
    P35 = P31 + P32 + P33 + P34

    # Debt amortization (L39 already computed above)
    # L40: Mgmt LTIP amortization
    L40 = -(E11 + E12) * H40
    M40 = -(E11 + E12) * H40
    N40 = -(E11 + E12) * H40
    O40 = -(E11 + E12) * H40
    P40 = -(E11 + E12) * H40

    # L41: Atar Ownership amortization
    L41 = -E14 * H41
    M41 = -E14 * H41
    N41 = -E14 * H41
    O41 = -E14 * H41
    P41 = -E14 * H41

    # Cashflow available for Earnout / Management Fees
    L42 = L39 + L40 + L41
    M42 = M39 + M40 + M41
    N42 = N39 + N40 + N41
    O42 = O39 + O40 + O41
    P42 = P39 + P40 + P41

    # Return of Equity series
    # L45 = -SUM(E11:E12) / H45
    L45 = -(E11 + E12) / H45 if H45 != 0 else 0.0
    M45 = -(E11 + E12) / H45 if (E11 + E12 + L45) > 0 and H45 != 0 else 0.0
    N45 = -(E11 + E12) / H45 if (E11 + E12 + L45 + M45) > 0 and H45 != 0 else 0.0
    O45 = -(E11 + E12) / H45 if (E11 + E12 + L45 + M45 + N45) > 0 and H45 != 0 else 0.0
    P45 = -(E11 + E12) / H45 if (E11 + E12 + L45 + M45 + N45 + O45) > 0 and H45 != 0 else 0.0

    # Atar ownership return series
    L46 = -E14 / H46 if H46 != 0 else 0.0
    M46 = -E14 / H46 if (E14 + L46) > 0 and H46 != 0 else 0.0
    N46 = -E14 / H46 if (E14 + L46 + M46) > 0 and H46 != 0 else 0.0
    O46 = -E14 / H46 if (E14 + L46 + M46 + N46) > 0 and H46 != 0 else 0.0
    P46 = -E14 / H46 if (E14 + L46 + M46 + N46 + O46) > 0 and H46 != 0 else 0.0

    # LP/GP Split = SUM(cashflow + equity returns)
    L47 = L42 + L45 + L46
    M47 = M42 + M45 + M46
    N47 = N42 + N45 + N46
    O47 = O42 + O45 + O46
    P47 = P42 + P45 + P46

    # Earnout Payments = Cash + LP/GP Split
    L50 = L35 + L47
    M50 = M35 + M47
    N50 = N35 + N47
    O50 = O35 + O47
    P50 = P35 + P47

    # QofE Diligence per year (constant)
    L53 = -qof_e_diligence
    M53 = -qof_e_diligence
    N53 = -qof_e_diligence
    O53 = -qof_e_diligence
    P53 = -qof_e_diligence

    # FCCR per year
    A52 = _f(inputs.get('fccr_rate', 0.08))
    # L54 = L50 + L53 + L52 (L52 is empty/0 in projections)
    L54 = L50 + L53
    M54 = M50 + M53
    N54 = N50 + N53
    O54 = O50 + O53
    P54 = P50 + P53

    # Running inventory balance (K57 = SUM(E7:E8))
    K57 = E7 + E8

    # R&W / rollforward (L55)
    # L55 = IF(K57 > L54, -MAX(L54), -K57)
    def calc_rw(prev_k57, avail):
        return -max(avail) if prev_k57 > avail else -prev_k57

    L55 = -max(L54, 0) if K57 > L54 else -K57
    L56 = L54 + L55

    # Simplified running balance L57
    L57 = K57 * L8 + K57 + L55
    M57 = L57 * M8 + L57 + ((-max(M54, 0)) if L57 > M54 else -L57)
    N57 = M57 * N8 + M57 + ((-max(N54, 0)) if M57 > N54 else -M57)
    O57 = N57 * O8 + N57 + ((-max(O54, 0)) if N57 > O54 else -N57)
    P57 = O57 * P8 + O57 + ((-max(P54, 0)) if O57 > P54 else -O57)

    M55 = -max(M54, 0) if L57 > M54 else -L57
    M56 = M54 + M55
    N55 = -max(N54, 0) if M57 > N54 else -M57
    N56 = N54 + N55
    O55 = -max(O54, 0) if N57 > O54 else -N57
    O56 = O54 + O55
    P55 = -max(P54, 0) if O57 > P54 else -O57
    P56 = P54 + P55

    # Net Cash Flow (DSCR per year)
    def dscr(abl, term, capex, qofe, lp_gp):
        denom = -lp_gp
        return (abl + term + capex + qofe) / denom if denom != 0 else 0.0

    L58 = dscr(L31, L32, L34, L53, L47)
    M58 = dscr(M31, M32, M34, M53, M47)
    N58 = dscr(N31, N32, N34, N53, N47)
    O58 = dscr(O31, O32, O34, O53, O47)
    P58 = dscr(P31, P32, P34, P53, P47)

    # ─────────────────────────────────────────────
    # LEFT SIDE SUMMARY
    # ─────────────────────────────────────────────
    # Interest = (L7 - L18) / 52 * 2 * 1.1
    E31 = (L7 - L18) / 52 * 2 * 1.1
    # ABL = tf_total (from Transaction Fees sheet)
    E32 = tf_total
    # Total Uses = DEBT SERVICE + Interest + ABL
    E34 = E30 + E31 + E32
    # Taxable Income = Total Uses (E22 = E34)
    E22 = E34
    # Other expense/(income) = E22 - SUM(E7:E18)
    E20 = E22 - (E7 + E8 + E9 + E11 + E12 + E14 + E16 + E18)
    # MOIC
    C29 = C26 * C27 if C28 < 1 else E30

    # Left side summary block (rows 39-57)
    C39 = P31   # - Expenses = last year's ABL
    C40 = 6     # Amount to Distribute (fixed)
    C41 = C39 * C40   # Mgmt LTIP
    C42 = sum([L56, M56, N56, O56, P56])   # Total Debt Service
    C43 = -P57   # Amount left after Mgmt LTIP

    A44 = _f(inputs.get('lp_pct', 0.03))
    C44 = -A44 * C41   # LP

    C45 = C41 + C44   # GP = SUM(C41:C44)

    A47 = _f(inputs.get('preferred_pct', 0.05))
    C47 = -A47 * C45   # Preferred

    C48 = C45 + C47   # Total = SUM(C45:C47)
    C49 = C48 * C28   # Revolver balance

    C52 = A52 * E20 * 5   # FCCR $
    C53_left = E20         # Net Cash Flow
    A54 = _f(inputs.get('remaining_cash_pct', 0.75))
    C54_left = (C49 - (C52 + C53_left)) * A54
    E54 = C49 - C52 - C53_left - C54_left

    C55_left = C52 + C53_left + C54_left
    E55 = E54   # = SUM(E52:E54) which is just E54
    C57_fccr = C55_left / E20 if E20 != 0 else 0.0

    # ─────────────────────────────────────────────
    # BUILD RESULTS DICT
    # ─────────────────────────────────────────────
    results = {
        # Transaction Fees Sheet
        'tf_debt_sourcing': round(tf_debt_sourcing, 2),
        'tf_lawyers': round(tf_lawyers, 2),
        'tf_qofe': round(qof_e_diligence, 2),
        'tf_tax': round(tax_fee, 2),
        'tf_rw': round(rw_insurance, 2),
        'tf_senior': round(atar_bonuses_senior, 2),
        'tf_junior': round(atar_bonuses_junior, 2),
        'tf_project': round(project_other, 2),
        'tf_total': round(tf_total, 2),

        # Sources
        'E7': round(E7, 2),    # Net Revenue source
        'E8': round(E8, 2),    # Inventory source
        'E9': round(E9, 2),    # M&E source
        'E11': round(E11, 2),  # Building & Land
        'E12': round(E12, 2),  # Term Loans
        'E14': round(E14, 2),  # Seller Note
        'E16': round(E16, 2),  # Earnout
        'E18': round(E18, 2),  # Equity Roll
        'E20': round(E20, 2),  # Other expense/(income)
        'E22': round(E22, 2),  # Taxable Income

        # Uses
        'E30': round(E30, 2),  # Debt Service
        'E31': round(E31, 2),  # Interest
        'E32': round(E32, 2),  # ABL (tx fees)
        'E34': round(E34, 2),  # Total Uses
        'C29': round(C29, 2),  # MOIC
        'C41': round(C41, 2),  # Mgmt LTIP
        'C42': round(C42, 2),  # Total Debt Service
        'C43': round(C43, 2),  # Amount left after Mgmt LTIP
        'C44': round(C44, 2),  # LP
        'C45': round(C45, 2),  # GP
        'C47': round(C47, 2),  # Preferred
        'C48': round(C48, 2),  # Total
        'C49': round(C49, 2),  # Revolver balance
        'C52': round(C52, 2),  # FCCR $
        'C53_left': round(C53_left, 2),
        'C54_left': round(C54_left, 2),
        'C55_left': round(C55_left, 2),
        'C57_fccr': round(C57_fccr, 4),
        'E54': round(E54, 2),

        # Projections - Revenue
        'revenue': {'FY1': I7, 'FY2': J7, 'FY3': K7, 'Y1': L7, 'Y2': M7, 'Y3': N7, 'Y4': O7, 'Y5': P7},
        'growth_rate': {'FY2': round(J8*100,1), 'FY3': round(K8*100,1), 'Y1': round(L8*100,1), 'Y2': round(M8*100,1), 'Y3': round(N8*100,1), 'Y4': round(O8*100,1), 'Y5': round(P8*100,1)},
        'gross_margin': {'FY1': I10, 'FY2': J10, 'FY3': K10, 'Y1': L10, 'Y2': M10, 'Y3': N10, 'Y4': O10, 'Y5': P10},
        'gm_pct': {'FY1': round(I11*100,1), 'FY2': round(J11*100,1), 'FY3': round(K11*100,1), 'Y1': round(L11*100,1), 'Y2': round(M11*100,1), 'Y3': round(N11*100,1), 'Y4': round(O11*100,1), 'Y5': round(P11*100,1)},
        'sga': {'FY1': I13, 'FY2': J13, 'FY3': K13, 'Y1': L13, 'Y2': M13, 'Y3': N13, 'Y4': O13, 'Y5': P13},
        'operating_income': {'FY1': round(I15,2), 'FY2': round(J15,2), 'FY3': round(K15,2), 'Y1': round(L15,2), 'Y2': round(M15,2), 'Y3': round(N15,2), 'Y4': round(O15,2), 'Y5': round(P15,2)},
        'adjustments': {'FY1': I17, 'FY2': J17, 'FY3': K17, 'Y1': L17, 'Y2': M17, 'Y3': N17, 'Y4': O17, 'Y5': P17},
        'adj_ebitda': {'FY1': round(I18,2), 'FY2': round(J18,2), 'FY3': round(K18,2), 'Y1': round(L18,2), 'Y2': round(M18,2), 'Y3': round(N18,2), 'Y4': round(O18,2), 'Y5': round(P18,2)},
        'ebitda_pct': {'FY1': round(I19*100,1), 'FY2': round(J19*100,1), 'FY3': round(K19*100,1), 'Y1': round(L19*100,1), 'Y2': round(M19*100,1), 'Y3': round(N19*100,1), 'Y4': round(O19*100,1), 'Y5': round(P19*100,1)},
        'interest_expense': {'FY1': I21, 'FY2': J21, 'FY3': K21, 'Y1': L21, 'Y2': M21, 'Y3': N21, 'Y4': O21, 'Y5': P21},
        'depreciation': {'Y1': round(L22,2), 'Y2': round(M22,2), 'Y3': round(N22,2), 'Y4': round(O22,2), 'Y5': round(P22,2)},
        'availability': {'FY1': 0, 'FY2': 0, 'FY3': 0, 'Y1': round(L24,2), 'Y2': round(M24,2), 'Y3': round(N24,2), 'Y4': round(O24,2), 'Y5': round(P24,2)},
        'capex': {'Y1': round(L26,2), 'Y2': round(M26,2), 'Y3': round(N26,2), 'Y4': round(O26,2), 'Y5': round(P26,2)},
        'total_uses_proj': {'Y1': round(L27,2), 'Y2': round(M27,2), 'Y3': round(N27,2), 'Y4': round(O27,2), 'Y5': round(P27,2)},
        'abl_proj': {'Y1': round(L31,2), 'Y2': round(M31,2), 'Y3': round(N31,2), 'Y4': round(O31,2), 'Y5': round(P31,2)},
        'term_proj': {'Y1': round(L32,2), 'Y2': round(M32,2), 'Y3': round(N32,2), 'Y4': round(O32,2), 'Y5': round(P32,2)},
        'ev_at_exit': {'Y1': round(L33,2), 'Y2': round(M33,2), 'Y3': round(N33,2), 'Y4': round(O33,2), 'Y5': round(P33,2)},
        'plus_cash': {'Y1': round(L35,2), 'Y2': round(M35,2), 'Y3': round(N35,2), 'Y4': round(O35,2), 'Y5': round(P35,2)},
        'lp_gp_split': {'Y1': round(L47,2), 'Y2': round(M47,2), 'Y3': round(N47,2), 'Y4': round(O47,2), 'Y5': round(P47,2)},
        'earnout_payments': {'Y1': round(L50,2), 'Y2': round(M50,2), 'Y3': round(N50,2), 'Y4': round(O50,2), 'Y5': round(P50,2)},
        'dscr': {'Y1': round(L58,2), 'Y2': round(M58,2), 'Y3': round(N58,2), 'Y4': round(O58,2), 'Y5': round(P58,2)},

        # Running balance
        'K57': round(K57, 2),
        'running_balance': {'K': round(K57,2), 'Y1': round(L57,2), 'Y2': round(M57,2), 'Y3': round(N57,2), 'Y4': round(O57,2), 'Y5': round(P57,2)},

        # Key metrics for display
        'total_sources': round(E7 + E8 + E9 + E11 + E12 + E14 + E16 + E18, 2),
        'total_uses_final': round(E34, 2),
        'balance_check': round(E34 - (E7 + E8 + E9 + E11 + E12 + E14 + E16 + E18), 2),
    }

    return results
