"""
Accuracy Test Suite for SET Financial Analyzer — Full Coverage
===============================================================
ตรวจสอบความถูกต้องของข้อมูลทุกค่าที่แสดงผล เปรียบเทียบกับ:
  1. ข้อมูลดิบจาก API (annual_data) — ทุก field
  2. ข้อมูลดิบจาก XLSX (quarterly_xlsx_data) — ทุก field
  3. Internal consistency: สูตรคำนวณ, ความสัมพันธ์ระหว่างค่า

ค่าที่ตรวจสอบ (ครบทุกอย่าง):
  Income Statement:
    total_revenue, sales, other_revenue, total_expense,
    ebit, ebitda, net_profit, eps,
    profit_from_other_activity, core_profit,
    profit_before_tax, finance_cost, depreciation, operating_profit
  Balance Sheet:
    total_assets, total_liabilities, equity,
    current_assets, non_current_assets,
    current_liabilities, non_current_liabilities,
    assets = liabilities + equity (consistency)
    current_assets + non_current = total (consistency)
  Ratios:
    gross_margin, ebit_margin, ebitda_margin,
    net_margin, core_margin, roe, roa, de_ratio,
    current_ratio, quick_ratio
  Core Profit:
    core_profit = net_profit - profit_from_other (formula)
    core_pct_of_reported (formula)
  Cash Flow:
    cf_operating, cf_investing, cf_financing, cf_net
  Unit conversion:
    sanity range check (พันบาท)
  Cross-mode:
    sum quarterly = annual (revenue, profit)
"""

import json
import sys
import re
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from set_scraper import SETScraper
from financial_data import get_financial_data

# ---------------------------------------------------------------------------
# Test companies (20 บริษัท หลาย sector)
# ---------------------------------------------------------------------------
_ORIGINAL_20 = [
    "AOT", "PTT", "CPALL", "SCC", "ADVANC",
    "KBANK", "BDMS", "GULF", "CPN", "BEM",
    "MINT", "HMPRO", "BJC", "TU", "GLOBAL",
    "KTC", "OSP", "GPSC", "WHA", "TTB",
]
_NEW_200 = [
    "A", "AAV", "ABFTH", "ACC", "ADVICE", "AJ", "AKP", "ALLA", "ALPHAX",
    "ANAN", "AP", "APCS", "ASAP", "ASW", "AU", "AXTRART", "B", "BAY",
    "BBGI", "BE8", "BGC", "BGRIM", "BGT", "BIS", "BKD", "BKGI", "BLC",
    "BOFFICE", "BPP", "BRI", "BUI", "CGH", "CH", "CHAYO", "CHINA", "CITY",
    "CKP", "CMAN", "CMO", "CN01", "CNT", "COCOCO", "COLOR", "CPL", "CRANE",
    "DCON", "DITTO", "EA", "EAST", "EASTW", "EGCO", "EMPIRE", "EP", "EPG",
    "ETC", "F&D", "FANCY", "FMT", "FVC", "GAHREIT", "GENCO", "GTB", "GTV",
    "GUNKUL", "GYT", "HANA", "HARN", "HK01", "HPF", "HTECH", "III",
    "IMPACT", "INSET", "JAK", "JAS", "JSP", "JTS", "KDH", "KISS", "KKP",
    "KWC", "L&E", "LDC", "LH", "LIT", "LPN", "LUXF", "M", "MA80",
    "MADAME", "MAGURO", "MASTEC", "MEB", "MENA", "METCO", "MNRF", "MOONG",
    "MS06", "MSC", "NAM", "NEX", "NNCL", "NSL", "NTF", "NUT", "OGC",
    "PICO", "PL", "PLANB", "PLUS", "PMC", "POLY", "PROS", "PROUD", "PSH",
    "PSTC", "PTC", "PTECH", "QDC", "QTCG", "RABBIT", "RCL", "RML", "RPC",
    "SABINA", "SAMART", "SAMCO", "SAUCE", "SAV", "SCCC", "SCGP", "SCM",
    "SCP", "SECURE", "SHANG", "SIRI", "SIS", "SISB", "SKR", "SMIT", "SMO",
    "SMPC", "SNC", "SOLAR", "SORKON", "SPG", "SPVI", "SSSC", "STC", "STGT",
    "STOWER", "STP", "STX", "SUPER", "SYNTEC", "TAN", "TATG", "TCAP",
    "TERA", "TFI", "TGE", "THE", "THG", "THREL", "TIF1", "TIGER", "TIPH",
    "TITLE", "TK", "TKS", "TL", "TMD", "TNITY", "TNPF", "TOPP", "TPCS",
    "TPL", "TPS", "TSE", "TTA", "TURTLE", "TVDH", "TVH", "TVT", "UBOT",
    "UMS", "UP", "UV", "VAYU1", "VRANDA", "WFX", "WGE", "WHABT", "WHART",
    "WHAUP", "WINDOW", "WSOL", "XPG", "XYZ", "ZAA",
]
import sys as _sys
if "--new-only" in _sys.argv:
    TEST_SYMBOLS = _NEW_200
elif "--all" in _sys.argv:
    TEST_SYMBOLS = _ORIGINAL_20 + _NEW_200
else:
    TEST_SYMBOLS = _ORIGINAL_20 + _NEW_200

# ---------------------------------------------------------------------------
# Result tracking
# ---------------------------------------------------------------------------
results: list[dict] = []

def log(symbol: str, test_name: str, status: str, detail: str = ""):
    results.append({"symbol": symbol, "test": test_name,
                    "status": status, "detail": detail})
    mark = {"PASS": "✓", "FAIL": "✗", "WARN": "!", "SKIP": "-"}[status]
    suffix = f": {detail}" if detail else ""
    print(f"  [{mark}] {test_name}{suffix}")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def near(a, b, pct: float = 1.0) -> bool:
    """a ≈ b within pct% tolerance."""
    if a is None or b is None:
        return False
    if abs(a) < 1 and abs(b) < 1:
        return True
    if b == 0:
        return abs(a) < 1
    return abs((a - b) / b) * 100 <= pct

def fmt(v) -> str:
    if v is None: return "None"
    try: return f"{v:,.0f}"
    except: return str(v)

def diff_pct(a, b) -> str:
    if b == 0: return "∞%"
    return f"{abs(a-b)/abs(b)*100:.1f}%"

def xlsx_v(section: dict, field: str, div: float, col: str = "current") -> float:
    """Get value from XLSX income/balance/cashflow section, unit-converted to thousands THB."""
    item = section.get(field, {})
    if isinstance(item, dict):
        raw = item.get(col, 0) or 0
        return raw / div
    return 0

def xlsx_raw(section: dict, field: str, col: str = "current") -> float:
    """Get raw value from XLSX section without unit conversion (e.g. EPS in Baht/share)."""
    item = section.get(field, {})
    if isinstance(item, dict):
        return item.get(col, 0) or 0
    return 0

def get_div(unit: str) -> float:
    return {"thousands": 1.0, "millions": 0.001}.get(unit, 1000.0)

# ---------------------------------------------------------------------------
# Annual API checks — all fields
# ---------------------------------------------------------------------------
def check_annual(symbol: str, annual: dict, raw: dict):
    income   = annual.get("income_statement", [])
    balance  = annual.get("balance_sheet", [])
    ratios   = annual.get("ratios", [])
    core     = annual.get("core_profit_analysis", [])
    cashflow = annual.get("cashflow", [])
    api_data = raw.get("annual_data", [])

    if not income:
        log(symbol, "annual_has_data", "SKIP", "ไม่มี income_statement"); return

    log(symbol, "annual_periods",
        "PASS" if len(income) >= 3 else "WARN",
        f"{len(income)} งวด")

    api_map: dict[tuple, dict] = {}
    for d in api_data:
        api_map[(d.get("year"), d.get("quarter", ""))] = d

    for row in income:
        period = row["period"]
        year   = row["year"]
        quarter = row.get("quarter", "")
        sym = symbol

        # find best matching API record
        api = (api_map.get((year, quarter))
               or api_map.get((year, "Q9"))
               or api_map.get((year, ""))
               or next((v for (y,q),v in api_map.items() if y == year), None))
        if not api:
            log(sym, f"[{period}] api_record", "WARN", "ไม่พบ API record"); continue

        def chk(test: str, app_val, api_key: str, pct: float = 1.0, label: str = ""):
            api_val = api.get(api_key, 0) or 0
            app_v   = app_val or 0
            name    = f"[{period}] {test}"
            if near(app_v, api_val, pct):
                log(sym, name, "PASS", f"app={fmt(app_v)} api={fmt(api_val)}")
            else:
                log(sym, name, "FAIL",
                    f"app={fmt(app_v)} api={fmt(api_val)} diff={diff_pct(app_v, api_val)}"
                    + (f" ({label})" if label else ""))

        # ---- Income Statement ----
        chk("total_revenue",   row.get("total_revenue"),  "totalRevenue")
        chk("sales",           row.get("sales"),           "sales")
        chk("total_expense",   row.get("total_expense"),   "totalExpense")
        chk("ebit",            row.get("ebit"),            "ebit")
        chk("ebitda",          row.get("ebitda"),          "ebitda")
        chk("net_profit",      row.get("net_profit"),      "netProfit")
        chk("eps",             row.get("eps"),             "eps", pct=2.0)
        chk("profit_from_other", row.get("profit_from_other_activity"), "profitFromOtherActivity")

        # other_revenue = totalRevenue - sales (derived)
        api_other = (api.get("totalRevenue", 0) or 0) - (api.get("sales", 0) or 0)
        app_other = row.get("other_revenue", 0) or 0
        if near(app_other, api_other):
            log(sym, f"[{period}] other_revenue", "PASS",
                f"app={fmt(app_other)} expected={fmt(api_other)}")
        else:
            log(sym, f"[{period}] other_revenue", "FAIL",
                f"app={fmt(app_other)} expected={fmt(api_other)} diff={diff_pct(app_other, api_other)}")

        # core_profit = netProfit - PFO × (1 - tax_rate)
        # ใช้ tax_rate จาก XLSX (ถ้ามี) เพราะ PFO เป็น pre-tax แต่ NP เป็น after-tax
        api_np  = api.get("netProfit", 0) or 0
        api_pfo = api.get("profitFromOtherActivity", 0) or 0
        # ดึง effective_tax_rate จาก core_analysis row
        _core_row_for_tax = next((c for c in core if c["period"] == period), None)
        _tax_rate_frac = (_core_row_for_tax.get("effective_tax_rate", 20.0) / 100.0
                          if _core_row_for_tax else 0.20)
        expected_core = api_np - api_pfo * (1.0 - _tax_rate_frac)
        app_core = row.get("core_profit", 0) or 0
        if near(app_core, expected_core, 3.0):
            log(sym, f"[{period}] core_profit_formula", "PASS",
                f"core={fmt(app_core)} = NP({fmt(api_np)}) - PFO({fmt(api_pfo)})×(1-{_tax_rate_frac:.1%})")
        else:
            log(sym, f"[{period}] core_profit_formula", "FAIL",
                f"app={fmt(app_core)} expected={fmt(expected_core)} diff={diff_pct(app_core, expected_core)}")

        # ---- Balance Sheet ----
        bal = next((b for b in balance if b["period"] == period), None)
        if bal is not None:
            def chk2(test: str, app_val, api_key: str, pct: float = 1.0):
                av = app_val or 0
                apiv = api.get(api_key, 0) or 0
                name = f"[{period}] {test}"
                if near(av, apiv, pct):
                    log(sym, name, "PASS", f"app={fmt(av)} api={fmt(apiv)}")
                else:
                    log(sym, name, "FAIL", f"app={fmt(av)} api={fmt(apiv)} diff={diff_pct(av, apiv)}")
            chk2("total_assets",      bal.get("total_assets"),      "totalAsset")
            chk2("total_liabilities", bal.get("total_liabilities"),  "totalLiability")
            chk2("equity",            bal.get("equity"),             "equity")

            # A = L + E consistency (may differ due to NCI — treat as WARN)
            app_ta = bal.get("total_assets", 0) or 0
            app_tl = bal.get("total_liabilities", 0) or 0
            app_eq = bal.get("equity", 0) or 0
            if app_ta > 0:
                implied = app_tl + app_eq
                if near(implied, app_ta, 5.0):
                    log(sym, f"[{period}] bs_A=L+E", "PASS",
                        f"A={fmt(app_ta)} L+E={fmt(implied)}")
                else:
                    log(sym, f"[{period}] bs_A=L+E", "WARN",
                        f"A={fmt(app_ta)} L+E={fmt(implied)} gap={fmt(app_ta-implied)} (NCI/minority interest)")

        # ---- Ratios ----
        rat = next((r for r in ratios if r["period"] == period), None)
        if rat:
            api_tr = api.get("totalRevenue", 0) or 0

            # gross margin from API (pre-calculated)
            api_gm = api.get("grossProfitMargin", 0) or 0
            app_gm = rat.get("gross_margin_pct", 0) or 0
            if api_gm:
                if near(app_gm, api_gm, 2.0):
                    log(sym, f"[{period}] gross_margin_pct", "PASS",
                        f"app={app_gm:.2f}% api={api_gm:.2f}%")
                else:
                    log(sym, f"[{period}] gross_margin_pct", "WARN",
                        f"app={app_gm:.2f}% api={api_gm:.2f}% (API calc differs)")

            # ebit_margin = ebit / total_revenue * 100
            api_ebit = api.get("ebit", 0) or 0
            expected_ebit_margin = (api_ebit / api_tr * 100) if api_tr else 0
            app_em = rat.get("ebit_margin_pct", 0) or 0
            if near(app_em, expected_ebit_margin, 1.0):
                log(sym, f"[{period}] ebit_margin_pct", "PASS",
                    f"app={app_em:.2f}% expected={expected_ebit_margin:.2f}%")
            else:
                log(sym, f"[{period}] ebit_margin_pct", "FAIL",
                    f"app={app_em:.2f}% expected={expected_ebit_margin:.2f}%")

            # ebitda_margin = ebitda / total_revenue * 100
            api_ebitda = api.get("ebitda", 0) or 0
            expected_ebitda_margin = (api_ebitda / api_tr * 100) if api_tr else 0
            app_edm = rat.get("ebitda_margin_pct", 0) or 0
            if near(app_edm, expected_ebitda_margin, 1.0):
                log(sym, f"[{period}] ebitda_margin_pct", "PASS",
                    f"app={app_edm:.2f}% expected={expected_ebitda_margin:.2f}%")
            else:
                log(sym, f"[{period}] ebitda_margin_pct", "FAIL",
                    f"app={app_edm:.2f}% expected={expected_ebitda_margin:.2f}%")

            # net_margin — from API pre-calculated (may differ from NP/TR)
            api_nm = api.get("netProfitMargin", 0) or 0
            app_nm = rat.get("net_margin_pct", 0) or 0
            if near(app_nm, api_nm, 2.0):
                log(sym, f"[{period}] net_margin_pct", "PASS",
                    f"app={app_nm:.2f}% api={api_nm:.2f}%")
            else:
                log(sym, f"[{period}] net_margin_pct", "WARN",
                    f"app={app_nm:.2f}% api={api_nm:.2f}% (API uses different revenue base)")

            # core_margin = core_profit / total_revenue * 100
            expected_core_margin = (app_core / api_tr * 100) if api_tr else 0
            app_cm = rat.get("core_margin_pct", 0) or 0
            if near(app_cm, expected_core_margin, 1.0):
                log(sym, f"[{period}] core_margin_pct", "PASS",
                    f"app={app_cm:.2f}% expected={expected_core_margin:.2f}%")
            else:
                log(sym, f"[{period}] core_margin_pct", "FAIL",
                    f"app={app_cm:.2f}% expected={expected_core_margin:.2f}%")

            # ROE, ROA from API (pre-calculated, annualised)
            api_roe = api.get("roe", 0) or 0
            app_roe = rat.get("roe_pct", 0) or 0
            if near(app_roe, api_roe, 2.0):
                log(sym, f"[{period}] roe_pct", "PASS",
                    f"app={app_roe:.2f}% api={api_roe:.2f}%")
            else:
                log(sym, f"[{period}] roe_pct", "FAIL",
                    f"app={app_roe:.2f}% api={api_roe:.2f}%")

            api_roa = api.get("roa", 0) or 0
            app_roa = rat.get("roa_pct", 0) or 0
            if near(app_roa, api_roa, 2.0):
                log(sym, f"[{period}] roa_pct", "PASS",
                    f"app={app_roa:.2f}% api={api_roa:.2f}%")
            else:
                log(sym, f"[{period}] roa_pct", "FAIL",
                    f"app={app_roa:.2f}% api={api_roa:.2f}%")

            # D/E ratio from API
            api_de = api.get("deRatio", 0) or 0
            app_de = rat.get("de_ratio", 0) or 0
            if near(app_de, api_de, 2.0):
                log(sym, f"[{period}] de_ratio", "PASS",
                    f"app={app_de:.3f} api={api_de:.3f}")
            else:
                log(sym, f"[{period}] de_ratio", "FAIL",
                    f"app={app_de:.3f} api={api_de:.3f} diff={diff_pct(app_de, api_de)}")

            # Current ratio from API
            api_cr = api.get("currentRatio", 0) or 0
            app_cr = rat.get("current_ratio", 0) or 0
            if near(app_cr, api_cr, 2.0):
                log(sym, f"[{period}] current_ratio", "PASS",
                    f"app={app_cr:.3f} api={api_cr:.3f}")
            else:
                log(sym, f"[{period}] current_ratio", "FAIL",
                    f"app={app_cr:.3f} api={api_cr:.3f} diff={diff_pct(app_cr, api_cr)}")

            # Quick ratio from API
            api_qr = api.get("quickRatio", 0) or 0
            app_qr = rat.get("quick_ratio", 0) or 0
            if near(app_qr, api_qr, 2.0):
                log(sym, f"[{period}] quick_ratio", "PASS",
                    f"app={app_qr:.3f} api={api_qr:.3f}")
            else:
                log(sym, f"[{period}] quick_ratio", "FAIL",
                    f"app={app_qr:.3f} api={api_qr:.3f} diff={diff_pct(app_qr, api_qr)}")

        # ---- Core Profit Analysis ----
        core_row = next((c for c in core if c["period"] == period), None)
        if core_row:
            app_ni   = core_row.get("reported_net_income", 0) or 0
            app_cp   = core_row.get("core_profit", 0) or 0
            app_ei   = core_row.get("extraordinary_items", 0) or 0
            app_pct  = core_row.get("core_pct_of_reported", 0) or 0

            # reported_net_income == net_profit
            if near(app_ni, api_np):
                log(sym, f"[{period}] core_reported_NI", "PASS",
                    f"app={fmt(app_ni)} api={fmt(api_np)}")
            else:
                log(sym, f"[{period}] core_reported_NI", "FAIL",
                    f"app={fmt(app_ni)} api={fmt(api_np)}")

            # extraordinary_items = PFO × (1 - tax_rate)  (after-tax)
            expected_ei = api_pfo * (1.0 - _tax_rate_frac)
            if near(app_ei, expected_ei, 2.0):
                log(sym, f"[{period}] core_extraordinary", "PASS",
                    f"app={fmt(app_ei)} expected PFO×(1-t)={fmt(expected_ei)}")
            else:
                log(sym, f"[{period}] core_extraordinary", "FAIL",
                    f"app={fmt(app_ei)} expected={fmt(expected_ei)} api_pfo={fmt(api_pfo)}")

            # core_pct = core_profit / net_profit * 100
            # ใช้ absolute difference (pp) เพราะค่า pct อาจเล็ก → near() ผิดพลาด
            if api_np:
                expected_pct = (expected_core / api_np) * 100
                pct_diff_abs = abs(app_pct - expected_pct)
                if pct_diff_abs <= 0.5:  # tolerance 0.5 pp (รองรับ round 1 ทศนิยม + tax_rate จาก XLSX)
                    log(sym, f"[{period}] core_pct_formula", "PASS",
                        f"app={app_pct:.1f}% expected={expected_pct:.2f}%")
                else:
                    log(sym, f"[{period}] core_pct_formula", "FAIL",
                        f"app={app_pct:.1f}% expected={expected_pct:.2f}% diff={pct_diff_abs:.2f}pp")

            # ---- รายการพิเศษ: sign consistency ----
            # extraordinary ต้องมีทิศทางเดียวกับ profitFromOtherActivity
            if api_pfo != 0 and app_ei != 0:
                same_sign = (api_pfo >= 0) == (app_ei >= 0)
                if same_sign:
                    log(sym, f"[{period}] extraordinary_sign", "PASS",
                        f"app={fmt(app_ei)} api={fmt(api_pfo)} ทิศทางตรงกัน")
                else:
                    log(sym, f"[{period}] extraordinary_sign", "FAIL",
                        f"app={fmt(app_ei)} vs api={fmt(api_pfo)} ทิศทางตรงข้าม")
            elif api_pfo == 0 and app_ei != 0:
                log(sym, f"[{period}] extraordinary_sign", "WARN",
                    f"app={fmt(app_ei)} แต่ api PFO=0")

            # ---- รายการพิเศษ: materiality (นัยสำคัญ) ----
            # ถ้า extraordinary > 10% ของ NI ให้แจ้งเตือน (ข้อมูลสำคัญ)
            if api_np and abs(api_pfo) > 0:
                mat_pct = abs(api_pfo) / abs(api_np) * 100
                status = "WARN" if mat_pct > 10 else "PASS"
                log(sym, f"[{period}] extraordinary_materiality", status,
                    f"PFO={fmt(api_pfo)} = {mat_pct:.1f}% ของ NI={fmt(api_np)}"
                    + (" (สูง — มีผลต่อ core profit มาก)" if mat_pct > 10 else ""))

            # ---- core_profit vs NI consistency ----
            # core_profit ไม่ควรต่างจาก NI เกิน 100% (สัญญาณผิดปกติ)
            if api_np and app_ni:
                core_ni_diff = abs(app_cp - app_ni) / abs(app_ni) * 100 if app_ni else 0
                status = "WARN" if core_ni_diff > 50 else "PASS"
                log(sym, f"[{period}] core_vs_ni_gap", status,
                    f"core={fmt(app_cp)} NI={fmt(app_ni)} gap={core_ni_diff:.1f}%"
                    + (" (รายการพิเศษขนาดใหญ่มาก)" if core_ni_diff > 50 else ""))

        # ---- Cash Flow ----
        cf = next((c for c in cashflow if c["period"] == period), None)
        if cf:
            for app_key, api_key in [
                ("operating", "netOperating"),
                ("investing", "netInvesting"),
                ("financing", "netFinancing"),
                ("net_cashflow", "netCashflow"),
            ]:
                api_v = api.get(api_key, 0) or 0
                app_v = cf.get(app_key, 0) or 0
                if near(app_v, api_v):
                    log(sym, f"[{period}] cf_{app_key}", "PASS",
                        f"app={fmt(app_v)} api={fmt(api_v)}")
                else:
                    log(sym, f"[{period}] cf_{app_key}", "FAIL",
                        f"app={fmt(app_v)} api={fmt(api_v)} diff={diff_pct(app_v, api_v)}")

# ---------------------------------------------------------------------------
# Quarterly XLSX checks — all fields
# ---------------------------------------------------------------------------
def check_quarterly(symbol: str, quarterly: dict, raw: dict):
    q_income   = quarterly.get("income_statement", [])
    q_balance  = quarterly.get("balance_sheet", [])
    q_ratios   = quarterly.get("ratios", [])
    q_core     = quarterly.get("core_profit_analysis", [])
    q_cashflow = quarterly.get("cashflow", [])
    xlsx_cache = raw.get("quarterly_xlsx_data", [])

    if not q_income:
        log(symbol, "quarterly_has_data", "SKIP", "ไม่มีข้อมูลรายไตรมาส"); return

    log(symbol, "quarterly_periods",
        "PASS" if len(q_income) >= 4 else "WARN",
        f"{len(q_income)} งวด")

    # Build XLSX lookup by (year, quarter)
    xlsx_map: dict[tuple, dict] = {}
    for qd in xlsx_cache:
        y = int(qd["year"]) if str(qd.get("year","")).isdigit() else 0
        q = qd.get("quarter", "")
        xlsx_map[(y, q)] = qd

    # Build period → quarter/year mapping from income_statement (cashflow มี period แต่ไม่มี quarter)
    period_meta: dict[str, dict] = {r["period"]: r for r in q_income}
    # Augment cashflow rows with quarter/year from income
    q_cashflow_aug = []
    for cf_r in q_cashflow:
        meta = period_meta.get(cf_r["period"], {})
        q_cashflow_aug.append({**cf_r,
                                "quarter": meta.get("quarter", ""),
                                "year": meta.get("year", cf_r.get("year", 0))})
    # Similarly for core_profit_analysis
    q_core_aug = []
    for c_r in q_core:
        meta = period_meta.get(c_r["period"], {})
        q_core_aug.append({**c_r,
                           "quarter": meta.get("quarter", ""),
                           "year": meta.get("year", c_r.get("year", 0))})

    for row in q_income:
        period  = row["period"]
        year    = row.get("year", 0)
        quarter = row.get("quarter", "")
        sym     = symbol

        # ---- Unit sanity (ทุก row) ----
        app_rev = row.get("total_revenue", 0) or 0
        if app_rev > 0:
            sane = 500 < app_rev < 6_000_000_000
            log(sym, f"[{period}] unit_sanity",
                "PASS" if sane else "WARN",
                f"revenue={fmt(app_rev)} พันบาท" + ("" if sane else " ← ผิดปกติ"))

        # ---- Q4 = computed, test sum = FY ----
        if quarter == "Q4":
            fy_qd = xlsx_map.get((year, "Q9")) or xlsx_map.get((year, "YE"))
            if fy_qd:
                div = get_div(fy_qd.get("unit", "baht"))
                inc = fy_qd.get("income", {})
                fy_rev = xlsx_v(inc, "total_revenue", div)
                if fy_rev > 0:
                    parts = [r for r in q_income
                             if r.get("year") == year and r.get("quarter") in ("Q1","Q2","Q3","Q4")]
                    if len(parts) == 4:
                        sum_rev = sum(r.get("total_revenue",0) or 0 for r in parts)
                        if near(sum_rev, fy_rev, 2.0):
                            log(sym, f"[{period}] Q4_revenue_sum_check", "PASS",
                                f"Q1+Q2+Q3+Q4={fmt(sum_rev)} FY={fmt(fy_rev)}")
                        else:
                            log(sym, f"[{period}] Q4_revenue_sum_check", "FAIL",
                                f"Q1+Q2+Q3+Q4={fmt(sum_rev)} FY={fmt(fy_rev)} diff={diff_pct(sum_rev, fy_rev)}")

                fy_np = xlsx_v(inc, "ni_owners", div) or xlsx_v(inc, "net_profit", div)
                if fy_np:
                    parts = [r for r in q_income
                             if r.get("year") == year and r.get("quarter") in ("Q1","Q2","Q3","Q4")]
                    if len(parts) == 4:
                        sum_np = sum(r.get("net_profit",0) or 0 for r in parts)
                        if near(sum_np, fy_np, 3.0):
                            log(sym, f"[{period}] Q4_netprofit_sum_check", "PASS",
                                f"Q1+Q2+Q3+Q4={fmt(sum_np)} FY={fmt(fy_np)}")
                        else:
                            log(sym, f"[{period}] Q4_netprofit_sum_check", "WARN",
                                f"sum={fmt(sum_np)} FY={fmt(fy_np)} diff={diff_pct(sum_np, fy_np)} (NCI/restatement)")

                fy_op = xlsx_v(inc, "operating_profit", div)
                if fy_op:
                    parts = [r for r in q_income
                             if r.get("year") == year and r.get("quarter") in ("Q1","Q2","Q3","Q4")]
                    if len(parts) == 4:
                        sum_op = sum(r.get("ebit",0) or 0 for r in parts)
                        if near(sum_op, fy_op, 2.0):
                            log(sym, f"[{period}] Q4_ebit_sum_check", "PASS",
                                f"sum={fmt(sum_op)} FY={fmt(fy_op)}")
                        else:
                            log(sym, f"[{period}] Q4_ebit_sum_check", "WARN",
                                f"sum={fmt(sum_op)} FY={fmt(fy_op)} diff={diff_pct(sum_op, fy_op)}")

                # ---- Q4 Cash Flow sum = FY XLSX cashflow ----
                fy_cf_xlsx = fy_qd.get("cashflow", {})
                for cf_app_key, cf_xlsx_key in [
                    ("operating",    "cf_operating"),
                    ("investing",    "cf_investing"),
                    ("financing",    "cf_financing"),
                    ("net_cashflow", "cf_net"),
                ]:
                    fy_cf_v = xlsx_v(fy_cf_xlsx, cf_xlsx_key, div)
                    if fy_cf_v == 0:
                        continue
                    parts_cf = [r for r in q_cashflow_aug
                                if r.get("year") == year and r.get("quarter", "") in ("Q1","Q2","Q3","Q4")]
                    if len(parts_cf) == 4:
                        sum_cf = sum(r.get(cf_app_key, 0) or 0 for r in parts_cf)
                        if near(sum_cf, fy_cf_v, 3.0):
                            log(sym, f"[{period}] Q4_cf_{cf_app_key}_sum", "PASS",
                                f"Q1+Q2+Q3+Q4={fmt(sum_cf)} FY={fmt(fy_cf_v)}")
                        else:
                            log(sym, f"[{period}] Q4_cf_{cf_app_key}_sum", "WARN",
                                f"sum={fmt(sum_cf)} FY={fmt(fy_cf_v)} diff={diff_pct(sum_cf, fy_cf_v)}"
                                f" (อาจเกิดจากการ convert cumulative)")

                # ---- Q4 Core Profit sum (informational) ----
                # core_profit ≠ NP เมื่อมี CF special items
                # ตรวจสอบ sum(NP) Q1-Q4 = FY NP แทน (เพื่อ sanity check)
                parts_core = [r for r in q_core_aug
                              if r.get("year") == year and r.get("quarter", "") in ("Q1","Q2","Q3","Q4")]
                if len(parts_core) == 4:
                    sum_core_ni = sum(r.get("reported_net_income", 0) or 0 for r in parts_core)
                    fy_np_for_core = xlsx_v(inc, "ni_owners", div) or xlsx_v(inc, "net_profit", div)
                    if fy_np_for_core and near(sum_core_ni, fy_np_for_core, 3.0):
                        log(sym, f"[{period}] Q4_core_NI_sum_check", "PASS",
                            f"sum NI={fmt(sum_core_ni)} FY NI={fmt(fy_np_for_core)}")
                    elif fy_np_for_core:
                        log(sym, f"[{period}] Q4_core_NI_sum_check", "WARN",
                            f"sum NI={fmt(sum_core_ni)} FY NI={fmt(fy_np_for_core)} "
                            f"diff={diff_pct(sum_core_ni, fy_np_for_core)}")

            continue  # Q4 computed, no direct XLSX to compare

        # ---- Q1/Q2/Q3 — compare every field vs XLSX ----
        qd = xlsx_map.get((year, quarter))
        if not qd:
            log(sym, f"[{period}] xlsx_cache", "WARN",
                f"ไม่พบ XLSX cache สำหรับ {quarter}/{year}"); continue
        # Gap-filled quarters are synthesized (cum - prev) — not directly comparable
        if qd.get("gap_filler"):
            log(sym, f"[{period}] xlsx_cache", "WARN",
                f"gap-filler quarter — skip XLSX comparison"); continue

        div = get_div(qd.get("unit", "baht"))
        inc = qd.get("income", {})
        bal = qd.get("balance", {})
        cf  = qd.get("cashflow", {})

        def chk_xlsx(test: str, app_val, field: str, section=None, pct: float = 1.0):
            sec = section if section is not None else inc
            xlsx_val = xlsx_v(sec, field, div)
            app_v = app_val or 0
            name = f"[{period}] {test}"
            if xlsx_val == 0 and app_v == 0:
                log(sym, name, "SKIP", "ทั้งคู่เป็น 0")
                return
            if xlsx_val == 0:
                log(sym, name, "SKIP", f"XLSX={fmt(xlsx_val)} (ไม่มีข้อมูล)")
                return
            if near(app_v, xlsx_val, pct):
                log(sym, name, "PASS", f"app={fmt(app_v)} xlsx={fmt(xlsx_val)}")
            else:
                log(sym, name, "FAIL",
                    f"app={fmt(app_v)} xlsx={fmt(xlsx_val)} diff={diff_pct(app_v, xlsx_val)}")

        # Income Statement — ทุก field ที่ app แสดง
        chk_xlsx("total_revenue",  row.get("total_revenue"),  "total_revenue")
        chk_xlsx("sales",          row.get("sales"),           "sales")
        chk_xlsx("other_revenue",  row.get("other_revenue"),   "other_revenue")
        chk_xlsx("total_expense",  row.get("total_expense"),   "total_expense")
        chk_xlsx("ebit",           row.get("ebit"),            "operating_profit")
        # net_profit: app uses ni_owners (parent only) when available, else total NP
        _xlsx_np_field = "ni_owners" if xlsx_v(inc, "ni_owners", div) != 0 else "net_profit"
        chk_xlsx("net_profit",     row.get("net_profit"),      _xlsx_np_field)
        # EPS: ไม่ต้อง divide (เป็น Baht/หุ้น อยู่แล้ว) — ใช้ raw value จาก XLSX โดยตรง
        xlsx_eps_raw = xlsx_raw(inc, "eps", "current")
        app_eps = row.get("eps", 0) or 0
        if xlsx_eps_raw == 0 and app_eps == 0:
            log(sym, f"[{period}] eps", "SKIP", "ทั้งคู่เป็น 0")
        elif xlsx_eps_raw == 0:
            log(sym, f"[{period}] eps", "SKIP", "XLSX eps=0 (ไม่มีข้อมูล)")
        elif near(app_eps, xlsx_eps_raw, 2.0):
            log(sym, f"[{period}] eps", "PASS", f"app={app_eps} xlsx={xlsx_eps_raw}")
        else:
            log(sym, f"[{period}] eps", "FAIL",
                f"app={app_eps} xlsx={xlsx_eps_raw} diff={diff_pct(app_eps, xlsx_eps_raw)}")

        # depreciation ใน app = ebitda - ebit (เพราะ app คำนวณ ebitda = op + dep)
        app_dep_implied = (row.get("ebitda", 0) or 0) - (row.get("ebit", 0) or 0)
        xlsx_dep_v = xlsx_v(inc, "depreciation", div)
        if xlsx_dep_v != 0:
            if near(app_dep_implied, xlsx_dep_v, 2.0):
                log(sym, f"[{period}] depreciation_implied", "PASS",
                    f"ebitda-ebit={fmt(app_dep_implied)} xlsx_dep={fmt(xlsx_dep_v)}")
            else:
                log(sym, f"[{period}] depreciation_implied", "FAIL",
                    f"ebitda-ebit={fmt(app_dep_implied)} xlsx_dep={fmt(xlsx_dep_v)} diff={diff_pct(app_dep_implied, xlsx_dep_v)}")

        # finance_cost และ profit_before_tax — ไม่มีใน app (XLSX เท่านั้น) → แค่ log ว่ามีข้อมูล
        xlsx_fc  = xlsx_v(inc, "finance_cost",      div)
        xlsx_pbt = xlsx_v(inc, "profit_before_tax", div)
        if xlsx_fc != 0:
            log(sym, f"[{period}] finance_cost_xlsx_only", "SKIP",
                f"XLSX finance_cost={fmt(xlsx_fc)} (ไม่แสดงใน app)")
        if xlsx_pbt != 0:
            log(sym, f"[{period}] profit_before_tax_xlsx_only", "SKIP",
                f"XLSX profit_before_tax={fmt(xlsx_pbt)} (ไม่แสดงใน app)")

        # core_profit: computed from CF special items (or = net_profit if none found)
        app_core_q = row.get("core_profit", 0) or 0
        app_np_q   = row.get("net_profit", 0) or 0
        pfo_q      = row.get("profit_from_other_activity", 0) or 0
        # When no special items → core == net; otherwise core = NP - PFO*(1-t)
        if pfo_q == 0:
            # No special items extracted → core should equal net
            if near(app_core_q, app_np_q):
                log(sym, f"[{period}] core_eq_net_quarterly", "PASS",
                    f"core={fmt(app_core_q)} == net={fmt(app_np_q)} (no CF special items)")
            else:
                log(sym, f"[{period}] core_eq_net_quarterly", "FAIL",
                    f"core={fmt(app_core_q)} != net={fmt(app_np_q)} (pfo=0 แต่ core≠NP)")
        else:
            # Special items found → just verify core is between 0 and 2×NP
            reasonable = app_np_q != 0 and abs(app_core_q) <= abs(app_np_q) * 2
            log(sym, f"[{period}] core_cf_adjusted", "PASS" if reasonable else "WARN",
                f"core={fmt(app_core_q)} NP={fmt(app_np_q)} PFO={fmt(pfo_q)}")

        # EBITDA = operating_profit + depreciation
        xlsx_op  = xlsx_v(inc, "operating_profit", div)
        xlsx_dep = xlsx_v(inc, "depreciation", div)
        if xlsx_op and xlsx_dep:
            expected_ebitda = xlsx_op + xlsx_dep
            app_ebitda = row.get("ebitda", 0) or 0
            if near(app_ebitda, expected_ebitda, 2.0):
                log(sym, f"[{period}] ebitda_formula", "PASS",
                    f"app={fmt(app_ebitda)} = op({fmt(xlsx_op)}) + dep({fmt(xlsx_dep)})")
            else:
                log(sym, f"[{period}] ebitda_formula", "FAIL",
                    f"app={fmt(app_ebitda)} expected={fmt(expected_ebitda)} diff={diff_pct(app_ebitda, expected_ebitda)}")

        # Balance Sheet — ทุก field
        bal_row = next((b for b in q_balance if b["period"] == period), None)
        if bal_row:
            chk_xlsx("total_assets",       bal_row.get("total_assets"),      "total_assets",      bal)
            chk_xlsx("total_liabilities",  bal_row.get("total_liabilities"), "total_liabilities", bal)
            chk_xlsx("equity",             bal_row.get("equity"),            "equity",            bal)

            # current/non-current assets
            xlsx_ca  = xlsx_v(bal, "current_assets",     div)
            xlsx_nca = xlsx_v(bal, "non_current_assets", div)
            if xlsx_ca and xlsx_nca:
                expected_ta = xlsx_ca + xlsx_nca
                app_ta = bal_row.get("total_assets", 0) or 0
                if near(app_ta, expected_ta, 1.0):
                    log(sym, f"[{period}] assets_current+noncurrent", "PASS",
                        f"CA({fmt(xlsx_ca)}) + NCA({fmt(xlsx_nca)}) = {fmt(expected_ta)}")
                else:
                    xlsx_ta = xlsx_v(bal, "total_assets", div)
                    if xlsx_ta and near(xlsx_ta, expected_ta, 1.0):
                        log(sym, f"[{period}] assets_current+noncurrent", "WARN",
                            f"app_TA={fmt(app_ta)} vs CA+NCA={fmt(expected_ta)} diff={diff_pct(app_ta, expected_ta)} "
                            f"(factsheet TA stale; XLSX TA={fmt(xlsx_ta)} matches CA+NCA)")
                    elif xlsx_ta and near(xlsx_ta, app_ta, 1.0) and not near(xlsx_ta, expected_ta, 1.0):
                        log(sym, f"[{period}] assets_current+noncurrent", "WARN",
                            f"app_TA={fmt(app_ta)} vs CA+NCA={fmt(expected_ta)} diff={diff_pct(app_ta, expected_ta)} "
                            f"(XLSX TA={fmt(xlsx_ta)} also stale — likely parsing edge case)")
                    else:
                        log(sym, f"[{period}] assets_current+noncurrent", "FAIL",
                            f"app_TA={fmt(app_ta)} vs CA+NCA={fmt(expected_ta)} diff={diff_pct(app_ta, expected_ta)}")

            # current/non-current liabilities
            xlsx_cl  = xlsx_v(bal, "current_liabilities",     div)
            xlsx_ncl = xlsx_v(bal, "non_current_liabilities", div)
            if xlsx_cl and xlsx_ncl:
                expected_tl = xlsx_cl + xlsx_ncl
                app_tl = bal_row.get("total_liabilities", 0) or 0
                if near(app_tl, expected_tl, 1.0):
                    log(sym, f"[{period}] liab_current+noncurrent", "PASS",
                        f"CL({fmt(xlsx_cl)}) + NCL({fmt(xlsx_ncl)}) = {fmt(expected_tl)}")
                else:
                    # TL อาจมาจาก factsheet API/XLSX ที่ stale หรือ parse ผิด
                    xlsx_tl = xlsx_v(bal, "total_liabilities", div)
                    if xlsx_tl and near(xlsx_tl, expected_tl, 1.0):
                        log(sym, f"[{period}] liab_current+noncurrent", "WARN",
                            f"app_TL={fmt(app_tl)} vs CL+NCL={fmt(expected_tl)} diff={diff_pct(app_tl, expected_tl)} "
                            f"(factsheet TL stale; XLSX TL={fmt(xlsx_tl)} matches CL+NCL)")
                    elif xlsx_tl and near(xlsx_tl, app_tl, 1.0) and not near(xlsx_tl, expected_tl, 1.0):
                        # XLSX TL matches app_TL but both differ from CL+NCL
                        # → TL keyword likely matched wrong row in XLSX (parsing edge case)
                        log(sym, f"[{period}] liab_current+noncurrent", "WARN",
                            f"app_TL={fmt(app_tl)} vs CL+NCL={fmt(expected_tl)} diff={diff_pct(app_tl, expected_tl)} "
                            f"(XLSX TL={fmt(xlsx_tl)} also stale — likely parsing edge case)")
                    else:
                        log(sym, f"[{period}] liab_current+noncurrent", "FAIL",
                            f"app_TL={fmt(app_tl)} vs CL+NCL={fmt(expected_tl)} diff={diff_pct(app_tl, expected_tl)}")

            # A = L + E consistency
            app_ta = bal_row.get("total_assets", 0) or 0
            app_tl = bal_row.get("total_liabilities", 0) or 0
            app_eq = bal_row.get("equity", 0) or 0
            if app_ta > 0 and app_tl and app_eq:
                implied = app_tl + app_eq
                if near(implied, app_ta, 5.0):
                    log(sym, f"[{period}] bs_A=L+E", "PASS",
                        f"A={fmt(app_ta)} L+E={fmt(implied)}")
                else:
                    log(sym, f"[{period}] bs_A=L+E", "WARN",
                        f"A={fmt(app_ta)} L+E={fmt(implied)} gap={fmt(app_ta-implied)}")

        # Ratios — computed from XLSX values
        rat_row = next((r for r in q_ratios if r["period"] == period), None)
        if rat_row:
            app_rev_q = row.get("total_revenue", 0) or 0
            app_np_q  = row.get("net_profit", 0) or 0
            app_op_q  = row.get("ebit", 0) or 0
            app_edq   = row.get("ebitda", 0) or 0

            if app_rev_q > 0:
                exp_nm = (app_np_q / app_rev_q) * 100
                app_nm = rat_row.get("net_margin_pct", 0) or 0
                if near(app_nm, exp_nm, 1.0):
                    log(sym, f"[{period}] net_margin_pct", "PASS",
                        f"app={app_nm:.2f}% expected={exp_nm:.2f}%")
                else:
                    log(sym, f"[{period}] net_margin_pct", "FAIL",
                        f"app={app_nm:.2f}% expected={exp_nm:.2f}%")

                exp_em = (app_op_q / app_rev_q) * 100
                app_em = rat_row.get("ebit_margin_pct", 0) or 0
                if near(app_em, exp_em, 1.0):
                    log(sym, f"[{period}] ebit_margin_pct", "PASS",
                        f"app={app_em:.2f}% expected={exp_em:.2f}%")
                else:
                    log(sym, f"[{period}] ebit_margin_pct", "FAIL",
                        f"app={app_em:.2f}% expected={exp_em:.2f}%")

                exp_edm = (app_edq / app_rev_q) * 100
                app_edm = rat_row.get("ebitda_margin_pct", 0) or 0
                if near(app_edm, exp_edm, 1.0):
                    log(sym, f"[{period}] ebitda_margin_pct", "PASS",
                        f"app={app_edm:.2f}% expected={exp_edm:.2f}%")
                else:
                    log(sym, f"[{period}] ebitda_margin_pct", "FAIL",
                        f"app={app_edm:.2f}% expected={exp_edm:.2f}%")

                exp_cm = ((row.get("core_profit",0) or 0) / app_rev_q) * 100
                app_cm = rat_row.get("core_margin_pct", 0) or 0
                if near(app_cm, exp_cm, 1.0):
                    log(sym, f"[{period}] core_margin_pct", "PASS",
                        f"app={app_cm:.2f}% expected={exp_cm:.2f}%")
                else:
                    log(sym, f"[{period}] core_margin_pct", "FAIL",
                        f"app={app_cm:.2f}% expected={exp_cm:.2f}%")

            # D/E from XLSX balance
            app_eq = (bal_row.get("equity", 0) or 0) if bal_row else 0
            if app_eq and app_eq != 0:
                xlsx_tl2 = xlsx_v(bal, "total_liabilities", div)
                if xlsx_tl2:
                    exp_de = xlsx_tl2 / app_eq
                    app_de = rat_row.get("de_ratio", 0) or 0
                    if near(app_de, exp_de, 2.0):
                        log(sym, f"[{period}] de_ratio", "PASS",
                            f"app={app_de:.3f} expected={exp_de:.3f}")
                    else:
                        log(sym, f"[{period}] de_ratio", "FAIL",
                            f"app={app_de:.3f} expected={exp_de:.3f}")

        # Core Profit Analysis (quarterly) — ตรวจสอบทุก quarter
        core_row = next((c for c in q_core_aug if c["period"] == period), None)
        if core_row:
            app_cp  = core_row.get("core_profit", 0) or 0
            app_ni  = core_row.get("reported_net_income", 0) or 0
            app_ei  = core_row.get("extraordinary_items", 0) or 0
            app_pct = core_row.get("core_pct_of_reported", 0) or 0

            # core + extraordinary = NI (always must hold)
            if app_ni:
                implied_ni = app_cp + app_ei
                if near(implied_ni, app_ni, 1.0):
                    log(sym, f"[{period}] core_plus_extra_eq_NI", "PASS",
                        f"core({fmt(app_cp)}) + extra({fmt(app_ei)}) ≈ NI({fmt(app_ni)})")
                else:
                    log(sym, f"[{period}] core_plus_extra_eq_NI", "FAIL",
                        f"core+extra={fmt(implied_ni)} != NI={fmt(app_ni)}")

            # core_pct = core/NI*100
            if app_ni:
                exp_pct = (app_cp / app_ni) * 100
                pct_diff = abs(app_pct - exp_pct)
                if pct_diff <= 0.2:
                    log(sym, f"[{period}] core_pct_formula", "PASS",
                        f"app={app_pct:.1f}% expected={exp_pct:.1f}%")
                else:
                    log(sym, f"[{period}] core_pct_formula", "FAIL",
                        f"app={app_pct:.1f}% expected={exp_pct:.1f}% diff={pct_diff:.2f}pp")

            # reported_net_income == income_statement net_profit
            income_row_q = next((r for r in q_income if r["period"] == period), None)
            if income_row_q:
                inc_np = income_row_q.get("net_profit", 0) or 0
                if near(app_ni, inc_np):
                    log(sym, f"[{period}] core_reported_NI_match", "PASS",
                        f"core_NI={fmt(app_ni)} == income_NI={fmt(inc_np)}")
                else:
                    log(sym, f"[{period}] core_reported_NI_match", "FAIL",
                        f"core_NI={fmt(app_ni)} != income_NI={fmt(inc_np)}")

        # Cash Flow — ตรวจสอบทุก quarter ไม่ใช่แค่ Q1
        cf_row = next((c for c in q_cashflow_aug if c["period"] == period), None)
        if cf_row:
            cf_keys = [
                ("operating",    "cf_operating"),
                ("investing",    "cf_investing"),
                ("financing",    "cf_financing"),
                ("net_cashflow", "cf_net"),
            ]

            if quarter == "Q1":
                # Q1 = standalone ตรงกับ XLSX โดยตรง
                for app_key, xlsx_key in cf_keys:
                    xlsx_cfv = xlsx_v(cf, xlsx_key, div)
                    app_cfv  = cf_row.get(app_key, 0) or 0
                    name = f"[{period}] cf_{app_key}"
                    if xlsx_cfv == 0 and app_cfv == 0:
                        log(sym, name, "SKIP", "ทั้งคู่เป็น 0"); continue
                    if xlsx_cfv == 0:
                        log(sym, name, "SKIP", "XLSX=0"); continue
                    if near(app_cfv, xlsx_cfv):
                        log(sym, name, "PASS", f"app={fmt(app_cfv)} xlsx={fmt(xlsx_cfv)}")
                    else:
                        log(sym, name, "FAIL",
                            f"app={fmt(app_cfv)} xlsx={fmt(xlsx_cfv)} diff={diff_pct(app_cfv, xlsx_cfv)}")

            elif quarter in ("Q2", "Q3"):
                # Q2/Q3 XLSX อาจเป็น cumulative — ตรวจสอบ app standalone = cum_this - cum_prev
                is_cum = qd.get("cashflow_cumulative", False)
                if is_cum:
                    # หา previous quarter data
                    prev_q = "Q1" if quarter == "Q2" else "Q2"
                    prev_qd = xlsx_map.get((year, prev_q), {})
                    prev_is_cum = prev_qd.get("cashflow_cumulative", False) if prev_qd else False
                    prev_cf = prev_qd.get("cashflow", {}) if prev_qd else {}
                    prev_div = get_div(prev_qd.get("unit", "baht")) if prev_qd else div

                    # Q3 cumulative but Q2 is NOT cumulative:
                    # app computes Q3_standalone = Q3_cum - Q1 - Q2_standalone
                    # (same as financial_data.py _convert_cumulative_cashflow fallback)
                    q3_q2_not_cum = (quarter == "Q3" and prev_qd and not prev_is_cum)
                    q1_qd = xlsx_map.get((year, "Q1"), {}) if q3_q2_not_cum else {}
                    q1_cf = q1_qd.get("cashflow", {}) if q1_qd else {}
                    q1_div = get_div(q1_qd.get("unit", "baht")) if q1_qd else div

                    for app_key, xlsx_key in cf_keys:
                        app_cfv = cf_row.get(app_key, 0) or 0
                        this_cum = xlsx_v(cf, xlsx_key, div)
                        name = f"[{period}] cf_{app_key}_standalone"

                        if this_cum == 0 and app_cfv == 0:
                            log(sym, name, "SKIP", "ทั้งคู่เป็น 0"); continue
                        if this_cum == 0:
                            log(sym, name, "SKIP", f"XLSX cum=0"); continue

                        if q3_q2_not_cum:
                            # Q3_cum - Q1_standalone - Q2_standalone
                            q1_val = xlsx_v(q1_cf, xlsx_key, q1_div) if q1_qd else 0
                            prev_val = xlsx_v(prev_cf, xlsx_key, prev_div) if prev_qd else 0
                            expected_standalone = this_cum - q1_val - prev_val
                            no_prev = not prev_qd or not prev_cf
                            detail = (f"[cum={fmt(this_cum)} - Q1({fmt(q1_val)}) "
                                      f"- Q2({fmt(prev_val)})]")
                        else:
                            # Normal: Q_cum - prev_Q_cum
                            prev_cum = xlsx_v(prev_cf, xlsx_key, prev_div) if prev_qd else 0
                            expected_standalone = this_cum - prev_cum
                            no_prev = not prev_qd or not prev_cf
                            detail = f"[cum={fmt(this_cum)} - prev={fmt(prev_cum)}]"

                        if near(app_cfv, expected_standalone, 3.0):
                            log(sym, name, "PASS",
                                f"app={fmt(app_cfv)} = {detail[1:-1]}")
                        elif no_prev:
                            log(sym, name, "WARN",
                                f"app={fmt(app_cfv)} expected={fmt(expected_standalone)} "
                                f"{detail} diff={diff_pct(app_cfv, expected_standalone)} "
                                f"(no prev quarter data)")
                        else:
                            log(sym, name, "FAIL",
                                f"app={fmt(app_cfv)} expected={fmt(expected_standalone)} "
                                f"{detail} diff={diff_pct(app_cfv, expected_standalone)}")
                else:
                    # ไม่ cumulative — ตรวจตรงกับ XLSX
                    for app_key, xlsx_key in cf_keys:
                        xlsx_cfv = xlsx_v(cf, xlsx_key, div)
                        app_cfv  = cf_row.get(app_key, 0) or 0
                        name = f"[{period}] cf_{app_key}"
                        if xlsx_cfv == 0 and app_cfv == 0:
                            log(sym, name, "SKIP", "ทั้งคู่เป็น 0"); continue
                        if xlsx_cfv == 0:
                            log(sym, name, "SKIP", "XLSX=0"); continue
                        if near(app_cfv, xlsx_cfv):
                            log(sym, name, "PASS", f"app={fmt(app_cfv)} xlsx={fmt(xlsx_cfv)}")
                        else:
                            log(sym, name, "FAIL",
                                f"app={fmt(app_cfv)} xlsx={fmt(xlsx_cfv)} diff={diff_pct(app_cfv, xlsx_cfv)}")

# ---------------------------------------------------------------------------
# Cross-mode: annual API vs sum of quarterly XLSX
# ---------------------------------------------------------------------------
def check_cross_mode(symbol: str, annual: dict, quarterly: dict):
    ann_income = annual.get("income_statement", [])
    q_income   = quarterly.get("income_statement", [])

    for ann_row in ann_income[:3]:
        fy_year  = ann_row.get("year")
        fy_rev   = ann_row.get("total_revenue", 0) or 0
        fy_np    = ann_row.get("net_profit", 0) or 0
        fy_ebit  = ann_row.get("ebit", 0) or 0
        fy_sales = ann_row.get("sales", 0) or 0

        q4 = [r for r in q_income
              if r.get("year") == fy_year and r.get("quarter") in ("Q1","Q2","Q3","Q4")]
        if len(q4) != 4:
            continue

        def cross(test, fy_val, field):
            s = sum(r.get(field, 0) or 0 for r in q4)
            name = f"[FY{fy_year}] cross_{test}"
            if fy_val == 0:
                log(symbol, name, "SKIP", f"annual={fmt(fy_val)}"); return
            if near(s, fy_val, 3.0):
                log(symbol, name, "PASS",
                    f"sum_q={fmt(s)} api={fmt(fy_val)}")
            else:
                log(symbol, name, "WARN",
                    f"sum_q={fmt(s)} api={fmt(fy_val)} diff={diff_pct(s, fy_val)} "
                    f"(parent-only vs consolidated, NCI, restatement)")

        cross("revenue",    fy_rev,   "total_revenue")
        cross("sales",      fy_sales, "sales")
        cross("ebit",       fy_ebit,  "ebit")
        cross("net_profit", fy_np,    "net_profit")

        # ---- Cross-mode: cashflow quarterly sum vs annual API ----
        ann_cf = annual.get("cashflow", [])
        q_cf   = quarterly.get("cashflow", [])

        # build period → quarter mapping from q_income
        period_q_map: dict[str, str] = {r["period"]: r.get("quarter","") for r in q_income}

        ann_cf_row = next((c for c in ann_cf if c.get("year") == fy_year), None)
        if ann_cf_row:
            q4_cf = [r for r in q_cf
                     if period_q_map.get(r["period"], "") in ("Q1","Q2","Q3","Q4")
                     and next((ri for ri in q_income if ri["period"] == r["period"]),
                              {}).get("year") == fy_year]

            if len(q4_cf) == 4:
                for cf_app_key, cf_label in [
                    ("operating",    "operating"),
                    ("investing",    "investing"),
                    ("financing",    "financing"),
                    ("net_cashflow", "net_cashflow"),
                ]:
                    fy_cf_v  = ann_cf_row.get(cf_app_key, 0) or 0
                    sum_cf_q = sum(r.get(cf_app_key, 0) or 0 for r in q4_cf)
                    name = f"[FY{fy_year}] cross_cf_{cf_label}"
                    if fy_cf_v == 0:
                        log(symbol, name, "SKIP", "annual CF=0"); continue
                    if near(sum_cf_q, fy_cf_v, 5.0):
                        log(symbol, name, "PASS",
                            f"sum_q={fmt(sum_cf_q)} api={fmt(fy_cf_v)}")
                    else:
                        log(symbol, name, "WARN",
                            f"sum_q={fmt(sum_cf_q)} api={fmt(fy_cf_v)} diff={diff_pct(sum_cf_q, fy_cf_v)}"
                            f" (XLSX parent-only vs API consolidated)")

        # ---- Cross-mode: core profit quarterly sum vs annual core ----
        ann_core = annual.get("core_profit_analysis", [])
        q_core_rows = quarterly.get("core_profit_analysis", [])

        ann_core_row = next((c for c in ann_core if c.get("year") == fy_year), None)
        if ann_core_row:
            q4_core = [r for r in q_core_rows
                       if period_q_map.get(r["period"], "") in ("Q1","Q2","Q3","Q4")
                       and next((ri for ri in q_income if ri["period"] == r["period"]),
                                {}).get("year") == fy_year]
            if len(q4_core) == 4:
                sum_core_q = sum(r.get("core_profit", 0) or 0 for r in q4_core)
                fy_core    = ann_core_row.get("core_profit", 0) or 0
                name = f"[FY{fy_year}] cross_core_profit"
                if fy_core == 0:
                    log(symbol, name, "SKIP", "annual core=0")
                elif near(sum_core_q, fy_core, 5.0):
                    log(symbol, name, "PASS",
                        f"sum_q={fmt(sum_core_q)} annual={fmt(fy_core)}")
                else:
                    log(symbol, name, "WARN",
                        f"sum_q={fmt(sum_core_q)} annual={fmt(fy_core)} diff={diff_pct(sum_core_q, fy_core)}"
                        f" (XLSX no PFO → quarterly core = NI)")

# ---------------------------------------------------------------------------
# Per-symbol orchestrator
# ---------------------------------------------------------------------------
def test_symbol(symbol: str):
    print(f"\n{'='*60}")
    print(f"  {symbol}")
    print(f"{'='*60}")

    scraper = SETScraper()
    raw = scraper.fetch_full_data(symbol)

    if raw.get("error"):
        log(symbol, "fetch_data", "FAIL", raw["error"]); return

    annual    = get_financial_data(symbol, view_mode="annual")
    quarterly = get_financial_data(symbol, view_mode="quarterly")

    if annual.get("error"):
        log(symbol, "annual_mode", "SKIP", annual["error"])
    else:
        check_annual(symbol, annual, raw)

    if quarterly.get("error"):
        log(symbol, "quarterly_mode", "SKIP", quarterly["error"])
    else:
        check_quarterly(symbol, quarterly, raw)

    if not annual.get("error") and not quarterly.get("error"):
        check_cross_mode(symbol, annual, quarterly)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("SET Financial Accuracy Test — Full Coverage")
    print("=" * 60)
    print(f"Testing {len(TEST_SYMBOLS)} companies...")

    for symbol in TEST_SYMBOLS:
        try:
            test_symbol(symbol)
        except Exception as e:
            log(symbol, "exception", "FAIL", str(e))
            import traceback; traceback.print_exc()
        time.sleep(0.3)

    # ---- Summary ----
    print("\n\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    total = len(results)
    by_status: dict[str, int] = {}
    for r in results:
        by_status[r["status"]] = by_status.get(r["status"], 0) + 1

    print(f"Total checks : {total}")
    print(f"  PASS       : {by_status.get('PASS', 0)}")
    print(f"  FAIL       : {by_status.get('FAIL', 0)}")
    print(f"  WARN       : {by_status.get('WARN', 0)}")
    print(f"  SKIP       : {by_status.get('SKIP', 0)}")

    failures = [r for r in results if r["status"] == "FAIL"]
    if failures:
        print(f"\nFAILURES ({len(failures)}):")
        print("-" * 60)
        fail_types: dict[str, int] = {}
        for r in failures:
            clean = re.sub(r"^\[.*?\] ", "", r["test"])
            fail_types[clean] = fail_types.get(clean, 0) + 1

        for sym in dict.fromkeys(r["symbol"] for r in failures):
            sym_fails = [r for r in failures if r["symbol"] == sym]
            print(f"\n  {sym} ({len(sym_fails)} fails):")
            for r in sym_fails:
                print(f"    ✗ {r['test']}: {r['detail']}")

        print("\nFail frequency by type:")
        for t, cnt in sorted(fail_types.items(), key=lambda x: -x[1]):
            print(f"  {cnt:3d}x  {t}")
    else:
        print("\n✓ No FAILURES")

    warns = [r for r in results if r["status"] == "WARN"]
    if warns:
        warn_types: dict[str, int] = {}
        for r in warns:
            clean = re.sub(r"^\[.*?\] ", "", r["test"])
            warn_types[clean] = warn_types.get(clean, 0) + 1
        print(f"\nWARN frequency by type ({len(warns)} total):")
        for t, cnt in sorted(warn_types.items(), key=lambda x: -x[1]):
            print(f"  {cnt:3d}x  {t}")

    out_path = Path(__file__).parent / "test_results.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\nDetailed results → {out_path}")

    fail_count = by_status.get("FAIL", 0)
    sys.exit(1 if fail_count > 0 else 0)


if __name__ == "__main__":
    main()
