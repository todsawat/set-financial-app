"""
Financial Data Processing Module
=================================
Processes data from SET.or.th APIs and ZIP/XLSX files.
Provides:
- Multi-year income statement, balance sheet, ratios (annual from API)
- Quarterly data from XLSX ZIPs (Q1-Q3 direct, Q4 = FY - Q1 - Q2 - Q3)
- Core profit vs extraordinary items separation
- Formatted DataFrames for display
"""

import re
import pandas as pd
from set_scraper import SETScraper


# Singleton scraper
_scraper: SETScraper | None = None


def get_scraper() -> SETScraper:
    global _scraper
    if _scraper is None:
        _scraper = SETScraper()
    return _scraper


# ============================================================
# Thai-calendar year helpers  (พ.ศ. ↔ ค.ศ.)
# ============================================================
_THAI_MONTHS = {
    "มกราคม": 1, "กุมภาพันธ์": 2, "มีนาคม": 3, "เมษายน": 4,
    "พฤษภาคม": 5, "มิถุนายน": 6, "กรกฎาคม": 7, "สิงหาคม": 8,
    "กันยายน": 9, "ตุลาคม": 10, "พฤศจิกายน": 11, "ธันวาคม": 12,
}


def _parse_thai_period(desc: str) -> dict:
    """
    Parse period description like
      "สำหรับงวดสามเดือนสิ้นสุดวันที่ 31 ธันวาคม 2568"
      "ณ วันที่ 31 ธันวาคม 2568"
    Returns {"month": 12, "year_ce": 2025, "months_count": 3}
    """
    result = {"month": 0, "year_ce": 0, "months_count": 0}
    if not desc:
        return result

    for th, num in _THAI_MONTHS.items():
        if th in desc:
            result["month"] = num
            break

    # Year: Thai calendar year (พ.ศ.) = CE + 543
    year_match = re.search(r"(\d{4})", desc)
    if year_match:
        y = int(year_match.group(1))
        result["year_ce"] = y - 543 if y > 2400 else y

    # Months count
    month_words = {
        "สามเดือน": 3, "หกเดือน": 6, "เก้าเดือน": 9,
        "สิบสองเดือน": 12, "หนึ่งปี": 12,
    }
    for w, n in month_words.items():
        if w in desc:
            result["months_count"] = n
            break

    return result


def _infer_quarter(q_str: str | None, period_desc: str) -> str:
    """Determine quarter label (Q1/Q2/Q3/YE) from API quarter field or XLSX period description."""
    if q_str and q_str in ("Q1", "Q2", "Q3", "YE", "Q4", "Q9"):
        return q_str

    p = _parse_thai_period(period_desc)
    mc = p.get("months_count", 0)
    if mc == 3:
        return "Q1"  # first quarter of fiscal year
    if mc == 6:
        return "Q2"
    if mc == 9:
        return "Q3"
    if mc == 12 or mc == 0:
        return "YE"
    return "Q1"


# ============================================================
# Data Fetching
# ============================================================

def get_financial_data(symbol: str, view_mode: str = "annual") -> dict:
    """
    Fetch and process complete financial data for a symbol.

    view_mode:
      - "annual"   → uses XLSX FY entries from ZIP cache (values converted to thousands THB)
      - "quarterly" → uses XLSX from ZIP cache (values in Baht, converted to thousands)
                      Q1-Q3 come directly from XLSX.
                      Q4 = FY(annual) − (Q1+Q2+Q3) when enough data exists.
    """
    scraper = get_scraper()
    raw = scraper.fetch_full_data(symbol)

    if raw.get("error"):
        return {"error": raw["error"], "symbol": symbol}

    company = raw.get("company", {
        "name": symbol, "name_th": symbol, "sector": "N/A", "market": "SET",
    })

    if view_mode == "quarterly":
        return _build_quarterly(raw, company)
    else:
        return _build_annual(raw, company)


# ============================================================
# Annual mode (from XLSX FY entries, no API)
# ============================================================

def _build_tax_rate_map(xlsx_data: list[dict]) -> dict[int, float]:
    """
    Build year → effective_tax_rate from XLSX annual (Q9/YE) data.
    effective_tax_rate = (PBT - NP) / PBT
    Used to convert pre-tax PFO to after-tax equivalent.
    Returns dict: {year: tax_rate_0to1}
    """
    tax_map: dict[int, float] = {}
    for qd in xlsx_data:
        if qd.get("quarter") not in ("Q9", "YE"):
            continue
        y = qd.get("year", 0)
        if isinstance(y, str):
            y = int(y) if y.isdigit() else 0
        inc = qd.get("income", {})
        unit = qd.get("unit", "baht")
        div = 1000.0 if unit == "baht" else (0.001 if unit == "millions" else 1.0)

        pbt_item = inc.get("profit_before_tax", {})
        np_item  = inc.get("net_profit", {}) or inc.get("ni_owners", {})
        if not isinstance(pbt_item, dict) or not isinstance(np_item, dict):
            continue
        pbt = (pbt_item.get("current", 0) or 0) / div
        np_ = (np_item.get("current", 0) or 0) / div
        if pbt and abs(pbt) > 1:
            rate = (pbt - np_) / pbt
            # Clamp to [0, 0.5] — ป้องกัน outlier เช่น บริษัทขาดทุน
            tax_map[y] = max(0.0, min(rate, 0.5))
    return tax_map


def _build_annual_xlsx_map(xlsx_data: list[dict]) -> dict[int, dict]:
    """
    Build year → {finance_cost, tax_expense, effective_tax_rate} from XLSX Q9/YE data.
    ใช้กับ annual path เพื่อเติมค่าที่ factsheet API ไม่มี
    """
    result: dict[int, dict] = {}
    for qd in xlsx_data:
        if qd.get("quarter") not in ("Q9", "YE"):
            continue
        y = qd.get("year", 0)
        if isinstance(y, str):
            y = int(y) if y.isdigit() else 0
        inc = qd.get("income", {})
        unit = qd.get("unit", "baht")
        div = 1000.0 if unit == "baht" else (0.001 if unit == "millions" else 1.0)

        def _v(field: str) -> float:
            item = inc.get(field, {})
            if not isinstance(item, dict):
                return 0.0
            return (item.get("current", 0) or 0) / div

        pbt = _v("profit_before_tax")
        tax = _v("tax_expense")   # มักเป็นค่าลบ
        fc  = _v("finance_cost")  # มักเป็นค่าลบ

        # effective tax rate จากค่าจริง
        if tax and pbt and abs(pbt) > 1:
            eff_tax = max(0.0, min(abs(tax) / abs(pbt), 0.60))
        elif pbt and abs(pbt) > 1:
            np_ = _v("net_profit") or _v("ni_owners")
            eff_tax = max(0.0, min((pbt - np_) / pbt, 0.60)) if np_ else 0.20
        else:
            eff_tax = 0.20

        result[y] = {
            "finance_cost": fc,
            "tax_expense": tax,
            "effective_tax_rate": eff_tax,
        }
    return result


def _build_annual(raw: dict, company: dict) -> dict:
    """
    Build annual view from XLSX ZIP data ONLY (no API).

    Data source: FY/Q9/YE entries from quarterly_xlsx_data cache.
    Each FY filing has current (own year) and prev (prior year) columns.
    """
    q_cache: list[dict] = raw.get("quarterly_xlsx_data", [])
    if not q_cache:
        return {
            "error": "ไม่พบข้อมูลงบการเงินรายปี (ไม่พบ ZIP files)\nลองเปลี่ยนเป็นมุมมอง 'รายไตรมาส' ก่อน",
            "symbol": raw["symbol"],
            "company": company,
        }

    # ── Step 1: Collect FY entries from XLSX cache ──
    annual_xlsx_map: dict[int, dict] = {}
    for qd in q_cache:
        q = qd.get("quarter", "")
        y = qd.get("year", 0)
        if isinstance(y, str):
            y = int(y) if y.isdigit() else 0
        if not q or q in ("Q9",):
            q = _infer_quarter(q, qd.get("period_description", ""))
        if q in ("YE", "Q9"):
            annual_xlsx_map[y] = qd

    if not annual_xlsx_map:
        return {
            "error": "ไม่พบข้อมูลงบการเงินรายปี (ไม่มี FY entries ใน ZIP cache)",
            "symbol": raw["symbol"],
            "company": company,
        }

    # ── Step 2: Build rows using _xlsx_quarter_to_row ──
    rows: list[dict] = []

    # Pass 1: current column (authoritative own-year data)
    for y, qd in sorted(annual_xlsx_map.items(), reverse=True):
        label = f"FY{y}"
        if not any(r["period"] == label for r in rows):
            row = _xlsx_quarter_to_row(qd, label, "current")
            row["quarter"] = "YE"
            row["year"] = y
            row["source"] = "XLSX (FY)"
            row["download_url"] = qd.get("download_url", "")
            rows.append(row)

    # Pass 2: prev column (extend one year back from oldest filing)
    for y, qd in sorted(annual_xlsx_map.items(), reverse=True):
        prev_label = f"FY{y - 1}"
        if not any(r["period"] == prev_label for r in rows):
            row = _xlsx_quarter_to_row(qd, prev_label, "prev")
            row["quarter"] = "YE"
            row["year"] = y - 1
            row["source"] = "XLSX (FY prev)"
            row["download_url"] = qd.get("download_url", "")
            rows.append(row)

    # Sort newest first, cap at 10 years
    rows.sort(key=lambda r: r.get("year", 0), reverse=True)
    rows = rows[:10]

    if not rows:
        return {
            "error": "ไม่สามารถสร้างข้อมูลรายปีได้",
            "symbol": raw["symbol"],
            "company": company,
        }

    # ── Step 3: Build standard output structures ──
    income_data = []
    balance_data = []
    ratios_data = []
    core_analysis = []
    cashflow_data = []

    for r in rows:
        tr = r.get("total_revenue", 0)
        np_ = r.get("net_profit", 0)
        gp = r.get("gross_profit", 0) or 0
        op = r.get("ebit", 0)
        ebitda = r.get("ebitda", 0)
        core = r.get("core_profit", np_)
        sales_val = r.get("sales", 0) or 0

        gross_margin = (gp / sales_val * 100) if sales_val and gp else 0
        net_margin = (np_ / tr * 100) if tr else 0
        ebit_margin = (op / tr * 100) if tr else 0
        ebitda_margin = (ebitda / tr * 100) if tr else 0
        core_margin = (core / tr * 100) if tr else 0
        core_pct = (core / np_ * 100) if np_ else 0

        eq = r.get("equity", 0)
        ta = r.get("total_assets", 0)
        tl = r.get("total_liabilities", 0)

        # FY: no annualization needed (already 12-month values)
        roe_pct = round(np_ / eq * 100, 2) if eq else 0
        roa_pct = round(np_ / ta * 100, 2) if ta else 0

        income_data.append({
            "period": r["period"], "quarter": r.get("quarter", ""),
            "year": r.get("year", 0),
            "total_revenue": tr, "sales": r.get("sales", 0),
            "other_revenue": r.get("other_revenue", 0),
            "total_expense": r.get("total_expense", 0),
            "finance_cost": r.get("finance_cost", 0) or 0,
            "tax_expense": r.get("tax_expense", 0) or 0,
            "ebit": op, "ebitda": ebitda,
            "net_profit": np_,
            "profit_from_other_activity": r.get("profit_from_other_activity", 0),
            "core_profit": core,
            "eps": r.get("eps", 0),
            "source": r.get("source", ""),
            "download_url": r.get("download_url", ""),
        })
        balance_data.append({
            "period": r["period"], "year": r.get("year", 0),
            "quarter": r.get("quarter", ""),
            "cash": r.get("cash", 0) or 0,
            "inventories": r.get("inventories", 0) or 0,
            "current_assets": r.get("current_assets", 0) or 0,
            "non_current_assets": r.get("non_current_assets", 0) or 0,
            "total_assets": ta,
            "current_liabilities": r.get("current_liabilities", 0) or 0,
            "non_current_liabilities": r.get("non_current_liabilities", 0) or 0,
            "total_liabilities": tl,
            "equity": eq, "net_debt": tl - eq,
        })
        ratios_data.append({
            "period": r["period"], "year": r.get("year", 0),
            "gross_margin_pct": round(gross_margin, 2),
            "ebit_margin_pct": round(ebit_margin, 2),
            "ebitda_margin_pct": round(ebitda_margin, 2),
            "net_margin_pct": round(net_margin, 2),
            "core_margin_pct": round(core_margin, 2),
            "roe_pct": roe_pct,
            "roa_pct": roa_pct,
            "de_ratio": round(tl / eq if eq else 0, 3),
            "current_ratio": round(r.get("current_assets", 0) / r.get("current_liabilities", 1)
                                   if r.get("current_liabilities") else 0, 3),
            "quick_ratio": round(
                (r.get("current_assets", 0) - r.get("inventories", 0)) / r.get("current_liabilities", 1)
                if r.get("current_liabilities") else 0, 3),
            "effective_tax_rate_pct": r.get("effective_tax_rate", 20.0),
        })
        extra_after_tax = np_ - core
        pfo_pretax = r.get("profit_from_other_activity", 0)
        tax_rate_pct = r.get("effective_tax_rate", 20.0)
        core_analysis.append({
            "period": r["period"], "year": r.get("year", 0),
            "reported_net_income": np_, "core_profit": core,
            "extraordinary_items": extra_after_tax,
            "extraordinary_items_pretax": pfo_pretax,
            "effective_tax_rate": tax_rate_pct,
            "core_pct_of_reported": round(core_pct, 1),
        })
        cashflow_data.append({
            "period": r["period"], "year": r.get("year", 0),
            "operating": r.get("cf_operating", 0),
            "investing": r.get("cf_investing", 0),
            "financing": r.get("cf_financing", 0),
            "net_cashflow": r.get("cf_net", 0),
        })

    return {
        "symbol": raw["symbol"],
        "company": company,
        "income_statement": income_data,
        "balance_sheet": balance_data,
        "ratios": ratios_data,
        "core_profit_analysis": core_analysis,
        "cashflow": cashflow_data,
        "xlsx_detail": raw.get("xlsx_data"),
        "zip_url": raw.get("zip_url"),
        "zip_info": raw.get("zip_info"),
        "data_source": "SET.or.th News Section ZIP (XLSX)",
        "view_mode": "annual",
        "unit": "thousands",
    }


def _make_period_label(quarter: str, year: int, view_mode: str) -> str:
    if quarter in ("Q9", "YE", "Q4"):
        return f"FY{year}"
    if quarter in ("Q1", "Q2", "Q3"):
        return f"{quarter}/{year}"
    if view_mode == "annual":
        return f"FY{year}"
    return f"{quarter}/{year}" if quarter else f"FY{year}"


def _process_highlight_data(data: list[dict], view_mode: str,
                            tax_map: dict | None = None,
                            annual_xlsx_map: dict | None = None) -> dict:
    """Process API highlight records into structured dicts.

    tax_map: {year: effective_tax_rate} from XLSX — used to convert pre-tax
    profitFromOtherActivity to after-tax before subtracting from net_profit.
    core_profit = net_profit - PFO × (1 - tax_rate)

    annual_xlsx_map: {year: {finance_cost, tax_expense, effective_tax_rate}}
    ใช้เติมค่าที่ API ไม่มี เช่น finance_cost, effective_tax_rate จากค่าจริง
    """
    income_data, balance_data, ratios_data, core_analysis, cashflow_data = [], [], [], [], []
    if tax_map is None:
        tax_map = {}
    if annual_xlsx_map is None:
        annual_xlsx_map = {}

    for item in data:
        quarter = item.get("quarter", "")
        year = item.get("year", 0)
        label = _make_period_label(quarter, year, view_mode)

        total_revenue = item.get("totalRevenue", 0) or 0
        sales = item.get("sales", 0) or 0
        total_expense = item.get("totalExpense", 0) or 0
        ebit = item.get("ebit", 0) or 0
        ebitda = item.get("ebitda", 0) or 0
        net_profit = item.get("netProfit", 0) or 0
        profit_from_other = item.get("profitFromOtherActivity", 0) or 0
        eps = item.get("eps", 0) or 0
        total_asset = item.get("totalAsset", 0) or 0
        total_liability = item.get("totalLiability", 0) or 0
        equity = item.get("equity", 0) or 0

        other_revenue = total_revenue - sales

        # ดึงค่าจาก annual_xlsx_map (finance_cost, tax_expense, effective_tax_rate จากค่าจริง)
        xlsx_yr = annual_xlsx_map.get(year, {})
        finance_cost_annual = xlsx_yr.get("finance_cost", 0) or 0
        tax_expense_annual = xlsx_yr.get("tax_expense", 0) or 0
        # effective tax rate: ใช้จาก XLSX ก่อน แล้ว fallback ไป tax_map
        if xlsx_yr.get("effective_tax_rate") is not None:
            tax_rate = xlsx_yr["effective_tax_rate"]
        else:
            tax_rate = tax_map.get(year, 0.20)
        pfo_after_tax = profit_from_other * (1.0 - tax_rate)
        core_profit = net_profit - pfo_after_tax

        ebit_margin = (ebit / total_revenue * 100) if total_revenue else 0
        ebitda_margin = (ebitda / total_revenue * 100) if total_revenue else 0
        core_margin = (core_profit / total_revenue * 100) if total_revenue else 0

        roa = item.get("roa", 0) or 0
        roe = item.get("roe", 0) or 0
        net_margin = item.get("netProfitMargin", 0) or 0
        gross_margin = item.get("grossProfitMargin", 0) or 0
        de_ratio = item.get("deRatio", 0) or 0
        current_ratio = item.get("currentRatio", 0) or 0
        quick_ratio = item.get("quickRatio", 0) or 0

        income_data.append({
            "period": label, "quarter": quarter, "year": year,
            "total_revenue": total_revenue, "sales": sales,
            "other_revenue": other_revenue, "total_expense": total_expense,
            "finance_cost": finance_cost_annual,
            "tax_expense": tax_expense_annual,
            "ebit": ebit, "ebitda": ebitda, "net_profit": net_profit,
            "profit_from_other_activity": profit_from_other,
            "core_profit": core_profit, "eps": eps,
        })
        balance_data.append({
            "period": label, "year": year, "quarter": quarter,
            "current_assets": 0, "non_current_assets": 0,
            "total_assets": total_asset,
            "current_liabilities": 0, "non_current_liabilities": 0,
            "total_liabilities": total_liability,
            "equity": equity, "net_debt": total_liability - equity,
        })
        ratios_data.append({
            "period": label, "year": year,
            "gross_margin_pct": round(gross_margin, 2),
            "ebit_margin_pct": round(ebit_margin, 2),
            "ebitda_margin_pct": round(ebitda_margin, 2),
            "net_margin_pct": round(net_margin, 2),
            "core_margin_pct": round(core_margin, 2),
            "roe_pct": round(roe, 2), "roa_pct": round(roa, 2),
            "de_ratio": round(de_ratio, 3),
            "current_ratio": round(current_ratio, 3),
            "quick_ratio": round(quick_ratio, 3),
            "effective_tax_rate_pct": round(tax_rate * 100, 1),  # ใช้จาก xlsx_yr ถ้ามี
        })
        core_pct = (core_profit / net_profit * 100) if net_profit else 0
        core_analysis.append({
            "period": label, "year": year,
            "reported_net_income": net_profit, "core_profit": core_profit,
            # extraordinary_items เก็บค่า after-tax (เพื่อให้ NI = Core + Extra)
            "extraordinary_items": pfo_after_tax,
            "extraordinary_items_pretax": profit_from_other,
            "effective_tax_rate": round(tax_rate * 100, 1),
            "core_pct_of_reported": round(core_pct, 1),
        })
        cashflow_data.append({
            "period": label, "year": year,
            "operating": item.get("netOperating", 0) or 0,
            "investing": item.get("netInvesting", 0) or 0,
            "financing": item.get("netFinancing", 0) or 0,
            "net_cashflow": item.get("netCashflow", 0) or 0,
        })

    return {
        "income_statement": income_data,
        "balance_sheet": balance_data,
        "ratios": ratios_data,
        "core_profit_analysis": core_analysis,
        "cashflow": cashflow_data,
    }


# ============================================================
# Quarterly mode (from XLSX ZIPs)
# ============================================================

def _quarter_sort_key(q: str) -> int:
    """Sort key for quarter labels: Q1=1, Q2=2, Q3=3, Q4=4, YE/Q9=5."""
    return {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4, "YE": 5, "Q9": 5}.get(q, 0)


def _get_divisor(unit: str) -> float:
    """Return divisor to convert XLSX raw values → thousands THB."""
    if unit == "baht":
        return 1000.0
    if unit == "millions":
        return 0.001
    return 1.0  # already thousands


def _get_val(section: dict, field: str, col: str = "current") -> float:
    """Safely get a value from quarterly summary section."""
    item = section.get(field, {})
    if isinstance(item, dict):
        return item.get(col, 0) or 0
    return 0


def _xlsx_quarter_to_row(q_data: dict, label: str, col: str = "current") -> dict:
    """
    Convert one XLSX quarterly summary into a standard row dict.
    Auto-detects unit from XLSX metadata:
      - "baht" → divide by 1000 to get thousands
      - "thousands" → already in thousands, no division
      - "millions" → multiply by 1000 to get thousands

    Core profit logic (from CF indirect method ground truth):
      1. Parse special items reversed out in CF adjustment section
         (negative CF value = gain was reversed → non-operating IS gain)
      2. special_items_pretax = -sum(CF special item values)
         (negate: CF shows them as negative, but they are positive IS gains)
      3. effective_tax_rate = (PBT - NP) / PBT
      4. core_profit = NP - special_items_pretax × (1 - tax_rate)
    """
    inc = q_data.get("income", {})
    bal = q_data.get("balance", {})
    cf = q_data.get("cashflow", {})

    unit = q_data.get("unit", "baht")
    if unit == "thousands":
        divisor = 1
    elif unit == "millions":
        divisor = 0.001  # multiply by 1000
    else:
        divisor = 1000  # Baht → thousands

    def v(section, field):
        return _get_val(section, field, col) / divisor

    total_revenue = v(inc, "total_revenue")
    sales = v(inc, "sales")
    other_rev = v(inc, "other_revenue")
    # Fallback: ถ้าไม่มี total_revenue line ใน XLSX → คำนวณจาก sales + other_revenue
    if not total_revenue and (sales or other_rev):
        total_revenue = sales + other_rev
    net_profit = v(inc, "ni_owners") or v(inc, "net_profit")
    gross_profit = v(inc, "gross_profit")
    operating_profit = v(inc, "operating_profit")
    eps_val = _get_val(inc, "eps", col)  # EPS stays in Baht (per share)
    depreciation = v(inc, "depreciation")

    # Effective tax rate — ใช้ค่าจริงจากงบ: tax_expense / profit_before_tax
    profit_before_tax = v(inc, "profit_before_tax")
    tax_expense = v(inc, "tax_expense")  # ค่าใช้จ่ายภาษีเงินได้ (มักเป็นค่าลบ)
    if tax_expense and profit_before_tax and abs(profit_before_tax) > 1:
        # tax_expense มักเป็น negative ใน XLSX → abs เพื่อให้ได้ rate เป็นบวก
        effective_tax_rate = max(0.0, min(abs(tax_expense) / abs(profit_before_tax), 0.60))
    elif profit_before_tax and abs(profit_before_tax) > 1 and net_profit is not None:
        # fallback: คำนวณจาก PBT - NP (เหมือนเดิม)
        effective_tax_rate = max(0.0, min((profit_before_tax - net_profit) / profit_before_tax, 0.60))
    else:
        effective_tax_rate = 0.20  # fallback: Thai corporate tax rate

    # Core profit: use CF special items (indirect method ground truth)
    # special_items_from_cf has raw CF sign: negative = IS gain reversed out
    # special_items_pretax = -sum(cf_values) → positive = total non-operating gains
    special_items_raw = q_data.get("special_items_from_cf", {})
    if special_items_raw:
        # Sum all special item values for the relevant column
        def _to_float(val: object) -> float:
            try:
                return float(val or 0)
            except (TypeError, ValueError):
                return 0.0

        cf_sum = sum(
            _to_float(item.get(col, 0)) / divisor
            for item in special_items_raw.values()
            if isinstance(item, dict)
        )
        # Negate: CF reversal of gains is negative → IS gain is positive
        special_items_pretax = -cf_sum
        core_profit = net_profit - special_items_pretax * (1.0 - effective_tax_rate)
    else:
        # No special items found → core profit = net profit (no adjustment)
        special_items_pretax = 0.0
        core_profit = net_profit

    row = {
        "period": label,
        "total_revenue": total_revenue,
        "sales": sales,
        "other_revenue": v(inc, "other_revenue"),
        "total_expense": v(inc, "total_expense"),
        "gross_profit": gross_profit,
        "finance_cost": v(inc, "finance_cost"),
        "tax_expense": v(inc, "tax_expense"),
        "ebit": operating_profit,
        "ebitda": operating_profit + depreciation if depreciation else operating_profit,
        "net_profit": net_profit,
        "profit_from_other_activity": special_items_pretax,
        "core_profit": core_profit,
        "effective_tax_rate": round(effective_tax_rate * 100, 1),
        "eps": eps_val,
        # Balance sheet (point-in-time, not flow)
        "cash": v(bal, "cash"),
        "short_term_investments": v(bal, "short_term_investments"),
        "trade_receivables": v(bal, "trade_receivables"),
        "inventories": v(bal, "inventories"),
        "current_assets": v(bal, "current_assets"),
        "non_current_assets": v(bal, "non_current_assets"),
        "total_assets": v(bal, "total_assets"),
        "current_liabilities": v(bal, "current_liabilities"),
        "non_current_liabilities": v(bal, "non_current_liabilities"),
        "total_liabilities": v(bal, "total_liabilities"),
        "equity": v(bal, "equity"),
        # Cashflow
        "cf_operating": v(cf, "cf_operating"),
        "cf_investing": v(cf, "cf_investing"),
        "cf_financing": v(cf, "cf_financing"),
        "cf_net": v(cf, "cf_net"),
    }

    # Flag if cashflow values are cumulative (Q2/Q3 filings)
    if q_data.get("cashflow_cumulative"):
        row["_cf_cumulative"] = True

    return row


def _factsheet_period_to_row(
    inc_period: dict | None,
    bal_period: dict | None,
    cf_period: dict | None,
    label: str,
) -> dict:
    """
    Convert one period from factsheet financialstatement API into a standard row.

    Each period has an 'accountList' with items like:
      {"accountNameEn": "Total Revenues", "amount": 1234.56, "divider": 1000, ...}

    Values with divider=1000 → already in thousands THB.
    EPS has divider=1 → keep as-is.
    """
    def _find(period: dict | None, *name_patterns: str) -> float:
        """Find an account by English name pattern, return amount (in thousands THB)."""
        if not period:
            return 0
        for item in period.get("accounts", period.get("accountList", [])):
            name = (item.get("accountName") or item.get("accountNameEn") or "").strip().lower()
            for pat in name_patterns:
                if pat.lower() in name:
                    amount = item.get("amount", 0) or 0
                    # Values with divider=1000 are already in thousands THB
                    return amount
        return 0

    def _find_exact(period: dict | None, *name_patterns: str) -> float:
        """Find by exact English name match."""
        if not period:
            return 0
        for item in period.get("accounts", period.get("accountList", [])):
            name = (item.get("accountName") or item.get("accountNameEn") or "").strip().lower()
            for pat in name_patterns:
                if name == pat.lower():
                    amount = item.get("amount", 0) or 0
                    return amount
        return 0

    def _find_eps(period: dict | None) -> float:
        """Find EPS value (divider=1, value in Baht)."""
        if not period:
            return 0
        for item in period.get("accounts", period.get("accountList", [])):
            name = (item.get("accountName") or item.get("accountNameEn") or "").strip().lower()
            if "eps" in name or "basic earnings per share" in name or "earnings per share" in name:
                return item.get("amount", 0) or 0
        return 0

    # Income statement items
    total_revenue = _find(inc_period, "total revenues", "total revenue")
    sales = _find(inc_period, "revenue from sale", "revenues from sale",
                  "revenue from contract", "revenues from contract",
                  "revenue from rendering", "sales")
    if not sales:
        sales = total_revenue  # fallback: some companies only report total
    other_revenue = _find(inc_period, "other income", "other revenue")
    total_expense = _find(inc_period, "total expenses", "total expense")
    ebitda = _find(inc_period, "ebitda")
    depreciation = _find(inc_period, "depreciation and amortisation",
                         "depreciation and amortization", "depreciation")
    ebit = _find(inc_period, "ebit", "earnings before interest")
    if not ebit and ebitda:
        ebit = ebitda - depreciation
    net_profit = _find(inc_period, "net profit", "profit for the period",
                       "profit for the year", "net income")
    eps = _find_eps(inc_period)

    # Balance sheet items
    total_assets = _find(bal_period, "total assets")
    total_liabilities = _find(bal_period, "total liabilities")
    equity = _find(bal_period, "total shareholders' equity",
                   "total equity", "shareholders' equity")

    # Cashflow items
    cf_operating = _find(cf_period, "operating cash flow", "operating activities",
                         "cash flow from operation")
    cf_investing = _find(cf_period, "investing cash flow", "investing activities",
                         "cash flow from invest")
    cf_financing = _find(cf_period, "financing cash flow", "financing activities",
                         "cash flow from financ")
    cf_net = _find(cf_period, "net cash flow", "net increase", "net decrease",
                   "net change in cash")

    return {
        "period": label,
        "total_revenue": total_revenue,
        "sales": sales,
        "other_revenue": other_revenue,
        "total_expense": total_expense,
        "ebit": ebit,
        "ebitda": ebitda,
        "net_profit": net_profit,
        "profit_from_other_activity": 0,
        "core_profit": net_profit,
        "eps": eps,
        "total_assets": total_assets,
        "total_liabilities": total_liabilities,
        "equity": equity,
        "cf_operating": cf_operating,
        "cf_investing": cf_investing,
        "cf_financing": cf_financing,
        "cf_net": cf_net,
    }


def _api_annual_to_row(fy: dict, label: str) -> dict:
    """
    Convert one annual API highlight record into a standard row dict.
    API values are already in thousands THB.
    """
    total_revenue = fy.get("totalRevenue", 0) or 0
    sales = fy.get("sales", 0) or 0
    net_profit = fy.get("netProfit", 0) or 0
    ebit = fy.get("ebit", 0) or 0
    ebitda = fy.get("ebitda", 0) or 0
    profit_from_other = fy.get("profitFromOtherActivity", 0) or 0

    return {
        "period": label,
        "total_revenue": total_revenue,
        "sales": sales,
        "other_revenue": total_revenue - sales,
        "total_expense": fy.get("totalExpense", 0) or 0,
        "ebit": ebit,
        "ebitda": ebitda,
        "net_profit": net_profit,
        "profit_from_other_activity": profit_from_other,
        "core_profit": net_profit - profit_from_other,
        "eps": fy.get("eps", 0) or 0,
        "total_assets": fy.get("totalAsset", 0) or 0,
        "total_liabilities": fy.get("totalLiability", 0) or 0,
        "equity": fy.get("equity", 0) or 0,
        "cf_operating": fy.get("netOperating", 0) or 0,
        "cf_investing": fy.get("netInvesting", 0) or 0,
        "cf_financing": fy.get("netFinancing", 0) or 0,
        "cf_net": fy.get("netCashflow", 0) or 0,
    }


def _get_raw_cf(quarters_map: dict, year: int, quarter: str) -> dict:
    """Get raw cashflow values from XLSX cache, converted to thousands THB."""
    qd = quarters_map.get((year, quarter), {})
    if not qd:
        return {}
    unit = qd.get("unit", "baht")
    if unit == "thousands":
        divisor = 1
    elif unit == "millions":
        divisor = 0.001
    else:
        divisor = 1000
    cf = qd.get("cashflow", {})
    return {
        f: _get_val(cf, f, "current") / divisor
        for f in ("cf_operating", "cf_investing", "cf_financing", "cf_net")
    }


def _convert_cumulative_cashflow(rows: list[dict], quarters_map: dict):
    """
    Convert cumulative cashflow values to standalone quarterly values.

    Q2/Q3 XLSX cashflow sheets are cumulative (6-month / 9-month).
    Convert using raw XLSX cache data:
      Q2_standalone = Q2_cum - Q1_cum   (Q1_cum = Q1 standalone)
      Q3_standalone = Q3_cum - Q2_cum
    """
    cf_fields = ("cf_operating", "cf_investing", "cf_financing", "cf_net")

    for r in rows:
        if not r.get("_cf_cumulative"):
            continue

        q = r.get("quarter", "")
        y = r.get("year", 0)

        if q == "Q2":
            # Q2_standalone = Q2_cumulative - Q1 (Q1 is always standalone)
            q1_raw = _get_raw_cf(quarters_map, y, "Q1")
            if q1_raw:
                for f in cf_fields:
                    r[f] = r.get(f, 0) - q1_raw.get(f, 0)
            r.pop("_cf_cumulative", None)

        elif q == "Q3":
            # Q3_standalone = Q3_cumulative - Q2_cumulative
            q2_xlsx = quarters_map.get((y, "Q2"), {})
            if q2_xlsx and q2_xlsx.get("cashflow_cumulative"):
                q2_cum = _get_raw_cf(quarters_map, y, "Q2")
                for f in cf_fields:
                    r[f] = r.get(f, 0) - q2_cum.get(f, 0)
            else:
                # Q2 not cumulative — fallback: Q3_cum - Q1 - Q2
                q1_raw = _get_raw_cf(quarters_map, y, "Q1")
                q2_raw = _get_raw_cf(quarters_map, y, "Q2")
                for f in cf_fields:
                    r[f] = r.get(f, 0) - q1_raw.get(f, 0) - q2_raw.get(f, 0)
            r.pop("_cf_cumulative", None)


def _build_quarterly(raw: dict, company: dict) -> dict:
    """
    Build quarterly view from XLSX ZIP data ONLY (no API).

    Data sources:
      1. XLSX ZIP cache — Q1/Q2/Q3 standalone income from quarterly ZIPs
      2. XLSX ZIP cache — FY (Q9/YE) annual income from annual ZIPs
      3. Q4 = FY_XLSX − (Q1+Q2+Q3) for all flow items
      4. Cashflow: Q1 standalone, Q2=cum6-Q1, Q3=cum9-cum6, Q4=FY-cum9

    No API data is used for quarterly values.
    """
    q_cache: list[dict] = raw.get("quarterly_xlsx_data", [])

    if not q_cache:
        return {
            "error": (
                "ไม่มีข้อมูลรายไตรมาส (ไม่พบ ZIP files)\n"
                "ลองเปลี่ยนเป็นมุมมอง 'รายปี' ก่อน"
            ),
            "symbol": raw["symbol"],
            "company": company,
        }

    # ----------------------------------------------------------------
    # Step 1: Separate quarterly vs annual XLSX data
    # q4_helper=True entries: Q3 fetched only to enable Q4 via Strategy B
    #   (income_9m cumulative) — go into quarters_map but NOT display rows.
    # gap_filler=True entries: Q1/Q2 of year+1 fetched to provide Q1/Q2 of
    #   year via their prev column — their current column is suppressed, only
    #   their prev column is used to fill missing Q1/Q2 display rows.
    # ----------------------------------------------------------------
    quarters_map: dict[tuple[int, str], dict] = {}   # (year, Q1/Q2/Q3) → raw xlsx
    annual_xlsx_map: dict[int, dict] = {}             # year → raw xlsx (FY)
    q4_helper_years: set[int] = set()                 # years whose Q3 is helper-only
    gap_filler_entries: list[dict] = []               # gap_filler=True entries

    for qd in q_cache:
        q = qd.get("quarter", "")
        y = qd.get("year", 0)
        if isinstance(y, str):
            y = int(y) if y.isdigit() else 0
        if not q or q in ("Q9",):
            q = _infer_quarter(q, qd.get("period_description", ""))
        if q in ("YE", "Q9"):
            annual_xlsx_map[y] = qd
        else:
            quarters_map[(y, q)] = qd
            if qd.get("q4_helper") and q == "Q3":
                q4_helper_years.add(y)
            if qd.get("gap_filler"):
                gap_filler_entries.append(qd)

    # ----------------------------------------------------------------
    # Step 2: Build rows from XLSX "current" column
    # ----------------------------------------------------------------
    rows: list[dict] = []

    for (y, q), qd in sorted(quarters_map.items(), reverse=True):
        if q in ("YE", "Q9", "Q4"):
            continue
        # Skip Q3 entries that were fetched only to compute Q4 — not for display
        if y in q4_helper_years and q == "Q3":
            continue
        # Skip gap_filler entries — their current column is a newer year's data,
        # not to be displayed. Only their prev column is useful (added below).
        if qd.get("gap_filler"):
            continue
        label = f"{q}/{y}"
        if not any(r["period"] == label for r in rows):
            row = _xlsx_quarter_to_row(qd, label, "current")
            row["quarter"] = q
            row["year"] = y
            row["source"] = "XLSX"
            row["download_url"] = qd.get("download_url", "")
            rows.append(row)

    # Also add "prev" column rows from ZIPs (previous year same quarter)
    for (y, q), qd in sorted(quarters_map.items(), reverse=True):
        if q in ("YE", "Q9", "Q4"):
            continue
        # Skip helper Q3 — prev column also excluded
        if y in q4_helper_years and q == "Q3":
            continue
        prev_label = f"{q}/{y - 1}"
        if not any(r["period"] == prev_label for r in rows):
            row = _xlsx_quarter_to_row(qd, prev_label, "prev")
            row["quarter"] = q
            row["year"] = y - 1
            row["source"] = "XLSX (prev)"
            row["download_url"] = qd.get("download_url", "")
            rows.append(row)

    # Gap-filler entries: add their prev column to fill missing Q1/Q2 rows.
    # These entries are Q1/Q2 of year+1 whose prev column gives Q1/Q2 of year.
    for qd in sorted(gap_filler_entries, key=lambda d: (d.get("year", 0), d.get("quarter", "")), reverse=True):
        q = qd.get("quarter", "")
        y = qd.get("year", 0)
        if isinstance(y, str):
            y = int(y) if str(y).isdigit() else 0
        if q not in ("Q1", "Q2", "Q3"):
            continue
        prev_label = f"{q}/{y - 1}"
        if not any(r["period"] == prev_label for r in rows):
            row = _xlsx_quarter_to_row(qd, prev_label, "prev")
            row["quarter"] = q
            row["year"] = y - 1
            row["source"] = "XLSX (gap)"
            row["download_url"] = qd.get("download_url", "")
            rows.append(row)

    # Add FY rows (needed for Q4 computation, filtered out later).
    # Pass 1: current-column entries take priority (own-year filing is authoritative).
    for y, qd in sorted(annual_xlsx_map.items(), reverse=True):
        label = f"FY{y}"
        if not any(r["period"] == label for r in rows):
            row = _xlsx_quarter_to_row(qd, label, "current")
            row["quarter"] = "YE"
            row["year"] = y
            row["source"] = "XLSX (FY)"
            row["download_url"] = qd.get("download_url", "")
            rows.append(row)

    # Pass 2: prev-column entries fill in years not yet covered by any filing.
    # Oldest FY filing is used to extend one extra year back (for Q4 computation).
    for y, qd in sorted(annual_xlsx_map.items(), reverse=True):
        prev_fy_label = f"FY{y - 1}"
        if not any(r["period"] == prev_fy_label for r in rows):
            row = _xlsx_quarter_to_row(qd, prev_fy_label, "prev")
            row["quarter"] = "YE"
            row["year"] = y - 1
            row["source"] = "XLSX (FY prev)"
            row["download_url"] = qd.get("download_url", "")
            rows.append(row)

    # ----------------------------------------------------------------
    # Step 3: Convert cumulative cashflow to standalone for Q2/Q3
    # ----------------------------------------------------------------
    _convert_cumulative_cashflow(rows, quarters_map)

    # ----------------------------------------------------------------
    # Step 3b: Trim quarterly rows that are older than needed.
    # The oldest FY year determines the oldest Q4 we can compute.
    # Quarterly rows from years older than (oldest_FY_year - 1) are
    # excess "prev column spillover" from direct q_cache entries and
    # should not be displayed or used for Q4 computation.
    # ----------------------------------------------------------------
    if annual_xlsx_map:
        oldest_fy_year = min(annual_xlsx_map.keys())
        # Keep quarterly rows only for years >= oldest_fy_year - 1
        # (the year before oldest FY is needed to compute that year's Q4)
        min_q_year = oldest_fy_year - 1
        rows = [
            r for r in rows
            if r.get("quarter") in ("YE", "Q9") or (r.get("year", 0) >= min_q_year)
        ]

    # ----------------------------------------------------------------
    # Step 4: Compute Q4 = FY_XLSX − (Q1+Q2+Q3) from XLSX data only
    # Strategy B: FY − 9M when only a q4_helper Q3 is available
    # ----------------------------------------------------------------
    _add_q4_rows_from_xlsx(rows, quarters_map=quarters_map, q4_helper_years=q4_helper_years)

    # ----------------------------------------------------------------
    # Step 5: Sort newest first, filter to Q1-Q4 only, keep 20 quarters.
    # Use the latest FY year (from annual_xlsx_map) as the anchor for the
    # 5-year window, so that an in-progress "partial" year (e.g. Q1/2026
    # filed before FY2025 closes) doesn't push the oldest year out of the
    # window.
    # ----------------------------------------------------------------
    rows.sort(key=lambda r: (r.get("year", 0), _quarter_sort_key(r.get("quarter", ""))), reverse=True)
    rows = [r for r in rows if r.get("quarter") in ("Q1", "Q2", "Q3", "Q4")]
    if rows and annual_xlsx_map:
        latest_fy = max(annual_xlsx_map.keys())
        oldest_allowed = latest_fy - 4  # 5 FY years: latest_fy down to latest_fy-4
        rows = [
            r for r in rows
            if oldest_allowed <= r.get("year", 0) <= latest_fy + 1
        ]
    rows = rows[:21]   # hard cap (21 allows one partial/extra quarter for off-FY companies)

    if not rows:
        return {
            "error": "ไม่สามารถสร้างข้อมูลรายไตรมาสได้",
            "symbol": raw["symbol"],
            "company": company,
        }

    # Build standard output structures
    income_data = []
    balance_data = []
    ratios_data = []
    core_analysis = []
    cashflow_data = []

    for r in rows:
        tr = r.get("total_revenue", 0)
        np_ = r.get("net_profit", 0)
        gp = r.get("gross_profit", 0) or 0
        op = r.get("ebit", 0)
        ebitda = r.get("ebitda", 0)
        core = r.get("core_profit", np_)
        sales_val = r.get("sales", 0) or 0

        gross_margin = (gp / sales_val * 100) if sales_val and gp else 0
        net_margin = (np_ / tr * 100) if tr else 0
        ebit_margin = (op / tr * 100) if tr else 0
        ebitda_margin = (ebitda / tr * 100) if tr else 0
        core_margin = (core / tr * 100) if tr else 0
        core_pct = (core / np_ * 100) if np_ else 0

        eq = r.get("equity", 0)
        ta = r.get("total_assets", 0)
        tl = r.get("total_liabilities", 0)

        income_data.append({
            "period": r["period"], "quarter": r.get("quarter", ""),
            "year": r.get("year", 0),
            "total_revenue": tr, "sales": r.get("sales", 0),
            "other_revenue": r.get("other_revenue", 0),
            "total_expense": r.get("total_expense", 0),
            "finance_cost": r.get("finance_cost", 0) or 0,
            "tax_expense": r.get("tax_expense", 0) or 0,
            "ebit": op, "ebitda": ebitda,
            "net_profit": np_,
            "profit_from_other_activity": r.get("profit_from_other_activity", 0),
            "core_profit": core,
            "eps": r.get("eps", 0),
            "source": r.get("source", ""),
            "download_url": r.get("download_url", ""),
        })
        balance_data.append({
            "period": r["period"], "year": r.get("year", 0),
            "quarter": r.get("quarter", ""),
            "cash": r.get("cash", 0) or 0,
            "inventories": r.get("inventories", 0) or 0,
            "current_assets": r.get("current_assets", 0) or 0,
            "non_current_assets": r.get("non_current_assets", 0) or 0,
            "total_assets": ta,
            "current_liabilities": r.get("current_liabilities", 0) or 0,
            "non_current_liabilities": r.get("non_current_liabilities", 0) or 0,
            "total_liabilities": tl,
            "equity": eq, "net_debt": tl - eq,
        })
        # Compute ratios from XLSX data (no API)
        # Annualize quarterly NP for ROE/ROA (×4 approximation)
        quarter_label = r.get("quarter", "")
        if quarter_label == "Q4":
            # Q4 is already FY cumulative subtracted to get Q4 only — still annualize ×4
            annualised_np = np_ * 4
        else:
            annualised_np = np_ * 4
        roe_pct = round(annualised_np / eq * 100, 2) if eq else 0
        roa_pct = round(annualised_np / ta * 100, 2) if ta else 0
        ratios_data.append({
            "period": r["period"], "year": r.get("year", 0),
            "gross_margin_pct": round(gross_margin, 2),
            "ebit_margin_pct": round(ebit_margin, 2),
            "ebitda_margin_pct": round(ebitda_margin, 2),
            "net_margin_pct": round(net_margin, 2),
            "core_margin_pct": round(core_margin, 2),
            "roe_pct": roe_pct,
            "roa_pct": roa_pct,
            "de_ratio": round(tl / eq if eq else 0, 3),
            "current_ratio": round(r.get("current_assets", 0) / r.get("current_liabilities", 1)
                                   if r.get("current_liabilities") else 0, 3),
            "quick_ratio": round(
                (r.get("current_assets", 0) - r.get("inventories", 0)) / r.get("current_liabilities", 1)
                if r.get("current_liabilities") else 0, 3),
            "effective_tax_rate_pct": r.get("effective_tax_rate", 20.0),
        })
        # extraordinary = NP - Core (after-tax gap), derived from CF special items
        extra_after_tax = np_ - core
        pfo_pretax = r.get("profit_from_other_activity", 0)  # special_items_pretax
        tax_rate_pct = r.get("effective_tax_rate", 20.0)
        core_analysis.append({
            "period": r["period"], "year": r.get("year", 0),
            "reported_net_income": np_, "core_profit": core,
            "extraordinary_items": extra_after_tax,
            "extraordinary_items_pretax": pfo_pretax,
            "effective_tax_rate": tax_rate_pct,
            "core_pct_of_reported": round(core_pct, 1),
        })
        cashflow_data.append({
            "period": r["period"], "year": r.get("year", 0),
            "operating": r.get("cf_operating", 0),
            "investing": r.get("cf_investing", 0),
            "financing": r.get("cf_financing", 0),
            "net_cashflow": r.get("cf_net", 0),
        })

    return {
        "symbol": raw["symbol"],
        "company": company,
        "income_statement": income_data,
        "balance_sheet": balance_data,
        "ratios": ratios_data,
        "core_profit_analysis": core_analysis,
        "cashflow": cashflow_data,
        "xlsx_detail": raw.get("xlsx_data"),
        "zip_url": raw.get("zip_url"),
        "zip_info": raw.get("zip_info"),
        "data_source": "SET.or.th News Section ZIP (XLSX)",
        "view_mode": "quarterly",
        "unit": "thousands",  # values in thousands THB
    }


def _add_q4_rows_from_xlsx(
    rows: list[dict],
    quarters_map: "dict[tuple[int,str],dict] | None" = None,
    q4_helper_years: "set[int] | None" = None,
):
    """
    Compute Q4 from XLSX data only.  Two strategies:

    Strategy A (normal): Q4 = FY − (Q1+Q2+Q3)
        Used when Q1/Q2/Q3 rows are all present.

    Strategy B (helper): Q4 = FY − 9M_cumulative
        Used when the oldest window boundary falls on Q4 so Q1/Q2 are absent,
        but a q4_helper Q3 entry was fetched whose income_9m contains the
        cumulative 9-month figure straight from the Q3 filing.
    """
    by_year: dict[int, dict[str, dict]] = {}
    fy_rows: dict[int, dict] = {}

    for r in rows:
        y = r.get("year", 0)
        q = r.get("quarter", "")
        if q in ("Q1", "Q2", "Q3"):
            by_year.setdefault(y, {})[q] = r
        elif q in ("YE", "Q9") and r["period"].startswith("FY"):
            fy_rows[y] = r

    flow_fields = [
        "total_revenue", "sales", "other_revenue", "total_expense",
        "gross_profit",
        "finance_cost", "tax_expense",
        "ebit", "ebitda", "net_profit", "core_profit",
        "profit_from_other_activity",
        "cf_operating", "cf_investing", "cf_financing", "cf_net",
    ]

    for fy_year, fy_row in fy_rows.items():
        if any(r["period"] == f"Q4/{fy_year}" for r in rows):
            continue

        year_qs = by_year.get(fy_year, {})

        # ── Strategy A: have Q1+Q2+Q3 as display rows ──────────────────
        if all(q in year_qs for q in ("Q1", "Q2", "Q3")):
            q4_row = {
                "period": f"Q4/{fy_year}",
                "quarter": "Q4",
                "year": fy_year,
                "source": "FY-(Q1+Q2+Q3)",
            }
            for field in flow_fields:
                fy_val = fy_row.get(field, 0) or 0
                sum_q123 = sum(year_qs[q].get(field, 0) or 0
                               for q in ("Q1", "Q2", "Q3"))
                q4_row[field] = fy_val - sum_q123
            fy_eps = fy_row.get("eps", 0) or 0
            sum_eps = sum(year_qs[q].get("eps", 0) or 0
                          for q in ("Q1", "Q2", "Q3"))
            q4_row["eps"] = fy_eps - sum_eps

        # ── Strategy B: helper Q3 provides 9M cumulative ───────────────
        elif (
            q4_helper_years is not None
            and fy_year in q4_helper_years
            and quarters_map is not None
            and (fy_year, "Q3") in quarters_map
        ):
            helper_qd = quarters_map[(fy_year, "Q3")]
            income_9m = helper_qd.get("income_9m", {})
            if not income_9m:
                continue  # helper has no 9M data — can't compute

            div = _get_divisor(helper_qd.get("unit", "baht"))
            q4_row = {
                "period": f"Q4/{fy_year}",
                "quarter": "Q4",
                "year": fy_year,
                "source": "FY-9M(Q3helper)",
            }
            for field in flow_fields:
                fy_val = fy_row.get(field, 0) or 0
                # income_9m stores raw {"current": ..., "prev": ...}
                nine_m_raw = income_9m.get(field, {})
                nine_m_val = (nine_m_raw.get("current", 0) or 0) / div if isinstance(nine_m_raw, dict) else 0
                q4_row[field] = fy_val - nine_m_val
            # EPS: no reliable 9M EPS in most filings — set to 0
            q4_row["eps"] = 0

        else:
            continue  # not enough data for either strategy

        # Balance sheet: FY point-in-time for both strategies
        q4_row["total_assets"] = fy_row.get("total_assets", 0) or 0
        q4_row["total_liabilities"] = fy_row.get("total_liabilities", 0) or 0
        q4_row["equity"] = fy_row.get("equity", 0) or 0
        q4_row["current_assets"] = fy_row.get("current_assets", 0) or 0
        q4_row["non_current_assets"] = fy_row.get("non_current_assets", 0) or 0
        q4_row["current_liabilities"] = fy_row.get("current_liabilities", 0) or 0
        q4_row["non_current_liabilities"] = fy_row.get("non_current_liabilities", 0) or 0
        q4_row["cash"] = fy_row.get("cash", 0) or 0
        q4_row["inventories"] = fy_row.get("inventories", 0) or 0

        # Compute effective_tax_rate from Q4 tax_expense / profit_before_tax
        q4_tax = q4_row.get("tax_expense", 0) or 0
        q4_np = q4_row.get("net_profit", 0) or 0
        q4_fc = q4_row.get("finance_cost", 0) or 0
        # profit_before_tax = net_profit + |tax_expense|
        # (approximate: PBT = NP + tax, since tax_expense is negative in XLSX)
        q4_pbt = q4_np + abs(q4_tax)
        if q4_tax and q4_pbt and abs(q4_pbt) > 1:
            q4_eff_tax = max(0.0, min(abs(q4_tax) / abs(q4_pbt), 0.60))
        elif fy_row.get("effective_tax_rate"):
            q4_eff_tax = fy_row["effective_tax_rate"]
        else:
            q4_eff_tax = 20.0
        q4_row["effective_tax_rate"] = q4_eff_tax

        rows.append(q4_row)


def _add_q4_rows(rows: list[dict], annual_data: list[dict]):
    """
    For each fiscal year in annual_data, if we have Q1+Q2+Q3 in rows,
    compute Q4 = FY − (Q1+Q2+Q3) for flow items (revenue, expense, profit, cashflow).
    Balance sheet items use FY values directly (point-in-time at year-end).
    """
    # Group existing quarterly rows by year
    by_year: dict[int, dict[str, dict]] = {}
    for r in rows:
        y = r.get("year", 0)
        q = r.get("quarter", "")
        if q in ("Q1", "Q2", "Q3"):
            by_year.setdefault(y, {})[q] = r

    flow_fields = [
        "total_revenue", "sales", "other_revenue", "total_expense",
        "gross_profit",
        "finance_cost", "tax_expense",
        "ebit", "ebitda", "net_profit", "core_profit",
        "cf_operating", "cf_investing", "cf_financing", "cf_net",
    ]

    for fy in annual_data:
        fy_year = fy.get("year", 0)
        fy_q = fy.get("quarter", "")
        if fy_q not in ("Q9", "YE", "Q4"):
            continue

        year_qs = by_year.get(fy_year, {})
        if len(year_qs) < 3:
            continue  # need all of Q1, Q2, Q3

        if not all(q in year_qs for q in ("Q1", "Q2", "Q3")):
            continue

        # Check if Q4 already exists
        if any(r["period"] == f"Q4/{fy_year}" for r in rows):
            continue

        q4_row = {"period": f"Q4/{fy_year}", "quarter": "Q4", "year": fy_year, "source": "FY-(Q1+Q2+Q3)"}

        for field in flow_fields:
            fy_val = fy.get({
                "total_revenue": "totalRevenue",
                "sales": "sales",
                "total_expense": "totalExpense",
                "ebit": "ebit",
                "ebitda": "ebitda",
                "net_profit": "netProfit",
                "core_profit": "netProfit",
                "other_revenue": "totalRevenue",
                "cf_operating": "netOperating",
                "cf_investing": "netInvesting",
                "cf_financing": "netFinancing",
                "cf_net": "netCashflow",
            }.get(field, field), 0) or 0

            # Special handling for other_revenue and core_profit
            if field == "other_revenue":
                fy_val = (fy.get("totalRevenue", 0) or 0) - (fy.get("sales", 0) or 0)
            elif field == "core_profit":
                fy_val = (fy.get("netProfit", 0) or 0) - (fy.get("profitFromOtherActivity", 0) or 0)

            sum_q123 = sum(year_qs[q].get(field, 0) for q in ("Q1", "Q2", "Q3"))
            q4_row[field] = fy_val - sum_q123

        # Balance sheet: use FY values (point-in-time) from API
        q4_row["total_assets"] = (fy.get("totalAsset", 0) or 0)
        q4_row["total_liabilities"] = (fy.get("totalLiability", 0) or 0)
        q4_row["equity"] = (fy.get("equity", 0) or 0)
        q4_row["current_assets"] = 0
        q4_row["non_current_assets"] = 0
        q4_row["current_liabilities"] = 0
        q4_row["non_current_liabilities"] = 0
        q4_row["cash"] = 0
        q4_row["inventories"] = 0

        # EPS: approximate Q4 EPS (not precise but useful)
        fy_eps = fy.get("eps", 0) or 0
        sum_eps_q123 = sum(year_qs[q].get("eps", 0) for q in ("Q1", "Q2", "Q3"))
        q4_row["eps"] = fy_eps - sum_eps_q123
        q4_row["profit_from_other_activity"] = 0

        rows.append(q4_row)


# ============================================================
# DataFrame converters for Streamlit display
# ============================================================

def income_statement_to_df(data: list[dict]) -> pd.DataFrame:
    rows = {
        "รายได้จากการขาย/บริการ (Sales)": "sales",
        "รายได้อื่น (Other Revenue)": "other_revenue",
        "รายได้รวม (Total Revenue)": "total_revenue",
        "ค่าใช้จ่ายรวม (Total Expense)": "total_expense",
        "ต้นทุนทางการเงิน (Finance Cost)": "finance_cost",
        "ค่าใช้จ่ายภาษีเงินได้ (Tax Expense)": "tax_expense",
        "กำไรจากการดำเนินงาน (EBIT)": "ebit",
        "EBITDA": "ebitda",
        "กำไรสุทธิ (Net Profit)": "net_profit",
        "กำไร/ขาดทุนจากรายการอื่น (Other Activity)": "profit_from_other_activity",
        "กำไรจากธุรกิจหลัก (Core Profit)": "core_profit",
        "กำไรต่อหุ้น (EPS)": "eps",
    }
    return _build_df(data, rows, "period")


def balance_sheet_to_df(data: list[dict]) -> pd.DataFrame:
    """
    สร้าง DataFrame งบแสดงฐานะการเงินแบบ hierarchy พร้อม indent
    แสดงหมวดหมุนเวียน / ไม่หมุนเวียน ถ้าข้อมูลมี (quarterly XLSX)
    ถ้าไม่มี (factsheet annual) แสดงแค่ยอดรวม
    """
    periods = [d["period"] for d in data]

    # ตรวจว่า data มี current/non_current หรือไม่
    has_detail = any(
        (d.get("current_assets") or 0) != 0 or (d.get("non_current_assets") or 0) != 0
        for d in data
    )

    def val(d: dict, key: str) -> float:
        v = d.get(key, 0)
        return v if v else 0.0

    if has_detail:
        row_defs = [
            # (label, key, is_header)
            ("สินทรัพย์ (Assets)", None, True),
            ("  สินทรัพย์หมุนเวียน (Current Assets)", "current_assets", False),
            ("    เงินสดและรายการเทียบเท่า (Cash & Equivalents)", "cash", False),
            ("    เงินลงทุนระยะสั้น (Short-term Investments)", "short_term_investments", False),
            ("    ลูกหนี้การค้า (Trade Receivables)", "trade_receivables", False),
            ("    สินค้าคงเหลือ (Inventories)", "inventories", False),
            ("  สินทรัพย์ไม่หมุนเวียน (Non-Current Assets)", "non_current_assets", False),
            ("รวมสินทรัพย์ (Total Assets)", "total_assets", False),
            ("หนี้สิน (Liabilities)", None, True),
            ("  หนี้สินหมุนเวียน (Current Liabilities)", "current_liabilities", False),
            ("  หนี้สินไม่หมุนเวียน (Non-Current Liabilities)", "non_current_liabilities", False),
            ("รวมหนี้สิน (Total Liabilities)", "total_liabilities", False),
            ("ส่วนของผู้ถือหุ้น (Equity)", "equity", False),
        ]
    else:
        row_defs = [
            ("สินทรัพย์รวม (Total Assets)", "total_assets", False),
            ("หนี้สินรวม (Total Liabilities)", "total_liabilities", False),
            ("ส่วนของผู้ถือหุ้น (Equity)", "equity", False),
        ]

    index_labels = [label for label, _, _ in row_defs]
    rows_data: dict[str, list] = {label: [] for label, _, _ in row_defs}

    for d in data:
        for label, key, is_header in row_defs:
            if is_header or key is None:
                rows_data[label].append(None)
            else:
                rows_data[label].append(val(d, key))

    df = pd.DataFrame(rows_data, index=periods).T  # type: ignore[arg-type]
    df.index.name = None
    # ซ่อน sub-rows (cash, inventories) ถ้าทุกงวดเป็น 0 หรือ None
    optional_keys = {
        "    เงินสดและรายการเทียบเท่า (Cash & Equivalents)",
        "    เงินลงทุนระยะสั้น (Short-term Investments)",
        "    ลูกหนี้การค้า (Trade Receivables)",
        "    สินค้าคงเหลือ (Inventories)",
    }
    rows_to_drop = [
        lbl for lbl in df.index
        if lbl in optional_keys and df.loc[lbl].apply(lambda x: (x or 0) == 0).all()
    ]
    if rows_to_drop:
        df = df.drop(index=rows_to_drop)
    return df


def ratios_to_df(data: list[dict]) -> pd.DataFrame:
    rows = {
        "อัตรากำไรขั้นต้น (Gross Margin %)": "gross_margin_pct",
        "EBIT Margin %": "ebit_margin_pct",
        "EBITDA Margin %": "ebitda_margin_pct",
        "อัตรากำไรสุทธิ (Net Margin %)": "net_margin_pct",
        "Core Profit Margin %": "core_margin_pct",
        "ROE %": "roe_pct",
        "ROA %": "roa_pct",
        "D/E Ratio": "de_ratio",
        "Current Ratio": "current_ratio",
        "Quick Ratio": "quick_ratio",
        "Effective Tax Rate %": "effective_tax_rate_pct",
    }
    return _build_df(data, rows, "period")


def core_profit_to_df(data: list[dict]) -> pd.DataFrame:
    rows = {
        "กำไรสุทธิที่รายงาน (Reported NI)": "reported_net_income",
        "กำไรจากธุรกิจหลัก (Core Profit)": "core_profit",
        "กำไร/ขาดทุนจากรายการอื่น (Extraordinary)": "extraordinary_items",
        "Core % ของ Reported NI": "core_pct_of_reported",
    }
    return _build_df(data, rows, "period")


def cashflow_to_df(data: list[dict]) -> pd.DataFrame:
    rows = {
        "กระแสเงินสดจากดำเนินงาน (Operating)": "operating",
        "กระแสเงินสดจากลงทุน (Investing)": "investing",
        "กระแสเงินสดจากจัดหาเงิน (Financing)": "financing",
        "กระแสเงินสดสุทธิ (Net)": "net_cashflow",
    }
    return _build_df(data, rows, "period")


def xlsx_detail_to_df(xlsx_data: dict, sheet_key: str) -> pd.DataFrame | None:
    sheet = xlsx_data.get(sheet_key, {})
    rows_data = sheet.get("rows", [])
    if not rows_data:
        return None

    labels = [r["label"] for r in rows_data]
    current = [r.get("consolidated_current", 0) for r in rows_data]
    prev = [r.get("consolidated_prev", 0) for r in rows_data]

    headers = sheet.get("headers", ["Current", "Previous"])
    col_current = headers[0] if len(headers) > 0 else "Current"
    col_prev = headers[1] if len(headers) > 1 else "Previous"

    return pd.DataFrame({col_current: current, col_prev: prev}, index=labels)


# Human-readable labels for CF special item field names
_SPECIAL_ITEM_LABELS: dict[str, str] = {
    "gain_on_disposal_assets":    "กำไรจากจำหน่ายสินทรัพย์",
    "gain_on_fv_investments":     "กำไร FV เงินลงทุน (FVTPL)",
    "gain_on_fv_inv_property":    "กำไร FV อสังหาริมทรัพย์เพื่อการลงทุน",
    "gain_on_sale_investments":   "กำไรจากการขายเงินลงทุน",
    "share_of_profit_associates": "ส่วนแบ่งกำไรจากร่วมค้า/บ.ร่วม",
    "interest_income_cf":         "รายได้ดอกเบี้ย",
    "dividend_received":          "เงินปันผลรับ",
    "unrealised_fx":              "กำไร/(ขาดทุน) FX ที่ยังไม่เกิดขึ้น",
    "gain_on_derivatives":        "กำไรจากตราสารอนุพันธ์",
}


def special_items_breakdown_to_df(
    raw_quarterly_xlsx: list[dict],
    periods_order: list[str],
    view_mode: str = "quarterly",
) -> dict | None:
    """
    Build a breakdown of special (non-operating) items.

    Returns a dict:
      {
        "item_df":   pd.DataFrame  — numeric, rows=item labels, cols=periods
                                     (item rows ONLY, no subtotals)
        "tax_rates": {period: float} — effective tax rate per period [0..1]
        "col_order": [period, ...]   — periods that have special item data
      }

    Values are in thousands THB.
    Sign convention (IS perspective):
      +  = this item INCREASED reported NI (gain recognised in IS)
      −  = this item DECREASED reported NI (loss recognised in IS)

    Returns None if no special items exist for any period.
    """
    if not raw_quarterly_xlsx:
        return None

    # Build map: period_label → special_items dict (values in thousands)
    period_si: dict[str, dict[str, float]] = {}
    period_tax: dict[str, float] = {}

    for qd in raw_quarterly_xlsx:
        q = qd.get("quarter", "")
        y = qd.get("year", 0)
        if isinstance(y, str):
            y = int(y) if y.isdigit() else 0

        # Determine label — match the format used in periods_order
        if q in ("YE", "Q9"):
            label = f"FY{y}"
        elif q == "Q4":
            label = f"Q4/{y}"
        else:
            label = f"{q}/{y}"

        # Include FY labels even if not in periods_order (needed for Q4 computation)
        is_fy = q in ("YE", "Q9")
        if label not in periods_order and not is_fy:
            continue

        si = qd.get("special_items_from_cf", {})
        if not si:
            continue

        unit = qd.get("unit", "baht")
        divisor = 1 if unit == "thousands" else (0.001 if unit == "millions" else 1000)

        converted: dict[str, float] = {}
        for field, val_dict in si.items():
            if not isinstance(val_dict, dict):
                continue
            cf_val = (val_dict.get("current", 0) or 0) / divisor
            # Negate: CF shows gains as negative (reversed out); we want IS sign
            converted[field] = -cf_val

        if converted:
            period_si[label] = converted

        # effective tax rate for this period
        inc = qd.get("income", {})
        pbt_item = inc.get("profit_before_tax", {})
        np_item = inc.get("ni_owners", {}) or inc.get("net_profit", {})
        pbt = (pbt_item.get("current", 0) or 0) / divisor
        np_ = (np_item.get("current", 0) or 0) / divisor
        if pbt and abs(pbt) > 1:
            period_tax[label] = max(0.0, min((pbt - np_) / pbt, 0.5))
        else:
            period_tax[label] = 0.20

    # Compute Q4 special items: FY - Q1 - Q2 - Q3 (for each year that has all 4)
    # Collect all years with Q9/YE
    years_fy = set()
    for qd in raw_quarterly_xlsx:
        if qd.get("quarter") in ("Q9", "YE"):
            y = qd.get("year", 0)
            if isinstance(y, str):
                y = int(y) if y.isdigit() else 0
            years_fy.add(y)

    for y in years_fy:
        q4_label = f"Q4/{y}"
        if q4_label not in periods_order:
            continue
        if q4_label in period_si:
            continue  # already have it

        fy_label = f"FY{y}"
        q1_label = f"Q1/{y}"
        q2_label = f"Q2/{y}"
        q3_label = f"Q3/{y}"

        fy_si = period_si.get(fy_label, {})
        q1_si = period_si.get(q1_label, {})
        q2_si = period_si.get(q2_label, {})
        q3_si = period_si.get(q3_label, {})

        if not fy_si:
            continue
        if not (q1_si or q2_si or q3_si):
            continue

        # Q4 = FY - Q1 - Q2 - Q3 for each field
        all_f = set(fy_si) | set(q1_si) | set(q2_si) | set(q3_si)
        q4_si: dict[str, float] = {}
        for field in all_f:
            q4_si[field] = (
                fy_si.get(field, 0.0)
                - q1_si.get(field, 0.0)
                - q2_si.get(field, 0.0)
                - q3_si.get(field, 0.0)
            )
        if q4_si:
            period_si[q4_label] = q4_si
            # Tax rate for Q4: use FY tax rate as approximation
            period_tax[q4_label] = period_tax.get(fy_label, 0.20)

    if not period_si:
        return None

    # Collect all field names that appear across all periods
    all_fields: list[str] = []
    for si in period_si.values():
        for f in si:
            if f not in all_fields:
                all_fields.append(f)

    if not all_fields:
        return None

    # Build item_df: rows = item labels, columns = periods (newest first)
    # FY labels are NOT included in col_order (only what's in periods_order)
    col_order = [p for p in periods_order if p in period_si]
    if not col_order:
        return None

    rows: dict[str, dict[str, float]] = {}
    for field in all_fields:
        label = _SPECIAL_ITEM_LABELS.get(field, field)
        row: dict[str, float] = {}
        for period in col_order:
            row[period] = period_si[period].get(field, 0.0)
        rows[label] = row

    item_df = pd.DataFrame(rows).T
    item_df = item_df[col_order]

    # tax_rates: only for periods in col_order
    tax_rates = {p: period_tax.get(p, 0.20) for p in col_order}

    return {
        "item_df": item_df,
        "tax_rates": tax_rates,
        "col_order": col_order,
    }


def _build_df(data: list[dict], row_map: dict, period_key: str) -> pd.DataFrame:
    df_data = {}
    for item in data:
        period = item[period_key]
        col = {}
        for label, key in row_map.items():
            col[label] = item.get(key, 0)
        df_data[period] = col
    return pd.DataFrame(df_data)
