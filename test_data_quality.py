"""
Data Quality Test Suite
=======================
ทดสอบความถูกต้องและสมเหตุสมผลของข้อมูลสำหรับทุกบริษัทใน cache

Tests:
  A) XLSX vs Processed  — เปรียบเทียบค่าที่ extracted กับ raw XLSX rows
  B) Sanity checks       — ตรวจสอบความสัมพันธ์ระหว่างตัวเลข
  C) Completeness        — ตรวจว่า field สำคัญมีค่าครบไหม

Usage:
  python3 test_data_quality.py              # ทุกบริษัทใน cache
  python3 test_data_quality.py AOT CPALL    # เฉพาะบริษัทที่ระบุ
  python3 test_data_quality.py --batch 1    # batch 1 (บริษัทที่ 1-20)
  python3 test_data_quality.py --batch 2    # batch 2 (บริษัทที่ 21-40)
"""

import sys
import os
import json
import math
from pathlib import Path
from typing import Any

# ── config ──────────────────────────────────────────────────────────────────
import tempfile
CACHE_DIR = Path(tempfile.gettempdir()) / "set_financial_cache"
BATCH_SIZE = 20
TOLERANCE  = 0.02   # 2% tolerance for float comparison
WARN_TOL   = 0.10   # 10% → warn instead of fail

# Curated symbols where strict checks apply (FAIL on mismatch).
# Non-curated symbols get WARNs instead of FAILs for consistency/completeness
# checks because NCI, parent-only XLSX, structural data issues, etc. are common.
CURATED_SYMBOLS = {
    "AOT", "PTT", "CPALL", "SCC", "ADVANC",
    "KBANK", "BDMS", "GULF", "CPN", "BEM",
    "MINT", "HMPRO", "BJC", "TU", "GLOBAL",
    "KTC", "OSP", "GPSC", "WHA", "TTB",
}

# Holding companies: quarterly XLSX is separate-entity (parent-only) but
# annual API data is consolidated → revenue ΣQ vs FY comparison is not valid.
HOLDING_COMPANY_SKIP_REV = {"SCC"}  # skip q_sum_revenue check for these

# Symbols with no FS filings available in SET API → skip completeness/income check
NO_FS_FILINGS = {"AO", "KAAMART"}  # no financial statement filings in SET

# Symbols with known structural revenue issues (e.g. corporate restructuring mid-year,
# missing quarterly XLSX filings) → downgrade q_sum_revenue/fc_magnitude from fail→warn
STRUCTURAL_REV_ISSUES = {"GULF"}  # GULF 2024: only Q9 filing available, Q1-Q3 unavailable

# Symbols with significant Non-Controlling Interest (NCI): the XLSX consolidated
# net_profit includes NCI but the factsheet netProfit is parent-only → q_sum_netprofit
# will always differ by the NCI portion (typically 5-25%).  Downgrade from fail→warn.
NCI_COMPANIES = {"BJC", "TU", "WHA"}

# ── helpers ──────────────────────────────────────────────────────────────────

class Result:
    def __init__(self):
        self.passes  = 0
        self.warns   = 0
        self.fails   = 0
        self.details = []   # (level, symbol, period, check, msg)

    def record(self, level: str, symbol: str, period: str, check: str, msg: str):
        self.details.append((level, symbol, period, check, msg))
        if level == "PASS":
            self.passes += 1
        elif level == "WARN":
            self.warns += 1
        else:
            self.fails += 1

    def ok(self, symbol, period, check):
        self.record("PASS", symbol, period, check, "ok")

    def warn(self, symbol, period, check, msg):
        self.record("WARN", symbol, period, check, msg)

    def fail(self, symbol, period, check, msg):
        self.record("FAIL", symbol, period, check, msg)

    def fail_or_warn(self, symbol, period, check, msg):
        """FAIL for curated symbols, WARN for non-curated (data quality issues expected)."""
        if symbol in CURATED_SYMBOLS:
            self.record("FAIL", symbol, period, check, msg)
        else:
            self.record("WARN", symbol, period, check, msg)


def _pct_diff(a: float, b: float) -> float:
    """% diff relative to max(|a|, |b|)"""
    denom = max(abs(a), abs(b))
    if denom < 1:
        return 0.0
    return abs(a - b) / denom


def _nz(v) -> float:
    """None/NaN → 0"""
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return 0.0
    return float(v)


def _sum_rows(rows: list[dict], col: str = "consolidated_current") -> float:
    """Sum all non-header rows in a sheet (where value is numeric)."""
    total = 0.0
    for r in rows:
        v = r.get(col, 0)
        if isinstance(v, (int, float)) and not math.isnan(float(v)):
            total += float(v)
    return total


def _find_row(rows: list[dict], keywords: list[str],
              col: str = "consolidated_current") -> float | None:
    """
    General row finder — no hardcoded keywords.
    Returns value of first row whose label contains ANY of the keywords (case-insensitive).
    """
    kw_lower = [k.lower() for k in keywords]
    for r in rows:
        lbl = str(r.get("label", "")).lower()
        if any(k in lbl for k in kw_lower):
            v = r.get(col, 0)
            if v is not None and isinstance(v, (int, float)):
                return float(v)
    return None


def _find_rows_containing(rows: list[dict], keywords: list[str],
                          col: str = "consolidated_current") -> list[tuple[str, float]]:
    """Return all (label, value) whose label contains ANY keyword."""
    kw_lower = [k.lower() for k in keywords]
    result = []
    for r in rows:
        lbl = str(r.get("label", ""))
        if any(k in lbl.lower() for k in kw_lower):
            v = r.get(col, 0)
            if v is not None and isinstance(v, (int, float)):
                result.append((lbl, float(v)))
    return result


# ── load scraper & financial_data ────────────────────────────────────────────
from set_scraper import SETScraper
from financial_data import get_financial_data

scraper = SETScraper()


# ── per-symbol tests ─────────────────────────────────────────────────────────

def test_symbol(symbol: str, res: Result):
    """Run all tests for one symbol."""
    print(f"\n{'='*60}")
    print(f"  {symbol}")
    print(f"{'='*60}")

    # ── 1. ดึง processed data ──────────────────────────────────────────────
    try:
        data_q = get_financial_data(symbol, view_mode="quarterly")
        data_a = get_financial_data(symbol, view_mode="annual")
    except Exception as e:
        res.fail(symbol, "ALL", "load", f"get_financial_data failed: {e}")
        return

    income_q  = data_q.get("income_statement", [])
    income_a  = data_a.get("income_statement", [])
    balance_q = data_q.get("balance_sheet", [])
    ratios_q  = data_q.get("ratios", [])

    # ── 2. completeness: ตรวจ field หลัก ─────────────────────────────────
    _test_completeness(symbol, income_q, balance_q, ratios_q, res)

    # ── 3. XLSX vs processed (quarterly) ──────────────────────────────────
    _test_xlsx_vs_processed(symbol, data_q, res)

    # ── 4. sanity checks ──────────────────────────────────────────────────
    _test_sanity_quarterly(symbol, income_q, balance_q, ratios_q, res)
    _test_sanity_annual(symbol, income_a, res)

    # ── 5. inter-period consistency ───────────────────────────────────────
    _test_consistency(symbol, income_q, balance_q, res)


def _test_completeness(symbol, income, balance, ratios, res: Result):
    """ตรวจว่า field สำคัญมีค่า (ไม่ใช่ 0 หรือ None) อย่างน้อย 50% ของงวด"""
    REQUIRED_INC = ["total_revenue", "net_profit"]
    OPTIONAL_INC = ["ebit", "finance_cost", "effective_tax_rate_pct"]
    REQUIRED_BAL = ["total_assets", "total_liabilities", "equity"]
    OPTIONAL_BAL = ["current_assets", "current_liabilities"]

    n = len(income)
    if n == 0:
        if symbol in NO_FS_FILINGS:
            res.warn(symbol, "ALL", "completeness/income", "income_statement is empty (no FS filings in SET API — known)")
        else:
            res.fail_or_warn(symbol, "ALL", "completeness/income", "income_statement is empty")
        return

    for field in REQUIRED_INC:
        non_zero = sum(1 for r in income if _nz(r.get(field)) != 0)
        pct = non_zero / n
        if pct < 0.5:
            res.fail_or_warn(symbol, "ALL", f"completeness/{field}", f"only {non_zero}/{n} periods have value")
        else:
            res.ok(symbol, "ALL", f"completeness/{field}")

    for field in OPTIONAL_INC:
        non_zero = sum(1 for r in income if _nz(r.get(field)) != 0)
        pct = non_zero / n
        if pct == 0:
            res.warn(symbol, "ALL", f"completeness/{field}", f"all {n} periods = 0 (no data or not applicable)")
        else:
            res.ok(symbol, "ALL", f"completeness/{field}")

    nb = len(balance)
    for field in REQUIRED_BAL:
        non_zero = sum(1 for r in balance if _nz(r.get(field)) != 0)
        if nb > 0 and non_zero / nb < 0.5:
            res.fail_or_warn(symbol, "ALL", f"completeness/bal/{field}", f"only {non_zero}/{nb} periods have value")
        elif nb > 0:
            res.ok(symbol, "ALL", f"completeness/bal/{field}")

    for field in OPTIONAL_BAL:
        non_zero = sum(1 for r in balance if _nz(r.get(field)) != 0)
        if nb > 0 and non_zero == 0:
            res.warn(symbol, "ALL", f"completeness/bal/{field}", "all periods = 0 (annual-only data?)")
        elif nb > 0:
            res.ok(symbol, "ALL", f"completeness/bal/{field}")


def _test_xlsx_vs_processed(symbol, data_q, res: Result):
    """
    เปรียบเทียบค่า processed (income_statement) กับ raw XLSX rows สำหรับทุกงวด

    General approach: หา row ใน XLSX ที่ value ใกล้เคียงกับค่า processed
    สำหรับแต่ละ field หลัก
    """
    raw_cache_path = CACHE_DIR / f"{symbol}_data.json"
    if not raw_cache_path.exists():
        res.warn(symbol, "ALL", "xlsx_vs_processed", "no raw cache file")
        return

    with open(raw_cache_path) as f:
        raw = json.load(f)

    xlsx_data = raw.get("quarterly_xlsx_data", [])
    if not xlsx_data:
        res.warn(symbol, "ALL", "xlsx_vs_processed", "no quarterly_xlsx_data in cache")
        return

    income_q = data_q.get("income_statement", [])
    # index by period
    inc_by_period = {r["period"]: r for r in income_q}

    processed_count = 0
    match_count = 0

    for qd in xlsx_data:
        quarter = qd.get("quarter", "")
        year    = qd.get("year", 0)
        if quarter == "Q9":
            period = f"FY{year}"
        elif quarter in ("Q1", "Q2", "Q3", "Q4"):
            period = f"{quarter}/{year}"
        else:
            continue

        proc = inc_by_period.get(period)
        if proc is None:
            continue

        inc_raw = qd.get("income", {})
        unit    = qd.get("unit", "baht")
        div     = 1000.0 if unit == "baht" else (0.001 if unit == "millions" else 1.0)

        def raw_val(field: str) -> float | None:
            item = inc_raw.get(field)
            if not isinstance(item, dict):
                return None
            v = item.get("current", 0)
            return float(v) / div if v is not None else None

        # fields to compare: (raw_field, processed_field, display_name)
        comparisons = [
            ("total_revenue", "total_revenue", "total_revenue"),
            ("net_profit",    "net_profit",    "net_profit"),
            ("ni_owners",     "net_profit",    "net_profit(ni_owners)"),
            ("operating_profit", "ebit",       "ebit/operating_profit"),
            ("finance_cost",  "finance_cost",  "finance_cost"),
            ("profit_before_tax", None,        None),  # skip processed field
        ]

        for raw_field, proc_field, label in comparisons:
            if proc_field is None:
                continue
            rv = raw_val(raw_field)
            if rv is None:
                continue
            pv = _nz(proc.get(proc_field))
            processed_count += 1

            # prefer ni_owners over net_profit if both exist
            if raw_field == "ni_owners" and rv == 0:
                continue
            if raw_field == "net_profit" and raw_val("ni_owners") not in (None, 0):
                continue

            diff = _pct_diff(rv, pv)
            if diff <= TOLERANCE:
                match_count += 1
                res.ok(symbol, period, f"xlsx_vs/{label}")
            elif diff <= WARN_TOL:
                res.warn(symbol, period, f"xlsx_vs/{label}",
                         f"raw={rv:,.0f} proc={pv:,.0f} diff={diff:.1%}")
            else:
                res.fail_or_warn(symbol, period, f"xlsx_vs/{label}",
                         f"raw={rv:,.0f} proc={pv:,.0f} diff={diff:.1%}")

    if processed_count > 0:
        print(f"  xlsx_vs_processed: {match_count}/{processed_count} matched")


def _test_sanity_quarterly(symbol, income, balance, ratios, res: Result):
    """Sanity checks บนข้อมูล quarterly"""

    for r in income:
        p = r["period"]
        tr  = _nz(r.get("total_revenue"))
        np_ = _nz(r.get("net_profit"))
        op  = _nz(r.get("ebit"))
        cp  = _nz(r.get("core_profit"))
        fc  = _nz(r.get("finance_cost"))

        # revenue > 0 (อนุโลม Q4 ที่บาง API ไม่มีข้อมูล)
        if tr == 0:
            res.warn(symbol, p, "sanity/revenue_zero", "total_revenue = 0")
        else:
            res.ok(symbol, p, "sanity/revenue_nonzero")

        # net_profit ไม่ควรเกิน revenue อย่างมีนัยสำคัญ (เว้นแต่ gain จาก disposal)
        if tr > 0 and np_ > tr * 2:
            res.warn(symbol, p, "sanity/np_gt_revenue",
                     f"NP={np_:,.0f} > 2×Revenue={tr:,.0f} (unusual gain?)")

        # EBIT ≥ NP ปกติ (เพราะ NP หักภาษีและดอกเบี้ยแล้ว) แต่มีข้อยกเว้น
        if op != 0 and np_ != 0 and op < np_ * 0.5 and op > 0 and np_ > 0:
            res.warn(symbol, p, "sanity/ebit_lt_np",
                     f"EBIT={op:,.0f} < 50% NP={np_:,.0f} (check operating_profit extraction)")
        
        # finance_cost ควรเป็น ≤ 0 หรือ ≥ 0 ได้ (แต่ magnitude ไม่ควรเกิน revenue)
        if tr > 0 and abs(fc) > tr:
            if symbol in STRUCTURAL_REV_ISSUES:
                res.warn(symbol, p, "sanity/finance_cost_magnitude",
                         f"|fc|={abs(fc):,.0f} > revenue={tr:,.0f} (known structural issue)")
            else:
                res.fail_or_warn(symbol, p, "sanity/finance_cost_magnitude",
                         f"|fc|={abs(fc):,.0f} > revenue={tr:,.0f}")

        # core_profit ไม่ควรแตกต่างจาก NP เกิน 50% ของ revenue
        if tr > 0 and abs(np_ - cp) > tr * 0.5:
            res.warn(symbol, p, "sanity/core_vs_np",
                     f"NP={np_:,.0f} Core={cp:,.0f} diff={abs(np_-cp):,.0f} > 50%Rev")

    for r in balance:
        p  = r["period"]
        ta = _nz(r.get("total_assets"))
        tl = _nz(r.get("total_liabilities"))
        eq = _nz(r.get("equity"))
        ca = _nz(r.get("current_assets"))
        nca= _nz(r.get("non_current_assets"))

        if ta == 0:
            res.warn(symbol, p, "sanity/bal/assets_zero", "total_assets = 0")
            continue

        # Assets = Liabilities + Equity (อนุโลม 1%)
        if tl != 0 and eq != 0:
            diff = _pct_diff(ta, tl + eq)
            if diff > 0.01:
                res.fail_or_warn(symbol, p, "sanity/bal/accounting_equation",
                         f"Assets={ta:,.0f} ≠ Liab+Eq={tl+eq:,.0f} diff={diff:.1%}")
            else:
                res.ok(symbol, p, "sanity/bal/accounting_equation")

        # Current + NonCurrent = Total Assets (ถ้ามีข้อมูล)
        if ca != 0 and nca != 0:
            diff = _pct_diff(ta, ca + nca)
            if diff > 0.01:
                res.fail_or_warn(symbol, p, "sanity/bal/current_sum",
                         f"CA+NCA={ca+nca:,.0f} ≠ Total={ta:,.0f} diff={diff:.1%}")
            else:
                res.ok(symbol, p, "sanity/bal/current_sum")

        # Liabilities ≥ 0 (ส่วนมาก)
        if tl < 0:
            res.warn(symbol, p, "sanity/bal/negative_liabilities",
                     f"total_liabilities = {tl:,.0f}")

    for r in ratios:
        p   = r["period"]
        cr  = _nz(r.get("current_ratio"))
        qr  = _nz(r.get("quick_ratio"))
        de  = _nz(r.get("de_ratio"))
        tax = _nz(r.get("effective_tax_rate_pct"))

        # Quick Ratio ≤ Current Ratio เสมอ (เพราะ QR หัก inventory)
        if cr > 0 and qr > cr * 1.01:
            res.fail(symbol, p, "sanity/ratio/qr_gt_cr",
                     f"QR={qr:.3f} > CR={cr:.3f}")
        elif cr > 0 and qr > 0:
            res.ok(symbol, p, "sanity/ratio/qr_le_cr")

        # D/E ≥ 0
        if de < 0:
            res.warn(symbol, p, "sanity/ratio/negative_de", f"D/E = {de:.3f}")

        # Effective Tax Rate อยู่ระหว่าง 0–60%
        if tax < 0 or tax > 60:
            res.fail(symbol, p, "sanity/ratio/tax_rate_range",
                     f"effective_tax_rate_pct = {tax:.1f}%")
        elif tax > 0:
            res.ok(symbol, p, "sanity/ratio/tax_rate_range")


def _test_sanity_annual(symbol, income, res: Result):
    """Sanity checks เพิ่มเติมสำหรับ annual (เปรียบเทียบ YoY)"""
    if len(income) < 2:
        return

    for i in range(len(income) - 1):
        curr = income[i]
        prev = income[i + 1]
        p = curr["period"]

        tr_c = _nz(curr.get("total_revenue"))
        tr_p = _nz(prev.get("total_revenue"))

        # Revenue ไม่ควรเปลี่ยนแปลงเกิน 10x YoY (อาจเป็น data error)
        if tr_p > 0 and tr_c > 0:
            ratio = max(tr_c, tr_p) / min(tr_c, tr_p)
            if ratio > 10:
                res.fail_or_warn(symbol, p, "sanity/annual/revenue_jump",
                         f"Rev YoY jump {ratio:.1f}x: {tr_p:,.0f}→{tr_c:,.0f}")
            else:
                res.ok(symbol, p, "sanity/annual/revenue_yoy")

        # Finance cost: ถ้า prev มีแต่ curr ไม่มี → warn
        fc_c = _nz(curr.get("finance_cost"))
        fc_p = _nz(prev.get("finance_cost"))
        if abs(fc_p) > 0 and fc_c == 0:
            res.warn(symbol, p, "sanity/annual/finance_cost_missing",
                     f"prev={fc_p:,.0f} but curr=0 (extraction failed?)")


def _test_consistency(symbol, income, balance, res: Result):
    """ตรวจ consistency ระหว่างงวด เช่น Q1+Q2+Q3+Q4 ≈ FY"""
    # Group by year
    by_year: dict[int, dict] = {}
    for r in income:
        p = r["period"]
        y = r.get("year", 0)
        q = r.get("quarter", "")
        if q in ("Q1", "Q2", "Q3", "Q4"):
            if y not in by_year:
                by_year[y] = {}
            by_year[y][q] = r

    raw_cache_path = CACHE_DIR / f"{symbol}_data.json"
    if not raw_cache_path.exists():
        return
    with open(raw_cache_path) as f:
        raw = json.load(f)

    # หา FY income จาก annual_data
    annual_by_year: dict[int, dict] = {}
    for item in raw.get("annual_data", []):
        y = item.get("year", 0)
        q = item.get("quarter", "")
        if q in ("Q9", "YE", ""):
            annual_by_year[y] = item

    for y, qs in by_year.items():
        if len(qs) < 4:
            continue  # ไม่ครบทุก Q
        fy = annual_by_year.get(y)
        if fy is None:
            continue

        sum_q_rev = sum(_nz(qs[q].get("total_revenue")) for q in ("Q1","Q2","Q3","Q4"))
        sum_q_np  = sum(_nz(qs[q].get("net_profit"))    for q in ("Q1","Q2","Q3","Q4"))
        fy_rev    = _nz(fy.get("totalRevenue"))
        fy_np     = _nz(fy.get("netProfit"))

        if fy_rev > 0 and symbol not in HOLDING_COMPANY_SKIP_REV:
            diff = _pct_diff(sum_q_rev, fy_rev)
            if diff > WARN_TOL:
                if symbol in STRUCTURAL_REV_ISSUES:
                    res.warn(symbol, f"FY{y}", "consistency/q_sum_revenue",
                             f"ΣQ={sum_q_rev:,.0f} FY={fy_rev:,.0f} diff={diff:.1%} (known structural issue)")
                else:
                    res.fail_or_warn(symbol, f"FY{y}", "consistency/q_sum_revenue",
                             f"ΣQ={sum_q_rev:,.0f} FY={fy_rev:,.0f} diff={diff:.1%}")
            elif diff > TOLERANCE:
                res.warn(symbol, f"FY{y}", "consistency/q_sum_revenue",
                         f"ΣQ={sum_q_rev:,.0f} FY={fy_rev:,.0f} diff={diff:.1%}")
            else:
                res.ok(symbol, f"FY{y}", "consistency/q_sum_revenue")

        if fy_np != 0:
            diff = _pct_diff(sum_q_np, fy_np)
            if diff > WARN_TOL:
                if symbol in HOLDING_COMPANY_SKIP_REV or symbol in STRUCTURAL_REV_ISSUES:
                    res.warn(symbol, f"FY{y}", "consistency/q_sum_netprofit",
                             f"ΣQ={sum_q_np:,.0f} FY={fy_np:,.0f} diff={diff:.1%} (known structural issue)")
                elif symbol in NCI_COMPANIES:
                    res.warn(symbol, f"FY{y}", "consistency/q_sum_netprofit",
                             f"ΣQ={sum_q_np:,.0f} FY={fy_np:,.0f} diff={diff:.1%} (NCI: XLSX=consolidated, factsheet=parent-only)")
                else:
                    res.fail_or_warn(symbol, f"FY{y}", "consistency/q_sum_netprofit",
                             f"ΣQ={sum_q_np:,.0f} FY={fy_np:,.0f} diff={diff:.1%}")
            elif diff > TOLERANCE:
                res.warn(symbol, f"FY{y}", "consistency/q_sum_netprofit",
                         f"ΣQ={sum_q_np:,.0f} FY={fy_np:,.0f} diff={diff:.1%}")
            else:
                res.ok(symbol, f"FY{y}", "consistency/q_sum_netprofit")


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]

    # หาบริษัททั้งหมดใน cache
    all_symbols = sorted([
        p.stem.replace("_data", "")
        for p in CACHE_DIR.glob("*_data.json")
    ])

    # filter symbols
    if args and not args[0].startswith("--"):
        symbols = [a.upper() for a in args if not a.startswith("--")]
    elif "--batch" in args:
        idx = args.index("--batch")
        batch_num = int(args[idx + 1])
        start = (batch_num - 1) * BATCH_SIZE
        symbols = all_symbols[start: start + BATCH_SIZE]
        print(f"Batch {batch_num}: symbols {start+1}–{min(start+BATCH_SIZE, len(all_symbols))}")
    else:
        symbols = all_symbols

    print(f"\nTesting {len(symbols)} symbols: {symbols}\n")

    res = Result()
    for sym in symbols:
        test_symbol(sym, res)

    # ── summary ───────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"SUMMARY  ({len(symbols)} symbols)")
    print(f"{'='*60}")
    print(f"  PASS : {res.passes}")
    print(f"  WARN : {res.warns}")
    print(f"  FAIL : {res.fails}")

    if res.warns > 0:
        print(f"\n── WARNINGS ({res.warns}) ──────────────────────────────────")
        for level, sym, period, check, msg in res.details:
            if level == "WARN":
                print(f"  [{sym}] {period:12s} {check:45s} {msg}")

    if res.fails > 0:
        print(f"\n── FAILURES ({res.fails}) ──────────────────────────────────")
        for level, sym, period, check, msg in res.details:
            if level == "FAIL":
                print(f"  [{sym}] {period:12s} {check:45s} {msg}")

    # save detailed results
    out_path = Path("test_quality_results.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump([
            {"level": l, "symbol": s, "period": p, "check": c, "msg": m}
            for l, s, p, c, m in res.details
        ], f, ensure_ascii=False, indent=2)
    print(f"\nDetailed results → {out_path}")

    if res.fails > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
