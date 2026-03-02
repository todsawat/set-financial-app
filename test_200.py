"""
Accuracy Test — 200 บริษัท (parallel)
ใช้ logic เดียวกับ test_accuracy.py แต่รัน parallel ด้วย ThreadPoolExecutor
และ load symbols จาก /tmp/symbols_200.json
"""

import json
import sys
import re
import time
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, str(Path(__file__).parent))

from financial_data import get_financial_data

# ---------------------------------------------------------------------------
# Load symbols
# ---------------------------------------------------------------------------
with open("/tmp/symbols_200.json") as f:
    _ALL_SYMBOLS: list[str] = json.load(f)

# Only test symbols that already have a cache file (to avoid long downloads)
import tempfile
from pathlib import Path as _Path
_CACHE_DIR = _Path(tempfile.gettempdir()) / "set_financial_cache"
_cached = {f.stem.replace("_data", "") for f in _CACHE_DIR.glob("*_data.json")}
TEST_SYMBOLS: list[str] = [s for s in _ALL_SYMBOLS if s in _cached]
print(f"Cached symbols available: {len(TEST_SYMBOLS)} / {len(_ALL_SYMBOLS)}")

# ---------------------------------------------------------------------------
# Result tracking (thread-safe)
# ---------------------------------------------------------------------------
results: list[dict] = []
_lock = threading.Lock()

def log(symbol, test, status, detail=""):
    with _lock:
        results.append({"symbol": symbol, "test": test, "status": status, "detail": detail})
    if status == "FAIL":
        print(f"  ✗ FAIL [{symbol}] {test}: {detail}")

# ---------------------------------------------------------------------------
# Tolerance helpers
# ---------------------------------------------------------------------------
def close_enough(a, b, pct=1.0):
    if a is None or b is None:
        return False
    if a == 0 and b == 0:
        return True
    denom = max(abs(a), abs(b), 1)
    return abs(a - b) / denom * 100 <= pct

def sign_ok(a, b):
    """Both same sign (or both zero)."""
    if a == 0 or b == 0:
        return True
    return (a > 0) == (b > 0)

# ---------------------------------------------------------------------------
# Annual checks
# ---------------------------------------------------------------------------
def check_annual(symbol: str, data: dict, raw: dict):
    income   = data.get("income_statement", [])
    balance  = data.get("balance_sheet", [])
    ratios   = data.get("ratios", [])
    core     = data.get("core_profit_analysis", [])
    cashflow = data.get("cashflow", [])
    api_rows = raw.get("annual_data", [])

    # Map API rows by year
    api_by_year: dict[int, dict] = {}
    for r in api_rows:
        if r.get("quarter") in ("Q9", "YE", "Q4"):
            api_by_year[r["year"]] = r

    periods = [r["period"] for r in income]
    if len(periods) < 2:
        log(symbol, "annual_periods", "WARN", f"only {len(periods)} periods")
    else:
        log(symbol, "annual_periods", "PASS", f"{len(periods)} งวด")

    for inc_r in income:
        year = inc_r.get("year")
        period = inc_r.get("period", f"FY{year}")
        api = api_by_year.get(year)
        if not api:
            log(symbol, f"[{period}] api_match", "SKIP", "no api row")
            continue

        # --- Income Statement ---
        for field, api_key in [
            ("total_revenue", "totalRevenue"),
            ("sales",         "sales"),
            ("total_expense", "totalExpense"),
            ("ebit",          "ebit"),
            ("ebitda",        "ebitda"),
            ("net_profit",    "netProfit"),
        ]:
            app_val = inc_r.get(field, 0) or 0
            api_val = api.get(api_key, 0) or 0
            if api_val == 0:
                log(symbol, f"[{period}] {field}", "SKIP", "api=0")
                continue
            if close_enough(app_val, api_val):
                log(symbol, f"[{period}] {field}", "PASS")
            else:
                log(symbol, f"[{period}] {field}", "FAIL",
                    f"app={app_val:,.0f} api={api_val:,.0f}")

        # EPS sign
        app_eps = inc_r.get("eps", 0) or 0
        api_eps = api.get("eps", 0) or 0
        if api_eps and not sign_ok(app_eps, api_eps):
            log(symbol, f"[{period}] eps_sign", "FAIL",
                f"app={app_eps} api={api_eps}")

        # profit_from_other
        app_pfo = inc_r.get("profit_from_other_activity", 0) or 0
        api_pfo = api.get("profitFromOtherActivity", 0) or 0
        if api_pfo != 0:
            if close_enough(app_pfo, api_pfo):
                log(symbol, f"[{period}] profit_from_other", "PASS")
            else:
                log(symbol, f"[{period}] profit_from_other", "FAIL",
                    f"app={app_pfo:,.0f} api={api_pfo:,.0f}")

        # --- Balance Sheet ---
        bal_r = next((b for b in balance if b.get("year") == year), None)
        if bal_r:
            for field, api_key in [
                ("total_assets",      "totalAsset"),
                ("total_liabilities", "totalLiability"),
                ("equity",            "equity"),
            ]:
                app_val = bal_r.get(field, 0) or 0
                api_val = api.get(api_key, 0) or 0
                if api_val == 0:
                    continue
                if close_enough(app_val, api_val):
                    log(symbol, f"[{period}] {field}", "PASS")
                else:
                    log(symbol, f"[{period}] {field}", "FAIL",
                        f"app={app_val:,.0f} api={api_val:,.0f}")

            # A = L + E
            ta = bal_r.get("total_assets", 0) or 0
            tl = bal_r.get("total_liabilities", 0) or 0
            eq = bal_r.get("equity", 0) or 0
            if ta and close_enough(ta, tl + eq, pct=2.0):
                log(symbol, f"[{period}] bs_A=L+E", "PASS")
            elif ta:
                log(symbol, f"[{period}] bs_A=L+E", "WARN",
                    f"A={ta:,.0f} L+E={tl+eq:,.0f}")

        # --- Ratios ---
        rat_r = next((r for r in ratios if r.get("year") == year), None)
        if rat_r:
            for field, api_key in [
                ("net_margin_pct", "netProfitMargin"),
                ("roe_pct",        "roe"),
                ("roa_pct",        "roa"),
                ("de_ratio",       "deRatio"),
                ("current_ratio",  "currentRatio"),
                ("quick_ratio",    "quickRatio"),
            ]:
                app_val = rat_r.get(field, 0) or 0
                api_val = api.get(api_key, 0) or 0
                if api_val == 0:
                    continue
                if close_enough(app_val, api_val, pct=2.0):
                    log(symbol, f"[{period}] {field}", "PASS")
                else:
                    log(symbol, f"[{period}] {field}", "FAIL",
                        f"app={app_val:.2f} api={api_val:.2f}")

        # --- Core Profit formula ---
        core_r = next((c for c in core if c.get("year") == year), None)
        if core_r and api_pfo != 0:
            np_ = inc_r.get("net_profit", 0) or 0
            tax_rate = (core_r.get("effective_tax_rate", 20.0) or 20.0) / 100
            expected_core = np_ - api_pfo * (1 - tax_rate)
            app_core = core_r.get("core_profit", 0) or 0
            if close_enough(app_core, expected_core, pct=3.0):
                log(symbol, f"[{period}] core_profit_formula", "PASS")
            else:
                log(symbol, f"[{period}] core_profit_formula", "FAIL",
                    f"app={app_core:,.0f} expected={expected_core:,.0f}")

        # --- Cash Flow ---
        cf_r = next((c for c in cashflow if c.get("year") == year), None)
        if cf_r:
            for field, api_key in [
                ("operating",    "netOperating"),
                ("investing",    "netInvesting"),
                ("financing",    "netFinancing"),
                ("net_cashflow", "netCashflow"),
            ]:
                app_val = cf_r.get(field, 0) or 0
                api_val = api.get(api_key, 0) or 0
                if api_val == 0:
                    continue
                if close_enough(app_val, api_val):
                    log(symbol, f"[{period}] cf_{field}", "PASS")
                else:
                    log(symbol, f"[{period}] cf_{field}", "FAIL",
                        f"app={app_val:,.0f} api={api_val:,.0f}")

# ---------------------------------------------------------------------------
# Quarterly checks
# ---------------------------------------------------------------------------
def check_quarterly(symbol: str, data: dict, raw: dict):
    """
    quarterly checks — compare app output against itself for internal consistency,
    and do unit-sanity checks. XLSX raw values are already baked into app output
    by get_financial_data(), so we check consistency rather than re-parsing xlsx.
    """
    income   = data.get("income_statement", [])
    balance  = data.get("balance_sheet", [])
    core     = data.get("core_profit_analysis", [])
    cashflow = data.get("cashflow", [])

    if not income:
        log(symbol, "quarterly_xlsx", "SKIP", "no income data")
        return

    non_q4 = [r for r in income if r.get("quarter") not in ("Q4", "")]

    for inc_r in non_q4:
        year    = inc_r.get("year")
        quarter = inc_r.get("quarter")
        period  = inc_r.get("period", f"{quarter}/{year}")

        # Unit sanity
        rev = inc_r.get("total_revenue", 0) or 0
        if rev > 0:
            if 1_000 <= rev <= 50_000_000_000:
                log(symbol, f"[{period}] unit_sanity", "PASS")
            else:
                log(symbol, f"[{period}] unit_sanity", "WARN",
                    f"revenue={rev:,.0f} อาจผิดหน่วย")

        # Balance Sheet A = L + E
        bal_r = next((b for b in balance
                      if b.get("year") == year and b.get("quarter") == quarter), None)
        if bal_r:
            ta = bal_r.get("total_assets", 0) or 0
            tl = bal_r.get("total_liabilities", 0) or 0
            eq = bal_r.get("equity", 0) or 0
            if ta and close_enough(ta, tl + eq, pct=2.0):
                log(symbol, f"[{period}] bs_A=L+E", "PASS")
            elif ta:
                log(symbol, f"[{period}] bs_A=L+E", "WARN",
                    f"A={ta:,.0f} L+E={tl+eq:,.0f}")

        # Core: core + extra ≈ NI
        core_r = next((c for c in core
                       if c.get("year") == year and c.get("period") == period), None)
        if core_r:
            np_  = core_r.get("reported_net_income", 0) or 0
            cp   = core_r.get("core_profit", 0) or 0
            ext  = core_r.get("extraordinary_items", 0) or 0
            if np_ and close_enough(cp + ext, np_, pct=1.0):
                log(symbol, f"[{period}] core_plus_extra_eq_NI", "PASS")
            elif np_:
                log(symbol, f"[{period}] core_plus_extra_eq_NI", "FAIL",
                    f"core({cp:,.0f})+extra({ext:,.0f})={cp+ext:,.0f} NI={np_:,.0f}")

        # CF operating sign sanity (operating CF should usually be positive)
        cf_r = next((c for c in cashflow
                     if c.get("period") == period), None)
        if cf_r:
            op_cf = cf_r.get("operating", 0) or 0
            np_   = inc_r.get("net_profit", 0) or 0
            # If company is profitable, operating CF should not be massively negative
            if np_ > 0 and op_cf < -abs(np_) * 5:
                log(symbol, f"[{period}] cf_operating_sanity", "WARN",
                    f"op_cf={op_cf:,.0f} vs NP={np_:,.0f} — looks unusual")
            else:
                log(symbol, f"[{period}] cf_operating_sanity", "PASS")

# ---------------------------------------------------------------------------
# Cross-mode checks
# ---------------------------------------------------------------------------
def check_cross_mode(symbol: str, annual: dict, quarterly: dict):
    annual_income    = annual.get("income_statement", [])
    quarterly_income = quarterly.get("income_statement", [])

    for ann_r in annual_income:
        year   = ann_r.get("year")
        period = ann_r.get("period", f"FY{year}")
        ann_rev = ann_r.get("total_revenue", 0) or 0
        ann_np  = ann_r.get("net_profit", 0) or 0
        if ann_rev == 0:
            continue

        # Sum Q1+Q2+Q3+Q4 quarterly for same year
        q_rows = [r for r in quarterly_income
                  if r.get("year") == year and r.get("quarter") in ("Q1","Q2","Q3","Q4")]
        if len(q_rows) != 4:
            continue

        sum_rev = sum(r.get("total_revenue", 0) or 0 for r in q_rows)
        sum_np  = sum(r.get("net_profit", 0) or 0 for r in q_rows)

        if close_enough(sum_rev, ann_rev, pct=2.0):
            log(symbol, f"[{period}] cross_revenue", "PASS")
        else:
            log(symbol, f"[{period}] cross_revenue", "FAIL",
                f"sum_q={sum_rev:,.0f} api={ann_rev:,.0f}")

        if close_enough(sum_np, ann_np, pct=3.0):
            log(symbol, f"[{period}] cross_net_profit", "PASS")
        else:
            log(symbol, f"[{period}] cross_net_profit", "WARN",
                f"sum_q={sum_np:,.0f} api={ann_np:,.0f}")

        # Cash flow cross-check
        ann_cf   = annual.get("cashflow", [])
        q_cf     = quarterly.get("cashflow", [])
        ann_cf_r = next((c for c in ann_cf if c.get("year") == year), None)
        if ann_cf_r:
            q_cf_rows = [c for c in q_cf
                         if c.get("year") == year and
                            c.get("period", "").startswith("Q")]
            if len(q_cf_rows) == 4:
                for field in ("operating", "investing", "financing", "net_cashflow"):
                    ann_val = ann_cf_r.get(field, 0) or 0
                    sum_q   = sum(r.get(field, 0) or 0 for r in q_cf_rows)
                    if ann_val == 0:
                        continue
                    if close_enough(sum_q, ann_val, pct=2.0):
                        log(symbol, f"[{period}] cross_cf_{field}", "PASS")
                    else:
                        log(symbol, f"[{period}] cross_cf_{field}", "WARN",
                            f"sum_q={sum_q:,.0f} api={ann_val:,.0f}")

# ---------------------------------------------------------------------------
# Per-symbol runner
# ---------------------------------------------------------------------------
def test_symbol(symbol: str):
    try:
        annual    = get_financial_data(symbol, view_mode="annual")
        quarterly = get_financial_data(symbol, view_mode="quarterly")

        from set_scraper import CACHE_DIR
        cache_file = CACHE_DIR / f"{symbol}_data.json"
        raw: dict = {}
        if cache_file.exists():
            with open(cache_file) as f:
                raw = json.load(f)

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

    except Exception as e:
        import traceback
        log(symbol, "exception", "FAIL", str(e))
        traceback.print_exc()

# ---------------------------------------------------------------------------
# Main — parallel execution
# ---------------------------------------------------------------------------
def main():
    print("SET Financial Accuracy Test — 200 บริษัท (parallel)")
    print("=" * 60)
    print(f"Testing {len(TEST_SYMBOLS)} companies with 8 workers...")
    print()

    completed = 0
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(test_symbol, sym): sym for sym in TEST_SYMBOLS}
        for future in as_completed(futures):
            sym = futures[future]
            completed += 1
            try:
                future.result()
            except Exception as e:
                log(sym, "exception", "FAIL", str(e))
            if completed % 20 == 0:
                with _lock:
                    n_fail = sum(1 for r in results if r["status"] == "FAIL")
                print(f"  [{completed}/{len(TEST_SYMBOLS)}] FAIL so far: {n_fail}")

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

    out_path = Path(__file__).parent / "test_200_results.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\nDetailed results → {out_path}")

    return by_status.get("FAIL", 0)


if __name__ == "__main__":
    n_fail = main()
    sys.exit(1 if n_fail > 0 else 0)
