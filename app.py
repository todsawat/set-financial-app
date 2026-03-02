"""
SET Financial Statement Analyzer
=================================
Streamlit app for downloading and analyzing Thai listed company
financial statements from SET.or.th

Features:
- Downloads real financial data via SET.or.th internal APIs
- Extracts FINANCIAL_STATEMENTS.XLSX from news section ZIP files
- Multi-year comparison (newest first, at least 3 years)
- Core profit vs extraordinary items separation
- Interactive Plotly charts
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


from version import __version__
from financial_data import (
    get_financial_data,
    income_statement_to_df,
    balance_sheet_to_df,
    ratios_to_df,
    core_profit_to_df,
    cashflow_to_df,
    special_items_breakdown_to_df,
    _SPECIAL_ITEM_LABELS,
)

# ============================================================
# Page Config
# ============================================================
st.set_page_config(
    page_title="SET Financial Analyzer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={},
)

# ============================================================
# Custom CSS
# ============================================================
st.markdown("""
<style>
    /* ลด gap ระหว่าง metric QoQ/YoY */
    div[data-testid="stMetric"] { margin-bottom: -1.2rem !important; }
    .main-header {
        font-size: 2rem; font-weight: 700; color: #4a6fa5;
        margin-bottom: 0.3rem;
    }
    .sub-header {
        font-size: 0.95rem; color: #666; margin-bottom: 1.5rem;
    }
    .section-divider {
        border-top: 3px solid #667eea; margin: 1.5rem 0;
    }
    .data-source-tag {
        display: inline-block; background: #e8f5e9; color: #2e7d32;
        padding: 2px 10px; border-radius: 12px; font-size: 0.75rem;
    }
    div[data-testid="stMetricValue"] { font-size: 1.3rem; }
    /* ปุ่มตัวอย่าง symbol ใน sidebar ให้ font เล็กลง */
    section[data-testid="stSidebar"] button[kind="secondary"] p {
        font-size: 0.75rem !important;
    }
</style>
""", unsafe_allow_html=True)


# ============================================================
# Number formatting
# ============================================================
def fmt(val, decimals=0, is_ratio=False):
    """Format number for display. Values are in thousands THB from the API."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "-"
    if val == 0:
        return "-"
    if is_ratio:
        return f"{val:,.2f}"
    if decimals > 0:
        return f"{val:,.{decimals}f}"
    return f"{val:,.0f}"


def fmt_df(df: pd.DataFrame, is_ratio: bool = False) -> pd.DataFrame:
    """Format all values in a DataFrame for display."""
    formatted = df.copy()
    for col in formatted.columns:
        formatted[col] = formatted[col].apply(
            lambda x: fmt(x, is_ratio=is_ratio) if isinstance(x, (int, float)) else str(x) if x else "-"
        )
    return formatted


def color_negative_red(df_numeric: pd.DataFrame, is_ratio: bool = False):
    """
    Return a Styler with negative numbers in red.
    df_numeric must contain raw numeric values (not yet formatted).
    Usage: color_negative_red(df).apply(hl_row, axis=1) etc.
    """
    fmt_func = (lambda x: fmt(x, is_ratio=is_ratio)) if not is_ratio else (lambda x: fmt(x, is_ratio=True))

    def _cell_style(val):
        if isinstance(val, (int, float)) and not pd.isna(val) and val < 0:
            return "color: #c62828; font-weight: 600;"
        return ""

    styled = df_numeric.style.format(
        lambda x: fmt(x, is_ratio=is_ratio) if isinstance(x, (int, float)) else (str(x) if x else "-")
    ).applymap(_cell_style)  # type: ignore[attr-defined]
    return styled


# ============================================================
# Chart helpers
# ============================================================
def _add_year_dividers(fig: "go.Figure", periods: list[str]) -> None:
    """
    Add a thin vertical dashed line between Q4 and Q1 of the next year.
    `periods` must be in the same order as the chart x-axis (oldest → newest).
    Works with categorical x-axes by using the numeric index position.
    Also rotates x-axis period labels to 90 degrees for readability.
    """
    for i in range(1, len(periods)):
        prev, curr = periods[i - 1], periods[i]
        # Detect a year boundary: previous period is Q4/YYYY, current is Q1/YYYY+1
        if prev.startswith("Q4/") and curr.startswith("Q1/"):
            # On a categorical axis Plotly maps category index i to x = i-0.5
            # The gap between index i-1 and i is at x = i - 0.5
            fig.add_vline(
                x=i - 0.5,
                line=dict(color="rgba(120,120,120,0.45)", width=1.5, dash="dot"),
                layer="below",
            )
    # Rotate x-axis period labels to vertical for readability
    # Add border frame around the plot area
    _axis_border = dict(showline=True, linewidth=1, linecolor="black", mirror=True)
    fig.update_layout(
        xaxis_tickangle=-90,
        xaxis=_axis_border,
        yaxis=_axis_border,
    )
    # Also apply to secondary y-axis if present (e.g. dual-axis charts)
    if hasattr(fig, "layout") and getattr(fig.layout, "yaxis2", None) is not None:
        fig.update_layout(yaxis2=dict(showline=True, linewidth=1, linecolor="black", mirror=True))


# ============================================================
# Chart builders
# ============================================================
def chart_revenue_profit(data: list[dict]) -> go.Figure:
    periods = [d["period"] for d in data][::-1]
    revenue = [d["total_revenue"] for d in data][::-1]
    net = [d["net_profit"] for d in data][::-1]
    core = [d["core_profit"] for d in data][::-1]
    ebitda = [d["ebitda"] for d in data][::-1]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Total Revenue", x=periods, y=revenue,
        marker_color="rgba(102,126,234,0.5)",
        text=[f"{v:,.0f}" for v in revenue], textposition="outside",
    ))
    fig.add_trace(go.Scatter(
        name="EBITDA", x=periods, y=ebitda,
        mode="lines+markers", line=dict(color="#ff9800", width=2.5), marker=dict(size=8),
    ))
    fig.add_trace(go.Scatter(
        name="Net Profit", x=periods, y=net,
        mode="lines+markers", line=dict(color="#2196f3", width=2.5), marker=dict(size=8),
    ))
    fig.add_trace(go.Scatter(
        name="Core Profit", x=periods, y=core,
        mode="lines+markers", line=dict(color="#e91e63", width=2.5, dash="dash"),
        marker=dict(size=8, symbol="diamond"),
    ))
    fig.update_layout(
        title="Revenue & Profitability Trend (พันบาท)",
        xaxis_title="Period", yaxis_title="Thousands THB",
        legend=dict(orientation="h", y=1.12, x=0.5, xanchor="center"),
        height=480, template="plotly_white", bargap=0.3,
    )
    _add_year_dividers(fig, periods)
    return fig


def chart_core_vs_reported(data: list[dict]) -> go.Figure:
    periods = [d["period"] for d in data][::-1]
    reported = [d["reported_net_income"] for d in data][::-1]
    core = [d["core_profit"] for d in data][::-1]
    extra = [d["extraordinary_items"] for d in data][::-1]

    # ถ้า extraordinary ทุกงวด = 0 (quarterly mode) → แสดงแค่ bar เดียวพร้อมข้อความแจ้ง
    all_zero_extra = all(v == 0 for v in extra)

    fig = go.Figure()

    # Bar: Reported Net Income (พื้นหลัง — สีเทา)
    fig.add_trace(go.Bar(
        name="Reported NI (กำไรสุทธิรวม)", x=periods, y=reported,
        marker_color="rgba(200,200,200,0.5)",
        marker_line=dict(color="#888", width=1),
        text=[f"{v:,.0f}" for v in reported],
        textposition="outside",
        textfont=dict(size=10, color="#555"),
    ))

    # Bar: Core Profit (ทับบน reported)
    fig.add_trace(go.Bar(
        name="Core Profit (กำไรธุรกิจหลัก)", x=periods, y=core,
        marker_color="#1565c0",
        text=[f"{v:,.0f}" for v in core],
        textposition="inside",
        textfont=dict(size=10, color="white"),
    ))

    if not all_zero_extra:
        # เพิ่ม scatter แยกต่างหากเพื่อแสดงค่า extraordinary อย่างชัดเจน
        colors_marker = ["#2e7d32" if v >= 0 else "#c62828" for v in extra]
        fig.add_trace(go.Scatter(
            name="Extraordinary (รายการพิเศษ)", x=periods, y=extra,
            mode="markers+text",
            marker=dict(size=12, color=colors_marker, symbol="diamond"),
            text=[f"{v:+,.0f}" for v in extra],
            textposition="top center",
            textfont=dict(size=9),
        ))

    fig.update_layout(
        title="Core Profit vs Reported Net Income (พันบาท)<br>"
              "<sub>แท่งสีเทา = Reported NI | แท่งน้ำเงิน = Core Profit | เพชร = รายการพิเศษ</sub>",
        barmode="overlay",
        xaxis_title="Period", yaxis_title="Thousands THB",
        legend=dict(orientation="h", y=1.15, x=0.5, xanchor="center"),
        height=500, template="plotly_white",
        bargap=0.25,
    )
    _add_year_dividers(fig, periods)
    return fig


def chart_margins(ratios: list[dict]) -> go.Figure:
    periods = [d["period"] for d in ratios][::-1]
    gpm = [d["gross_margin_pct"] for d in ratios][::-1]
    net_m = [d["net_margin_pct"] for d in ratios][::-1]
    core_m = [d["core_margin_pct"] for d in ratios][::-1]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        name="Gross Margin", x=periods, y=gpm,
        mode="lines+markers+text", line=dict(color="#ff9800", width=2.5),
        marker=dict(size=8), text=[f"{v:.1f}%" for v in gpm], textposition="top center",
    ))
    fig.add_trace(go.Scatter(
        name="Net Margin", x=periods, y=net_m,
        mode="lines+markers+text", line=dict(color="#2196f3", width=2.5),
        marker=dict(size=8), text=[f"{v:.1f}%" for v in net_m], textposition="bottom center",
    ))
    fig.add_trace(go.Scatter(
        name="Core Margin", x=periods, y=core_m,
        mode="lines+markers+text", line=dict(color="#e91e63", width=2.5, dash="dash"),
        marker=dict(size=8, symbol="diamond"),
        text=[f"{v:.1f}%" for v in core_m], textposition="bottom center",
    ))
    fig.update_layout(
        title="Profit Margin Trends (%)",
        xaxis_title="Period", yaxis_title="%",
        legend=dict(orientation="h", y=1.12, x=0.5, xanchor="center"),
        height=420, template="plotly_white",
    )
    _add_year_dividers(fig, periods)
    return fig


def chart_roe_roa(ratios: list[dict]) -> go.Figure:
    periods = [d["period"] for d in ratios][::-1]
    roe = [d["roe_pct"] for d in ratios][::-1]
    roa = [d["roa_pct"] for d in ratios][::-1]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        name="ROE", x=periods, y=roe, mode="lines+markers+text",
        line=dict(color="#2196f3", width=3), marker=dict(size=10),
        text=[f"{v:.1f}%" for v in roe], textposition="top center",
    ))
    fig.add_trace(go.Scatter(
        name="ROA", x=periods, y=roa, mode="lines+markers+text",
        line=dict(color="#4caf50", width=3), marker=dict(size=10),
        text=[f"{v:.1f}%" for v in roa], textposition="bottom center",
    ))
    fig.update_layout(
        title="ROE & ROA (%)",
        yaxis_title="%",
        height=400, template="plotly_white",
        legend=dict(orientation="h", y=1.1, x=0.5, xanchor="center"),
    )
    _add_year_dividers(fig, periods)
    return fig


def chart_de_ratio(ratios: list[dict]) -> go.Figure:
    periods = [d["period"] for d in ratios][::-1]
    de = [d["de_ratio"] for d in ratios][::-1]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="D/E", x=periods, y=de, marker_color="#ff9800",
        text=[f"{v:.3f}" for v in de], textposition="auto",
    ))
    fig.update_layout(
        title="D/E Ratio",
        yaxis_title="เท่า",
        height=400, template="plotly_white",
        legend=dict(orientation="h", y=1.1, x=0.5, xanchor="center"),
    )
    _add_year_dividers(fig, periods)
    return fig


def chart_roe_roa_de(ratios: list[dict]) -> go.Figure:
    """Legacy: kept for backward compatibility — returns ROE/ROA only."""
    return chart_roe_roa(ratios)


def chart_tax_rate(ratios: list[dict], income: list[dict] | None = None) -> go.Figure:
    """กราฟค่าใช้จ่ายภาษี (แท่ง) + Effective Tax Rate % (เส้น)"""
    periods = [d["period"] for d in ratios][::-1]
    tax_rate = [d.get("effective_tax_rate_pct", 0) for d in ratios][::-1]

    # สร้าง tax_expense จาก income data (ถ้ามี)
    tax_expense: list[float] = []
    if income:
        inc_map = {d["period"]: d for d in income}
        tax_expense = [abs(inc_map.get(p, {}).get("tax_expense", 0) or 0) for p in periods]

    has_tax_expense = any(v > 0 for v in tax_expense)

    if has_tax_expense:
        fig = make_subplots(specs=[[{"secondary_y": True}]])

        fig.add_trace(go.Bar(
            name="ค่าใช้จ่ายภาษี (Tax Expense)",
            x=periods, y=tax_expense,
            marker_color="#ce93d8",
            text=[f"{v:,.0f}" for v in tax_expense],
            textposition="auto",
        ), secondary_y=False)

        fig.add_trace(go.Scatter(
            name="Effective Tax Rate",
            x=periods, y=tax_rate,
            mode="lines+markers+text",
            line=dict(color="#7b1fa2", width=2.5),
            marker=dict(size=8),
            text=[f"{v:.1f}%" for v in tax_rate],
            textposition="top center",
            textfont=dict(size=8),
        ), secondary_y=True)

        fig.add_hline(
            y=20, line_dash="dash", line_color="#aaa",
            annotation_text="20% (ภาษีนิติบุคคล)", annotation_position="bottom right",
            secondary_y=True,
        )
        fig.update_layout(
            title="ค่าใช้จ่ายภาษีเงินได้ & Effective Tax Rate",
            height=420, template="plotly_white",
            legend=dict(orientation="h", y=1.1, x=0.5, xanchor="center"),
            yaxis=dict(title="พันบาท"),
            yaxis2=dict(title="%", range=[0, max(max(tax_rate) * 1.3, 35) if tax_rate else 35]),
        )
    else:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            name="Effective Tax Rate",
            x=periods, y=tax_rate,
            mode="lines+markers+text",
            line=dict(color="#7b1fa2", width=2.5),
            marker=dict(size=8),
            text=[f"{v:.1f}%" for v in tax_rate],
            textposition="top center",
            textfont=dict(size=8),
        ))
        fig.add_hline(
            y=20, line_dash="dash", line_color="#aaa",
            annotation_text="20% (ภาษีนิติบุคคล)", annotation_position="bottom right",
        )
        fig.update_layout(
            title="Effective Tax Rate %",
            yaxis_title="%",
            yaxis=dict(range=[0, max(max(tax_rate) * 1.3, 35) if tax_rate else 35]),
            height=400, template="plotly_white",
        )

    _add_year_dividers(fig, periods)
    return fig


def _growth_series(values: list, step: int) -> list:
    """คำนวณ growth% เทียบ `step` งวดก่อนหน้า — None ถ้าคำนวณไม่ได้"""
    result = [None] * len(values)
    for i in range(step, len(values)):
        prev = values[i - step]
        curr = values[i]
        if prev is not None and prev != 0 and curr is not None:
            result[i] = (curr - prev) / abs(prev) * 100
    return result


def _growth_line(fig, name: str, periods: list, vals: list, color: str,
                 dash: str = "solid", symbol: str = "circle", row: int | None = None) -> None:
    """เพิ่ม Scatter line trace — row=None สำหรับ go.Figure() ธรรมดา, row=1/2 สำหรับ subplot"""
    import math
    texts = [f"{v:+.1f}%" if v is not None else "" for v in vals]
    y = [v if v is not None else math.nan for v in vals]
    trace = go.Scatter(
        name=name, x=periods, y=y,
        mode="lines+markers+text",
        line=dict(color=color, width=2.5, dash=dash),
        marker=dict(size=8, symbol=symbol),
        text=texts, textposition="top center", textfont=dict(size=8),
        connectgaps=False,
    )
    if row is not None:
        fig.add_trace(trace, row=row, col=1)
    else:
        fig.add_trace(trace)


def chart_growth_yoy(data: list[dict], view_mode: str = "quarterly") -> go.Figure:
    """กราฟเส้น YoY% สำหรับ Revenue, Net Profit, Core Profit (กราฟเดียว)"""
    periods = [d["period"] for d in data][::-1]
    revenue = [d["total_revenue"] for d in data][::-1]
    net     = [d["net_profit"]    for d in data][::-1]
    core    = [d["core_profit"]   for d in data][::-1]

    step = 4 if view_mode == "quarterly" else 1
    subtitle = "vs same quarter last year" if view_mode == "quarterly" else "vs previous year"

    rev_yoy  = _growth_series(revenue, step)
    net_yoy  = _growth_series(net,     step)
    core_yoy = _growth_series(core,    step)

    fig = go.Figure()
    _growth_line(fig, "Revenue YoY%",     periods, rev_yoy,  "#667eea")
    _growth_line(fig, "Net Profit YoY%",  periods, net_yoy,  "#2196f3")
    _growth_line(fig, "Core Profit YoY%", periods, core_yoy, "#e91e63", dash="dash", symbol="diamond")

    fig.add_hline(y=0, line=dict(color="rgba(0,0,0,0.25)", width=1))

    fig.update_layout(
        title=f"YoY Growth (%) — {subtitle}",
        height=420, template="plotly_white",
        xaxis_title="Period", yaxis_title="%",
        legend=dict(orientation="h", y=1.1, x=0.5, xanchor="center"),
    )

    _add_year_dividers(fig, periods)
    return fig


def chart_growth_qoq(data: list[dict]) -> go.Figure:
    """กราฟเส้น QoQ% สำหรับ Revenue, Net Profit, Core Profit (แยก 2 subplots)"""
    periods = [d["period"] for d in data][::-1]
    revenue = [d["total_revenue"] for d in data][::-1]
    net     = [d["net_profit"]    for d in data][::-1]
    core    = [d["core_profit"]   for d in data][::-1]

    rev_qoq  = _growth_series(revenue, 1)
    net_qoq  = _growth_series(net,     1)
    core_qoq = _growth_series(core,    1)

    fig = go.Figure()
    _growth_line(fig, "Revenue QoQ%",     periods, rev_qoq,  "#ff9800")
    _growth_line(fig, "Net Profit QoQ%",  periods, net_qoq,  "#2196f3")
    _growth_line(fig, "Core Profit QoQ%", periods, core_qoq, "#e91e63", dash="dash", symbol="diamond")

    fig.add_hline(y=0, line=dict(color="rgba(0,0,0,0.25)", width=1))

    fig.update_layout(
        title="QoQ Growth (%) — vs previous quarter",
        height=420, template="plotly_white",
        xaxis_title="Period", yaxis_title="%",
        legend=dict(orientation="h", y=1.1, x=0.5, xanchor="center"),
    )

    _add_year_dividers(fig, periods)
    return fig


def chart_balance_sheet(data: list[dict]) -> go.Figure:
    periods = [d["period"] for d in data][::-1]
    assets = [d["total_assets"] for d in data][::-1]
    liab = [d["total_liabilities"] for d in data][::-1]
    eq = [d["equity"] for d in data][::-1]

    fig = go.Figure()
    fig.add_trace(go.Bar(name="Equity", x=periods, y=eq, marker_color="#4caf50"))
    fig.add_trace(go.Bar(name="Liabilities", x=periods, y=liab, marker_color="#ff5722"))
    fig.add_trace(go.Scatter(
        name="Total Assets", x=periods, y=assets,
        mode="lines+markers", line=dict(color="#2196f3", width=3), marker=dict(size=10),
    ))
    fig.update_layout(
        title="Balance Sheet Composition (พันบาท)",
        barmode="stack", xaxis_title="Period", yaxis_title="Thousands THB",
        legend=dict(orientation="h", y=1.12, x=0.5, xanchor="center"),
        height=420, template="plotly_white",
    )
    _add_year_dividers(fig, periods)
    return fig


def chart_cash_and_inventories(data: list[dict]) -> go.Figure | None:
    """กราฟเงินสด, เงินลงทุนระยะสั้น, ลูกหนี้, สินค้าคงเหลือ (stacked bar)"""
    periods = [d["period"] for d in data][::-1]
    cash = [d.get("cash", 0) or 0 for d in data][::-1]
    st_inv = [d.get("short_term_investments", 0) or 0 for d in data][::-1]
    recv = [d.get("trade_receivables", 0) or 0 for d in data][::-1]
    inv = [d.get("inventories", 0) or 0 for d in data][::-1]

    # ถ้าข้อมูลทุกงวดเป็น 0 ทั้งหมด → ไม่แสดงกราฟ
    if not any(cash) and not any(st_inv) and not any(recv) and not any(inv):
        return None

    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="เงินสด (Cash)",
        x=periods, y=cash,
        marker_color="#26a69a",
        text=[f"{v:,.0f}" if v else "" for v in cash],
        textposition="auto",
    ))
    if any(st_inv):
        fig.add_trace(go.Bar(
            name="เงินลงทุนระยะสั้น (ST Inv.)",
            x=periods, y=st_inv,
            marker_color="#42a5f5",
            text=[f"{v:,.0f}" if v else "" for v in st_inv],
            textposition="auto",
        ))
    if any(recv):
        fig.add_trace(go.Bar(
            name="ลูกหนี้การค้า (Receivables)",
            x=periods, y=recv,
            marker_color="#ab47bc",
            text=[f"{v:,.0f}" if v else "" for v in recv],
            textposition="auto",
        ))
    fig.add_trace(go.Bar(
        name="สินค้าคงเหลือ (Inventories)",
        x=periods, y=inv,
        marker_color="#ff8a65",
        text=[f"{v:,.0f}" if v else "" for v in inv],
        textposition="auto",
    ))
    fig.update_layout(
        title="สินทรัพย์หมุนเวียนหลัก: เงินสด / เงินลงทุน / ลูกหนี้ / สินค้าคงเหลือ (พันบาท)",
        barmode="stack",
        xaxis_title="Period", yaxis_title="พันบาท",
        legend=dict(orientation="h", y=1.15, x=0.5, xanchor="center"),
        height=450, template="plotly_white",
    )
    _add_year_dividers(fig, periods)
    return fig


def chart_finance_cost(data: list[dict]) -> go.Figure:
    """กราฟต้นทุนทางการเงิน (Finance Cost) — แท่ง absolute + เส้น % ของรายได้"""
    import math
    periods  = [d["period"] for d in data][::-1]
    # finance_cost มักเป็นค่าลบใน XLSX (รายจ่าย) → เอา absolute
    fc_raw   = [d.get("finance_cost", 0) or 0 for d in data][::-1]
    fc       = [abs(v) for v in fc_raw]
    rev      = [d.get("total_revenue", 0) or 0 for d in data][::-1]
    fc_pct   = [round(f / r * 100, 2) if r else None for f, r in zip(fc, rev)]

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(go.Bar(
        name="ต้นทุนทางการเงิน (Finance Cost)",
        x=periods, y=fc,
        marker_color="#ef5350",
        text=[f"{v:,.0f}" for v in fc], textposition="auto",
    ), secondary_y=False)

    fig.add_trace(go.Scatter(
        name="% ของรายได้",
        x=periods,
        y=[v if v is not None else math.nan for v in fc_pct],
        mode="lines+markers+text",
        line=dict(color="#7b1fa2", width=2.5),
        marker=dict(size=8),
        text=[f"{v:.1f}%" if v is not None else "" for v in fc_pct],
        textposition="top center", textfont=dict(size=8),
        connectgaps=False,
    ), secondary_y=True)

    fig.update_layout(
        title="ต้นทุนทางการเงิน (Finance Cost)",
        height=420, template="plotly_white",
        legend=dict(orientation="h", y=1.1, x=0.5, xanchor="center"),
        yaxis=dict(title="พันบาท"),
        yaxis2=dict(title="% ของรายได้", ticksuffix="%"),
    )
    _add_year_dividers(fig, periods)
    return fig


def chart_cashflow(data: list[dict]) -> go.Figure:
    periods = [d["period"] for d in data][::-1]
    operating = [d["operating"] for d in data][::-1]
    investing = [d["investing"] for d in data][::-1]
    financing = [d["financing"] for d in data][::-1]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        name="Operating", x=periods, y=operating,
        mode="lines+markers", line=dict(color="#4caf50", width=2.5), marker=dict(size=8),
    ))
    fig.add_trace(go.Scatter(
        name="Investing", x=periods, y=investing,
        mode="lines+markers", line=dict(color="#f44336", width=2.5), marker=dict(size=8),
    ))
    fig.add_trace(go.Scatter(
        name="Financing", x=periods, y=financing,
        mode="lines+markers", line=dict(color="#ff9800", width=2.5), marker=dict(size=8),
    ))
    fig.add_hline(y=0, line=dict(color="rgba(0,0,0,0.2)", width=1))
    fig.update_layout(
        title="Cash Flow Breakdown (พันบาท)",
        xaxis_title="Period", yaxis_title="Thousands THB",
        legend=dict(orientation="h", y=1.12, x=0.5, xanchor="center"),
        height=420, template="plotly_white",
    )
    _add_year_dividers(fig, periods)
    return fig


# ============================================================
# MAIN APP
# ============================================================
def main():
    # ---- Sidebar ----
    def _set_symbol(sym: str):
        """Callback for example buttons — runs before next rerun renders widgets."""
        st.session_state["symbol_widget"] = sym

    with st.sidebar:
        st.markdown("### SET Financial Analyzer")
        st.markdown("ดาวน์โหลดงบการเงินจาก SET.or.th")
        st.markdown("---")

        # Initialise widget default for the text_input
        if "symbol_widget" not in st.session_state:
            st.session_state["symbol_widget"] = "AOT"

        symbol = st.text_input(
            "พิมพ์ Symbol หุ้น",
            key="symbol_widget",
            placeholder="เช่น AOT, PTT, CPALL, SCC",
            help="พิมพ์ Symbol แล้วกด Enter เพื่อดาวน์โหลดงบการเงิน",
        ).upper().strip()

        st.markdown("---")

        # ---- View Mode Toggle ----
        view_mode = st.radio(
            "มุมมองข้อมูล",
            options=["annual", "quarterly"],
            format_func=lambda x: "รายปี (Annual)" if x == "annual" else "รายไตรมาส (Quarterly)",
            index=1,
            help="เลือกแสดงข้อมูลรายปีหรือรายไตรมาส",
            horizontal=True,
        )

        st.markdown("---")
        st.markdown("**ตัวอย่าง Symbol:**")
        example_cols = st.columns(3)
        examples = ["AOT", "PTT", "CPALL", "SCC", "ADVANC", "BDMS", "KBANK", "GULF", "CPN"]
        for i, sym in enumerate(examples):
            col = example_cols[i % 3]
            col.button(sym, key=f"btn_{sym}", width="stretch",
                       on_click=_set_symbol, args=(sym,))

        # ---- Clear cache button ----
        st.markdown("---")
        _cur_sym = st.session_state.get("symbol_widget", "AOT").upper().strip()
        if st.button(f"ลบ Cache: {_cur_sym}", key="btn_clear_cache",
                     help="ลบข้อมูล cache ทั้งหมดของหุ้นตัวนี้ แล้วดาวน์โหลดใหม่",
                     type="secondary"):
            import shutil
            from set_scraper import CACHE_DIR as _CD
            _data_f = _CD / f"{_cur_sym}_data.json"
            _q_dir = _CD / f"{_cur_sym}_quarters"
            deleted = []
            if _data_f.exists():
                _data_f.unlink()
                deleted.append("data cache")
            if _q_dir.exists():
                shutil.rmtree(_q_dir)
                deleted.append("quarter cache")
            if deleted:
                st.toast(f"ลบ {', '.join(deleted)} ของ {_cur_sym} แล้ว — กำลังโหลดใหม่...")
            else:
                st.toast(f"ไม่พบ cache ของ {_cur_sym}")
            st.rerun()

        st.markdown("---")
        st.markdown(
            f"""
            <div style='font-size:0.75rem; color:#888;'>
            <b>แหล่งข้อมูล:</b> SET.or.th<br>
            <b>วิธีการ:</b> ดาวน์โหลด ZIP จาก News Section<br>
            <b>หน่วย:</b> พันบาท (Thousands THB)<br>
            <b>ข้อมูลใหม่สุดอยู่ทางซ้ายเสมอ</b><br>
            <b>ข้อมูลใหม่ล่าสุดถูกต้องที่สุด</b><br><br>
            <span style='color:#aaa;'>v{__version__}</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # ---- Main Content ----
    if not symbol:
        st.info("กรุณาพิมพ์ Symbol หุ้นที่ต้องการวิเคราะห์ในแถบด้านซ้าย")
        return

    # Fetch data with progress bar
    view_label = "รายไตรมาส" if view_mode == "quarterly" else "รายปี"
    progress_container = st.container()
    with progress_container:
        progress_bar = st.progress(0, text=f"กำลังเริ่มดาวน์โหลดงบการเงิน {symbol} ...")
        status_text = st.empty()

    def _on_progress(msg: str, current: int = 0, total: int = 0):
        pct = current / total if total > 0 else 0
        progress_bar.progress(min(pct, 1.0), text=f"{msg}  ({current}/{total})" if total > 0 else msg)
        if total > 0:
            status_text.caption(f"งบที่ {current} จาก {total} งบ")

    data = get_financial_data(symbol, view_mode=view_mode, progress_callback=_on_progress)
    progress_bar.empty()
    status_text.empty()

    if data.get("error"):
        st.error(f"ไม่สามารถดึงข้อมูลได้: {data['error']}")
        return

    company = data.get("company", {"name": symbol, "name_th": symbol, "sector": "N/A", "market": "SET"})
    income = data.get("income_statement", [])
    balance = data.get("balance_sheet", [])
    ratios = data.get("ratios", [])
    core = data.get("core_profit_analysis", [])
    cashflow = data.get("cashflow", [])

    if not income:
        st.warning(f"ไม่พบข้อมูลงบการเงินสำหรับ {symbol}")
        return

    # ---- Header ----
    mode_badge = (
        '<span style="background:#667eea;color:white;padding:2px 10px;border-radius:12px;font-size:0.75rem;">รายปี Annual</span>'
        if view_mode == "annual"
        else '<span style="background:#e91e63;color:white;padding:2px 10px;border-radius:12px;font-size:0.75rem;">รายไตรมาส Quarterly</span>'
    )
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown(
            f'<div class="main-header">{symbol} - {company.get("name", symbol)} {mode_badge}</div>',
            unsafe_allow_html=True,
        )
        sector = company.get("sector", "N/A")
        industry = company.get("industry", "")
        name_th = company.get("name_th", "")
        fy_end = company.get("fiscal_year_end", "")
        sub_parts = [name_th, f"Sector: {sector}"]
        if industry:
            sub_parts.append(f"Industry: {industry}")
        if fy_end:
            sub_parts.append(f"FY End: {fy_end}")
        sub_parts.append(f'Market: {company.get("market","SET")}')
        st.markdown(
            f'<div class="sub-header">{" | ".join(sub_parts)}'
            f' <span class="data-source-tag">Data: SET.or.th</span></div>',
            unsafe_allow_html=True,
        )
    with col2:
        n_periods = len(income)
        label_range = f"{income[0]['period']} - {income[-1]['period']}" if n_periods > 1 else income[0]["period"]
        period_type = "งวด" if view_mode == "annual" else "ไตรมาส"
        st.markdown(
            f"""<div style='text-align:right; padding:0.5rem; background:#f0f2f6; border-radius:8px;'>
            <div style='font-size:0.8rem; color:#666;'>ข้อมูล {n_periods} {period_type}</div>
            <div style='font-size:1.1rem; font-weight:700; color:#1a1a2e;'>{label_range}</div>
            </div>""",
            unsafe_allow_html=True,
        )



    # ── Load special items & recompute core_adj (needed for Quick Metrics) ──────
    from set_scraper import CACHE_DIR
    import json as _json
    _cache_file = CACHE_DIR / f"{symbol}_data.json"
    _raw_xlsx: list[dict] = []
    if _cache_file.exists():
        try:
            with open(_cache_file) as _f:
                _raw_xlsx = _json.load(_f).get("quarterly_xlsx_data", [])
        except Exception:
            pass

    _periods_list = [r["period"] for r in income]
    _si_result = special_items_breakdown_to_df(_raw_xlsx, _periods_list, view_mode)
    _all_item_names: list[str] = list(_si_result["item_df"].index) if _si_result else []

    # ดึง selection จาก session_state (ถ้า widget ยังไม่ถูก render จะใช้ default = all)
    _si_key = f"si_select_{symbol}_{view_mode}"
    _selected_items: list[str] = st.session_state.get(_si_key, _all_item_names)

    def _recompute_core_global(base_core, si_res, selected):
        if not si_res or not base_core:
            return base_core
        item_df = si_res["item_df"]
        tax_rates = si_res["tax_rates"]
        all_items = list(item_df.index)
        if set(selected) == set(all_items):
            return base_core
        adjusted = []
        for row in base_core:
            period = row["period"]
            ni = row.get("reported_net_income", 0) or 0
            if period not in item_df.columns:
                adjusted.append(row)
                continue
            pretax = sum(
                float(item_df.at[item, period])
                for item in selected if item in item_df.index
            )
            tax = tax_rates.get(period, 0.20)
            extra_aftertax = pretax * (1.0 - tax)
            new_core = ni - extra_aftertax
            adjusted.append({**row, "core_profit": new_core,
                              "extraordinary_items": extra_aftertax,
                              "extraordinary_items_pretax": pretax,
                              "core_pct_of_reported": round((new_core / ni * 100) if ni else 0, 1)})
        return adjusted

    _core_adj_global = _recompute_core_global(core, _si_result, _selected_items)

    # ---- ZIP download info ----
    if data.get("zip_url"):
        zi = data.get("zip_info", {})
        st.caption(
            f"Latest ZIP: {zi.get('quarter','')}/{zi.get('year','')} "
            f"({zi.get('fsType','')}, {zi.get('status','')}) "
            f"[Download ZIP]({data['zip_url']})"
        )

    # ---- Quick Metrics ----
    latest = income[0]
    latest_r = ratios[0] if ratios else {}
    latest_c = core[0] if core else {}

    def _pct(curr, prev_val):
        if prev_val and prev_val != 0:
            return (curr - prev_val) / abs(prev_val) * 100
        return None

    # QoQ: เทียบงวดก่อน (index 1)
    prev_q = income[1] if len(income) > 1 else None
    # YoY: เทียบ 4 งวดก่อน (quarterly) หรืองวดก่อน (annual)
    yoy_step = 4 if view_mode == "quarterly" else 1
    prev_y = income[yoy_step] if len(income) > yoy_step else None

    def _fmt_delta(val, label):
        if val is None:
            return f"{label}: N/A"
        return f"{val:+.1f}% {label}"

    rev_qoq = _pct(latest["total_revenue"], prev_q["total_revenue"] if prev_q else None)
    rev_yoy = _pct(latest["total_revenue"], prev_y["total_revenue"] if prev_y else None)
    ni_qoq  = _pct(latest["net_profit"],    prev_q["net_profit"]    if prev_q else None)
    ni_yoy  = _pct(latest["net_profit"],    prev_y["net_profit"]    if prev_y else None)
    cp_curr = _core_adj_global[0].get("core_profit", 0) if _core_adj_global else latest_c.get("core_profit", 0)
    cp_qoq  = _pct(cp_curr, _core_adj_global[1].get("core_profit") if len(_core_adj_global) > 1 else None)
    cp_yoy  = _pct(cp_curr, _core_adj_global[yoy_step].get("core_profit") if len(_core_adj_global) > yoy_step else None)

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric(f"Revenue ({latest['period']})", fmt(latest["total_revenue"]), _fmt_delta(rev_qoq, "QoQ"))
    m1.metric("", "", _fmt_delta(rev_yoy, "YoY"), label_visibility="collapsed")
    m2.metric("Net Profit", fmt(latest["net_profit"]), _fmt_delta(ni_qoq, "QoQ"))
    m2.metric("", "", _fmt_delta(ni_yoy, "YoY"), label_visibility="collapsed")
    m3.metric("Core Profit", fmt(cp_curr), _fmt_delta(cp_qoq, "QoQ"))
    m3.metric("", "", _fmt_delta(cp_yoy, "YoY"), label_visibility="collapsed")
    roe = latest_r.get('roe_pct', 0)
    roa = latest_r.get('roa_pct', 0)
    prev_r = ratios[yoy_step] if len(ratios) > yoy_step else None
    prev_rq = ratios[1] if len(ratios) > 1 else None
    roe_qoq = _pct(roe, prev_rq.get('roe_pct') if prev_rq else None)
    roe_yoy = _pct(roe, prev_r.get('roe_pct') if prev_r else None)

    de = latest_r.get('de_ratio', 0)
    de_qoq = _pct(de, prev_rq.get('de_ratio') if prev_rq else None)
    de_yoy = _pct(de, prev_r.get('de_ratio') if prev_r else None)

    m4.metric("ROE", f"{roe:.1f}%", _fmt_delta(roe_qoq, "QoQ"))
    m4.metric("", "", _fmt_delta(roe_yoy, "YoY"), label_visibility="collapsed")
    m5.metric("D/E", f"{de:.3f}", _fmt_delta(de_qoq, "QoQ"), delta_color="inverse")
    m5.metric("", "", _fmt_delta(de_yoy, "YoY"), label_visibility="collapsed", delta_color="inverse")

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ---- Tabs ----
    tab_names = ["Overview Charts", "Income Statement", "Balance Sheet",
                  "Financial Ratios", "Core Profit Analysis", "Cash Flow"]
    tabs = st.tabs(tab_names)

    # ==== TAB 0: Overview ====
    with tabs[0]:
        st.plotly_chart(chart_revenue_profit(income), width="stretch", key="overview_revenue")
        st.plotly_chart(chart_growth_yoy(income, view_mode), width="stretch", key="overview_growth_yoy")
        if view_mode == "quarterly":
            st.plotly_chart(chart_growth_qoq(income), width="stretch", key="overview_growth_qoq")
        st.plotly_chart(chart_margins(ratios), width="stretch", key="overview_margins")
        st.plotly_chart(chart_roe_roa(ratios), width="stretch", key="overview_roe_roa")
        st.plotly_chart(chart_de_ratio(ratios), width="stretch", key="overview_de")
        st.plotly_chart(chart_tax_rate(ratios, income), width="stretch", key="overview_tax")
        st.plotly_chart(chart_finance_cost(income), width="stretch", key="overview_finance_cost")
        st.plotly_chart(chart_core_vs_reported(core), width="stretch", key="overview_core")

    # ==== TAB 1: Income Statement ====
    with tabs[1]:
        st.markdown("### งบกำไรขาดทุน (Income Statement)")
        mode_note = "รายปี" if view_mode == "annual" else "รายไตรมาส"
        st.markdown(f"*หน่วย: พันบาท (Thousands THB) | {mode_note} | ข้อมูลใหม่สุดอยู่ทางซ้าย*")

        # Show XLSX download links per quarter (quarterly mode only)
        if view_mode == "quarterly":
            url_map = {i["period"]: i.get("download_url", "") for i in income if i.get("download_url")}
            if url_map:
                link_parts = [
                    f"[{period}]({url})" for period, url in url_map.items()
                ]
                st.markdown("**ดาวน์โหลด ZIP งบการเงิน:** " + " | ".join(link_parts))

        inc_df = income_statement_to_df(income)

        def hl_income(row):
            important = ["รายได้รวม (Total Revenue)", "กำไรจากการดำเนินงาน (EBIT)",
                         "กำไรสุทธิ (Net Profit)", "กำไรจากธุรกิจหลัก (Core Profit)"]
            if row.name in important:
                return ["background-color:#1565c0; color:#ffffff; font-weight:bold"] * len(row)
            if "รายการอื่น" in str(row.name) or "Other" in str(row.name):
                return ["background-color:#e65100; color:#ffffff; font-style:italic"] * len(row)
            return [""] * len(row)

        st.dataframe(color_negative_red(inc_df).apply(hl_income, axis=1), width="stretch", height=440)

        st.plotly_chart(chart_finance_cost(income), width="stretch", key="income_finance_cost")
        st.plotly_chart(chart_growth_yoy(income, view_mode), width="stretch", key="income_growth_yoy")
        if view_mode == "quarterly":
            st.plotly_chart(chart_growth_qoq(income), width="stretch", key="income_growth_qoq")

    # ==== TAB 2: Balance Sheet ====
    with tabs[2]:
        st.markdown("### งบแสดงฐานะการเงิน (Balance Sheet)")
        st.markdown(f"*หน่วย: พันบาท | {mode_note} | ข้อมูลใหม่สุดอยู่ทางซ้าย*")

        # Show XLSX download links per quarter (quarterly mode only)
        if view_mode == "quarterly":
            url_map = {i["period"]: i.get("download_url", "") for i in income if i.get("download_url")}
            if url_map:
                link_parts = [f"[{period}]({url})" for period, url in url_map.items()]
                st.markdown("**ดาวน์โหลด ZIP งบการเงิน:** " + " | ".join(link_parts))

        bal_df = balance_sheet_to_df(balance)

        # ใช้ non-breaking space (NBSP) แทน space ธรรมดา เพื่อ indent จริงใน dataframe
        _NB = "\u00a0"  # non-breaking space

        # remap index labels → indent ด้วย NBSP
        _INDENT_MAP = {
            # header sections
            "สินทรัพย์ (Assets)":   "สินทรัพย์ (Assets)",
            "หนี้สิน (Liabilities)": "หนี้สิน (Liabilities)",
            # totals (ไม่ indent)
            "รวมสินทรัพย์ (Total Assets)":               "รวมสินทรัพย์ (Total Assets)",
            "รวมหนี้สิน (Total Liabilities)":             "รวมหนี้สิน (Total Liabilities)",
            "ส่วนของผู้ถือหุ้น (Equity)":                 "ส่วนของผู้ถือหุ้น (Equity)",
            "สินทรัพย์รวม (Total Assets)":                "รวมสินทรัพย์ (Total Assets)",
            "หนี้สินรวม (Total Liabilities)":             "รวมหนี้สิน (Total Liabilities)",
            # current/non-current — indent 1 level (4 NBSP)
            "  สินทรัพย์หมุนเวียน (Current Assets)":         _NB*4 + "สินทรัพย์หมุนเวียน (Current Assets)",
            "  สินทรัพย์ไม่หมุนเวียน (Non-Current Assets)":  _NB*4 + "สินทรัพย์ไม่หมุนเวียน (Non-Current Assets)",
            "  หนี้สินหมุนเวียน (Current Liabilities)":      _NB*4 + "หนี้สินหมุนเวียน (Current Liabilities)",
            "  หนี้สินไม่หมุนเวียน (Non-Current Liabilities)":_NB*4 + "หนี้สินไม่หมุนเวียน (Non-Current Liabilities)",
            # sub-items — indent 2 levels (10 NBSP)
            "    เงินสดและรายการเทียบเท่า (Cash & Equivalents)": _NB*10 + "เงินสดและรายการเทียบเท่า",
            "    เงินลงทุนระยะสั้น (Short-term Investments)":     _NB*10 + "เงินลงทุนระยะสั้น",
            "    ลูกหนี้การค้า (Trade Receivables)":               _NB*10 + "ลูกหนี้การค้า",
            "    สินค้าคงเหลือ (Inventories)":                   _NB*10 + "สินค้าคงเหลือ (Inventories)",
        }

        bal_df.index = [_INDENT_MAP.get(lbl, lbl) for lbl in bal_df.index]

        _BAL_HEADERS = {
            "สินทรัพย์ (Assets)",
            "หนี้สิน (Liabilities)",
        }
        _BAL_TOTALS = {
            "รวมสินทรัพย์ (Total Assets)",
            "รวมหนี้สิน (Total Liabilities)",
            "ส่วนของผู้ถือหุ้น (Equity)",
        }
        _BAL_SUB = {
            _NB*4 + "สินทรัพย์หมุนเวียน (Current Assets)",
            _NB*4 + "สินทรัพย์ไม่หมุนเวียน (Non-Current Assets)",
            _NB*4 + "หนี้สินหมุนเวียน (Current Liabilities)",
            _NB*4 + "หนี้สินไม่หมุนเวียน (Non-Current Liabilities)",
        }

        def _fmt_bal_cell(val) -> str:
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return ""
            if not isinstance(val, (int, float)):
                return str(val)
            if val == 0:
                return "-"
            return f"{val:,.0f}"

        bal_display = bal_df.copy().astype(object)
        for _row in bal_df.index:
            for _col in bal_df.columns:
                bal_display.loc[_row, _col] = _fmt_bal_cell(bal_df.loc[_row, _col])

        _BAL_SUBSUB = {
            _NB*10 + "เงินสดและรายการเทียบเท่า",
            _NB*10 + "สินค้าคงเหลือ (Inventories)",
        }

        def hl_bal(row):
            if row.name in _BAL_HEADERS:
                # section header: พื้นหลังเทากลาง ตัวขาว ตัวหนา
                return ["background-color:#546e7a; color:#ffffff; font-weight:700; font-size:0.9em"] * len(row)
            if row.name in _BAL_TOTALS:
                # grand total: พื้นหลังน้ำเงินเข้มปานกลาง ตัวขาว ตัวหนา
                return ["background-color:#1976d2; color:#ffffff; font-weight:700"] * len(row)
            if row.name in _BAL_SUB:
                # หมุนเวียน/ไม่หมุนเวียน: พื้นหลังเทาอ่อนมาก ตัวเข้ม
                return ["background-color:#eceff1; color:#263238; font-weight:600"] * len(row)
            if row.name in _BAL_SUBSUB:
                # sub-items: ไม่มีพื้นหลัง ตัวสีเทา
                return ["background-color:#ffffff; color:#607d8b; font-style:italic"] * len(row)
            return ["background-color:#ffffff; color:#212121"] * len(row)

        bal_height = 340 if len(bal_df) > 4 else 220
        st.dataframe(
            bal_display.style.apply(hl_bal, axis=1),
            width="stretch", height=bal_height,
        )
        st.plotly_chart(chart_balance_sheet(balance), width="stretch", key="bal_chart")

        cash_inv_fig = chart_cash_and_inventories(balance)
        if cash_inv_fig is not None:
            st.plotly_chart(cash_inv_fig, width="stretch", key="bal_cash_inv_chart")

    # ==== TAB 3: Financial Ratios ====
    with tabs[3]:
        st.markdown("### อัตราส่วนทางการเงิน (Financial Ratios)")
        ratio_df = ratios_to_df(ratios)

        _PCT_ROWS = {
            "อัตรากำไรขั้นต้น (Gross Margin %)",
            "EBIT Margin %",
            "EBITDA Margin %",
            "อัตรากำไรสุทธิ (Net Margin %)",
            "Core Profit Margin %",
            "ROE %",
            "ROA %",
            "Effective Tax Rate %",
        }

        def _fmt_ratio_cell(val, row_name: str) -> str:
            if not isinstance(val, (int, float)) or pd.isna(val):
                return "-"
            if row_name in _PCT_ROWS:
                return f"{val:,.2f}%"
            return f"{val:,.3f}"

        # แปลง DataFrame เป็น string ก่อน แล้ว style ทับด้วย color
        ratio_display = ratio_df.copy().astype(object)
        for _row in ratio_df.index:
            for _col in ratio_df.columns:
                ratio_display.loc[_row, _col] = _fmt_ratio_cell(ratio_df.loc[_row, _col], _row)

        def _red_if_negative(val: str) -> str:
            try:
                if float(str(val).replace(",", "").replace("%", "")) < 0:
                    return "color: #c62828; font-weight: 600;"
            except Exception:
                pass
            return ""

        st.dataframe(
            ratio_display.style.applymap(_red_if_negative),  # type: ignore[attr-defined]
            width="stretch", height=420,
        )
        st.plotly_chart(chart_margins(ratios), width="stretch", key="ratios_margins")
        st.plotly_chart(chart_roe_roa(ratios), width="stretch", key="ratios_roe_roa")
        st.plotly_chart(chart_de_ratio(ratios), width="stretch", key="ratios_de")
        st.plotly_chart(chart_tax_rate(ratios, income), width="stretch", key="ratios_tax")

    # ==== TAB 4: Core Profit ====
    with tabs[4]:
        st.markdown("### วิเคราะห์กำไรจากธุรกิจหลัก vs รายการพิเศษ")
        st.markdown(
            "*Core Profit = กำไรสุทธิ − รายการพิเศษ (หลังภาษี) | "
            "วิเคราะห์จากงบกระแสเงินสด indirect method | หน่วย: พันบาท*"
        )

        # ── reuse global si_result + render multiselect ──────────────────────
        si_result = _si_result
        all_item_names = _all_item_names

        if all_item_names:
            selected_items = st.multiselect(
                "เลือกรายการพิเศษที่ต้องการรวมใน Extraordinary (ยกเลิกเลือก = ถือว่าเป็น operating)",
                options=all_item_names,
                default=all_item_names,
                key=_si_key,
            )
        else:
            selected_items = []

        # recompute based on current widget value
        core_adj = _recompute_core_global(core, si_result, selected_items)

        # ── Section A: Special items breakdown table ───────────────────────────
        st.markdown("#### แจกแจงรายการพิเศษ")
        st.markdown(
            "*ค่าบวก = IS กำไร (เพิ่ม NI) · ค่าลบ = IS ขาดทุน (ลด NI)*  "
            "— ยกเลิกเลือกรายการเพื่อจัดประเภทใหม่เป็น Operating"
        )

        if si_result:
            item_df = si_result["item_df"]
            tax_rates = si_result["tax_rates"]
            col_order = si_result["col_order"]

            def _fmt_si(val):
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    return "-"
                if abs(val) < 0.5:
                    return "-"
                return f"{val:+,.0f}"

            # Build display df: item rows + subtotal rows
            # Selected rows shown normally; deselected rows shown greyed out
            display_rows = {}
            for item_name in item_df.index:
                display_rows[item_name] = {
                    p: item_df.at[item_name, p] for p in col_order
                }

            # Subtotals based on selected items only
            pretax_sel: dict[str, float] = {}
            aftertax_sel: dict[str, float] = {}
            for p in col_order:
                pt = sum(
                    float(item_df.at[n, p])
                    for n in selected_items
                    if n in item_df.index
                )
                pretax_sel[p] = pt
                aftertax_sel[p] = pt * (1.0 - tax_rates.get(p, 0.20))

            # Format table
            si_display = {}
            for name, vals in display_rows.items():
                si_display[name] = {p: _fmt_si(v) for p, v in vals.items()}

            _PRETAX_ROW  = "รวมที่เลือก — ก่อนภาษี"
            _AFTERTAX_ROW = "รวมที่เลือก — หลังภาษี (= Extraordinary)"
            si_display[_PRETAX_ROW]  = {p: _fmt_si(v) for p, v in pretax_sel.items()}
            si_display[_AFTERTAX_ROW] = {p: _fmt_si(v) for p, v in aftertax_sel.items()}

            si_display_df = pd.DataFrame(si_display).T[col_order]

            def hl_si(row):
                name = str(row.name)
                if name == _AFTERTAX_ROW:
                    return ["background-color:#bf360c; color:#ffffff; font-weight:bold"] * len(row)
                if name == _PRETAX_ROW:
                    return ["background-color:#e65100; color:#ffffff; font-weight:bold"] * len(row)
                # Deselected rows: grey out
                if name not in selected_items:
                    return ["color:#aaaaaa; text-decoration:line-through"] * len(row)
                return [""] * len(row)

            n_rows = len(si_display_df)
            row_h = max(200, min(42 * n_rows + 55, 640))
            st.dataframe(
                si_display_df.style.apply(hl_si, axis=1),
                width="stretch",
                height=row_h,
            )

            with st.expander("วิธีอ่านตาราง"):
                st.markdown(
                    "- **แถวที่ขีดฆ่า (สีเทา)** = รายการที่ถูกยกเลิกเลือก → ถือว่าเป็น operating → ไม่รวมใน Extraordinary\n"
                    "- **แถวสีส้ม** = ยอดรวมรายการที่เลือก ก่อนภาษี\n"
                    "- **แถวสีแดง** = ยอดรวมหลังภาษี = impact ต่อ Core Profit\n"
                    "- รายการพิเศษมาจากการวิเคราะห์งบกระแสเงินสด (indirect method): "
                    "รายการที่ถูก reverse ออกใน 'adjustment to reconcile profit before tax'"
                )
        else:
            if view_mode == "annual":
                st.info(
                    "ข้อมูลแจกแจงรายการพิเศษมีเฉพาะในโหมดรายไตรมาส (XLSX) "
                    "— ในโหมดรายปีใช้ profitFromOtherActivity จาก SET API"
                )
            else:
                st.info("ไม่พบรายการพิเศษจากงบกระแสเงินสด (อาจไม่มีรายการนอก operating)")

        # ── Section B: Summary table (reacts to selection) ────────────────────
        st.markdown("#### ตารางสรุป Core Profit")

        core_df_adj = core_profit_to_df(core_adj)

        def hl_core(row):
            if "Core Profit" in str(row.name) or "กำไรจากธุรกิจหลัก" in str(row.name):
                return ["background-color:#1b5e20; color:#ffffff; font-weight:bold"] * len(row)
            if "Extraordinary" in str(row.name) or "รายการอื่น" in str(row.name):
                return ["background-color:#bf360c; color:#ffffff"] * len(row)
            return [""] * len(row)

        st.dataframe(color_negative_red(core_df_adj).apply(hl_core, axis=1), width="stretch", height=220)

        # ── Section C: Chart (reacts to selection) ────────────────────────────
        st.plotly_chart(chart_core_vs_reported(core_adj), width="stretch", key="core_chart")

        # ── Section D: Core Profit Growth (reacts to selection) ──────────────
        if len(core_adj) > 1:
            import math as _math
            _periods_adj  = [d["period"] for d in core_adj][::-1]
            _core_vals    = [d["core_profit"] for d in core_adj][::-1]
            _step_yoy = 4 if view_mode == "quarterly" else 1
            _yoy_sub  = "vs same quarter last year" if view_mode == "quarterly" else "vs previous year"

            def _safe_pct_core(curr, prev):
                if prev is None or prev == 0 or curr is None:
                    return None
                return (curr - prev) / abs(prev) * 100

            _core_yoy = [
                _safe_pct_core(_core_vals[i], _core_vals[i - _step_yoy])
                if i >= _step_yoy else None
                for i in range(len(_core_vals))
            ]
            _core_qoq = [
                _safe_pct_core(_core_vals[i], _core_vals[i - 1])
                if i >= 1 else None
                for i in range(len(_core_vals))
            ]

            def _growth_scatter(periods, vals, name, color, dash="solid"):
                y = [v if v is not None else _math.nan for v in vals]
                texts = [f"{v:+.1f}%" if v is not None else "" for v in vals]
                return go.Scatter(
                    name=name, x=periods, y=y,
                    mode="lines+markers+text",
                    line=dict(color=color, width=2.5, dash=dash),
                    marker=dict(size=8),
                    text=texts, textposition="top center", textfont=dict(size=8),
                    connectgaps=False,
                )

            fig_cg = go.Figure()
            fig_cg.add_trace(_growth_scatter(_periods_adj, _core_yoy, f"Core Profit YoY%", "#e91e63"))
            if view_mode == "quarterly":
                fig_cg.add_trace(_growth_scatter(_periods_adj, _core_qoq, "Core Profit QoQ%", "#9c27b0", dash="dash"))
            fig_cg.add_hline(y=0, line=dict(color="rgba(0,0,0,0.2)", width=1))
            fig_cg.update_layout(
                title=f"Core Profit Growth (%) — YoY: {_yoy_sub}" + (" | QoQ: vs previous quarter" if view_mode == "quarterly" else ""),
                height=400, template="plotly_white",
                xaxis_title="Period", yaxis_title="%",
                legend=dict(orientation="h", y=1.1, x=0.5, xanchor="center"),
            )
            _add_year_dividers(fig_cg, _periods_adj)
            st.plotly_chart(fig_cg, width="stretch", key="core_growth")



    # ==== TAB 5: Cash Flow ====
    with tabs[5]:
        st.markdown("### งบกระแสเงินสด (Cash Flow Statement)")
        st.markdown("*หน่วย: พันบาท*")

        # Show XLSX download links per quarter (quarterly mode only)
        if view_mode == "quarterly":
            url_map = {i["period"]: i.get("download_url", "") for i in income if i.get("download_url")}
            if url_map:
                link_parts = [f"[{period}]({url})" for period, url in url_map.items()]
                st.markdown("**ดาวน์โหลด ZIP งบการเงิน:** " + " | ".join(link_parts))

        if cashflow:
            cf_df = cashflow_to_df(cashflow)
            st.dataframe(color_negative_red(cf_df), width="stretch", height=220)
            st.plotly_chart(chart_cashflow(cashflow), width="stretch", key="cashflow_chart")
        else:
            st.info("ไม่มีข้อมูลกระแสเงินสด")

    # ---- Footer ----
    st.markdown("---")
    st.markdown(
        f"""<div style='text-align:center; color:#888; font-size:0.8rem; padding:1rem;'>
        <b>SET Financial Analyzer</b> &nbsp;|&nbsp;
        ข้อมูลจาก <a href="https://www.set.or.th" target="_blank">SET.or.th</a> &nbsp;|&nbsp;
        ดาวน์โหลดงบการเงินจาก News Section ZIP &nbsp;|&nbsp;
        หน่วย: พันบาท &nbsp;|&nbsp; ข้อมูลใหม่สุดอยู่ทางซ้ายเสมอ<br>
        <span style='color:#bbb; font-size:0.72rem;'>v{__version__}</span>
        </div>""",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
