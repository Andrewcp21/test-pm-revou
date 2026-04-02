"""
Marketing Report Pipeline — RD-All (Google Sheets)
Usage:
  python marketing_pipeline.py                  # analyze + generate report (opens in browser)
  python marketing_pipeline.py --no-open        # generate without opening browser
  python marketing_pipeline.py --no-telegram    # skip Telegram
  python marketing_pipeline.py --url <url>      # use a custom Google Sheets CSV export URL
"""

import argparse
import os
import sys
import webbrowser
from datetime import date
from io import StringIO

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from dotenv import load_dotenv
import urllib.request

load_dotenv(override=True)

SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1x-FYQR6V_QHNUBszF49k3_UkAxned3zwLdLgH1I8BYY"
    "/export?format=csv&gid=1769434757"
)

# ── Formatting helpers ──────────────────────────────────────────────────────

def fmt_idr(value):
    if value >= 1_000_000_000:
        return f"Rp {value/1_000_000_000:.1f}B"
    if value >= 1_000_000:
        return f"Rp {value/1_000_000:.1f}M"
    return f"Rp {value:,.0f}"

def fmt_pct(value):
    return f"{value:.2f}%"

def fmt_num(value):
    if value >= 1_000_000:
        return f"{value/1_000_000:.1f}M"
    if value >= 1_000:
        return f"{value/1_000:.1f}K"
    return f"{value:,.0f}"

# ── 1. DATA LOADING ──────────────────────────────────────────────────────────

def load_data(url: str) -> pd.DataFrame:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as resp:
        # Follow redirect if needed
        final_url = resp.geturl()
        content = resp.read().decode("utf-8")

    if final_url != url:
        req2 = urllib.request.Request(final_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req2) as resp2:
            content = resp2.read().decode("utf-8")

    df = pd.read_csv(StringIO(content))

    # Clean column names
    df.columns = df.columns.str.strip()

    # Parse date
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])

    # Numeric cleanup — remove commas and convert
    num_cols = ["Spend", "Impressions", "Link Clicks", "Conversion",
                "CPM", "CTR", "CPC", "CR", "Cost per Conversion",
                "Spend L3D", "Impressions L3D", "Link Clicks L3D", "Conversion L3D",
                "Spend L14D", "Impressions L14D", "Link Clicks L14D", "Conversion L14D"]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", "").str.replace("%", ""),
                errors="coerce"
            ).fillna(0)

    return df

# ── 2. DATA ANALYSIS ─────────────────────────────────────────────────────────

def analyze(df: pd.DataFrame) -> dict:
    # Date range
    latest_date = df["date"].max()
    earliest_date = df["date"].min()

    # Overall totals
    total_spend = df["Spend"].sum()
    total_impressions = df["Impressions"].sum()
    total_clicks = df["Link Clicks"].sum()
    total_conversions = df["Conversion"].sum()
    overall_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
    overall_cr = (total_conversions / total_clicks * 100) if total_clicks > 0 else 0
    overall_cpc = (total_spend / total_clicks) if total_clicks > 0 else 0
    overall_cpa = (total_spend / total_conversions) if total_conversions > 0 else 0

    # By Channel
    by_channel = df.groupby("Channel").agg(
        Spend=("Spend", "sum"),
        Impressions=("Impressions", "sum"),
        Clicks=("Link Clicks", "sum"),
        Conversions=("Conversion", "sum"),
    ).reset_index()
    by_channel["CTR"] = by_channel["Clicks"] / by_channel["Impressions"] * 100
    by_channel["CR"] = by_channel["Conversions"] / by_channel["Clicks"] * 100
    by_channel["CPA"] = by_channel["Spend"] / by_channel["Conversions"]
    by_channel = by_channel.sort_values("Spend", ascending=False)

    # By Campaign
    by_campaign = df.groupby("Campaign").agg(
        Spend=("Spend", "sum"),
        Impressions=("Impressions", "sum"),
        Clicks=("Link Clicks", "sum"),
        Conversions=("Conversion", "sum"),
    ).reset_index()
    by_campaign["CPA"] = by_campaign["Spend"] / by_campaign["Conversions"].replace(0, float("nan"))
    by_campaign = by_campaign.sort_values("Spend", ascending=False)

    # By Vertical
    by_vertical = df.groupby("Vertical").agg(
        Spend=("Spend", "sum"),
        Conversions=("Conversion", "sum"),
    ).reset_index()
    by_vertical["CPA"] = by_vertical["Spend"] / by_vertical["Conversions"].replace(0, float("nan"))

    # Daily trend
    daily = df.groupby("date").agg(
        Spend=("Spend", "sum"),
        Impressions=("Impressions", "sum"),
        Clicks=("Link Clicks", "sum"),
        Conversions=("Conversion", "sum"),
    ).reset_index().sort_values("date")

    # Channel × Vertical heatmap (Spend)
    pivot_spend = df.pivot_table(
        values="Spend", index="Vertical", columns="Channel", aggfunc="sum", fill_value=0
    )

    # L3D vs L14D delta summary
    latest = df[df["date"] == latest_date].copy()
    spend_l3d = latest["Spend L3D"].sum()
    spend_l14d_avg = latest["Spend L14D"].sum() / 14 * 3 if latest["Spend L14D"].sum() > 0 else 0

    # ── Last week vs previous week ──────────────────────────────────────────
    import numpy as np
    lw_end = latest_date
    lw_start = lw_end - pd.Timedelta(days=6)
    pw_end = lw_start - pd.Timedelta(days=1)
    pw_start = pw_end - pd.Timedelta(days=6)

    lw_df = df[(df["date"] >= lw_start) & (df["date"] <= lw_end)]
    pw_df = df[(df["date"] >= pw_start) & (df["date"] <= pw_end)]

    lw_spend = lw_df["Spend"].sum()
    pw_spend = pw_df["Spend"].sum()
    spend_wow = ((lw_spend - pw_spend) / pw_spend * 100) if pw_spend > 0 else 0

    # By Channel — last week
    by_channel_lw = lw_df.groupby("Channel").agg(
        Spend=("Spend", "sum"),
        Conversions=("Conversion", "sum"),
    ).reset_index()
    by_channel_lw["CPA"] = by_channel_lw["Spend"] / by_channel_lw["Conversions"].replace(0, np.nan)
    by_channel_lw = by_channel_lw.sort_values("Spend", ascending=False)

    # Vertical > Campaign > Channel — last week (only DM, DA, SWE)
    target_verticals = ["DM", "DA", "SWE"]
    lw_vcc = lw_df[lw_df["Vertical"].isin(target_verticals)].groupby(
        ["Vertical", "Campaign", "Channel"]
    ).agg(
        Spend=("Spend", "sum"),
        Conversions=("Conversion", "sum"),
    ).reset_index()
    lw_vcc["CPA"] = lw_vcc["Spend"] / lw_vcc["Conversions"].replace(0, np.nan)

    return {
        "df": df,
        "latest_date": latest_date,
        "earliest_date": earliest_date,
        "lw_start": lw_start,
        "lw_end": lw_end,
        "total_spend": total_spend,
        "total_impressions": total_impressions,
        "total_clicks": total_clicks,
        "total_conversions": total_conversions,
        "overall_ctr": overall_ctr,
        "overall_cr": overall_cr,
        "overall_cpc": overall_cpc,
        "overall_cpa": overall_cpa,
        "by_channel": by_channel,
        "by_campaign": by_campaign,
        "by_vertical": by_vertical,
        "daily": daily,
        "pivot_spend": pivot_spend,
        "spend_l3d": spend_l3d,
        "spend_l14d_avg": spend_l14d_avg,
        "lw_spend": lw_spend,
        "pw_spend": pw_spend,
        "spend_wow": spend_wow,
        "by_channel_lw": by_channel_lw,
        "lw_vcc": lw_vcc,
        "target_verticals": target_verticals,
    }

# ── 3. CHART BUILDERS ────────────────────────────────────────────────────────

COLORS = ["#2563EB", "#7C3AED", "#059669", "#D97706", "#DC2626", "#0891B2"]

CHART_LAYOUT = dict(
    paper_bgcolor="white", plot_bgcolor="#F8FAFC",
    font=dict(family="Inter, system-ui, sans-serif", color="#1E293B"),
    margin=dict(t=50, b=40, l=60, r=30),
    hoverlabel=dict(bgcolor="white", font_size=13),
)


def chart_spend_by_channel(by_channel):
    fig = go.Figure(go.Bar(
        x=by_channel["Channel"],
        y=by_channel["Spend"],
        marker_color=COLORS[:len(by_channel)],
        text=[fmt_idr(v) for v in by_channel["Spend"]],
        textposition="outside",
        hovertemplate="<b>%{x}</b><br>Spend: %{text}<br>Conversions: %{customdata[0]:,.0f}<br>CPA: %{customdata[1]}<extra></extra>",
        customdata=list(zip(by_channel["Conversions"], [fmt_idr(v) for v in by_channel["CPA"]])),
    ))
    fig.update_layout(**CHART_LAYOUT, title="Spend by Channel", yaxis_title="Spend (Rp)", height=380)
    return fig.to_html(full_html=False, include_plotlyjs=False)


def chart_daily_trend(daily):
    fig = make_subplots(rows=2, cols=1,
                        subplot_titles=("Daily Spend", "Daily Conversions"),
                        vertical_spacing=0.15, shared_xaxes=True)
    fig.add_trace(go.Scatter(
        x=daily["date"], y=daily["Spend"],
        mode="lines+markers", name="Spend",
        line=dict(color="#2563EB", width=2.5), marker=dict(size=6),
        hovertemplate="%{x|%b %d}: %{y:,.0f}<extra></extra>",
    ), row=1, col=1)
    fig.add_trace(go.Bar(
        x=daily["date"], y=daily["Conversions"],
        name="Conversions", marker_color="#059669",
        hovertemplate="%{x|%b %d}: %{y:,.0f} conversions<extra></extra>",
    ), row=2, col=1)
    fig.update_layout(**CHART_LAYOUT, height=480, showlegend=False)
    fig.update_yaxes(title_text="Spend (Rp)", row=1, col=1)
    fig.update_yaxes(title_text="Conversions", row=2, col=1)
    return fig.to_html(full_html=False, include_plotlyjs=False)


def chart_campaign_performance(by_campaign):
    fig = make_subplots(rows=1, cols=2,
                        subplot_titles=("Spend by Campaign", "Conversions by Campaign"),
                        horizontal_spacing=0.12)
    fig.add_trace(go.Bar(
        x=by_campaign["Spend"], y=by_campaign["Campaign"], orientation="h",
        marker_color="#2563EB",
        hovertemplate="<b>%{y}</b><br>Spend: %{x:,.0f}<extra></extra>",
        name="Spend",
    ), row=1, col=1)
    fig.add_trace(go.Bar(
        x=by_campaign["Conversions"], y=by_campaign["Campaign"], orientation="h",
        marker_color="#059669",
        hovertemplate="<b>%{y}</b><br>Conversions: %{x:,.0f}<extra></extra>",
        name="Conversions",
    ), row=1, col=2)
    fig.update_layout(**CHART_LAYOUT, height=360, showlegend=False)
    return fig.to_html(full_html=False, include_plotlyjs=False)


def chart_funnel(total_impressions, total_clicks, total_conversions):
    fig = go.Figure(go.Funnel(
        y=["Impressions", "Clicks", "Conversions"],
        x=[total_impressions, total_clicks, total_conversions],
        textinfo="value+percent initial",
        marker=dict(color=["#2563EB", "#7C3AED", "#059669"]),
    ))
    fig.update_layout(**CHART_LAYOUT, title="Marketing Funnel", height=360)
    return fig.to_html(full_html=False, include_plotlyjs=False)


def chart_heatmap(pivot_spend):
    if pivot_spend.empty:
        return "<p>No data available for heatmap.</p>"
    z = pivot_spend.values / 1_000_000
    text = [[f"Rp {v:.1f}M" for v in row] for row in z]
    fig = go.Figure(go.Heatmap(
        z=z, x=pivot_spend.columns.tolist(), y=pivot_spend.index.tolist(),
        text=text, texttemplate="%{text}", colorscale="Blues",
        hovertemplate="<b>%{y}</b> · %{x}<br>Spend: %{text}<extra></extra>",
    ))
    fig.update_layout(**CHART_LAYOUT, title="Spend: Vertical × Channel (Rp Juta)", height=360)
    return fig.to_html(full_html=False, include_plotlyjs=False)


def chart_cpa_by_channel(by_channel):
    fig = go.Figure(go.Bar(
        x=by_channel["Channel"],
        y=by_channel["CPA"],
        marker_color=[COLORS[i % len(COLORS)] for i in range(len(by_channel))],
        text=[fmt_idr(v) for v in by_channel["CPA"]],
        textposition="outside",
        hovertemplate="<b>%{x}</b><br>CPA: %{text}<extra></extra>",
    ))
    fig.update_layout(**CHART_LAYOUT, title="Cost per Acquisition (CPA) by Channel",
                      yaxis_title="CPA (Rp)", height=360)
    return fig.to_html(full_html=False, include_plotlyjs=False)

# ── 4. CHANNEL METRICS TABLE ─────────────────────────────────────────────────

def channel_table_html(by_channel):
    rows = ""
    for _, row in by_channel.iterrows():
        rows += f"""
        <tr>
          <td><b>{row['Channel']}</b></td>
          <td>{fmt_idr(row['Spend'])}</td>
          <td>{fmt_num(row['Impressions'])}</td>
          <td>{fmt_num(row['Clicks'])}</td>
          <td>{fmt_num(row['Conversions'])}</td>
          <td>{fmt_pct(row['CTR'])}</td>
          <td>{fmt_pct(row['CR'])}</td>
          <td>{fmt_idr(row['CPA'])}</td>
        </tr>"""
    return f"""
    <table class="data-table">
      <thead><tr>
        <th>Channel</th><th>Spend</th><th>Impressions</th><th>Clicks</th>
        <th>Conversions</th><th>CTR</th><th>CR</th><th>CPA</th>
      </tr></thead>
      <tbody>{rows}</tbody>
    </table>"""

# ── 5. HTML REPORT ───────────────────────────────────────────────────────────

def build_html_report(data: dict) -> str:
    c1 = chart_spend_by_channel(data["by_channel"])
    c2 = chart_daily_trend(data["daily"])
    c3 = chart_campaign_performance(data["by_campaign"])
    c4 = chart_funnel(data["total_impressions"], data["total_clicks"], data["total_conversions"])
    c5 = chart_heatmap(data["pivot_spend"])
    c6 = chart_cpa_by_channel(data["by_channel"])
    tbl = channel_table_html(data["by_channel"])

    date_range = f"{data['earliest_date'].strftime('%b %d')} – {data['latest_date'].strftime('%b %d, %Y')}"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Marketing Performance Report</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: 'Inter', system-ui, sans-serif; background: #F1F5F9; color: #1E293B; }}
  .header {{ background: linear-gradient(135deg, #1E1B4B 0%, #4F46E5 100%); color: white; padding: 36px 48px; }}
  .header h1 {{ font-size: 1.8rem; font-weight: 700; margin-bottom: 4px; }}
  .header p {{ opacity: 0.8; font-size: 0.95rem; }}
  .container {{ max-width: 1200px; margin: 0 auto; padding: 32px 24px; }}
  .kpi-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 32px; }}
  .kpi-grid-2 {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 32px; }}
  .card {{ background: white; border-radius: 12px; padding: 24px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }}
  .card .label {{ font-size: 0.75rem; font-weight: 600; color: #64748B; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 8px; }}
  .card .value {{ font-size: 1.5rem; font-weight: 700; }}
  .card .sub {{ font-size: 0.82rem; color: #64748B; margin-top: 4px; }}
  .section {{ background: white; border-radius: 12px; padding: 28px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); margin-bottom: 24px; }}
  .section h2 {{ font-size: 1.05rem; font-weight: 600; color: #1E293B; margin-bottom: 20px; padding-bottom: 12px; border-bottom: 2px solid #E2E8F0; }}
  .two-col {{ display: grid; grid-template-columns: 1fr 1fr; gap: 24px; margin-bottom: 24px; }}
  .data-table {{ width: 100%; border-collapse: collapse; font-size: 0.88rem; }}
  .data-table th {{ background: #F8FAFC; padding: 10px 14px; text-align: left; font-weight: 600; color: #64748B; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.04em; border-bottom: 2px solid #E2E8F0; }}
  .data-table td {{ padding: 10px 14px; border-bottom: 1px solid #F1F5F9; }}
  .data-table tr:hover td {{ background: #F8FAFC; }}
  .footer {{ text-align: center; color: #94A3B8; font-size: 0.8rem; margin-top: 40px; padding-bottom: 32px; }}
  @media (max-width: 768px) {{ .kpi-grid, .kpi-grid-2 {{ grid-template-columns: repeat(2, 1fr); }} .two-col {{ grid-template-columns: 1fr; }} .header {{ padding: 24px; }} }}
</style>
</head>
<body>
<div class="header">
  <h1>📣 Marketing Performance Report</h1>
  <p>{date_range} &nbsp;|&nbsp; Generated {date.today().strftime('%B %d, %Y')} &nbsp;|&nbsp; RD-All Channels</p>
</div>
<div class="container">

  <div class="kpi-grid">
    <div class="card"><div class="label">Total Spend</div><div class="value">{fmt_idr(data['total_spend'])}</div><div class="sub">All channels combined</div></div>
    <div class="card"><div class="label">Total Impressions</div><div class="value">{fmt_num(data['total_impressions'])}</div><div class="sub">Across all campaigns</div></div>
    <div class="card"><div class="label">Total Clicks</div><div class="value">{fmt_num(data['total_clicks'])}</div><div class="sub">CTR: {fmt_pct(data['overall_ctr'])}</div></div>
    <div class="card"><div class="label">Total Conversions</div><div class="value">{fmt_num(data['total_conversions'])}</div><div class="sub">CR: {fmt_pct(data['overall_cr'])}</div></div>
  </div>

  <div class="kpi-grid-2">
    <div class="card"><div class="label">Overall CPC</div><div class="value">{fmt_idr(data['overall_cpc'])}</div><div class="sub">Cost per Click</div></div>
    <div class="card"><div class="label">Overall CPA</div><div class="value">{fmt_idr(data['overall_cpa'])}</div><div class="sub">Cost per Acquisition</div></div>
    <div class="card"><div class="label">Active Channels</div><div class="value">{data['by_channel']['Channel'].nunique()}</div><div class="sub">Channels tracked</div></div>
    <div class="card"><div class="label">Date Range</div><div class="value" style="font-size:1.1rem">{date_range}</div><div class="sub">{(data['latest_date'] - data['earliest_date']).days + 1} days</div></div>
  </div>

  <div class="section"><h2>📊 Channel Overview</h2>{tbl}</div>

  <div class="section"><h2>💰 Spend by Channel</h2>{c1}</div>

  <div class="section"><h2>📈 Daily Spend & Conversions</h2>{c2}</div>

  <div class="two-col">
    <div class="section"><h2>🎯 Funnel Overview</h2>{c4}</div>
    <div class="section"><h2>💸 CPA by Channel</h2>{c6}</div>
  </div>

  <div class="section"><h2>🗂️ Campaign Performance</h2>{c3}</div>

  <div class="section"><h2>🗺️ Spend Heatmap: Vertical × Channel</h2>{c5}</div>

  <div class="footer">Generated automatically by Marketing Pipeline &nbsp;·&nbsp; {date.today().strftime('%Y-%m-%d')} &nbsp;·&nbsp; Source: RD-All Google Sheets</div>
</div>
</body>
</html>"""

# ── 6. TELEGRAM SENDER ───────────────────────────────────────────────────────

def _build_summary_chart(data: dict) -> bytes:
    """Generate a 2-panel PNG: WoW spend bar + spend by channel."""
    by_ch = data["by_channel_lw"].head(6)  # top 6 channels
    wow = data["spend_wow"]
    arrow = f"+{wow:.1f}%" if wow >= 0 else f"{wow:.1f}%"
    wow_color = "#16A34A" if wow >= 0 else "#DC2626"

    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=(
            f"Week-over-Week Spend  ({arrow})",
            "Spend by Channel  (last week)",
        ),
        horizontal_spacing=0.14,
    )

    # Left: WoW bars
    fig.add_trace(go.Bar(
        x=["Prev Week", "Last Week"],
        y=[data["pw_spend"], data["lw_spend"]],
        marker_color=["#94A3B8", wow_color],
        text=[fmt_idr(data["pw_spend"]), fmt_idr(data["lw_spend"])],
        textposition="outside",
        hoverinfo="skip",
    ), row=1, col=1)

    # Right: spend by channel
    fig.add_trace(go.Bar(
        x=by_ch["Channel"],
        y=by_ch["Spend"],
        marker_color=COLORS[:len(by_ch)],
        text=[fmt_idr(v) for v in by_ch["Spend"]],
        textposition="outside",
        hoverinfo="skip",
    ), row=1, col=2)

    fig.update_layout(
        **CHART_LAYOUT,
        height=420, width=960,
        showlegend=False,
        title=dict(
            text=f"Marketing Summary  —  {data['lw_start'].strftime('%b %d')}–{data['lw_end'].strftime('%b %d, %Y')}",
            font=dict(size=15),
        ),
    )
    fig.update_yaxes(visible=False)
    return fig.to_image(format="png", scale=2)


def _send_multipart(url: str, fields: dict, file_field: str, filename: str,
                    file_bytes: bytes, mime: str):
    """POST multipart/form-data with one file field."""
    boundary = "----TGBoundary7MA4YWxkTrZu0gW"
    body = b""
    for k, v in fields.items():
        body += (
            f"--{boundary}\r\nContent-Disposition: form-data; name=\"{k}\"\r\n\r\n{v}\r\n"
        ).encode()
    body += (
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"{file_field}\"; "
        f"filename=\"{filename}\"\r\nContent-Type: {mime}\r\n\r\n"
    ).encode() + file_bytes + f"\r\n--{boundary}--\r\n".encode()
    urllib.request.urlopen(urllib.request.Request(
        url, data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    ))


def send_telegram(data: dict, report_path: str):
    import urllib.parse

    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        raise ValueError("TELEGRAM_TOKEN and TELEGRAM_CHAT_ID must be set in .env")

    base = f"https://api.telegram.org/bot{token}"
    lw_label = f"{data['lw_start'].strftime('%b %d')}–{data['lw_end'].strftime('%b %d, %Y')}"
    wow_sign = "+" if data["spend_wow"] >= 0 else ""
    arrow = "↑" if data["spend_wow"] >= 0 else "↓"

    # ── 1. Chart image + short caption ──────────────────────────────────────
    print("    → Generating summary chart ...")
    chart_png = _build_summary_chart(data)

    # Top 3 channels for caption
    top3_ch = data["by_channel_lw"].head(3)
    top3_lines = "\n".join(
        f"  • {row['Channel']}: {fmt_idr(row['Spend'])} | CPA {fmt_idr(row['CPA']) if row['Conversions'] > 0 else 'N/A'}"
        for _, row in top3_ch.iterrows()
    )
    # Vertical totals for caption
    vcc_df = data["lw_vcc"]
    vert_lines = []
    for v in data["target_verticals"]:
        v_df = vcc_df[vcc_df["Vertical"] == v]
        if v_df.empty:
            continue
        vs = v_df["Spend"].sum()
        vc = v_df["Conversions"].sum()
        vc_cpa = vs / vc if vc > 0 else 0
        vert_lines.append(f"  {v}: {fmt_idr(vs)} | {vc:,.0f} conv | CPA {fmt_idr(vc_cpa)}")

    caption = (
        f"📣 *Marketing Report — {lw_label}*\n\n"
        f"📊 Spend: *{fmt_idr(data['lw_spend'])}* ({wow_sign}{data['spend_wow']:.1f}% {arrow} vs prev week)\n"
        f"🎯 Conv: *{data['by_channel_lw']['Conversions'].sum():,.0f}*  |  "
        f"CPA: *{fmt_idr(data['lw_spend'] / data['by_channel_lw']['Conversions'].sum())}*\n\n"
        f"💰 *Top Channels:*\n{top3_lines}\n\n"
        f"🎯 *By Vertical:*\n" + "\n".join(vert_lines)
    ).replace("_", "\\_")

    _send_multipart(
        url=f"{base}/sendPhoto",
        fields={"chat_id": chat_id, "caption": caption, "parse_mode": "Markdown"},
        file_field="photo", filename="summary.png",
        file_bytes=chart_png, mime="image/png",
    )
    print("    → Chart + caption sent")

    # ── 2. Full vertical breakdown as text ──────────────────────────────────
    vertical_blocks = []
    for vertical in data["target_verticals"]:
        v_df = vcc_df[vcc_df["Vertical"] == vertical]
        if v_df.empty:
            continue
        v_spend = v_df["Spend"].sum()
        v_conv = v_df["Conversions"].sum()
        v_cpa = v_spend / v_conv if v_conv > 0 else 0
        lines = [f"🎯 *{vertical}* — {fmt_idr(v_spend)} | {v_conv:,.0f} conv | CPA {fmt_idr(v_cpa)}"]
        for campaign in v_df["Campaign"].unique():
            c_df = v_df[v_df["Campaign"] == campaign].sort_values("Spend", ascending=False)
            c_spend = c_df["Spend"].sum()
            c_conv = c_df["Conversions"].sum()
            lines.append(f"\n  📌 *{campaign}* — {fmt_idr(c_spend)} | {c_conv:,.0f} conv")
            for _, row in c_df.iterrows():
                cpa_str = fmt_idr(row["CPA"]) if row["Conversions"] > 0 else "N/A"
                lines.append(f"    · {row['Channel']}: {fmt_idr(row['Spend'])} | {row['Conversions']:,.0f} conv | CPA {cpa_str}")
        vertical_blocks.append("\n".join(lines))

    breakdown_msg = (
        f"📋 *Breakdown by Vertical — {lw_label}*\n\n"
        + "\n\n━━━━━━━━━━━━━━━━━━━\n\n".join(vertical_blocks)
    ).replace("_", "\\_")

    # Send in chunks if needed
    chunks = [breakdown_msg[i:i+4000] for i in range(0, len(breakdown_msg), 4000)]
    for chunk in chunks:
        params = urllib.parse.urlencode({
            "chat_id": chat_id, "text": chunk, "parse_mode": "Markdown",
        }).encode()
        try:
            urllib.request.urlopen(urllib.request.Request(f"{base}/sendMessage", data=params))
        except Exception as e:
            print(f"    ⚠️  Markdown failed ({e}), retrying plain text...")
            params = urllib.parse.urlencode({
                "chat_id": chat_id,
                "text": chunk.replace("*", "").replace("_", "").replace("\\", ""),
            }).encode()
            urllib.request.urlopen(urllib.request.Request(f"{base}/sendMessage", data=params))
    print("    → Breakdown text sent")

    # ── 3. HTML report as file attachment ───────────────────────────────────
    with open(report_path, "rb") as f:
        html_bytes = f.read()
    report_filename = f"marketing_report_{date.today().isoformat()}.html"
    _send_multipart(
        url=f"{base}/sendDocument",
        fields={"chat_id": chat_id, "caption": f"📎 Full report — {lw_label}"},
        file_field="document", filename=report_filename,
        file_bytes=html_bytes, mime="text/html",
    )
    print("    → HTML report file sent")

    print(f"  ✅ All messages sent to Telegram chat {chat_id}")

# ── 7. MAIN ──────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Marketing report from RD-All Google Sheets")
    parser.add_argument("--url", default=SHEET_URL, help="Google Sheets CSV export URL")
    parser.add_argument("--no-open", action="store_true", help="Skip opening browser")
    parser.add_argument("--no-telegram", action="store_true", help="Skip Telegram")
    args = parser.parse_args()

    print(f"\n📥 Loading data from Google Sheets ...")
    df = load_data(args.url)
    print(f"   → {len(df)} rows | {df['date'].min().date()} to {df['date'].max().date()}")
    print(f"   → Channels: {', '.join(df['Channel'].unique())}")

    print("\n📊 Analyzing ...")
    data = analyze(df)
    print(f"   → Spend: {fmt_idr(data['total_spend'])} | "
          f"Conversions: {data['total_conversions']:,.0f} | "
          f"CPA: {fmt_idr(data['overall_cpa'])}")

    print("\n🖨️  Building HTML report ...")
    html = build_html_report(data)
    report_name = f"marketing_report_{date.today().isoformat()}.html"
    report_path = report_name
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"   → Saved: {report_path}")

    has_telegram = bool(os.getenv("TELEGRAM_TOKEN") and os.getenv("TELEGRAM_CHAT_ID"))
    if has_telegram and not args.no_telegram:
        print("\n📨 Sending to Telegram ...")
        try:
            send_telegram(data, report_path)
        except Exception as e:
            print(f"  ❌ Telegram failed: {e}")

    if not args.no_open:
        import pathlib
        webbrowser.open(pathlib.Path(report_path).resolve().as_uri())

    print(f"\n✅ Done! Report: {report_path}")

if __name__ == "__main__":
    main()
