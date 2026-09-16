"""
Chart generation for the email report (matplotlib, headless).

Design rules applied: one axis per chart, thin bars, hairline grid, direct
labels only where they carry the story, diverging blue/red for gains/losses,
a single sequential hue for magnitude, text always in ink colours.
"""

import logging
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

logger = logging.getLogger(__name__)

# Palette (light surface)
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
UP = "#2a78d6"      # diverging cool pole
DOWN = "#e34948"    # diverging warm pole
SERIES = "#2a78d6"  # sequential / single-series hue
OTHER = "#c3c2b7"   # residual bucket

plt.rcParams.update({
    "font.family": "sans-serif",
    "font.sans-serif": ["Segoe UI", "DejaVu Sans", "Arial", "sans-serif"],
    "figure.facecolor": SURFACE,
    "axes.facecolor": SURFACE,
    "savefig.facecolor": SURFACE,
    "text.color": INK,
    "axes.labelcolor": INK_SECONDARY,
    "xtick.color": MUTED,
    "ytick.color": INK_SECONDARY,
    "axes.titlecolor": INK,
    "axes.titleweight": "semibold",
    "axes.titlesize": 13,
    "axes.titlelocation": "left",
})


def compact_usd(value: float) -> str:
    """$1.52T / $291.8B / $33.3M style formatting."""
    sign = "-" if value < 0 else ""
    value = abs(value)
    for threshold, suffix in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if value >= threshold:
            return f"{sign}${value / threshold:,.2f}{suffix}".replace(".00", "")
    return f"{sign}${value:,.0f}"


def _style_axes(ax, grid_axis: str = "x") -> None:
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(AXIS)
        ax.spines[side].set_linewidth(1)
    ax.grid(axis=grid_axis, color=GRID, linewidth=1, linestyle="-")
    ax.set_axisbelow(True)
    ax.tick_params(length=0, labelsize=10)


def _save(fig, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=160, bbox_inches="tight", pad_inches=0.3)
    plt.close(fig)
    logger.info("Chart saved: %s", path)
    return path


def chart_movers(insights: dict, path: Path) -> Path | None:
    """Top gainers and losers (24h %) as a diverging horizontal bar chart."""
    gainers = insights.get("top_gainers", [])
    losers = insights.get("top_losers", [])
    if not gainers and not losers:
        return None

    rows = list(gainers) + list(reversed(losers))  # best at top, worst at bottom
    labels = [f"{r['symbol']}  {r['name']}" for r in rows]
    values = [r["price_change_pct_24h"] for r in rows]
    colors = [UP if v >= 0 else DOWN for v in values]

    fig, ax = plt.subplots(figsize=(8, 0.42 * len(rows) + 1.4))
    y = range(len(rows))[::-1]
    ax.barh(y, values, height=0.55, color=colors)
    ax.axvline(0, color=AXIS, linewidth=1)
    ax.set_yticks(list(y), labels)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:+.0f}%"))
    _style_axes(ax, grid_axis="x")

    span = max(abs(v) for v in values) or 1
    ax.set_xlim(-span * 1.25 if min(values) < 0 else 0, span * 1.25 if max(values) > 0 else 0)
    pad = span * 0.03
    for yi, v in zip(y, values):
        ax.text(v + (pad if v >= 0 else -pad), yi, f"{v:+.1f}%",
                va="center", ha="left" if v >= 0 else "right", fontsize=9.5, color=INK_SECONDARY)

    ax.set_title(f"24h price movers - top {len(gainers)} gainers and losers ({insights['fetch_date']})")
    return _save(fig, path)


def chart_market_cap(insights: dict, path: Path) -> Path | None:
    """Top 10 by market cap plus the residual bucket, single sequential hue."""
    top = insights.get("top_by_market_cap", [])
    if not top:
        return None

    total = insights["total_market_cap"]
    top_sum = sum(r["market_cap"] for r in top)
    others = insights["coins"] - len(top)
    labels = [f"{r['symbol']}  {r['name']}" for r in top]
    values = [r["market_cap"] for r in top]
    colors = [SERIES] * len(top)
    if others > 0 and total > top_sum:
        labels.append(f"Other ({others} coins)")
        values.append(total - top_sum)
        colors.append(OTHER)

    fig, ax = plt.subplots(figsize=(8, 0.42 * len(values) + 1.4))
    y = range(len(values))[::-1]
    ax.barh(y, values, height=0.55, color=colors)
    ax.set_yticks(list(y), labels)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: compact_usd(v)))
    _style_axes(ax, grid_axis="x")
    ax.set_xlim(0, max(values) * 1.22)
    for yi, v in zip(y, values):
        ax.text(v + max(values) * 0.015, yi, f"{compact_usd(v)}  ({v / total:.0%})",
                va="center", fontsize=9.5, color=INK_SECONDARY)

    ax.set_title(f"Market cap share of the top {insights['coins']} ({insights['fetch_date']})")
    return _save(fig, path)


def chart_market_cap_trend(insights: dict, path: Path) -> Path | None:
    """Total market cap of the stored set over time. Needs at least two days."""
    history = insights.get("history", [])
    if len(history) < 2:
        logger.info("Trend chart skipped: only %s day(s) of history", len(history))
        return None

    dates = [h["fetch_date"] for h in history]
    caps = [h["total_market_cap"] for h in history]

    fig, ax = plt.subplots(figsize=(8, 3.6))
    x = range(len(dates))
    ax.plot(x, caps, color=SERIES, linewidth=2, solid_joinstyle="round", solid_capstyle="round",
            marker="o", markersize=8, markerfacecolor=SERIES, markeredgecolor=SURFACE, markeredgewidth=2)
    ax.fill_between(x, caps, min(caps) * 0.98, color=SERIES, alpha=0.10, linewidth=0)
    step = max(1, len(dates) // 8)
    ax.set_xticks(list(x)[::step], dates[::step], rotation=0)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: compact_usd(v)))
    _style_axes(ax, grid_axis="y")
    ax.margins(x=0.04)
    ax.text(x[-1], caps[-1], f"  {compact_usd(caps[-1])}", va="center", fontsize=10, color=INK)
    ax.set_title(f"Total market cap of the top {insights['coins']} - last {len(dates)} snapshots")
    return _save(fig, path)


def generate_charts(insights: dict | None, out_dir: Path) -> dict[str, Path]:
    """Render every chart that has data. Returns {chart_name: path}."""
    if not insights:
        return {}
    out_dir = Path(out_dir)
    charts: dict[str, Path] = {}
    for name, fn in (
        ("movers", chart_movers),
        ("market_cap", chart_market_cap),
        ("trend", chart_market_cap_trend),
    ):
        try:
            result = fn(insights, out_dir / f"{name}.png")
            if result:
                charts[name] = result
        except Exception as exc:  # noqa: BLE001 - a chart failure must not block the report
            logger.exception("Chart '%s' failed: %s", name, exc)
    logger.info("Generated %s chart(s)", len(charts))
    return charts
