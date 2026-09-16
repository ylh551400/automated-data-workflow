"""
Day-over-day market insights computed from stored snapshots.

Everything the email report says about the market comes from here, so the
logic is testable without network access or SMTP.
"""

import logging
import sqlite3

import pandas as pd

import config

logger = logging.getLogger(__name__)


def load_snapshot(conn: sqlite3.Connection, fetch_date: str) -> pd.DataFrame:
    return pd.read_sql_query(
        f"SELECT * FROM {config.TABLE_NAME} WHERE fetch_date = ? ORDER BY market_cap_rank",
        conn,
        params=(fetch_date,),
    )


def previous_date(conn: sqlite3.Connection, fetch_date: str) -> str | None:
    row = conn.execute(
        f"SELECT MAX(fetch_date) FROM {config.TABLE_NAME} WHERE fetch_date < ?", (fetch_date,)
    ).fetchone()
    return row[0] if row and row[0] else None


def market_cap_history(conn: sqlite3.Connection, up_to: str, days: int = 30) -> pd.DataFrame:
    """Total market cap and volume per stored day, up to and including `up_to`."""
    df = pd.read_sql_query(
        f"""
        SELECT fetch_date,
               SUM(market_cap)   AS total_market_cap,
               SUM(total_volume) AS total_volume,
               COUNT(*)          AS coins
        FROM {config.TABLE_NAME}
        WHERE fetch_date <= ?
        GROUP BY fetch_date
        ORDER BY fetch_date DESC
        LIMIT ?
        """,
        conn,
        params=(up_to, days),
    )
    return df.iloc[::-1].reset_index(drop=True)


def _records(df: pd.DataFrame, columns: list[str]) -> list[dict]:
    return df[columns].to_dict(orient="records")


def build_insights(
    conn: sqlite3.Connection,
    fetch_date: str,
    top_n: int = 5,
    alert_pct: float = config.MARKET_CAP_ALERT_PCT,
) -> dict | None:
    """
    Summarise `fetch_date` and compare it with the most recent earlier snapshot.
    Returns None when there is no data for `fetch_date`.
    """
    today = load_snapshot(conn, fetch_date)
    if today.empty:
        logger.warning("No snapshot stored for %s - insights unavailable", fetch_date)
        return None

    prev_date = previous_date(conn, fetch_date)
    prev = load_snapshot(conn, prev_date) if prev_date else pd.DataFrame()

    total_cap = float(today["market_cap"].sum())
    total_vol = float(today["total_volume"].sum())
    insights: dict = {
        "fetch_date": fetch_date,
        "previous_date": prev_date,
        "coins": int(len(today)),
        "total_market_cap": total_cap,
        "total_volume": total_vol,
        "total_market_cap_change_pct": None,
        "total_volume_change_pct": None,
        "btc_price": None,
        "btc_change_pct_24h": None,
        "top_gainers": [],
        "top_losers": [],
        "new_entrants": [],
        "dropouts": [],
        "rank_climbers": [],
        "rank_fallers": [],
        "top_by_market_cap": [],
        "history": market_cap_history(conn, fetch_date).to_dict(orient="records"),
        "alerts": [],
    }

    btc = today[today["coin_id"] == "bitcoin"]
    if not btc.empty:
        insights["btc_price"] = float(btc["current_price"].iloc[0])
        insights["btc_change_pct_24h"] = float(btc["price_change_pct_24h"].iloc[0])

    mover_cols = ["coin_id", "symbol", "name", "market_cap_rank", "current_price", "price_change_pct_24h"]
    ranked = today.sort_values("price_change_pct_24h", ascending=False)
    insights["top_gainers"] = _records(ranked.head(top_n), mover_cols)
    insights["top_losers"] = _records(ranked.tail(top_n).iloc[::-1], mover_cols)

    cap_cols = ["coin_id", "symbol", "name", "market_cap_rank", "market_cap"]
    insights["top_by_market_cap"] = _records(today.nsmallest(10, "market_cap_rank"), cap_cols)

    if not prev.empty:
        prev_cap = float(prev["market_cap"].sum())
        prev_vol = float(prev["total_volume"].sum())
        if prev_cap > 0:
            insights["total_market_cap_change_pct"] = (total_cap / prev_cap - 1) * 100
        if prev_vol > 0:
            insights["total_volume_change_pct"] = (total_vol / prev_vol - 1) * 100

        today_ids = set(today["coin_id"])
        prev_ids = set(prev["coin_id"])
        insights["new_entrants"] = _records(
            today[today["coin_id"].isin(today_ids - prev_ids)], ["coin_id", "symbol", "name", "market_cap_rank"]
        )
        insights["dropouts"] = _records(
            prev[prev["coin_id"].isin(prev_ids - today_ids)], ["coin_id", "symbol", "name", "market_cap_rank"]
        )

        merged = today.merge(
            prev[["coin_id", "market_cap_rank"]].rename(columns={"market_cap_rank": "prev_rank"}),
            on="coin_id",
        )
        merged["rank_change"] = merged["prev_rank"] - merged["market_cap_rank"]  # positive = climbed
        movers = merged[merged["rank_change"] != 0]
        rank_cols = ["coin_id", "symbol", "name", "market_cap_rank", "prev_rank", "rank_change"]
        insights["rank_climbers"] = _records(movers.nlargest(3, "rank_change"), rank_cols)
        insights["rank_fallers"] = _records(movers.nsmallest(3, "rank_change"), rank_cols)

        change = insights["total_market_cap_change_pct"]
        if change is not None and abs(change) >= alert_pct:
            direction = "up" if change > 0 else "down"
            insights["alerts"].append(
                f"Total market cap moved {direction} {abs(change):.1f}% since {prev_date} "
                f"(threshold {alert_pct:.0f}%)."
            )
        if insights["new_entrants"] or insights["dropouts"]:
            insights["alerts"].append(
                f"Top-{len(today)} composition changed: {len(insights['new_entrants'])} entered, "
                f"{len(insights['dropouts'])} dropped out."
            )
    else:
        insights["alerts"].append("First stored snapshot - day-over-day comparison starts tomorrow.")

    return insights
