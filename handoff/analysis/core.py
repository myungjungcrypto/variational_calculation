"""Local-only data loading and piecewise-constant OI integration.

All intervals are [start, end). Same-second fills are aggregated before valuing
positions. No intrasecond ordering or external reference prices are assumed.
"""
from pathlib import Path
import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
DATA = HERE.parent / "data" / "lighter"
WEEK = 7 * 24 * 3600
STOCKS = {"SPY", "QQQ"}
EXPORT_END = {"A1": "2026-09-27T15:00Z", "A2": "2026-09-27T15:04Z", "A3": "2026-09-27T10:40Z"}


def ts(x):
    return pd.Timestamp(x).tz_convert("UTC") if pd.Timestamp(x).tzinfo else pd.Timestamp(x).tz_localize("UTC")


def seconds(x):
    return ts(x).value / 1e9


def load():
    frames = {}
    for account in EXPORT_END:
        path = next(DATA.glob(account.lower() + "_lighter_export_*"))
        d = pd.read_csv(path)
        d.columns = d.columns.str.strip()
        d["time"] = pd.to_datetime(d["Date"].str.strip(), utc=True)
        for col in ["Market", "Side", "Role", "Type"]:
            d[col] = d[col].str.strip()
        for col in ["Trade Value", "Size", "Price", "Fee", "Closed PnL"]:
            d[col] = pd.to_numeric(d[col].replace("-", np.nan), errors="raise")
        signs = d["Side"].map({"Open Long": 1, "Close Short": 1, "Open Short": -1, "Close Long": -1})
        assert signs.notna().all(), "Unknown side"
        assert not d["Trade ID"].duplicated().any(), "Duplicate trade IDs within account"
        assert d[["Trade Value", "Size", "Price", "Fee"]].notna().all().all()
        d["signed_size"] = d["Size"] * signs
        d["account"] = account
        frames[account] = d.sort_values(["time", "Trade ID"]).reset_index(drop=True)
    return frames


def snapshots():
    p = pd.read_csv(DATA / "points_snapshots.csv")
    p["time"] = pd.to_datetime(p.timestamp_utc, utc=True)
    return p.sort_values(["account", "time"])


class OI:
    def __init__(self, trades, method="shared", add_marks=True):
        """shared: all supplied fills as price proxies; own: own latest fill only.

        Neither price proxy measures the actual continuously changing mark price.
        Position values after export end require a no-further-trades assumption.
        """
        self.trades = trades
        self.curves = {}
        all_fills = pd.concat(trades.values(), ignore_index=True)
        marks = pd.read_csv(DATA / "positions_snapshots.csv")
        marks["time"] = pd.to_datetime(marks.timestamp_utc, utc=True)
        for account, d in trades.items():
            market_curves = {}
            for market, q in d.groupby("Market"):
                qty = q.groupby("time").signed_size.sum().cumsum()
                p = all_fills[all_fills.Market == market] if method == "shared" else q
                agg = p.groupby("time")[["Trade Value", "Size"]].sum()
                prices = agg["Trade Value"] / agg["Size"]
                if add_marks:
                    m = marks[marks.market == market].groupby("time").mark_price.mean()
                    prices = pd.concat([prices, m]).groupby(level=0).last().sort_index()
                grid = qty.index.union(prices.index).sort_values()
                sizes = qty.reindex(grid).ffill().fillna(0).to_numpy()
                values = np.abs(sizes) * prices.reindex(grid).ffill().fillna(0).to_numpy()
                t = grid.asi8.astype(float) / 1e9
                area = np.r_[0., np.cumsum(values[:-1] * np.diff(t))]
                market_curves[market] = (t, sizes, values, area)
            self.curves[account] = market_curves

    def point(self, account, when):
        out = {}
        for market, (t, q, v, a) in self.curves[account].items():
            k = np.searchsorted(t, seconds(when), side="right") - 1
            out[market] = {"size": q[k] if k >= 0 else 0., "value": v[k] if k >= 0 else 0.}
        return out

    def integral(self, account, start, end, group=None):
        def primitive(when, t, v, a):
            u = seconds(when)
            k = np.searchsorted(t, u, side="right") - 1
            return 0. if k < 0 else a[k] + v[k] * (u - t[k])
        total = 0.
        for market, (t, q, v, a) in self.curves[account].items():
            if group == "stock" and market not in STOCKS:
                continue
            if group == "other" and market in STOCKS:
                continue
            total += primitive(end, t, v, a) - primitive(start, t, v, a)
        return total / 1e6 / WEEK


def flow(trades, account, start, end, group=None):
    d = trades[account]
    d = d[(d.time >= ts(start)) & (d.time < ts(end))]
    if group == "stock":
        d = d[d.Market.isin(STOCKS)]
    elif group == "other":
        d = d[~d.Market.isin(STOCKS)]
    return {"volume_m": d["Trade Value"].sum() / 1e6, "fee_usd": d.Fee.sum(), "fills": len(d)}


def intervals(trades, oi):
    rows = []
    for account, p in snapshots().groupby("account"):
        for (_, a), (_, b) in zip(p.iloc[:-1].iterrows(), p.iloc[1:].iterrows()):
            row = {"account": account, "start": a.time, "end": b.time,
                   "hours": (b.time-a.time).total_seconds()/3600,
                   "delta_points": b.points-a.points,
                   "export_covers_end": b.time <= ts(EXPORT_END[account])}
            row.update(flow(trades, account, a.time, b.time))
            row["oi_m_week"] = oi.integral(account, a.time, b.time)
            row["oi_stock_m_week"] = oi.integral(account, a.time, b.time, "stock")
            row["volume_stock_m"] = flow(trades, account, a.time, b.time, "stock")["volume_m"]
            row["a3_oi_m_week"] = oi.integral("A3", a.time, b.time)
            row["a3_volume_m"] = flow(trades, "A3", a.time, b.time)["volume_m"]
            row["naive_live_rate"] = row["delta_points"] / row["oi_m_week"] if row["oi_m_week"] else np.nan
            rows.append(row)
    return pd.DataFrame(rows)
