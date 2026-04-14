"""
Shared chart generation for per-symbol detail CSVs (``<SYMBOL>/<SYMBOL>.csv``).

Writes a single stacked figure (daily returns on top, equity below) used by
``filter_latest_summaries`` and ``generate_all_symbol_graphs``.
"""
from __future__ import annotations

import re
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

RETURN_SIGN_EPS = 1e-12

RETURNS_EQUITY_STACKED_JPEG = "returns_equity_stacked.jpeg"

_ORACLE_HEADER = re.compile("oracle", re.IGNORECASE)


def rename_dataframe_columns_oracle_to_actual(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace the substring ``oracle`` with ``actual`` in column names (e.g. ``equity_oracle``).
    Leaves data values unchanged.
    """
    mapping: dict[str, str] = {}
    for c in df.columns:
        name = str(c)
        if not _ORACLE_HEADER.search(name):
            continue
        mapping[c] = _ORACLE_HEADER.sub("actual", name)
    if not mapping:
        return df
    return df.rename(columns=mapping)


def delete_symbol_folder_jpegs(sym_dir: Path) -> None:
    """Remove ``*.jpg`` / ``*.jpeg`` under ``sym_dir`` so chart outputs do not accumulate stale files."""
    if not sym_dir.is_dir():
        return
    for pattern in ("*.jpg", "*.jpeg", "*.JPG", "*.JPEG"):
        for p in sym_dir.glob(pattern):
            try:
                p.unlink()
            except OSError:
                pass


def _suggestion_position(s: str) -> float:
    u = str(s).strip().upper()
    if u == "BUY":
        return 1.0
    if u == "SHORT":
        return -1.0
    return 0.0


def _direction_day_correct_from_suggestion(suggestion_pred: str, actual_ret: float) -> bool:
    if not np.isfinite(actual_ret):
        return False
    pp = _suggestion_position(suggestion_pred)
    ar = float(actual_ret)
    sp = 0.0 if abs(pp) < RETURN_SIGN_EPS else (1.0 if pp > 0 else -1.0)
    sa = 0.0 if abs(ar) < RETURN_SIGN_EPS else (1.0 if ar > 0 else -1.0)
    return sp == sa


def count_suggestion_direction_match_days(detail: pd.DataFrame) -> tuple[int, int]:
    """
    Count trading days where model **suggestion** side (long/short/flat) matched the **sign**
    of **actual_daily_return** (same rule as green chart shading).
    Returns (correct_days, total_days).
    """
    if "suggestion_pred" not in detail.columns or "actual_daily_return" not in detail.columns:
        return 0, 0
    act = pd.to_numeric(detail["actual_daily_return"], errors="coerce")
    sugg = detail["suggestion_pred"].astype(str)
    n = len(detail)
    ok = 0
    for i in range(n):
        ar = float(act.iloc[i]) if np.isfinite(act.iloc[i]) else float("nan")
        if _direction_day_correct_from_suggestion(str(sugg.iloc[i]), ar):
            ok += 1
    return ok, n


def _add_direction_shading(ax, dts: pd.Series, suggestion_pred: pd.Series, actual_ret: pd.Series) -> None:
    ar = pd.to_numeric(actual_ret, errors="coerce").to_numpy()
    for i in range(len(dts)):
        dt = dts.iloc[i]
        x0 = dt - pd.Timedelta(days=0.45)
        x1 = dt + pd.Timedelta(days=0.45)
        ok = _direction_day_correct_from_suggestion(str(suggestion_pred.iloc[i]), float(ar[i]))
        shade = "#90EE90" if ok else "#FF6B6B"
        ax.axvspan(x0, x1, facecolor=shade, alpha=0.28, linewidth=0, zorder=0)


def _buy_hold_equity_from_actual_returns(equity_predicted: pd.Series, actual_daily_return: pd.Series) -> np.ndarray:
    base = float(pd.to_numeric(equity_predicted.iloc[0], errors="coerce"))
    if not np.isfinite(base):
        base = 100_000.0
    r = pd.to_numeric(actual_daily_return, errors="coerce").fillna(0.0).to_numpy(dtype=float)
    return base * np.cumprod(1.0 + r)


def _load_detail_csv(sym_dir: Path) -> tuple[pd.DataFrame, Path]:
    safe = sym_dir.name
    csv_path = (sym_dir / f"{safe}.csv").resolve()
    if not csv_path.is_file():
        raise FileNotFoundError(f"Missing detail CSV: {csv_path}")
    if csv_path.parent.resolve() != sym_dir.resolve():
        raise ValueError(f"Unexpected detail CSV path: {csv_path}")
    return pd.read_csv(csv_path), csv_path


def write_returns_equity_stacked_jpeg(sym_dir: Path, sym: str) -> None:
    detail, _ = _load_detail_csv(sym_dir)
    need = {
        "DATE",
        "actual_daily_return",
        "predicted_return",
        "equity_predicted",
        "equity_actual",
        "suggestion_pred",
    }
    missing = need - set(detail.columns)
    if missing:
        raise KeyError(f"detail CSV missing columns for stacked chart: {sorted(missing)}")

    dts = pd.to_datetime(detail["DATE"])
    act = pd.to_numeric(detail["actual_daily_return"], errors="coerce")
    pred = pd.to_numeric(detail["predicted_return"], errors="coerce")
    eqp = pd.to_numeric(detail["equity_predicted"], errors="coerce")
    eqa = pd.to_numeric(detail["equity_actual"], errors="coerce")
    sugg = detail["suggestion_pred"]
    buy_hold = _buy_hold_equity_from_actual_returns(eqp, act)

    fig, (ax0, ax1) = plt.subplots(2, 1, sharex=True, figsize=(11, 9), facecolor="white")
    _add_direction_shading(ax0, dts, sugg, act)
    _add_direction_shading(ax1, dts, sugg, act)

    # Passive long daily return is exactly actual_daily_return; blue line is that benchmark.
    ax0.plot(
        dts,
        act * 100.0,
        color="blue",
        linewidth=1.8,
        label="Actual % (passive buy-hold daily)",
        zorder=3,
    )
    ax0.plot(dts, pred * 100.0, color="black", linewidth=1.8, label="Predicted % (model output)", zorder=3)
    ax0.set_ylabel("Return (%)")
    ax0.set_title(f"{sym}: daily returns (passive long vs model)")
    ax0.legend(loc="upper left", fontsize=8)
    ax0.grid(True, alpha=0.25)

    ax1.plot(dts, eqp, color="black", linewidth=1.8, label="Equity predicted", zorder=3)
    ax1.plot(dts, eqa, color="blue", linewidth=1.8, label="Equity actual", zorder=3)
    ax1.plot(
        dts,
        buy_hold,
        color="#CC6600",
        linewidth=1.8,
        label="Buy-hold actual (compound 1+actual_daily_return)",
        zorder=3,
    )
    ax1.set_ylabel("Equity ($)")
    ax1.set_title(f"{sym}: equity — predicted vs actual vs buy-hold")
    ax1.legend(loc="upper left", fontsize=8)
    ax1.grid(True, alpha=0.25)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    fig.autofmt_xdate()
    fig.suptitle(f"{sym}: returns (top) and equity (bottom)", y=1.02)
    fig.tight_layout()
    sym_dir.mkdir(parents=True, exist_ok=True)
    fig.savefig(sym_dir / RETURNS_EQUITY_STACKED_JPEG, format="jpeg", dpi=140, bbox_inches="tight")
    plt.close(fig)


def write_all_symbol_graphs_for_folder(sym_dir: Path, sym: str) -> None:
    """Remove old chart JPEGs, then write ``returns_equity_stacked.jpeg`` only."""
    delete_symbol_folder_jpegs(sym_dir)
    write_returns_equity_stacked_jpeg(sym_dir, sym)
