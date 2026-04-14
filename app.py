#!/usr/bin/env python3
"""
Browse per-symbol JPEG charts and spreadsheets across timestamped backtest runs (Streamlit).

Tabs: **Returns-based accuracy** (leaderboard + long/short/flat note), **Charts** (runs side by side),
**Data tables** (previews stacked vertically). Charts and ``.xlsx`` come from each run’s
``filter/<SYMBOL>/`` or ``by_symbol/<SYMBOL>/``. Pilot sheet uses the repo default workbook
(``filter_latest_summaries``).

  pip install streamlit
  streamlit run filter_graph_viewer.py
"""
from __future__ import annotations

import io
import math
import re
from pathlib import Path

import pandas as pd
import streamlit as st

from filter_latest_summaries import (
    FILTER_SYMBOLS,
    resolve_portfolio_workbook,
    safe_symbol_filename,
)
from symbol_graph_tools import (
    RETURNS_EQUITY_STACKED_JPEG,
    count_suggestion_direction_match_days,
)

RUN_DIR_PATTERN = re.compile(r"^\d{8}_\d{6}$")
REPO_ROOT = Path(__file__).resolve().parent

# Same fills as replicate_client_backtest.write_detail_xlsx_with_row_colors (Excel theme)
_DETAIL_ROW_GREEN = "#C6EFCE"
_DETAIL_ROW_RED = "#FFC7CE"

MOVES_HELP_MD = """
**How suggestions map to sides (same as the backtest):** **Long** (often labeled BUY) means you want
exposure to positive daily moves; **Short** means exposure to negative moves; **Flat** (HOLD / cash)
means no net long or short bet—treated as neutral versus the day’s return sign. A day counts toward
**Days correct** (daily count) when **`suggestion_pred`** matches the sign of **`actual_daily_return`** that day (positive,
negative, or approximately zero = flat band)—same logic as chart shading.
"""

TERMS_GLOSSARY_MD = """
### Glossary: equity, returns, PnL, and paths

**Equity ($).** The simulated **portfolio value in dollars** at the end of each day after the backtest applies position sizing and that day’s returns. It answers: “How many dollars would the account show?”
- **`equity_predicted`**: Value along the **model path**—following **`suggestion_pred`** and **`predicted_return`** (black line on the equity chart).
- **`equity_actual`**: Value along the **realized path** in the export—driven by **`actual_daily_return`** and the rules that produce **`suggestion_actual`** (blue line).

**Daily return.** A **one-day simple return** for the symbol (a small decimal; charts often multiply by 100 to show **%**).
- **`actual_daily_return`**: The **realized** market move used as the benchmark for each date.
- **`predicted_return`**: The **model’s forecast** of that day’s return.

**Total return (%).** **Percent** gain or loss from **starting equity to ending equity** over the whole backtest window (**not annualized**). When computed from equity columns, it is **(last equity ÷ first equity − 1) × 100**.

**Daily PnL ($).** **Dollar change in equity from the prior row to this row** on the same path: **`daily_pnl_predicted`** for the model path, **`daily_pnl_actual`** for the realized path. “PnL” is **profit and loss** for that single day.

**Predicted vs actual columns.** “**Pred**” / **`_*_predicted`** always refers to the **model simulation**; “**actual**” / **`_*_actual`** refers to the **realized benchmark path** in the export (not “ground truth” outside the file—just the labeled path in your data).

**Accuracy.** **Daily prediction accuracy** here means: each **trading day** in the detail CSV, does the **side** implied by **`suggestion_pred`** (long / short / flat) match the **sign** of **`actual_daily_return`**? **Days correct** counts “yes”; **Days total** counts rows; **Daily Prediction Accuracy $%** is 100 × correct ÷ total.
"""

# Header tooltips (Streamlit ``column_config``); hover the ``?`` on each column in ``st.dataframe``.
# Keys must match the ``DataFrame`` column names built in ``build_leaderboard`` / detail CSV headers.
LEADERBOARD_COLUMN_HELP: dict[str, str] = {
    "Symbol": "**Ticker** for the row—the company or instrument whose detail CSV and folder you are viewing. Same string as **`symbol`** in that CSV.",
    "Days correct": "**Daily direction hits:** number of detail rows where the **side** from **`suggestion_pred`** (long / short / flat) matched the **sign** of **`actual_daily_return`** that day (with a small band around zero counted as flat). See glossary: *Accuracy*.",
    "Days total": "**Trading-day count** in the symbol’s detail CSV used as the denominator for **Daily Prediction Accuracy $%**—one row per **`DATE`** in that file.",
    "Daily Prediction Accuracy $%": "**Daily prediction accuracy:** 100 × (**`Days correct`** ÷ **`Days total`**). Measures how often the model’s **discrete suggestion** lined up with the **realized daily return sign**, not dollar PnL quality. The “$%” label is display text; the value is still a **percent**.",
    "Equity pred ($)": "**Equity** = simulated portfolio **value in dollars** on the **model path** (see glossary). This cell is the **last day’s** **`final_equity_predicted`** from ``summary.csv`` if present, else the last **`equity_predicted`** in the detail CSV.",
    "Equity actual ($)": "**Equity** on the **realized path** in the export (glossary: **`equity_actual`**). Last day’s **`final_equity_actual`** from ``summary.csv`` if present, else last **`equity_actual`** in the detail CSV—the same endpoint as the **blue** equity curve.",
    "Return pred (%)": "**Total return** (glossary) on the **predicted** path, expressed as **percent** over the window: from **`total_return_predicted`** in ``summary.csv`` when present (scaled to % in this table), else from first vs last **`equity_predicted`**.",
    "Return actual (%)": "**Total return** on the **realized** path as **percent**: from **`total_return_actual`** in ``summary.csv`` when present, else from first vs last **`equity_actual`**.",
}

_LEADERBOARD_COLS = frozenset(LEADERBOARD_COLUMN_HELP)
assert _LEADERBOARD_COLS == {
    "Symbol",
    "Days correct",
    "Days total",
    "Daily Prediction Accuracy $%",
    "Equity pred ($)",
    "Equity actual ($)",
    "Return pred (%)",
    "Return actual (%)",
}, "LEADERBOARD_COLUMN_HELP keys must match build_leaderboard row_out keys"

DETAIL_BACKTEST_COLUMN_HELP: dict[str, str] = {
    "symbol": "**Ticker**; same meaning as **Symbol** on the leaderboard.",
    "DATE": "Calendar **trading date** for this row (one observation per market day in the export).",
    "actual_daily_return": "**Realized daily simple return** for the symbol that day (decimal form; charts may show ×100). Used with **`suggestion_pred`** to score **daily direction accuracy** (sign match). See glossary: *Daily return*.",
    "predicted_return": "**Model-forecast** daily simple return for this **`DATE`** (same units as **`actual_daily_return`**). Feeds the **`equity_predicted`** path.",
    "same_side_as_actual": "True when **`suggestion_pred`** and **`suggestion_actual`** strings are equal—same rule as **green** vs **red** row shading in the preview.",
    "daily_pnl_diff_actual_minus_model": "**Dollar** difference for this date: **`daily_pnl_actual`** − **`daily_pnl_predicted`**. See glossary: *Daily PnL*.",
    "suggestion_pred": "**Model output** for the day: discrete stance (e.g. BUY = long, SHORT, HOLD = flat) used to build **`equity_predicted`**.",
    "suggestion_actual": "**Realized-path stance** from the export, paired with **`suggestion_pred`** for **`same_side_as_actual`** and row colors.",
    "suggestion_oracle": "Legacy column name; same meaning as **`suggestion_actual`** if your file predates the rename.",
    "daily_pnl_predicted": "**Daily PnL ($):** one-day **dollar** change on the **`equity_predicted`** path (glossary: *Daily PnL*).",
    "daily_pnl_actual": "**Daily PnL ($):** one-day **dollar** change on the **`equity_actual`** path.",
    "equity_predicted": "**Cumulative equity ($)** on the **model simulation** after this date’s move (glossary: *Equity*; black line on the chart).",
    "equity_actual": "**Cumulative equity ($)** on the **realized path** in the file (glossary: *Equity*; blue line).",
}

PILOT_COLUMN_HELP_FALLBACK = (
    "Workbook column **`{col}`** from the pilot / compiled sheet. "
    "For **equity**, **return**, **PnL**, and path definitions, see the **Glossary** on the **Returns-based accuracy** tab. "
    "Open the downloaded `.xlsx` for full workbook context."
)


def _detail_preview_columns(df_or_styler: object) -> pd.Index:
    from pandas.io.formats.style import Styler

    if isinstance(df_or_styler, Styler):
        return df_or_styler.data.columns
    return df_or_styler.columns  # type: ignore[union-attr]


def dataframe_column_tooltips(
    df_or_styler: object,
    help_by_column: dict[str, str],
    *,
    unknown_template: str = "Column `{col}` — values come from the loaded file.",
) -> dict[str, st.column_config.Column]:
    lower = {str(k).lower(): v for k, v in help_by_column.items()}
    out: dict[str, st.column_config.Column] = {}
    for c in _detail_preview_columns(df_or_styler):
        name = str(c)
        h = help_by_column.get(name) or lower.get(name.lower())
        if h is None:
            h = unknown_template.format(col=name)
        out[name] = st.column_config.Column(help=h)
    return out


def arrow_safe_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Coerce ``object`` columns to strings so PyArrow can serialize Streamlit tables
    (fixes mixed str/number cells e.g. pilot sheets with ``'n'`` in a mostly-numeric column).
    """
    if df.empty:
        return df
    out = df.copy()
    for c in out.columns:
        if out[c].dtype != object:
            continue
        s = out[c].map(lambda v: "" if pd.isna(v) else str(v))
        out[c] = s
    return out


def _detail_suggestion_actual_column(df: pd.DataFrame) -> str | None:
    """Realized-side suggestion column: new runs use ``*_actual``, older CSVs ``*_oracle``."""
    if "suggestion_actual" in df.columns:
        return "suggestion_actual"
    if "suggestion_oracle" in df.columns:
        return "suggestion_oracle"
    return None


def style_backtest_detail_preview(df: pd.DataFrame) -> pd.DataFrame | pd.io.formats.style.Styler:
    """
    Row shading matching the downloaded .xlsx: green when ``suggestion_pred`` equals
    ``suggestion_actual`` (or legacy ``suggestion_oracle`` if that column name is still present), else red.
    Falls back to a plain frame if columns are missing.
    """
    act_col = _detail_suggestion_actual_column(df)
    if act_col is None or "suggestion_pred" not in df.columns:
        return df
    match = (
        df["suggestion_pred"].astype(str).str.strip()
        == df[act_col].astype(str).str.strip()
    )

    def row_colors(row: pd.Series) -> list[str]:
        ok = bool(match.loc[row.name])
        bg = f"background-color: {_DETAIL_ROW_GREEN}" if ok else f"background-color: {_DETAIL_ROW_RED}"
        return [bg] * len(row)

    sty = df.style.apply(row_colors, axis=1).hide(axis="index")
    return sty


def default_runs_root() -> Path:
    return (REPO_ROOT / "backtest_runs").resolve()


def list_run_dirs(runs_root: Path) -> list[Path]:
    if not runs_root.is_dir():
        return []
    out: list[Path] = []
    for p in runs_root.iterdir():
        if p.is_dir() and RUN_DIR_PATTERN.match(p.name):
            out.append(p)
    out.sort(key=lambda x: x.name, reverse=True)
    return out


def negative_mode_for_run(run_dir: Path) -> str | None:
    p = run_dir / "summary.csv"
    if not p.is_file():
        return None
    try:
        df = pd.read_csv(p, usecols=["negative_mode"], nrows=100)
    except ValueError:
        df = pd.read_csv(p, nrows=100)
        if "negative_mode" not in df.columns:
            return None
    if df.empty or "negative_mode" not in df.columns:
        return None
    modes = df["negative_mode"].dropna().astype(str).str.strip().str.lower().unique()
    if len(modes) == 0:
        return None
    if len(modes) > 1:
        return "mixed"
    return str(modes[0])


def run_heading(run_dir: Path) -> str:
    mode = negative_mode_for_run(run_dir)
    return f"{run_dir.name} ({mode})" if mode else run_dir.name


def _has_any_chart(sym_dir: Path) -> bool:
    return (sym_dir / RETURNS_EQUITY_STACKED_JPEG).is_file()


def _bases_for_location_mode(mode: str) -> tuple[str, ...]:
    if mode == "filter":
        return ("filter",)
    if mode == "by_symbol":
        return ("by_symbol",)
    return ("filter", "by_symbol")


def symbols_union_for_runs(run_paths: list[Path], location_mode: str) -> list[str]:
    names: set[str] = set()
    for rd in run_paths:
        for base in _bases_for_location_mode(location_mode):
            fd = rd / base
            if not fd.is_dir():
                continue
            for p in fd.iterdir():
                if not p.is_dir():
                    continue
                if _has_any_chart(p) or (p / f"{p.name}.csv").is_file():
                    names.add(p.name)
    return sorted(names)


def resolve_symbol_dir(run: Path, folder_name: str, location_mode: str) -> Path | None:
    if location_mode == "filter":
        order = [run / "filter" / folder_name]
    elif location_mode == "by_symbol":
        order = [run / "by_symbol" / folder_name]
    else:
        order = [run / "filter" / folder_name, run / "by_symbol" / folder_name]
    with_charts = [d for d in order if d.is_dir() and _has_any_chart(d)]
    if with_charts:
        return with_charts[0]
    for d in order:
        if d.is_dir():
            return d
    return None


FILTER_TICKER_SET = {safe_symbol_filename(s).upper() for s in FILTER_SYMBOLS}


def symbols_in_filter_list(all_names: list[str]) -> list[str]:
    return sorted(n for n in all_names if n.upper() in FILTER_TICKER_SET)


def symbols_not_in_filter_list(all_names: list[str]) -> list[str]:
    return sorted(n for n in all_names if n.upper() not in FILTER_TICKER_SET)


def pilot_sheet_df(sym: str) -> pd.DataFrame | None:
    try:
        wb = resolve_portfolio_workbook(REPO_ROOT, None)
    except FileNotFoundError:
        return None
    xl = pd.ExcelFile(wb, engine="openpyxl")
    u = str(sym).strip().upper()
    for name in xl.sheet_names:
        if str(name).strip().upper() == u:
            return pd.read_excel(xl, sheet_name=name, header=0)
    return None


def df_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "sheet") -> bytes:
    buf = io.BytesIO()
    safe_sn = sheet_name[:31] or "sheet"
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=safe_sn)
    return buf.getvalue()


def _load_run_summary_by_symbol(run_dir: Path) -> dict[str, pd.Series]:
    """``symbol`` (upper) -> row from ``summary.csv`` with final equity and total returns."""
    p = run_dir / "summary.csv"
    if not p.is_file():
        return {}
    try:
        sdf = pd.read_csv(p)
    except Exception:
        return {}
    if "symbol" not in sdf.columns:
        return {}
    out: dict[str, pd.Series] = {}
    for _, row in sdf.iterrows():
        key = str(row["symbol"]).strip().upper()
        out[key] = row
    return out


def _float_cell(x: object) -> float | None:
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return None
        v = float(x)
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        return v
    except (TypeError, ValueError):
        return None


def _equity_from_summary_row(row: pd.Series) -> tuple[float | None, float | None, float | None, float | None]:
    """Final equities ($) and total return on initial equity as **percentage** (e.g. 12.5 not 0.125)."""
    fp = _float_cell(row.get("final_equity_predicted"))
    fo = _float_cell(row.get("final_equity_actual"))
    trp = _float_cell(row.get("total_return_predicted"))
    tro = _float_cell(row.get("total_return_actual"))
    if trp is not None:
        trp *= 100.0
    if tro is not None:
        tro *= 100.0
    return fp, fo, trp, tro


def _equity_from_detail_csv(df: pd.DataFrame) -> tuple[float | None, float | None, float | None, float | None]:
    """Last-day equity and total return % from ``equity_predicted`` / ``equity_actual`` columns."""
    if "equity_predicted" not in df.columns or "equity_actual" not in df.columns:
        return None, None, None, None
    ep = pd.to_numeric(df["equity_predicted"], errors="coerce")
    eo = pd.to_numeric(df["equity_actual"], errors="coerce")
    if ep.empty or eo.empty or pd.isna(ep.iloc[0]) or pd.isna(eo.iloc[0]):
        return None, None, None, None
    ini = float(ep.iloc[0])
    if ini == 0.0:
        return None, None, None, None
    fp = _float_cell(ep.iloc[-1])
    fo = _float_cell(eo.iloc[-1])
    if fp is None or fo is None:
        return None, None, None, None
    trp = (fp / ini - 1.0) * 100.0
    tro = (fo / ini - 1.0) * 100.0
    return fp, fo, trp, tro


def _merge_equity_metrics(summary_row: pd.Series | None, detail_df: pd.DataFrame) -> tuple[float | None, float | None, float | None, float | None]:
    fp = fo = trp = tro = None
    if summary_row is not None:
        fp, fo, trp, tro = _equity_from_summary_row(summary_row)
    dfp, dfo, dtrp, dtro = _equity_from_detail_csv(detail_df)
    fp = fp if fp is not None else dfp
    fo = fo if fo is not None else dfo
    trp = trp if trp is not None else dtrp
    tro = tro if tro is not None else dtro
    return fp, fo, trp, tro


@st.cache_data(ttl=120)
def build_leaderboard(run_name: str, runs_root_str: str, loc_key: str, sym_filter: str) -> pd.DataFrame:
    """
    sym_filter: ``all`` | ``in_filter`` | ``not_in_filter`` (matches sidebar symbol group).

    Equity and return % come from the run ``summary.csv`` when present, else from the last row
    of each symbol detail CSV (same definitions as the stacked equity chart).
    """
    run = Path(runs_root_str) / run_name
    syms = symbols_union_for_runs([run], loc_key)
    summary_by_sym = _load_run_summary_by_symbol(run)
    rows: list[dict[str, object]] = []
    for sym in syms:
        u = sym.upper()
        if sym_filter == "in_filter" and u not in FILTER_TICKER_SET:
            continue
        if sym_filter == "not_in_filter" and u in FILTER_TICKER_SET:
            continue
        sd = resolve_symbol_dir(run, sym, loc_key)
        if sd is None:
            continue
        csvp = sd / f"{sd.name}.csv"
        if not csvp.is_file():
            continue
        df = pd.read_csv(csvp)
        c, t = count_suggestion_direction_match_days(df)
        pct = round(100.0 * float(c) / float(t), 2) if t else 0.0
        srow = summary_by_sym.get(u)
        fp, fo, trp, tro = _merge_equity_metrics(srow, df)
        row_out: dict[str, object] = {
            "Symbol": sym,
            "Days correct": int(c),
            "Days total": int(t),
            "Daily Prediction Accuracy $%": pct,
            "Equity pred ($)": round(fp, 2) if fp is not None else math.nan,
            "Equity actual ($)": round(fo, 2) if fo is not None else math.nan,
            "Return pred (%)": round(trp, 2) if trp is not None else math.nan,
            "Return actual (%)": round(tro, 2) if tro is not None else math.nan,
        }
        rows.append(row_out)
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(["Days correct", "Daily Prediction Accuracy $%"], ascending=[False, False]).reset_index(
        drop=True
    )


def main() -> None:
    st.set_page_config(page_title="Backtest graph viewer", layout="wide")
    st.title("Backtest graph viewer")
    st.caption("Charts and workbooks under each run’s `filter/<SYMBOL>/` and/or `by_symbol/<SYMBOL>/`.")

    with st.sidebar:
        runs_root = Path(
            st.text_input("Backtest runs folder", value=str(default_runs_root()))
        ).expanduser().resolve()
        runs = list_run_dirs(runs_root)
        if not runs:
            st.error(f"No timestamped run folders under {runs_root}")
            st.stop()

        default_pick = [p.name for p in runs[:2]]

        def _run_label(name: str) -> str:
            return run_heading(runs_root / name)

        run_names = st.multiselect(
            "Runs to compare",
            options=[p.name for p in runs],
            default=default_pick,
            format_func=_run_label,
        )
        if not run_names:
            st.warning("Pick at least one run.")
            st.stop()

        location_mode = st.radio(
            "Chart folder",
            options=("Prefer filter, else by_symbol", "filter only", "by_symbol only"),
            index=0,
        )
        loc_key = (
            "both"
            if location_mode.startswith("Prefer")
            else ("filter" if location_mode.startswith("filter") else "by_symbol")
        )

        run_paths = [runs_root / n for n in run_names]
        all_syms = symbols_union_for_runs(run_paths, loc_key)
        if not all_syms:
            st.error("No symbol subfolders found for the selected runs and folder mode.")
            st.stop()

        symbol_group = st.radio(
            "Symbols",
            options=(
                "In filter list",
                "Not in filter list",
            ),
            horizontal=True,
        )
        if symbol_group == "In filter list":
            filtered_syms = symbols_in_filter_list(all_syms)
        else:
            filtered_syms = symbols_not_in_filter_list(all_syms)
        if not filtered_syms:
            st.error("No symbols in this group for the selected runs (try the other toggle).")
            st.stop()

        symbol = st.selectbox("Symbol", options=filtered_syms)

        sym_filter_key = "in_filter" if symbol_group == "In filter list" else "not_in_filter"

    tab_acc, tab_charts, tab_tables = st.tabs(
        ["Returns-based accuracy", "Charts", "Data tables"],
    )

    with tab_acc:
        st.markdown(MOVES_HELP_MD)
        st.markdown(TERMS_GLOSSARY_MD)
        st.markdown(
            "Per-symbol **daily direction accuracy**: trading days where **`suggestion_pred`** matched the sign of "
            "**`actual_daily_return`** in the detail CSV — same rule as green/red shading on the charts. "
            "**Equity** columns are **dollar** portfolio values; **Return** columns are **whole-window total return in %** "
            "(not annualized), aligned with ``summary.csv`` when those fields exist."
        )
        for i, rd in enumerate(run_paths):
            st.subheader(run_heading(rd))
            lb = build_leaderboard(rd.name, str(runs_root.resolve()), loc_key, sym_filter_key)
            if lb.empty:
                st.info("No symbol CSVs found for this run.")
            else:
                st.metric("Symbols in table", len(lb))
                st.caption(
                    "Sorted by **Days correct**, then **Daily Prediction Accuracy $%**. "
                    "**Equity pred ($)** / **Equity actual ($)** are **last-day portfolio value in dollars** on the "
                    "model vs realized path (see glossary). **Return pred (%)** / **Return actual (%)** are **total return "
                    "over the run in percent** (not annualized). Data source: ``summary.csv`` fields "
                    "**`final_equity_predicted`**, **`final_equity_actual`**, **`total_return_predicted`**, "
                    "**`total_return_actual`** when present; otherwise last **`equity_predicted`** / **`equity_actual`** "
                    "in each symbol’s detail CSV. Hover the **?** on each column header for the same definitions in short form."
                )
                st.dataframe(
                    arrow_safe_dataframe(lb),
                    width="stretch",
                    hide_index=True,
                    column_config=dataframe_column_tooltips(lb, LEADERBOARD_COLUMN_HELP),
                )
            if i < len(run_paths) - 1:
                st.divider()

    with tab_charts:
        st.caption(
            "Compare runs **side by side**: each row is one chart type; columns are the selected runs. "
            "Top subplot: **daily returns** (**`actual_daily_return`** vs **`predicted_return`**, scaled to %). "
            "Bottom subplot: **equity** in **$** (**`equity_predicted`** black, **`equity_actual`** blue, buy-hold orange). "
            "For **long / short / flat** and the full **glossary** (equity, total return, PnL), see **Returns-based accuracy**."
        )
        sym_dirs: list[Path | None] = [
            resolve_symbol_dir(rd, symbol, loc_key) for rd in run_paths
        ]
        header_cols = st.columns(len(run_paths))
        for col, rd, sd in zip(header_cols, run_paths, sym_dirs):
            with col:
                st.markdown(f"**{run_heading(rd)}**")
                if sd is None:
                    st.warning("No symbol folder.")
                else:
                    try:
                        st.caption(str(sd.relative_to(runs_root)))
                    except ValueError:
                        st.caption(str(sd))

        for label, fname in (("Returns + equity (stacked)", RETURNS_EQUITY_STACKED_JPEG),):
            st.markdown(f"**{label}**")
            img_cols = st.columns(len(run_paths))
            for img_col, rd, sd in zip(img_cols, run_paths, sym_dirs):
                with img_col:
                    if sd is None:
                        st.empty()
                        continue
                    path = sd / fname
                    if path.is_file():
                        st.image(str(path), width="stretch")
                    else:
                        st.info(f"Missing `{fname}`")

    with tab_tables:
        st.caption(
            "Tables are **top to bottom**: backtest detail per run, then pilot. "
            "**Equity** = dollar portfolio value along a path; **returns** in the detail sheet are **daily** simple returns; "
            "**PnL** columns are **dollar day-over-day** changes. For full definitions, open the glossary below."
        )
        with st.expander("Glossary: equity, returns, PnL, predicted vs actual", expanded=False):
            st.markdown(TERMS_GLOSSARY_MD)
        st.markdown(
            "Spreadsheet previews: one block per run, then the pilot workbook at the bottom. "
            "Hover the **?** on each column header for column-specific notes (aligned with the glossary)."
        )
        for i, rd in enumerate(run_paths):
            st.subheader(run_heading(rd))
            sym_dir = resolve_symbol_dir(rd, symbol, loc_key)
            if sym_dir is None:
                st.warning(f"No folder for `{symbol}` under filter/ or by_symbol/.")
                if i < len(run_paths) - 1:
                    st.divider()
                continue
            try:
                st.caption(str(sym_dir.relative_to(runs_root)))
            except ValueError:
                st.caption(str(sym_dir))

            xlsx_bt = sym_dir / f"{sym_dir.name}.xlsx"
            st.markdown("**Backtest detail** (`<SYMBOL>.xlsx`)")
            if xlsx_bt.is_file():
                st.download_button(
                    label=f"Download {xlsx_bt.name}",
                    data=xlsx_bt.read_bytes(),
                    file_name=xlsx_bt.name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"dl-bt-{rd.name}-{symbol}",
                )
                st.caption(
                    "Row colors match the downloaded .xlsx: green when **`suggestion_pred`** equals **`suggestion_actual`**, "
                    "red otherwise (Excel theme: light green / light red fills). "
                    "**Equity** = cumulative **$** path; **daily PnL** = one-day **$** change on that path; "
                    "**daily returns** are **`predicted_return`** vs **`actual_daily_return`**. "
                    "Hover the **?** on each column header for definitions."
                )
                try:
                    dfb = pd.read_excel(xlsx_bt, engine="openpyxl")
                    preview = style_backtest_detail_preview(arrow_safe_dataframe(dfb))
                    st.dataframe(
                        preview,
                        height=320,
                        width="stretch",
                        column_config=dataframe_column_tooltips(
                            preview,
                            DETAIL_BACKTEST_COLUMN_HELP,
                        ),
                    )
                except Exception as e:
                    st.warning(f"Preview failed: {e}")
            else:
                st.caption(f"No `{xlsx_bt.name}` in this folder.")

            if i < len(run_paths) - 1:
                st.divider()

        st.subheader("Pilot compiled workbook")
        st.caption("Same pilot sheet for every run (repo default workbook).")
        pilot_df = pilot_sheet_df(symbol)
        if pilot_df is not None and not pilot_df.empty:
            st.download_button(
                label=f"Download {symbol}_pilot_sheet.xlsx",
                data=df_to_excel_bytes(pilot_df, sheet_name=symbol),
                file_name=f"{symbol}_pilot_sheet.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"dl-pilot-{symbol}",
            )
            st.markdown("**Pilot sheet preview**")
            st.caption(
                "This grid is read as plain data; any Excel-only formatting in the source "
                "workbook is not shown in Streamlit. Open the download in Excel for full styling. "
                "Hover the **?** on each column header for definitions where available."
            )
            st.dataframe(
                arrow_safe_dataframe(pilot_df),
                height=360,
                width="stretch",
                column_config=dataframe_column_tooltips(
                    pilot_df,
                    {},
                    unknown_template=PILOT_COLUMN_HELP_FALLBACK,
                ),
            )
        else:
            st.caption("No matching sheet in the default pilot workbook (see filter_latest_summaries).")


if __name__ == "__main__":
    main()
