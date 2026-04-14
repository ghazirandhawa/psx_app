#!/usr/bin/env python3
"""
Data sources
------------
* **Summary row filter + portfolio column on summary_filtered_*.csv:** reads each run’s
  ``<run>/summary.csv`` at the run root, then the compiled workbook (``--portfolio-xlsx``) only
  to attach the scalar ``portfolio`` column per symbol for that summary file.
* **Charts (JPEG):** implemented in ``symbol_graph_tools``; reads only
  ``<run>/filter/<SYMBOL>/<SYMBOL>.csv``. Writes ``returns_equity_stacked.jpeg`` (returns + equity incl. buy-hold).

For each of the two most recent timestamped runs under the output base, read summary.csv,
filter rows to a fixed ticker list, merge portfolio from the client Excel (per-symbol sheet),
and write <run_folder>/filter/summary_filtered_<flat|short>.csv (suffix from negative_mode).

For each filtered symbol, moves <run_folder>/by_symbol/<SYMBOL>/ into <run_folder>/filter/<SYMBOL>/
(same safe folder names as replicate_client_backtest).

After that, writes under each filter/<SYMBOL>/ (data from that folder’s ``<SYMBOL>.csv`` only):
  returns_equity_stacked.jpeg (see symbol_graph_tools; old ``*.jpg``/``*.jpeg`` in that folder are removed first).

Run folders are subdirectories named YYYYMMDD_HHMMSS (same convention as replicate_client_backtest).

The workbook "compiled predictions vs actuals wide V 1.0.xlsx" (or --portfolio-xlsx) is read:
each symbol's sheet must contain a column named portfolio (case-insensitive). The last non-null
numeric value in that column is used as the scalar for that symbol on the **filtered summary CSV only**.
"""
from __future__ import annotations

import argparse
import re
import shutil
from pathlib import Path

import openpyxl
import pandas as pd

from symbol_graph_tools import (
    rename_dataframe_columns_oracle_to_actual,
    write_all_symbol_graphs_for_folder,
)

RUN_DIR_PATTERN = re.compile(r"^\d{8}_\d{6}$")
_ORACLE_IN_HEADER = re.compile("oracle", re.IGNORECASE)

# Client pilot workbook shipped in this repo (repo root, next to this script).
DEFAULT_PORTFOLIO_XLSX = "compiled predictions vs actuals wide V 1.0.xlsx"

FILTER_SYMBOLS = frozenset(
    {
        "ATRL",
        "AVN",
        "BOK",
        "BOP",
        "BWCL",
        "CHCC",
        "CNERGY",
        "COLG",
        "CPHL",
        "DCR",
        "DKGC",
        "EFERT",
        "FABL",
        "FATIMA",
        "BERG",
        "BFMOD",
        "BIFO",
        "BIPL",
        "BML",
        "BNL",
        "BNWM",
        "BAFL",
        "BAHL",
        "BAPL",
        "BATA",
        "BBFL",
        "BCL",
        "BECO",
        "BELA",
    }
)


def safe_symbol_filename(symbol: str) -> str:
    """Match replicate_client_backtest.safe_symbol_filename for by_symbol folder names."""
    s = re.sub(r'[<>:"/\\|?*\[\]]+', "_", symbol.strip())
    s = s.strip("._") or "UNKNOWN"
    return s


def symbol_column(df: pd.DataFrame) -> str:
    for c in df.columns:
        if str(c).upper() == "SYMBOL":
            return c
    raise KeyError("No SYMBOL/symbol column in summary CSV")


def normalize_run_folder_csvs(run_dir: Path) -> int:
    """
    Rewrite every ``*.csv`` under ``run_dir`` whose headers contain ``oracle`` (case-insensitive),
    replacing that substring with ``actual`` in column names only.
    """
    n = 0
    if not run_dir.is_dir():
        return 0
    for path in sorted(run_dir.rglob("*.csv")):
        try:
            df = pd.read_csv(path)
        except Exception:
            continue
        fixed = rename_dataframe_columns_oracle_to_actual(df)
        if list(fixed.columns) == list(df.columns):
            continue
        fixed.to_csv(path, index=False)
        n += 1
    return n


def normalize_run_folder_xlsx_headers(run_dir: Path) -> int:
    """
    For each ``*.xlsx`` under ``run_dir``, replace ``oracle`` with ``actual`` in **row 1** cell text
    (column headers). Other rows and formatting are left unchanged.
    """
    n = 0
    if not run_dir.is_dir():
        return 0
    for path in sorted(run_dir.rglob("*.xlsx")):
        try:
            wb = openpyxl.load_workbook(path)
        except Exception:
            continue
        changed = False
        try:
            for ws in wb.worksheets:
                for cell in ws[1]:
                    v = cell.value
                    if v is None or not isinstance(v, str):
                        continue
                    if not _ORACLE_IN_HEADER.search(v):
                        continue
                    cell.value = _ORACLE_IN_HEADER.sub("actual", v)
                    changed = True
            if changed:
                wb.save(path)
                n += 1
        except Exception:
            pass
        finally:
            try:
                wb.close()
            except Exception:
                pass
    return n


def latest_run_dirs(output_base: Path, n: int = 2) -> list[Path]:
    if not output_base.is_dir():
        raise FileNotFoundError(f"Output base not found: {output_base}")
    candidates: list[tuple[str, Path]] = []
    for p in output_base.iterdir():
        if p.is_dir() and RUN_DIR_PATTERN.match(p.name):
            candidates.append((p.name, p))
    candidates.sort(key=lambda x: x[0], reverse=True)
    if len(candidates) < n:
        raise RuntimeError(
            f"Need at least {n} timestamped run folders under {output_base}; found {len(candidates)}"
        )
    return [p for _, p in candidates[:n]]


def _portfolio_col_name(df: pd.DataFrame) -> str | None:
    for c in df.columns:
        if str(c).strip().lower() == "portfolio":
            return c
    return None


def _last_numeric_portfolio(df: pd.DataFrame) -> float | None:
    col = _portfolio_col_name(df)
    if col is None:
        return None
    s = pd.to_numeric(df[col], errors="coerce")
    s = s.dropna()
    if s.empty:
        return None
    return float(s.iloc[-1])


def _sheet_name_for_symbol(xl: pd.ExcelFile, sym: str) -> str | None:
    u = str(sym).strip().upper()
    for name in xl.sheet_names:
        if str(name).strip().upper() == u:
            return name
    return None


def load_portfolio_by_symbol(xlsx_path: Path, symbols: frozenset[str]) -> dict[str, float]:
    out: dict[str, float] = {}
    missing_sheets: list[str] = []
    missing_col: list[str] = []
    empty_portfolio: list[str] = []

    with pd.ExcelFile(xlsx_path, engine="openpyxl") as xl:
        for sym in sorted(symbols):
            sh = _sheet_name_for_symbol(xl, sym)
            if sh is None:
                missing_sheets.append(sym)
                continue
            raw = pd.read_excel(xl, sheet_name=sh, header=0)
            if _portfolio_col_name(raw) is None:
                missing_col.append(sym)
                continue
            val = _last_numeric_portfolio(raw)
            if val is None:
                empty_portfolio.append(sym)
                continue
            out[sym] = val

    if missing_sheets:
        print(
            f"Warning: {len(missing_sheets)} symbol(s) have no matching sheet in {xlsx_path.name} "
            "(sheet name must equal the ticker)."
        )
    if missing_col:
        print(f"Warning: {len(missing_col)} sheet(s) have no 'portfolio' column.")
    if empty_portfolio:
        print(f"Warning: {len(empty_portfolio)} sheet(s) have no numeric portfolio values.")

    return out


def resolve_portfolio_workbook(root: Path, explicit: Path | None) -> Path:
    if explicit is not None:
        p = explicit.expanduser().resolve()
        if not p.is_file():
            raise FileNotFoundError(f"Portfolio workbook not found: {p}")
        return p
    primary = (root / DEFAULT_PORTFOLIO_XLSX).resolve()
    if primary.is_file():
        return primary
    matches = list(root.glob("*compiled*predictions*actuals*wide*.xlsx"))
    if matches:
        return max(matches, key=lambda p: p.stat().st_mtime)
    raise FileNotFoundError(
        f"Portfolio workbook not found at {primary}. "
        "Pass --portfolio-xlsx with the correct path."
    )


def negative_mode_suffix(df: pd.DataFrame) -> str:
    if "negative_mode" not in df.columns:
        raise KeyError("summary.csv must include negative_mode for output filename suffix")
    modes = df["negative_mode"].dropna().astype(str).str.strip().str.lower().unique()
    if len(modes) != 1:
        raise ValueError(f"Expected one negative_mode per run; got {sorted(modes)!r}")
    tag = modes[0]
    if tag not in ("flat", "short"):
        raise ValueError(f"negative_mode must be 'flat' or 'short' for filename suffix; got {tag!r}")
    return tag


def round_numeric_two_decimals(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        s = out[c]
        if pd.api.types.is_bool_dtype(s):
            continue
        if pd.api.types.is_datetime64_any_dtype(s):
            continue
        if s.dtype == object:
            continue
        if pd.api.types.is_numeric_dtype(s):
            out[c] = s.round(2)
    return out


def insert_portfolio_between_equities(df: pd.DataFrame) -> pd.DataFrame:
    """Order: final_equity_predicted, portfolio, final_equity_actual (then rest unchanged)."""
    if "portfolio" not in df.columns:
        return df
    pred, act = "final_equity_predicted", "final_equity_actual"
    cols = [c for c in df.columns if c != "portfolio"]
    if pred in cols and act in cols:
        i = cols.index(pred)
        cols = cols[: i + 1] + ["portfolio"] + cols[i + 1 :]
    else:
        cols.append("portfolio")
    return df[cols]


def write_charts_for_filtered_run(filter_root: Path, symbols: frozenset[str]) -> None:
    for sym in sorted(symbols):
        sym_dir = filter_root / safe_symbol_filename(sym)
        if not sym_dir.is_dir():
            continue
        try:
            write_all_symbol_graphs_for_folder(sym_dir, sym.upper())
        except Exception as e:
            print(f"Warning: charts failed for {sym}: {type(e).__name__}: {e}")


def move_filtered_by_symbol_folders(
    run_path: Path,
    symbols: frozenset[str],
    by_symbol_subdir: str,
) -> None:
    by_root = run_path / by_symbol_subdir
    filt = run_path / "filter"
    if not by_root.is_dir():
        return
    filt.mkdir(parents=True, exist_ok=True)
    moved = 0
    absent = 0
    for sym in symbols:
        name = safe_symbol_filename(sym)
        src = by_root / name
        if not src.is_dir():
            absent += 1
            continue
        dst = filt / name
        if dst.exists():
            shutil.rmtree(dst)
        shutil.move(str(src), str(dst))
        moved += 1
    if moved:
        print(f"{run_path.name}: moved {moved} folder(s) from {by_symbol_subdir}/ to filter/")
    if absent:
        print(
            f"{run_path.name}: {absent} filtered ticker(s) had no {by_symbol_subdir}/<name>/ directory "
            "(skipped; older runs may only have <name>.csv files there)."
        )


def main() -> None:
    root = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser(description="Filter summaries from latest two runs into each run's filter/ folder.")
    ap.add_argument(
        "--output-dir",
        type=Path,
        default=root / "backtest_runs",
        help="Base folder containing timestamped run subfolders (default: <repo>/backtest_runs).",
    )
    ap.add_argument(
        "--summary-filename",
        type=str,
        default="summary.csv",
        help="Summary file name inside each run folder.",
    )
    ap.add_argument(
        "--out-csv",
        type=str,
        default="summary_filtered.csv",
        help="Base output name under <each-run-folder>/filter/; _flat or _short is inserted before .csv.",
    )
    ap.add_argument(
        "--portfolio-xlsx",
        type=Path,
        default=None,
        help="Excel workbook with one sheet per symbol and a 'portfolio' column. "
        f"Default: <repo>/{DEFAULT_PORTFOLIO_XLSX}",
    )
    ap.add_argument(
        "--by-symbol-subdir",
        type=str,
        default="by_symbol",
        help="Per-symbol detail parent folder inside each run (default: by_symbol).",
    )
    ap.add_argument(
        "--no-filter-graphs",
        action="store_true",
        help="Skip JPEG charts under filter/<SYMBOL>/.",
    )
    ap.add_argument(
        "--normalize-oracle-headers-only",
        action="store_true",
        help="Under each timestamped run: replace oracle->actual in CSV column names and in row 1 of "
        "each .xlsx, then exit.",
    )
    args = ap.parse_args()

    output_base = args.output_dir.resolve()
    if args.normalize_oracle_headers_only:
        if not output_base.is_dir():
            raise FileNotFoundError(f"Output base not found: {output_base}")
        total_files = 0
        run_paths = sorted(
            (p for p in output_base.iterdir() if p.is_dir() and RUN_DIR_PATTERN.match(p.name)),
            key=lambda x: x.name,
            reverse=True,
        )
        total_xlsx = 0
        for run_path in run_paths:
            n_csv = normalize_run_folder_csvs(run_path)
            n_xlsx = normalize_run_folder_xlsx_headers(run_path)
            if n_csv or n_xlsx:
                print(f"{run_path.name}: {n_csv} CSV, {n_xlsx} XLSX")
            total_files += n_csv
            total_xlsx += n_xlsx
        print(f"Done. CSV header fixes: {total_files} file(s); XLSX header row fixes: {total_xlsx} file(s).")
        return

    portfolio_path = resolve_portfolio_workbook(root, args.portfolio_xlsx)
    portfolio_map = load_portfolio_by_symbol(portfolio_path, FILTER_SYMBOLS)

    run_dirs = latest_run_dirs(output_base, n=2)

    for run_path in run_dirs:
        n_csv = normalize_run_folder_csvs(run_path)
        n_xlsx = normalize_run_folder_xlsx_headers(run_path)
        if n_csv or n_xlsx:
            print(
                f"{run_path.name}: normalized `oracle` -> `actual` in {n_csv} CSV header(s) "
                f"and {n_xlsx} XLSX workbook(s)"
            )
        csv_path = run_path / args.summary_filename
        if not csv_path.is_file():
            raise FileNotFoundError(f"Missing {csv_path}")
        df = pd.read_csv(csv_path)
        sym_col = symbol_column(df)
        mask = df[sym_col].astype(str).str.upper().isin(FILTER_SYMBOLS)
        filtered = df.loc[mask].copy()

        sym_u = filtered[sym_col].astype(str).str.upper()
        filtered["portfolio"] = sym_u.map(lambda s: portfolio_map.get(s, float("nan")))

        filtered = insert_portfolio_between_equities(filtered)

        mode_tag = negative_mode_suffix(filtered)
        filtered = round_numeric_two_decimals(filtered)

        out_dir = run_path / "filter"
        out_dir.mkdir(parents=True, exist_ok=True)
        base_out = Path(args.out_csv)
        out_path = out_dir / f"{base_out.stem}_{mode_tag}{base_out.suffix}"
        filtered.to_csv(out_path, index=False)

        symbols_in_run = frozenset(filtered[sym_col].astype(str).str.upper())
        move_filtered_by_symbol_folders(run_path, symbols_in_run, args.by_symbol_subdir)

        if not args.no_filter_graphs:
            write_charts_for_filtered_run(out_dir, symbols_in_run)


if __name__ == "__main__":
    main()
