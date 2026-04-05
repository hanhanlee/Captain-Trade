"""
持股資料匯入 / 匯出 helper

提供：
1. 標準欄位匯出 DataFrame
2. CSV 編碼容錯讀取（UTF-8 / UTF-8-SIG / CP950 / BIG5）
3. 券商 CSV 欄位關鍵字偵測與標準化
"""
from __future__ import annotations

from io import BytesIO
import re
import pandas as pd


STANDARD_COLUMNS = [
    "stock_id",
    "stock_name",
    "shares",
    "cost_price",
    "stop_loss",
    "take_profit",
    "notes",
]


def holdings_to_export_df(holdings: list[dict]) -> pd.DataFrame:
    """將持股清單轉成系統標準匯出格式。"""
    rows = []
    for h in holdings:
        rows.append({
            "stock_id": str(h.get("stock_id", "")).strip(),
            "stock_name": h.get("stock_name", "") or "",
            "shares": int(h.get("shares", 0) or 0),
            "cost_price": float(h.get("cost_price", 0) or 0),
            "stop_loss": h.get("stop_loss"),
            "take_profit": h.get("take_profit"),
            "notes": h.get("notes", "") or "",
        })
    return pd.DataFrame(rows, columns=STANDARD_COLUMNS)


def read_csv_with_fallback(file_bytes: bytes) -> tuple[pd.DataFrame, str]:
    """
    以常見台股 CSV 編碼順序嘗試讀取。
    優先讀 UTF-8，其次 CP950 / BIG5。
    """
    encodings = ["utf-8-sig", "utf-8", "cp950", "big5"]
    last_error = None
    for enc in encodings:
        try:
            # 先全部以字串讀入，避免股票代碼前導零被 pandas 當數字吃掉。
            df = pd.read_csv(BytesIO(file_bytes), encoding=enc, dtype=str)
            return df, enc
        except UnicodeDecodeError as exc:
            last_error = exc
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"CSV 讀取失敗，請確認檔案格式。最後錯誤：{last_error}")


def parse_holdings_csv(file_bytes: bytes) -> tuple[pd.DataFrame, dict]:
    """
    將上傳 CSV 解析成系統標準欄位。

    支援：
    - 系統原生匯出格式
    - 券商欄位容錯格式（股票/代碼/股名/餘股數/成本價...）
    """
    raw_df, encoding = read_csv_with_fallback(file_bytes)
    parsed_df, mapping = normalize_holdings_df(raw_df)
    meta = {
        "encoding": encoding,
        "mapping": mapping,
        "row_count": len(parsed_df),
    }
    return parsed_df, meta


def normalize_holdings_df(raw_df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    將任意 CSV 欄位標準化為系統 schema。
    """
    df = raw_df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    col_map = detect_holding_columns(df.columns.tolist())
    if not col_map.get("stock_id") or not col_map.get("shares") or not col_map.get("cost_price"):
        raise ValueError("找不到必要欄位：至少需要股票代碼、股數、成本價。")

    normalized = pd.DataFrame(index=df.index)
    normalized["stock_id"] = _clean_stock_id_series(df[col_map["stock_id"]])
    normalized["stock_name"] = _clean_text_series(df[col_map["stock_name"]]) if col_map.get("stock_name") else ""
    normalized["shares"] = _to_int_series(df[col_map["shares"]])
    normalized["cost_price"] = _to_float_series(df[col_map["cost_price"]])
    normalized["stop_loss"] = _to_float_series(df[col_map["stop_loss"]]) if col_map.get("stop_loss") else pd.NA
    normalized["take_profit"] = _to_float_series(df[col_map["take_profit"]]) if col_map.get("take_profit") else pd.NA
    normalized["notes"] = _clean_text_series(df[col_map["notes"]]) if col_map.get("notes") else ""

    # 移除空白列；保留缺失 stop_loss / take_profit 供使用者在 data_editor 補值。
    normalized = normalized.loc[
        normalized["stock_id"].ne("") | normalized["stock_name"].ne("")
    ].copy()

    return normalized.reindex(columns=STANDARD_COLUMNS), col_map


def detect_holding_columns(columns: list[str]) -> dict:
    """依欄位關鍵字偵測對應標準欄位。"""
    mapping = {}
    normalized_cols = {col: _normalize_col_name(col) for col in columns}
    used_cols: set[str] = set()

    # 先找最不歧義的欄位，避免「股票名稱」被誤判成 stock_id。
    keyword_map = [
        ("stock_name", ["stock_name", "股名", "股票名稱", "名稱", "商品名稱"]),
        ("stock_id", ["stock_id", "股票代碼", "股票代號", "代碼", "代號", "商品代號", "股票"]),
        ("shares", ["shares", "餘股數", "持股股數", "庫存股數", "股數", "成交股數"]),
        ("cost_price", ["cost_price", "成本價", "平均成本", "庫存均價", "買進均價", "均價"]),
        ("stop_loss", ["stop_loss", "停損價", "停損"]),
        ("take_profit", ["take_profit", "停利價", "停利"]),
        ("notes", ["notes", "note", "備註", "註記"]),
    ]

    for target, keywords in keyword_map:
        for raw_col, norm_col in normalized_cols.items():
            if raw_col in used_cols:
                continue
            if any(keyword in norm_col for keyword in keywords):
                mapping[target] = raw_col
                used_cols.add(raw_col)
                break

    return mapping


def validate_holdings_df(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """
    驗證 data_editor 回傳內容並整理型別，回傳 (clean_df, errors)。
    """
    work = df.copy()
    for col in STANDARD_COLUMNS:
        if col not in work.columns:
            work[col] = pd.NA
    work = work.reindex(columns=STANDARD_COLUMNS)

    work["stock_id"] = _clean_stock_id_series(work["stock_id"])
    work["stock_name"] = _clean_text_series(work["stock_name"])
    work["shares"] = _to_int_series(work["shares"])
    work["cost_price"] = _to_float_series(work["cost_price"])
    work["stop_loss"] = _to_float_series(work["stop_loss"])
    work["take_profit"] = _to_float_series(work["take_profit"])
    work["notes"] = _clean_text_series(work["notes"])

    work = work.loc[work["stock_id"].ne("")].copy()

    errors: list[str] = []
    if work.empty:
        errors.append("沒有可匯入的持股資料。")
        return work, errors

    missing_required = work["shares"].isna() | work["cost_price"].isna()
    if missing_required.any():
        bad_ids = "、".join(work.loc[missing_required, "stock_id"].astype(str).head(5))
        errors.append(f"以下股票缺少必要欄位（股數/成本價）：{bad_ids}")

    non_positive = (work["shares"].fillna(0) <= 0) | (work["cost_price"].fillna(0) <= 0)
    if non_positive.any():
        bad_ids = "、".join(work.loc[non_positive, "stock_id"].astype(str).head(5))
        errors.append(f"以下股票的股數或成本價不合法：{bad_ids}")

    dup_ids = work["stock_id"][work["stock_id"].duplicated()].unique().tolist()
    if dup_ids:
        errors.append(f"匯入資料中有重複股票代碼：{'、'.join(dup_ids[:5])}")

    return work, errors


def _normalize_col_name(col: str) -> str:
    return str(col).strip().lower().replace(" ", "").replace("_", "")


def _clean_text_series(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip()


def _clean_stock_id_series(series: pd.Series) -> pd.Series:
    cleaned = series.fillna("").astype(str).str.strip()
    cleaned = cleaned.str.replace(r"^='?", "", regex=True)
    cleaned = cleaned.str.replace(r"'$", "", regex=True)
    cleaned = cleaned.str.replace(r"\.0$", "", regex=True)
    cleaned = cleaned.str.extract(r"([A-Za-z0-9]+)", expand=False).fillna("")
    return cleaned.str.upper()


def _to_float_series(series: pd.Series) -> pd.Series:
    cleaned = (
        series.fillna("")
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("%", "", regex=False)
        .str.strip()
    )
    cleaned = cleaned.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
    return pd.to_numeric(cleaned, errors="coerce")


def _to_int_series(series: pd.Series) -> pd.Series:
    num = _to_float_series(series)
    return num.round().astype("Int64")
