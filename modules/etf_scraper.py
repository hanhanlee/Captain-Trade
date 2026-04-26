"""
ETF 成分股爬蟲統一介面

各投信對應關係：
  元大投信 (Yuanta)  : 0050, 0056, 006208 — 支援指定日期
  統一投信 (Uni)     : 00981A             — 支援指定日期
  群益投信 (Capital) : 00982A, 00992A, 00919 — 只能抓最新（Imperva 防火牆，不支援歷史）
  復華投信 (Fuhhwa)  : 00991A             — 支援指定日期

防封鎖設計：
  - 每次請求加 1.5–3.5 秒隨機延遲
  - 失敗時指數退避最多 3 次 retry
  - Session 共用，模擬真實瀏覽器行為
  - 首次請求先訪 landing page 暖身（統一/群益）
"""
from __future__ import annotations

import logging
import random
import time
from datetime import date, datetime, timedelta

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# ── ETF → 投信 路由表 ─────────────────────────────────────────
_YUANTA_ETFS = {"0050", "0056"}

_UNI_ETF_MAP = {
    "00981A": "61YTW",
}

_CAPITAL_ETF_MAP = {
    "00982A": "399",
    "00992A": "500",
    "00919":  "195",
}

_FUHHWA_ETF_MAP = {
    "00991A": "ETF23",
}


_BASE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/147.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

_EMPTY_COLS = ["etf_id", "date", "hold_stock_id", "hold_stock_name", "percentage", "shares"]


# ── 工具函式 ──────────────────────────────────────────────────

def _polite_sleep(min_s: float = 1.5, max_s: float = 3.5) -> None:
    """隨機延遲，避免被視為機器人。"""
    time.sleep(random.uniform(min_s, max_s))


def _make_session(retries: int = 3) -> requests.Session:
    """建立帶 retry/backoff 的 Session。"""
    session = requests.Session()
    session.headers.update(_BASE_HEADERS)
    retry_strategy = Retry(
        total=retries,
        backoff_factor=2,          # 1s → 2s → 4s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _to_df(rows: list[dict]) -> pd.DataFrame:
    """將爬蟲 raw rows 轉為標準 DataFrame（欄位對齊 etf_cache）。"""
    if not rows:
        return pd.DataFrame(columns=_EMPTY_COLS)
    df = pd.DataFrame(rows)
    df = df.rename(columns={
        "stock_id":   "hold_stock_id",
        "stock_name": "hold_stock_name",
        "weight":     "percentage",
    })
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")
    df["percentage"] = pd.to_numeric(df["percentage"], errors="coerce").fillna(0.0)
    if "shares" not in df.columns:
        df["shares"] = 0
    df["shares"] = pd.to_numeric(df["shares"], errors="coerce").fillna(0).astype(int)
    return df[_EMPTY_COLS].copy()


def _business_days_in_range(start: str, end: str) -> list[str]:
    """回傳 [start, end] 之間的所有交易日（週一到週五，YYYYMMDD 格式）。"""
    d = datetime.strptime(start, "%Y%m%d").date()
    end_d = datetime.strptime(end, "%Y%m%d").date()
    result = []
    while d <= end_d:
        if d.weekday() < 5:
            result.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    return result


def _prev_business_days(n: int = 5) -> list[str]:
    """回傳最近 n 個可能的交易日（YYYYMMDD 格式，從昨天往回推）。"""
    days = []
    d = date.today() - timedelta(days=1)
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d.strftime("%Y%m%d"))
        d -= timedelta(days=1)
    return days


# ── 元大投信 ──────────────────────────────────────────────────

def _fetch_yuanta(etf_id: str, target_date: str,
                  session: requests.Session | None = None) -> list[dict]:
    sess = session or _make_session()
    url = "https://etfapi.yuantaetfs.com/ectranslation/api/bridge"
    params = {
        "APIType": "ETFAPI", "CompanyName": "YUANTAFUNDS",
        "PageName": f"/tradeInfo/pcf/{etf_id}",
        "DeviceId": "e7a29639-f413-493c-96f0-d6d4cfbedf4e",
        "FuncId": "PCF/Daily", "AppName": "ETF",
        "Device": "3", "Platform": "ETF",
        "ticker": etf_id, "date": target_date,
    }
    sess.headers.update({"Referer": f"https://www.yuantaetfs.com/#/tradeInfo/pcf/{etf_id}"})
    r = sess.get(url, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    stock_weights = data.get("FundWeights", {}).get("StockWeights", [])
    return [
        {"date": target_date, "etf_id": etf_id,
         "stock_id": item["code"], "stock_name": item.get("name", ""),
         "weight": float(item["weights"])}
        for item in stock_weights
        if item.get("code") and item.get("weights") is not None
    ]


# ── 統一投信 ──────────────────────────────────────────────────

def _fetch_uni(etf_id: str, target_date: str,
               session: requests.Session | None = None) -> list[dict]:
    internal_code = _UNI_ETF_MAP[etf_id]
    dt = datetime.strptime(target_date, "%Y%m%d")
    minguo_date = f"{dt.year - 1911}/{dt.month:02d}/{dt.day:02d}"

    # ezmoney.com.tw 每次請求都需要新 cookie，強制每次暖身
    sess = _make_session()
    pcf_url = "https://www.ezmoney.com.tw/ETF/Transaction/PCF"
    sess.get(pcf_url, timeout=10)
    _polite_sleep(1.5, 2.5)

    post_headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/json; charset=UTF-8",
        "Origin": "https://www.ezmoney.com.tw",
        "Referer": pcf_url,
        "X-Requested-With": "XMLHttpRequest",
    }
    payload = {"fundCode": internal_code, "date": minguo_date, "specificDate": True}

    # ezmoney 偶爾回傳空 body（速率限制），最多 retry 一次
    r = sess.post("https://www.ezmoney.com.tw/ETF/Transaction/GetPCF",
                  json=payload, headers=post_headers, timeout=15)
    r.raise_for_status()
    if not r.content:
        logger.debug("ezmoney 回傳空 body，等待後重試…")
        _polite_sleep(8.0, 12.0)
        sess2 = _make_session()
        sess2.get(pcf_url, timeout=10)
        _polite_sleep(2.0, 3.0)
        r = sess2.post("https://www.ezmoney.com.tw/ETF/Transaction/GetPCF",
                       json=payload, headers=post_headers, timeout=15)
        r.raise_for_status()
    data = r.json()

    stock_details = next(
        (a.get("Details", []) for a in data.get("asset", []) if a.get("AssetName") == "股票"),
        [],
    )
    return [
        {"date": target_date, "etf_id": etf_id,
         "stock_id": str(item["DetailCode"]).strip(),
         "stock_name": str(item.get("DetailName", "")).split(".")[0].strip(),
         "weight": float(item["NavRate"]),
         "shares": int(item["Share"]) if item.get("Share") else 0}
        for item in stock_details
        if item.get("DetailCode") and item.get("NavRate") is not None
    ]


# ── 群益投信 ──────────────────────────────────────────────────

def _fetch_capital(etf_id: str, target_date: str | None = None,
                   session: requests.Session | None = None) -> list[dict]:
    """
    群益投信 ETF 持股爬蟲。
    target_date: YYYYMMDD 字串；None 時抓最新。
    API payload 用 YYYY/MM/DD 格式，None 時傳 null 抓最新。
    """
    internal_id = _CAPITAL_ETF_MAP[etf_id]
    sess = session or _make_session()
    landing = f"https://www.capitalfund.com.tw/etf/product/detail/{internal_id}/buyback"

    sess.headers.update({"Referer": "https://www.capitalfund.com.tw/"})
    sess.get(landing, timeout=15)
    _polite_sleep(2.0, 4.0)

    # 轉換日期格式：YYYYMMDD → YYYY/MM/DD，None 維持 null
    api_date = None
    if target_date:
        dt = datetime.strptime(target_date, "%Y%m%d")
        api_date = dt.strftime("%Y/%m/%d")

    r = sess.post(
        "https://www.capitalfund.com.tw/CFWeb/api/etf/buyback",
        json={"fundId": internal_id, "date": api_date},
        headers={"Referer": landing, "Origin": "https://www.capitalfund.com.tw"},
        timeout=20,
    )
    r.raise_for_status()
    data = r.json().get("data", {})

    raw_date = data.get("pcf", {}).get("date1", "")
    clean_date = raw_date.split(" ")[0].replace("-", "") if raw_date else ""

    return [
        {"date": clean_date, "etf_id": etf_id,
         "stock_id": str(item["stocNo"]).strip(),
         "stock_name": str(item.get("stocName", "")).strip(),
         "weight": float(item["weight"])}
        for item in data.get("stocks", [])
        if item.get("stocNo") and item.get("weight") is not None
    ]


# ── 復華投信 ──────────────────────────────────────────────────

def _fetch_fuhhwa(etf_id: str, target_date: str,
                  session: requests.Session | None = None) -> list[dict]:
    internal_id = _FUHHWA_ETF_MAP[etf_id]
    dt = datetime.strptime(target_date, "%Y%m%d")
    api_date = dt.strftime("%Y/%m/%d")

    sess = session or _make_session()
    sess.headers.update({"Referer": "https://www.fhtrust.com.tw/"})
    r = sess.get(
        "https://www.fhtrust.com.tw/api/assets",
        params={"fundID": internal_id, "qDate": api_date},
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()

    details = (data.get("result") or [{}])[0].get("detail", [])
    rows = []
    for item in details:
        if item.get("ftype") != "股票":
            continue
        code = item.get("stockid")
        if not code:
            continue
        raw = item.get("prate_addaccint", "0")
        try:
            weight = float(str(raw).replace("%", "").strip())
        except ValueError:
            weight = 0.0
        shares_raw = item.get("qshare", "0")
        try:
            shares_val = int(str(shares_raw).replace(",", ""))
        except ValueError:
            shares_val = 0
        rows.append({
            "date": target_date,
            "etf_id": etf_id,
            "stock_id": str(code).strip(),
            "stock_name": str(item.get("stockname", "")).strip(),
            "weight": weight,
            "shares": shares_val,
        })
    return rows


# ── 公開介面 ──────────────────────────────────────────────────

def fetch_etf_holdings(etf_id: str, target_date: str | None = None,
                       session: requests.Session | None = None) -> pd.DataFrame:
    """
    抓取指定 ETF 的持股明細，回傳標準 DataFrame。

    欄位：etf_id, date (datetime), hold_stock_id, hold_stock_name, percentage

    target_date: YYYYMMDD 字串；None 時自動往回找最近 5 個交易日。
    群益 (00982A, 00992A, 00919) 不支援歷史日期，target_date 參數無效。
    """
    dates_to_try = [target_date] if target_date else _prev_business_days(5)

    if etf_id in _CAPITAL_ETF_MAP:
        # 群益支援歷史日期：逐一嘗試 dates_to_try
        for d in dates_to_try:
            try:
                rows = _fetch_capital(etf_id, target_date=d, session=session)
                if rows:
                    return _to_df(rows)
            except Exception as e:
                logger.debug("群益 %s %s 抓取失敗：%s", etf_id, d, e)
        logger.warning("ETF %s 最近 %d 個交易日均無資料", etf_id, len(dates_to_try))
        return pd.DataFrame(columns=_EMPTY_COLS)

    fetch_fn = None
    if etf_id in _YUANTA_ETFS:
        fetch_fn = _fetch_yuanta
    elif etf_id in _UNI_ETF_MAP:
        fetch_fn = _fetch_uni
    elif etf_id in _FUHHWA_ETF_MAP:
        fetch_fn = _fetch_fuhhwa
    elif etf_id in _CAPITAL_ETF_MAP:
        fetch_fn = _fetch_capital
    else:
        logger.warning("ETF %s 尚無對應爬蟲，略過", etf_id)
        return pd.DataFrame(columns=_EMPTY_COLS)

    for d in dates_to_try:
        try:
            rows = fetch_fn(etf_id, d, session=session)
            if rows:
                return _to_df(rows)
        except Exception as e:
            logger.debug("%s %s 抓取失敗：%s", etf_id, d, e)

    logger.warning("ETF %s 最近 %d 個交易日均無資料", etf_id, len(dates_to_try))
    return pd.DataFrame(columns=_EMPTY_COLS)


def backfill_etf_holdings(
    etf_ids: list[str] | None = None,
    start_date: str = "20260421",
    end_date: str = "20260424",
    skip_existing: bool = True,
) -> dict[str, int]:
    """
    補抓指定日期範圍的 ETF 持股資料並存入快取。

    etf_ids    : 要補的 ETF 清單，None 時使用所有有爬蟲支援的 ETF。
    start_date : YYYYMMDD
    end_date   : YYYYMMDD
    skip_existing: True 時略過快取中已有該日期資料的 ETF。

    回傳 {etf_id: 新增筆數} 字典。
    """
    from db.etf_cache import save_etf_holdings, load_etf_holdings

    if etf_ids is None:
        etf_ids = SUPPORTED_ETFS

    dates = _business_days_in_range(start_date, end_date)
    summary: dict[str, int] = {etf: 0 for etf in etf_ids}

    logger.info("補抓範圍 %s ~ %s，共 %d 個交易日，%d 支 ETF",
                start_date, end_date, len(dates), len(etf_ids))

    for etf_id in etf_ids:
        is_capital = etf_id in _CAPITAL_ETF_MAP

        if is_capital:
            # 群益支援歷史日期，逐日補抓
            pass  # fall through to the shared logic below

        fetch_fn = (
            _fetch_yuanta  if etf_id in _YUANTA_ETFS    else
            _fetch_uni     if etf_id in _UNI_ETF_MAP     else
            _fetch_fuhhwa  if etf_id in _FUHHWA_ETF_MAP  else
            _fetch_capital if etf_id in _CAPITAL_ETF_MAP else None
        )
        if fetch_fn is None:
            logger.warning("[%s] 無對應爬蟲，略過", etf_id)
            continue

        # 統一投信 / 群益每次都需要重新暖身，不共用 session
        use_shared_session = etf_id not in _UNI_ETF_MAP and etf_id not in _CAPITAL_ETF_MAP
        sess = _make_session() if use_shared_session else None

        for d in dates:
            # 檢查快取是否已有該日
            if skip_existing:
                cached = load_etf_holdings(etf_id, start_date=d, end_date=d)
                if not cached.empty:
                    logger.info("[%s] %s 已有資料，略過", etf_id, d)
                    continue

            logger.info("[%s] 抓取 %s...", etf_id, d)
            try:
                current_sess = sess if use_shared_session else None
                rows = fetch_fn(etf_id, d, session=current_sess)
                if rows:
                    df = _to_df(rows)
                    saved = save_etf_holdings(etf_id, df)
                    summary[etf_id] += saved
                    logger.info("[%s] %s 儲存 %d 筆", etf_id, d, saved)
                else:
                    logger.info("[%s] %s 無資料（可能是非交易日或假日）", etf_id, d)
            except Exception as e:
                logger.error("[%s] %s 抓取失敗：%s", etf_id, d, e)

            # 統一投信 ezmoney 較敏感，多等一點
            if etf_id in _UNI_ETF_MAP:
                _polite_sleep(5.0, 8.0)
            else:
                _polite_sleep()

        _polite_sleep(3.0, 5.0)  # 換下一支 ETF 多等一點

    logger.info("補抓完成：%s", summary)
    return summary


SUPPORTED_ETFS: list[str] = sorted(
    _YUANTA_ETFS | set(_UNI_ETF_MAP) | set(_CAPITAL_ETF_MAP) | set(_FUHHWA_ETF_MAP)
)
