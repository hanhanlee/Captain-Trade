"""Central capability registry for FinMind datasets used by this project."""

from __future__ import annotations


DATASET_CAP: dict[str, dict] = {
    "TaiwanStockInfo": {
        "endpoint_type": "data",
        "premium": False,
        "all_by_date": False,
        "single_day_only": False,
    },
    "TaiwanStockTradingDate": {
        "endpoint_type": "data",
        "premium": False,
        "all_by_date": False,
        "single_day_only": False,
    },
    "TaiwanStockPrice": {
        "endpoint_type": "data",
        "premium": False,
        "all_by_date": True,
        "single_day_only": False,
        "single_day_only_for_all_by_date": True,
    },
    "TaiwanStockInstitutionalInvestorsBuySell": {
        "endpoint_type": "data",
        "premium": False,
        "all_by_date": True,
        "single_day_only": False,
        "single_day_only_for_all_by_date": True,
    },
    "TaiwanStockMarginPurchaseShortSale": {
        "endpoint_type": "data",
        "premium": False,
        "all_by_date": True,
        "single_day_only": False,
        "single_day_only_for_all_by_date": True,
    },
    "TaiwanStockTradingDailyReport": {
        "endpoint_type": "special",
        "premium": True,
        "all_by_date": False,
        "single_day_only": True,
        "endpoint": "taiwan_stock_trading_daily_report",
    },
    "TaiwanStockTradingDailyReportSecIdAgg": {
        "endpoint_type": "special",
        "premium": True,
        "all_by_date": False,
        "single_day_only": False,
        "endpoint": "taiwan_stock_trading_daily_report_secid_agg",
        "note": "Dedicated special endpoint: /api/v4/taiwan_stock_trading_daily_report_secid_agg. securities_trader_id is REQUIRED by the API (HTTP 400 if omitted).",
    },
    "TaiwanStockHoldingSharesPer": {
        "endpoint_type": "data",
        "premium": True,
        "all_by_date": True,
        "single_day_only": False,
        "single_day_only_for_all_by_date": True,
    },
    "TaiwanStockFinancialStatements": {
        "endpoint_type": "data",
        "premium": True,
        "all_by_date": True,
        "single_day_only": False,
        "single_day_only_for_all_by_date": True,
    },
    "TaiwanStockBalanceSheet": {
        "endpoint_type": "data",
        "premium": True,
        "all_by_date": True,
        "single_day_only": False,
        "single_day_only_for_all_by_date": True,
    },
    "TaiwanStockCashFlowsStatement": {
        "endpoint_type": "data",
        "premium": True,
        "all_by_date": True,
        "single_day_only": False,
        "single_day_only_for_all_by_date": True,
    },
    "TaiwanStockDispositionSecuritiesPeriod": {
        "endpoint_type": "data",
        "premium": True,
        "all_by_date": True,
        "single_day_only": False,
        "single_day_only_for_all_by_date": True,
    },
    "TaiwanStockSuspended": {
        "endpoint_type": "data",
        "premium": True,
        "all_by_date": False,
        "single_day_only": False,
    },
    "TaiwanStockShareholdingTransfer": {
        "endpoint_type": "data",
        "premium": True,
        "all_by_date": False,
        "single_day_only": False,
    },
    "TaiwanStockAttentionSecuritiesPeriod": {
        "endpoint_type": "data",
        "premium": True,
        "all_by_date": False,
        "single_day_only": False,
    },
    "TaiwanStockTreasuryShares": {
        "endpoint_type": "data",
        "premium": True,
        "all_by_date": False,
        "single_day_only": False,
    },
    "TaiwanStockPriceLimit": {
        "endpoint_type": "data",
        "premium": True,
        "all_by_date": True,
        "single_day_only": False,
        "single_day_only_for_all_by_date": True,
    },
    "TaiwanStockKBar": {
        "endpoint_type": "data",
        "premium": True,
        "all_by_date": False,
        "single_day_only": True,
    },
    "TaiwanStockPriceTick": {
        "endpoint_type": "data",
        "premium": True,
        "all_by_date": True,
        "single_day_only": True,
        "single_day_only_for_all_by_date": True,
    },
    "taiwan_stock_tick_snapshot": {
        "endpoint_type": "snapshot",
        "premium": True,
        "all_by_date": False,
        "single_day_only": False,
    },
}


def get_dataset_capability(dataset: str) -> dict:
    """Return normalized capability metadata for a dataset."""
    cap = DATASET_CAP.get(dataset, {})
    return {
        "endpoint_type": cap.get("endpoint_type", "data"),
        "premium": bool(cap.get("premium", False)),
        "all_by_date": bool(cap.get("all_by_date", False)),
        "single_day_only": bool(cap.get("single_day_only", False)),
        "single_day_only_for_all_by_date": bool(cap.get("single_day_only_for_all_by_date", False)),
        **cap,
    }