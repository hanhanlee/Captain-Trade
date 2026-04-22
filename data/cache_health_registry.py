from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CacheHealthDatasetSpec:
    key: str
    label: str
    table_name: str
    date_column: str
    description: str
    supports_repair: bool = True
    premium_required: bool = False


DATASET_SPECS: dict[str, CacheHealthDatasetSpec] = {
    "price": CacheHealthDatasetSpec(
        key="price",
        label="日線價格",
        table_name="price_cache",
        date_column="date",
        description="以 active 股票 x 交易日估算價格快取完整度。",
    ),
    "institutional": CacheHealthDatasetSpec(
        key="institutional",
        label="三大法人",
        table_name="inst_cache",
        date_column="date",
        description="以 active 股票 x 交易日估算法人資料完整度。",
    ),
    "margin": CacheHealthDatasetSpec(
        key="margin",
        label="融資融券",
        table_name="margin_cache",
        date_column="date",
        description="以 active 股票 x 交易日估算融資融券完整度。",
    ),
    "broker_main_force": CacheHealthDatasetSpec(
        key="broker_main_force",
        label="主力券商",
        table_name="broker_main_force_cache",
        date_column="date",
        description="以 active 股票 x 交易日估算主力券商快取完整度。",
        premium_required=True,
    ),
}


def get_cache_health_dataset(key: str) -> CacheHealthDatasetSpec:
    if key not in DATASET_SPECS:
        raise KeyError(f"Unknown cache health dataset: {key}")
    return DATASET_SPECS[key]


def list_cache_health_datasets() -> list[CacheHealthDatasetSpec]:
    return list(DATASET_SPECS.values())


def list_cache_health_dataset_keys() -> list[str]:
    return list(DATASET_SPECS.keys())