from functools import lru_cache
from pathlib import Path

import pandas as pd


PHYPOX_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "phyphox_parquet"
DEVICES_DATA_PATH = PHYPOX_DATA_DIR / "devices.parquet"
METADATA_PATH = PHYPOX_DATA_DIR / "metadata.parquet"


@lru_cache(maxsize=1)
def devices_data() -> pd.DataFrame:
    return pd.read_parquet(DEVICES_DATA_PATH, engine="pyarrow")


@lru_cache(maxsize=1)
def metadata() -> pd.DataFrame:
    return pd.read_parquet(METADATA_PATH, engine="pyarrow")


def manufacturers() -> list[str]:
    values = metadata()["manufacturers"].iloc[0].tolist()
    return sorted(values, key=lambda value: str(value).lower())


def numeric_columns() -> list[str]:
    values = metadata()["numeric_cols"].iloc[0].tolist()
    return sorted(values)


def availability_column(variable: str) -> str:
    return "_".join(variable.split("_")[:-1] + ["available"])
