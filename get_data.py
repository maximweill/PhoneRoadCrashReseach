import pandas as pd
from pathlib import Path

# Base directories
BASE_DIR = Path(__file__).parent

# Crash data ---------------
CRASH_DATA_DIR = BASE_DIR / "car_crash_data_parquet" 
CRASH_SAMPLE_CHOICES = {f.stem: str(f) for f in CRASH_DATA_DIR.glob("*.parquet")}

# Phone Drop Tests data ---------------
PHONE_FRAMED_DIR = BASE_DIR / "phone_drop_test_data_parquet" / "phone_framed"
PHONE_DROP_SAMPLE_CHOICES = {f.stem: str(f) for f in PHONE_FRAMED_DIR.glob("*.parquet") if f.stat().st_size > 0}

PHONE_REF_DIR = BASE_DIR / "phone_drop_test_data_parquet" / "phone_reference_signals"
PHONE_REF_SAMPLE_CHOICES = {f.stem: str(f) for f in PHONE_REF_DIR.glob("*.parquet") if f.stat().st_size > 0}

# Logs ----------------
LOGS_DIR = BASE_DIR / "test_log_parquet" 

# Stationary data ---------------
STATIONARY_DATA_DIR = BASE_DIR / "stationary_parquet" / "framed"
STATIONARY_SAMPLE_CHOICES = {f.stem: str(f) for f in STATIONARY_DATA_DIR.glob("*.parquet") if f.stat().st_size > 0}
STATIONARY_ALLAN_DIR = BASE_DIR / "stationary_parquet" / "allan_variance"
STATIONARY_ALLAN_CHOICES = {f.name: str(f) for f in STATIONARY_ALLAN_DIR.glob("*.parquet") if f.stat().st_size > 0}
STATIONARY_FRAMING_LOGS = pd.read_parquet(LOGS_DIR / "stationary_framing_logs.parquet")

# Core Logs
LOGS = pd.read_parquet(LOGS_DIR / "Data Collection Log.parquet")
LOGS_CHOICES = LOGS["Test Name"].dropna().unique().tolist()
DEDUPLICATION_LOG = pd.concat([pd.read_parquet(p) for p in LOGS_DIR.glob("deduplication_log*.parquet")], ignore_index=True)
FRAMING_LOGS = pd.read_parquet(LOGS_DIR / "framing_logs.parquet")
PHONE_CHARACTERISTICS_AGGREGATED = pd.read_parquet(LOGS_DIR / "phone_characteristics_aggregated.parquet")

# Phyphox data ---------------
PHYPHOX_FAST_DATA_DIR = BASE_DIR / "phyphox_data" / "fast_data"
DEVICE_DATA_DIR_DATA = PHYPHOX_FAST_DATA_DIR / "devices.parquet"
META_DATA_DIR = PHYPHOX_FAST_DATA_DIR / "metadata.parquet"
