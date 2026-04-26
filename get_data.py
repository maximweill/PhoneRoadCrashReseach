# imports --------
print("importing data")
import pandas as pd
from pathlib import Path

# Base directories
BASE_DIR = Path(__file__).parent

# Crash data ---------------
CRASH_DATA_DIR = BASE_DIR / "car_crash_data_parquet" 
CRASH_SAMPLE_CHOICES = {f.stem: str(f) for f in CRASH_DATA_DIR.glob("*.parquet")}

# Phone Drop Tests data ---------------

# Phone cleaned data
PHONE_CLEANED_DIR = BASE_DIR / "phone_drop_test_data_parquet" / "phone_cleaned"
PHONE_DROP_SAMPLE_CHOICES = {f.stem: str(f) for f in PHONE_CLEANED_DIR.glob("*.parquet")}

# Phone reference signals
PHONE_REF_DIR = BASE_DIR / "phone_drop_test_data_parquet" / "phone_reference_signals"
PHONE_REF_SAMPLE_CHOICES = {f.stem: str(f) for f in PHONE_REF_DIR.glob("*.parquet")}

# Logs and Mapping ----------------
LOGS_DIR = BASE_DIR / "test_log_parquet" 

# Data Collection Log
LOGS = pd.read_parquet(LOGS_DIR / "Data Collection Log.parquet")
LOGS_CHOICES = LOGS["Test Name"].dropna().unique().tolist()

# Deduplication Log
DEDUPLICATION_LOG = pd.read_parquet(LOGS_DIR / "deduplication_log.parquet")

# Phone Cropping Params (The master mapping for phone-to-ref)
CROPPING_PARAMS = pd.read_parquet(LOGS_DIR / "phone_cropping_params.parquet")

# Phyphox data ---------------
PHYPHOX_FAST_DATA_DIR = BASE_DIR / "phyphox_data" / "fast_data"
DEVICE_DATA_DIR_DATA = PHYPHOX_FAST_DATA_DIR / "devices.parquet"
META_DATA_DIR = PHYPHOX_FAST_DATA_DIR / "metadata.parquet"

# print -------------------
print(f"Loaded {len(PHONE_DROP_SAMPLE_CHOICES)} phone files and {len(PHONE_REF_SAMPLE_CHOICES)} reference signals.")
print("Loaded data")
