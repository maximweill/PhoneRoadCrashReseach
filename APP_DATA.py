"""
This file contains the paths to the data used in the webapp
no file reading happens here, just the path constants are defined
"""
from pathlib import Path


# Base directories
BASE_DIR = Path(__file__).parent / "webapp_data"

# 1. Sensor Data Choices (from file system) ---------------
CRASH_DATA_DIR = BASE_DIR / "car_crash_data_parquet" 
CRASH_SAMPLE_CHOICES = {f.stem: str(f) for f in CRASH_DATA_DIR.glob("*.parquet")}

STATIONARY_DATA_DIR = BASE_DIR / "stationary_parquet" / "framed"
STATIONARY_SAMPLE_CHOICES = {f.stem: str(f) for f in STATIONARY_DATA_DIR.glob("*.parquet") if f.stat().st_size > 0}

STATIONARY_ALLAN_DIR = BASE_DIR / "stationary_parquet" / "allan_variance"
STATIONARY_ALLAN_CHOICES = {f.name: str(f) for f in STATIONARY_ALLAN_DIR.glob("*.parquet") if f.stat().st_size > 0}

# 3. Drop Test Data ----------------
DROP_TEST_DIR = BASE_DIR / "phone_drop_test_data_parquet"
PHONE_DROP_DIR = DROP_TEST_DIR / "phone_framed"
PHONE_REF_DIR = DROP_TEST_DIR / "phone_reference_signals"

PHONE_DROP_SAMPLE_CHOICES = {f.stem: str(f) for f in PHONE_DROP_DIR.glob("*.parquet")}
PHONE_REF_SAMPLE_CHOICES = {f.stem: str(f) for f in PHONE_REF_DIR.glob("*.parquet")}

# 4. Lookup Tables (from webapp_data/lookup_tables_parquet) ----------------
LOG_PATH = BASE_DIR / "lookup_tables_parquet"/ "Data Collection Log.parquet"
PHONE_CHARACTERISTICS = BASE_DIR / "lookup_tables_parquet" / "phone_characteristics_aggregated.parquet"

# 5. Phyphox data ---------------
PHYPHOX_FAST_DATA_DIR = Path(__file__).parent / "phyphox_data" / "fast_data"
DEVICE_DATA_DIR_DATA = PHYPHOX_FAST_DATA_DIR / "devices.parquet"
META_DATA_DIR = PHYPHOX_FAST_DATA_DIR / "metadata.parquet"

BUILD_DIR = Path("built_data")
