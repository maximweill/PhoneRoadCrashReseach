# imports --------
print("importing data")
import pandas as pd
from pathlib import Path



#time data---------------
DATA_DIR = Path(__file__).parent / "car_crash_data_parquet" 
CRASH_SAMPLE_CHOICES = {f.stem: str(f) for f in DATA_DIR.glob("*.parquet")}
DATA_DIR = Path(__file__).parent / "phone_drop_test_data_parquet"/ "headform_parquet" 
HEAD_DROP_SAMPLE_CHOICES = {f.stem: str(f) for f in DATA_DIR.glob("*.parquet")}
DATA_DIR = Path(__file__).parent / "phone_drop_test_data_parquet" / "phone_parquet" 
PHONE_DROP_SAMPLE_CHOICES = {f.stem: str(f) for f in DATA_DIR.glob("*.parquet")}


print(list(PHONE_DROP_SAMPLE_CHOICES.keys()))
#crash logs----------------
DATA_DIR = Path(__file__).parent / "test_log_parquet" 
LOGS = pd.read_parquet(DATA_DIR / "Data Collection Log.parquet")
LOGS_CHOICES = LOGS["Test Name"]


#phyphox data---------------

HERE = Path(__file__).resolve().parent
DATA_DIR = HERE / "phyphox_data" / "fast_data"
DEVICE_DATA_DIR_DATA = DATA_DIR / "devices.parquet"
META_DATA_DIR = DATA_DIR / "metadata.parquet"

# print -------------------
print("Loaded data")