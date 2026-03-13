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
devices_data = pd.read_parquet(DATA_DIR / "devices.parquet", engine="pyarrow")
meta_df = pd.read_parquet(DATA_DIR / "metadata.parquet", engine="pyarrow")

manufacturers = meta_df["manufacturers"].iloc[0].tolist()
numeric_cols = meta_df["numeric_cols"].iloc[0].tolist()

manufacturers = sorted(manufacturers, key=lambda s: str(s).lower())
numeric_cols = sorted(numeric_cols)


# print -------------------
print("Loaded data")