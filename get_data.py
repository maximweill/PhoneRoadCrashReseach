# get_data.py
from pathlib import Path

# Adjust this path if your script is in a subfolder
DATA_DIR = Path(__file__).parent / "data_parquet" 
SAMPLE_CHOICES = {f.stem: str(f) for f in DATA_DIR.glob("*.parquet")}

import pandas as pd

# 1. Define the path to your data folder
HERE = Path(__file__).resolve().parent
DATA_DIR = HERE / "phyphox_data" / "fast_data"

# 2. Load the main devices data
# engine='pyarrow' is the fastest for reading back into memory
devices_data = pd.read_parquet(DATA_DIR / "devices.parquet", engine="pyarrow")

# 3. Load the lists from the metadata parquet
meta_df = pd.read_parquet(DATA_DIR / "metadata.parquet", engine="pyarrow")


# ADD .tolist() to convert from numpy/pandas objects back to standard Python lists
manufacturers = meta_df["manufacturers"].iloc[0].tolist()
numeric_cols = meta_df["numeric_cols"].iloc[0].tolist()

# Optional: Ensure they are sorted (Parquet doesn't always guarantee order)
manufacturers = sorted(manufacturers, key=lambda s: str(s).lower())
numeric_cols = sorted(numeric_cols)

print(f"Loaded {len(devices_data)} devices across {len(manufacturers)} manufacturers.")