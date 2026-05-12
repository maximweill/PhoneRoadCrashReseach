import pandas as pd
import numpy as np

import re

def extract_phone_id(filename):
    """
    Extracts PhoneID from filename (e.g., Phone001, Phone_001).
    """
    match = re.search(r"Phone_?(\d+)", filename, re.IGNORECASE)
    if match:
        return f"Phone{match.group(1)}"
    return "Unknown"

def load_phone_data(path):
    return pd.read_parquet(path)

def load_allan_data(path):
    return pd.read_parquet(path)

def load_reference_data(path):
    return pd.read_parquet(path)

def get_phone_sampling_rate(df):
    t_col = "Time (s)" if "Time (s)" in df.columns else "time_ns"
    factor = 1.0 if "Time (s)" in df.columns else 1e-9
    diffs = np.diff(df[t_col].values) * factor
    diffs = diffs[diffs > 0]
    return 1.0 / diffs if len(diffs) > 0 else []

def get_peak_accel(df):
    cols = [c for c in ["LinAccX (m/s2)", "LinAccY (m/s2)", "LinAccZ (m/s2)", "accelX_g", "accelY_g", "accelZ_g"] if c in df.columns]
    return df[cols].abs().max().round(3).tolist() if cols else []

def get_peak_gyro(df):
    cols = [c for c in ["RotVelX (rad/s)", "RotVelY (rad/s)", "RotVelZ (rad/s)", "gyroX_dps", "gyroY_dps", "gyroZ_dps"] if c in df.columns]
    return df[cols].abs().max().round(3).tolist() if cols else []
