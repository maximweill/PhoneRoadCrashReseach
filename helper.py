import pandas as pd
import numpy as np

def load_phone_data(path):
    df = pd.read_parquet(path)
    return df

def load_head_data(path):
    df = pd.read_parquet(path)
    return df

def load_reference_data(path):
    df = pd.read_parquet(path)
    return df

def get_phone_sampling_rate(df):
    if "time_ns" not in df.columns:
        return []
    nano = df["time_ns"]
    diffs = np.diff(nano)
    # Avoid division by zero
    diffs = diffs[diffs > 0]
    if len(diffs) == 0:
        return []
    differences_fps = 1 / (diffs * 1e-9)
    return differences_fps

def get_peak_accel(df):
    params = ["accelX_g", "accelY_g", "accelZ_g"]
    existing_params = [p for p in params if p in df.columns]
    if not existing_params:
        return []
    return df[existing_params].max().round(3).tolist()

def get_peak_gyro(df):
    params = ["gyroX_dps", "gyroY_dps", "gyroZ_dps"]
    existing_params = [p for p in params if p in df.columns]
    if not existing_params:
        return []
    return df[existing_params].max().round(3).tolist()
