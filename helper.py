import pandas as pd
import numpy as np

def load_phone_data(path):
    df = pd.read_parquet(path)
    df = df.rename(columns={
        "sensor_time_ns":"time_ns",
    })
    return df

def load_head_data(path):
    df = pd.read_parquet(path)
    df= df.rename(columns={
        "sensor_time_ns":"time_ns",
    })
    return df

def get_phone_sampling_rate(df):
    nano = df["time_ns"]
    differences_fps = 1 / (np.diff(nano) * 1e-9)
    return differences_fps

def get_peak_accel(df):
    params = ["accelX_g", "accelY_g", "accelZ_g"]
    return df[params].max().round(3).tolist()

def get_peak_gyro(df):
    params = ["gyroX_dps", "gyroY_dps", "gyroZ_dps"]
    return df[params].max().round(3).tolist()