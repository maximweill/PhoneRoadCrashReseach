"""
This file contains non ui and not reactive functions used
by the webapp. These can include, defining conventions, 
functions for loading data.  
"""

import pandas as pd
import numpy as np

import re



#----------------------------------------------------
#CONVENTIONS
#----------------------------------------------------
def naming_convention(phone_id:str,config:str,target_speed_mps:str,repeat:str,is_reference = False, is_framed = False):
    if is_reference:
        return f"{target_speed_mps}mps_{config}_REPEAT{repeat}_Headform_Transformed_{phone_id}_REF"
    if is_framed:
        return f"{target_speed_mps}mps_{config}_REPEAT{repeat}_{phone_id}_framed"

    return f"{target_speed_mps}mps_{config}_REPEAT{repeat}_{phone_id}"

def extract_phone_id(filename):
    """
    Extracts PhoneID from filename (e.g., Phone001, Phone_001).
    """
    match = re.search(r"Phone_?(\d+)", filename, re.IGNORECASE)
    if match:
        return f"Phone{match.group(1)}"
    return "Unknown"

def parse_stationary_filename(filename):
    """
    Parses stationary filename: {sensor}_stationary_{date}_{time}_{PhoneID}
    """
    parts = filename.split("_")
    sensor = parts[0] # accel or gyro
    phone_id = parts[-1] # Phone004
    return sensor, phone_id

#----------------------------------------------------
#DATA LOADING
#----------------------------------------------------

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


def read_pq(path):
    from pathlib import Path
    p = Path(path)
    return pd.read_parquet(p) if p.exists() else pd.DataFrame()

# 4. Global Choice Lists ----------------
def get_choices(df, col):
    return ["All"] + sorted(df[col].dropna().unique().astype(str).tolist()) if not df.empty and col in df.columns else ["All"]