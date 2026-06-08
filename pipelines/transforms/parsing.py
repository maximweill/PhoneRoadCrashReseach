import pandas as pd
import numpy as np
import re
from pathlib import Path
from ..core import Context
from .base import G, DEG2RAD

# =========================================================
# PARSING TRANSFORMS
# =========================================================

def normalize_column_names(df: pd.DataFrame, ctx: Context):
    # Conservative cleaning: strip and remove only truly problematic chars
    # Keeps spaces, parens, and slashes for unit consistency
    df.columns = (
        df.columns
        .str.strip()
        .str.replace(r"[^a-zA-Z0-9 ()//_]", "", regex=True)
    )
    return df, ctx

def normalize_time_column(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:

    if "Time (s)" in df.columns:
        return df, ctx

    # Phyphox raw output
    if "sensor_time_ns" in df.columns:
        df = df.rename(columns={"sensor_time_ns": "time_ns"})

    if "time_ns" in df.columns:
        df["Time (s)"] = df["time_ns"].astype(np.float64) / 1e9
        df = df.drop(columns=["time_ns"])
    elif "time" in df.columns:
        df = df.rename(columns={"time": "Time (s)"})
    
    if "Time (s)" not in df.columns:
        raise ValueError(f"Missing time column. Found columns: {df.columns.tolist()}")

    return df, ctx

def ensure_sensor_columns(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:

    cols = [
        "accelX_g", "accelY_g", "accelZ_g","accelMag_g",
        "gyroX_dps", "gyroY_dps", "gyroZ_dps","gyroMag_dps",
    ]

    for c in cols:
        if c not in df.columns:
            df[c] = 0.0

    return df, ctx

def normalize_headform_columns(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    # Take inspiration from DEPRECATED_headform_parser.py
    rename_map = {
        'Chan 0:6DX0855-AV1': 'gyroZ_dps',
        'Chan 1:6DX0855-AV2': 'gyroY_dps',
        'Chan 2:6DX0855-AV3': 'gyroX_dps',
        'Chan 3:6DX0855-AC1': 'accelZ_g',
        'Chan 4:6DX0855-AC2': 'accelY_g',
        'Chan 5:6DX0855-AC3': 'accelX_g',
        'Time': 'Time (s)'
    }
    
    # If the exact names are not found, try index-based as fallback
    if not any(col in df.columns for col in rename_map.keys() if col != 'Time'):
        mapping = {
            0: "Time (s)",
            1: "gyroZ_dps",
            2: "gyroY_dps",
            3: "gyroX_dps",
            4: "accelZ_g",
            5: "accelY_g",
            6: "accelX_g"
        }
        new_cols = []
        for i in range(len(df.columns)):
            if i in mapping:
                new_cols.append(mapping[i])
            else:
                new_cols.append(df.columns[i])
        df.columns = new_cols
    else:
        df = df.rename(columns=rename_map)
    
    # Ensure all sensor columns are numeric
    sensor_cols = [
        'gyroZ_dps', 'gyroY_dps', 'gyroX_dps', 
        'accelZ_g', 'accelY_g', 'accelX_g', 'Time (s)'
    ]
    for col in sensor_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    return df, ctx

def sort_by_time(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:

    if not df["Time (s)"].is_monotonic_increasing:
        df = df.sort_values("Time (s)").reset_index(drop=True)

    return df, ctx

def interpolate_outliers(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:
    # 1. Work on a copy to protect the original dataset
    df_clean = df.copy()
    sensor_cols = [col for col in df_clean.columns if "Acc" in col or "Rot" in col]

    # 3. Define your threshold multiplier
    # For MAD, a multiplier between 3 and 5 is standard practice
    threshold_multiplier = 7.4 # ~5sigma 

    for col in sensor_cols:
        # Calculate global median
        median = df_clean[col].median()

        # Calculate global Median Absolute Deviation (MAD)
        mad = (df_clean[col] - median).abs().median()

        # 4. Find deviations that exceed (multiplier * MAD)
        is_outlier = (df_clean[col] - median).abs() > (
            threshold_multiplier * mad
        )
        ctx[f"{col}_MAD"] = mad
        ctx[f"{col}_median"] = median
        ctx[f"{col}_n_outliers"] = int(is_outlier.sum())

        # 5. Mask outliers with NaN and apply linear interpolation
        df_clean.loc[is_outlier, col] = np.nan
        df_clean[col] = df_clean[col].interpolate(
            method="linear", limit_direction="both"
        )

    return df_clean, ctx


def accelerometer_based_timestamps(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:

    sensor_cols = [
        "accelX_g",
        "accelY_g",
        "accelZ_g",
    ]

    vals = df[sensor_cols].to_numpy(dtype=np.float32)

    keep = np.ones(len(df), dtype=bool)

    # compare each sample with previous sample
    same_as_prev = np.all(vals[1:] == vals[:-1], axis=1)

    keep[1:] = ~same_as_prev

    removed = int(np.sum(~keep))

    df = df.loc[keep].reset_index(drop=True)

    ctx["acc_removed_rows"] = removed
    ctx["acc_initial_rows"] = len(keep)
    ctx["acc_final_rows"] = len(df)

    return df, ctx

def deduplicate(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:

    keep = ~df["Time (s)"].duplicated(keep="first")

    removed = int((~keep).sum())

    df = df.loc[keep].reset_index(drop=True)
    
    ctx["dedup_removed_rows"] = removed
    ctx["dedup_initial_rows"] = len(keep)
    ctx["dedup_final_rows"] = len(df)

    return df, ctx

def convert_units(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:

    t = df["Time (s)"].to_numpy(dtype=np.float64)

    # Components only for vector math
    accel_vec = (
        df[["accelX_g", "accelY_g", "accelZ_g"]]
        .to_numpy(dtype=np.float32)
        * G
    )

    gyro_vec = (
        df[["gyroX_dps", "gyroY_dps", "gyroZ_dps"]]
        .to_numpy(dtype=np.float32)
        * DEG2RAD
    )

    # Recompute magnitudes to be sure they are consistent with components
    accel_mag = np.linalg.norm(accel_vec, axis=1)
    gyro_mag = np.linalg.norm(gyro_vec, axis=1)

    # Rotational acceleration: derivative of the angular velocity VECTOR
    rot_acc_vec = np.gradient(gyro_vec, t, axis=0)
    rot_acc_mag = np.linalg.norm(rot_acc_vec, axis=1)

    # Preserve columns that might have been added by previous transforms
    preserve = ["trigger", "axis", "triggered","batt_temp_c"]
    extra_cols = {c: df[c].to_numpy() for c in preserve if c in df.columns}

    df = pd.DataFrame({
        "Time (s)": t,

        "LinAccX (m/s2)": accel_vec[:, 0],
        "LinAccY (m/s2)": accel_vec[:, 1],
        "LinAccZ (m/s2)": accel_vec[:, 2],
        "LinAccRes (m/s2)": accel_mag,

        "RotVelX (rad/s)": gyro_vec[:, 0],
        "RotVelY (rad/s)": gyro_vec[:, 1],
        "RotVelZ (rad/s)": gyro_vec[:, 2],
        "RotVelRes (rad/s)": gyro_mag,

        "RotAccX (rad/s2)": rot_acc_vec[:, 0],
        "RotAccY (rad/s2)": rot_acc_vec[:, 1],
        "RotAccZ (rad/s2)": rot_acc_vec[:, 2],
        "RotAccRes (rad/s2)": rot_acc_mag,
        
        **extra_cols
    })

    return df, ctx
