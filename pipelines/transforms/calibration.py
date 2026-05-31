import pandas as pd
import numpy as np
import re
from ..core import Context
from .base import G

# =========================================================
# CALIBRATION PARAMETERS TRANSFORMS
# =========================================================

def compute_6axis_calibration(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    """
    Computes 6-axis calibration parameters for accelerometer and gyroscope.
    Expects 'axis' column with labels: x, -x, y, -y, z, -z
    """
    if "axis" not in df.columns:
        print(f"Warning: 'axis' column missing in {ctx.get('input_path', 'unknown file')}")
        return df, ctx

    accel_cols = ["LinAccX (m/s2)", "LinAccY (m/s2)", "LinAccZ (m/s2)"]
    gyro_cols = ["RotVelX (rad/s)", "RotVelY (rad/s)", "RotVelZ (rad/s)"]

    # Ensure columns exist
    for col in accel_cols + gyro_cols:
        if col not in df.columns:
            print(f"Warning: Column {col} missing in {ctx.get('input_path', 'unknown file')}")
            return df, ctx

    # Group by axis and compute means
    axis_means = df.groupby("axis")[accel_cols + gyro_cols].mean()

    params = {}

    # Gyro bias is the mean over all stationary data (as it's stationary in all 6 axiss)
    for col in gyro_cols:
        params[f"{col}_bias"] = df[col].mean()

    # Accel parameters: offset and scale for each axis
    for ax in ['X', 'Y', 'Z']:
        col = f"LinAcc{ax} (m/s2)"
        pos_label = ax.lower()
        neg_label = "-" + ax.lower()
        if pos_label not in axis_means.index:
            print(f"Warning: Missing axis for axis {pos_label} in {ctx.get('input_path', 'unknown file')}")
            print(df["axis"].unique())
            print(axis_means.columns)
            params[f"LinAcc{ax}_offset"] = np.nan
            params[f"LinAcc{ax}_scale"] = np.nan
            continue
        if neg_label not in axis_means.index:
            print(f"Warning: Missing axis for axis {neg_label} in {ctx.get('input_path', 'unknown file')}")
            print(df["axis"].unique())
            print(axis_means.columns)
            params[f"LinAcc{ax}_offset"] = np.nan
            params[f"LinAcc{ax}_scale"] = np.nan
            continue

        m_plus = axis_means.loc[pos_label, col]
        m_minus = axis_means.loc[neg_label, col]

        offset = (m_plus + m_minus) / 2
        scale = (m_plus - m_minus) / (2 * G)

        params[f"LinAcc{ax}_offset"] = offset
        params[f"LinAcc{ax}_scale"] = scale



    ctx.update(params)
    return df, ctx

def create_calibration_summary(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    """
    Prepares a final summary dictionary for logging, similar to create_characteristics_summary.
    """
    # 1. Extract phone id from filename if not already present in ctx
    if "phone_id" not in ctx:
        match = re.search(r"(Phone\d+)", ctx["input_path"].name)
        ctx["phone_id"] = match.group(1) if match else "Unknown"

    final_row = {}
    final_row["input_path"] = ctx["input_path"]
    final_row["phone_id"] = ctx["phone_id"]

    # 2. Extract metadata
    metadata = ctx.get("metadata", {})
    for meta_key, meta_val in metadata.items():
        final_row[meta_key] = meta_val

    # 3. Pull calibration results from context
    for key, val in ctx.items():
        if any(suffix in key for suffix in ["_offset", "_scale", "_bias"]):
            final_row[key] = val

    final_row["global_time"] = ctx["global_time"]
    final_row["repeat"] = ctx["log_row"]["repeat"]
    final_row["target_speed_mps"] = ctx["log_row"]["target_speed_mps"]


    return df, final_row
