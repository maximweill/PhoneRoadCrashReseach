import pandas as pd
import numpy as np
from ..core import Context
from ..helper import _compute_allan_variance

# =========================================================
# ALLAN VARIANCE TRANSFORMS
# =========================================================

def calculate_allan_variance_transform(df: pd.DataFrame, ctx: Context,both = False) -> tuple[pd.DataFrame, Context]:
    """
    Computes Allan variance for relevant columns in the dataframe.
    """
    ctx.setdefault("errors", [])
    filename = ctx["input_path"].name

    # Determine columns to analyze based on file prefix
    if not both and (filename.startswith("accel") or filename.startswith("gyro")):
        if filename.startswith("accel"):
            cols = ["LinAccX (m/s2)", "LinAccY (m/s2)", "LinAccZ (m/s2)", "LinAccRes (m/s2)"]
        elif filename.startswith("gyro"):
            cols = ["RotVelX (rad/s)", "RotVelY (rad/s)", "RotVelZ (rad/s)", "RotVelRes (rad/s)"]
    else:
        # Default to all numeric columns except Time, triggered, and battery info
        cols = [c for c in df.columns if any(x in c for x in ["Acc", "Vel", "Rot"])]

    # Filtering existing columns
    cols = [c for c in cols if c in df.columns]

    if not cols:
        ctx["errors"].append(f"Skipping {filename}: No relevant columns found.")
        return None, ctx

    # Sampling interval
    t = df["Time (s)"].to_numpy()
    dt = np.median(np.diff(t))

    if pd.isna(dt) or dt <= 0:
        ctx["errors"].append(f"Skipping {filename}: Invalid sampling interval (dt={dt}).")
        return None, ctx

    all_results = {}
    max_taus = []

    for col in cols:
        data = df[col].values
        taus, sigmas = _compute_allan_variance(data, dt)

        if len(taus) > 0:
            all_results[f"{col}_sigma"] = sigmas
            if len(max_taus) < len(taus):
                max_taus = taus

    if not all_results:
        return None, ctx

    res_df = pd.DataFrame({"tau_s": max_taus})
    for col_name, sigmas in all_results.items():
        res_df[col_name] = sigmas

    return res_df, ctx
