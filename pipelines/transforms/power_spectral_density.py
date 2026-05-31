import pandas as pd
import numpy as np
from ..core import Context
from ..helper import _compute_psd

# =========================================================
# PSD TRANSFORMS
# =========================================================

def calculate_psd_transform(df: pd.DataFrame, ctx: Context, both=False) -> tuple[pd.DataFrame, Context]:
    """
    Computes Power Spectral Density for relevant columns in the dataframe.
    """
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
        print(f"  Skipping {filename}: No relevant columns found.")
        return None, ctx

    # Sampling frequency
    t = df["Time (s)"].to_numpy()
    dt = np.median(np.diff(t))

    if pd.isna(dt) or dt <= 0:
        print(f"  Skipping {filename}: Invalid sampling interval (dt={dt}).")
        return None, ctx

    fs = 1.0 / dt

    all_results = {}
    max_freqs = None

    for col in cols:
        data = df[col].values
        freqs, psd = _compute_psd(data, fs)

        if len(freqs) > 0:
            all_results[f"{col}_psd"] = psd
            if max_freqs is None or len(max_freqs) < len(freqs):
                max_freqs = freqs

    if not all_results:
        return None, ctx

    res_df = pd.DataFrame({"freq_hz": max_freqs})
    for col_name, psd_vals in all_results.items():
        # Ensure psd_vals matches the length of max_freqs (should be true if nperseg is constant)
        if len(psd_vals) == len(max_freqs):
            res_df[col_name] = psd_vals
        else:
            # Handle cases where lengths might differ (though unlikely with constant nperseg)
            print(f"  Warning: PSD length mismatch for {col_name} in {filename}")

    return res_df, ctx
