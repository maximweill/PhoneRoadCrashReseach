import pandas as pd
import numpy as np
import re
from pathlib import Path
from scipy.signal import butter, sosfiltfilt
from ..core import Context

ALLOWED_CUTOFFS = [200, 160, 80]  # nyquist freq * 0.8

def nearest(value, choices):
    def distance(x):
        return abs(x - value)

    return min(choices, key=distance)

def CFC_filter(df: pd.DataFrame, ctx: Context, cutoff_override: float = None) -> tuple[pd.DataFrame, Context]:
    """
        Applies a zero-phase, 4-pole Butterworth low-pass filter to an impact signal.
        
        This function utilizes the mathematical architecture specified by the 
        SAE J211-1 and ISO 6487 standards for biomechanical impact data (Channel 
        Frequency Class filtering). However, it accepts a custom cutoff frequency 
        rather than restricting the user to the standard CFC bins (60, 180, 600, 1000).
        
        This is primarily intended to be used as a bespoke anti-aliasing filter 
        prior to downsampling high-frequency reference data to match the physical 
        Nyquist limits of lower-frequency commercial sensors.

        Returns
        -------
            The filtered signal, It maintains the 
            exact same length and phase as the input `data`.

        Notes
        -----
        - **Architecture:** Uses a 4th-order Butterworth digital filter.
        - **Zero-Phase:** Uses `scipy.signal.sosfiltfilt` to apply the filter both 
        forward and backward. This achieves the zero-phase (phaseless) requirement 
        of SAE J211-1, ensuring the impact peak does not shift in time.
        - **Stability:** Uses Second-Order Sections (SOS) formatting rather than 
        [b, a] coefficients to maintain numerical stability, especially critical 
        when the sampling rate is exceptionally high compared to the cutoff.
    """

    if cutoff_override:
        nearest_defined_cutoff = cutoff_override
    else:
        ctx.setdefault("errors", [])
        chars_path = ctx.get("phone_characteristics_aggregated")
        
        if not chars_path:
            return df, ctx

        chars_path = Path(chars_path)
        if not chars_path.exists():
            ctx["errors"].append(f"Characteristics file not found at {chars_path}")
            return df, ctx

        chars_df = pd.read_csv(chars_path)

        # Extract phone ID from reference filename
        filename = ctx["input_path"].name
        match = re.search(r"(Phone\d+)", filename)
        if not match:
            ctx["errors"].append(f"Could not extract Phone ID from {filename}")
            return df, ctx

        phone_id = match.group(1)

        # Get target frequency
        phone_info = chars_df[chars_df["phone_id"] == phone_id]
        if phone_info.empty:
            ctx["errors"].append(f"No characteristics found for {phone_id} in {chars_path}")
            return df, ctx

        fs_target = phone_info["fs_median"].iloc[0]
        nyquist_frequency = fs_target / 2.0
        ideal_cutoff = nyquist_frequency * 0.8  # enough space for filter roll off
        nearest_defined_cutoff = nearest(ideal_cutoff, ALLOWED_CUTOFFS)

    ctx["nearest_defined_cutoff"] = nearest_defined_cutoff

    # Calculate source sampling frequency
    if "Time (s)" in df.columns:
        dt = df["Time (s)"].diff().median()
        fs_src = 1.0 / dt
    else:
        ctx.setdefault("errors", [])
        ctx["errors"].append(f"No time column found in {filename} for sampling rate calculation")
        return df, ctx

    # Design 2nd-order Butterworth filter
    # Applying it with sosfiltfilt makes it 4th-order and zero-phase.
    # Ensure cutoff is below Nyquist of source signal
    cutoff = min(nearest_defined_cutoff, fs_src * 0.45)
    sos = butter(N=2, Wn=cutoff, btype='low', fs=fs_src, output='sos')

    df_new = df.copy()
    
    # Select numeric columns to filter, excluding time columns
    time_cols = {"Time (s)", "index"}
    for col in df.columns:
        if col in time_cols:
            continue
        
        if pd.api.types.is_numeric_dtype(df[col]):
            # Apply filter using SOS for numerical stability
            # Use nan_to_num to handle any potential NaNs which would break the filter
            signal_data = np.nan_to_num(df[col].to_numpy())
            df_new[col] = sosfiltfilt(sos, signal_data)

    return df_new, ctx
