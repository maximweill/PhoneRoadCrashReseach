import pandas as pd
import numpy as np
from scipy.interpolate import interp1d
from scipy import signal
from ..core import Context

# =========================================================
# FRAMING TRANSFORMS
# =========================================================

def ref_timestamps_matching(df: pd.DataFrame, ctx: Context):
    ref_time = ctx["ref_df"]["Time (s)"]

    t_orig = df["Time (s)"]
    t_new = ref_time.copy()

    resampled_data = {"Time (s)": ref_time.copy()}

    # Resample each sensor column
    for col in df.columns:
        if col == "Time (s)":
            continue

        # interp1d for linear interpolation
        f = interp1d(t_orig, df[col].to_numpy(), kind='linear', fill_value="extrapolate")
        resampled_data[col] = f(t_new)

    df_new = pd.DataFrame(resampled_data)

    ctx["len_ratio"] = len(df)/len(df_new)

    return df_new, ctx

def compute_lag(df: pd.DataFrame, ctx: Context):
    ref_df = ctx["ref_df"]

    sig_col = ctx.get("signal_col", "RotVelX (rad/s)")
    ref_sig_col = ctx.get("ref_signal_col", "RotVelX (rad/s)")

    sig_p = np.nan_to_num(df[sig_col].to_numpy())
    sig_r = np.nan_to_num(ref_df[ref_sig_col].to_numpy())

    sig_p = (sig_p - sig_p.mean()) / (sig_p.std() + 1e-9)
    sig_r = (sig_r - sig_r.mean()) / (sig_r.std() + 1e-9)

    corr = signal.correlate(sig_p, sig_r, mode="full", method="fft")
    lags = signal.correlation_lags(len(sig_p), len(sig_r), mode="full")

    ctx["lag_idx"] = int(lags[np.argmax(corr)])
    return df, ctx

def align_to_reference(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:

    lag = int(ctx.get("lag_idx", 0))
    ref_df = ctx["ref_df"]

    if lag >= 0:
        start_df = lag
        start_ref = 0
    else:
        start_df = 0
        start_ref = -lag

    overlap = min(
        len(df) - start_df,
        len(ref_df) - start_ref,
    )

    if overlap <= 0:
        raise ValueError("No overlap between signals")

    df_aligned = df.iloc[start_df:start_df + overlap].copy()

    t_df = df_aligned["Time (s)"].iloc[0]
    t_ref = ref_df["Time (s)"].iloc[start_ref]

    offset = t_ref - t_df
    df_aligned["Time (s)"] += offset

    ctx["offset"] = offset

    return df_aligned, ctx

def trim_stationary(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:

    threshold = ctx.get("impact_threshold", 11)

    t = df["Time (s)"].to_numpy()
    acc = df["LinAccRes (m/s2)"].to_numpy()

    mask = acc > threshold
    impact_times = t[mask]

    t_start, t_end = t[0], t[-1]
    t_mid = (t_start + t_end) / 2

    first = impact_times[impact_times < t_mid]
    second = impact_times[impact_times > t_mid]

    start_time = first[-1] if len(first) else t_start
    end_time = second[0] if len(second) else t_end

    start_time += ctx.get("buffer_after", 600)
    end_time -= ctx.get("buffer_before", 60)

    df = df[(df["Time (s)"] > start_time) & (df["Time (s)"] < end_time)].copy()

    if len(df) > 0:
        df["Time (s)"] -= df["Time (s)"].iloc[0]

    return df, ctx

def trim_reference(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:

    start_time = -0.1
    end_time = 0.4

    df = df[(df["Time (s)"] > start_time) & (df["Time (s)"] < end_time)].copy()

    return df, ctx


def match_indices_ref(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:

    ref_df = ctx["ref_df"].reset_index(drop=True)
    df = df.reset_index(drop=True)
    ctx["len_match"] = len(ref_df) - len(df)
    time_diff = ref_df["Time (s)"]-df["Time (s)"]
    ctx["time_match_mean"] = time_diff.mean()
    ctx["time_match_iqr"] = (
        time_diff.quantile(0.75)
        - time_diff.quantile(0.25)
    )


    new_df = pd.concat(
        [
            ref_df.add_suffix("_ref"),
            df.add_suffix("_framed"),
        ],
        axis=1,
    )
    ctx["skip_col"] = []
    for col in df:
        if col not in ref_df.columns:
            ctx["skip_col"].append(col)
            new_df.drop(columns=[f"{col}_framed"], inplace=True)
            continue
        
        new_df[f"{col}_diff"] = new_df[f"{col}_ref"]-new_df[f"{col}_framed"]
    return new_df, ctx
    

def drop_time(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:
    for col in df.columns:
        if "Time" in col:
            df.drop(columns=[col], inplace=True)
    return df, ctx
