import pandas as pd
import numpy as np
import re
from pathlib import Path
import warnings
from scipy.stats import pearsonr, ttest_1samp
import statsmodels.api as sm
import pingouin as pg
from ..core import Context

# =========================================================
# AGREEMENT TRANSFORMS
# =========================================================
def reset_index(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    df_rest = df.reset_index(drop=True)
    ctx["bug"] = [] #create a bugs catcher
    return df_rest, ctx 

def ignore_saturated(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    """
    Identifies and masks saturated sensor readings based on device-specific ranges.
    """
    # 0. Find the characteristics file
    # Fully rely on the drop characteristics file
    chars_path = Path("data_processing_gitignore/phone_characteristics/aggregated/characteristics_drops.csv")
            
    if not chars_path.exists():
        print(f"Warning: Characteristics file not found at {chars_path}. Skipping ignore_saturated.")
        return df, ctx

    # 1. Extract phone_id from filename
    input_path = ctx.get("input_path")
    if not input_path:
        print("Warning: input_path not found in context. Skipping ignore_saturated.")
        return df, ctx
        
    filename = input_path.name
    match = re.search(r"(Phone\d+)", filename)
    if not match:
        print(f"Warning: Could not extract Phone ID from {filename}. Skipping ignore_saturated.")
        return df, ctx
    phone_id = match.group(1)

    # 2. Load characteristics data
    chars_df = pd.read_csv(chars_path)
    phone_info = chars_df[chars_df["phone_id"] == phone_id]
    if phone_info.empty:
        print(f"Warning: No characteristics found for {phone_id} in {chars_path}. Skipping ignore_saturated.")
        return df, ctx

    # 3. Get ranges (handling potential NaNs)
    acc_range = phone_info["accelerometer_range"].iloc[0]
    gyro_range = phone_info["gyroscope_range"].iloc[0]
    
    if pd.isna(acc_range) or pd.isna(gyro_range):
        print(f"Warning: Missing range data for {phone_id}. Skipping ignore_saturated.")
        return df, ctx

    # 4. Define thresholds (using a 2% tolerance to account for near-saturation effects)
    acc_thresh = acc_range * 0.98
    gyro_thresh = gyro_range * 0.98

    # 5. Identify saturated indices
    # We check the '_framed' columns for saturation
    acc_cols = ["LinAccX (m/s2)", "LinAccY (m/s2)", "LinAccZ (m/s2)"]
    gyro_cols = ["RotVelX (rad/s)", "RotVelY (rad/s)", "RotVelZ (rad/s)"]

    acc_mask = pd.Series(False, index=df.index)
    for col in acc_cols:
        c = f"{col}_framed"
        if c in df.columns:
            acc_mask |= df[c].abs() > acc_thresh

    gyro_mask = pd.Series(False, index=df.index)
    for col in gyro_cols:
        c = f"{col}_framed"
        if c in df.columns:
            gyro_mask |= df[c].abs() > gyro_thresh

    # 6. Define effected column groups
    # Saturated accel affects components and magnitude
    acc_group = acc_cols + ["LinAccRes (m/s2)"]
    
    # Saturated gyro affects components, magnitude, and all rotational accelerations
    gyro_group = gyro_cols + [
        "RotVelRes (rad/s)", 
        "RotAccX (rad/s2)", "RotAccY (rad/s2)", "RotAccZ (rad/s2)", "RotAccRes (rad/s2)"
    ]

    # Magnetometer columns
    mag_cols = [c.replace("_framed", "") for c in df.columns if "mag" in c.lower() and c.endswith("_framed")]

    # 7. Apply masking (mask _framed, _ref, and _diff for the same indices)
    for col_stem in acc_group:
        for suffix in ["_framed", "_ref", "_diff"]:
            c = f"{col_stem}{suffix}"
            if c in df.columns:
                df.loc[acc_mask, c] = np.nan

    for col_stem in gyro_group:
        for suffix in ["_framed", "_ref", "_diff"]:
            c = f"{col_stem}{suffix}"
            if c in df.columns:
                df.loc[gyro_mask, c] = np.nan

    # Mask mag if either sensor is saturated
    for col_stem in mag_cols:
        for suffix in ["_framed", "_ref", "_diff"]:
            c = f"{col_stem}{suffix}"
            if c in df.columns:
                df.loc[acc_mask | gyro_mask, c] = np.nan

    ctx["acc_saturated_samples"] = int(acc_mask.sum())
    ctx["gyro_saturated_samples"] = int(gyro_mask.sum())

    return df, ctx


def compute_n(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    ctx["n"] = len(df)
    # Also record valid sample counts for each diff column
    for col in df:
        if col.endswith("_diff"):
            ctx[f"{col}_n_valid"] = int(df[col].notna().sum())
    return df, ctx

def compute_mae_from_ideal(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    for col in df:
        if col.endswith("_diff"):
            # pandas mean() skips NaNs by default
            ctx[f"{col}_mae"] = df[col].abs().mean()
    return df, ctx

def compute_pearson_correlation(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    for col in df:
        if col.endswith("_diff"):
            stem = col.replace("_diff", "")
            
            # Pairwise drop NaNs
            valid_mask = df[f"{stem}_ref"].notna() & df[f"{stem}_framed"].notna()
            if valid_mask.sum() < 2:
                continue
                
            r, p = pearsonr(df.loc[valid_mask, f"{stem}_ref"], df.loc[valid_mask, f"{stem}_framed"])
            ctx[f"{stem}_pearson_r"] = r
            ctx[f"{stem}_pearson_p"] = p

    return df, ctx

def compute_trendline_regression(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    for col in df:
        if col.endswith("_diff"):
            stem = col.replace("_diff", "")

            # Pairwise drop NaNs
            valid_mask = df[f"{stem}_ref"].notna() & df[f"{stem}_framed"].notna()
            if valid_mask.sum() < 2:
                continue

            x = df.loc[valid_mask, f"{stem}_ref"]
            y = df.loc[valid_mask, f"{stem}_framed"]

            X = sm.add_constant(x)
            model = sm.OLS(y, X).fit()

            # parameters
            intercept = model.params.iloc[0]
            slope = model.params.iloc[1]

            # predictions
            y_hat = model.predict(X)

            # RMSE of regression fit
            rmse = np.sqrt(np.mean((y - y_hat) ** 2))

            # store metrics
            ctx[f"{stem}_slope"] = slope
            ctx[f"{stem}_intercept"] = intercept
            ctx[f"{stem}_trend_rmse"] = rmse
            ctx[f"{stem}_r2"] = model.rsquared

    return df, ctx

def compute_icc(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:

    for col in df:
        if col.endswith("_diff"):
            stem = col.replace("_diff", "")
            ref_col = f"{stem}_ref"
            framed_col = f"{stem}_framed"

            # Check if columns actually exist in the DataFrame
            if ref_col not in df.columns or framed_col not in df.columns:
                ctx["bug"].append(f"Skipped {col}: Matching ref/framed columns not found.")
                continue

            # Pairwise drop NaNs
            valid_mask = df[ref_col].notna() & df[framed_col].notna()
            n = valid_mask.sum()
            
            if n < 2:
                ctx["bug"].append(f"icc failed {col}: Insufficient data points (n={n}).")
                continue

            x = df.loc[valid_mask, ref_col].to_numpy()
            y = df.loc[valid_mask, framed_col].to_numpy()

            # Pre-flight check: Zero variance breaks matrix math
            if np.var(x) == 0 and np.var(y) == 0:
                ctx["bug"].append(f"icc failed {col}: Zero variance in signals (n={n}).")
                continue

            # --- build long format ---
            data = pd.DataFrame({
                "targets": np.repeat(np.arange(n), 2),
                "raters": ["ref", "framed"] * n,
                "values": np.ravel(np.column_stack([x, y]))
            })

            # Catch run-time issues safely
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                try:
                    icc_result = pg.intraclass_corr(
                        data=data,
                        targets="targets",
                        raters="raters",
                        ratings="values"
                    )
                except Exception as e:
                    ctx["bug"].append(f"icc failed {col}: Pingouin exception -> {str(e)}")
                    continue

            icc_row = icc_result[icc_result["Type"].isin(["ICC(A,1)", "ICC(A,k)"])]

            if icc_row.empty:
                warning_msg = f" ({w[0].message})" if w else ""
                ctx["bug"].append(f"icc failed {col}: 'ICC(A,1)'/'ICC(A,k)' missing from output{warning_msg}.")
                continue

            # 'ICC' remains the same, but 'CI95%' is now 'CI95'
            icc_value = icc_row.iloc[0]["ICC"]
            ci95 = icc_row.iloc[0]["CI95"] 

            if np.isnan(icc_value):
                ctx["bug"].append(f"icc failed {col}: Result is NaN.")
                continue

            ctx[f"{stem}_icc"] = icc_value
            ctx[f"{stem}_icc_ci95_lower"] = ci95[0]
            ctx[f"{stem}_icc_ci95_upper"] = ci95[1]

    return df, ctx

def compute_bland_altman(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    """
    Bland Altman agreement analysis per channel.
    """

    for col in df:
        if col.endswith("_diff"):
            stem = col.replace("_diff", "")

            diff = df[col].dropna().to_numpy()

            if len(diff) < 2:
                continue

            bias = np.mean(diff)
            sd = np.std(diff)

            # Use the already dropped-nan diff for the t-test
            t_stat, p = ttest_1samp(diff, 0)
            ctx[f"{col}_bias_t_stat"] = t_stat
            ctx[f"{col}_bias_p"] = p

            loa_upper = bias + 1.96 * sd
            loa_lower = bias - 1.96 * sd

            ctx[f"{stem}_ba_sd"] = sd
            ctx[f"{stem}_ba_loa_upper"] = loa_upper
            ctx[f"{stem}_ba_loa_lower"] = loa_lower

    return df, ctx
