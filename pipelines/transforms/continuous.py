import pandas as pd
import numpy as np
from ..core import Context

# =========================================================
# CONTINUOUS DROPS & CALIBRATION TRANSFORMS
# =========================================================
def drop_motion(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    max_rot = 5.0 * np.pi / 180
    rot_mask = df["RotVelRes (rad/s)"] <= max_rot

    max_accel = 12
    accel_mask = df["LinAccRes (m/s2)"] <= max_accel

    df = df.loc[rot_mask&accel_mask].reset_index(drop=True)

    return df, ctx



def get_block_indices(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    triggered = df["triggered"].to_numpy()

    blocks = []

    # find change points
    change_points = np.where(np.diff(triggered, prepend=triggered[0] - 1))[0]

    for i in range(len(change_points)):
        start = change_points[i]

        if i + 1 < len(change_points):
            end = change_points[i + 1] - 1
        else:
            end = len(triggered) - 1

        blocks.append((start, end))

    df = df.drop(columns=["triggered"])

    ctx["t0"] = df["Time (s)"].iloc[blocks[0][1]]
    ctx["initial_block_end_times"] = [f"{df["Time (s)"].iloc[end]-ctx["t0"]:.0f}" for _, end in blocks]
    ctx["blocks"] = blocks
    ctx["initial_n"] = len(blocks)
    

    return df, ctx

def filter_blocks_by_duration(
    df: pd.DataFrame,
    ctx: Context,
    min_interval_s: float = 150.0
) -> tuple[pd.DataFrame, Context]:

    blocks = ctx["blocks"]

    time = df["Time (s)"].to_numpy()
    new_blocks = []
    for start, end in blocks:
        duration = time[end] - time[start]
        if duration > min_interval_s:
            new_blocks.append((start, end))
        else:
            #combine with prev
            new_blocks[-1] = [new_blocks[-1][0],end]

    ctx["duration_filter_block_end_times"] = [f"{time[end]-ctx["t0"]:.0f}" for _, end in new_blocks]
    ctx["blocks"] = new_blocks
    ctx["duration_filtered_n"] = len(new_blocks)
    return df, ctx

def nudge_blocks(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:

    blocks = ctx["blocks"]
    time = df["Time (s)"]
    buffer:float = 5.0 #to account for phones triggering y target at different times

    new_blocks = [blocks[0]] #skip t0 where there is no drop

    for start, end in blocks[1:]:
        start_time = time.iloc[start]
        start_target_time = start_time + buffer
        end_time = time.iloc[end]
        end_target_time = end_time + buffer

        # closest index where time >= target (fallback to nearest overall)
        start_candidates = (time - start_target_time).abs().to_numpy()
        new_start = int(start_candidates.argmin())
        end_candidates = (time - end_target_time).abs().to_numpy()
        new_end = int(end_candidates.argmin())

        new_blocks.append((new_start, new_end))

    ctx["blocks"] = new_blocks
    ctx["nudged_end_times"] = [f"{time[end]-ctx["t0"]:.0f}" for _, end in new_blocks]
    return df, ctx

def recalc_triggered_by_blocks(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:

    blocks = ctx["blocks"]
    df["triggered"] = np.nan
    for t_value,(start,end) in enumerate(blocks):
        df.loc[start:end-1, "triggered"] = t_value
    df["triggered"] = df["triggered"].astype("Int64")
    return df, ctx

def axis_column(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:
    
    accel_cols = [
        "LinAccX (m/s2)",
        "LinAccY (m/s2)",
        "LinAccZ (m/s2)",
    ]

    accel = df[accel_cols].to_numpy()

    # dominant axis by absolute acceleration
    dominant_idx = np.abs(accel).argmax(axis=1)

    axis_names = np.array(["x", "y", "z"])
    signs = np.sign(accel[np.arange(len(df)), dominant_idx])

    axis = axis_names[dominant_idx]
    axis = np.where(signs < 0, "-" + axis, axis)

    df = df.copy()
    df["axis"] = axis

    return df, ctx

def _choose_best(lst:list,max_start_idx:float = np.inf)->dict:
    ori_df = pd.DataFrame(lst)
    if len(ori_df) == 0:
        return {}
    
    defore_end = ori_df["start"] < max_start_idx
    long_enough = ori_df["duration"] > 1
    
    mask = defore_end & long_enough
    if not mask.any():
        return {}

    valid = ori_df[mask]
    best_row = valid.loc[valid["end"].idxmax()] #priority to the later ones that were def after the drop

    return  best_row.to_dict()


def choose_axis_group(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:

    axes = df["axis"].to_numpy()
    time = df["Time (s)"].to_numpy()

    start = 0
    end = len(df)-1

    ctx["missing"] = []

    classification = {
        "x": [], "-x": [],
        "y": [], "-y": [],
        "z": [], "-z": [],
    }

    current_ori = axes[start]
    current_ori_start = start

    for idx in range(start + 1, end):
        ori = axes[idx]

        if ori != current_ori or idx == end-1:
            dt = time[idx] - time[current_ori_start]
            if dt > 0.2: #worth saving for debugs
                classification[current_ori].append({
                    "duration": dt,
                    "start":current_ori_start,
                    "end": idx,
                    "start_time": time[current_ori_start],
                    "end_time": time[idx],
                })
            current_ori = ori
            current_ori_start = idx

    chosen = {
        "x": {}, "-x": {},
        "y": {}, "-y": {},
        "z": {}, "-z": {},
    }
    end_ori = ctx["metadata"]["Target Orientation"]
    chosen_ending = _choose_best(classification[end_ori])

    for ori, lst in classification.items():
        if ori == end_ori:
            chosen[ori] = chosen_ending
        else:
            if len(chosen_ending)>0:
                chosen[ori] = _choose_best(lst,chosen_ending["end"])
            else:
                chosen[ori] = _choose_best(lst)
        
    col_idx = df.columns.get_loc("axis")
    df.iloc[start:end, col_idx] = np.nan

    for ori, params in chosen.items():
        if params:
            ori_start, ori_end = int(params["start"]),int(params["end"])
            df.iloc[ori_start:ori_end, col_idx] = ori
        else:
            ctx["missing"].append(ori)

    #for debugging
    ctx["chosen"] = {key:pd.DataFrame([d]).astype(int) for key,d in chosen.items()}
    ctx["classification"] = {key:pd.DataFrame(lst).astype(int) for key,lst in classification.items()}

    return df, ctx

def drop_nan(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    df = df.dropna()
    return df,ctx