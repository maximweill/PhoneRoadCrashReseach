import pandas as pd
from ..core import Context

# =========================================================
# LOGGING TRANSFORMS
# =========================================================
def normalize_log_columns(df: pd.DataFrame, ctx: Context):
    # Aggressive cleaning for logbooks: convert to snake_case
    df.columns = (
        df.columns
        .str.strip()
        .str.replace(r"[\s/-]+", "_", regex=True)
        .str.replace(r"[^a-zA-Z0-9_]", "", regex=True)
        .str.replace(r"__+", "_", regex=True)
        .str.strip("_")
        .str.lower()
    )
    return df, ctx


def drop_failed_rows(df: pd.DataFrame, ctx: Context):
    df = df[df["successful"]=="TRUE"]
    return df, ctx

def drop_empty_rows(df: pd.DataFrame, ctx: Context):
    df = df.dropna(how="all")
    df = df.dropna(axis=1, how="all")
    return df, ctx

def drop_no_calibration(df: pd.DataFrame, ctx: Context):
    mask = df["file_name"].str.contains("continuous", na=False)
    df = df[mask]
    return df, ctx


def extract_phone_id(df: pd.DataFrame, ctx: Context):
    if "file_name" in df.columns:
        df["phone_id"] = df["file_name"].str.extract(r"(Phone\d+)", expand=False)
        df["phone_id"] = df["phone_id"].fillna("Headform")

    return df, ctx

def drop_headform(df: pd.DataFrame, ctx: Context):
    df = df[df["phone_id"].ne("Headform")].copy()
    return df, ctx


def parse_test_metadata(df: pd.DataFrame, ctx: Context):
    """
    Extract structured fields from raw log rows.
    """
    df["config"] = df["test_configuration"].fillna("unknown")
    df["target_speed_mps"] = pd.to_numeric(df["target_speed"], errors="coerce")
    df = df.drop(columns=["test_configuration","target_speed"])

    return df, ctx

def extract_repeat_from_test_name(df: pd.DataFrame, ctx: Context):
    """
    Extracts repeat information (V1, REPEAT1, etc.) from the Test_name column.
    """
    df["repeat"] = (
        df["test_name"]
        .astype(str)
        .str.extract(r'(?i)\bv(\d+)\b', expand=False)
        .astype("Int64")  # keeps it nullable-safe
    )

    return df, ctx

def combined_time(df: pd.DataFrame, ctx: Context):

    df["global_time"] = pd.to_datetime(
        df["date"].astype(str) + " " + df["test_time"].astype(str),
        errors="coerce"
    )
    df = df.drop(columns=["date","test_time"])


    return df, ctx

def clean_speed_column(df: pd.DataFrame, ctx: Context):
    # sometimes "Speed (m/s)" or "Speed" or empty
    for col in df.columns:
        if "speed" in col.lower() and "target" not in col.lower():
            df["measured_speed_mps"] = pd.to_numeric(df[col], errors="coerce")
            df = df.drop(columns=[col])
            break

    return df, ctx

def rename_continuous_test_files(df: pd.DataFrame, ctx: Context, add_trigger0:bool = False):
    
    # Ensure ctx has an errors list
    ctx.setdefault("errors", [])
    df["trigger"] = -1

    # Find duplicated filenames
    duplicated_mask = df["file_name"].duplicated(keep=False)

    new_rows = []

    for file_name, group_idx in df[duplicated_mask].groupby("file_name").groups.items():

        if "continuous" not in file_name.lower():
            ctx["errors"].append(
                f"Duplicate non-continuous filename found: {file_name}"
            )
            continue
        
        if "." in file_name:
            file_name, _ = file_name.rsplit(".", 1)

        first_idx = group_idx[0]

        if add_trigger0:
            row = df.loc[first_idx].copy()

            row["file_name"] = f"{file_name}_trigger0"
            row["trigger"] = 0
            row["global_time"] -= pd.Timedelta(minutes=10)# around
            row["target_speed_mps"] = int(row["global_time"].timestamp())
            row["measured_speed_mps"] = 0
            row["repeat"] = -1

            new_rows.append(row)

        for trigger_num, idx in enumerate(group_idx, start=1):
            df.at[idx, "file_name"] = f"{file_name}_trigger{trigger_num}"
            df.at[idx, "trigger"] = trigger_num

    if new_rows:
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    df = df.sort_values("global_time").reset_index(drop=True)

    return df, ctx