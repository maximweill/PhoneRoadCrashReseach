from .core import Path,Context,pd


# =========================================================
# LOADER
# =========================================================

from pathlib import Path
import pandas as pd
import re
from datetime import datetime

def parse_file_date(input_path:Path)->dict:
    filename = input_path.stem  # e.g., 'crash_data_20260313_165424__Phone003'
    
    # Matches exactly 8 digits, an underscore, and 6 digits anywhere in the name
    match = re.search(r"(\d{8})_(\d{6})", filename)
    
    if match:
        date_str = match.group(1)  # Captures the 8 digits (e.g., '20260313')
        time_str = match.group(2)  # Captures the 6 digits (e.g., '165424')
        
        try:
            date = {}
            dt = datetime.strptime(f"{date_str}_{time_str}", "%Y%m%d_%H%M%S")
            date["Date"] = dt.strftime("%Y-%m-%d")
            date["Time"] = dt.strftime("%H:%M:%S")
            return date
        except ValueError:
            return {}
    return {}
    


def load_csv(input_path: Path, context: Context):
    # Initialize the metadata dictionary
    metadata = {}
    
    # Read the file line by line to extract comments/metadata
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.startswith("#"):
                # Strip the '#' and whitespace, then try to split by the first ':'
                clean_line = line.lstrip("#").strip()
                if ":" in clean_line:
                    key, val = clean_line.split(":", 1)
                    metadata[key.strip()] = val.strip()
            else:
                # Stop reading once we hit the actual data rows to save time
                break

    if "Date" not in metadata:
        metadata.update(parse_file_date(input_path=input_path))

    # Load the dataframe using the 'c' engine as before
    df = pd.read_csv(
        input_path,
        comment="#",
        engine="c",
    )

    # Update context
    context["input_path"] = input_path
    if "metadata" in context:
        context["metadata"].update(metadata)
    else:
        context["metadata"] = metadata
    
    return df, context

def load_excel(input_path: Path, context: Context):
    df = pd.read_excel(input_path)
    context["input_path"] = input_path
    return df, context



# =========================================================
# FRAMER LOADER
# =========================================================

def load_phone_drop_with_ref(input_path: Path, context: dict) -> tuple[pd.DataFrame, dict]:
    context["metadata"]={}


    if not input_path.exists():
        context["skip"] = True
        context["skip_reason"] = "input_doesnt_exist"
        return None, context
    
    ref_path = context["ref_path"]
    if not ref_path.exists():
        context["skip"] = True
        context["skip_reason"] = "ref_doesnt_exist"
        return None, context
    
    if input_path.suffix == ".csv":
        df,context = load_csv(input_path=input_path,context=context)
    elif  input_path.suffix == ".xlsx":
        df = pd.read_excel(input_path)
    else:
        context["skip"] = True
        context["skip_reason"] = f"input extension not recognised {input_path.suffix}"
        return None, context

    ref_df,ref_ctx = load_csv(input_path=ref_path,context={})
    context["metadata"].update(ref_ctx["metadata"])

    context["ref_df"] = ref_df
    context["ref_path"] = ref_path

    return df, context

# =========================================================
# PARSING SAVERS
# =========================================================


def save_single_csv(df: pd.DataFrame, context: Context) -> list[Path]:
    input_path: Path = context["input_path"]
    metadata: dict = context.get("metadata", {})  # Use .get() in case metadata isn't present

    if "output_path" in context:
        output_path:Path = context["output_path"]
    else:
        output_dir: Path = context["output_dir"]
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{input_path.stem}.csv"


    # 1. Open the file in write mode ('w') to write the metadata headers
    with open(output_path, "w", encoding="utf-8") as f:
        for key, value in metadata.items():
            f.write(f"# {key}: {value}\n")
            
    # 2. Open the file in append mode ('a') for pandas to dump the dataframe
    with open(output_path, "a", encoding="utf-8", newline="") as f:
        df.to_csv(f, index=False)

    context["output"] = output_path
    return [output_path]



def save_split_by_trigger(df: pd.DataFrame, context: Context) -> list[Path]:

    if "trigger" not in df.columns:
        raise ValueError("Missing trigger column")

    output_dir: Path = context["output_dir"]
    input_path: Path = context["input_path"]

    output_dir.mkdir(parents=True, exist_ok=True)

    outputs: list[Path] = []

    for trig in sorted(df["trigger"].unique()):

        sub = df[df["trigger"] == trig].reset_index(drop=True)

        out = output_dir / f"{input_path.stem}_trigger{trig}.csv"
        context["output_path"] = out
        save_single_csv(sub,context)
        outputs.append(out)

    return outputs

# =========================================================
# FRAMING SAVERS
# =========================================================

def save_stationary(df: pd.DataFrame, context: Context, both = False) -> list[Path]:

    file: Path = context["input_path"]
    output_dir: Path = context["output_dir"]

    output_dir.mkdir(parents=True, exist_ok=True)
    parts = file.stem.split("_")

    if not both:
        sensor, _, date, time, phone_id = parts[:5]
        out_name = f"{sensor}_stationary_{date}_{time}_{phone_id}.csv"
    else:
        _, date, time, phone_id = parts[:4]
        out_name = f"both_stationary_{date}_{time}_{phone_id}.csv"
    out_path = output_dir / out_name

    context["output_path"]=out_path

    return save_single_csv(df,context)

def save_allan_variance(df: pd.DataFrame, context: Context) -> list[Path]:
    """
    Saves the Allan variance results with an '_allan.csv' suffix.
    """
    if df is None:
        return []
        
    output_dir: Path = context["output_dir"]
    input_path: Path = context["input_path"]
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    out_name = f"{input_path.stem}_allan.csv"
    out_path = output_dir / out_name

    context["output_path"]=out_path

    return save_single_csv(df,context)

def null_saver(df: pd.DataFrame, context: Context) -> list[Path]:
    """
    Does not save anything to disk. Used when we only care about the context results.
    """
    return []


# =========================================================
# Logging SAVERS
# =========================================================


def load_raw_log_csv(path: Path, ctx: Context):
    # IMPORTANT: logs are messy, so we avoid strict parsing
    df = pd.read_csv(path, comment="#", dtype=str)

    ctx["input_path"] = path
    return df, ctx