from .core import Path,Context,pd


# =========================================================
# LOADER
# =========================================================

def load_csv(input_path: Path, context: Context):
    df = pd.read_csv(
        input_path,
        comment="#",
        engine="c",   # fastest stable pandas engine
    )

    context["input_path"] = input_path
    return df, context

def load_excel(input_path: Path, context: Context):
    df = pd.read_excel(input_path)
    context["input_path"] = input_path
    return df, context



# =========================================================
# FRAMER LOADER
# =========================================================

def load_phone_drop_with_ref(input_path: Path, context: dict) -> tuple[pd.DataFrame, dict]:
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
        df = pd.read_csv(input_path)
    elif  input_path.suffix == ".xlsx":
        df = pd.read_excel(input_path)
    else:
        context["skip"] = True
        context["skip_reason"] = f"input extension not recognised {input_path.suffix}"
        return None, context

    ref_df = pd.read_csv(ref_path)

    context["ref_df"] = ref_df
    context["ref_path"] = ref_path

    return df, context

# =========================================================
# PARSING SAVERS
# =========================================================

def save_single_csv(df: pd.DataFrame, context: Context) -> list[Path]:

    output_dir: Path = context["output_dir"]
    input_path: Path = context["input_path"]

    output_dir.mkdir(parents=True, exist_ok=True)

    out = output_dir / f"{input_path.stem}.csv"

    df.to_csv(out, index=False)

    return [out]



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

        sub.to_csv(out, index=False)

        outputs.append(out)

    return outputs

# =========================================================
# FRAMING SAVERS
# =========================================================

def save_output_path_ctx(df: pd.DataFrame, context: Context) -> list[Path]:

    output_path: Path = context["output_path"]

    df.to_csv(output_path, index=False)

    context["output"] = output_path
    return [output_path]

def save_reference(df: pd.DataFrame, context: Context) -> list[Path]:

    output_dir: Path = context["output_dir"]
    output_dir.mkdir(parents=True, exist_ok=True)

    stem = context["input_path"].stem
    out_path = output_dir / f"{stem}.csv"

    df.to_csv(out_path, index=False)

    context["output"] = out_path
    return [out_path]

def save_stationary(df: pd.DataFrame, context: Context) -> list[Path]:

    file: Path = context["input_path"]
    output_dir: Path = context["output_dir"]

    output_dir.mkdir(parents=True, exist_ok=True)

    parts = file.stem.split("_")
    sensor, _, date, time, phone_id = parts[:5]

    out_name = f"{sensor}_stationary_{date}_{time}_{phone_id}.csv"
    out_path = output_dir / out_name

    df.to_csv(out_path, index=False)

    context["output"] = out_path
    return [out_path]

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
    
    df.to_csv(out_path, index=False)
    
    return [out_path]

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