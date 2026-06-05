
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path


from .core import Context,Pipeline
from .naming import NameConvention,ref_convention,framed_convention



# ---------------------------------------------------------
# WORKER FUNCTION FOR PARALLEL PROCESSING
# ---------------------------------------------------------
def _process_single_row(
    i: int,
    row_dict: dict,
    pipeline: Pipeline,
    src_dir: Path,
    output_dir: Path,
    ref_dir: Path | None,
    ref_naming_convention: NameConvention,
    out_naming_convention: NameConvention,
    src_naming_convention: NameConvention | None,
    input_extension: str,
    extra_context: Context | None = None
) -> Context:
    """Top-level function required for multiprocessing serialization."""
    file_base = Path(row_dict["file_name"]).name

    # Determine input path
    if src_naming_convention is None:
        filename = f"{file_base}.{input_extension}"
    else:
        filename = f"{src_naming_convention(
            phone_id=row_dict['phone_id'],
            config=row_dict['config'],
            target_speed_mps=row_dict['target_speed_mps'],
            repeat=row_dict['repeat'],
        )}.{input_extension}"

    matches = list(src_dir.rglob(filename))

    if not matches:
        raise FileNotFoundError(f"Could not find {filename} under {src_dir}")

    if len(matches) > 1:
        raise ValueError(f"Multiple matches found for {filename}: {matches}")

    input_path = matches[0]

    # Determine reference path safely if ref_dir is provided
    ref_path = None
    if ref_dir is not None:
        ref_path = ref_dir / f"{ref_naming_convention(
            phone_id=row_dict['phone_id'],
            config=row_dict['config'],
            target_speed_mps=row_dict['target_speed_mps'],
            repeat=row_dict['repeat'],
        )}.csv"

    # Determine output path
    output_path = output_dir / f"{out_naming_convention(
        phone_id=row_dict['phone_id'],
        config=row_dict['config'],
        target_speed_mps=row_dict['target_speed_mps'],
        repeat=row_dict['repeat']
    )}.csv"

    # Build context
    ctx: Context = {
        "input_path": input_path,
        "output_path": output_path,
        "ref_path": ref_path,
        "signal_col": "RotVelRes (rad/s)",
        "ref_signal_col": "RotVelRes (rad/s)",
        "outputs": [],
        "global_time":row_dict["global_time"],
        "log_row": row_dict,
        **(extra_context or {}),
    }

    print(f"{i}: Processing {input_path.name}")
    outputs, ctx = pipeline.run(input_path, ctx)
    ctx["outputs"] = [str(p) for p in outputs]

    return ctx


# ---------------------------------------------------------
# RUNNER FUNCTIONS
# ---------------------------------------------------------
def run_from_clean_log(
    pipeline: Pipeline,
    clean_log_path: Path,
    src_dir: Path,
    output_dir: Path,
    log_path: Path,
    ref_dir: Path = None,
    ref_naming_convention: NameConvention = ref_convention,
    out_naming_convention: NameConvention = framed_convention,
    src_naming_convention: NameConvention | None = None,
    input_extension: str = "csv",
    extra_context: Context | None = None
) -> pd.DataFrame:
    """Original Sequential Runner"""

    output_dir.mkdir(parents=True, exist_ok=True)
    log_df = pd.read_csv(clean_log_path)
    contexts: list[Context] = []

    print(f"-------- Running pipeline from log: {clean_log_path} -------")

    for i, row in log_df.iterrows():
        if pd.isna(row["file_name"]):
            continue
        
        file_base = Path(row["file_name"]).name
        if file_base == "" or "FILTERED" in file_base:
            continue

        # Convert row to dict for processing
        ctx = _process_single_row(
            i, row.to_dict(), pipeline, src_dir, output_dir, ref_dir,
            ref_naming_convention, out_naming_convention, src_naming_convention, input_extension,
            extra_context
        )
        if "error" in ctx:
            if len(ctx["errors"])>0:
                print(f"+ WARNING encountered errors : {ctx["errors"]}")
        contexts.append(ctx)

    out_log = pd.DataFrame(contexts)
    if "ref_df" in out_log.columns:
        out_log = out_log.drop(columns=["ref_df"], errors="ignore")
    out_log.to_csv(log_path, index=False)

    return out_log


def run_from_clean_log_parallel(
    pipeline: Pipeline,
    clean_log_path: Path,
    src_dir: Path,
    output_dir: Path,
    log_path: Path,
    ref_dir: Path = None,
    ref_naming_convention: NameConvention = ref_convention,
    out_naming_convention: NameConvention = framed_convention,
    src_naming_convention: NameConvention | None = None,
    input_extension: str = "csv",
    max_workers: int | None = None,
    extra_context: Context | None = None
) -> pd.DataFrame:
    """Parallelized Runner using ProcessPoolExecutor"""
    output_dir.mkdir(parents=True, exist_ok=True)
    log_df = pd.read_csv(clean_log_path)
    contexts: list[Context] = []

    print(f"-------- Running pipeline from log in PARALLEL: {clean_log_path} -------")

    # Filter out invalid rows before submitting to the pool
    valid_tasks = []
    for i, row in log_df.iterrows():
        if pd.isna(row["file_name"]):
            continue
        
        file_base = Path(row["file_name"]).name
        if file_base == "" or "FILTERED" in file_base:
            continue
        
        valid_tasks.append((i, row.to_dict()))

    # Spin up the ProcessPoolExecutor
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for i, row_dict in valid_tasks:
            futures.append(
                executor.submit(
                    _process_single_row,
                    i, row_dict, pipeline, src_dir, output_dir, ref_dir,
                    ref_naming_convention, out_naming_convention,
                    src_naming_convention, input_extension,
                    extra_context
                )
            )

        # Collect results as they complete
        for future in as_completed(futures):
            try:
                ctx = future.result()   # <-- retrieve the Context returned by _process_single_row
                if "errors" in ctx:
                    if len(ctx["errors"]) > 0:
                        print(f"+ WARNING encountered errors : {ctx['errors']}")
                else:
                    print("- no errors column")
                contexts.append(ctx)
            except Exception as e:
                print(f"Pipeline process encountered an error: {e}")

    out_log = pd.DataFrame(contexts)
    if "ref_df" in out_log.columns:
        out_log = out_log.drop(columns=["ref_df"], errors="ignore")
    out_log.to_csv(log_path, index=False)

    return out_log