from pathlib import Path
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed

import pipelines as ppl
import pipelines.naming as n


# ---------------------------------------------------------
# WORKER FUNCTION FOR PARALLEL PROCESSING
# ---------------------------------------------------------
def _process_single_row(
    i: int,
    row_dict: dict,
    pipeline: ppl.Pipeline,
    src_dir: Path,
    output_dir: Path,
    ref_dir: Path | None,
    ref_naming_convention: n.NameConvention,
    out_naming_convention: n.NameConvention,
    src_naming_convention: n.NameConvention | None,
    input_extension: str
) -> ppl.Context:
    """Top-level function required for multiprocessing serialization."""
    file_base = Path(row_dict["File_Name"]).name

    # Determine input path
    if src_naming_convention is None:
        input_path = src_dir / f"{file_base}.{input_extension}"
    else:
        input_path = src_dir / f"{src_naming_convention(
            phone_id=row_dict['phone_id'],
            config=row_dict['config'],
            target_speed_mps=row_dict['target_speed_mps'],
            repeat=row_dict['repeat'],
        )}.{input_extension}"

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
    ctx: ppl.Context = {
        "input_path": input_path,
        "output_path": output_path,
        "ref_path": ref_path,
        "signal_col": "RotVelRes (rad/s)",
        "ref_signal_col": "RotVelRes (rad/s)",
        "outputs": [],
        "log_row": row_dict
    }

    print(f"{i}: Processing {input_path.name}")
    outputs, ctx = pipeline.run(input_path, ctx)
    ctx["outputs"] = [str(p) for p in outputs]

    return ctx


# ---------------------------------------------------------
# RUNNER FUNCTIONS
# ---------------------------------------------------------
def run_from_clean_log(
    pipeline: ppl.Pipeline,
    clean_log_path: Path,
    src_dir: Path,
    output_dir: Path,
    log_path: Path,
    ref_dir: Path = None,
    ref_naming_convention: n.NameConvention = n.ref_convention,
    out_naming_convention: n.NameConvention = n.framed_convention,
    src_naming_convention: n.NameConvention | None = None,
    input_extension: str = "csv"
) -> pd.DataFrame:
    """Original Sequential Runner"""
    log_df = pd.read_csv(clean_log_path)
    contexts: list[ppl.Context] = []

    print(f"-------- Running pipeline from log: {clean_log_path} -------")

    for i, row in log_df.iterrows():
        if pd.isna(row["File_Name"]) or row["Successful"] == "FALSE":
            continue
        
        file_base = Path(row["File_Name"]).name
        if file_base == "" or "FILTERED" in file_base:
            continue

        # Convert row to dict for processing
        ctx = _process_single_row(
            i, row.to_dict(), pipeline, src_dir, output_dir, ref_dir,
            ref_naming_convention, out_naming_convention, src_naming_convention, input_extension
        )
        contexts.append(ctx)

    out_log = pd.DataFrame(contexts)
    if "ref_df" in out_log.columns:
        out_log = out_log.drop(columns=["ref_df"])
    out_log.to_csv(log_path, index=False)

    return out_log


def run_from_clean_log_parallel(
    pipeline: ppl.Pipeline,
    clean_log_path: Path,
    src_dir: Path,
    output_dir: Path,
    log_path: Path,
    ref_dir: Path = None,
    ref_naming_convention: n.NameConvention = n.ref_convention,
    out_naming_convention: n.NameConvention = n.framed_convention,
    src_naming_convention: n.NameConvention | None = None,
    input_extension: str = "csv",
    max_workers: int | None = None
) -> pd.DataFrame:
    """Parallelized Runner using ProcessPoolExecutor"""
    log_df = pd.read_csv(clean_log_path)
    contexts: list[ppl.Context] = []

    print(f"-------- Running pipeline from log in PARALLEL: {clean_log_path} -------")

    # Filter out invalid rows before submitting to the pool
    valid_tasks = []
    for i, row in log_df.iterrows():
        if pd.isna(row["File_Name"]) or row["Successful"] == "FALSE":
            continue
        
        file_base = Path(row["File_Name"]).name
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
                    src_naming_convention, input_extension
                )
            )

        # Collect results as they complete
        for future in as_completed(futures):
            try:
                ctx = future.result()
                contexts.append(ctx)
            except Exception as e:
                print(f"Pipeline process encountered an error: {e}")

    out_log = pd.DataFrame(contexts)
    if "ref_df" in out_log.columns:
        out_log = out_log.drop(columns=["ref_df"]) # Just for easier reading of logs
    out_log.to_csv(log_path, index=False)

    return out_log


# ---------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------
#????? meta disapear, rename results to be more obv what they are
if __name__ == "__main__":

    # =========================================================
    # ONLY RUN REFERENCE PARSING AFTER PARSING THE PHONE CHARACTERISTICS 
    # =========================================================
    # Parse reference signals from transformed headform data
    # downsampled_reference_results = ppl.run_directory_parallel(
    #     pipeline=ppl.definitions.REFERENCE_PARSING_PIPELINE,
    #     src_dir=Path("data_processing_gitignore/RAW_DATA/transformed_headform"),
    #     output_dir=Path("data_processing_gitignore/phone_drop_test_data/sync_ref"),
    #     log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/reference_parsing_log.csv"),
    #     extension="*.xlsx",
    #     extra_context = {
    #         "phone_characteristics_aggregated": Path("data_processing_gitignore/phone_characteristics/aggregated/characteristics_drops.csv")
    #     }
    # )

    # # Note: Swapped to parallel execution
    # phone_framed_to_ref_results = run_from_clean_log_parallel(
    #     pipeline=ppl.definitions.PHONE_DROP_FRAMING_PIPELINE,
    #     clean_log_path=Path("data_processing_gitignore/lookup_tables/data_collection_log.csv"),
    #     src_dir=Path("data_processing_gitignore/phone_drop_test_data/parsed"),
    #     output_dir=Path("data_processing_gitignore/phone_drop_test_data/framed"),
    #     ref_dir=Path("data_processing_gitignore/phone_drop_test_data/sync_ref"),
    #     log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/framing_run_log.csv"),
    # )

    reference_exact_sampling_results = run_from_clean_log_parallel(
        pipeline=ppl.definitions.FRAMED_RESAMPLING_PIPELINE,
        clean_log_path=Path("data_processing_gitignore/lookup_tables/data_collection_log.csv"),
        src_dir=Path("data_processing_gitignore/RAW_DATA/transformed_headform"),
        output_dir=Path("data_processing_gitignore/phone_drop_test_data/reference"),
        ref_dir=Path("data_processing_gitignore/phone_drop_test_data/framed"),
        log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/framed_resampling_log.csv"),
        ref_naming_convention=n.framed_convention,
        out_naming_convention=n.ref_convention,
        src_naming_convention=n.ref_convention,
        input_extension="xlsx"
    )

    # match the files index by index to create correlation plots
    correlation_results = run_from_clean_log_parallel(
        pipeline=ppl.definitions.CORRELATION_PIPELINE,
        clean_log_path=Path("data_processing_gitignore/lookup_tables/data_collection_log.csv"),
        src_dir=Path("data_processing_gitignore/phone_drop_test_data/framed"),
        output_dir=Path("data_processing_gitignore/phone_drop_test_data/correlation"),
        ref_dir= Path("data_processing_gitignore/phone_drop_test_data/reference"),
        log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/correlation_log.csv"),
        ref_naming_convention=n.ref_convention,
        out_naming_convention=n.default_convention,
        src_naming_convention=n.framed_convention,
    )