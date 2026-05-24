from pathlib import Path
import pandas as pd

import pipelines as ppl
import pipelines.naming as n


def run_from_clean_log(
    pipeline: ppl.Pipeline,
    clean_log_path: Path,
    src_dir: Path,
    output_dir: Path,
    log_path: Path,
    ref_dir: Path = None,
    ref_naming_convention:n.NameConvention = n.ref_convention,
    out_naming_convention:n.NameConvention = n.framed_convention,
    src_naming_convention:n.NameConvention | None = None,
    input_extension:str = "csv"
) -> pd.DataFrame:

    log_df = pd.read_csv(clean_log_path)

    results: list[pd.DataFrame] = []
    contexts: list[ppl.Context] = []

    print(f"-------- Running pipeline from log: {clean_log_path} -------")

    for i, row in log_df.iterrows():

        if pd.isna(row["File_Name"]):
            continue
        if row["Successful"]=="FALSE":
            continue
        file_base = Path(row["File_Name"]).name
        # skip invalid rows
        if file_base == "":
            continue
        #skip headform data
        if "FILTERED" in file_base:
            continue

        if src_naming_convention is None:
            input_path = src_dir / f"{file_base}.{input_extension}"
        else:
            input_path = src_dir / f"{src_naming_convention(
                phone_id=row["phone_id"],
                config=row["config"],
                target_speed_mps=row["target_speed_mps"],
                repeat=row["repeat"],
            )}.{input_extension}"
        ref_path = ref_dir / f"{ref_naming_convention(
            phone_id=row["phone_id"],
            config=row["config"],
            target_speed_mps=row["target_speed_mps"],
            repeat=row["repeat"],
        )}.csv"
        output_path = output_dir / f"{out_naming_convention(
            phone_id=row["phone_id"],
            config=row["config"],
            target_speed_mps=row["target_speed_mps"],
            repeat=row["repeat"]
        )}.csv"

        ctx: ppl.Context = {
            "input_path": input_path,
            "output_path": output_path,
            "ref_path": ref_path,
            "signal_col": "RotVelRes (rad/s)",
            "ref_signal_col": "RotVelRes (rad/s)",
            "outputs" : [],

            # carry full metadata forward (this is powerful later)
            "log_row": row.to_dict()
        }
        print(f"{i}: Processing {input_path.name}")
        outputs, ctx = pipeline.run(input_path, ctx)
        ctx["outputs"] = [str(p) for p in outputs]


        results.append(row.to_dict())
        contexts.append(ctx)

    out_log = pd.DataFrame(contexts)
    out_log.to_csv(log_path, index=False)

    return out_log


# ---------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------

if __name__ == "__main__":

    # #=========================================================
    # #ONLY RUN REFERENCE PARSING AFTER PARSING THE PHONE CHARACTERISTICS 
    # ##=========================================================
    # # Parse reference signals from transformed headform data
    # reference_results = ppl.run_directory(
    #     pipeline=ppl.definitions.REFERENCE_PARSING_PIPELINE,
    #     src_dir=Path("data_processing_gitignore/RAW_DATA/transformed_headform"),
    #     output_dir=Path("data_processing_gitignore/phone_drop_test_data/sync_ref"),
    #     log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/reference_parsing_log.csv"),
    #     extension="*.xlsx",
    #     extra_context = {
    #         "phone_characteristics_aggregated": Path("data_processing_gitignore/phone_characteristics/aggregated/characteristics_drops.csv")
    #     }
    # )

    run_from_clean_log(
        pipeline=ppl.definitions.PHONE_DROP_FRAMING_PIPELINE,
        clean_log_path=Path("data_processing_gitignore/lookup_tables/data_collection_log.csv"),
        src_dir=Path("data_processing_gitignore/phone_drop_test_data/parsed"),
        output_dir=Path("data_processing_gitignore/phone_drop_test_data/framed"),
        ref_dir=Path("data_processing_gitignore/phone_drop_test_data/sync_ref"),
        log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/framing_run_log.csv"),
    )

    run_from_clean_log(
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