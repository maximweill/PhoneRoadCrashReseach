from pathlib import Path
import pandas as pd

from pipelines import core as ppl
from pipelines.definitions import PHONE_DROP_FRAMING_PIPELINE

def naming_convention(phone_id:str,config:str,target_speed_mps:str,repeat:str,is_reference = False, is_framed = False):
    if is_reference:
        return f"{target_speed_mps}mps_{config}_REPEAT{repeat}_Headform_Transformed_{phone_id}_REF"
    if is_framed:
        return f"{target_speed_mps}mps_{config}_REPEAT{repeat}_{phone_id}_framed"

    return f"{target_speed_mps}mps_{config}_REPEAT{repeat}_{phone_id}"

def run_from_clean_log(
    pipeline: ppl.Pipeline,
    clean_log_path: Path,
    src_dir: Path,
    output_dir: Path,
    log_path: Path,
    ref_dir: Path = None,
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

        input_path = src_dir / f"{file_base}.csv"
        ref_path = ref_dir / f"{naming_convention(
            phone_id=row["phone_id"],
            config=row["config"],
            target_speed_mps=row["target_speed_mps"],
            repeat=row["repeat"],
            is_reference=True
        )}.csv"
        output_path = output_dir / f"{naming_convention(
            phone_id=row["phone_id"],
            config=row["config"],
            target_speed_mps=row["target_speed_mps"],
            repeat=row["repeat"],
            is_framed=True
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

    run_from_clean_log(
        pipeline=PHONE_DROP_FRAMING_PIPELINE,
        clean_log_path=Path("data_processing_gitignore/lookup_tables/data_collection_log.csv"),
        src_dir=Path("data_processing_gitignore/phone_drop_test_data/parsed"),
        output_dir=Path("data_processing_gitignore/phone_drop_test_data/framed"),
        ref_dir=Path("data_processing_gitignore/phone_drop_test_data/reference"),
        log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/framing_run_log.csv"),
    )