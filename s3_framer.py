from pathlib import Path
import pipelines as ppl
from pipelines.log_runner import run_from_clean_log_parallel
import pipelines.naming as n

if __name__ == "__main__":

    # =========================================================
    # ONLY RUN REFERENCE PARSING AFTER PARSING THE PHONE CHARACTERISTICS 
    # =========================================================
    #Parse reference signals from transformed headform data
    downsampled_reference_results = ppl.run_directory_parallel(
        pipeline=ppl.definitions.REFERENCE_PARSING_PIPELINE,
        src_dir=Path("data_processing_gitignore/RAW_DATA/transformed_headform"),
        output_dir=Path("data_processing_gitignore/phone_drop_test_data/sync_ref"),
        log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/reference_parsing_log.csv"),
        extension="*.xlsx",
        extra_context = {
            "phone_characteristics_aggregated": Path("data_processing_gitignore/phone_characteristics/aggregated/characteristics_drops.csv")
        }
    )


    phone_framed_to_ref_results = run_from_clean_log_parallel(
        pipeline=ppl.definitions.PHONE_DROP_FRAMING_PIPELINE,
        clean_log_path=Path("data_processing_gitignore/lookup_tables/data_collection_log.csv"),
        src_dir=Path("data_processing_gitignore/phone_drop_test_data/parsed"),
        output_dir=Path("data_processing_gitignore/phone_drop_test_data/framed"),
        ref_dir=Path("data_processing_gitignore/phone_drop_test_data/sync_ref"),
        log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/framing_run_log.csv"),
    )

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