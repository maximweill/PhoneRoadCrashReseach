from pathlib import Path
import pipelines as ppl
from pipelines.log_runner import run_from_clean_log_parallel,run_from_clean_log
import pipelines.naming as n


if __name__ == "__main__":
    
    continous_framed_for_calibration_results = run_from_clean_log_parallel(
        pipeline=ppl.definitions.FRAME_CALIBRATION_PIPELINE,
        clean_log_path=Path("data_processing_gitignore/calibration_lookup_tables/data_collection_log.csv"),
        src_dir=Path("data_processing_gitignore/phone_drop_test_data/parsed/continuous"),
        output_dir=Path("data_processing_gitignore/6axis_calibration/framed"),
        log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/calibration_framing_run_log.csv"),
    )

    # 1. Process directories to collect 6-axis calibration parameters
    # Results saved to log_path, individual files not saved (null_saver used in pipeline)

    print("Collecting calibration parameters...")
    
    calibration_params_results = run_from_clean_log_parallel(
        pipeline=ppl.definitions.CALIBRATION_PARAMETERS_PIPELINE,
        clean_log_path=Path("data_processing_gitignore/calibration_lookup_tables/data_collection_log.csv"),
        src_dir=Path("data_processing_gitignore/6axis_calibration/framed"),
        output_dir=Path("data_processing_gitignore/6axis_calibration/calibration"),
        log_path=Path("data_processing_gitignore/6axis_calibration/calibration") / "parameters.csv",
        src_naming_convention= n.framed_convention,
    )

    print("Workflow complete")
