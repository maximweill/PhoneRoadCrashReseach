from pathlib import Path
import pipelines as ppl

if __name__ == "__main__":
    
    # # 1. Process directories to collect raw characteristics
    # # Intermediate results saved as directory-level logs in 'individual'
    
    individual_dir = Path("data_processing_gitignore/phone_characteristics/individual")
    individual_dir.mkdir(parents=True, exist_ok=True)

    print("Collecting characteristics...")
    
    # # Drops
    # ppl.run_directory_parallel(
    #     pipeline=ppl.definitions.PHONE_CHARACTERISTICS_PIPELINE,
    #     src_dir=Path("data_processing_gitignore/phone_drop_test_data/parsed"),
    #     output_dir=individual_dir,
    #     log_path=individual_dir / "characteristics_drops.csv"
    # )

    # # Stationary
    # ppl.run_directory_parallel(
    #     pipeline=ppl.definitions.PHONE_CHARACTERISTICS_PIPELINE,
    #     src_dir=Path("data_processing_gitignore/stationary/start/parsed"),
    #     output_dir=individual_dir,
    #     log_path=individual_dir / "start_characteristics_stationary.csv"
    # )
    # ppl.run_directory_parallel(
    #     pipeline=ppl.definitions.PHONE_CHARACTERISTICS_PIPELINE,
    #     src_dir=Path("data_processing_gitignore/stationary/end/parsed"),
    #     output_dir=individual_dir,
    #     log_path=individual_dir / "end_characteristics_stationary.csv"
    # )
    ppl.run_directory_parallel(
        pipeline=ppl.definitions.PHONE_CHARACTERISTICS_PIPELINE,
        src_dir=Path("data_processing_gitignore/stationary/end2/parsed"),
        output_dir=individual_dir,
        log_path=individual_dir / "end2_characteristics_stationary.csv"
    )

    # # Headform
    # ppl.run_directory_parallel(
    #     pipeline=ppl.definitions.PHONE_CHARACTERISTICS_PIPELINE,
    #     src_dir=Path("data_processing_gitignore/stationary/headform/parsed"),
    #     output_dir=individual_dir,
    #     log_path=individual_dir / "headform_characteristics_stationary.csv"
    # )
    # ppl.run_directory_parallel(
    #     pipeline=ppl.definitions.PHONE_CHARACTERISTICS_PIPELINE,
    #     src_dir=Path("data_processing_gitignore/stationary/headform_filtered/parsed"),
    #     output_dir=individual_dir,
    #     log_path=individual_dir / "headform_filtered_characteristics_stationary.csv"
    # )

    # # Calibration
    # ppl.run_directory_parallel(
    #     pipeline=ppl.definitions.PHONE_CHARACTERISTICS_PIPELINE,
    #     src_dir=Path("data_processing_gitignore/6axis_calibration/framed"),
    #     output_dir=individual_dir,
    #     log_path=individual_dir / "characteristics_calibration.csv"
    # )

    # # 2. Aggregate the collected logs into the final project summary
    # # We use the same run_directory pattern with null_saver
    

    print("Running final aggregation...")
    ppl.run_directory_parallel(
        pipeline=ppl.definitions.CHARACTERISTICS_AGGREGATION_PIPELINE,
        src_dir=individual_dir,
        output_dir=Path("data_processing_gitignore/phone_characteristics/aggregated"),
        log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/characteristics_aggregation_log.csv")
    )




    print("Workflow complete")
