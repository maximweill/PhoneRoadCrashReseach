from pathlib import Path
import pipelines as ppl

def main():
    # log_results = ppl.run_directory(
    #     pipeline=ppl.definitions.LOG_CLEANING_PIPELINE,
    #     src_dir=Path("data_processing_gitignore/RAW_LOGBOOK"),
    #     output_dir=Path("data_processing_gitignore/lookup_tables"),
    #     log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/log_book_parsing.csv"),
    # )

    # calibration_log_results = ppl.run_directory(
    #     pipeline=ppl.definitions.CALIBRATION_LOG_CLEANING_PIPELINE,
    #     src_dir=Path("data_processing_gitignore/RAW_LOGBOOK"),
    #     output_dir=Path("data_processing_gitignore/calibration_lookup_tables"),
    #     log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/calibration_log_book_parsing.csv"),
    # )


    # continuous_drops_results = ppl.run_directory_parallel(
    #     pipeline=ppl.definitions.EXTRACT_CONTINUOUS_PIPELINE,
    #     src_dir=Path("data_processing_gitignore/RAW_DATA/continuous_drops"),
    #     output_dir=Path("data_processing_gitignore/phone_drop_test_data/parsed/continuous"),
    #     log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/continuous_parsing_log.csv")
    # )

    # initial_drop_results = ppl.run_directory_parallel(
    #     pipeline=ppl.definitions.OLD_PHONE_PIPELINE,
    #     src_dir=Path("data_processing_gitignore/RAW_DATA/phone_drop_initial"),
    #     output_dir=Path("data_processing_gitignore/phone_drop_test_data/parsed/initial"),
    #     log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/phone_drop_initial_parsing_log.csv")
    # )

    # secondary_01052026_drop_results = ppl.run_directory_parallel(
    #     pipeline=ppl.definitions.PHONE_PIPELINE,
    #     src_dir=Path("data_processing_gitignore/RAW_DATA/phone_drop_01052026"),
    #     output_dir=Path("data_processing_gitignore/phone_drop_test_data/parsed/secodary"),
    #     log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/phone_drop_01052026_parsing_log.csv")
    # )

    # crashtest_results = ppl.run_directory_parallel(
    #     pipeline=ppl.definitions.OLD_PHONE_PIPELINE,
    #     src_dir=Path("data_processing_gitignore/RAW_DATA/JLR_past_crashtests"),
    #     output_dir=Path("data_processing_gitignore/car_crash_data/parsed"),
    #     log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/JLR_past_crashtests_parsing_log.csv")
    # )

    # stationary_results = ppl.run_directory_parallel(
    #     pipeline=ppl.definitions.STATIONARY_PARSING_PIPELINE,
    #     src_dir=Path("data_processing_gitignore/RAW_DATA/continuous_stationary"),
    #     output_dir=Path("data_processing_gitignore/stationary/start/parsed"),
    #     log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/continuous_stationary_parsing_log.csv")
    # )


    continuous_stationary_accel_framed_end_results = ppl.run_directory_parallel(
        pipeline=ppl.definitions.STATIONARY_PARSING_PIPELINE_BOTH,
        src_dir=Path("data_processing_gitignore/RAW_DATA/continuous_stationary_accel_framed_end"),
        output_dir=Path("data_processing_gitignore/stationary/end/parsed"),
        log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/continuous_stationary_accel_framed_end_parsing_log.csv")
    )


if __name__ == '__main__':
    main()