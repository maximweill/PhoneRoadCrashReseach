from pathlib import Path
import pipelines as ppl


# log_results = ppl.run_directory(
#     pipeline=ppl.definitions.LOG_CLEANING_PIPELINE,
#     src_dir=Path("data_processing_gitignore/RAW_LOGBOOK"),
#     output_dir=Path("data_processing_gitignore/lookup_tables"),
#     log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/log_book_parsing.csv"),
# )


continuous_drops_results = ppl.run_directory(
    pipeline=ppl.definitions.EXTRACT_DROPS_PIPELINE,
    src_dir=Path("data_processing_gitignore/RAW_DATA/continuous_drops"),
    output_dir=Path("data_processing_gitignore/phone_drop_test_data/parsed"),
    log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/continuous_drops_parsing_log.csv")
)

# continuous_drops_calibration_results = ppl.run_directory(
#     pipeline=ppl.definitions.EXTRACT_CALIBRATION_PIPELINE,
#     src_dir=Path("data_processing_gitignore/RAW_DATA/continuous_drops"),
#     output_dir=Path("data_processing_gitignore/6axis_calibaration/parsed"),
#     log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/six_axis_calibration_parsing_log.csv")
# )


# initial_drop_results = ppl.run_directory(
#     pipeline=ppl.definitions.OLD_PHONE_PIPELINE,
#     src_dir=Path("data_processing_gitignore/RAW_DATA/phone_drop_initial"),
#     output_dir=Path("data_processing_gitignore/phone_drop_test_data/parsed"),
#     log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/phone_drop_initial_parsing_log.csv")
# )

# secondary_01052026_drop_results = ppl.run_directory(
#     pipeline=ppl.definitions.PHONE_PIPELINE,
#     src_dir=Path("data_processing_gitignore/RAW_DATA/phone_drop_01052026"),
#     output_dir=Path("data_processing_gitignore/phone_drop_test_data/parsed"),
#     log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/phone_drop_01052026_parsing_log.csv")
# )

# crashtest_results = ppl.run_directory(
#     pipeline=ppl.definitions.OLD_PHONE_PIPELINE,
#     src_dir=Path("data_processing_gitignore/RAW_DATA/JLR_past_crashtests"),
#     output_dir=Path("data_processing_gitignore/car_crash_data/parsed"),
#     log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/JLR_past_crashtests_parsing_log.csv")
# )

# stationary_results = ppl.run_directory(
#     pipeline=ppl.definitions.STATIONARY_PARSING_PIPELINE,
#     src_dir=Path("data_processing_gitignore/RAW_DATA/continuous_stationary"),
#     output_dir=Path("data_processing_gitignore/stationary/parsed"),
#     log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/continuous_stationary_parsing_log.csv")
# )

# ACCEL_continuous_stationary_accel_framed_end_results = ppl.run_directory(
#     pipeline=ppl.definitions.PHONE_ACCEL_PIPELINE,
#     src_dir=Path("data_processing_gitignore/RAW_DATA/continuous_stationary_accel_framed_end"),
#     output_dir=Path("data_processing_gitignore/phone_drop_test_data/parsed"),
#     log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/accel_continuous_stationary_accel_framed_end_parsing_log.csv")
# )
# GYRO_continuous_stationary_accel_framed_end_results = ppl.run_directory(
#     pipeline=ppl.definitions.PHONE_GYRO_PIPELINE,
#     src_dir=Path("data_processing_gitignore/RAW_DATA/continuous_stationary_accel_framed_end"),
#     output_dir=Path("data_processing_gitignore/phone_drop_test_data/parsed"),
#     log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/gyro_continuous_stationary_accel_framed_end_parsing_log.csv")
# )

