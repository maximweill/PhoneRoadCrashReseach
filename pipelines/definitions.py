from .core import *
from .io import*
from .transforms import *




# =========================================================
# PIPELINES
# =========================================================

OLD_PHONE_PIPELINE = Pipeline(
    name="old_phone",
    loader=load_csv,
    transforms=[
        normalize_time_column,
        ensure_sensor_columns,
        sort_by_time,
        accelerometer_based_timestamps,
        deduplicate,
        convert_units,
    ],
    saver=save_single_csv,
)

PHONE_PIPELINE = Pipeline(
    name="new_phone",
    loader=load_csv,
    transforms=[
        normalize_time_column,
        ensure_sensor_columns,
        sort_by_time,
        convert_units,
    ],
    saver=save_single_csv,
)


CONTINUOUS_PHONE_PIPELINE = Pipeline(
    name="continuous_phone",
    loader=load_csv,
    transforms=[
        normalize_time_column,
        ensure_sensor_columns,
        sort_by_time,
        convert_units,
    ],
    saver=save_split_by_trigger,
)

FRAMED_RESAMPLING_PIPELINE = Pipeline(
    name="framed_resampling",
    loader=load_phone_drop_with_ref,
    transforms=[
        normalize_column_names,
        ref_timestamps_matching,
    ],
    saver=save_output_path_ctx,
)

PHONE_DROP_FRAMING_PIPELINE = Pipeline(
    name="phone_drop",
    loader=load_phone_drop_with_ref,
    transforms=[
        normalize_column_names,
        compute_lag,
        align_to_reference,
    ],
    saver=save_output_path_ctx,
)

STATIONARY_PARSING_PIPELINE = Pipeline(
    name="stationary",
    loader=load_csv,
    transforms=[
        normalize_time_column,
        ensure_sensor_columns,
        sort_by_time,
        convert_units,

        trim_stationary,
    ],
    saver=save_stationary,
)

LOG_CLEANING_PIPELINE = Pipeline(
    name="log_cleaning",
    loader=load_raw_log_csv,
    transforms=[
        drop_failed_rows,
        drop_empty_rows,
        normalize_log_columns,
        extract_phone_id,
        parse_test_metadata,
        extract_repeat_from_test_name,
        clean_speed_column,
        rename_continuous_test_files,
    ],
    saver=save_single_csv,
)


REFERENCE_PARSING_PIPELINE = Pipeline(
    name="reference_parsing",
    loader=load_excel,
    transforms=[
        normalize_column_names,
        resample_reference,
    ],
    saver=save_reference,
)


EXTRACT_DROPS_PIPELINE = Pipeline(
    name="extract_drops",
    loader=load_csv,
    transforms=[
        normalize_time_column,
        ensure_sensor_columns,
        sort_by_time,
        convert_units,
        remove_accidental_triggers,
        extract_drops,
    ],
    saver=save_split_by_trigger,
)

EXTRACT_CALIBRATION_PIPELINE = Pipeline(
    name="extract_calibration",
    loader=load_csv,
    transforms=[
        normalize_time_column,
        ensure_sensor_columns,
        sort_by_time,
        convert_units,
        remove_accidental_triggers,
        extract_calibration,
    ],
    saver=save_split_by_trigger,
)


PHONE_CHARACTERISTICS_PIPELINE = Pipeline(
    name="phone_characteristics",
    loader=load_csv,
    transforms=[
        compute_sampling_rate_stats,
        compute_battery_stats,
        compute_magnetic_stats,
        compute_sensor_max_stats,
        create_characteristics_summary,
    ],
    saver=null_saver,
)

CHARACTERISTICS_AGGREGATION_PIPELINE = Pipeline(
    name="characteristics_aggregation",
    loader=load_csv, 
    transforms=[
        aggregate_characteristics_by_phone,
    ],
    saver=save_single_csv,
)

ALLAN_VARIANCE_PIPELINE = Pipeline(
    name="allan_variance",
    loader=load_csv,
    transforms=[
        calculate_allan_variance_transform,
    ],
    saver=save_allan_variance,
)