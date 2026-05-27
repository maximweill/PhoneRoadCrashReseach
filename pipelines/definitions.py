from .core import *
from .io import*
from .transforms import *
from functools import partial
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
        trim_reference
    ],
    saver=save_single_csv,
)

PHONE_DROP_FRAMING_PIPELINE = Pipeline(
    name="phone_drop",
    loader=load_phone_drop_with_ref,
    transforms=[
        normalize_column_names,
        compute_lag,
        align_to_reference,
        trim_reference
    ],
    saver=save_single_csv,
)

#
#CORRELATIONS
#
CORRELATION_PIPELINE = Pipeline(
    name="index_correlation",
    loader=load_phone_drop_with_ref,
    transforms=[
        match_indices_ref,
        drop_time,
    ],
    saver=save_single_csv,
)

CORRELATION_PARAMETERS = Pipeline(
    loader=load_csv,
    name="SENSOR_AGREEMENT_VALIDATION",
    transforms=[
        reset_index,
        ignore_saturated,
        compute_n,
        compute_mae_from_ideal,        # error magnitude
        compute_pearson_correlation,      # signal similarity
        compute_trendline_regression,     # calibration (slope/intercept)
        compute_icc,          # agreement metric
        compute_bland_altman,       # gold standard agreement analysis loa_upper aka
    ],
    saver=null_saver,
)
#
#Beans
#
STATIONARY_PARSING_PIPELINE = Pipeline(
    name="stationary",
    loader=load_csv,
    transforms=[
        normalize_time_column,
        ensure_sensor_columns,
        sort_by_time,
        convert_units,
        
        trim_stationary,
        interpolate_outliers
    ],
    saver=partial(save_stationary,both=False),
)

STATIONARY_PARSING_PIPELINE_BOTH = Pipeline(
    name="stationary",
    loader=load_csv,
    transforms=[
        normalize_time_column,
        ensure_sensor_columns,
        sort_by_time,
        convert_units,

        trim_stationary,
        interpolate_outliers
    ],
    saver=partial(save_stationary,both=True),
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
        trim_reference
    ],
    saver=save_single_csv,
)

#CONTINUOUS EXTRACTION

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


#CHARACTERISTICS ----------------

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
        add_phyphox_data,
    ],
    saver=save_single_csv,
)

#ALLAN ------------------


ALLAN_VARIANCE_PIPELINE_BOTH = Pipeline(
    name="allan_variance",
    loader=load_csv,
    transforms=[
        partial(calculate_allan_variance_transform,both=True),
    ],
    saver=save_allan_variance,
)

ALLAN_VARIANCE_PIPELINE = Pipeline(
    name="allan_variance",
    loader=load_csv,
    transforms=[
        calculate_allan_variance_transform,
    ],
    saver=save_allan_variance,
)