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
    saver=save_split_by_triggered,
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
#STATIONARY
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


#LOOKUP

LOG_CLEANING_PIPELINE = Pipeline(
    name="log_cleaning",
    loader=load_raw_log_csv,
    transforms=[
        normalize_log_columns,

        drop_failed_rows,
        drop_empty_rows,
        
        combined_time,
        extract_phone_id,
        drop_headform,

        parse_test_metadata,
        extract_repeat_from_test_name,
        clean_speed_column,
        partial(rename_continuous_test_files,add_trigger0=False),
    ],
    saver=save_single_csv,
)

CALIBRATION_LOG_CLEANING_PIPELINE = Pipeline(
    name="calib_log_cleaning",
    loader=load_raw_log_csv,
    transforms=[
        normalize_log_columns,

        drop_failed_rows,
        drop_empty_rows,

        combined_time,
        extract_phone_id,
        drop_headform,

        parse_test_metadata,
        extract_repeat_from_test_name,
        clean_speed_column,
        partial(rename_continuous_test_files,add_trigger0=True),

        drop_no_calibration,
    ],
    saver=save_single_csv,
)



#CONTINUOUS EXTRACTION

EXTRACT_CONTINUOUS_PIPELINE = Pipeline(
    name="extract_continous",
    loader=load_csv,
    transforms=[
        normalize_time_column,
        ensure_sensor_columns,
        sort_by_time,
        convert_units,
        get_block_indices,
        filter_blocks_by_duration,
        nudge_blocks,
        recalc_triggered_by_blocks,
        drop_nan,

        axis_column,
    ],
    saver=save_split_by_triggered,
)

FRAME_CALIBRATION_PIPELINE = Pipeline(
    name="frame_calibration",
    loader=load_csv,
    transforms=[
        drop_motion,
        choose_axis_group,
        drop_nan,
    ],
    saver=save_single_csv,
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

CALIBRATION_PARAMETERS_PIPELINE = Pipeline(
    name="calibration_parameters",
    loader=load_csv,
    transforms=[
        compute_6axis_calibration,
        create_calibration_summary,
    ],
    saver=null_saver,
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


# PSD ------------------

POWER_SPECTRAL_DENSITY_PIPELINE_BOTH = Pipeline(
    name="power_spectral_density",
    loader=load_csv,
    transforms=[
        partial(calculate_psd_transform, both=True),
    ],
    saver=save_psd,
)

POWER_SPECTRAL_DENSITY_PIPELINE = Pipeline(
    name="power_spectral_density",
    loader=load_csv,
    transforms=[
        calculate_psd_transform,
    ],
    saver=save_psd,
)
