from pathlib import Path
import pipelines as ppl

def main():
    print("Starting Headform Stationary data processing...")

    # Standard parsing and trimming
    # Setting small buffers because headform stationary files are relatively short (approx 100s)
    # Using a high impact_threshold (500 m/s2) because headform sensors can have high bias
    # or the recordings may not contain the marker impacts expected by trim_stationary.
    headform_stationary_results = ppl.run_directory_parallel(
        pipeline=ppl.definitions.HEADFORM_STATIONARY_PIPELINE,
        src_dir=Path("data_processing_gitignore/RAW_DATA/stationary_headform"),
        output_dir=Path("data_processing_gitignore/stationary/headform/parsed"),
        log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/headform_stationary_parsing_log.csv"),
        extra_context={"buffer_after": 1, "buffer_before": 1, "impact_threshold": 500}
    )

    # Filtered parsing (200Hz) and trimming
    headform_filtered_stationary_results = ppl.run_directory_parallel(
        pipeline=ppl.definitions.HEADFORM_FILTERED_STATIONARY_PIPELINE,
        src_dir=Path("data_processing_gitignore/RAW_DATA/stationary_headform"),
        output_dir=Path("data_processing_gitignore/stationary/headform_filtered/parsed"),
        log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/headform_filtered_stationary_parsing_log.csv"),
        extra_context={"buffer_after": 1, "buffer_before": 1, "impact_threshold": 500}
    )
    
    print("Headform Stationary data processing complete.")

if __name__ == '__main__':
    main()
