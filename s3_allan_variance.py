from pathlib import Path
import pipelines as ppl

def main():
    # Source: framed stationary data
    # Output: allan variance CSVs
    start_allan_results = ppl.run_directory(
        pipeline=ppl.definitions.ALLAN_VARIANCE_PIPELINE,
        src_dir=Path("data_processing_gitignore/stationary/start/parsed"),
        output_dir=Path("data_processing_gitignore/stationary/start/allan_variance"),
        log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/start_allan_variance_log.csv"),
    )

    end_allan_results = ppl.run_directory(
        pipeline=ppl.definitions.ALLAN_VARIANCE_PIPELINE_BOTH,
        src_dir=Path("data_processing_gitignore/stationary/end/parsed"),
        output_dir=Path("data_processing_gitignore/stationary/end/allan_variance"),
        log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/end_allan_variance_log.csv"),
    )

    print("Allan Variance processing complete.")

if __name__ == "__main__":
    main()
