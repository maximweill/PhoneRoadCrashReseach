from pathlib import Path
import pipelines as ppl

def main():
    # Source: framed stationary data
    # Output: allan variance CSVs
    allan_results = ppl.run_directory(
        pipeline=ppl.definitions.ALLAN_VARIANCE_PIPELINE,
        src_dir=Path("data_processing_gitignore/stationary/framed"),
        output_dir=Path("data_processing_gitignore/stationary/allan_variance"),
        log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/allan_variance_log.csv"),
    )
    print("Allan Variance processing complete.")

if __name__ == "__main__":
    main()
