from pathlib import Path
import pipelines as ppl

def main():
    # Source: framed stationary data
    # Output: allan variance CSVs
    print("Starting Allan Variance processing...")
    ppl.run_directory_parallel(
        pipeline=ppl.definitions.ALLAN_VARIANCE_PIPELINE,
        src_dir=Path("data_processing_gitignore/stationary/end2/parsed"),
        output_dir=Path("data_processing_gitignore/stationary/end2/allan_variance"),
        log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/end2_allan_variance_log.csv"),
    )

    # ppl.run_directory_parallel(
    #     pipeline=ppl.definitions.ALLAN_VARIANCE_PIPELINE,
    #     src_dir=Path("data_processing_gitignore/stationary/start/parsed"),
    #     output_dir=Path("data_processing_gitignore/stationary/start/allan_variance"),
    #     log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/start_allan_variance_log.csv"),
    # )

    # ppl.run_directory_parallel(
    #     pipeline=ppl.definitions.ALLAN_VARIANCE_PIPELINE_BOTH,
    #     src_dir=Path("data_processing_gitignore/stationary/end/parsed"),
    #     output_dir=Path("data_processing_gitignore/stationary/end/allan_variance"),
    #     log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/end_allan_variance_log.csv"),
    # )
    print("Allan Variance processing complete.")

    print("Starting PSD processing...")
    # ppl.run_directory_parallel(
    #     pipeline=ppl.definitions.POWER_SPECTRAL_DENSITY_PIPELINE,
    #     src_dir=Path("data_processing_gitignore/stationary/start/parsed"),
    #     output_dir=Path("data_processing_gitignore/stationary/start/power_spectral_density"),
    #     log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/start_psd_log.csv"),
    # )

    ppl.run_directory_parallel(
        pipeline=ppl.definitions.POWER_SPECTRAL_DENSITY_PIPELINE,
        src_dir=Path("data_processing_gitignore/stationary/end2/parsed"),
        output_dir=Path("data_processing_gitignore/stationary/end2/power_spectral_density"),
        log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/end2_psd_log.csv"),
    )

    # ppl.run_directory_parallel(
    #     pipeline=ppl.definitions.POWER_SPECTRAL_DENSITY_PIPELINE_BOTH,
    #     src_dir=Path("data_processing_gitignore/stationary/end/parsed"),
    #     output_dir=Path("data_processing_gitignore/stationary/end/power_spectral_density"),
    #     log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/end_psd_log.csv"),
    # )

    # print("Starting Headform noise analysis...")
    # ppl.run_directory_parallel(
    #     pipeline=ppl.definitions.ALLAN_VARIANCE_PIPELINE_BOTH,
    #     src_dir=Path("data_processing_gitignore/stationary/headform/parsed"),
    #     output_dir=Path("data_processing_gitignore/stationary/headform/allan_variance"),
    #     log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/headform_allan_variance_log.csv"),
    # )
    # ppl.run_directory_parallel(
    #     pipeline=ppl.definitions.POWER_SPECTRAL_DENSITY_PIPELINE_BOTH,
    #     src_dir=Path("data_processing_gitignore/stationary/headform/parsed"),
    #     output_dir=Path("data_processing_gitignore/stationary/headform/power_spectral_density"),
    #     log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/headform_psd_log.csv"),
    # )

    # print("Starting Filtered Headform noise analysis...")
    # ppl.run_directory_parallel(
    #     pipeline=ppl.definitions.ALLAN_VARIANCE_PIPELINE_BOTH,
    #     src_dir=Path("data_processing_gitignore/stationary/headform_filtered/parsed"),
    #     output_dir=Path("data_processing_gitignore/stationary/headform_filtered/allan_variance"),
    #     log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/headform_filtered_allan_variance_log.csv"),
    # )
    # ppl.run_directory_parallel(
    #     pipeline=ppl.definitions.POWER_SPECTRAL_DENSITY_PIPELINE_BOTH,
    #     src_dir=Path("data_processing_gitignore/stationary/headform_filtered/parsed"),
    #     output_dir=Path("data_processing_gitignore/stationary/headform_filtered/power_spectral_density"),
    #     log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/headform_filtered_psd_log.csv"),
    # )
    # print("Headform noise analysis complete.")

    # print("PSD processing complete.")

if __name__ == "__main__":
    main()
