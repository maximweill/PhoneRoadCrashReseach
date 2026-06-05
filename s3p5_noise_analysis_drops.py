from pathlib import Path
import pipelines as ppl

def main():

    headform_results = ppl.run_directory_parallel(
        pipeline=ppl.definitions.HEADFORM_PARSING_PIPELINE,
        src_dir=Path("data_processing_gitignore/RAW_DATA/transformed_headform"),
        output_dir=Path("data_processing_gitignore/phone_drop_test_data/headform"),
        log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/headform_parsing_log.csv"),
        extension="*.xlsx",
        extra_context = {
            "phone_characteristics_aggregated": Path("data_processing_gitignore/phone_characteristics/aggregated/characteristics_drops.csv")
        }
    )

    ppl.run_directory_parallel(
        pipeline=ppl.definitions.POWER_SPECTRAL_DENSITY_PIPELINE_BOTH,
        src_dir=Path("data_processing_gitignore/phone_drop_test_data/headform"),
        output_dir=Path("data_processing_gitignore/phone_drop_test_data/power_spectral_density/headform"),
        log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/head_psd_log.csv"),
    )

    ppl.run_directory_parallel(
        pipeline=ppl.definitions.POWER_SPECTRAL_DENSITY_PIPELINE_BOTH,
        src_dir=Path("data_processing_gitignore/phone_drop_test_data/framed"),
        output_dir=Path("data_processing_gitignore/phone_drop_test_data/power_spectral_density/framed"),
        log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/framed_psd_log.csv"),
    )
    
    ppl.run_directory_parallel(
        pipeline=ppl.definitions.POWER_SPECTRAL_DENSITY_PIPELINE_BOTH,
        src_dir=Path("data_processing_gitignore/phone_drop_test_data/reference"),
        output_dir=Path("data_processing_gitignore/phone_drop_test_data/power_spectral_density/reference"),
        log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/reference_psd_log.csv"),
    )

if __name__ == "__main__":
    main()