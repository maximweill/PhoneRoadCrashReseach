

SIMPLE_SAMPLING_PLOT = Pipeline(
    name="simple_sampling_plot",
    loader=load_csv,
    transforms=[
    ],
    saver=save_interactive_plot,
)

DOUBLED_SAMPLING_PLOT = Pipeline(
    name="simple_sampling_plot",
    loader=load_csv,
    transforms=[
    ],
    saver=save_doubled_interactive_plot,
)

def main():
    from pathlib import Path
    import pipelines as ppl

    fig_dir = Path("my_app/data/sampling_figs")



    ind_stationary_results = ppl.run_directory_parallel(
        pipeline=ppl.definitions.DOUBLED_SAMPLING_PLOT,
        src_dir=Path("data_processing_gitignore/RAW_DATA/ind_stationary"),
        output_dir=Path("data_processing_gitignore/stationary/end2/parsed"),
        log_path=Path("data_processing_gitignore/DEBUGGING_LOGS/end2_stationary_parsing_log.csv")
    )

if __name__ == "__main__":
    main()