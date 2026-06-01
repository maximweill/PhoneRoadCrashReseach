if __name__ == '__main__':
    from pathlib import Path
    import pipelines as ppl

    individual_dir = Path("data_processing_gitignore/phone_drop_test_data/agreement")
    
    # Path to the aggregated characteristics from drops
    chars_path = Path("data_processing_gitignore/phone_characteristics/aggregated/characteristics_drops.csv")

    print("Running correlation parameter analysis with saturation ignoring...")
    
    agreement_results = ppl.run_directory(
        pipeline=ppl.definitions.CORRELATION_PARAMETERS,
        src_dir=Path("data_processing_gitignore/phone_drop_test_data/correlation"),
        output_dir=individual_dir,
        log_path=individual_dir / "agreement.csv",
        extra_context={
            "characteristics_path": chars_path
        }
    )

    print(f"Workflow complete. Results saved to {individual_dir / 'agreement.csv'}")
