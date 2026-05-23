from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
import pandas as pd 

# =========================================================
# TYPES
# =========================================================

Context = dict[str, Any]

Loader = Callable[[Path, Context], tuple[pd.DataFrame, Context]]

Transform = Callable[[pd.DataFrame, Context], tuple[pd.DataFrame, Context]]

Saver = Callable[[pd.DataFrame, Context], list[Path]]


# =========================================================
# PIPELINE
# =========================================================


@dataclass(slots=True)
class Pipeline:

    name: str

    loader: Loader
    transforms: list[Transform]
    saver: Saver

    def run(
        self,
        input_path: Path,
        context: Context | None = None,
    ) -> tuple[list[Path], Context]:

        ctx: Context = context or {}

        df, ctx = self.loader(input_path, ctx)

        if df is None:
            return [], ctx

        for t in self.transforms:
            df, ctx = t(df, ctx)

        output_files = self.saver(df, ctx)

        # attach per-file result for reporting
        ctx["_outputs"] = output_files

        return output_files, ctx
    
# =========================================================
# DIRECTORY RUNNER
# =========================================================

def run_directory(
    pipeline: Pipeline,
    src_dir: Path,
    output_dir: Path,
    log_path: Path,
    extra_context: Context | None = None,
    extension: str = "*.csv",
) -> pd.DataFrame:

    print(f"--------{src_dir}-------")
    contexts: list[Context] = []

    files = list(src_dir.glob(extension))

    for i, file in enumerate(files):
        print(f"{i}/{len(files)}: Processing {file.name}")

        ctx: Context = {
            "output_dir": output_dir,
            "input_path": file,
            **(extra_context or {}),
        }

        _, ctx = pipeline.run(file, ctx)
        contexts.append(ctx)

    log_df = pd.DataFrame(contexts)
    log_df.to_csv(log_path, index=False)

    return log_df
