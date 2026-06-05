from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
import pandas as pd 
import concurrent.futures
import shutil

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
        ctx.setdefault("errors", [])

        df, ctx = self.loader(input_path, ctx)

        if df is None:
            return [], ctx

        for t in self.transforms:
            df, ctx = t(df, ctx)
            if df is None:
                break

        if df is None:
            return [], ctx

        output_files = self.saver(df, ctx)

        # attach per-file result for reporting
        ctx["_outputs"] = output_files

        return output_files, ctx
    
# =========================================================
# DIRECTORY RUNNER
# =========================================================

def _process_single_file(args: tuple) -> Context:
    pipeline, file, output_dir, extra_context = args
    
    ctx: Context = {
        "output_dir": output_dir,
        "input_path": file,
        **(extra_context or {}),
    }
    
    _, ctx = pipeline.run(file, ctx)
    return ctx

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

    files = list(src_dir.rglob(extension))
    output_dir.mkdir(parents=True, exist_ok=True)

    for i, file in enumerate(files):
        print(f"{i}/{len(files)}: Processing {file.name}")

        # Pack the arguments into a tuple matching what _process_single_file expects
        args = (pipeline, file, output_dir, extra_context)
        
        # Execute via the helper function
        ctx = _process_single_file(args)
        if "errors" in ctx:
            if len(ctx["errors"])>0:
                print(f"+ WARNING encountered errors : {ctx["errors"]}")
        else:
            print("- no errors column")
        
        contexts.append(ctx)

    log_df = pd.DataFrame(contexts)
    log_df.to_csv(log_path, index=False)

    return log_df


def run_directory_parallel(
    pipeline: Pipeline,
    src_dir: Path,
    output_dir: Path,
    log_path: Path,
    extra_context: Context | None = None,
    extension: str = "*.csv",
    max_workers: int | None = None, # None will default to the number of processors on your machine
) -> pd.DataFrame:

    print(f"--------{src_dir}-------")
    files = list(src_dir.rglob(extension))
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Found {len(files)} files. Starting parallel processing...")

    # 1. Package the arguments for the workers
    tasks = [
        (pipeline, file, output_dir, extra_context)
        for file in files
    ]

    contexts: list[Context] = []

    # 2. Spin up the pool (Swap ProcessPoolExecutor for ThreadPoolExecutor if you hit pickling errors)
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        
        # executor.map guarantees the results are returned in the exact order the tasks were submitted
        for i, ctx in enumerate(executor.map(_process_single_file, tasks)):
            print(f"Completed {i+1}/{len(files)}: {ctx['input_path'].name}")
            if "errors" in ctx:
                if len(ctx["errors"])>0:
                    print(f"+ WARNING encountered errors : {ctx["errors"]}")
            else:
                print("- no errors column")
            contexts.append(ctx)

    # 3. Save the logs
    log_df = pd.DataFrame(contexts)
    log_df.to_csv(log_path, index=False)

    return log_df