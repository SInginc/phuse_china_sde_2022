import os
import time
import platform
from pathlib import Path

import pandas as pd
import polars as pl
import numpy as np


def get_file_size(filename) -> None:
    size = os.stat(filename).st_size
    if size < 1024:
        unit = "bytes"
    elif size < 1024**2:
        unit = "kilobytes"
        size = size / (1024)
    elif size < 1024**3:
        unit = "megabytes"
        size = size / (1024**2)
    else:
        unit = "gigabytes"
        size = size / (1024**3)
    print(f"{filename} is {round(size, 4)} {unit}")


def time_loading(data_file: str, func, **kwargs) -> float:
    print(f"Running {func.__module__}.{func.__name__}")
    get_file_size(data_file)
    start = time.perf_counter()
    func(**kwargs)
    t = round(time.perf_counter() - start, 4)
    print(f"Used: {t}s")
    return t


if __name__ == "__main__":
    print(f"Using Python of {platform.python_version()} version")
    print(f"Using Polars of {pl.__version__} version")
    print(f"Using Pandas of {pd.__version__} version")
    print(f"Using Numpy of {np.__version__} version")

    res = []
    csv_data_dir = Path("data/csv")
    # for csv_file in csv_data_dir.iterdir():
    #     t = time_loading(csv_file, pl.read_csv, file=csv_file, has_header=True)
    res = [
        (csv_file, time_loading(csv_file, pl.read_csv, file=csv_file, has_header=True))
        for csv_file in csv_data_dir.iterdir()
    ]
    res = sorted(res, key=lambda x: x[0])
    print(res)

    # feather_data_dir = Path("data/feather")
    # for feather_file in feather_data_dir.iterdir():
    #     time_loading(feather_file, pl.read_ipc, file=feather_file)
