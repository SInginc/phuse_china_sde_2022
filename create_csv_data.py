import os
import sys
import csv
import random
import time
import gc
import multiprocessing as mp
from typing import List, Tuple

from faker import Faker


def generate_rows(faker_obj, start: int, end: int, filename: str) -> Tuple:
    print(f"{os.getpid()} running from {start} to {end}")
    return filename, [generate_row(faker_obj, i) for i in range(start, end)]


def generate_row(faker_obj, row_number) -> Tuple:
    sex_range = ("M", "F")
    return (
        row_number,
        faker_obj.name(),
        faker_obj.address().replace("\n", " "),
        faker_obj.date(),
        random.choice(sex_range),
        faker_obj.country(),
        faker_obj.color(),
        faker_obj.company(),
        faker_obj.name(),
        random.randrange(1, 100),
        faker_obj.phone_number(),
        random.randrange(1, 220),
        random.randrange(1, 220),
    )


def generate_jobs(n_of_jobs: int, n_of_workers: int) -> List[Tuple[int]]:
    print(f"Generating {n_of_jobs} rows...")
    res = []
    workload = n_of_jobs // n_of_workers
    _start = 1
    for i in range(1, n_of_jobs + 2, workload):
        if i != _start:
            res.append((_start, i))
            _start = i
    if _start <= n_of_jobs:
        res.append((_start, n_of_jobs + 1))
    return res


def collect_results(callback) -> None:
    filename, result = callback
    print(f"Writing {len(result)} data to {filename}...")
    start = time.perf_counter()
    with open(filename, "a", encoding="utf-8") as wh:
        csv_writer = csv.writer(wh, delimiter=",")
        csv_writer.writerows(result)
    print(f"{len(result)} rows written to {filename}...")
    del callback
    gc.collect()


if __name__ == "__main__":
    nrows = int(sys.argv[1])
    filename = f"data/csv/{nrows}.csv"
    fake_generator = Faker("en_US")
    rownames = (
        "id",
        "name",
        "address",
        "birth_date",
        "sex",
        "country",
        "favorite_color",
        "company",
        "pet_name",
        "pet_age",
        "phone_number",
        "height_cm",
        "weight_kg",
    )

    with open(filename, "w+") as wh:
        csv_writer = csv.writer(wh, delimiter=",")
        csv_writer.writerow(rownames)

    jobs = generate_jobs(nrows, os.cpu_count())
    start = time.perf_counter()
    pool = mp.Pool(os.cpu_count())
    for job in jobs:
        pool.apply_async(
            func=generate_rows,
            args=(fake_generator, job[0], job[1], filename),
            callback=collect_results,
        )
    pool.close()
    pool.join()
    print(f"Used: {round(time.perf_counter() - start, 4)} seconds with {nrows} rows.")
