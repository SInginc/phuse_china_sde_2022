import os
import sys
import csv
import random
import time
import multiprocessing as mp
from typing import List, Tuple

from faker import Faker


def generate_rows(faker_obj, start: int, end: int) -> List[Tuple]:
    print(f"{os.getpid()} running from {start} to {end}")
    return [generate_row(faker_obj, i) for i in range(start, end)]


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
    return res


results = []


def collect_results(result) -> None:
    results.extend(result)
    print(f"{len(result)} rows appended.")


if __name__ == "__main__":
    nrows = int(sys.argv[1])
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
    jobs = generate_jobs(nrows, os.cpu_count())
    start = time.perf_counter()
    pool = mp.Pool(os.cpu_count())
    for job in jobs:
        pool.apply_async(
            func=generate_rows,
            args=(fake_generator, job[0], job[1]),
            callback=collect_results,
        )
    pool.close()
    pool.join()
    print(
        f"Used: {round(time.perf_counter() - start, 4)} seconds with {len(results)} rows."
    )

    filename = f"data/{nrows}.csv"
    print(f"Writing data to {filename}...")
    start = time.perf_counter()
    with open(filename, "w+", encoding="utf-8") as wh:
        csv_writer = csv.writer(wh, delimiter=",")
        csv_writer.writerow(rownames)
        csv_writer.writerows(results)
    print(
        f"Used: {round(time.perf_counter() - start, 4)} seconds.\nData written to {filename}..."
    )
