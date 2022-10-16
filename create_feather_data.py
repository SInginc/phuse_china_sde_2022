import sys
import time
import polars as pl

if __name__ == "__main__":
    nrows = sys.argv[1]
    feather_file = f"data/feather/{nrows}.feather"
    csv_data = f"data/csv/{nrows}.csv"

    print(f"Reading {csv_data}")
    start = time.perf_counter()
    df = pl.read_csv(
        file=csv_data,
        has_header=True,
        columns=(
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
        ),
    )
    print(f"read Used: {round(time.perf_counter() - start, 4)}")

    start = time.perf_counter()
    print(f"Writing to {feather_file}")
    df.write_ipc(feather_file)
    print(f"Used: {round(time.perf_counter() - start, 4)} seconds.")
