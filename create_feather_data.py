import time
import polars as pl
from faker import Faker

if __name__ == "__main__":
    data_10m = "data/10000000.csv"
    df = pl.read_csv(
        file=data_10m,
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
    start = time.perf_counter()
    print(f"Writing to {data_10m}")
    df.write_ipc("data/10000000.feather")
    print(f"Used: {round(time.perf_counter() - start, 4)} seconds.")
