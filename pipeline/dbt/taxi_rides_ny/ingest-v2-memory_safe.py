import duckdb
import requests
from pathlib import Path
import sys
import shutil

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
fhv_flag = sys.argv[1] if len(sys.argv) > 1 else None


def download_and_convert_files(taxi_type, years):
    data_dir = Path("data") / taxi_type
    data_dir.mkdir(exist_ok=True, parents=True)

    for year in years:
        for month in range(1, 13):
            parquet_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            parquet_filepath = data_dir / parquet_filename

            if parquet_filepath.exists():
                print(f"Skipping {parquet_filename} (already exists)")
                continue

            csv_gz_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
            csv_gz_filepath = data_dir / csv_gz_filename

            print(f"Downloading {BASE_URL}/{taxi_type}/{csv_gz_filename}...")
            response = requests.get(f"{BASE_URL}/{taxi_type}/{csv_gz_filename}", stream=True)
            response.raise_for_status()

            with open(csv_gz_filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            print(f"Converting {csv_gz_filename} to Parquet...")
            con = duckdb.connect()
            con.execute(f"""
                COPY (SELECT * FROM read_csv_auto('{csv_gz_filepath}'))
                TO '{parquet_filepath}' (FORMAT PARQUET)
            """)
            con.close()

            csv_gz_filepath.unlink()
            print(f"Completed {parquet_filename}")


def load_parquet_folder_to_table(con, taxi_type):
    folder = Path("data") / taxi_type
    parquet_files = sorted(folder.glob("*.parquet"))

    if not parquet_files:
        print(f"No parquet files found for {taxi_type}")
        return

    table_name = f"prod.{taxi_type}_tripdata"

    print(f"Creating {table_name} from first file...")
    first_file = parquet_files[0].as_posix()

    con.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT *
        FROM read_parquet('{first_file}')
    """)

    for parquet_file in parquet_files[1:]:
        parquet_path = parquet_file.as_posix()
        print(f"Appending {parquet_file.name} into {table_name}...")
        con.execute(f"""
            INSERT INTO {table_name}
            SELECT *
            FROM read_parquet('{parquet_path}')
        """)

    print(f"Cleaning {taxi_type}")
    shutil.rmtree(folder)


def update_gitignore():
    gitignore_path = Path(".gitignore")
    content = gitignore_path.read_text() if gitignore_path.exists() else ""

    if 'data/' not in content:
        with open(gitignore_path, 'a') as f:
            f.write('\n# Data directory\ndata/\n' if content else '# Data directory\ndata/\n')


if __name__ == "__main__":

    # --- FHV ---
    download_and_convert_files("fhv", [2019])

    con = duckdb.connect("taxi_rides_ny.duckdb")
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS prod")
        print("Inserting fhv parquet files into db...")
        load_parquet_folder_to_table(con, "fhv")
    finally:
        con.close()

    # --- Yellow + Green ---
    for taxi_type in ["yellow", "green"]:
        download_and_convert_files(taxi_type, [2019, 2020])

    con = duckdb.connect("taxi_rides_ny.duckdb")
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS prod")

        for taxi_type in ["yellow", "green"]:
            print(f"Inserting {taxi_type} parquet files into db...")
            load_parquet_folder_to_table(con, taxi_type)

    finally:
        con.close()

    update_gitignore()

    print("Done! Enjoy!")