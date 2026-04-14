import duckdb
import requests
from pathlib import Path
import sys
import shutil

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
# run as python ingest.py fhv for downloading fhw_tripdata
fhv_flag = sys.argv[1] if len(sys.argv) > 1 else None

def download_and_convert_files(taxi_type, years):
    data_dir = Path("data") / taxi_type
    # parents=True - create the nested folder dir/dir2
    data_dir.mkdir(exist_ok=True, parents=True)

    for year in years:
        for month in range(1, 13):
            parquet_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            parquet_filepath = data_dir / parquet_filename

            if parquet_filepath.exists():
                print(f"Skipping {parquet_filename} (already exists)")
                continue

            # Download CSV.gz file
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

            # Remove the CSV.gz file to save space
            csv_gz_filepath.unlink()
            print(f"Completed {parquet_filename}")

def update_gitignore():
    gitignore_path = Path(".gitignore")

    # Read existing content or start with empty string
    content = gitignore_path.read_text() if gitignore_path.exists() else ""

    # Add data/ if not already present
    if 'data/' not in content:
        with open(gitignore_path, 'a') as f:
            f.write('\n# Data directory\ndata/\n' if content else '# Data directory\ndata/\n')

if __name__ == "__main__":
        download_and_convert_files("fhv", [2019])
        con = duckdb.connect("taxi_rides_ny.duckdb")
        con.execute("CREATE SCHEMA IF NOT EXISTS prod")
        print("Inserting fhv parquet files into db...")
        con.execute(f"""
                CREATE OR REPLACE TABLE prod.{"fhv"}_tripdata AS
                SELECT * FROM read_parquet('data/{"fhv"}/*.parquet', union_by_name=true)
            """)
        con.close()

        print("Cleaning fhv")
        shutil.rmtree(Path("data") / "fhv")


        # Update .gitignore to exclude data directory
        update_gitignore()

        for taxi_type in ["yellow", "green"]:
            download_and_convert_files(taxi_type, [2019, 2020])

        con = duckdb.connect("taxi_rides_ny.duckdb")
        con.execute("CREATE SCHEMA IF NOT EXISTS prod")
        
        for taxi_type in ["yellow", "green"]:
            print(f"Inserting {taxi_type} parquet files into db")
            con.execute(f"""
                CREATE OR REPLACE TABLE prod.{taxi_type}_tripdata AS
                SELECT * FROM read_parquet('data/{taxi_type}/*.parquet', union_by_name=true)
            """)
            print(f"Cleaning {taxi_type}")
            shutil.rmtree(Path("data") / taxi_type)

        con.close()

        print("Done! Enjoy!")