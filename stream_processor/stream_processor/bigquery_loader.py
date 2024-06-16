import logging
import os
from google.cloud import bigquery
import pandas as pd
from glob import glob
from dotenv import dotenv_values, find_dotenv, load_dotenv, get_key

logging.basicConfig(level=logging.DEBUG)

dotenv_path = find_dotenv(raise_error_if_not_found=True)
successfully_loaded = load_dotenv(dotenv_path)

if not successfully_loaded:
    raise EnvironmentError(f"Dotenv file {dotenv_path} not found!")
else:
    for name, value in dotenv_values().items():
        logging.warning(f"{name}=`{value}`")

PROJECT_ID = get_key(dotenv_path, "PROJECT_ID")
DATASET_ID = get_key(dotenv_path, "DATASET_ID")
TABLE_ID = get_key(dotenv_path, "TABLE_ID")
GOOGLE_APPLICATION_CREDENTIALS = get_key(dotenv_path, "GOOGLE_APPLICATION_CREDENTIALS")
MERGED_OUTPUT_PATH = get_key(dotenv_path, "MERGED_OUTPUT_PATH")

if GOOGLE_APPLICATION_CREDENTIALS is None:
    raise EnvironmentError("GOOGLE_APPLICATION_CREDENTIALS is None")

if TABLE_ID is None:
    raise EnvironmentError("TABLE_ID is None")

if MERGED_OUTPUT_PATH is None:
    raise EnvironmentError("MERGED_OUTPUT_PATH is None")

destination_table = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

files = glob(os.path.join(MERGED_OUTPUT_PATH, "merged_output_*.csv"))

if not files:
    raise FileNotFoundError(f"No CSV files found in {MERGED_OUTPUT_PATH}")

latest_file = max(files, key=os.path.getctime)
logging.info(f"Loading data from the latest file: {latest_file}")

client = bigquery.Client()

df = pd.read_csv(latest_file)
df["window_start"] = pd.to_datetime(df["window_start"], format="%Y-%m-%d %H:%M:%S")

logging.debug(f"DataFrame shape: {df.shape}")
logging.debug(f"DataFrame contents:\n{df.head(10)}")

if df.empty:
    raise ValueError("The DataFrame is empty after loading the CSV file.")

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    schema=[
        bigquery.SchemaField("window_start", bigquery.enums.SqlTypeNames.TIMESTAMP),
        bigquery.SchemaField("device_mac", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("avg_carbon_oxide", bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField("avg_humidity", bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField(
            "avg_liquid_petroleum_gas", bigquery.enums.SqlTypeNames.FLOAT
        ),
        bigquery.SchemaField("avg_smoke", bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField("avg_temperature", bigquery.enums.SqlTypeNames.FLOAT),
    ],
)

try:
    load_job = client.load_table_from_dataframe(
        df,
        destination=destination_table,
        job_config=job_config,
    )

    load_job.result()

    if load_job.errors:
        logging.error(f"Load job errors: {load_job.errors}")

    logging.critical(
        f"{len(df)} elements loaded from `{latest_file}` into `{destination_table}`"
    )
except KeyboardInterrupt:
    logging.error("You terminated the process!")
except Exception as e:
    logging.error(f"Error loading data to BigQuery: {e}")

logging.debug(f"Load job status: {load_job.state}")

table = client.get_table(destination_table)

logging.critical(
    f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {destination_table}"
)
