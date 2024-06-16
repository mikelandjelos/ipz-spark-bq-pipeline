import datetime
import logging
import os
import pandas as pd
import time
from dotenv import find_dotenv, dotenv_values, load_dotenv, get_key

dotenv_path = find_dotenv(raise_error_if_not_found=True)
successfully_loaded = load_dotenv(dotenv_path)

if not successfully_loaded:
    raise EnvironmentError(f"Dotenv file {dotenv_path} not found!")
else:
    for name, value in dotenv_values().items():
        logging.warning(f"{name}=`{value}`")

OUTPUT_PATH = get_key(dotenv_path, "OUTPUT_PATH")
MERGED_OUTPUT_PATH = get_key(dotenv_path, "MERGED_OUTPUT_PATH")

if MERGED_OUTPUT_PATH is None:
    raise EnvironmentError("MERGED_OUTPUT_PATH is None")

merged_csv_path = os.path.join(
    MERGED_OUTPUT_PATH,
    f"merged_output_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.csv",
)

os.makedirs(MERGED_OUTPUT_PATH, exist_ok=True)

header = "window_start,device_mac,avg_carbon_oxide,avg_humidity,avg_liquid_petroleum_gas,avg_smoke,avg_temperature\n"


def csv_file_generator(input_path):
    for file in os.listdir(input_path):
        if file.endswith(".csv"):
            yield os.path.join(input_path, file)


try:
    header_written = False
    while True:
        new_files_processed = False
        try:
            for file_path in csv_file_generator(OUTPUT_PATH):
                if os.path.getsize(file_path) == 0:
                    logging.warning(f"Skipping empty file: {file_path}")
                    os.remove(file_path)
                    continue

                df = pd.read_csv(file_path)

                if not header_written:
                    with open(merged_csv_path, "w") as f:
                        f.write(header)
                    header_written = True

                df.to_csv(merged_csv_path, index=False, mode="a", header=False)

                logging.info(f"Merged doc {file_path}")
                os.remove(file_path)
                new_files_processed = True

            if new_files_processed:
                logging.critical(f"Merged new files into {merged_csv_path}")
        except Exception as ex:
            logging.error(str(ex))

        time.sleep(1)
except KeyboardInterrupt:
    logging.critical("Stopped the merging process.")
