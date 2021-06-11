import datetime
import os
import sys
import pytz


CURRENT_DIR = os.path.dirname(__file__)
sys.path.append(CURRENT_DIR)

from cowidev.utils.db_imports import import_dataset
from cowidev.hosp import export_hospitalizations


GRAPHER_PATH = os.path.join(CURRENT_DIR, "../grapher/")
DATASET_NAME = "COVID-2019 - Hospital & ICU"
OUTPUT_CSV_PATH = os.path.join(GRAPHER_PATH, DATASET_NAME + ".csv")
ZERO_DAY = "2020-01-21"


def update_db():
    time_str = datetime.datetime.now().astimezone(pytz.timezone("Europe/London")).strftime("%-d %B, %H:%M")
    source_name = (
        f"European CDC for EU countries, government sources for other countries – Last updated {time_str} "
        "(London time)"
    )
    import_dataset(
        dataset_name=DATASET_NAME,
        namespace='owid',
        csv_path=OUTPUT_CSV_PATH,
        default_variable_display={
            'yearIsDay': True,
            'zeroDay': ZERO_DAY
        },
        source_name=source_name,
        slack_notifications=True
    )


if __name__ == "__main__":
    export_hospitalizations()
