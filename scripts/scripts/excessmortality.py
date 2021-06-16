"""Get excess mortality dataset and publish it in public/data."""


import os
from datetime import datetime, timedelta

import pandas as pd


CURRENT_DIR = os.path.dirname(__file__)
SOURCE = (
    "https://github.com/owid/owid-datasets/raw/master/datasets/"
    "Excess%20Mortality%20Data%20%E2%80%93%20OWID%20(2021)/"
    "Excess%20Mortality%20Data%20%E2%80%93%20OWID%20(2021).csv"
)
DATA_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "../../public/data/"))
OUTPUT_PATH = os.path.join(DATA_DIR, "excess_mortality/excess_mortality.csv")
TIMESTAMP_DIR = os.path.abspath(os.path.join(DATA_DIR, "internal/timestamp"))


def read(source):
    return pd.read_csv(source)


def pipeline(df: pd.DataFrame):
    # Rename columns
    df = df.rename(columns={
        "Entity": "location",
        "Year": "date",
        "Excess mortality P-scores, all ages": "p_scores_all_ages",
        "Excess mortality P-scores, ages 0–14": "p_scores_0_14",
        "Excess mortality P-scores, ages 15–64": "p_scores_15_64",
        "Excess mortality P-scores, ages 65–74": "p_scores_65_74",
        "Excess mortality P-scores, ages 75–84": "p_scores_75_84",
        "Excess mortality P-scores, ages 85+": "p_scores_85plus",
        "Deaths, 2020, all ages": "deaths_2020_all_ages",
        "Average deaths, 2015–2019, all ages": "average_deaths_2015_2019_all_ages"
    })
    # Fix date
    df.loc[:, "date"] = [(datetime(2020, 1, 1) + timedelta(days=d)).strftime("%Y-%m-%d") for d in df.date]
    # Sort rows
    df = df.sort_values(["location", "date"])
    return df


def export_timestamp(timestamp_filename):
    with open(timestamp_filename, "w") as timestamp_file:
        timestamp_file.write(datetime.utcnow().replace(microsecond=0).isoformat())


def main():
    read(SOURCE).pipe(pipeline).to_csv(
        os.path.join(OUTPUT_PATH),
        index=False
    )
    timestamp_filename = os.path.join(
        TIMESTAMP_DIR,
        "owid-covid-data-last-updated-timestamp-xm.txt"
    )
    export_timestamp(timestamp_filename)


def update_dataset():
    # For backward compatibility
    main()


if __name__ == "__main__":
    main()
