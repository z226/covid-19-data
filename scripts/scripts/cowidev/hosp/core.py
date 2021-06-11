import datetime
import os

import pandas as pd


from cowidev.utils import export_timestamp
from cowidev.hosp.locations import Canada, UnitedStates, UnitedKingdom, ECDC, Israel


CURRENT_DIR = ""
INPUT_PATH = os.path.join(CURRENT_DIR, "../input/")
TIMESTAMP_PATH = os.path.join(CURRENT_DIR, "..", "..", "..", "public", "data", "internal", "timestamp")
GRAPHER_PATH = os.path.join(CURRENT_DIR, "..", "..", "grapher")


def load_data() -> pd.DataFrame:
    return pd.concat([
        Canada().run(),
        ECDC().run(),
        UnitedKingdom().run(),
        UnitedStates().run(),
        Israel().run(),
    ])

def add_per_million(df: pd.DataFrame) -> pd.DataFrame:
    per_million = df.copy()
    per_million.loc[:, "value"] = per_million["value"].div(per_million["population"]).mul(1000000)
    per_million.loc[:, "indicator"] = per_million["indicator"] + " per million"
    df = pd.concat([df, per_million]).drop(columns="population")
    return df


def owid_format(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[:, "value"] = df["value"].round(3)
    df = df.drop(columns="iso_code")

    # Data cleaning
    df = df[-df["indicator"].str.contains("Weekly new plot admissions")]
    df = df.groupby(["entity", "date", "indicator"], as_index=False).max()

    df = df.pivot_table(index=["entity", "date"], columns="indicator").value.reset_index()
    df = df.rename(columns={"entity": "Country"})
    return df


def date_to_owid_year(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[:, "date"] = (pd.to_datetime(df.date, format="%Y-%m-%d") - datetime.datetime(2020, 1, 21)).dt.days
    df = df.rename(columns={"date": "Year"})
    return df


def pipeline(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df
        .pipe(add_per_million)
        .pipe(owid_format)
        .pipe(date_to_owid_year)
    )


def export_hospitalizations():
    df = load_data().pipe(pipeline)
    # Export data
    df.to_csv(os.path.join(GRAPHER_PATH, "COVID-2019 - Hospital & ICU.csv"), index=False)
    # Export timestamp
    filename = os.path.join(TIMESTAMP_PATH, "owid-covid-data-last-updated-timestamp-hosp.txt")
    export_timestamp(filename)
