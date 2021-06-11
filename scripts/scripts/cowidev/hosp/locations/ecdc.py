import sys
import os
import pandas as pd


CURRENT_DIR = os.path.dirname(__file__)
sys.path.append(CURRENT_DIR)
INPUT_PATH = os.path.join(CURRENT_DIR, "../../../input/")


df_population = pd.read_csv(
    os.path.join(INPUT_PATH, "un/population_2020.csv"),
    usecols=["iso_code", "entity", "population"]
)


class ECDC:
    def __init__(self):
        self.source_url = "https://opendata.ecdc.europa.eu/covid19/hospitalicuadmissionrates/csv/data.csv"

    def read(self):
        print("Downloading ECDC data…")
        return pd.read_csv(
            self.source_url,
            usecols=["country", "indicator", "date", "value", "year_week"]
        )

    def pipe_process(self, df: pd.DataFrame) -> pd.DataFrame:
        df = (
            df
            .drop_duplicates()
            .rename(columns={"country": "entity"})
            .merge(df_population, on="entity", how="left")
        )
        if df[df.population.isna()].shape[0] != 0:
            raise ValueError("Country missing from population file")

        df.loc[df["indicator"].str.contains(" per 100k"), "value"] = (
            df["value"].div(100000).mul(df["population"])
        )
        df.loc[:, "indicator"] = df["indicator"].str.replace(" per 100k", "")
        return df

    def pipe_date(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.date.dtypes == "int64":
            df["date"] = pd.to_datetime(df.date, format="%Y%m%d").dt.date
        daily_records = df[df["indicator"].str.contains("Daily")]
        date_week_mapping = (
            daily_records[["year_week", "date"]].groupby("year_week", as_index=False).max()
        )
        weekly_records = df[df["indicator"].str.contains("Weekly")].drop(columns="date")
        weekly_records = pd.merge(weekly_records, date_week_mapping, on="year_week")
        df = pd.concat([daily_records, weekly_records]).drop(columns="year_week")
        return df
    
    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .pipe(self.pipe_process)
            .pipe(self.pipe_date)
        )

    def run(self):
        return self.read().pipe(self.pipeline)
