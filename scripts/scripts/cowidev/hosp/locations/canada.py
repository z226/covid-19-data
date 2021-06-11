import requests
import json

import pandas as pd


class Canada():
    def __init__(self):
        self.source_url = "https://api.covid19tracker.ca/reports?after=2020-03-09"
        self.location = "Canada"

    def read(self):
        print("Downloading Canada data…")
        data = requests.get(self.source_url).json()
        data = json.dumps(data["data"])
        df = pd.read_json(data, orient="records")
        return df

    def pipe_process(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[["date", "total_hospitalizations", "total_criticals"]]
        df = df.melt("date", ["total_hospitalizations", "total_criticals"], "indicator")
        df.loc[:, "indicator"] = df["indicator"].replace({
            "total_hospitalizations": "Daily hospital occupancy",
            "total_criticals": "Daily ICU occupancy"
        })
        df.loc[:, "date"] = df["date"].dt.date
        return df

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            entity=self.location,
            iso_code="CAN",
            population=37742157,
        )

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .pipe(self.pipe_process)
            .pipe(self.pipe_metadata)
        )

    def run(self):
        return self.read().pipe(self.pipeline)
