from datetime import date

import pandas as pd
import numpy as np


class Israel():
    def __init__(self):
        self.source_url = "https://datadashboardapi.health.gov.il/api/queries/patientsPerDate"
        self.location = "Israel"

    def read(self):
        print("Downloading Israel data…")
        return pd.read_json(self.source_url)

    def pipe_process(self, df: pd.DataFrame) -> pd.DataFrame:
        df.loc[:, "date"] = pd.to_datetime(df["date"])
        stock = df[["date", "Counthospitalized", "CountCriticalStatus"]].copy()
        stock.loc[:, "date"] = stock["date"].dt.date
        stock.loc[stock["date"].astype(str) < "2020-08-17", "CountCriticalStatus"] = np.nan
        stock = stock.melt("date", var_name="indicator")

        flow = df[["date", "new_hospitalized", "serious_critical_new"]].copy()
        flow.loc[:, "date"] = (flow["date"] + pd.to_timedelta(6 - flow["date"].dt.dayofweek, unit="d")).dt.date
        flow = flow[flow["date"] <= date.today()]
        flow = flow.groupby("date", as_index=False).agg({
            "new_hospitalized": ["sum", "count"],
            "serious_critical_new": "sum"
        })
        flow.columns = ["date", "new_hospitalized", "count", "serious_critical_new"]
        flow = flow[flow["count"] == 7]
        flow = flow.drop(columns="count").melt("date", var_name="indicator")

        df = pd.concat([stock, flow]).dropna(subset=["value"])
        df.loc[:, "indicator"] = df["indicator"].replace({
            "Counthospitalized": "Daily hospital occupancy",
            "CountCriticalStatus": "Daily ICU occupancy",
            "new_hospitalized": "Weekly new hospital admissions",
            "serious_critical_new": "Weekly new ICU admissions"
        })
        return df

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            entity=self.location,
            iso_code="ISR",
            population=8655541,
        )

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .pipe(self.pipe_process)
            .pipe(self.pipe_metadata)
        )

    def run(self):
        return self.read().pipe(self.pipeline)
