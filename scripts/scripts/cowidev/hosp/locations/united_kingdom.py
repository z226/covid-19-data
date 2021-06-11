from datetime import date

import pandas as pd


class UnitedKingdom():
    def __init__(self):
        self.source_url = (
            "https://api.coronavirus.data.gov.uk/v2/data?areaType=overview&metric=hospitalCases&metric=newAdmissions&"
            "metric=covidOccupiedMVBeds&format=csv"
        )
        self.location = "United Kingdom"

    def read(self):
        print("Downloading UK data…")
        return pd.read_csv(self.source_url, usecols=["date", "hospitalCases", "newAdmissions", "covidOccupiedMVBeds"])

    def pipe_process(self, df: pd.DataFrame) -> pd.DataFrame:
        df.loc[:, "date"] = pd.to_datetime(df["date"])

        stock = df[["date", "hospitalCases", "covidOccupiedMVBeds"]].copy()
        stock = stock.melt("date", var_name="indicator")
        stock.loc[:, "date"] = stock["date"].dt.date

        flow = df[["date", "newAdmissions"]].copy()
        flow.loc[:, "date"] = (flow["date"] + pd.to_timedelta(6 - flow["date"].dt.dayofweek, unit="d")).dt.date
        flow = flow[flow["date"] <= date.today()]
        flow = flow.groupby("date", as_index=False).agg({"newAdmissions": ["sum", "count"]})
        flow.columns = ["date", "newAdmissions", "count"]
        flow = flow[flow["count"] == 7]
        flow = flow.drop(columns="count").melt("date", var_name="indicator")

        df = pd.concat([stock, flow]).dropna(subset=["value"])
        df.loc[:, "indicator"] = df["indicator"].replace({
            "hospitalCases": "Daily hospital occupancy",
            "covidOccupiedMVBeds": "Daily ICU occupancy",
            "newAdmissions": "Weekly new hospital admissions",
        })
        return df

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            entity=self.location,
            iso_code="GBR",
            population=67886004,
        )

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .pipe(self.pipe_process)
            .pipe(self.pipe_metadata)
        )

    def run(self):
        return self.read().pipe(self.pipeline)
