from datetime import date

import pandas as pd


class UnitedStates():
    def __init__(self):
        self.source_url = "https://healthdata.gov/api/views/g62h-syeh/rows.csv"
        self.location = "United States"

    def read(self):
        print("Downloading US data…")
        return pd.read_csv(self.source_url, usecols=[
            "date",
            "total_adult_patients_hospitalized_confirmed_covid",
            "total_pediatric_patients_hospitalized_confirmed_covid",
            "staffed_icu_adult_patients_confirmed_covid",
            "previous_day_admission_adult_covid_confirmed",
            "previous_day_admission_pediatric_covid_confirmed",
        ])

    def pipe_process(self, df: pd.DataFrame) -> pd.DataFrame:
        df.loc[:, "date"] = pd.to_datetime(df.date)
        df = df[df.date >= pd.to_datetime("2020-07-15")]
        df = df.groupby("date", as_index=False).sum()

        stock = df[[
            "date",
            "total_adult_patients_hospitalized_confirmed_covid",
            "total_pediatric_patients_hospitalized_confirmed_covid",
            "staffed_icu_adult_patients_confirmed_covid",
        ]].copy()
        stock.loc[:, "Daily hospital occupancy"] = (
            stock.total_adult_patients_hospitalized_confirmed_covid
            .add(stock.total_pediatric_patients_hospitalized_confirmed_covid)
        )
        stock = stock.rename(columns={
            "staffed_icu_adult_patients_confirmed_covid": "Daily ICU occupancy"
        })
        stock = stock[["date", "Daily hospital occupancy", "Daily ICU occupancy"]]
        stock = stock.melt(id_vars="date", var_name="indicator")
        stock.loc[:, "date"] = stock["date"].dt.date

        flow = df[[
            "date",
            "previous_day_admission_adult_covid_confirmed",
            "previous_day_admission_pediatric_covid_confirmed",
        ]].copy()
        flow.loc[:, "value"] = (
            flow.previous_day_admission_adult_covid_confirmed
            .add(flow.previous_day_admission_pediatric_covid_confirmed)
        )
        flow.loc[:, "date"] = (
            (flow["date"] + pd.to_timedelta(6 - flow["date"].dt.dayofweek, unit="d")).dt.date
        )
        flow = flow[flow["date"] <= date.today()]
        flow = flow[["date", "value"]]
        flow = flow.groupby("date", as_index=False).agg({"value": ["sum", "count"]})
        flow.columns = ["date", "value", "count"]
        flow = flow[flow["count"] == 7]
        flow = flow.drop(columns="count")
        flow.loc[:, "indicator"] = "Weekly new hospital admissions"

        # Merge all subframes
        df = pd.concat([stock, flow])
        return df

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            entity=self.location,
            iso_code="USA",
            population=331002647,
        )

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .pipe(self.pipe_process)
            .pipe(self.pipe_metadata)
        )

    def run(self):
        return self.read().pipe(self.pipeline)
