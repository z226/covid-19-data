import os
import pandas as pd
from glob import glob
from vax.utils.incremental import enrich_data, increment
import requests


class UnitedStates:

    def __init__(self, source_url: str, location: str):
        self.source_url = source_url
        self.location = location

    def read(self) -> pd.Series:
        data = requests.get(self.source_url).json()
        data = data["vaccination_data"]

        for d in data:
            if d["ShortName"] == "USA":
                data = d
                break

        return pd.Series({
            "total_vaccinations": data["Doses_Administered"],
            "people_vaccinated": data["Administered_Dose1_Recip"],
            "people_fully_vaccinated": data["Series_Complete_Yes"],
            "date": self.parse_date(data)
        })

    def parse_date(self, data: pd.DataFrame):
        date = data["Date"]
        try:
            date = pd.to_datetime(date, format="%m/%d/%Y")
        except Exception:
            date = pd.to_datetime(date, format="%Y-%m-%d")
        date = str(date.date())
        return date

    def read_by_age_group(self) -> pd.DataFrame:
        url = "https://data.cdc.gov/resource/km4m-vcsb.json"
        data = requests.get(url).json()
        columns = ['total_vaccinations', 'date', 'age_group']
        rows = []
        age = ['Ages_75+_yrs', 'Ages_<18yrs', 'Ages_18-29_yrs', 'Ages_30-39_yrs',
               'Ages_40-49_yrs', 'Ages_65-74_yrs', 'Ages_50-64_yrs']
        for s in range(len(data)):
            age_group = data[s]["demographic_category"]
            total_vaccinations = int(data[s]["administered_dose1"]) + int(data[s]["series_complete_yes"])
            date = pd.to_datetime(data[s]["date"], format="%Y-%m-%d").date()
            if age_group in age:
                if age_group == "Ages_<18yrs":
                    rows.append([total_vaccinations, date, "0-18"])
                elif age_group == "Ages_18-29_yrs":
                    rows.append([total_vaccinations, date, "18-29"])
                elif age_group == "Ages_30-39_yrs":
                    rows.append([total_vaccinations, date, "30-39"])
                elif age_group == "Ages_40-49_yrs":
                    rows.append([total_vaccinations, date, "40-49"])
                elif age_group == "Ages_50-64_yrs":
                    rows.append([total_vaccinations, date, "50-64"])
                elif age_group == "Ages_65-74_yrs":
                    rows.append([total_vaccinations, date, "65-74"])
                elif age_group == "Ages_75+_yrs":
                    rows.append([total_vaccinations, date, "75+"])

        df_age = pd.DataFrame(rows, columns=columns)
        return df_age

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, 'location', self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, 'vaccine', "Johnson&Johnson, Moderna, Pfizer/BioNTech")

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, 'source_url', self.source_url)

    def pipe_rename_columns_by_age_group(self, df: pd.DataFrame) -> pd.DataFrame:
        df.rename(columns={"Ages_<18yrs": "0-18", "Ages_18-29_yrs": "18-29", "Ages_18-29_yrs": "18-29",
                           "Ages_30-39_yrs": "30-39", "Ages_40-49_yrs": "40-49", "Ages_50-64_yrs": "50-64"
            , "Ages_65-74_yrs": "65-74", "Ages_75+_yrs": "75+"})
        return df

    def pipe_location_by_age_group(self, df: pd.DataFrame) -> pd.DataFrame:
        df[["location"]] = "United States"
        return df

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return (
            ds
                .pipe(self.pipe_location)
                .pipe(self.pipe_vaccine)
                .pipe(self.pipe_source)
        )

    def pipeline_by_age_group(self, ds: pd.DataFrame) -> pd.DataFrame:
        return (
            ds
                .pipe(self.pipe_rename_columns_by_age_group)
                .pipe(self.pipe_location_by_age_group)
        )

    def to_csv(self, paths):
        """Generalized."""
        data = self.read().pipe(self.pipeline)

        increment(
            paths=paths,
            location=data['location'],
            total_vaccinations=data['total_vaccinations'],
            people_vaccinated=data['people_vaccinated'],
            people_fully_vaccinated=data['people_fully_vaccinated'],
            date=data['date'],
            source_url=data['source_url'],
            vaccine=data['vaccine']
        )

        # Vaccination by age group
        df_by_age_group = self.read_by_age_group().pipe(self.pipeline_by_age_group)
        df_by_age_group = df_by_age_group.replace(to_replace="75+", value="75")
        df_by_age_group[["age_group_min", "age_group_max"]] = df_by_age_group.age_group.apply(
            lambda x: pd.Series(str(x).split("-")))
        df_by_age_group = df_by_age_group[["date", "age_group_min", "age_group_max", "total_vaccinations", "location"]]
        df_by_age_group.to_csv(paths.tmp_vax_out_by_age_group("United States"), index=False)


def get_vaccine_data(paths):
    vaccine_cols = ["Administered_Pfizer", "Administered_Moderna", "Administered_Janssen"]
    dfs = []
    for file in glob(os.path.join(paths.in_us_states, "cdc_data_*.csv")):
        try:
            df = pd.read_csv(file)
            for vc in vaccine_cols:
                if vc not in df.columns:
                    df[vc] = pd.NA
            df = df[["Date", "LongName"] + vaccine_cols]
            dfs.append(df)
        except Exception:
            continue
    df = pd.concat(dfs)
    df = df[df.LongName == "United States"].sort_values("Date").rename(columns={
        "Date": "date",
        "LongName": "location",
        "Administered_Pfizer": "Pfizer/BioNTech",
        "Administered_Moderna": "Moderna",
        "Administered_Janssen": "Johnson&Johnson",
    })
    df = df.melt(["date", "location"], var_name="vaccine", value_name="total_vaccinations")
    df = df.dropna(subset=["total_vaccinations"])
    df.to_csv(paths.tmp_vax_out_man("United States"), index=False)


def main(paths):
    UnitedStates(
        source_url="https://covid.cdc.gov/covid-data-tracker/COVIDData/getAjaxData?id=vaccination_data",
        location="United States",
    ).to_csv(paths)
    get_vaccine_data(paths)


if __name__ == "__main__":
    main()
