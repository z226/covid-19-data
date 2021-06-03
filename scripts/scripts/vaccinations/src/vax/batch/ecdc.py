import os
from datetime import datetime

import pandas as pd

from vax.utils.dates import clean_date


age_groups_known = {
    '1_Age60+',
    '1_Age<60',
    'ALL',
    'Age18_24',
    'Age25_49',
    'Age50_59',
    'Age60_69',
    'Age70_79',
    'Age80+',
    'AgeUNK',
    'HCW',
    'LTCF'
}


age_groups_relevant = {
    'Age18_24',
    'Age25_49',
    'Age50_59',
    'Age60_69',
    'Age70_79',
    'Age80+',
}


locations_age_exclude = [
    "Italy",
]

locations_manufacturer_exclude = [
    "",
]


class ECDC:

    def __init__(self, source_url: str, iso_path: str):
        self.source_url = source_url
        self.country_mapping = self._load_country_mapping(iso_path)
        self.vaccine_mapping = {
            "COM": "Pfizer/BioNTech",
            "MOD": "Moderna",
            "AZ": "Oxford/AstraZeneca",
            "JANSS": "Johnson&Johnson",
            "SPU": "Sputnik V",
            "BECNBG": "Sinopharm/Beijing"
        }

    def read(self):
        return pd.read_csv(self.source_url)

    def _load_country_mapping(self ,iso_path: str):
        country_mapping = pd.read_csv(iso_path)
        return dict(zip(country_mapping["alpha-2"], country_mapping["location"]))

    def _weekday_to_date(self, d):
        if datetime.now().weekday() >= 5:
            return clean_date(d + '+5', "%Y-W%W+%w")
            #print(r.strftime("%c"))
        else:
            return clean_date(d + '+2', "%Y-W%W+%w")
            #print(r.strftime("%c"))
    
    def pipe_base(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            total_vaccinations=df[["FirstDose", "SecondDose", "UnknownDose"]].sum(axis=1),
            date=df.YearWeekISO.apply(self._weekday_to_date),
            location=df.ReportingCountry.replace(self.country_mapping),
        )

    def pipe_group(self, df: pd.DataFrame, group_field: str, group_field_renamed: str) -> pd.DataFrame:
        return (
            df
            .loc[df.Region.isin(self.country_mapping.keys())]
            .groupby(["date", "location", group_field], as_index=False)
            ["total_vaccinations"]
            .sum()
            .rename(columns={group_field: group_field_renamed})
        )

    def pipe_enrich_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            total_vaccinations=df[["FirstDose", "SecondDose", "UnknownDose"]].sum(axis=1),
            date=df.YearWeekISO.apply(self._weekday_to_date),
            location=df.ReportingCountry.replace(self.country_mapping),
        )

    def pipe_rename_vaccines(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(vaccine=df.vaccine.replace(self.vaccine_mapping))

    def pipe_cumsum(self, df: pd.DataFrame, group_field_renamed: str) -> pd.DataFrame:
        return df.assign(
            total_vaccinations=df.groupby(["location", group_field_renamed])["total_vaccinations"].cumsum()
        )

    def pipe_age_checks(self, df: pd.DataFrame) -> pd.DataFrame:
        # Check all age groups are valid names
        age_groups_found = set(df.age_group)
        if age_groups_found.difference(age_groups_known):
            raise ValueError(f"Unknown age groups found. Check {age_groups_found}")
        # Get valid locations
        df = (
            df
            .pipe(self.pipe_age_filter_locations)
        )
        return df

    def pipe_age_filter_locations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter locations and keep only valid ones."""
        locations = df.location.unique()
        locations_valid = []
        for location in locations:
            df_c = df.loc[df.location==location]
            if not age_groups_relevant.difference(df_c.age_group.unique()):
                locations_valid.append(location)
        locations_valid = [loc for loc in locations_valid if loc not in locations_age_exclude]
        df = df[df.location.isin(locations_valid)]
        return df

    def pipe_age_filter_entries(self, df: pd.DataFrame) -> pd.DataFrame:
        """More granular filter. Keep entries where data is deemed reliable."""
        # Find valid dates + location
        x = df.pivot(index=["date", "location"], columns="age_group", values="total_vaccinations").reset_index()
        x = x.dropna(subset=age_groups_relevant, how="any")
        x = x.assign(debug=x[age_groups_relevant].sum(axis=1))
        x = x.assign(
            debug_diff=x.ALL - x.debug,
            debug_diff_perc=(x.ALL - x.debug)/x.ALL,
        )
        x = x[x.debug_diff_perc<=0.05]  # Keep only those days where missmatch between sum(ages) and total is <5%
        valid_entries_ids = x[["date", "location"]]
        if not valid_entries_ids.value_counts().max() == 1:
            raise ValueError("Some entries appear to be duplicated")
        df = df.merge(valid_entries_ids, on=["date", "location"])
        return df

    def pipe_age_groups(self, df: pd.DataFrame) -> pd.DataFrame:
        """Build age groups."""
        # df = df[~df.age_group.isin(['LTCF', 'HCW', 'AgeUNK', 'ALL'])]
        df_ = df[df.age_group.isin(age_groups_relevant)].copy()
        regex = r"(?:1_)?Age(\d{1,2})\+?(?:_(\d{1,2}))?"
        df_[["age_group_min", "age_group_max"]] = df_.age_group.str.extract(regex)
        # df.loc[df.age_group == "1_Age60+", ["age_group_min", "age_group_max"]] = [60, pd.NA]
        # df.loc[df.age_group == "1_Age<60", ["age_group_min", "age_group_max"]] = [0, 60]
        return df_

    def pipeline_common(self, df: pd.DataFrame, group_field: str, group_field_renamed: str) -> pd.DataFrame:
        return (
            df
            .pipe(self.pipe_base)
            .pipe(self.pipe_group, group_field, group_field_renamed)
            [["date", "location", group_field_renamed, "total_vaccinations"]]
            .sort_values("date")
            .pipe(self.pipe_cumsum, group_field_renamed)
        )

    def pipeline_manufacturer(self, df: pd.DataFrame):
        return (
            df
            .pipe(self.pipeline_common, "Vaccine", "vaccine")
            .pipe(self.pipe_rename_vaccines)
        )

    def pipeline_age(self, df: pd.DataFrame):
        group_field_renamed = "age_group"
        return (
            df
            .pipe(self.pipeline_common,  "TargetGroup", group_field_renamed)
            .pipe(self.pipe_age_checks)
            .pipe(self.pipe_age_filter_entries)
            .pipe(self.pipe_age_filter_locations)
            .pipe(self.pipe_age_groups)
            .drop(columns=[group_field_renamed])
        )

    def _export_country_data(self, df, locations: list, path_callable, columns: list):
        for location in locations:
            df_c = df[df.location==location]
            df_c.to_csv(
                path_callable(location),
                index=False,
                columns=columns,
            )

    def to_csv_age(self, paths, df: pd.DataFrame):
        df_age = df.pipe(self.pipeline_age)
        # df_age = self._check_data_age(df_age)
        return df_age
        # df_age = df_age.pipe(self.pipe_age_groups)
        # Export
        # locations = df_age.location.unique()
        # self._export_country_data(
        #     df=df,
        #     locations=locations,
        #     path_callable=paths.tmp_vax_out_by_age_group,
        #     columns=["location", "date", "age_group_min", "age_group_max", "total_vaccinations"]
        # )

    def to_csv_manufacturer(self, paths, df: pd.DataFrame):
        df_manufacuter = df.pipe(self.pipeline_manufacturer)
        # Export
        locations = df_manufacuter.location.unique()
        self._export_country_data(
            df=df,
            locations=locations,
            path_callable=paths.tmp_vax_out_man,
            columns=["location", "date", "vaccine", "total_vaccinations"]
        )

    def to_csv(self, paths):
        # Read data
        df = self.read()
        # Age
        self.to_csv_age(paths, df)
        # Manufacturer
        self.to_csv_manufacturer(paths, df)


def main(paths):
    ECDC(
        source_url="https://opendata.ecdc.europa.eu/covid19/vaccine_tracker/csv/data.csv",
        iso_path=os.path.join(paths.tmp_inp, "iso", "iso.csv")
    ).to_csv(paths)