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
    "Czechia",
    "France",
    "Germany",
    "Italy",
    "Latvia",
    "Lithuania",
    "Romania",
    "Iceland",
    "Switzerland",
]


class ECDC:

    def __init__(self, iso_path: str):
        self.source_url = "https://opendata.ecdc.europa.eu/covid19/vaccine_tracker/csv/data.csv"
        self.country_mapping = self._load_country_mapping(iso_path)
        self.vaccine_mapping = {
            "COM": "Pfizer/BioNTech",
            "MOD": "Moderna",
            "AZ": "Oxford/AstraZeneca",
            "JANSS": "Johnson&Johnson",
            "SPU": "Sputnik V",
            "BECNBG": "Sinopharm/Beijing",
            "UNK": "Unknown",
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

    def pipe_cumsum(self, df: pd.DataFrame, group_field_renamed: str) -> pd.DataFrame:
        return df.assign(
            total_vaccinations=df.groupby(["location", group_field_renamed])["total_vaccinations"].cumsum()
        )

    def pipeline_common(self, df: pd.DataFrame, group_field: str, group_field_renamed: str) -> pd.DataFrame:
        return (
            df
            .pipe(self.pipe_base)
            .pipe(self.pipe_group, group_field, group_field_renamed)
            [["date", "location", group_field_renamed, "total_vaccinations"]]
            .sort_values("date")
            .pipe(self.pipe_cumsum, group_field_renamed)
        )

    def pipe_rename_vaccines(self, df: pd.DataFrame) -> pd.DataFrame:
        vaccines_wrong = set(df.vaccine).difference(self.vaccine_mapping)
        if vaccines_wrong:
            raise ValueError(f"Unknown vaccines found. Check {vaccines_wrong}")
        return df.assign(vaccine=df.vaccine.replace(self.vaccine_mapping))

    def pipe_manufacturer_filter_locations(self, df: pd.DataFrame):
        """Filters countries to be excluded and those with a high number of unknown doses."""
        def _get_perc_unk(x):
            res = x.groupby("vaccine").total_vaccinations.sum()
            res /= res.sum()
            if not "Unknown" in res:
                return 0
            return res.loc["Unknown"]
        threshold_unk_ratio = 0.05
        mask = (df.groupby("location").apply(_get_perc_unk) < threshold_unk_ratio)
        locations_valid = mask[mask].index.tolist()
        locations_valid = [loc for loc in locations_valid if loc not in locations_manufacturer_exclude]
        df = df[df.location.isin(locations_valid)]
        return df

    def pipe_manufacturer_filter_entries(self, df: pd.DataFrame):
        return df[~df.vaccine.isin(["Unknown"])]

    def pipeline_manufacturer(self, df: pd.DataFrame):
        group_field_renamed = "vaccine"
        return (
            df
            .loc[df.TargetGroup == "ALL"]
            .pipe(self.pipeline_common, "Vaccine", group_field_renamed)
            .pipe(self.pipe_rename_vaccines)
            .pipe(self.pipe_manufacturer_filter_locations)
            .pipe(self.pipe_manufacturer_filter_entries)
            [["location", "date", "vaccine", "total_vaccinations"]]
            .sort_values(["location", "date", "vaccine"])
        )

    def pipe_age_checks(self, df: pd.DataFrame) -> pd.DataFrame:
        # Check all age groups are valid names
        ages_groups_wrong = set(df.age_group).difference(age_groups_known)
        if ages_groups_wrong:
            raise ValueError(f"Unknown age groups found. Check {ages_groups_wrong}")
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
        threshold_missmatch_ratio = 0.05 # Keep only those days where missmatch between sum(ages) and total is <5%
        x = x[x.debug_diff_perc<=threshold_missmatch_ratio]  
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

    def pipeline_age(self, df: pd.DataFrame):
        group_field_renamed = "age_group"
        return (
            df
            .pipe(self.pipeline_common,  "TargetGroup", group_field_renamed)
            .pipe(self.pipe_age_checks)
            .pipe(self.pipe_age_filter_locations)
            .pipe(self.pipe_age_filter_entries)
            .pipe(self.pipe_age_groups)
            .drop(columns=[group_field_renamed])
            [["location", "date", "age_group_min", "age_group_max", "total_vaccinations"]]
            .sort_values(["location", "date", "age_group_min"])
        )

    def _export_country_data(self, df, path_generator_fct, columns: list):
        locations = df.location.unique()
        for location in locations:
            df_c = df[df.location==location]
            df_c.to_csv(
                path_generator_fct(location),
                index=False,
                columns=columns,
            )

    def to_csv_age(self, paths, df: pd.DataFrame):
        df_age = df.pipe(self.pipeline_age)
        # Export
        self._export_country_data(
            df=df_age,
            path_generator_fct=paths.tmp_vax_out_by_age_group,
            columns=["location", "date", "age_group_min", "age_group_max", "total_vaccinations"]
        )

    def to_csv_manufacturer(self, paths, df: pd.DataFrame):
        df_manufacuter = df.pipe(self.pipeline_manufacturer)
        # Export
        self._export_country_data(
            df=df_manufacuter,
            path_generator_fct=paths.tmp_vax_out_man,
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
        iso_path=os.path.join(paths.tmp_inp, "iso", "iso.csv")
    ).to_csv(paths)
