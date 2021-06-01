from urllib.parse import urlparse


import pandas as pd
from bs4 import BeautifulSoup

import vax.utils.utils as utils


class NewZealand:

    def __init__(self, source_url: str, location: str, columns_rename: dict = None, columns_by_age_group_rename: dict = None, columns_cumsum: list = None,
                 columns_cumsum_by_age: list = None):
        """Constructor.

        Args:
            source_url (str): Source data url
            location (str): Location name
            columns_rename (dict, optional): Maps original to new names. Defaults to None.
            columns_by_age_group_rename (dict, optional): Maps original to new age by group names.
            columns_cumsum (list, optional): List of columns to apply cumsum to. Comes handy when the values reported
                                                are daily. Defaults to None.
            columns_cumsum_by_age (list, optional): Group by age column to apply cumsum.
        """
        self.source_url = source_url
        self.location = location
        self.columns_rename = columns_rename
        self.columns_by_age_group_rename = columns_by_age_group_rename
        self.columns_cumsum = columns_cumsum
        self.columns_cumsum_by_age = columns_cumsum_by_age

    def load_data(self) -> pd.DataFrame:
        """Load original data."""
        soup = utils.get_soup(self.source_url)
        link = self._parse_file_link(soup)
        return utils.read_xlsx_from_url(link, sheet_name="Date")

    def load_data_by_age(self) -> pd.DataFrame:
        """Load original data."""
        soup = utils.get_soup(self.source_url)
        link = self._parse_file_link(soup)
        return utils.read_xlsx_from_url(link, sheet_name="Ethnicity, Age, Gender by dose")

    def _parse_file_link(self, soup: BeautifulSoup) -> str:
        href = soup.find(id="download").find_next("a")["href"]
        link = f"https://{urlparse(self.source_url).netloc}/{href}"
        return link

    def rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generalized."""
        if self.columns_rename:
            return df.rename(columns=self.columns_rename)
        return df

    def rename_by_age_group_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generalized."""
        if self.columns_by_age_group_rename:
            return df.rename(columns=self.columns_by_age_group_rename)
        return df

    def cumsum_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generalized."""
        if self.columns_cumsum:
            df[self.columns_cumsum] = df[self.columns_cumsum].cumsum()
        return df

    def cumsum_by_age(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generalized."""
        df = df.groupby('Ten year age group')['# doses administered'].sum().reset_index()
        return df

    def add_totals(self, df: pd.DataFrame) -> pd.DataFrame:
        """Could be generalized."""
        return df.assign(total_vaccinations=df.people_vaccinated + df.people_fully_vaccinated)

    def enrich_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        """Could be generalized."""
        return df.assign(vaccine="Pfizer/BioNTech")

    def enrich_location(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generalized."""
        return df.assign(location=self.location)

    def enrich_source_url(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generalized."""
        return df.assign(source_url=self.source_url)

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        """Could be generalized."""
        return (
            df
                .pipe(self.cumsum_columns)
                .pipe(self.rename_columns)
                .pipe(self.add_totals)
                .pipe(self.enrich_vaccine)
                .pipe(self.enrich_location)
                .pipe(self.enrich_source_url)
        )

    def pipeline_by_age(self, df: pd.DataFrame) -> pd.DataFrame:
        """Could be generalized."""
        return (
            df.pipe(self.cumsum_by_age)
                .pipe(self.rename_by_age_group_columns)
                .pipe(self.enrich_location)

        )

    def to_csv(self, paths):
        """Generalized."""
        df = self.load_data().pipe(self.pipeline)
        df.to_csv(paths.tmp_vax_out(self.location), index=False)
        df_by_age = self.load_data_by_age().pipe(self.pipeline_by_age)
        df_by_age = df_by_age.replace(to_replace=' to ', value='-', regex=True)
        df_by_age[["age_group_min", "age_group_max"]] = df_by_age.age_group.apply(
            lambda x: pd.Series(str(x).split("-")))
        df_by_age["age_group_min"] = df_by_age["age_group_min"].str.replace("+/Unknown", "", regex=False)
        df_by_age['date'] = df['date'].tail(1).to_string().split()[1]
        df_by_age = df_by_age[["date", "age_group_min", "age_group_max", "total_vaccinations", "location"]]
        df_by_age.to_csv(paths.tmp_vax_out_by_age_group(self.location), index=False)


def main(paths):
    NewZealand(
        source_url=(
            "https://www.health.govt.nz/our-work/diseases-and-conditions/covid-19-novel-coronavirus/"
            "covid-19-data-and-statistics/covid-19-vaccine-data"
        ),
        location="New Zealand",
        columns_rename={
            "First dose administered": "people_vaccinated",
            "Second dose administered": "people_fully_vaccinated",
            "Date": "date",
        },
        columns_cumsum=["First dose administered", "Second dose administered"],
        columns_cumsum_by_age=["Ten year age group"],
        columns_by_age_group_rename={
            "# doses administered": "total_vaccinations",
            "Ten year age group": "age_group",
        }
    ).to_csv(paths)


if __name__ == "__main__":
    main
