from urllib.parse import urlparse


import pandas as pd
from bs4 import BeautifulSoup

import cowidev.vax.utils.utils as utils
from cowidev.vax.utils.dates import clean_date_series


class NewZealand:
    def __init__(self):
        """Constructor."""
        self.source_url = (
            "https://www.health.govt.nz/our-work/diseases-and-conditions/covid-19-novel-coronavirus/"
            "covid-19-data-and-statistics/covid-19-vaccine-data"
        )
        self.location = "New Zealand"
        self.columns_rename = {
            "First dose administered": "people_vaccinated",
            "Second dose administered": "people_fully_vaccinated",
            "Date": "date",
        }
        self.columns_by_age_group_rename={
            "# doses administered": "total_vaccinations",
            "Ten year age group": "age_group",
        }
        self.columns_cumsum=["First dose administered", "Second dose administered"]
        self.columns_cumsum_by_age=["Ten year age group"]

    def read(self) -> pd.DataFrame:
        """Load data."""
        soup = utils.get_soup(self.source_url)
        link = self._parse_file_link(soup)
        df = utils.read_xlsx_from_url(link, sheet_name="Date")
        print(link)
        # df_by_age = utils.read_xlsx_from_url(link, sheet_name="Ethnicity Age Gender by dose")
        return df  # , df_by_age

    def _parse_file_link(self, soup: BeautifulSoup) -> str:
        href = soup.find(id="download").find_next("a")["href"]
        link = f"https://{urlparse(self.source_url).netloc}/{href}"
        return link

    def pipe_rename(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generalized."""
        if self.columns_rename:
            return df.rename(columns=self.columns_rename)
        return df

    def pipe_cumsum(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generalized."""
        if self.columns_cumsum:
            df[self.columns_cumsum] = df[self.columns_cumsum].cumsum()
        return df

    def pipe_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Could be generalized."""
        return df.assign(
            total_vaccinations=df.people_vaccinated + df.people_fully_vaccinated
        )

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        """Could be generalized."""
        return df.assign(vaccine="Pfizer/BioNTech")

    def pipe_location(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generalized."""
        return df.assign(location=self.location)

    def pipe_source_url(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generalized."""
        return df.assign(source_url=self.source_url)

    def pipe_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generalized."""
        return df.assign(date=clean_date_series(df.date))

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        """Could be generalized."""
        return (
            df
            .pipe(self.pipe_cumsum)
            .pipe(self.pipe_rename)
            .pipe(self.pipe_metrics)
            .pipe(self.pipe_date)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_location)
            .pipe(self.pipe_source_url)
        )

    def pipe_rename_by_age(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generalized."""
        if self.columns_by_age_group_rename:
            return df.rename(columns=self.columns_by_age_group_rename)
        return df

    def pipe_aggregate_by_age(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generalized."""
        df = (
            df.groupby("Ten year age group")["# doses administered"].sum().reset_index()
        )
        return df

    def pipe_postprocess(self, df: pd.DataFrame, date_str: str) -> pd.DataFrame:
        df[["age_group_min", "age_group_max"]] = df.age_group.str.split(
            r" to |\+\/Unknown", expand=True
        )
        df["date"] = date_str
        df = df[
            ["date", "age_group_min", "age_group_max", "total_vaccinations", "location"]
        ]
        return df

    def pipeline_by_age(self, df: pd.DataFrame, date_str: str) -> pd.DataFrame:
        """Could be generalized."""
        return (
            df
            .pipe(self.pipe_aggregate_by_age)
            .pipe(self.pipe_rename_by_age)
            .pipe(self.pipe_location)
            .pipe(self.pipe_postprocess, date_str)
        )

    def export(self, paths):
        """Generalized."""
        df = self.read()
        # Main data
        df = df.pipe(self.pipeline)
        df.to_csv(paths.tmp_vax_out(self.location), index=False)
        # Age data (metrics need to be relative to age group sizes)
        # df_by_age = df_by_age.pipe(self.pipeline_by_age, df['date'].max())
        # df_by_age = merge_with_current_data(df_by_age, paths.tmp_vax_out_by_age_group(self.location))
        # df_by_age.to_csv(paths.tmp_vax_out_by_age_group(self.location), index=False)


def main(paths):
    NewZealand().export(paths)


if __name__ == "__main__":
    main()
