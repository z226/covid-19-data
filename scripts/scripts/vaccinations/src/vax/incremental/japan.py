from datetime import datetime

import pandas as pd

from vax.utils.incremental import enrich_data, increment
from vax.utils.utils import clean_column_name
from vax.utils.dates import clean_date


vaccine_mapping = {
    "ファイザー社": "Pfizer/BioNTech",
    "武田/モデルナ社": "Moderna",
}


class Japan:

    def __init__(self, source_url_health: str, source_url_general: str, source_url_ref: str, location: str):
        self.source_url_health = source_url_health
        self.source_url_general = source_url_general
        self.source_url_ref = source_url_ref
        self.location = location

    def read(self):
        ds_health = self._parse_data(self.source_url_health, skiprows=3)
        ds_general = self._parse_data(self.source_url_general, skiprows=4)
        return self._merge_series(ds_health, ds_general)

    def _parse_vaccines(self, df):
        vaccines = []
        processed = []

        for col in df.columns:
            if "Unnamed" in col:
                processed.append(col)
                pass
            else:
                for japanese_name, english_name in vaccine_mapping.items():
                    if japanese_name in col:
                        vaccines.append(english_name)
                        processed.append(col)

        assert len(processed) == len(df.columns), f"New vaccine found! Update `vaccine_mapping`"
        return ", ".join(list(set(vaccines)))

    def _parse_data(self, source: str, skiprows: int) -> pd.Series:

        df = pd.read_excel(source, usecols="A:G", skiprows=skiprows, nrows=2)

        # Sanity checks
        assert [*df.columns] == [
            'Unnamed: 0', 'Unnamed: 1', 'Unnamed: 2', 'ファイザー社', '武田/モデルナ社',
            'ファイザー社.1', '武田/モデルナ社.1'
        ], "Columns are not as expected. Unknown field detected."

        # Parse metrics
        total_vaccinations = df.iloc[0, 2]
        people_vaccinated = df.iloc[0, 3] + df.iloc[0, 4]
        people_fully_vaccinated = df.iloc[0, 5] + df.iloc[0, 6]

        # Parse date
        date = str(df.iloc[1, 0].date())

        # Parse vaccines
        vaccines = self._parse_vaccines(df)

        return pd.Series(data={
            "total_vaccinations": total_vaccinations,
            "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_fully_vaccinated,
            "date": date,
            "vaccine": vaccines,
        })

    def _merge_series(self, ds_health: pd.Series, ds_general: pd.Series) -> pd.Series:
        vaccine = ", ".join(sorted(list(set(ds_health.vaccine.split(", ") + ds_general.vaccine.split(", ")))))
        return pd.Series(data={
            "total_vaccinations": ds_health.total_vaccinations + ds_general.total_vaccinations,
            "people_vaccinated": ds_health.people_vaccinated + ds_general.people_vaccinated,
            "people_fully_vaccinated": ds_health.people_fully_vaccinated + ds_general.people_fully_vaccinated,
            "date": max([ds_health.date, ds_general.date]),
            "vaccine": vaccine,
        })

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(
            ds, "source_url", self.source_url_ref
        )

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return (
            ds
            .pipe(self.pipe_location)
            .pipe(self.pipe_source)
        )

    def to_csv(self, paths):
        data = self.read().pipe(self.pipeline)
        increment(
            paths=paths,
            location=data["location"],
            total_vaccinations=data["total_vaccinations"],
            people_vaccinated=data["people_vaccinated"],
            people_fully_vaccinated=data["people_fully_vaccinated"],
            date=data["date"],
            source_url=data["source_url"],
            vaccine=data["vaccine"]
        )


def main(paths):
    Japan(
        source_url_health="https://www.kantei.go.jp/jp/content/IRYO-vaccination_data3.xlsx",
        source_url_general="https://www.kantei.go.jp/jp/content/KOREI-vaccination_data3.xlsx",
        source_url_ref="https://www.kantei.go.jp/jp/headline/kansensho/vaccine.html",
        location="Japan",
    ).to_csv(paths)


if __name__ == "__main__":
    main()
