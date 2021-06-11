from datetime import datetime

import pandas as pd

from cowidev.vax.utils.incremental import enrich_data, increment
from cowidev.vax.utils.utils import clean_string
from cowidev.vax.utils.dates import clean_date


vaccine_mapping = {
    "ファイザー社": "Pfizer/BioNTech",
    "武田/モデルナ社": "Moderna",
}


class Japan:

    def __init__(self, source_url_health: str, source_url_old: str,source_url_ref: str, location: str):
        self.source_url_health = source_url_health
        self.source_url_old = source_url_old
        self.source_url_ref = source_url_ref
        self.location = location

    def read(self):
        ds_health = self._parse_data(self.source_url_health)
        ds_old = self._parse_data(self.source_url_old)
        return self._merge_series(ds_health, ds_old)

    def _parse_colnames(self, df: pd.DataFrame) -> pd.DataFrame:
        cols_new = []
        for col in df.columns:
            col_new = (clean_string(col[0]), clean_string(col[1]))
            cols_new.append(col_new)
        df.columns = pd.MultiIndex.from_tuples(cols_new)
        return df

    def _get_data_raw(self, source: str) -> pd.DataFrame:
        df = pd.read_excel(source, header=[2, 3]).dropna(axis=1, how="all")
        df = self._parse_colnames(df)
        df = df.set_index("集計日")
        # Sanity checks
        cond1 = df.columns.levels[0].difference(['内1回目', '内2回目', '接種回数', '曜日', '集計日'])
        cond2 = df.columns.levels[1].difference(['', 'ファイザー社', '武田/モデルナ社'])
        if not (cond1.empty and cond2.empty):
            raise ValueError("Columns are not as expected. Unknown field detected, maybe due to new vaccine added!")
        return df

    def _parse_vaccines(self, df):
        x = df.loc["合計", "内1回目"]
        vax1 = x[x != 0].index.tolist()
        x = df.loc["合計", "内2回目"]
        vax2 = x[x != 0].index.tolist()
        vaccines = set(vax1+vax2)
        vaccines_wrong = vaccines.difference(vaccine_mapping)
        if vaccines_wrong:
            raise ValueError(f"New vaccine:  {vaccines_wrong}. Update `vaccine_mapping`.")
        return ", ".join([vaccine_mapping[v] for v in vaccines])

    def _parse_data(self, source: str) -> pd.Series:
        df = self._get_data_raw(source)
        # Parse metrics
        total_vaccinations = df.loc["合計", "接種回数"].item()
        people_vaccinated = df.loc["合計", "内1回目"].sum()
        people_fully_vaccinated = df.loc["合計", "内2回目"].sum()
        # Parse date
        date = clean_date(max(dt for dt in df.index.values if isinstance(dt, datetime)))
        # Parse vaccines
        vaccines = self._parse_vaccines(df)
        return pd.Series(data={
            "total_vaccinations": total_vaccinations,
            "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_fully_vaccinated,
            "date": date,
            "vaccine": vaccines
        })

    def _merge_series(self, ds_health: pd.Series, ds_old: pd.Series) -> pd.Series:
        vaccine = ", ".join(set(ds_health.vaccine.split(", ") + ds_old.vaccine.split(", ")))
        return pd.Series(data={
            "total_vaccinations": ds_health.total_vaccinations + ds_old.total_vaccinations,
            "people_vaccinated": ds_health.people_vaccinated + ds_old.people_vaccinated,
            "people_fully_vaccinated": ds_health.people_fully_vaccinated + ds_old.people_fully_vaccinated,
            "date": max([ds_health.date, ds_old.date]),
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
        source_url_health="https://www.kantei.go.jp/jp/content/IRYO-vaccination_data2.xlsx",
        source_url_old="https://www.kantei.go.jp/jp/content/KOREI-vaccination_data2.xlsx",
        source_url_ref="https://www.kantei.go.jp/jp/headline/kansensho/vaccine.html",
        location="Japan",
    ).to_csv(paths)


if __name__ == "__main__":
    main()
