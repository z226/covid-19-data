from datetime import datetime

import pandas as pd

from vax.utils.utils import get_soup, clean_df_columns_multiindex
from vax.utils.dates import clean_date_series


class Japan:
    def __init__(self):
        self.source_url_1 = "https://www.mhlw.go.jp/stf/seisakunitsuite/bunya/vaccine_sesshujisseki.html"
        self.source_url_2_health = "https://www.kantei.go.jp/jp/content/IRYO-vaccination_data3.xlsx"
        self.source_url_2_general = "https://www.kantei.go.jp/jp/content/KOREI-vaccination_data3.xlsx"
        self.source_url_2_ref = "https://www.kantei.go.jp/jp/headline/kansensho/vaccine.html"
        self.location = "Japan"
        self.vaccine_mapping = {
            "ファイザー社": "Pfizer/BioNTech",
            "武田/モデルナ社": "Moderna",
        }

    def read(self):
        df_1 = self.read_1().pipe(self.pipeline_1)
        df_2 = self.read_2().pipe(self.pipeline_2)
        return (
            pd.concat([
                df_1,
                df_2
            ])
            .reset_index(drop=True)
        )

    def read_1(self):
        soup = get_soup(self.source_url_1)
        dfs = pd.read_html(str(soup), header=0)
        if len(dfs) != 1:
            raise ValueError(f"Only one table should be present. {len(dfs)} tables detected.")
        df = dfs[0]
        return df

    def pipe_1_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns={
            "日付": "date",
            "接種回数": "total_vaccinations",
            "内１回目": "people_vaccinated",
            "内２回目": "people_fully_vaccinated",
        })

    def pipe_1_filter_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[df.date != "合計"]

    def pipe_1_source(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(source_url=self.source_url_1)

    def pipe_1_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(vaccine="Pfizer/BioNTech")

    def pipe_1_date(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(date=clean_date_series(df.date, "%Y/%m/%d"))

    def pipeline_1(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .pipe(self.pipe_1_rename_columns)
            .pipe(self.pipe_1_filter_dates)
            .pipe(self.pipe_1_source)
            .pipe(self.pipe_1_vaccine)
            .pipe(self.pipe_1_date)
        )

    def read_2(self):
        df_health = self._parse_data_2(
            source=self.source_url_2_health,
            header=[2, 3],
            date_col="集計日"
        )
        df_general = self._parse_data_2(
            source=self.source_url_2_general,
            header=[2, 3, 4],
            date_col="接種日",
            extra_levels=["すべて"]
        )
        return (
            pd.concat([
                df_health,
                df_general,
            ])
            .reset_index(drop=True)
        )

    def _parse_data_2(self, source: str, header: list, date_col: str, extra_levels: list = None) -> pd.DataFrame:
        # Read general
        df = pd.read_excel(source, header=header)
        df = df.dropna(axis=1, how='all')
        # Clean column names
        df = clean_df_columns_multiindex(df)
        # Filter date rows
        df = df[df[date_col].apply(isinstance, args=(datetime,))].set_index(date_col).sort_index()
        # Build DataFrame
        if extra_levels is None:
            extra_levels = []
        doses_1 = df.loc[:, tuple(extra_levels + ["内1回目"])]
        doses_2 = df.loc[:, tuple(extra_levels + ["内2回目"])]
        return (
            pd.DataFrame({
                "people_vaccinated": doses_1.sum(axis=1),
                "people_fully_vaccinated": doses_2.sum(axis=1),
                "vaccine": self._parse_data_2_vaccines(doses_1, doses_2, date_col),
            })
            .reset_index()
            .rename(columns={date_col: "date"})
        )

    def _parse_data_2_vaccines(self, doses_1, doses_2, date_col) -> pd.DataFrame:
        if not all(doses_1.columns == doses_2.columns):
            raise ValueError("Missmatch in vaccines for dose 1 and dose 2")
        x = doses_1.cumsum()
        vaccines_wrong = set(x.columns).difference(self.vaccine_mapping)
        if vaccines_wrong:
            raise ValueError(f"Unknown vaccine(s): {vaccines_wrong}")
        vaccines = x.mask(x==0).stack().reset_index().groupby(date_col).level_1.unique()
        return vaccines

    def pipe_2_aggregate(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .groupby("date", as_index=False)
            .agg({
                "people_vaccinated": "sum",
                "people_fully_vaccinated": "sum",
                "vaccine": lambda x: ", ".join(sorted(set(self.vaccine_mapping[xxx] for xx in list(x) for xxx in xx)))
            })
        )

    def pipe_2_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(total_vaccinations=df.people_vaccinated+df.people_fully_vaccinated)

    def pipe_2_source(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(source_url=self.source_url_2_ref)

    def pipe_2_date(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(date=clean_date_series(df.date))

    def pipeline_2(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .pipe(self.pipe_2_aggregate)
            .pipe(self.pipe_2_metrics)
            .pipe(self.pipe_2_source)
            .pipe(self.pipe_2_date)
        )

    def pipe_location(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location)

    def pipe_cumsum(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values("date")
        column_metrics = ["total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]
        df.loc[:, column_metrics] = df[column_metrics].cumsum()
        return df

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .pipe(self.pipe_location)
            .pipe(self.pipe_cumsum)
            [[
                "location", "date", "vaccine", "source_url", "total_vaccinations", "people_vaccinated",
                "people_fully_vaccinated",
            ]])

    def export(self, paths):
        df = self.read().pipe(self.pipeline)
        df.to_csv(paths.tmp_vax_out(self.location), index=False, float_format='%.f')


def main(paths):
    Japan().export(paths)

