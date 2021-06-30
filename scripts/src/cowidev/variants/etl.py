import os

import requests
import pandas as pd

from cowidev.utils.utils import get_project_dir
from cowidev.grapher.csv.core import Grapheriser


class VariantsETL:
    def __init__(self) -> None:
        self.source_url = (
            "https://raw.githubusercontent.com/hodcroftlab/covariants/master/web/data/perCountryData.json"
        )
        self.variants_mapping = {
            '20A.EU2': 'B.1.160',
            '20A/S:439K': 'B.1.258',
            '20A/S:98F': 'B.1.221',
            '20B/S:1122L': 'B.1.1.302',
            '20B/S:626S': 'B.1.1.277',
            '20C/S:80Y': 'B.1.367',
            '20E (EU1)': 'B.1.177',
            '20H (Beta, V2)': 'Beta',
            '20I (Alpha, V1)': 'Alpha',
            '20J (Gamma, V3)': 'Gamma',
            '21A (Delta)': 'Delta',
            '21B (Kappa)': 'Kappa',
            '21C (Epsilon)': 'Epsilon',
            '21D (Eta)': 'Eta',
            '21F (Iota)': 'Iota',
            'S:677H.Robin1': 'S:677H.Robin1',
            'S:677P.Pelican': 'S:677P.Pelican',
        }
        self.country_mapping = {
            "USA": "United States",
            "Czech Republic": "Czechia",
            "Sint Maarten": "Sint Maarten (Dutch part)",
        }
        self.column_rename = {
            "total_sequences": "num_sequences_total",
        }
        self.columns_out = [
            "location", "date", "variant", "num_sequences", "perc_sequences", "num_sequences_total"
        ]

    def extract(self) -> dict:
        data = requests.get(self.source_url).json()
        data = list(filter(lambda x: x["region"] == "World", data["regions"]))[0]["distributions"]
        return data

    def transform(self, data: dict) -> pd.DataFrame:
        df = (
            self.json_to_df(data)
            .pipe(self.pipe_edit_columns)
            .pipe(self.pipe_filter_locations)
            .pipe(self.pipe_variant_others)
            .pipe(self.pipe_percent)
            .pipe(self.pipe_out)
        )
        return df

    def load(self, df: pd.DataFrame) -> None:
        # Export data
        output_path = os.path.join(get_project_dir(), "public", "data", "variants", "covid-variants.csv")
        output_path_grapher = os.path.join(get_project_dir(), "scripts", "grapher", "COVID-19 - Variants.csv")
        df.to_csv(output_path, index=False)
        Grapheriser(
            pivot_column="variant",
            pivot_values="perc_sequences"
        ).run(output_path, output_path_grapher)

    def json_to_df(self, data: dict) -> pd.DataFrame:
        df = pd.json_normalize(
            data,
            record_path=['distribution'],
            meta=["country"]
        ).melt(
            id_vars=["country", "total_sequences", "week"],
            var_name="cluster",
            value_name="num_sequences"
        )
        return df

    def pipe_edit_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        # Modify/add columns
        df = df.assign(
            variant=df.cluster.str.replace('cluster_counts.', '', regex=True).replace(self.variants_mapping),
            date=df.week,
            location=df.country.replace(self.country_mapping),
        )
        df = df.rename(columns=self.column_rename)
        return df

    def pipe_filter_locations(self, df: pd.DataFrame) -> pd.DataFrame:
        # Filter locations
        populations_path = os.path.join(get_project_dir(), "scripts", "input", "un", "population_2020.csv")
        dfc = pd.read_csv(populations_path)
        df = df[df.location.isin(dfc.entity.unique())]
        return df

    def pipe_variant_others(self, df: pd.DataFrame) -> pd.DataFrame:
        df_a = df[["date", "location", "num_sequences_total"]].drop_duplicates()
        df_b = df.groupby(["date", "location"], as_index=False).agg({"num_sequences": sum}).rename(columns={"num_sequences": "all_seq"})
        df_c = df_a.merge(df_b, on=["date", "location"])
        df_c = df_c.assign(others=df_c["num_sequences_total"] - df_c["all_seq"])
        df_c = df_c.melt(id_vars=["location", "date", "num_sequences_total"], value_vars="num_sequences_others", var_name="variant", value_name="num_sequences")
        df = pd.concat([df, df_c])
        return df

    def pipe_percent(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            perc_sequences=(100 * df["num_sequences"] / df["num_sequences_total"]).round(2)
        )

    def pipe_out(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[self.columns_out].sort_values(["location", "date"])