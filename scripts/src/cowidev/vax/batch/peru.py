import pandas as pd


class Peru:
    def __init__(self) -> None:
        self.location = "Peru"
        self.source_url = (
            "https://github.com/jmcastagnetto/covid-19-peru-vacunas/raw/main/datos/vacunas_covid_resumen.csv"
        )
        self.source_url_ref = (
            "https://www.datosabiertos.gob.pe/dataset/vacunaci%C3%B3n-contra-covid-19-ministerio-de-salud-minsa"
        )
        self.vaccine_mapping = {
            "SINOPHARM": "Sinopharm/Beijing",
            "PFIZER": "Pfizer/BioNTech",
            "ASTRAZENECA": "Oxford/AstraZeneca",
        }

    def read(self):
        return pd.read_csv(
            self.source_url,
            usecols=["fecha_vacunacion", "fabricante", "dosis", "n_reg"],
        )

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(columns={"fecha_vacunacion": "date", "fabricante": "vaccine"})
        return df.dropna(subset=["vaccine"])

    def pipe_checks(self, df: pd.DataFrame) -> pd.DataFrame:
        # Check vaccine names
        unknown_vaccines = set(df["vaccine"].unique()).difference(self.vaccine_mapping.keys())
        if unknown_vaccines:
            raise ValueError("Found unknown vaccines: {}".format(unknown_vaccines))
        # Check dose number
        dose_num_wrong = {1, 2}.difference(df.dosis.unique())
        if dose_num_wrong:
            raise ValueError(f"Invalid dose number. Check field `dosis`: {dose_num_wrong}")
        return df

    def pipe_rename_vaccines(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.replace(self.vaccine_mapping)

    def pipe_format(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.drop(columns="vaccine")
            .groupby(["date", "dosis"], as_index=False)
            .sum()
            .pivot(index="date", columns="dosis", values="n_reg")
            .rename(columns={1: "people_vaccinated", 2: "people_fully_vaccinated"})
            .fillna(0)
            .sort_values("date")
            .cumsum()
            .reset_index()
        )

    def pipe_total_vaccinations(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(total_vaccinations=df.people_vaccinated + df.people_fully_vaccinated)

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            location=self.location,
            vaccine=", ".join(sorted(self.vaccine_mapping.values())),
            source_url=self.source_url_ref,
        )

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_rename_columns)
            .pipe(self.pipe_checks)
            .pipe(self.pipe_rename_vaccines)
            .pipe(self.pipe_format)
            .pipe(self.pipe_total_vaccinations)
            .pipe(self.pipe_metadata)
        )

    def export(self, paths):
        df = self.read().pipe(self.pipeline)
        df.to_csv(paths.tmp_vax_out(self.location), index=False)


def main(paths):
    Peru().export(paths)
