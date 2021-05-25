import pandas as pd

from vax.utils.dates import clean_date_series


class Paraguay:

    def __init__(self, source_url: str, location: str, vaccine_mapping: dict):
        self.source_url = source_url
        self.location = location
        self.vaccine_mapping = vaccine_mapping

    def read(self) -> pd.DataFrame:
        return pd.read_csv(self.source_url, sep=";")

    def pipe_date(self, df: pd.DataFrame) -> pd.DataFrame:
        # Date format
        return df.assign(date=clean_date_series(df.fecha_aplicacion, "%Y-%m-%d %H:%M:%S")).sort_values("date")

    def _pipe_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        column_mapping = {1: "people_vaccinated", 2: "people_fully_vaccinated"}
        # Build data
        df1 = df.groupby(["date", "dosis"], as_index=False).agg(
            counts=("cedula", lambda x: x.nunique()),
        )
        df1 = (
            df1
            .pivot(index="date", columns="dosis", values="counts")
            .reset_index()
            .rename(columns=column_mapping)
        )
        # Cum sum
        df1.loc[:, column_mapping.values()] = df1.loc[:, column_mapping.values()].fillna(0).cumsum()
        return df1

    def _pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        # Complement with vaccine data
        return df.groupby(["date"], as_index=False).agg(
            vaccine=("descripcion_vacuna", lambda x: ", ".join(sorted(self.vaccine_mapping[xx] for xx in set(x))))
        )

    def pipe_process(self, df: pd.DataFrame) -> pd.DataFrame:
        df1 = self._pipe_metrics(df)
        df2 = self._pipe_vaccine(df)
        df = df1.merge(df2, on="date")
        return df

    def pipe_total_vaccinations(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(total_vaccinations=df.people_vaccinated+df.people_fully_vaccinated)
    
    def pipe_location(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location)

    def pipe_source(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(source_url=self.source_url)

    def pipe_select_output_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[[
            "date",
            "location",
            "vaccine",
            "source_url",
            "total_vaccinations",
            "people_vaccinated",
            "people_fully_vaccinated"
        ]]

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .pipe(self.pipe_process)
            .pipe(self.pipe_total_vaccinations)
            .pipe(self.pipe_location)
            .pipe(self.pipe_source)
            .pipe(self.pipe_select_output_columns)
        )

    def pipeline_manufacturer(self, df: pd.DataFrame) -> pd.DataFrame:
        # Map vaccine names
        df = df.assign(vaccine=df.descripcion_vacuna.replace(self.vaccine_mapping))
        # Count doses per day and brand
        df = df.groupby(["date", "vaccine"], as_index=False)[["dosis"]].count()
        # Fill dates with no data (pivot - melt)
        return (
            df
            .pivot(index="date", columns="vaccine", values="dosis")
            .fillna(0)
            .cumsum()
            .reset_index()
            .melt(
                id_vars=["date"], value_vars=self.vaccine_mapping.values(), var_name="vaccine",
                value_name="total_vaccinations"
            )
            .sort_values("date")
            .reset_index(drop=True)
            .assign(location=self.location)
            .astype({"total_vaccinations": int})
            [["date", "location", "vaccine", "total_vaccinations"]]
        )

    def to_csv(self, paths):
        df_base = self.read().pipe(self.pipe_date)
        # Export data
        df = df_base.copy().pipe(self.pipeline)
        df.to_csv(paths.tmp_vax_out(self.location), index=False)
        # Export manufacturer data
        df = df_base.copy().pipe(self.pipeline_manufacturer)
        df.to_csv(paths.tmp_vax_out_man(f"{self.location}"), index=False)


def main(paths):
    Paraguay(
        source_url="path/to/vacunados.csv",
        location="Paraguay",
        vaccine_mapping = {
            "SPUTNIK V COVID-19": "Sputnik V",
            "ASTRAZENECA COVID-19": "Oxford/AstraZeneca",
            "COVAXIN COVID-19": "Covaxin",
            "CORONAVAC COVID-19": "Sinovac",
            "SINOPHARM COVID-19": "Sinopharm/Beijing",
        },
    ).to_csv(paths)


if __name__ == "__main__":
    main()
