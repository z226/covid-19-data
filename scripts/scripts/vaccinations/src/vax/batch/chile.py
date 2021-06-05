import datetime

import pandas as pd


vaccine_mapping = {
    "Pfizer": "Pfizer/BioNTech",
    "Sinovac": "Sinovac",
    "Astra-Zeneca": "Oxford/AstraZeneca",
    "CanSino": "CanSino",
}
vaccines_one_dose = ["CanSino"]


class Chile:

    def __init__(self):
        self.location = "Chile"
        self.source_url = "https://github.com/juancri/covid19-vaccination/raw/master/output/chile-vaccination-type.csv"
        self.source_url_ref = "https://www.gob.cl/yomevacuno/"

    def read(self) -> pd.DataFrame:
        return pd.read_csv(self.source_url)

    def pipe_melt(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.melt(["Type", "Dose"], var_name="date", value_name="value")

    def pipe_filter_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[(df.Type != "Total") & (df.value > 0)]

    def pipe_pivot(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pivot(index=["Type", "date"], columns="Dose", values="value").reset_index()

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .rename(columns={
                "First": "people_vaccinated",
                "Second": "people_fully_vaccinated",
                "Type": "vaccine",
            })
        )

    def pipe_rename_vaccines(self, df: pd.DataFrame) -> pd.DataFrame:
        vaccines_wrong = set(df["vaccine"].unique()).difference(vaccine_mapping)
        if vaccines_wrong:
            raise ValueError(f"Missing vaccines: {vaccines_wrong}")
        return df.replace(vaccine_mapping)

    def pipe_total_vaccinations(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.loc[df.vaccine.isin(vaccines_one_dose), "people_vaccinated"].sum() != 0:
            raise ValueError(
                f"Reporting of one dose vaccines changed! Check column `people_vaccinated` for these."
            )
        return (
            df
            .assign(
                total_vaccinations=df.people_vaccinated.fillna(0) + df.people_fully_vaccinated.fillna(0)
            )
        )

    def pipe_process_onedose_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        for vaccine in vaccines_one_dose:
            mask = df.vaccine == vaccine
            df.loc[mask, "people_vaccinated"] = df.loc[mask, "people_fully_vaccinated"]
        return df

    def pipeline_base(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .pipe(self.pipe_melt)
            .pipe(self.pipe_filter_rows)
            .pipe(self.pipe_pivot)
            .pipe(self.pipe_rename_columns)
            .pipe(self.pipe_rename_vaccines)
            .pipe(self.pipe_total_vaccinations)
            .pipe(self.pipe_process_onedose_metrics)
        )

    def pipe_aggregate(self, df: pd.DataFrame) -> pd.DataFrame:
        
        return (
            df
            .sort_values("vaccine")
            .groupby("date", as_index=False)
            .agg(
                people_vaccinated=("people_vaccinated", "sum"),
                people_fully_vaccinated=("people_fully_vaccinated", "sum"),
                total_vaccinations=("total_vaccinations", "sum"),
                vaccine=("vaccine", ", ".join),
            )
            .assign(location=self.location)
        )

    def pipe_source(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add columns."""
        return df.assign(
            source_url=self.source_url_ref,
        )

    def pipeline_vaccinations(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .pipe(self.pipe_aggregate)
            .pipe(self.pipe_source)
        )

    def pipeline_manufacturer(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.
            assign(location=self.location)
            [["location", "vaccine", "date", "total_vaccinations"]]
        )

    def to_csv(self, paths):
        data = self.read().pipe(self.pipeline_base)
        # condition = (datetime.datetime.now() - pd.to_datetime(data.date.max())).days < 3
        # assert condition, "Data in external repository has not been updated for some days now"

        data.pipe(self.pipeline_vaccinations).to_csv(
            paths.tmp_vax_out(self.location),
            index=False
        )
        data.pipe(self.pipeline_manufacturer).to_csv(
            paths.tmp_vax_out_man(self.location),
            index=False
        )


def main(paths):
    Chile().to_csv(paths)


if __name__ == "__main__":
    main()
