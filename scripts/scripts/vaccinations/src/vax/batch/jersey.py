import requests
import tempfile

import pandas as pd


class Jersey:

    def __init__(self, source_url: str, location: str, columns_rename: dict = None):
        """Constructor.

        Args:
            source_url (str): Source data url
            location (str): Location name
            columns_rename (dict, optional): Maps original to new names. Defaults to None.
        """
        self.source_url = source_url
        self.location = location
        self.columns_rename = columns_rename

    def read(self) -> pd.DataFrame:
        tf = tempfile.NamedTemporaryFile()

        with open(tf.name, mode="wb") as f:
            f.write(requests.get(self.source_url).content)
        return pd.read_csv(tf.name, usecols=[
            "Date",
            "VaccinationsTotalNumberDoses",
            "VaccinationsTotalNumberFirstDoseVaccinations",
            "VaccinationsTotalNumberSecondDoseVaccinations",
        ])

    def read_by_age_group(self) -> pd.DataFrame:
        tf = tempfile.NamedTemporaryFile()

        with open(tf.name, mode="wb") as f:
            f.write(requests.get(self.source_url).content)
        return pd.read_csv(tf.name, usecols=[
            "Date",
            "VaccinationsTotalNumberDoses",
            "VaccinationsTotalVaccinationDosesFirstDose80yearsandover",
            "VaccinationsTotalVaccinationDosesFirstDose75to79years",
            "VaccinationsTotalVaccinationDosesFirstDose70to74years",
            "VaccinationsTotalVaccinationDosesFirstDose65to69years",
            "VaccinationsTotalVaccinationDosesFirstDose60to64years",
            "VaccinationsTotalVaccinationDosesFirstDose55to59years",
            "VaccinationsTotalVaccinationDosesFirstDose50to54years",
            "VaccinationsTotalVaccinationDosesFirstDose40to49years",
            "VaccinationsTotalVaccinationDosesFirstDose30to39years",
            "VaccinationsTotalVaccinationDosesFirstDose18to29years",
            "VaccinationsTotalVaccinationDosesFirstDose17yearsandunder",
            "VaccinationsTotalVaccinationDosesSecondDose80yearsandover",
            "VaccinationsTotalVaccinationDosesSecondDose75to79years",
            "VaccinationsTotalVaccinationDosesSecondDose70to74years",
            "VaccinationsTotalVaccinationDosesSecondDose65to69years",
            "VaccinationsTotalVaccinationDosesSecondDose60to64years",
            "VaccinationsTotalVaccinationDosesSecondDose55to59years",
            "VaccinationsTotalVaccinationDosesSecondDose50to54years",
            "VaccinationsTotalVaccinationDosesSecondDose40to49years",
            "VaccinationsTotalVaccinationDosesSecondDose30to39years",
            "VaccinationsTotalVaccinationDosesSecondDose18to29years",
            "VaccinationsTotalVaccinationDosesSecondDose17yearsandunder",
        ])

    def pipe_add_totals(self, df: pd.DataFrame) -> pd.DataFrame:
        df["80+"] = df["VaccinationsTotalVaccinationDosesFirstDose80yearsandover"] + df[
            "VaccinationsTotalVaccinationDosesSecondDose80yearsandover"]
        df["75-79"] = df["VaccinationsTotalVaccinationDosesFirstDose75to79years"] + df[
            "VaccinationsTotalVaccinationDosesSecondDose75to79years"]
        df["70-74"] = df["VaccinationsTotalVaccinationDosesFirstDose70to74years"] + df[
            "VaccinationsTotalVaccinationDosesSecondDose70to74years"]
        df["65-69"] = df["VaccinationsTotalVaccinationDosesFirstDose65to69years"] + df[
            "VaccinationsTotalVaccinationDosesSecondDose65to69years"]
        df["60-64"] = df["VaccinationsTotalVaccinationDosesFirstDose60to64years"] + df[
            "VaccinationsTotalVaccinationDosesSecondDose60to64years"]
        df["55-59"] = df["VaccinationsTotalVaccinationDosesFirstDose55to59years"] + df[
            "VaccinationsTotalVaccinationDosesSecondDose55to59years"]
        df["50-54"] = df["VaccinationsTotalVaccinationDosesFirstDose50to54years"] + df[
            "VaccinationsTotalVaccinationDosesSecondDose50to54years"]
        df["40-49"] = df["VaccinationsTotalVaccinationDosesFirstDose40to49years"] + df[
            "VaccinationsTotalVaccinationDosesSecondDose40to49years"]
        df["30-39"] = df["VaccinationsTotalVaccinationDosesFirstDose30to39years"] + df[
            "VaccinationsTotalVaccinationDosesSecondDose30to39years"]
        df["18-29"] = df["VaccinationsTotalVaccinationDosesFirstDose18to29years"] + df[
            "VaccinationsTotalVaccinationDosesSecondDose18to29years"]
        df["0-17"] = df["VaccinationsTotalVaccinationDosesFirstDose17yearsandunder"] + df[
            "VaccinationsTotalVaccinationDosesSecondDose17yearsandunder"]
        df = df[
            ["Date", "0-17", "18-29", "30-39", "40-49", "50-54", "55-59", "60-64", "65-69", "70-74", "75-79", "80+"]]
        return df

    def pipe_melt_by_age_group(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.melt(["Date"], var_name='age_group')

    def pipe_minmax_by_age_group(self, df: pd.DataFrame) -> pd.DataFrame:
        df[["age_group_min", "age_group_max"]] = df.age_group.apply(lambda x: pd.Series(str(x).split("-")))
        return df

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[self.columns_rename.keys()].rename(columns=self.columns_rename)

    def pipe_rename_columns_by_age_group(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns={"Date": "date", "value": "total_vaccinations"})

    def pipe_enrich_vaccine_name(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(vaccine="Oxford/AstraZeneca, Pfizer/BioNTech")

    def pipe_enrich_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            location="Jersey",
            source_url=self.source_url
        )

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
                .pipe(self.pipe_rename_columns)
                .pipe(self.pipe_enrich_vaccine_name)
                .pipe(self.pipe_enrich_columns)
                .sort_values("date")
        )

    def pipeline_by_age_group(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
                .pipe(self.pipe_add_totals)
                .pipe(self.pipe_melt_by_age_group)
                .pipe(self.pipe_rename_columns_by_age_group)
                .pipe(self.pipe_minmax_by_age_group)
                .pipe(self.pipe_enrich_columns)
                .sort_values("date")
        )

    def to_csv(self, paths):
        """Generalized."""
        df = self.read().pipe(self.pipeline)
        df.to_csv(paths.tmp_vax_out(self.location), index=False)

        df_age = self.read_by_age_group().pipe(self.pipeline_by_age_group)
        df_age = df_age.replace(to_replace="80+", value="80")
        df_age = df_age[["date", "age_group_min", "age_group_max", "total_vaccinations", "location"]]
        df_age.to_csv(paths.tmp_vax_out_by_age_group(self.location), index=False)


def main(paths):
    Jersey(
        source_url="https://www.gov.je/Datasets/ListOpenData?ListName=COVID19Weekly&clean=true",
        location="Jersey",
        columns_rename={
            "Date": "date",
            "VaccinationsTotalNumberDoses": "total_vaccinations",
            "VaccinationsTotalNumberFirstDoseVaccinations": "people_vaccinated",
            "VaccinationsTotalNumberSecondDoseVaccinations": "people_fully_vaccinated",
        },
    ).to_csv(paths)


if __name__ == "__main__":
    main()
