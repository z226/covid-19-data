import requests
import tempfile

import pandas as pd


class Jersey:

    def __init__(self):
        """Constructor.

        Args:
            source_url (str): Source data url
            location (str): Location name
            columns_rename (dict, optional): Maps original to new names. Defaults to None.
        """
        self.source_url = "https://www.gov.je/Datasets/ListOpenData?ListName=COVID19Weekly&clean=true"
        self.location = "Jersey"
        self.columns_rename = {
            "Date": "date",
            "VaccinationsTotalNumberDoses": "total_vaccinations",
            "VaccinationsTotalNumberFirstDoseVaccinations": "people_vaccinated",
            "VaccinationsTotalNumberSecondDoseVaccinations": "people_fully_vaccinated",
        }

    def read(self):
        with tempfile.NamedTemporaryFile() as tf:
            with open(tf.name, mode="wb") as f:
                f.write(requests.get(self.source_url).content)
            return pd.read_csv(tf.name)

    def pipe_select_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[self.columns_rename.keys()]

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[self.columns_rename.keys()].rename(columns=self.columns_rename)

    def pipe_enrich_vaccine_name(self, df: pd.DataFrame) -> pd.DataFrame:
        def _enrich_vaccine(date: str) -> str:
            if date >= "2021-04-07":
                return "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
            return "Oxford/AstraZeneca, Pfizer/BioNTech"
        return df.assign(vaccine=df.date.astype(str).apply(_enrich_vaccine))

    def pipe_enrich_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            location=self.location,
            source_url=self.source_url
        )

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .pipe(self.pipe_select_columns)
            .pipe(self.pipe_rename_columns)
            .pipe(self.pipe_enrich_vaccine_name)
            .pipe(self.pipe_enrich_columns)
            .sort_values("date")
            [[
                "location", "date", "vaccine", "source_url", "total_vaccinations", "people_vaccinated",
                "people_fully_vaccinated"
            ]]
        )

    def pipe_age_select_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[[
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
        ]]

    def _filter_df_columns_age_group(self, df: pd.DataFrame, age_group: str) -> pd.DataFrame:
        if age_group == "80+":
            res = df.filter(regex=(r"VaccinationsTotalVaccinationDoses(?:First|Second)Dose80yearsandover"))
        elif age_group == "0-17":
            res = df.filter(regex=(r"VaccinationsTotalVaccinationDoses(?:First|Second)Dose17yearsandunder"))
        else:
            age_min, age_max = age_group.split("-")
            res = df.filter(regex=(
                r"VaccinationsTotalVaccinationDoses(?:First|Second)" + 
                f"Dose{age_min}to{age_max}years"))
        return res.sum(axis=1)

    def pipe_age_create_groups(self, df: pd.DataFrame) -> pd.DataFrame:
        age_groups = ["0-17", "18-29", "30-39", "40-49", "50-54", "55-59", "60-64", "65-69", "70-74", "75-79", "80+"]
        # df.filter(regex=(r"VaccinationsTotalVaccinationDoses(?:First|Second)Dose80yearsandover")).sum(axis=1)
        dix = {
            age_group: self._filter_df_columns_age_group(df, age_group) for age_group in age_groups
        }
        df= df.assign(**dix)
        df = df[
            ["Date", "0-17", "18-29", "30-39", "40-49", "50-54", "55-59", "60-64", "65-69", "70-74", "75-79", "80+"]]
        return df

    def pipe_age_melt(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.melt(["Date"], var_name='age_group')

    def pipe_age_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns={"Date": "date", "value": "total_vaccinations"})

    def pipe_age_minmax_values(self, df: pd.DataFrame) -> pd.DataFrame:
        df[["age_group_min", "age_group_max"]] = df.age_group.str.split("-", expand=True)
        df = df.replace(to_replace="80+", value="80")
        return df

    def pipeline_age(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .pipe(self.pipe_age_create_groups)
            .pipe(self.pipe_age_melt)
            .pipe(self.pipe_age_rename_columns)
            .pipe(self.pipe_age_minmax_values)
            .pipe(self.pipe_enrich_columns)
            .sort_values(["date", "age_group_min"])
            [["location", "date", "age_group_min", "age_group_max", "total_vaccinations"]]
        )

    def to_csv(self, paths):
        """Generalized."""
        df_base = self.read()
        # Main data
        df = df_base.pipe(self.pipeline)
        df.to_csv(paths.tmp_vax_out(self.location), index=False)
        # Age data
        df_age = df_base.pipe(self.pipeline_age)
        df_age.to_csv(paths.tmp_vax_out_by_age_group(self.location), index=False)


def main(paths):
    Jersey().to_csv(paths)


if __name__ == "__main__":
    main()
