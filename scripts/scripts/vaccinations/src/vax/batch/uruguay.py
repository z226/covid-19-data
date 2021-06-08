import pandas as pd

vaccines_mapping = {
    "total_coronavac": "Sinovac",
    "total_pfizer": "Pfizer/BioNTech",
    "total_astrazeneca": "Oxford/AstraZeneca",
}


class Uruguay:

    def __init__(self):
        self.source_url = "https://raw.githubusercontent.com/3dgiordano/covid-19-uy-vacc-data/main/data/Uruguay.csv"
        self.source_url_age = "https://raw.githubusercontent.com/3dgiordano/covid-19-uy-vacc-data/main/data/Age.csv"
        self.location = "Uruguay"
    
    def read(self):
        return pd.read_csv(self.source_url), pd.read_csv(self.source_url_age)
    
    def pipe_to_csv_main(self, paths, df: pd.DataFrame):
        df.to_csv(
            paths.tmp_vax_out(self.location),
            index=False,
            columns=[
                "location",
                "date",
                "vaccine",
                "source_url",
                "total_vaccinations",
                "people_vaccinated",
                "people_fully_vaccinated",
            ],
        )

    def pipeline_manufacturer(self, df: pd.DataFrame):
        return (
            df
            .drop(columns=["total_vaccinations"])
            .melt(
                id_vars=["date", "location"],
                value_vars=["total_coronavac", "total_pfizer", "total_astrazeneca"],
                var_name="vaccine",
                value_name="total_vaccinations"
            )
            .replace(vaccines_mapping)
            .sort_values(["date", "vaccine"])
        )

    def to_csv(self, paths):
        df, df_age = self.read()

        # Export main
        df.pipe(self.pipe_to_csv_main)
        # Export manufacturer data
        df.pipe(self.pipeline_manufacturer).to_csv(paths.tmp_vax_out_man(self.location), index=False)
        # Export age data
        df_age.pipe(self.pipeline_age).to_csv(paths.tmp_vax_out_by_age_group(self.location), index=False)
        
def main(paths):
    df = pd.read_csv(
        "https://raw.githubusercontent.com/3dgiordano/covid-19-uy-vacc-data/main/data/Uruguay.csv",
    )
    # Export main data
    df.to_csv(paths.tmp_vax_out("Uruguay"), index=False, columns=[
        "location",
        "date",
        "vaccine",
        "source_url",
        "total_vaccinations",
        "people_vaccinated",
        "people_fully_vaccinated",
    ])
    # Generate manufacturer data
    df_manufacturer = (
        df
        .drop(columns=["total_vaccinations"])
        .melt(
            id_vars=["date", "location"],
            value_vars=["total_coronavac", "total_pfizer", "total_astrazeneca"],
            var_name="vaccine",
            value_name="total_vaccinations"
        )
        .replace(vaccines_mapping)
        .sort_values(["date", "vaccine"])
    )

    df_manufacturer.to_csv(paths.tmp_vax_out_man("Uruguay"), index=False)

    # Generate Age data
    df_age = pd.read_csv(
        "https://raw.githubusercontent.com/3dgiordano/covid-19-uy-vacc-data/main/data/Age.csv",
        usecols=lambda x: x == "date" or x.startswith("coverage_people_") and (
                    x.split("_")[2].endswith("5") or x.split("_")[2] == "18")
    ).rename(columns=lambda x: x.replace("coverage_people_", "")).replace(to_replace=r"%", value="",
                                                                          regex=True).set_index("date")

    df_age.columns = df_age.columns.str.split("_", expand=True)
    df_age = df_age.stack(dropna=True).stack(dropna=True).rename_axis(
        ("date", "age_group_max", "age_group_min")).reset_index()
    df_age.rename(columns={0: "people_vaccinated_per_100"}, inplace=True)
    df_age["location"] = "Uruguay"
    df_age["age_group_max"].replace({"115": None}, inplace=True)
    df_age = df_age[["date", "age_group_min", "age_group_max", "location", "people_vaccinated_per_100"]].sort_values(
        ["date", "age_group_min"])

    df_age.to_csv(paths.tmp_vax_out_by_age_group("Uruguay"), index=False)

if __name__ == "__main__":
    main()
