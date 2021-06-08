import pandas as pd

vaccines_mapping = {
    "total_coronavac": "Sinovac",
    "total_pfizer": "Pfizer/BioNTech",
    "total_astrazeneca": "Oxford/AstraZeneca",
}

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
    assert set(vaccines_mapping.values()) == set(df_manufacturer.vaccine)

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
