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
            id_vars=["date"],
            value_vars=["total_coronavac", "total_pfizer", "total_astrazeneca"],
            var_name="manufacturer",
            value_name="total_vaccinations"
        )
        .rename(columns=vaccines_mapping)
        .sort_values(["date", "manufacturer"])
    )

    df_manufacturer.to_csv(paths.tmp_vax_out_man("Uruguay"), index=False)


if __name__ == "__main__":
    main()
