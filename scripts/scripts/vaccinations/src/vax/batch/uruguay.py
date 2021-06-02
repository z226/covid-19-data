import pandas as pd


def main(paths):
    df = pd.read_csv(
        "https://raw.githubusercontent.com/3dgiordano/covid-19-uy-vacc-data/main/data/Uruguay.csv",

    )
    
    df.to_csv(paths.tmp_vax_out("Uruguay"), index=False, columns=[
        "location",
        "date",
        "vaccine",
        "source_url",
        "total_vaccinations",
        "people_vaccinated",
        "people_fully_vaccinated",
    ])

    by_man_sinovac = pd.DataFrame(
        {
            "date": df['date'].values,
            "location": df['location'].values,
            "vaccine": "Sinovac",
            "total_vaccinations": df['total_coronavac'].values,
        }
    )

    by_man_pfizer = pd.DataFrame(
        {
            "date": df['date'].values,
            "location": df['location'].values,
            "vaccine": "Pfizer/BioNTech",
            "total_vaccinations": df['total_pfizer'].values,
        }
    )

    by_man_astrazeneca = pd.DataFrame(
        {
            "date": df['date'].values,
            "location": df['location'].values,
            "vaccine": "Oxford/AstraZeneca",
            "total_vaccinations": df['total_astrazeneca'].values,
        }
    )

    by_manufacturer = pd.concat([by_man_sinovac, by_man_pfizer, by_man_astrazeneca]).sort_values("date")

    by_manufacturer.to_csv(paths.tmp_vax_out_man("Uruguay"), index=False)


if __name__ == "__main__":
    main()
