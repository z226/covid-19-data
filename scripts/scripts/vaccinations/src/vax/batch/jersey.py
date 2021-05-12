import requests
import tempfile

import pandas as pd


def main(paths):

    source_url = "https://www.gov.je/Datasets/ListOpenData?ListName=COVID19Weekly&clean=true"

    tf = tempfile.NamedTemporaryFile()

    with open(tf.name, mode="wb") as f:
        f.write(requests.get(source_url).content)

    df = pd.read_csv(tf.name, usecols=[
        "Date",
        "VaccinationsTotalNumberDoses",
        "VaccinationsTotalNumberFirstDoseVaccinations",
        "VaccinationsTotalNumberSecondDoseVaccinations",
    ])

    df = df.rename(columns={
        "Date": "date",
        "VaccinationsTotalNumberDoses": "total_vaccinations",
        "VaccinationsTotalNumberFirstDoseVaccinations": "people_vaccinated",
        "VaccinationsTotalNumberSecondDoseVaccinations": "people_fully_vaccinated",
    })

    df = df.assign(
        location="Jersey",
        vaccine="Oxford/AstraZeneca, Pfizer/BioNTech",
        source_url=source_url,
    )

    df = df.sort_values("date")

    df.to_csv(paths.tmp_vax_out("Jersey"), index=False)


if __name__ == "__main__":
    main()
