import requests
import pandas as pd

from cowidev.vax.utils.incremental import enrich_data, increment
from cowidev.vax.utils.dates import localdatenow, clean_date


def read(source: str) -> pd.Series:
    data = requests.get(source).json()

    people_vaccinated = data["topBlock"]["vaccination"]["tot_dose_1"]
    people_fully_vaccinated = data["topBlock"]["vaccination"]["tot_dose_2"]
    total_vaccinations = data["topBlock"]["vaccination"]["total_doses"]
    date = clean_date(data["timestamp"], "%Y-%m-%d %H:%M:%S")

    return pd.Series({
        "date": date,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
        "total_vaccinations": total_vaccinations,
    })


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "India")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Covaxin, Oxford/AstraZeneca")


def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "source_url", "https://dashboard.cowin.gov.in/")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source)
    )


def main(paths):
    date_str = localdatenow()
    source = f"https://api.cowin.gov.in/api/v1/reports/v2/getPublicReports?state_id=&district_id=&date={date_str}"
    data = read(source).pipe(pipeline)
    increment(
        paths=paths,
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        people_vaccinated=data["people_vaccinated"],
        people_fully_vaccinated=data["people_fully_vaccinated"],
        date=data["date"],
        source_url=data["source_url"],
        vaccine=data["vaccine"]
    )


if __name__ == "__main__":
    main()
