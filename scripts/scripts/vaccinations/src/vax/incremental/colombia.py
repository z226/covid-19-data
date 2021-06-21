import os

import pandas as pd
import json 

import gspread

from vax.utils.incremental import enrich_data, increment, clean_count
from vax.utils.dates import clean_date


def read(source: str) -> pd.Series:
    return connect_parse_data(source)

def open_google_sheet(source: str):
    vax_credentials_file = os.environ['OWID_COVID_VAX_CREDENTIALS_FILE']
    with open(vax_credentials_file) as f:
        data = json.load(f)
        google_credentials_json = data['google_credentials']

    gc = gspread.service_account(google_credentials_json)

    ssh = gc.open_by_url(source)
    wks = ssh.get_worksheet(1)

    return wks

def connect_parse_data(source: str) -> pd.Series:
    sheet = open_google_sheet(source)

    date = sheet.get('C44').first().strip()
    total_vaccinations = int(sheet.get('K16').first().strip().replace(',', ''))
    people_fully_vaccinated = int(sheet.get('K27').first().strip().replace(',', ''))

    people_vaccinated = total_vaccinations - people_fully_vaccinated

    return pd.Series({
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
        "date": clean_date(date, "%d/%m/%Y")
    })


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Colombia")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Pfizer/BioNTech, Sinovac")


def enrich_source(ds: pd.Series, source: str) -> pd.Series:
    return enrich_data(ds, "source_url", source)


def pipeline(ds: pd.Series, source: str) -> pd.Series:
    return (
        ds
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source, source)
    )


def main(paths):
    source = (
        "https://docs.google.com/spreadsheets/d/1eblBeozGn1soDGXbOIicwyEDkUqNMzzpJoAKw84TTA4"
    )
    data = read(source).pipe(pipeline, source)

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
