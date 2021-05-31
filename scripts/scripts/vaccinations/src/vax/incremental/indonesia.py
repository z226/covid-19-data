from datetime import datetime
from bs4 import BeautifulSoup

import pandas as pd
import requests
import json
import re

from vax.utils.incremental import enrich_data, increment
from vax.utils.utils import get_soup

def read(dose1_source: str, dose2_source: str) -> pd.Series:
    dose1_soup = get_soup(dose1_source)
    dose2_soup = get_soup(dose2_source)
    return parse_data(dose1_soup, dose2_soup)

def parse_data(dose1_soup: BeautifulSoup, dose2_soup: BeautifulSoup) -> pd.Series:
    dose1 = parse_tableau(dose1_soup)
    dose2 = parse_tableau(dose2_soup)
    data = pd.Series({
        "date": generate_date(dose1 + dose2),
        "people_vaccinated": dose1,
        "people_fully_vaccinated": dose2,
        "total_vaccinations": dose1 + dose2
    })
    return data

def generate_date(total_vaccinations: int) -> str:
    previous_data = pd.read_csv("output/Indonesia.csv").iloc[-1]
    if (total_vaccinations > previous_data.total_vaccinations):
        return datetime.now().strftime("%Y-%m-%d")
    else:
        return previous_data.date

def parse_tableau(soup: BeautifulSoup) -> int:
    tableauData = json.loads(soup.find("textarea",{"id": "tsConfigContainer"}).text)
    dataUrl = f'https://public.tableau.com{tableauData["vizql_root"]}/bootstrapSession/sessions/{tableauData["sessionid"]}'
    r = requests.post(dataUrl, data = {"sheet_id": tableauData["sheetId"]})
    dataReg = re.search('\d+;({.*})\d+;({.*})', r.text, re.MULTILINE)
    data = json.loads(dataReg.group(2))
    return data["secondaryInfo"]["presModelMap"]["dataDictionary"]["presModelHolder"]["genDataDictionaryPresModel"]["dataSegments"]["0"]["dataColumns"][0]["dataValues"][0]

def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Indonesia")

def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Sinovac")

def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "source_url", "https://vaksin.kemkes.go.id/#/vaccines")

def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source)
    )

def main(paths):
    dose1_source = "https://public.tableau.com/views/DashboardVaksinKemkes/TotalVaksinasiDosis1?:embed=yes&:showVizHome=no"
    dose2_source = "https://public.tableau.com/views/DashboardVaksinKemkes/TotalVaksinasiDosis2?:embed=yes&:showVizHome=no"
    data = read(dose1_source, dose2_source).pipe(pipeline)
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