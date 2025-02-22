import re

import requests
import pandas as pd
from bs4 import BeautifulSoup

from cowidev.vax.utils.incremental import enrich_data, increment, clean_count
from cowidev.vax.utils.dates import clean_date


METRIC_LABELS = {
    "total_vaccinations": "toplamasidozusayisi",
    "people_vaccinated": "doz1asisayisi",
    "people_fully_vaccinated": "doz2asisayisi",
    "total_boosters": "doz3asisayisi",
}


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:
    data = {"date": parse_date(soup)}
    for k, v in METRIC_LABELS.items():
        data[k] = parse_metric(soup, v)
    return pd.Series(data=data)


def parse_date(soup: BeautifulSoup) -> str:
    date_raw = re.search(rf"var asidozuguncellemesaati = '(.*202\d)", str(soup))
    return clean_date(date_raw.group(1), fmt="%d %B %Y", lang="tr_TR", loc="tr_TR")


def parse_metric(soup: BeautifulSoup, metric_name: str) -> int:
    metric = re.search(rf"var {metric_name} = '([\d\.]+)';", str(soup)).group(1)
    return clean_count(metric)


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Turkey")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Pfizer/BioNTech, Sinovac")


def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "source_url", "https://covid19asi.saglik.gov.tr/")


def pipeline(ds: pd.Series) -> pd.Series:
    return ds.pipe(enrich_location).pipe(enrich_vaccine).pipe(enrich_source)


def main(paths):
    source = "https://covid19asi.saglik.gov.tr/"
    data = read(source).pipe(pipeline)
    increment(
        paths=paths,
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        people_vaccinated=data["people_vaccinated"],
        people_fully_vaccinated=data["people_fully_vaccinated"],
        total_boosters=data["total_boosters"],
        date=data["date"],
        source_url=data["source_url"],
        vaccine=data["vaccine"],
    )


if __name__ == "__main__":
    main()
