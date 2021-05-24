from datetime import datetime
import re

import pandas as pd
import tabula

from vax.utils.utils import get_soup
from vax.utils.incremental import clean_count, enrich_data, increment, clean_date


def read(source: str) -> pd.Series:
    url = f"{source}/Category/Page/9jFXNbCe-sFK9EImRRi2Og"
    soup = get_soup(url)
    url_pdf = parse_pdf_link(source, soup)
    df = parse_table(url_pdf)
    return pd.Series({
        "total_vaccinations": parse_total_vaccinations(df),
        "people_vaccinated": parse_total_vaccinations(df),
        "date": parse_date(soup),
    })


def parse_pdf_link(base_url: str, soup) -> str:
    a = soup.find(class_="download").find("a")
    url_pdf = f"{base_url}{a['href']}"
    soup = get_soup(url_pdf)
    a = soup.find(class_="viewer-button")
    return f"{base_url}{a['href']}"


def parse_table(url_pdf: str) -> int:
    kwargs = {"pandas_options": {"dtype": str, "header": None}}
    dfs_from_pdf = tabula.read_pdf(url_pdf, pages="all", **kwargs)
    df = dfs_from_pdf[0]
    # df = df.dropna(subset=[2])
    return df


def parse_total_vaccinations(df: pd.DataFrame) -> int:
    num = df.iloc[-1, 4]
    num = re.match(r"([0-9,]+)", num).group(1)
    return clean_count(num)


def parse_date(soup) -> str:
    date_raw = soup.find(class_="download").text
    regex = r"(\d{4})\sCOVID-19疫苗日報表"
    date_str = re.search(regex, date_raw).group(1)
    date_str = clean_date("2021" + date_str, "%Y%m%d")
    return date_str


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Taiwan")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Oxford/AstraZeneca")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
    )


def main(paths):
    source = "https://www.cdc.gov.tw"
    data = read(source).pipe(pipeline)
    increment(
        paths=paths,
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        date=data["date"],
        source_url=f"{source}/Category/Page/9jFXNbCe-sFK9EImRRi2Og",
        vaccine=data["vaccine"]
    )


if __name__ == "__main__":
    main()
