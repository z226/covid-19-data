from datetime import datetime
import re

import pandas as pd
import tabula

from vax.utils.utils import get_soup
from vax.utils.incremental import clean_count, enrich_data, increment, clean_date


vaccines_mapping = {
    "AstraZeneca": "Oxford/AstraZeneca",
    "Moderna": "Moderna",
}


def read(source: str) -> pd.Series:
    url = f"{source}/Category/Page/9jFXNbCe-sFK9EImRRi2Og"
    soup = get_soup(url)
    url_pdf = parse_pdf_link(source, soup)
    dfs = parse_tables(url_pdf)
    total_vaccinations = parse_total_vaccinations(dfs[0])
    people_vaccinated = parse_people_vaccinated(dfs[1])
    return pd.Series({
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "date": parse_date(soup),
        "vaccine": parse_vaccines(dfs[0]),
    })


def parse_pdf_link(base_url: str, soup) -> str:
    a = soup.find(class_="download").find("a")
    url_pdf = f"{base_url}{a['href']}"
    for i in range(10):
        soup = get_soup(url_pdf)
        a = soup.find(class_="viewer-button")
        if a is not None:
            break
    return f"{base_url}{a['href']}"


def parse_tables(url_pdf: str) -> int:
    kwargs = {"pandas_options": {"dtype": str, "header": None}}
    dfs = tabula.read_pdf(url_pdf, pages="all", **kwargs)
    if dfs[0].shape[1] != 3:
        raise ValueError(f"Table 1: format has changed! New columns were added")
    if dfs[0].shape[0] < 3:
        raise ValueError(f"Table 1: format has changed! Not enough rows!")
    if dfs[1].shape[1] != 4:
        raise ValueError(f"Table 2: format has changed! New columns were added")
    if dfs[1].shape[0] < 16:
        raise ValueError(f"Table 2: format has changed! Not enough rows!")
    return dfs

def parse_total_vaccinations(df: pd.DataFrame) -> int:
    # Expect df to be "Table 1"
    num = df.iloc[-1, 2]
    num = re.match(r"([0-9,]+)", num).group(1)
    return clean_count(num)

def parse_people_vaccinated(df: pd.DataFrame) -> int:
    # Expect df to be "Table 2"
    num1 = df.iloc[-1, 2].split(" ")[0]
    num2 = df.iloc[-1, 3].split(" ")[0]
    return clean_count(num1) + clean_count(num2)

def parse_vaccines(df: pd.DataFrame) -> str:
    vaccines = set(df.iloc[1:-1, 0])
    vaccines_wrong = vaccines.difference(vaccines_mapping)
    if vaccines_wrong:
        raise ValueError(f"Invalid vaccines: {vaccines_wrong}")
    return ", ".join(sorted(vaccines_mapping[vax] for vax in vaccines))


def parse_date(soup) -> str:
    date_raw = soup.find(class_="download").text
    regex = r"(\d{4})\sCOVID-19疫苗日報表"
    date_str = re.search(regex, date_raw).group(1)
    date_str = clean_date("2021" + date_str, "%Y%m%d")
    return date_str


def enrich_metrics(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "people_fully_vaccinated", ds.total_vaccinations - ds.people_vaccinated)


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Taiwan")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(enrich_metrics)
        .pipe(enrich_location)
    )


def main(paths):
    source = "https://www.cdc.gov.tw"
    data = read(source).pipe(pipeline)
    increment(
        paths=paths,
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        people_vaccinated=data["people_vaccinated"],
        people_fully_vaccinated=data["people_fully_vaccinated"],
        date=data["date"],
        source_url=f"{source}/Category/Page/9jFXNbCe-sFK9EImRRi2Og",
        vaccine=data["vaccine"]
    )


if __name__ == "__main__":
    main()
