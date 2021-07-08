import requests
import numpy as np
import pandas as pd

from vax.utils.incremental import enrich_data, increment
from vax.utils.dates import clean_date
from vax.utils.utils import get_soup


def read(source: str) -> pd.Series:
    soup = get_soup(source)
    source, date = find_last_report(soup)
    data = parse_data(source)
    data["date"] = clean_date(date, "%Y-%m-%d", minus_days=1)
    return data


def find_last_report(soup) -> str:
    for report in soup.find(id="listView").find_all("ul"):
        if "코로나19 국내 발생 및 예방접종 현황" in report.find(class_="title").text:
            source = "http://www.kdca.go.kr" + report.find("a")["href"]
            date = report.find_all("li")[3].text
            break
    return source, date


def parse_data(source: str) -> pd.Series:

    soup = get_soup(source)
    html_table = str(soup.find_all("table")[2])
    df = pd.read_html(html_table, header=0)[0]

    assert len(df) == 8, "Wrong number of rows in the vaccine table"

    astrazeneca = df.loc[df["백신"] == "아스트라제네카1)", "누적 접종(C)"].dropna().values.astype(int)
    pfizer = df.loc[df["백신"] == "화이자", "누적 접종(C)"].values.astype(int)
    moderna = df.loc[df["백신"] == "모더나", "누적 접종(C)"].values.astype(int)
    johnson = df.loc[df["백신"] == "얀센2)", "누적 접종(C)"].values.astype(int)

    if len(moderna) == 1:
        moderna = np.append(moderna, 0)

    total_vaccinations = astrazeneca.sum() + pfizer.sum() + moderna.sum() + johnson[0]
    people_vaccinated = astrazeneca[0] + pfizer[0] + moderna[0] + johnson[0]
    people_fully_vaccinated = astrazeneca[1] + pfizer[1] + moderna[1] + johnson[0]

    data = {
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
        "total_vaccinations": total_vaccinations,
        "source_url": source,
    }
    return pd.Series(data=data)


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "South Korea")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Johnson&Johnson, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
    )


def main(paths):
    source = "http://www.kdca.go.kr/board/board.es?mid=a20501010000&bid=0015"
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
