import time

import pandas as pd

from vax.utils.incremental import enrich_data, increment, clean_count
from vax.utils.utils import get_driver


def read(source: str) -> pd.Series:

    with get_driver() as driver:
        driver.get(source)
        time.sleep(2)
        spans = [
            span
            for span in driver.find_elements_by_tag_name("span")
            if span.get_attribute("data-text")
        ]

        date_raw = spans[6].text
        date = str(
            pd.to_datetime(date_raw.replace("(as of ", "").replace(")", "")).date()
        )

        total_vaccinations = clean_count(spans[8].text)
        people_vaccinated = clean_count(spans[13].text)
        people_fully_vaccinated = clean_count(spans[15].text)

    assert total_vaccinations == people_vaccinated + people_fully_vaccinated

    return pd.Series(
        data={
            "total_vaccinations": total_vaccinations,
            "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_fully_vaccinated,
            "date": date,
        }
    )


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Philippines")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(
        ds,
        "vaccine",
        "Johnson&Johnson, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech, Sinovac, Sputnik V",
    )


def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(
        ds,
        "source_url",
        "https://news.abs-cbn.com/spotlight/multimedia/infographic/03/23/21/philippines-covid-19-vaccine-tracker",
    )


def pipeline(ds: pd.Series) -> pd.Series:
    return ds.pipe(enrich_location).pipe(enrich_vaccine).pipe(enrich_source)


def main(paths):
    source = "https://e.infogram.com/_/yFVE69R1WlSdqY3aCsBF"
    data = read(source).pipe(pipeline)
    increment(
        paths=paths,
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        people_vaccinated=data["people_vaccinated"],
        people_fully_vaccinated=data["people_fully_vaccinated"],
        date=data["date"],
        source_url=data["source_url"],
        vaccine=data["vaccine"],
    )


if __name__ == "__main__":
    main()
