import re

import requests
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from vax.utils.incremental import enrich_data, increment, clean_count
from vax.utils.dates import localdate


def read(source: str) -> pd.Series:
    return connect_parse_data(source)

def connect_parse_data(source: str) -> pd.Series:
    op = Options()
    op.add_argument("--headless")

    with webdriver.Chrome(options=op) as driver:
        # Main page
        driver.get(source)
        # Get report page from within iframe
        source = driver.find_element_by_xpath("/html/body/section[2]/iframe").get_attribute("src")

        driver.get(source)

        data_blocks = (
            WebDriverWait(driver, 20)
            .until(EC.visibility_of_all_elements_located((By.CLASS_NAME, "card")))
        )
        for block in data_blocks:
            block_title = block.get_attribute("aria-label")
            if "first dose" in block_title:
                people_vaccinated = re.search(r"first dose +(\d+)\.", block_title).group(1)
            elif "sec dose" in block_title:
                people_fully_vaccinated = re.search(r"sec dose +(\d+)\.", block_title).group(1)

        people_vaccinated=clean_count(people_fully_vaccinated)
        people_fully_vaccinated=clean_count(people_fully_vaccinated)

        total_vaccinations = people_vaccinated+people_fully_vaccinated

    return pd.Series({
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
    })

def format_date(ds: pd.Series) -> pd.Series:
    date = localdate("Asia/Amman")
    return enrich_data(ds, 'date', date)

def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, 'location', "Jordan")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, 'vaccine', "Pfizer/BioNTech, Sinopharm/Beijing, Sputnik V, Oxford/AstraZeneca") # Johnson&Johnson authorized, no doses arrived or given yet


def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, 'source_url', "https://corona.moh.gov.jo/ar")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(format_date)
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source)
    )


def main(paths):
    # At the date of this automation, only the Arabic version of the website had the vaccination numbers
    source = "https://corona.moh.gov.jo/ar"
    data = read(source).pipe(pipeline)
    increment(
        paths=paths,
        location=data['location'],
        total_vaccinations=data['total_vaccinations'],
        people_vaccinated=data['people_vaccinated'],
        people_fully_vaccinated=data['people_fully_vaccinated'],
        date=data['date'],
        source_url=data['source_url'],
        vaccine=data['vaccine']
    )

if __name__ == "__main__":
    main()
