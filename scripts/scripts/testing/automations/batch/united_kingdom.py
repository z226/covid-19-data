import pandas as pd
import requests
from datetime import datetime
from uk_covid19 import Cov19API


def main():

    # England
    filters = ["areaType=Nation", "areaName=England"]
    structure = {
        "Date": "date",
        "Country": "areaName",
        "Daily change in cumulative total": "newTestsByPublishDate",
    }
    api = Cov19API(filters=filters, structure=structure)
    england = api.get_dataframe()

    # N ireland (PCR only)
    filters = ["areaType=Nation", "areaName=Northern Ireland"]
    structure = {
        "Date": "date",
        "Country": "areaName",
        "Daily change in cumulative total": "newPCRTestsByPublishDate",
    }
    api = Cov19API(filters=filters, structure=structure)
    nireland = api.get_dataframe()

    # Scotland (PCR only)
    filters = ["areaType=Nation", "areaName=Scotland"]
    structure = {
        "Date": "date",
        "Country": "areaName",
        "Daily change in cumulative total": "newPCRTestsByPublishDate",
    }
    api = Cov19API(filters=filters, structure=structure)
    scotland = api.get_dataframe()

    # Wales (PCR only)
    filters = ["areaType=Nation", "areaName=Wales"]
    structure = {
        "Date": "date",
        "Country": "areaName",
        "Daily change in cumulative total": "newPCRTestsByPublishDate",
    }
    api = Cov19API(filters=filters, structure=structure)
    wales = api.get_dataframe()

    countries = [england, nireland, scotland, wales]
    uk = pd.concat(countries).sort_values("Date")
    uk = uk.groupby("Date", as_index=False).agg(
        {"Daily change in cumulative total": "sum"}
    )

    uk["Country"] = "United Kingdom"
    uk["Source URL"] = "https://coronavirus.data.gov.uk/details/testing"
    uk["Source label"] = "Public Health England"
    uk["Units"] = "tests performed"
    uk["Notes"] = pd.NA

    uk.to_csv("automated_sheets/United Kingdom.csv", index=False)


if __name__ == "__main__":
    main()
