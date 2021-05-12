import pandas as pd

from vax.utils.incremental import increment
from vax.utils.checks import VACCINES_ONE_DOSE

# Dict mapping WHO country names -> OWID country names
COUNTRIES = {
    "Egypt": "Egypt",
}

# Dict mapping WHO vaccine names -> OWID vaccine names
VACCINES = {
    "Beijing CNBG - Inactivated": "Sinopharm/Beijing",
    "Wuhan CNBG - Inactivated": "Sinopharm/Wuhan",
    "Sinovac - CoronaVac": "Sinovac",
    "Gamaleya - Sputnik V": "Sputnik V",
    "Pfizer BioNTech - Comirnaty": "Pfizer/BioNTech",
    "Moderna - mRNA-1273": "Moderna",
    "SII - Covishield": "Oxford/AstraZeneca",
    "AstraZeneca - AZD1222": "Oxford/AstraZeneca",
    "Bharat - Covaxin": "Covaxin",
    "SRCVB - EpiVacCorona": "EpiVacCorona",
    "Janssen - Ad26.COV 2.5": "Johnson&Johnson",
    "CanSino - Ad5-nCOV": "CanSino",
}


def read(source: str) -> pd.DataFrame:
    return pd.read_csv(source)

def source_checks(df: pd.DataFrame) -> pd.DataFrame:
    if len(df) > 300:
        raise ValueError(f"Check source, it may contain updates from several dates! Shape found was {df.shape}")
    if df.groupby("COUNTRY").DATE_UPDATED.nunique().nunique() == 1:
        if df.groupby("COUNTRY").DATE_UPDATED.nunique().unique()[0] != 1:
            raise ValueError("Countries have more than one date update!")
    else:
        raise ValueError("Countries have more than one date update!")
    return df

def filter_countries(df: pd.DataFrame) -> pd.DataFrame:
    """Get rows from selected countries."""
    df = df[df.DATA_SOURCE == "REPORTING"].copy()
    df = df[df.COUNTRY.isin(COUNTRIES.values())]
    df["COUNTRY"] = df.COUNTRY.replace(COUNTRIES)
    return df


def vaccine_checks(df: pd.DataFrame) -> pd.DataFrame:
    vaccines_used = set(
        df.VACCINES_USED
        .dropna()
        .apply(lambda x: [xx.strip() for xx in x.split(",")])
        .sum()
    )
    vaccines_unknown = vaccines_used.difference(VACCINES)
    if vaccines_unknown:
        raise ValueError(f"Unknown vaccines {vaccines_unknown}. Update vax.incremental.who.VACCINES accordingly.")
    return df

def map_vaccines_func(row) -> tuple:
    """Replace vaccine names and create column `only_2_doses`."""
    if pd.isna(row.VACCINES_USED):
        raise ValueError("Vaccine field is NaN")
    vaccines = pd.Series(row.VACCINES_USED.split(","))
    vaccines = vaccines.replace(VACCINES)
    only_2doses = all(-vaccines.isin(pd.Series(VACCINES_ONE_DOSE)))

    return pd.Series([", ".join(sorted(vaccines.unique())), only_2doses])


def map_vaccines(df: pd.DataFrame) -> pd.DataFrame:
    # Based on the list of known vaccines, identifies whether each country is using only 2-dose
    # vaccines or also some 1-dose vaccines. This determines whether people_fully_vaccinated can be
    # calculated as total_vaccinations - people_vaccinated.
    # Vaccines check
    df[["VACCINES_USED", "only_2doses"]] = df.apply(map_vaccines_func, axis=1)
    return df


def calculate_metrics(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[df.only_2doses, "people_fully_vaccinated"] = (
        df.TOTAL_VACCINATIONS - df.PERSONS_VACCINATED_1PLUS_DOSE
    )
    df.loc[~df.only_2doses, "people_fully_vaccinated"] = None
    df[["TOTAL_VACCINATIONS", "PERSONS_VACCINATED_1PLUS_DOSE", "people_fully_vaccinated"]] = (
        df[["TOTAL_VACCINATIONS", "PERSONS_VACCINATED_1PLUS_DOSE", "people_fully_vaccinated"]]
        .astype("Int64").fillna(pd.NA)
    )
    return df


def increment_countries(df: pd.DataFrame):
    for row in df.iterrows():
        row = row[1]
        increment(
            location=row["COUNTRY"],
            total_vaccinations=row["TOTAL_VACCINATIONS"],
            people_vaccinated=row["PERSONS_VACCINATED_1PLUS_DOSE"],
            people_fully_vaccinated=row["people_fully_vaccinated"],
            date=row["DATE_UPDATED"],
            vaccine=row["VACCINES_USED"],
            source_url="https://covid19.who.int/",
        )


def main():
    source_url = "https://covid19.who.int/who-data/vaccination-data.csv"
    df = (
        read(source_url)
        .pipe(source_checks)
        .pipe(filter_countries)
        .pipe(vaccine_checks)
        .pipe(map_vaccines)
        .pipe(calculate_metrics)
    )
    increment_countries(df)


if __name__ == "__main__":
    main()
