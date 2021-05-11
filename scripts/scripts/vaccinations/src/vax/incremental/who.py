import pandas as pd

from vax.utils.incremental import increment


# Dict of WHO country names: OWID country names
COUNTRIES = {
    "Afghanistan": "Afghanistan",
}

# Dict of WHO vaccine names: OWID vaccine names
VACCINES = {
    "Beijing CNBG - Inactivated": "Sinopharm/Beijing",
    "Pfizer BioNTech - Comirnaty": "Pfizer/BioNTech",
    "SII - Covishield": "Oxford/AstraZeneca",
}

# List of OWID vaccine names
ONE_DOSE_VACCINES = ["Johnson&Johnson"]


def read(source: str) -> pd.DataFrame:
    df = pd.read_csv(source)
    assert len(df) < 300
    return df


def filter_rows(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df.DATA_SOURCE == "REPORTING"].copy()

    df["COUNTRY"] = df.COUNTRY.replace(COUNTRIES)
    df = df[df.COUNTRY.isin(COUNTRIES.values())]

    return df


def map_vaccines_func(row) -> tuple:

    vaccines = pd.Series(row.VACCINES_USED.split(","))
    assert all(vaccines.isin(VACCINES.keys())), f"Unkwown vaccine: {vaccines.values}"
    vaccines = vaccines.replace(VACCINES)

    only_2doses = all(-vaccines.isin(pd.Series(ONE_DOSE_VACCINES)))

    return pd.Series([", ".join(sorted(vaccines.unique())), only_2doses])


def map_vaccines(df: pd.DataFrame) -> pd.DataFrame:
    # Based on the list of known vaccines, identifies whether each country is using only 2-dose
    # vaccines or also some 1-dose vaccines. This determines whether people_fully_vaccinated can be
    # calculated as total_vaccinations - people_vaccinated.
    df[["VACCINES_USED", "only_2doses"]] = df.apply(map_vaccines_func, axis=1)
    return df


def calculate_metrics(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[df.only_2doses, "people_fully_vaccinated"] = (
        df.TOTAL_VACCINATIONS - df.PERSONS_VACCINATED_1PLUS_DOSE
    )
    df[["TOTAL_VACCINATIONS", "PERSONS_VACCINATED_1PLUS_DOSE", "people_fully_vaccinated"]] = (
        df[["TOTAL_VACCINATIONS", "PERSONS_VACCINATED_1PLUS_DOSE", "people_fully_vaccinated"]]
        .astype(int)
    )
    return df


def increment_countries(df: pd.DataFrame):
    for row in df.iterrows():
        row = row[1]
        increment(
            location=row["COUNTRY"],
            total_vaccinations=row["TOTAL_VACCINATIONS"],
            people_vaccinated=row["PERSONS_VACCINATED_1PLUS_DOSE"],
            people_fully_vaccinated=(
                None if pd.isna(row["people_fully_vaccinated"]) else row["people_fully_vaccinated"]
            ),
            date=row["DATE_UPDATED"],
            vaccine=row["VACCINES_USED"],
            source_url="https://covid19.who.int/",
        )


def main():
    source_url = "https://covid19.who.int/who-data/vaccination-data.csv"
    df = (
        read(source_url)
        .pipe(filter_rows)
        .pipe(map_vaccines)
        .pipe(calculate_metrics)
    )
    increment_countries(df)


if __name__ == "__main__":
    main()
