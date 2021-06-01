import datetime

import pandas as pd


def read(source: str) -> pd.DataFrame:
    return pd.read_csv(source)


def melt(df: pd.DataFrame) -> pd.DataFrame:
    return df.melt(["Type", "Dose"], var_name="date", value_name="value")


def melt_by_age_group(df: pd.DataFrame) -> pd.DataFrame:
    return df.melt(["Age", "Dose"], var_name="date", value_name="value")


def filter_rows(df: pd.DataFrame) -> pd.DataFrame:
    return df[(df.Type != "Total") & (df.value > 0)]


def filter_rows_by_age_group(df: pd.DataFrame) -> pd.DataFrame:
    return df[(df.Age != "Total") & (df.value > 0)]


def pivot(df: pd.DataFrame) -> pd.DataFrame:
    return df.pivot(index=["Type", "date"], columns="Dose", values="value").reset_index()


def pivot_by_age_group(df: pd.DataFrame) -> pd.DataFrame:
    return df.pivot(index=["Age", "date"], columns="Dose", values="value").reset_index()


def enrich_vaccinations(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.assign(total_vaccinations=df.First.fillna(0) + df.Second.fillna(0))
        .rename(columns={"First": "people_vaccinated", "Second": "people_fully_vaccinated"})
    )


def rename_vaccines(df: pd.DataFrame) -> pd.DataFrame:
    vaccine_mapping = {
        "Pfizer": "Pfizer/BioNTech",
        "Sinovac": "Sinovac",
        "Astra-Zeneca": "Oxford/AstraZeneca"
    }
    assert set(df["Type"].unique()) == set(vaccine_mapping.keys())
    return df.replace(vaccine_mapping)


def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.pipe(melt)
        .pipe(filter_rows)
        .pipe(pivot)
        .pipe(enrich_vaccinations)
        .pipe(rename_vaccines)
    )


def preprocess_by_age_group(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.pipe(melt_by_age_group)
        .pipe(filter_rows_by_age_group)
        .pipe(pivot_by_age_group)
        .pipe(enrich_vaccinations)
        .pipe(enrich_location)
    )


def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df
        .sort_values("Type")
        .groupby("date", as_index=False)
        .agg(
            people_vaccinated=("people_vaccinated", "sum"),
            people_fully_vaccinated=("people_fully_vaccinated", "sum"),
            total_vaccinations=("total_vaccinations", "sum"),
            vaccine=("Type", ", ".join),
        )
    )


def enrich_metadata(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        source_url="https://www.gob.cl/yomevacuno/",
        location="Chile",
    )


def postprocess_vaccinations(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.pipe(aggregate)
        .pipe(enrich_metadata)
    )


def postprocess_manufacturer(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df[["Type", "date", "total_vaccinations"]]
        .rename(columns={"Type": "vaccine"})
        .assign(location="Chile")
    )


def enrich_location(df: pd.DataFrame) -> pd.DataFrame:
    """Generalized."""
    return df.assign(location="Chile")


def main(paths):
    source = "https://github.com/juancri/covid19-vaccination/raw/master/output/chile-vaccination-type.csv"
    data = read(source).pipe(preprocess)
    location = "Chile"

    condition = (datetime.datetime.now() - pd.to_datetime(data.date.max())).days < 3
    # assert condition, "Data in external repository has not been updated for some days now"

    data.pipe(postprocess_vaccinations).to_csv(
        paths.tmp_vax_out(location),
        index=False
    )
    data.pipe(postprocess_manufacturer).to_csv(
        paths.tmp_vax_out_man(location),
        index=False
    )

    # Vaccination data by age group
    source_by_age_group = "https://raw.githubusercontent.com/juancri/covid19-vaccination/master/output/chile-vaccination-ages.csv"
    data_by_age_group = read(source_by_age_group).pipe(preprocess_by_age_group)
    data_by_age_group[["age_group_min", "age_group_max"]] = data_by_age_group.Age.apply(
        lambda x: pd.Series(str(x).split("-")))
    data_by_age_group = data_by_age_group[["date", "age_group_min", "age_group_max", "total_vaccinations", "location"]]
    data_by_age_group = data_by_age_group.replace(to_replace='80 y más años', value='80', regex=True)
    data_by_age_group = data_by_age_group.replace(to_replace='80 y más', value='80', regex=True)
    data_by_age_group = data_by_age_group.replace(to_replace='90 y más', value='90', regex=True)
    data_by_age_group = data_by_age_group.sort_values(by="date")
    data_by_age_group.to_csv(
        paths.tmp_vax_out_by_age_group(location),
        index=False
    )


if __name__ == "__main__":
    main()
