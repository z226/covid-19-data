import pandas as pd

def read(source: str) -> pd.DataFrame:
    return pd.read_csv(source, usecols=["date", "dose1_cumul", "dose2_cumul", "total_cumul"])

def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={"dose1_cumul": "people_vaccinated", "dose2_cumul": "people_fully_vaccinated", "total_cumul": "total_vaccinations"})

def enrich_vaccine(date: str) -> str:
    if date >= "2021-05-05":
      return "Oxford/AstraZeneca, Pfizer/BioNTech, Sinovac"
    if date >= "2021-03-17":
      return "Pfizer/BioNTech, Sinovac"
    return "Pfizer/BioNTech"

def enrich_metadata(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        location="Malaysia",
        source_url="https://github.com/CITF-Malaysia/citf-public",
    )

def pipeline(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df
        .pipe(rename_columns)
        .assign(vaccine=df.date.astype(str).apply(enrich_vaccine))
        .pipe(enrich_metadata)
    )


def main():
    source = "https://raw.githubusercontent.com/CITF-Malaysia/citf-public/main/vax_malaysia.csv"
    read(source).pipe(pipeline).to_csv("Malaysia", index=False)

if __name__ == "__main__":
    main()
