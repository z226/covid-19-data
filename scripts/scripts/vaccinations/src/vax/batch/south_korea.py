import pandas as pd

from utils.utils import read_xlsx_from_url
from utils.dates import clean_date_series


class South_korea:
    def __init__(self):
        self.location = "South Korea"
        self.source_url = "https://ncv.kdca.go.kr/boardDownload.es?bid=0037&list_no=443&seq=1"

    def read(self):
        df = read_xlsx_from_url(self.source_url)
        return df

    def pipe_extract(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.iloc[:,[0,1,2,7]]

    def pipe_drop(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.drop([0,1,2,3,4,5],axis=0)

    def pipe_drop_JJ_vac_column(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.iloc[:,[0,1,2,4]]

    def pipe_rename_column(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .rename(columns={
            "Unnamed: 0": "date",
            "Unnamed: 1": "people_vaccinated",
            "Unnamed: 2": "people_fully_vaccinated",
            "Unnamed: 7": "J&J_vaccinated",
            })
        )

    def pipe_source(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(source_url=self.source_url)

    def pipe_location(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location)

    def pipe_sum(self, df: pd.DataFrame) -> pd.DataFrame:
        df['total_vaccinations'] = df['people_vaccinated'] + df['people_fully_vaccinated'] - df['J&J_vaccinated']
        return df

    def pipe_date(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(date=clean_date_series(df.date, "%Y-%m-%d"))

    def pipe_vac(self, df: pd.DataFrame) -> pd.DataFrame:
        def _enrich_vaccine(date: str):
            if date >= '2021-06-18':
                return 'Oxford/AstraZeneca, Pfizer/BioNTech, Johnson&Johnson, Moderna'
            elif date >= '2021-06-10':
                return "Oxford/AstraZeneca, Pfizer/BioNTech, Johnson&Johnson"
            elif date >= '2021-02-27':
                return "Oxford/AstraZeneca, Pfizer/BioNTech"
            elif date >= '2021-02-26':
                return "Oxford/AstraZeneca"
        return df.assign(vaccine=df.date.apply(_enrich_vaccine))

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .pipe(self.pipe_extract)
            .pipe(self.pipe_rename_column)
            .pipe(self.pipe_drop)
            .pipe(self.pipe_sum)
            .pipe(self.pipe_drop_JJ_vac_column)
            .pipe(self.pipe_date)
            .pipe(self.pipe_source)
            .pipe(self.pipe_location)
            .pipe(self.pipe_vac)
            [[
                "location", "date", "vaccine", "source_url", "total_vaccinations", "people_vaccinated",
                "people_fully_vaccinated",
            ]]
        )

    def export(self, paths):
        df = self.read()
        df.pipe(self.pipeline).to_csv(paths.tmp_vax_out(self.location), index=False)

def main(paths):
    South_korea().export(paths)
