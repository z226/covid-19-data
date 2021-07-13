import pandas as pd

from vax.utils.incremental import enrich_data, increment, clean_count
from vax.utils.dates import clean_date


class Colombia:

    def __init__(self, gsheets_api) -> None:
        self.location = "Colombia"
        self.source_url = "https://docs.google.com/spreadsheets/d/1eblBeozGn1soDGXbOIicwyEDkUqNMzzpJoAKw84TTA4"
        self.gsheets_api = gsheets_api

    @property
    def sheet_id(self):
        return self.source_url.split("/")[-1]

    def read(self) -> pd.Series:
        ws = self.gsheets_api.get_worksheet(self.sheet_id, "Reporte diario")
        df = self._parse_data(ws)
        return df

    def _parse_data(self, worksheet):
        if worksheet.nrows != 45:
            raise ValueError("Sheet format changed!")
        total_vaccinations = self._parse_total_vaccinations(worksheet)
        people_fully_vaccinated = self._parse_people_fully_vaccinated(worksheet)
        unique_doses = self._parse_unique_doses(worksheet)
        if total_vaccinations is None or people_fully_vaccinated is None:
            return None
        return pd.Series({
            "date": self._parse_date(worksheet),
            "total_vaccinations": total_vaccinations,
            "people_fully_vaccinated": people_fully_vaccinated,
            "people_vaccinated": total_vaccinations - people_fully_vaccinated + unique_doses,
        })

    def _parse_total_vaccinations(self, worksheet):
        nrow_total_doses = 23
        if "Total dosis aplicadas al" in worksheet.at(nrow_total_doses, 13):
            return worksheet.at(nrow_total_doses, 14)
        else:
            raise ValueError("Sheet format changed!")

    def _parse_people_fully_vaccinated(self, worksheet):
        nrow_fully_vaccinated = 40
        if worksheet.at(nrow_fully_vaccinated, 13) == "Esquemas completos con segundas dosis y dosis única":
            return worksheet.at(nrow_fully_vaccinated, 14)
        else:
            raise ValueError("Sheet format changed!")

    def _parse_unique_doses(self, worksheet):
        nrow_doses_unique = 38
        if worksheet.at(nrow_doses_unique, 13) == "Total únicas dosis acumuladas":
            return worksheet.at(nrow_doses_unique, 14)
        else:
            raise ValueError("Sheet format changed!")

    def _parse_date(self, worksheet):
        nrow_date = 44
        if worksheet.at(nrow_date, 1) == "Fecha de corte:":
            date_raw = worksheet.at(nrow_date, 2)
            date_str = clean_date(date_raw, "%d/%m/%Y")
            return date_str
        else:
            raise ValueError("Date is not where it is expected be! Check worksheet")

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", "Colombia")

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Pfizer/BioNTech, Sinovac")

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url)

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return (
            ds
            .pipe(self.pipe_location)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_source)
        )

    def to_csv(self, paths):
        data = self.read()
        if "total_vaccinations" in data:
            data = data.pipe(self.pipeline)
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
        else:
            print("skipped")


def main(paths, gsheets_api):
    Colombia(gsheets_api=gsheets_api).to_csv(paths)
