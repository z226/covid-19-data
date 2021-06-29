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
        return self._parse_data(ws)

    def _parse_data(self, worksheet):
        if worksheet.nrows != 44:
            raise ValueError("Sheet format changed!")
        total_vaccinations = self._parse_total_vaccinations(worksheet)
        people_fully_vaccinated = self._parse_people_fully_vaccinated(worksheet)
        if total_vaccinations is None or people_fully_vaccinated is None:
            return None
        return pd.Series({
            "date": self._parse_date(worksheet),
            "total_vaccinations": total_vaccinations,
            "people_fully_vaccinated": people_fully_vaccinated,
        })

    def _parse_total_vaccinations(self, worksheet):
        nrow_doses_1 = 15
        if worksheet.at(nrow_doses_1, 13) == "Total dosis aplicadas":
            return worksheet.at(nrow_doses_1, 14)

    def _parse_people_fully_vaccinated(self, worksheet):
        nrow_doses_1 = 32
        if worksheet.at(nrow_doses_1, 13) == "Esquemas completos con segudas dosis y dosis única":
            return worksheet.at(nrow_doses_1, 14)

    def _parse_date(self, worksheet):
        nrow_date = 43
        if worksheet.at(nrow_date, 1) == "Fecha de corte:":
            return clean_date(worksheet.at(nrow_date, 2), "%d/%m/%Y")
        else:
            raise ValueError("Date is not where it is expected be! Check worksheet")

    def pipe_metrics(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "people_vaccinated", ds.total_vaccinations - ds.people_fully_vaccinated)

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", "Colombia")

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Pfizer/BioNTech, Sinovac")

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url)

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return (
            ds
            .pipe(self.pipe_metrics)
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
