import pandas as pd

from vax.utils.incremental import clean_count, enrich_data, increment
from vax.utils.utils import get_soup
from vax.utils.dates import localdate

class Bolivia:

    def __init__(self, source_url: str, location: str):
        self.source_url = source_url
        self.location = location

    def read(self):
        people_vaccinated, people_fully_vaccinated = self.parse_metrics()
        return pd.Series({
            "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_fully_vaccinated,
            "date": self.get_date()
        })

    def parse_metrics(self) -> tuple:
        soup = get_soup(self.source_url)
        elems = soup.find_all(class_="vacunados")
        if len(elems) != 2:
            raise ValueError(
                "Something changed in source layout. More than two elemnts with class='vacunados' were found."
            )
        for elem in elems:
            _ = elem.find_all("span")
            if _[0].text == "1ra Dosis":
                dose_1 = _[1].text
            elif _[0].text == "2da Dosis":
                dose_2 = _[1].text
            else:
                raise ValueError("Something changed in source layout. Name different than '1ra Dosis' or '2da Dosis'")
        return clean_count(dose_1), clean_count(dose_2)
    
    def get_date(self):
        return localdate("America/La_Paz")

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(
            ds,
            "vaccine", 
            "Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sputnik V"
        )

    def pipe_vaccinations(self, ds: pd.Series) -> pd.Series:
        total_vaccinations = ds.people_vaccinated + ds.people_fully_vaccinated
        return enrich_data(ds, "total_vaccinations", total_vaccinations)

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url)

    def pipeline(self,  ds: pd.Series) -> pd.Series:
        return (
            ds
            .pipe(self.pipe_location)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_source)
            .pipe(self.pipe_vaccinations)
        )

    def to_csv(self, paths):
        """Generalized."""
        data = self.read().pipe(self.pipeline)
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


def main(paths):
    Bolivia(
        source_url="https://www.boliviasegura.gob.bo/index.php/category/estadistica/",
        location="Bolivia",
    ).to_csv(paths) 


if __name__ == "__main__":
    main()
