import re
from datetime import datetime

from bs4 import BeautifulSoup
import pandas as pd

from vax.utils.utils import get_soup
from vax.utils.incremental import clean_count, merge_with_current_data
from vax.utils.dates import clean_date


class Cuba:

    def __init__(self, source_url: str, location: str):
        self.source_url = source_url
        self.location = location
        self._num_max_pages = 5
        self.regex = {
            "title": r"Parte de cierre del d.+a (\d{1,2} de [a-z]+) a las \d{1,2} de la noche",
            "data": r"Al cierre del (\d{1,2} de [a-z]+) se acumulan ([0-9millones ]+)"
        }

    def read(self, last_update: str) -> pd.DataFrame:
        data = []
        for cnt in range(1, self._num_max_pages+1):
            url = f"{self.source_url}/{cnt}/"
            # print(f"page: {cnt}", url)
            soup = get_soup(url)
            data_, proceed = self.parse_data(soup, last_update)
            data.extend(data_)
            if not proceed:
                break
        return pd.DataFrame(data)

    def parse_data(self, soup: BeautifulSoup, last_update: str) -> tuple:
        links = self.get_links(soup)
        records = []
        for link in links:
            soup = get_soup(link)
            # print(link)
            record = self.parse_data_news_page(soup)
            if "date" in record:
                if record["date"] > last_update:
                    # print(record["date"], "adding")
                    record = {
                        **record,
                        "source_url": link
                    }
                    records.append(record)
                else: 
                    # print(record["date"], "END")
                    return records, False
        return records, True

    def get_links(self, soup):
        news = soup.find_all("a", text=re.compile(self.regex["title"]))
        # print("* "+"\n* ".join(n.text for n in news))
        return [n.get("href") for n in news]

    def parse_data_news_page(self, soup):
        match = re.search(self.regex["data"], soup.text)
        record = {}
        if match:
            # date
            date_str = match.group(1)
            date_str = clean_date(f"{date_str} {datetime.now().year}", "%d de %B %Y", lang="es")
            record = {
                "date": date_str,
                "total_vaccinations":  _clean_metric(match.group(2)),
            }
            # vaccinations
        return record

    def pipe_add_remaining_metrics_default(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            people_vaccinated=df.total_vaccinations,
            people_fully_vaccinated=0
        )

    def pipe_drop_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.sort_values("date")
            .drop_duplicates(
                subset=[
                    "total_vaccinations",
                    "people_vaccinated",
                    "people_fully_vaccinated"
                ],
                keep="first"
            )
        )

    def pipe_location(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location)

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(vaccine="Abdala, Soberana02")

    def pipe_select_output_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[[
            "loction",
            "date",
            "vaccine",
            "source_url",
            "total_vaccinations",
            "people_vaccinated",
            "people_fully_vaccinated"
        ]]

    def pipeline(self, df: pd.Series) -> pd.Series:
        return (
            df
            .pipe(self.pipe_add_remaining_metrics_default)
            .pipe(self.pipe_drop_duplicates)
            .pipe(self.pipe_location)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_select_output_columns)
            .sort_values(by="date")
        )

    def to_csv(self, paths):
        """Generalized."""
        output_file = paths.tmp_vax_out(self.location)
        last_update = pd.read_csv(output_file).date.max()
        df = self.read(last_update)
        if not df.empty:
            df = df.pipe(self.pipeline)
            df = merge_with_current_data(df, output_file)
            df = df.pipe(self.pipe_drop_duplicates)
            df.to_csv(output_file, index=False)


def _clean_metric(x):
    x = re.split(' mil | millon ', x)
    return clean_count("".join(xx.zfill(3) for xx in x))


def main(paths):
    Cuba(
        source_url="https://salud.msp.gob.cu/category/covid-19/page",
        location="Cuba",
    ).to_csv(paths) 

