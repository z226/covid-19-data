import tempfile
import re

import requests
import pandas as pd
import PyPDF2

from vax.utils.incremental import merge_with_current_data, clean_count
from vax.utils.utils import get_soup
from vax.utils.dates import clean_date


class Gambia:

    def __init__(self, source_url: str, location: str):
        self.source_url = source_url
        self.location = location
        self._num_links_max = 3

    def read(self, last_update) -> pd.Series:
        links = self.get_pdf_links(self.source_url)
        # print("--------------------")
        # print(links[:self._num_links_max])
        # print("--------------------")
        data = []
        for link in links[:self._num_links_max]:
            _data = self.parse_data_pdf(link)
            if _data["date"] <= last_update:
                break
            data.append(_data)
        df = pd.DataFrame(data)
        return df

    def get_pdf_links(self, source) -> list:
        soup = get_soup(source, verify=False)
        links = soup.find_all(class_="wp-block-file")
        return [link.a.get("href") for link in links]

    def parse_data_pdf(self, link) -> dict:
        text = self._get_pdf_text(link)

        regex = (
            r"([\d,]+) are already vaccinated against COVID-19 as of (\d{1,2})(?:th|nd|st|rd) ([a-zA-Z]+) (202\d)"
        )
        match = re.search(regex, text)
        if match:
            people_vaccinated = clean_count(match.group(1))
            date_raw = " ".join(match.group(2, 3, 4))
            date_str = str(pd.to_datetime(date_raw).date())

            regex = r"([\d,]+) people have received their 2nd dose"
            people_fully_vaccinated = re.search(regex, text).group(1)
            people_fully_vaccinated = clean_count(people_fully_vaccinated)
            total_vaccinations = people_vaccinated + people_fully_vaccinated
        else:
            regex = (
                r"([\d,]+) and ([\d,]+) people received the 1st and 2nd doses respectively bringing the total number "
                r"vaccinated against COVID-19 to ([\d,]+) as of (\d{1,2})(?:th|nd|st|rd) ([a-zA-Z]+) (202\d)"
            )
            match = re.search(regex, text)
            if match:
                people_fully_vaccinated = clean_count(match.group(2))
                people_vaccinated = clean_count(match.group(3))
                total_vaccinations = people_vaccinated + people_fully_vaccinated
                date_raw = " ".join(match.group(4, 5, 6))
                date_str = clean_date(date_raw, "%d %B %Y", lang="en")
            else:
                regex = (
                    r"As of (\d{1,2})(?:th|nd|st|rd) ([a-zA-Z]+) (202\d), ([\d,]+) and ([\d,]+) "
                    r"people received the 1st and 2nd doses of AstraZeneca Vaccine respectively, "
                    r"bringing the total number ever vaccinated to ([\d,]+)"
                )
                match = re.search(regex, text)
                people_fully_vaccinated = clean_count(match.group(5))
                people_vaccinated = clean_count(match.group(6))
                total_vaccinations = people_vaccinated + people_fully_vaccinated
                date_raw = " ".join(match.group(1, 2, 3))
                date_str = clean_date(date_raw, fmt="%d %B %Y", lang="en")
        return {
            "total_vaccinations": total_vaccinations,
            "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_fully_vaccinated,
            "date": date_str,
            "source_url": link,
        }

    def _get_pdf_text(self, url) -> str:
        with tempfile.NamedTemporaryFile() as tf:
            with open(tf.name, mode="wb") as f:
                f.write(requests.get(url, verify=False).content)
            with open(tf.name, mode="rb") as f:
                reader = PyPDF2.PdfFileReader(f)
                page = reader.getPage(0)
                text = page.extractText()
        text = text.replace("\n", "")
        return text

    def pipe_drop_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.sort_values("date")
            .drop_duplicates(
                subset=["total_vaccinations", "people_vaccinated", "people_fully_vaccinated"],
                keep="first"
            )
        )

    def pipe_location(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location)

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(vaccine="Oxford/AstraZeneca")

    def pipe_select_output_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[[
            "location",
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
            .pipe(self.pipe_drop_duplicates)
            .pipe(self.pipe_location)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_select_output_columns)
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


def main(paths):
    Gambia(
        source_url="https://www.moh.gov.gm/covid-19-report",
        location="Gambia",
    ).to_csv(paths) 


if __name__ == "__main__":
    main()
