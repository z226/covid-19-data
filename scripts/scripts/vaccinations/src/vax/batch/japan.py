from datetime import datetime

import pandas as pd
import numpy as np

vaccine_mapping = {
    "ファイザー社": "Pfizer/BioNTech",
    "武田/モデルナ社": "Moderna",
}


class Japan:
    def __init__(self, source_url_health: str, source_url_general: str,
                 source_url_ref: str, location: str):
        self.source_url_health = source_url_health
        self.source_url_general = source_url_general
        self.source_url_ref = source_url_ref
        self.location = location

        self.column_mapping = {
            'Unnamed: 0': 'date',
        }

        def _comma_merger(values):
            ret = []
            for v in values:
                ret += v.split(', ')
            return ', '.join(set(ret))

        self.merge_data_aggregators = {
            'source_url': _comma_merger,
        }

        for japanese_name, english_name in vaccine_mapping.items():
            self.column_mapping[
                japanese_name] = 'people_vaccinated_' + english_name
            self.column_mapping[
                japanese_name +
                '.1'] = 'people_fully_vaccinated_' + english_name
            self.merge_data_aggregators['people_vaccinated_' +
                                        english_name] = sum
            self.merge_data_aggregators['people_fully_vaccinated_' +
                                        english_name] = sum

    def read(self):
        df_health_early = self._health_early_data()
        df_health = self._parse_data(self.source_url_health, skiprows=3)
        df_general = self._parse_data(self.source_url_general, skiprows=4)
        return self._merge_data(self._merge_data(df_health_early, df_health),
                                df_general)

    def _health_early_data(self):
        # Early health data until Apr/09 exists on a separate HTML page.
        # Hardcode the data since it is no longer updating.
        raw_data = [(2, 17, 125, 0), (2, 18, 486, 0), (2, 19, 4428, 0),
                    (2, 22, 6895, 0), (2, 24, 5954, 0), (2, 25, 4008, 0),
                    (2, 26, 6634, 0), (3, 1, 3255, 0), (3, 2, 2987, 0),
                    (3, 3, 2531, 0), (3, 4, 1871, 0), (3, 5, 7295, 0),
                    (3, 8, 24327, 0), (3, 9, 36762, 0), (3, 10, 41357, 35),
                    (3, 11, 31826, 408), (3, 12, 46453, 2905),
                    (3, 15, 55204, 4529), (3, 16, 67446, 1470),
                    (3, 17, 73352, 4942), (3, 18, 67217, 4000),
                    (3, 19, 63041, 7092), (3, 22, 70115, 3748),
                    (3, 23, 43801, 2627), (3, 24, 38731, 3323),
                    (3, 25, 31571, 2371), (3, 26, 43993, 3754),
                    (3, 29, 44039, 23754), (3, 30, 27242, 31827),
                    (3, 31, 24213, 28795), (4, 1, 16156, 31217),
                    (4, 2, 20026, 26560), (4, 5, 43297, 56889),
                    (4, 6, 39420, 52262), (4, 7, 40371, 64171),
                    (4, 8, 29956, 64542), (4, 9, 35313, 69598)]
        date_col = []
        people_vaccinated_col = []
        people_fully_vaccinated_col = []
        for month, day, dose1, dose2 in raw_data:
            date_col.append(datetime(2021, month, day))
            people_vaccinated_col.append(dose1)
            people_fully_vaccinated_col.append(dose2)

        df = pd.DataFrame()
        df['date'] = date_col
        df['people_vaccinated_Pfizer/BioNTech'] = people_vaccinated_col
        df['people_fully_vaccinated_Pfizer/BioNTech'] = people_fully_vaccinated_col
        df['people_vaccinated_Moderna'] = 0
        df['people_fully_vaccinated_Moderna'] = 0
        df['source_url'] = 'https://www.mhlw.go.jp/stf/seisakunitsuite/bunya/vaccine_sesshujisseki.html'
        return df

    def _parse_data(self, source: str, skiprows: int) -> pd.Series:

        df = pd.read_excel(source, usecols="A,D:G", skiprows=skiprows)

        # Sanity checks
        assert [*df.columns
                ] == ['Unnamed: 0'] + list(vaccine_mapping.keys()) + [
                    x + '.1' for x in vaccine_mapping.keys()
                ], "Columns are not as expected. Unknown field detected."

        # Select the correct subregion with time series.
        df = df.iloc[1:][::-1]
        while type(df.iloc[0, 0]) is not datetime:
            df = df.iloc[1:]

        return df.pipe(self.pipe_rename_columns).pipe(self.pipe_source)

    def _merge_data(self, df_health: pd.DataFrame,
                    df_general: pd.DataFrame) -> pd.DataFrame:
        return pd.concat([
            df_health.sort_values(by='date'),
            df_general.sort_values(by='date')
        ]).groupby('date').agg(
            self.merge_data_aggregators).sort_values(by='date').reset_index()

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.column_mapping)

    def pipe_location(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location)

    def pipe_source(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(source_url=self.source_url_ref)

    def pipe_assign_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        def _get_vaccine(data: tuple) -> str:
            vaccines = []
            _, ds = data
            for vaccine_name in vaccine_mapping.values():
                if ds['people_vaccinated_' + vaccine_name] + ds[
                        'people_fully_vaccinated_' + vaccine_name] > 0:
                    vaccines.append(vaccine_name)
            return ', '.join(vaccines)

        df['vaccine'] = [_get_vaccine(row) for row in df.iterrows()]
        return df

    def pipe_calc_row_data(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            people_vaccinated=lambda ds: sum(ds['people_vaccinated_' + x] for x
                                             in vaccine_mapping.values()),
            people_fully_vaccinated=lambda ds: sum(
                ds['people_fully_vaccinated_' + x]
                for x in vaccine_mapping.values())).assign(
                    total_vaccinations=lambda ds: ds.people_vaccinated + ds.
                    people_fully_vaccinated)

    def pipe_cumsum(self, df: pd.DataFrame) -> pd.DataFrame:
        df['people_vaccinated'] = df['people_vaccinated'].cumsum()
        df['people_fully_vaccinated'] = df['people_fully_vaccinated'].cumsum()
        df['total_vaccinations'] = df['total_vaccinations'].cumsum()
        return df

    def pipe_separate_manufacturer(self, df: pd.DataFrame) -> pd.DataFrame:
        df_by_vaccine = []
        reset_assign = {}
        for vaccine_name in vaccine_mapping.values():
            reset_assign['people_vaccinated_' + vaccine_name] = 0
            reset_assign['people_fully_vaccinated_' + vaccine_name] = 0
        for vaccine_name in vaccine_mapping.values():
            assign = reset_assign.copy()
            del assign['people_vaccinated_' + vaccine_name]
            del assign['people_fully_vaccinated_' + vaccine_name]
            df_by_vaccine.append(df.copy().assign(**assign).pipe(
                self.pipe_calc_row_data).pipe(self.pipe_cumsum))
        return pd.concat(df_by_vaccine)

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.pipe_calc_row_data).pipe(self.pipe_location).pipe(
            self.pipe_assign_vaccine).pipe(self.pipe_cumsum)[[
                'date', 'total_vaccinations', 'people_vaccinated',
                'people_fully_vaccinated', 'location', 'source_url', 'vaccine'
            ]]

    def pipeline_manufacturer(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.pipe_separate_manufacturer).pipe(
            self.pipe_location).pipe(self.pipe_assign_vaccine)[[
                'date',
                'location',
                'vaccine',
                'total_vaccinations',
            ]].sort_values(
                by=['date', 'vaccine']).query('total_vaccinations > 0')

    def to_csv(self, paths):
        df = self.read()
        df.copy().pipe(self.pipeline).to_csv(paths.tmp_vax_out(self.location),
                                             index=False,
                                             float_format='%.f')
        df.copy().pipe(self.pipeline_manufacturer).to_csv(
            paths.tmp_vax_out_man(f"{self.location}"),
            index=False,
            float_format='%.f')


def main(paths):
    Japan(
        source_url_health=
        "https://www.kantei.go.jp/jp/content/IRYO-vaccination_data3.xlsx",
        source_url_general=
        "https://www.kantei.go.jp/jp/content/KOREI-vaccination_data3.xlsx",
        source_url_ref=
        "https://www.kantei.go.jp/jp/headline/kansensho/vaccine.html",
        location="Japan",
    ).to_csv(paths)


if __name__ == "__main__":
    main()
