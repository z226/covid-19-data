import pandas as pd


class OxCGRTETL:
    def __init__(self) -> None:
        self.source_url = "https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker/master/data/OxCGRT_latest.csv"

    def extract(self):
        return pd.read_csv(
            self.source_url,
            usecols=[
                "CountryName",
                "Date",
                "C1_School closing",
                "C2_Workplace closing",
                "C3_Cancel public events",
                "C4_Restrictions on gatherings",
                "C5_Close public transport",
                "C6_Stay at home requirements",
                "C7_Restrictions on internal movement",
                "C8_International travel controls",
                "E1_Income support",
                "E2_Debt/contract relief",
                "E3_Fiscal measures",
                "E4_International support",
                "H1_Public information campaigns",
                "H2_Testing policy",
                "H3_Contact tracing",
                "H4_Emergency investment in healthcare",
                "H5_Investment in vaccines",
                "H6_Facial Coverings",
                "H7_Vaccination policy",
                "StringencyIndex",
                "ContainmentHealthIndex"
            ]
        )

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def load(self, df: pd.DataFrame, output_path: str):
        df.to_csv(output_path, index=False)

    def run(self, output_path: str):
        df = self.extract()
        self.load(df, output_path)


def run_etl(output_path: str):
    etl = OxCGRTETL()
    etl.run(output_path)
