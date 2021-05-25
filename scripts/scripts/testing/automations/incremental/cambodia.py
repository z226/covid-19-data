import os

import pandas as pd
import requests
from datetime import date

def main():

    data = pd.read_csv("automated_sheets/Cambodia.csv")

    url = 'http://cdcmoh.gov.kh/'
    req = requests.get(url)
    text = req.text

    count = int(text.split('ááá¸ááá¶áááááá¸ááááááááá½áâ ')[1].split(' ')[0])

    date_str = date.today().strftime("%Y-%m-%d")

    if count > data["Cumulative total"].max() and date_str > data["Date"].max():

        new = pd.DataFrame({
            'Country': 'Cambodia',
            'Date': [date_str],
            'Cumulative total': count,
            'Source URL': url,
            'Source label': 'CDCMOH',
            'Units': 'tests performed',
        })

    df = pd.concat([new, data], sort=False)
    df.to_csv("automated_sheets/Cambodia.csv", index=False)

if __name__ == '__main__':
    main()
