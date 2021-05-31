import os
from datetime import date

import requests
import pandas as pd
from bs4 import BeautifulSoup


def main():
    url = 'https://covid19.gov.kn/stats2.php'
    location = "Saint Kitts and Nevis"
    output_file = f'automated_sheets/{location}.csv'

    req = requests.get(url)
    soup = BeautifulSoup(req.text, 'html.parser')
    stats = soup.find_all('td')
    count = int(stats[7].text)
    # print(count)

    date_str = date.today().strftime("%Y-%m-%d")
    df = pd.DataFrame({
        'Country': location,
        'Date': [date_str],
        'Cumulative total': count,
        'Source URL': url,
        'Source label': 'Government of Saint Kitts and Nevis',
        'Units': 'unclear'
    })

    if os.path.isfile(output_file):
        existing = pd.read_csv(output_file)
        df = pd.concat([df, existing]).sort_values('Date', ascending=False)
    df.to_csv(output_file, index=False)


if __name__=="__main__":
    main()