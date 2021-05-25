import os
import pandas as pd
from bs4 import BeautifulSoup

from datetime import date
import urllib.request

def main():
    
    data = pd.read_csv("automated_sheets/Belize.csv")

    url = 'https://sib.org.bz/covid-19/by-the-numbers/'
    req = urllib.request.urlopen(url)
    soup = BeautifulSoup(req.read(), 'html.parser')

    stats = soup.select('div.stats-number.ult-responsive')
    count = int(stats[0]['data-counter-value'])
    print(count)

    date = str(date.today())

    if count > data["Cumulative total"].max() and date > data["Date"].max():
        
        new = pd.DataFrame({'Country': 'Belize',
            'Date': [date],
            'Cumulative total': count,
            'Source URL': url,
            'Source label': 'Statistical Institute of Belize',
            'Units': 'tests performed',
        })

    df = pd.concat([new, data], sort=False)
    df.to_csv("automated_sheets/Belize.csv", index=False)

if __name__ == '__main__':
    main()
