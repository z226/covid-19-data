import os
from datetime import date
import urllib.request

import pandas as pd
from bs4 import BeautifulSoup

url = 'https://www.gouv.bj/coronavirus/'
req = urllib.request.urlopen(url)
soup = BeautifulSoup(req.read(), 'html.parser')

stats = soup.find_all('h2', attrs={'class', 'h1 adapt white regular'})

count = int(stats[0].text) + int(stats[1].text)
date_str = date.today().strftime("%Y-%m-%d")
df = pd.DataFrame({
    'Country': 'Benin',
    'Date': [date_str],
    'Cumulative total': count,
    'Source URL': url,
    'Source label': 'Government of Benin',
    'Units': 'unclear',
})

output_file = 'automated_sheets/Benin.csv'
if os.path.isfile(output_file):
    existing = pd.read_csv(output_file)
    df = pd.concat([df, existing]).sort_values('Date', ascending=False)

df.to_csv(output_file, index=False)