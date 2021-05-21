import os

import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import date

url = 'https://www.covid19.gov.la/index.php'
#Only works when making two requests. The first always returns an error.
try:
    data = BeautifulSoup(requests.get(url).text, features="lxml")
except:
    data = BeautifulSoup(requests.get(url).text, features="lxml")

stats = data.find_all('p')
count = int(stats[11].text.split(' ')[0].replace(',', ''))

date_str = date.today().strftime("%Y-%m-%d")
df = pd.DataFrame({
    'Country': 'Laos',
    'Date': [date_str],
    'Cumulative total': count,
    'Source URL': url,
    'Source label': 'Government of Laos',
    'Units': 'people tested'
})

output_file = 'automated_sheets/Laos.csv'
if os.path.isfile(output_file):
    existing = pd.read_csv(output_file)
    df = pd.concat([df, existing]).sort_values('Date', ascending=False)

df.to_csv(output_file, index=False)
