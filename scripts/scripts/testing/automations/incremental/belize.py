import os
import pandas as pd
from bs4 import BeautifulSoup

from datetime import date
import urllib.request


url = 'https://sib.org.bz/covid-19/by-the-numbers/'
req = urllib.request.urlopen(url)
soup = BeautifulSoup(req.read(), 'html.parser')

stats = soup.select('div.stats-number.ult-responsive')
count = int(stats[0]['data-counter-value'])
print(count)

date = date.today().strftime("%Y-%m-%d")
df = pd.DataFrame({'Country': 'Belize',
                   'Date': [date],
                   'Cumulative total': count,
                   'Source URL': url,
                   'Source label': 'Government of Suriname',
                   'Units': 'unclear'})

output_file = 'automated_sheets/Belize.csv'
if os.path.isfile(output_file):
    existing = pd.read_csv(output_file)
    df = pd.concat([df, existing]).sort_values('Date', ascending=False)

df.to_csv(output_file, index=False)
