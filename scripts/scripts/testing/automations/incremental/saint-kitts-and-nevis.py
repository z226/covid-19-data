import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import date
import urllib

url = 'https://covid19.gov.kn/stats2.php'
req = requests.get(url)
soup = BeautifulSoup(req.text, 'html.parser')

stats = soup.find_all('td')
count = int(stats[7].text)
print(count)

date = str(date.today())
new = pd.DataFrame({'Country': 'Saint Kitts and Nevis',
                   'Date': [date],
                   'Cumulative total': count,
                   'Source URL': url,
                   'Source label': 'Government of Saint Kitts and Nevis',
                   'Units': 'unclear'})

existing = pd.read_csv('automated_sheets/saint-kitts-and-nevis.csv')
df = pd.concat([new,existing]).sort_values('Date',ascending=False)
df.to_csv('automated_sheets/saint-kitts-and-nevis.csv',index=False)
