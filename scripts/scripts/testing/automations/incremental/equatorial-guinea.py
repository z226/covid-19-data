import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import date
import urllib

url = 'https://guineasalud.org/estadisticas/'
req = requests.get(url)
soup = BeautifulSoup(req.text, 'html.parser')

stats = soup.find_all('tr')
count = int(stats[8].find_all('td')[-1].text)
print(count)

date = str(date.today())
new = pd.DataFrame({'Country': 'Equatorial Guinea',
                   'Date': [date],
                   'Cumulative total': count,
                   'Source URL': url,
                   'Source label': 'Ministerio de Sanidad y Bienestar Social',
                   'Units': 'unclear'})

existing = pd.read_csv('automated_sheets/Equatorial-Guinea.csv')
df = pd.concat([new,existing]).sort_values('Date',ascending=False)
df.to_csv('automated_sheets/Equatorial-Guinea.csv',index=False)
