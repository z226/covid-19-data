import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime

url = 'https://stopcov.ge'
req = requests.get(url)
soup = BeautifulSoup(req.text, 'html.parser')

daily = data.find_all('span',attrs={'class':'quantity-numver'}[6])
daily = daily.text.replace(' ','')

date = str(date.today())

new = pd.DataFrame({'Country': 'Georgia',
                   'Date': [date],
                   'Daily change in cumulative total':daily,
                   'Source URL': url,
                   'Source label': 'Government of Georgia',
                   'Units': 'unclear'})
existing = pd.read_csv('automated_sheets/Georgia.csv')
df = pd.concat([new,existing]).sort_values('Date',ascending=False)
df.to_csv('automated_sheets/Georgia.csv',index=False)
