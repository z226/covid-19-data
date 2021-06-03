import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import date
import urllib
import json
def main():
    url_original = 'https://www.gob.pe/busquedas?categoria[]=6-salud&contenido[]=noticias&institucion[]=minsa&sheet=1&sort_by=recent&term=ascienden&tipo_noticia[]=3-comunicado'
    req = urllib.request.urlopen(url_original)
    soup = BeautifulSoup(req.read(), 'html.parser')
    results = json.loads(soup.decode('utf-8').split('initialData=')[1].split('</script>')[0])
    result = results['data']['attributes']['results'][0]
    url = 'https://www.gob.pe'+BeautifulSoup(result['url'], 'html.parser').find('a')['href']
    req = requests.get(url)
    soup = BeautifulSoup(req.text, 'html.parser')
    count = int(soup.find_all('strong')[1].text.replace(' ',''))
    print(count)

    date_str = str(date.today())
    new = pd.DataFrame({'Country': 'Peru',
                       'Date': [date_str],
                       'Cumulative total': count,
                       'Source URL': url,
                       'Source label': 'Ministerio de Salud',
                       'Units': 'people tested'})

    existing = pd.read_csv('automated_sheets/peru.csv')
    df = pd.concat([new,existing]).sort_values('Date',ascending=False)
    df.to_csv('automated_sheets/peru.csv',index=False)
if __name__ == '__main__':
    main()
