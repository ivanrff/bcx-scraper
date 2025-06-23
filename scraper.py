from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs

def parser(file_path):

    with open(file_path, 'r', encoding='utf-8') as f:
        html = f.read()

    bs = BeautifulSoup(html, 'html.parser')

    numero_cemercante = bs.find('div', id='layout').find('h1').find('a').get('href')
    params = parse_qs(urlparse(numero_cemercante).query)
    numero_cemercante = params.get('nrCE', [None])[0]
    print(numero_cemercante)


parser('items.html')