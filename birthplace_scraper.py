import requests
from bs4 import BeautifulSoup
import pandas as pd
import spacy
# python -m spacy download en_core_web_lg
# python -m spacy download en_core_web_sm


var = ['Vincent_van_Gogh', 'Claude_Monet']

main_list = []

for v in var: 
    url = 'https://en.wikipedia.org/wiki/'+ v
    
    r = requests.get(url)
    html_contents = r.text
    html_soup = BeautifulSoup(html_contents, 'html.parser')
    div_birth = html_soup.find_all('div', class_='birthplace')
    for d in div_birth: 
        location = d.text
    name = v
    location = location.replace(',', '')
    city = location.split()[0]
    country = location.split()[1]
    # Store list 
    locations = [name, location, city, country]
    main_list.append(locations)

print(main_list)

nlp = spacy.load('en_core_web_sm')




# Only with one
import geonamescache
gc = geonamescache.GeonamesCache()
var = 'Vincent_van_Gogh'
url = 'https://en.wikipedia.org/wiki/'+ var
    
r = requests.get(url)
html_contents = r.text
html_soup = BeautifulSoup(html_contents, 'html.parser')
div_birth = html_soup.find_all('div', class_='birthplace')
for d in div_birth: 
    location = d.text
name = var
location = location.replace(',', '')


import geonamescache

gc = geonamescache.GeonamesCache()
countries = gc.get_countries()
# print countries dictionary
