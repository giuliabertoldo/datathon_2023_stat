import pandas as pd
from requests_html import HTMLSession
import requests
from bs4 import BeautifulSoup

def glimpse(df):
    print(f"Rows: {df.shape[0]}")
    print(f"Columns: {df.shape[1]}")
    for col in df.columns:
        print(f"$ {col} <{df[col].dtype}> {df[col].head().values}")


# Read data
artists = pd.read_csv("data/Artist.csv")

########################################
# DATA CLEANING ########################
########################################

# names: lowercase, delete (after)
artists['name'] = artists['name'].apply(str.lower)

# check all unique values in names
artists['name'].unique()
artists['name'].count()

# some names contain (after): delete it and count how many rows with same name 
after = artists[artists['name'].str.contains('after')]
after.count()
## Create a new dataframe
artists_mod = artists.copy()

## Remove leading and trailing whitespaces
artists_mod['name'] = artists_mod['name'].str.strip()

## Remove (after)
artists_mod['name']= artists_mod['name'].str.replace('(after)', '')

## Remove ()
artists_mod['name']= artists_mod['name'].str.replace('(', '')
artists_mod['name']= artists_mod['name'].str.replace(')', '')
## Remove leading and trailing whitespaces
artists_mod['name'] = artists_mod['name'].str.strip()

## Replace whitespaces in names with underscores
artists_mod['name']= artists_mod['name'].str.replace(' ', '_')

## Replace - in names with underscores
artists_mod['name']= artists_mod['name'].str.replace('-', '_')

## How many duplicate rows?
sum(artists_mod.duplicated())

## TODO: Fix that françois_boucher and francois_boucher are different people now + others?

########################################
# BIRTHPLACES ##########################
########################################

# How many artists per birthplace?
artists_mod.groupby(['birthplace'])['birthplace'].count().sort_values(ascending=False)

# Who are the 32 artists in location 2?
loc2 = artists_mod.loc[artists_mod['birthplace'] == 2]
glimpse(loc2)
print(loc2['name'])


########################################
# WEBSCRAPING ##########################
########################################

import requests
from bs4 import BeautifulSoup
import pandas as pd
from requests_html import HTMLSession
import spacy

# FUNCTIONS #############################################
# Function to scrape paragraphs of wikipedia page 
def scrape_wiki_page(user_url):
    session = HTMLSession()
    r = session.get(user_url)
    html = r.html
    text = ''
    for item in html.find('p'):
        text = text + item.text
    return text 

# Function to extract sentences containing "was born"
def extract_word(text):
    words = ['was born']
    sentences = text.split('.')
    for sentence in sentences:
        if any(word in sentence for word in words):
            yield sentence


#  Apply function Example with 11 artists 
wikis = []
pages = artists_mod['name'][0:10]
for page in pages:
    url = 'https://en.wikipedia.org/wiki/'+ page
    text = scrape_wiki_page(url)
    w = {'page': page,
    'url': url, 
    'text': text}
    wikis.append(w)

# Convert list to dataframe
wikis = pd.DataFrame(wikis)

# Check if all rows contain "was born"
pattern = "was born"
wikis['text'].str.contains(pattern, case = False)

# Create dataframe with artist name and sentence "was born"
name_sentence = []
row_number = -1

for t in wikis['text']:
    row_number = row_number + 1 
    name = wikis['page'][row_number] 
    sentence = list(extract_word(wikis['text'][row_number]))[0]
    s = {'name': name, 
    'sentence':sentence}
    name_sentence.append(s)

print(name_sentence)

# Convert list to dataframe
df_name_sentence = pd.DataFrame(name_sentence)


# Extract location names from sentences
# https://stackoverflow.com/questions/47793516/finding-city-names-in-string
nlp = spacy.load('en_core_web_lg')
i = -1 
locations = []
for ns in df_name_sentence['sentence']:
    i = i+1
    name = df_name_sentence['name'][i]
    doc = nlp(df_name_sentence['sentence'][i])
    for ent in doc.ents:
        if ent.label_ == 'GPE':
            o = {'name': name, 
            'GPE': ent.text}
            locations.append(o)

# Convert locations to dataframe
df_locations = pd.DataFrame(locations)
print(df_locations)

# Save dataframe as csv 
df_locations.to_csv("locations.csv")

# FOR NEXT #########################################################
# Load locations as dataframe
locations_df = pd.read_csv("locations.csv")
glimpse(locations_df)
locations_df.head()
# Merge locations and artists df
df_merged =  pd.concat([artists_mod, locations_df], axis=1, join = "inner")

# 

