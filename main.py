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

var = artists_mod['name'][0:10]

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

# convert list of lists in dataframe 
data_transposed = zip(main_list)
df = pd.DataFrame(data_transposed, columns=["name", "location", "city", "country"])

# Save dataframe as csv 
df.to_csv("locations.csv")


# Load locations as dataframe
locations_df = pd.read_csv("locations.csv")
glimpse(locations_df)
locations_df.head()
# Merge locations and artists df
df_merged =  pd.concat([artists_mod, locations_df], axis=1, join = "inner")

# 

