import pandas as pd


# Read data
artists = pd.read_csv("data/Artist.csv")

# name: lowercase
artists['name'] = artists['name'].apply(str.lower)

# Remove leading and trailing whitespaces
artists['name'] = artists['name'].str.strip()

# Remove (after)
artists['name']= artists['name'].str.replace('(after)', '')

# Remove round brackets
artists['name']= artists['name'].str.replace('(', '')
artists['name']= artists['name'].str.replace(')', '')

# Remove leading and trailing whitespaces
artists['name'] = artists['name'].str.strip()

# create last_name variables 
for i in range(1, 6, 1):
    artists.loc[artists['name'].str.split().str.len() == i, 'last name'] = artists['name'].str.split().str[-1]
# check that no null values are in last_name
sum(artists['last name'].isnull())

# which rows have same last_name?
duplicated = artists[artists['last name'].duplicated()]

# keep only one artist with specified last name
artists = artists.drop_duplicates(subset='last name')


 # Save in csv file 
artists.to_csv('cleaned_df.csv')


