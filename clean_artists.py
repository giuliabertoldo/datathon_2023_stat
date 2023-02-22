import pandas as pd

# Functions 
def clean_artist_name(df): 
    # name: lowercase
    df['name'] = df['name'].apply(str.lower)

    # Remove leading and trailing whitespaces
    df['name'] = df['name'].str.strip()

    # Remove (after)
    df['name']= df['name'].str.replace('(after)', '')

    # Remove round brackets
    df['name']= df['name'].str.replace('(', '')
    df['name']= df['name'].str.replace(')', '')

    # Remove leading and trailing whitespaces
    df['name'] = df['name'].str.strip()

    # Save in csv file 
    df.to_csv('cleaned_df.csv')

# Read data
artists = pd.read_csv("data/Artist.csv")

# Clean artists
clean_artist_name(artists)





