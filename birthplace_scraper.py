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


#  Apply function Example with two artists 
wikis = []
pages = ['Vincent_van_Gogh','Claude_Monet']
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



