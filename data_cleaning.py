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
    artists.loc[artists['name'].str.split().str.len() == i, 'last_name'] = artists['name'].str.split().str[-1]
# check that no null values are in last_name
sum(artists['last_name'].isnull())

# which rows have same last_name?
duplicated = artists[artists['last_name'].duplicated()]

# keep only one artist with specified last_name
artists = artists.drop_duplicates(subset='last_name')


 # Save in csv file 
artists.to_csv('artists.csv')


# APPR

# Read data
appr = pd.read_csv("data/Apprenticeship.csv")

# Keep only student_id and teacher_id that are in artist id 
# Create new vector 

new_student_id = []
new_teacher_id = []

for i in range(len(appr.index)):
    if (appr['student_id'][i] and appr['teacher_id'][i]) in artists['id']:
        new_student_id.append(appr['student_id'][i])
        new_teacher_id.append(appr['teacher_id'][i])

appr = pd.DataFrame({'student_id':new_student_id, 
'teacher_id':new_teacher_id})

# Save in csv file 
appr.to_csv('appr.csv')