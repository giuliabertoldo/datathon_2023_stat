# import the streamlit library
import streamlit as st
import pandas as pd
import numpy as np


#read csv files
artists = pd.read_csv(r'artists.csv')
apprenticeship = pd.read_csv(r'appr.csv')


df = pd.DataFrame(artists)
name1=df['name']


#give a title to the app
st.title('Relation between Artists')


option1 = st.selectbox(
    'Choose the artist:',
    name1)

str(option1)





if(st.button('Find')):
    try:

        #find students
        l=[]
        findId = [artists[artists['name']==option1]['id'].iloc[0]]

        i=0
        while i < len(apprenticeship):
            if apprenticeship['teacher_id'][i]==findId[0]:
                l = l + [apprenticeship['student_id'][i]]
            else:
                pass
            i = i + 1
    
        l = [int(x) for x in l]
        final=[]
        for j in l:
            art_new=artists.to_numpy()
            final = final + [artists[artists['id']==j]['name'].iloc[0]]
        s = ''
        if final == []:
            st.error('No students')
        else:
            for i in final:
                s += "- " + i + "\n"
            st.write('Students:')
            s

        #find professors
        l2=[]
        findId2 = [artists[artists['name']==option1]['id'].iloc[0]]

        i=0
        while i < len(apprenticeship):
            if apprenticeship['student_id'][i]==findId2[0]:
                l2 = l2 + [apprenticeship['teacher_id'][i]]
            else:
                pass
            i = i + 1
    
        l2 = [int(x) for x in l2]
        final2=[]
        for j in l2:
            final2 = final2 + [artists[artists['id']==j]['name'].iloc[0]]
        s2 = ''
        if final2 == []:
            st.error('No teachers')
        else:
            for i in final2:
                s2 += "- " + i + "\n"
            st.write('Teachers:')
            s2
    except:
        pass
