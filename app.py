# import the streamlit library
import streamlit as st
import pandas as pd
import numpy as np
#import matplotlib.pyplot as plt

artists = pd.read_csv(r'artists.csv')
apprenticeship = pd.read_csv(r'appr.csv')




#artists = artists.sort_values(by='name')
#artists = artists[34:-1]
#artists = artists.sort_values(by='id')
#artists = artists.sort_values(by='name')

df = pd.DataFrame(artists)
name1=df['name']
#name1=name1.sort_values()
#name1=name1[34:]


st.title('Find the students and the teachers')


option1 = st.selectbox(
    'Choose the artist:',
    name1)

str(option1)


#option1 = "option1"


#if(st.button('Find')):
#    st.write("You selected", option1, "and", option2)

#if(st.button('Find')):
#    try:
#        l=[]
#        findId = artists[artists['name']==option1]['id'].index.tolist()

    #print(findId)
#        i=0
#        while i < len(apprenticeship):
#            if apprenticeship['teacher_id'][i]==findId[0]:
#            #print([apprenticeship['teacher_id'][i]]) 
#                l = l + [apprenticeship['student_id'][i]]
#            else:
#                pass
#            i = i + 1
#    
#       l = [int(x) for x in l]
#        final=[]
#        for j in l:
#            art_new=artists.to_numpy()
#            final = final + [art_new[j][1]]
#        s = ''
#        if final == []:
#            st.error('No students')
#        else:
#            for i in final:
#                s += "- " + i + "\n"
#            st.write('Students:')
#            s
#    except:
#        pass
    




if(st.button('Find')):
    try:
        l=[]
        findId = [artists[artists['name']==option1]['id'].iloc[0]]

        #df_temp = pd.DataFrame(artists[artists['name']==option1]['id'])
        #findId = [df_temp['id'].iloc[0]]


    #print(findId)
        i=0
        while i < len(apprenticeship):
            if apprenticeship['teacher_id'][i]==findId[0]:
                # print([apprenticeship['teacher_id'][i]]) 
                l = l + [apprenticeship['student_id'][i]]
            else:
                pass
            i = i + 1
            # print(l)
    
        l = [int(x) for x in l]
        final=[]
        for j in l:
            art_new=artists.to_numpy()
            final = final + [art_new[j][2]]
        s = ''
        if final == []:
            st.error('No students')
        else:
            for i in final:
                s += "- " + i + "\n"
            st.write('Students:')
            s

        #professors
        l2=[]
        findId2 = artists[artists['name']==option1]['id'].index.tolist()

    #print(findId)
        i=0
        while i < len(apprenticeship):
            if apprenticeship['student_id'][i]==findId2[0]:
            #print([apprenticeship['teacher_id'][i]]) 
                l2 = l2 + [apprenticeship['teacher_id'][i]]
            else:
                pass
            i = i + 1
    
        l2 = [int(x) for x in l2]
        final2=[]
        for j in l2:
            art_new2=artists.to_numpy()
            final2 = final2 + [art_new2[j][2]]
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
