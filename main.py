# Copyright (c) Streamlit Inc. (2018-2022) Snowflake Inc. (2022)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import streamlit as st
from utils import *
# from streamlit.logger import get_logger

def findAutoDERefsandTargs(activities):
    actList = convertColumnToList(activities['Activities'])
    refs = []
    targs = []

    for act in actList:
        if act.find('(Extract)') != -1:
            refs = getReferenceDE(
                extractDF, act.replace('(Extract)', ''), refs)
        elif act.find('(Import)') != -1:
            targs = getTargetDE(
                importsDF, act.replace('(Import)', ''), targs)
        elif act.find('(Transfer)') != -1:
            st.write('None')
        elif act.find('(Script)') != -1:
            refs = getReferenceDE(
                scriptsDF, act.replace('(Script)', ''), refs)
        else:
            refs = getReferenceDE(queriesDF, act, refs)
            targs = getTargetDE(queriesDF, act, targs)

    return removeListDuplicates(refs), removeListDuplicates(targs)

st.write("# SalseForce Lookup")

queriesDF = pd.read_csv('./data/queries.csv', index_col=0)
autosDF = pd.read_csv('./data/automations.csv', index_col=0)
importsDF = pd.read_csv('./data/import.csv', index_col=0)
scriptsDF = pd.read_csv('./data/scripts.csv', index_col=0)
extractDF = pd.read_csv('./data/data_extract.csv', index_col=0)

lookupSource = st.selectbox(
    'What would you like to lookup?',
    ['Automation', 'Data Extension', 'Query', 'Script', 'Import', 'Extract'])

name = st.text_input(f'{lookupSource} name')

if st.button('Search'):

    if lookupSource == 'Automation':
        autoRow = autosDF[autosDF['Automation Name'] == name]

        refList, targList = findAutoDERefsandTargs(autoRow)

        autoInfoData = {"Automation Name": name,
                        "Location": autoRow['Path'], "Status": autoRow['Status'], 'Activities': autoRow['Activities'],
                        'Target Data Extensions': '\n'.join(targList), 'Reference Data Extensions': '\n'.join(refList)}
        autoInfoDF = pd.DataFrame(autoInfoData)

        autoInfoDF

    elif lookupSource == 'Query':
        queryRow = queriesDF[queriesDF['Name'] == name]

        autoList = findAutos(autosDF, name)

        queryInfoData = {'Query Name': name, 'Type': queryRow['Update Type'], 'Target Data Extension': queryRow['Target Data Extension'],
                         'Reference Data Extensions':  queryRow['Reference Data Extensions'], 'Automantions Used': '\n'.join(autoList)}

        queryInfoDF = pd.DataFrame(queryInfoData)

        queryInfoDF

    elif lookupSource == 'Script':
        scriptRow = scriptsDF[scriptsDF['Name'] == name]

        autoList = findAutos(autosDF, name)

        scriptInfoData = {'Script Name': name, 'Reference Data Extensions':
                          scriptRow['Reference Data Extensions'], 'Automantions Used': '\n'.join(autoList)}

        scriptInfoDF = pd.DataFrame(scriptInfoData)

        scriptInfoDF

    elif lookupSource == 'Extract':
        extractRow = extractDF[extractDF['Name'] == name]

        autoList = findAutos(autosDF, name)

        extractInfoData = {'Extract Name': name, 'Reference Data Extension':
                           extractRow['Reference Data Extensions'], 'Automantions Used': '\n'.join(autoList)}

        extractInfoDF = pd.DataFrame(extractInfoData)

        extractInfoDF

    elif lookupSource == 'Import':
        importRow = importsDF[importsDF['Name'] == name]

        autoList = findAutos(autosDF, name)

        importInfoData = {'Import Name': name, 'Target Data Extension':
                          importRow['Target Data Extension'], 'Automantions Used': '\n'.join(autoList)}

        importInfoDF = pd.DataFrame(importInfoData)

        importInfoDF

    elif lookupSource == 'Data Extension':
        importList, qtList, qrList, scriptList, extractList = findDERefsInActs(
            name, importsDF, queriesDF, scriptsDF, extractDF)

        actList = importList + qtList + qrList + scriptList + extractList

        autoList = []
        for act in actList:
            autoList.extend(findAutos(autosDF, act))

        autoList = removeListDuplicates(autoList)

        deData = {'Data Extension Name': name, 'Automations Featured': '\n'.join(autoList), 'Reference Queries': '\n'.join(qrList),
                  'Target Queries': '\n'.join(qtList), 'Imports': '\n'.join(importList), 'Scripts': '\n'.join(scriptList), 'Extract': '\n'.join(extractList)}

        deInfoDF = pd.DataFrame(deData,  index=[0])

        deInfoDF