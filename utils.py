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


import pandas as pd

def removeListDuplicates(myList):
    return list(set(myList))


def convertColumnToList(dfColumn):
    rawList = dfColumn.tolist()
    finalList = []
    for i in rawList:
        if i is not None and not isinstance(i, float):
            t = i.splitlines()

            finalList.extend(t)

    return finalList


def findAutos(autoDF, actName):
    copyDF = autoDF.copy()
    copyDF = copyDF.fillna('')

    foundAutosDF = copyDF.query(f'Activities.str.contains("{actName}")')

    return convertColumnToList(foundAutosDF['Automation Name'])


def getTargetDE(sourceDE, name, targs):
    filterRow = sourceDE[sourceDE['Name']
                         == name.strip()].reset_index(drop=True)

    for i in range(len(filterRow)):
        targs.append(filterRow['Target Data Extension'].loc[i])

    return targs


def getReferenceDE(sourceDE, name, refs):
    filterRow = sourceDE[sourceDE['Name']
                         == name.strip()].reset_index(drop=True)

    for i in range(len(filterRow)):
        refs.extend(
            filterRow['Reference Data Extensions'].loc[i].splitlines())

    return refs


def getActListForDE(deName, actDE, searchColumn, nameCol):
    copyDF = actDE.copy()
    copyDF = copyDF.fillna('')
    copyDF.columns = [c.replace(' ', '_') for c in copyDF.columns]

    foundAutosDF = copyDF.query(
        f'{searchColumn.replace(" ", "_")}.str.contains("{deName}")')

    return convertColumnToList(foundAutosDF[nameCol.replace(' ', '_')])


def addLabeltoListElement(myList, label):
    newList = []
    for i in myList:
        newList.append(i + ' ' + label)

    return newList


def findDERefsInActs(deName, importDE, queryDE, scriptsDE, extractDF):
    importList = getActListForDE(
        deName, importDE, 'Target Data Extension', "Name")

    qtList = getActListForDE(
        deName, queryDE, 'Target Data Extension', 'Name')

    qrList = getActListForDE(
        deName, queryDE, 'Reference Data Extensions', 'Name')

    scriptList = getActListForDE(
        deName, scriptsDE, 'Reference Data Extensions', 'Name')

    extractList = getActListForDE(
        deName, extractDF, 'Reference Data Extensions', 'Name')

    return importList, qtList, qrList, scriptList, extractList