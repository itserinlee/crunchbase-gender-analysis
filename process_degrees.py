import pandas as pd
from rich_dataframe import prettify
import pprint
import time
import pytz
from datetime import date, datetime
import pickle
import json
import sys


def programsProgress():
    zoneLA = pytz.timezone('America/Los_Angeles')
    nextTimeInstance = datetime.now(zoneLA)
    nextTimeFormatted = nextTimeInstance.strftime('%I:%M %p (%H:%M)')
    return print(f'Progress: {nextTimeFormatted}')


def writeToCSV(df: pd.DataFrame, filename: str, noIndex: bool):
    df.to_csv(f'{filename}.csv', index=not noIndex)


def processDegreeCompleted(degreesDF: pd.DataFrame):

    degreesDict = dict()
    for idx, row in degreesDF.iterrows():
        
        # keys to a dictionary is a set
        degree = cleanDegree(row['degreeAttainment'])
        if degree in degreesDict:
            degreesDict[degree] += 1
        else:
            degreesDict[degree] = 1

    with open('datasets/finalDegreeDict.json', 'w') as fp: # fp means file pointer
        json.dump(degreesDict, fp, sort_keys=True, indent=4)


def cleanDegree(degree: str) -> str:
    '''
    returns an arbitrary set of strings, representing degrees
    prioritizing higher level degrees: doctorate, then masters, then undergraduates, a few others, then unknown
    currently: dumps about 25% into unknown bc data quality was poor quality (derived from free-form text)
    '''

    degree = str(degree)\
                .replace('.', '')\
                .replace(')', '')\
                .replace('(', '')\
                .upper()
    
    
    cleanDegreesList = [('POSTDOC', 'Doctorate'),
                        ('SCHOOL OF MEDICINE MD', 'Doctorate'), ('JD', 'Doctorate'), 
                        ('RESIDENCY', 'Doctorate'), ('DMD', 'Masters'), ('MD,', 'Doctorate'),
                        ('DS', 'Doctorate'), ('DSC', 'Doctorate'), ('DTech', 'Doctorate'),
                        ('DOCTER', 'Doctorate'), ('DOC', 'Doctorate'), ('DO', 'Doctorate'),
                        ('PHD', 'Doctorate'), ('PH,D', 'Doctorate'), ('PH D', 'Doctorate'),
                        ('MBA', 'Masters'), ('MA', 'Masters'),
                        ('MSC', 'Masters'), ('MS', 'Masters'), ('MTECH', 'Masters'),
                        ('MD CANDIDATE', 'Bachelors'), ('MD SUMMER EXPERIENCE', 'Bachelors'),
                        ('BS', 'Bachelors'), ('BA', 'Bachelors'), ('BACH', 'Bachelors'), ('BE', 'Bachelors'),
                        ('BFA', 'Bachelors'), ('BOA', 'Bachelors'), ('BOE', 'Bachelors'), ('BOE', 'Bachelors'),
                        ('BTECH', 'Bachelors'), ('UUNDERGRADUATE', 'Bachelors'), ('BENG', 'Bachelors'),
                        ('AB', 'Bachelors'), ('SCB', 'Bachelors'),
                        ('THREE-YEAR DEGREE', 'Associates'), ('AA', 'Associates'), ('Associate', 'Associates'),
                        ('HIGH SCHOOL', 'High School'),
                        ('EXECUTIVE', 'Executive Program'),
                        ('UNKNOWN', 'Unknown')]


    for cleanDegree in cleanDegreesList:
        if cleanDegree[0] in degree: # cleanDegree is a tuple
            return cleanDegree[1]

    return 'Unknown'


def cleanGender(gender: str)->str:
    genderDict = {'male': 'male', 'female': 'female', 'non_binary': 'non-binary',
                    'agender': 'non-binary', 'androgynous': 'non-binary', 'two_spirit': 'non-binary', 'bigender': 'non-binary',
                    'gender_fluid': 'non-binary', 'gender_nonconforming': 'non-binary', 'ftm': 'non-binary',
                    'Unknown': 'unspecified', 'null': 'unspecified',
                    'not_provided': 'unspecified'}

    if gender in genderDict:
        return genderDict[gender]
    
    print(f'{gender} was not found.')
    return 'unspecified'


def getTotalFounders(orgDF: pd.DataFrame) -> dict:
    '''
    used in the funding events graph by processDegreesData()
    '''

    orgTotalFounders = dict()
    for idx, row in orgDF.iterrows():
        orgUuid = row['uuid']
        orgTotalFounders[orgUuid] = row['numFounders'] # numFounders is a float here

    return orgTotalFounders


def getFoundersOrgDict(orgFoundersDict: dict) -> dict:
    '''
    to avoid nested loop, making this a map
    reverses the dictionary to access founder info

    nested under the degrees graph by processDegreesData()
    '''

    foundersOrgDict = dict()
    for orgUuid, founderUuids in orgFoundersDict.items():

        for founderUuid in founderUuids:

            if founderUuid in foundersOrgDict:
                foundersOrgDict[founderUuid].append(orgUuid)
            else:
                foundersOrgDict[founderUuid] = [orgUuid]

    return foundersOrgDict


def processDegreesData(orgDF: pd.DataFrame, degreesDF: pd.DataFrame, foundersOrgDict: dict, orgFundingDict: dict, founderGenderDict: dict) ->list:
    '''
    purpose:
    this function returns 2 datasets:
        -modifies degrees dataset (oriented by founder) to stamp in 3 new columns: funding total, degree attainment, and the gender
        -creates a binned dataset to analyze degrees where row = a gender, column = a degree level, values for those row-columns = sum of money invested

    graphically, this means:
    (orgDF, fundingRoundDF) -> funding total
    (degreeDF) -> degree attainment, reduced to a set
    (peopleDF) -> gender of founder

    nested set of graphs:
    first, walks the degrees graph (to get founder id)
    second, walks the founder-org graph (to get from founder id to org)
    third, walks the org-funding graph (to get org to funding events)
    fourth, walks the funding graph (to sum up funding)
    fifth, uses orgTotalFounders graph to break up the funding, so each person only gets their fraction
    
    the outermost loop sums up the money for the gender-degree combination (in rows & columns, respectively)
    '''


    degrees = dict()
    binnedDegrees = dict() # key are the pair of (degree, gender) & the values are the aggregate money invested

    degreesDF = degreesDF.reset_index(drop = True)

    degreesDF.insert(len(degreesDF.columns), 'fundingTotal', [0] * len(degreesDF))                      # a list gets mapped vertically & put an empty list in each row of that col
    degreesDF.insert(len(degreesDF.columns), 'cleanedDegreeAttainment', ['Unknown'] * len(degreesDF))
    degreesDF.insert(len(degreesDF.columns), 'founderGender', ['unspecified'] * len(degreesDF))

    print(f'Total rows: {degreesDF.shape[0]=}')

    orgTotalFounders = getTotalFounders(orgDF)

    for idx, row in degreesDF.iterrows():
        if idx % 50000 == 0:
            # print(f'{idx=} {row["personUuid"]=}')
            programsProgress()

        totalFunding = 0
        founderUuid = row['personUuid']

        if founderUuid in foundersOrgDict:
            orgList = foundersOrgDict[founderUuid] # these are the values of org uuids

            for org in orgList:

                if org in orgFundingDict:
                    fundingList = orgFundingDict[org]

                    for funding in fundingList:
                        
                        totalFounders = orgTotalFounders[org]
                        if not (pd.isna(totalFounders) or totalFounders == 'N/A' or totalFounders == 0):
                            totalFunding += funding[1]/totalFounders
                            # print(f'{totalFunding=}')
        
        degreesDF.at[idx, 'fundingTotal'] = totalFunding
        
        degree = cleanDegree(row['degreeAttainment'])
        degreesDF.at[idx, 'cleanedDegreeAttainment'] = degree
        
        gender = 'unspecified'
        if founderUuid in founderGenderDict:
            gender = cleanGender(founderGenderDict[founderUuid])
            degreesDF.at[idx, 'founderGender'] = gender
        
        key = f'{degree}, {gender}' # this is a string not a tuple
        if key in binnedDegrees:
            binnedDegrees[key] += totalFunding
        else:
            binnedDegrees[key] = totalFunding
        
    with open('datasets/binnedDegreesDict.json', 'w') as fp: # fp means file pointer
        json.dump(binnedDegrees, fp, sort_keys=True, indent=4)
    
    genderRows = set()
    degreeColumns = set()
    for key, value in binnedDegrees.items():
        degree = key.split(', ')[0]
        gender = key.split(', ')[1]
        degreeColumns.add(degree)
        genderRows.add(gender)
    
    # pprint.pprint(degreeColumns)
    # pprint.pprint(genderRows)

    binnedDegreeDF = pd.DataFrame(index=list(genderRows), columns=list(degreeColumns))

    for key, value in binnedDegrees.items():
        degree = key.split(', ')[0]
        gender = key.split(', ')[1]
        if pd.isna(value):
            value = 0 # replace nan with 0
        binnedDegreeDF.at[gender, degree] = value # value should be money invested

    binnedDegreesScaledDF = binnedDegreeDF.copy()
    binnedDegreesScaledDF = binnedDegreesScaledDF.transpose()

    for idx, row in binnedDegreesScaledDF.iterrows():
        
        rowSum = 0
        for key, value in row.items():
            if pd.isna(value):
                row[key] = 0 # replace nan with 0
            else:
                rowSum += value
        
        for key, value in row.items():
            binnedDegreesScaledDF.at[idx, key] = value/rowSum

    return [degreesDF, binnedDegreeDF, binnedDegreesScaledDF]


if __name__ == '__main__':

    degreesDF = pd.read_pickle('datasets/degrees.csv.pickle')

    args = sys.argv[1:] # position 0 is the name of the program
    
    if len(args) > 0 and args[0] == 'degrees': # degrees parameter to make it only run this if statement
        processDegreeCompleted(degreesDF)
        exit()

    with open('datasets/founderGenderDict.pickle', 'rb') as handle:
        founderGenderDict = pickle.load(handle) # founder uuid & genders graph where founderGenderDict[uuid] = gender

    with open('datasets/orgFoundersDict.pickle', 'rb') as handle:
        orgFoundersDict = pickle.load(handle)

    foundersOrgDict = getFoundersOrgDict(orgFoundersDict)
    
    with open('datasets/fundingRoundDict.pickle', 'rb') as handle:
        orgFundingDict = pickle.load(handle)

    orgDF = pd.read_csv('datasets/organizations.csv')
    [degreesDF, binnedDegreeDF, binnedDegreesScaledDF] = processDegreesData(orgDF, degreesDF, foundersOrgDict, orgFundingDict, founderGenderDict)

    writeToCSV(degreesDF, 'datasets/degrees_processed', True)
    writeToCSV(binnedDegreeDF, 'binned_output/degrees_count', False)
    writeToCSV(binnedDegreesScaledDF, 'binned_output/degrees_fraction', False)