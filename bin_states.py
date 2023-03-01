import pandas as pd
from rich_dataframe import prettify
import pprint
import time


def writeToCSV(df: pd.DataFrame, filename: str):
    df.to_csv(f'{filename}.csv', index=True)


def getPopulationDict(populationDF: pd.DataFrame) -> dict:

    populationDict = dict()
    for idx, row in populationDF.iterrows():
        state = row['state']

        if state not in populationDict:
            populationDict[state] = row['estimate']
    
    return populationDict


def scalesByRow(df: pd.DataFrame)->None:

    for idx, row in df.iterrows():
        rowSum = 0
        for key, value in row.items():
            if pd.isna(value):
                row[key] = 0 # replace nan with 0
            else:
                rowSum += value
        
        if rowSum != 0: # check for ZeroDivisionError
            for key, value in row.items():
                df.at[idx, key] = value/rowSum
        else:
            for key, value in row.items():
                df.at[idx, key] = 0 # cleans dataframe of any nan's


def processStatesFunc(orgDF: pd.DataFrame, populationDF: pd.DataFrame) ->list:

    population = getPopulationDict(populationDF) # returns a dictionary

    statesDict = dict()
    for state in population.keys():
        statesDict[state] = [0, 0, 0, 0, 0] # state has 0 gender to begin with

    for idx, row in orgDF.iterrows():

        orgState = row['state'] 

        if orgState in statesDict and orgState in population:
            statesDict[orgState][0] += row['genderMale']
            statesDict[orgState][1] += row['genderFemale']
            statesDict[orgState][2] += row['genderNonBinary']
            statesDict[orgState][3] += row['genderNotProvided']
            statesDict[orgState][4] += row['genderDiversity']


    # for key, value in statesDict.items():
    #     statesDict[key][0] /= int(population[key].replace(',', '')) # per capita
    #     statesDict[key][1] /= int(population[key].replace(',', ''))
    #     statesDict[key][2] /= int(population[key].replace(',', ''))
    #     statesDict[key][3] /= int(population[key].replace(',', ''))
    #     statesDict[key][4] /= int(population[key].replace(',', ''))
    # pprint.pprint(statesDict)


    for key, value in statesDict.items():
        statesDict[key][0] /= int(population[key].replace(',', '')) / 100000 # per 100,000
        statesDict[key][1] /= int(population[key].replace(',', '')) / 100000
        statesDict[key][2] /= int(population[key].replace(',', '')) / 100000
        statesDict[key][3] /= int(population[key].replace(',', '')) / 100000
        statesDict[key][4] /= int(population[key].replace(',', '')) / 100000


    statesDF = pd.DataFrame.from_dict(statesDict, orient='columns').transpose()
    statesDF.rename({0: 'male', 1: 'female', 2: 'non-binary', 3: 'unspecified', 4: 'diversity'}, axis='columns', inplace=True)
    
    statesScaledDF = statesDF.drop(columns=['female', 'non-binary', 'unspecified'])
    scalesByRow(statesScaledDF) # modifies dataframe

    return [statesDF, statesScaledDF]


if __name__ == '__main__':

    populationDF = pd.read_csv('datasets/cb_state_estimates.csv')
    orgDF = pd.read_csv('datasets/organizations.csv')

    [statesDF, statesScaledDF] = processStatesFunc(orgDF, populationDF)

    writeToCSV(statesDF, 'binned_output/state_count')
    writeToCSV(statesScaledDF, 'binned_output/state_fraction')