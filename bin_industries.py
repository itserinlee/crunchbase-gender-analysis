import pandas as pd
import pprint
import json


def writeToCSV(industryDict: dict, industriesFile: str, countMode: bool):

    finalDF = pd.DataFrame()

    for key in industryDict.keys():

        values = industryDict[key]
        
        if countMode:
            row = {'industry': key, 'male': values[0], 'female': values[1], 'nonbinary': values[2], 'unspecified': values[3], 'diversity': values[4], 'disparityFactor': values[0]/(values[0]+ values[4]), 'equalityFactor': values[4]/(values[0]+ values[4])}
        else:
            row = {'industry': key, 'male': values[0], 'female': values[1], 'nonbinary': values[2], 'unspecified': values[3]}

        tempDF = pd.DataFrame([row])
        finalDF = pd.concat([finalDF, tempDF], ignore_index=True)
    
    finalDF.to_csv(industriesFile, index=False)


def getCategoryGroups(orgDF: pd.DataFrame) -> dict:
    '''
    purpose:
    takes in all possible industries of companies in our data & distills this down to 47 industries

    how:
    uses a dictionary to get a distinct set of industries (category groups) stored as keys
    the value for each key stores a list where we have fraction founders by gender
    we are counting how many companies were founded because it gives each person a fraction, not a count
    i.e. this is counting funded companies by genders
    
    design choices:
    reason we use a dictionary is we map a set (unique industries as key) to a value, which is the count of gender
    count reflects the number of founders in each category group (industry)
    '''

    categoryDict = dict()
    uniqueIndustries = 0
    blanks = 0
    totalObservations = orgDF.shape[0]

    for idx, row in orgDF.iterrows():

        if pd.isna(row['categoryGroups']) or row['categoryGroups'] == 'N/A':
            blanks += 1
            continue

        categoryGroups = row['categoryGroups'].split(', ')

        for category in categoryGroups:
            # step 1: check if key is already there
            if category in categoryDict: # if yes, initialize counts
                count = categoryDict[category]

                # step 2: adding the pre-existing count to the new count
                count = [count[0] + row['genderMalePercent'], count[1] + row['genderFemalePercent'], count[2] + row['genderNonBinaryPercent'], count[3] + row['genderNotProvidedPercent'], count[4] + row['genderDiversityPercent']]

                # step 3: update dictionary
                categoryDict[category] = count

            else: # if no, we need to inialize the dictionary with a list that has 1 entry
                categoryDict[category] = [row['genderMalePercent'], row['genderFemalePercent'], row['genderNonBinaryPercent'], row['genderNotProvidedPercent'], row['genderDiversityPercent']]
                uniqueIndustries += 1  # program came across a new distinct industry, (which is the key in the dictionary)

    # statistics
    print(f'{blanks=}')
    print(f'{uniqueIndustries=}')
    fractionNull = round(blanks/totalObservations * 100, 2)
    print(f'Fraction of industry data missing: {fractionNull}% ({blanks} out of {totalObservations})')  # as of 4/21/2022: 1.74% (7471 out of 429935)

    # output cleaned industries to a file
    with open('datasets/cleanedIndustriesDict.json', 'w') as fp: # fp means file pointer
        json.dump(categoryDict, fp, sort_keys=True, indent=4)

    categoryDictScaled = dict()
    for key, values in categoryDict.items():
        # values is a list of fraction founders by gender
        # calculate the sum of first 4 items of list bc they add up to a whole
        founderSum = values[0] + values[1] + values[2] + values[3]
        scaledValues = []

        for value in values:
            scaledValues.append(value/founderSum) # getting this to add up to 1
        
        categoryDictScaled[key] = scaledValues

    return [categoryDict, categoryDictScaled] # the total possible options from the category groups graph (a.k.a collection)


if __name__ == '__main__':

    orgFile = 'datasets/organizations.csv.pickle'
    industriesFounderCount = 'binned_output/industries_count.csv'
    industriesFounderFraction = 'binned_output/industries_fraction.csv'
    orgDF = pd.read_pickle(orgFile)

    [categoryDict, categoryDictScaled] = getCategoryGroups(orgDF)
    writeToCSV(categoryDict, industriesFounderCount, True)
    writeToCSV(categoryDictScaled, industriesFounderFraction, False)