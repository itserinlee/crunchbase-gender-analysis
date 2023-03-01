import pandas as pd
from rich_dataframe import prettify
import pprint
import time


def postProcessOrgPeopleFunc(fundingRoundDF: pd.DataFrame, orgDF: pd.DataFrame) -> pd.DataFrame:
    '''
    purpose:
    processing 2 separate datasets, organizations & funding rounds, in order to track money raised at each funding round
    
    what this does:
    first, loops over the funding rounds
    maps all that data back to the organizations dataset
    builds a dictionary that maps from the organization uuid to all the funding round data available for it
    then it inserts this back into the organization dataset
    
    note:
    for each funding round, an organization name can be repeated
    '''
    
    fundingRoundDict = dict()

    for idx, row in fundingRoundDF.iterrows(): # iterrows is a method on a dataframe

        key = row['fundedOrganizationIdentifierUuid']
        closedOnYearStr = row['closedOn']
        moneyRaised = row['moneyRaisedUSD']

        if pd.isna(moneyRaised) or moneyRaised == 'N/A':
            continue
        if pd.isna(closedOnYearStr) or closedOnYearStr == 'N/A':
            closedOnYear = 0
        else:
            try:
                closedOnYear = int(closedOnYearStr[0:4])
            except:
                print(f'Exception: {closedOnYearStr=}')
                closedOnYear = 0

        values = (closedOnYear, moneyRaised)


        if key in fundingRoundDict:

            fundingRoundList = fundingRoundDict[key]

            for each, fundingRound in enumerate(fundingRoundList):

                if fundingRound[0] == closedOnYear: # if we came across it, we know its the only one in there

                    fundingRoundList[each] = (closedOnYear, values[1] + fundingRound[1])

                    values = None   # because we have used it
                    break
            
            if values is not None:
                fundingRoundDict[key].append(values)

        else:
            fundingRoundDict[key] = [values]
            

    orgDF.insert(len(orgDF.columns), 'moneyRaised', [[]] * len(orgDF)) # a list gets mapped vertically & put an empty list in each row of that col
    
    print(f'{len(fundingRoundDict)=}')
    foundCounter = 0
    matchedRounds = 0 # how many rounds got matched back to an organization

    for idx, row in orgDF.iterrows():

        if idx % 50000 == 0:
            print(f'Progress: {idx=}')
        
        key = row['uuid']

        if key not in fundingRoundDict: # if it does not find funding, continue looping through org table
            continue
        else:                           # if it does find funding, update the org dataframe
            orgDF.at[idx, 'moneyRaised'] = sorted(fundingRoundDict[key])

            foundCounter += 1
            matchedRounds += len(fundingRoundDict[key]) # fundingRoundDict[key] is actually a list

    return orgDF


if __name__ == '__main__':

    fundingRoundDF = pd.read_pickle('datasets/funding_rounds.csv.pickle')
    orgDF = pd.read_csv('datasets/organizations.csv')

    orgfundDF = postProcessOrgPeopleFunc(fundingRoundDF, orgDF)

    orgfundDF.to_pickle('datasets/org_funding.csv.pickle')
    orgfundDF.to_csv('datasets/org_funding.csv', index=False)