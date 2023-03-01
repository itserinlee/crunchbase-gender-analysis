import pandas as pd
from rich_dataframe import prettify
import pprint
import time


def writeToCSV(df: pd.DataFrame, filename: str):
    df.to_csv(f'{filename}.csv', index=False)


def createOrgDict(orgDF: pd.DataFrame) -> dict:
    '''
    this produces the organizations dictionary
    source of data: orgDF
    '''

    organizations = dict()
    for idx, row in orgDF.iterrows():
        orgUuid = row['uuid']
        genderMaleFraction = row['genderMalePercent']/100.0   # divide by 100 to get a fraction
        genderFemaleFraction = row['genderFemalePercent']/100.0
        genderNonBinaryFraction = row['genderNonBinaryPercent']/100.0
        genderNotProvidedFraction = row['genderNotProvidedPercent']/100.0

        genderMale = row['genderMale']
        genderFemale = row['genderFemale']
        genderNonBinary = row['genderNonBinary']
        genderNotProvided = row['genderNotProvided']

        organizations[orgUuid] = (genderMale, genderFemale, genderNonBinary, genderNotProvided, genderMaleFraction, genderFemaleFraction, genderNonBinaryFraction, genderNotProvidedFraction)

    return organizations


def postProcessInvestorsFunc(investmentsDF: pd.DataFrame, organizations: dict, moneyMode: bool, filename: str):
    '''
    purpose:
    for each investor, we need a list of pairs: (org, moneyInvested)
    key = investor uuid
    take moneyInvested from each investor
    tie it to the money to a founder
    '''
    
    # algorithm
    # first loop over investor table
    # then loop over org table
    # post loop to analyze the money that went to each gender to avoid token investment


    # produces investors & investors metadata
    investors = dict()
    investorMetadata = dict()
    noMoneyInvested = 0
    for idx, row in investmentsDF.iterrows():
        investorUuid = row['investorUuid']
        investorName = row['investorIdentifier']
        moneyInvested = 0

        if not (pd.isna(row['moneyInvested']) or row['moneyInvested'] == 'N/A'):
            moneyInvested = row['moneyInvested']

        if (moneyInvested > 0) or (not moneyMode):

            if not investorUuid in investorMetadata:
                investorMetadata[investorUuid] = investorName

            if investorUuid in investors:
                existingInvestments = investors[investorUuid]
                existingInvestments.append((row['organizationUuid'], moneyInvested))
                investors[investorUuid] = existingInvestments

            else:
                investors[investorUuid] = [(row['organizationUuid'], moneyInvested)]
        
        else: # count of times when no money was found
            noMoneyInvested += 1

    
    # producing investor dataframe
    # source of data: organizations, investors, and investors metadata
    investorDF = pd.DataFrame()
    for investorUuid, values in investors.items():
        genderMale = 0
        genderFemale = 0
        genderNonBinary = 0
        genderNotProvided = 0
        genderMaleFraction = 0
        genderFemaleFraction = 0
        genderNonBinaryFraction = 0
        genderNotProvidedFraction = 0
        
        for investment in values: # values is a list of tuples of (org, moneyInvested)
            '''
            investment[0] = org uuid
            investment[1] = money invested
            '''

            if investment[0] in organizations: # because so many rows don't label any money

                # count
                genderMale += organizations[investment[0]][0]
                genderFemale += organizations[investment[0]][1]
                genderNonBinary += organizations[investment[0]][2]
                genderNotProvided += organizations[investment[0]][3]

                # by money invested
                genderMaleFraction += investment[1] * organizations[investment[0]][4]
                genderFemaleFraction += investment[1] * organizations[investment[0]][5]
                genderNonBinaryFraction += investment[1] * organizations[investment[0]][6]
                genderNotProvidedFraction += investment[1] * organizations[investment[0]][7]
        

        if genderMale + genderFemale + genderNonBinary + genderNotProvided == 0:
            continue # skipping this investor if all cases are 0 bc no founder was found

        # add all together for total investment for this one investor
        moneyInvestedByInvestor = genderMaleFraction + genderFemaleFraction + genderNonBinaryFraction + genderNotProvidedFraction
        
        if moneyMode:
            if moneyInvestedByInvestor > 0: # ZeroDivisionError
                genderMaleFraction /= moneyInvestedByInvestor
                genderFemaleFraction /= moneyInvestedByInvestor
                genderNonBinaryFraction /= moneyInvestedByInvestor
                genderNotProvidedFraction /= moneyInvestedByInvestor
            
            row = {'investorUuid': investorUuid,
                    'investorName': investorMetadata[investorUuid], # where the value is just the name
                    'moneyInvested': moneyInvestedByInvestor,
                    'genderMale': genderMale,
                    'genderFemale': genderFemale,
                    'genderNonBinary': genderNonBinary,
                    'genderNotProvided': genderNotProvided,
                    'maleFractionByMoney': genderMaleFraction,
                    'femaleFractionByMoney': genderFemaleFraction,
                    'nonbinaryFractionByMoney': genderNonBinaryFraction,
                    'notprovidedFractionByMoney': genderNotProvidedFraction}
        
        else: # just represents activity even if no money is counted
            totalFounders = genderMale + genderFemale + genderNonBinary + genderNotProvided

            if totalFounders > 0: # ZeroDivisionError
                genderMaleFraction = genderMale/totalFounders
                genderFemaleFraction = genderFemale/totalFounders
                genderNonBinaryFraction = genderNonBinary/totalFounders
                genderNotProvidedFraction = genderNotProvided/totalFounders
                    
            row = {'investorUuid': investorUuid,
                    'investorName': investorMetadata[investorUuid], # where the value is just the name
                    'moneyInvested': moneyInvestedByInvestor,
                    'genderMale': genderMale,
                    'genderFemale': genderFemale,
                    'genderNonBinary': genderNonBinary,
                    'genderNotProvided': genderNotProvided,
                    'fractionMaleFounders': genderMaleFraction,
                    'fractionFemaleFounders': genderFemaleFraction,
                    'fractionNonbinaryFounders': genderNonBinaryFraction,
                    'fractionNotProvidedFounders': genderNotProvidedFraction}

        tempDF = pd.DataFrame([row])
        investorDF = pd.concat([investorDF, tempDF], ignore_index=True)

    print(f'Number of instances where no money was labelled: {noMoneyInvested=}')
    return investorDF


if __name__ == '__main__':

    investmentsDF = pd.read_pickle('datasets/investments.csv.pickle')
    orgDF = pd.read_csv('datasets/organizations.csv')

    organizations = createOrgDict(orgDF)
    investorDF = postProcessInvestorsFunc(investmentsDF, organizations, False,'investors_per_founder')
    
    writeToCSV(investorDF, 'binned_output/investors_fraction')
