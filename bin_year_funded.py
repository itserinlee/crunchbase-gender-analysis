import pandas as pd
import matplotlib.pyplot as plt
from rich_dataframe import prettify
import pprint
import time
import pytz
from datetime import date, datetime


def programsProgress():
    zoneLA = pytz.timezone('America/Los_Angeles')
    nextTimeInstance = datetime.now(zoneLA)
    nextTimeFormatted = nextTimeInstance.strftime('%I:%M %p (%H:%M)')
    return print(f'Progress: {nextTimeFormatted}')


def listMaker(n: int) -> list:
    return [0] * n


def prepareData(df: pd.DataFrame) -> pd.DataFrame:
    '''
    purpose:
    adds an extra column to dataframe based on the founded on date to extract just the year
    sorts the dataframe by the year an org was founded
    returns the modified dataframe
    '''

    df['foundedOnYear'] = pd.DatetimeIndex(df['foundedOn']).year
    df = df.sort_values('foundedOnYear')

    return df


def writeToCSV(df: pd.DataFrame):

    try:
        df.to_csv('binned_output/year_funded.csv', index = False)
    
    except ValueError as v:
        print(f'{v}=')
    
    
def plotData(df: pd.DataFrame):
    '''
    purpose:
    plots a line graph
    
    note:
    need to specify x-axis before every y-axis element
    '''

    # graph 1:
    plt.figure()
    plt.title('Funding Per Year')
    plt.xlabel('year')
    plt.ylabel('funding dollars (in USD)')
    plt.plot(df['years'], df['fundingTotal'])
    plt.show()


    # graph 2:
    plt.figure()
    plt.title('Fraction of Total Funding Per Year by Gender')
    plt.xlabel('year')
    plt.ylabel('percentage')
    # normalized in terms of sum for that year
    plt.plot(df['years'], df['male'],
            df['years'], df['female'],
            df['years'], df['nonbinary'],
            df['years'], df['unspecified'])
    plt.legend(['male', 'female', 'nonbinary', 'unspecified'])
    plt.show()


def binFundingByYear(orgDF: pd.DataFrame) -> pd.DataFrame:
    '''
    purpose:
    bins total funding by year from 1990-2021

    binning technique:
    create 3 lists with all the years in it, initializing values of arrays with zeros
    loops over all the data
    looks at a given year & then sees what gender it can map to from the org table
    plunks that gender fraction into an array
    adds to that array for every iteration
    '''

    startYear = 1990
    numYears = 32
    male = listMaker(32)
    female = listMaker(32)
    nonbinary = listMaker(32)
    unspecified = listMaker(32)

    years = range(startYear, startYear + numYears)

    for each, row in orgDF.iterrows():

        if each % 20000 == 0: # checking program's progress
            programsProgress()

        fundingEventList = row['moneyRaised']

        for fundingEvent in fundingEventList:
            '''
            For simplicity’s sake, this processes the data to make a linear profile of total funding.
            Where the year was missing, money is being spread from the year a company was founded (since we don’t have that funded event’s year)
            all the way to the current year.

            Regardless of where the funding event year was missing or not,
            this function determines the amount to add per year over many years and then spreads that money all the way across.
            '''

            
            year = int(fundingEvent[0]) # convert from str to int
            money = fundingEvent[1]

            if year == 0:
                year = row['foundedOnYear']

                if (year - startYear >= 0) and (year - startYear < numYears):   # if data is too old or too new
                    stopYear = startYear + numYears - 1                         # minus 1 in the case that stopYear = startYear
                    
                    # yearSpan is how many years to spread the money over
                    yearSpan = stopYear - year + 1                              # plus 1 in the case that stopYear = startYear
                    moneyPerYear = money/yearSpan

                    startIndex = year - startYear
                    stopIndex = startIndex + yearSpan
                    for i in range(startIndex, stopIndex):
                        # total money for each year
                        male[i] += (moneyPerYear * row['genderMalePercent']) / 100.0
                        female[i] += (moneyPerYear * row['genderFemalePercent']) / 100.0
                        nonbinary[i] += (moneyPerYear * row['genderNonBinaryPercent']) / 100.0
                        unspecified[i] += (moneyPerYear * row['genderNotProvidedPercent']) / 100.0
                
                continue


            if (year - startYear >= 0) and (year - startYear < numYears):
                
                # total money for each year
                male[year-startYear] += (money * row['genderMalePercent']) / 100.0
                female[year-startYear] += (money * row['genderFemalePercent']) / 100.0
                nonbinary[year-startYear] += (money * row['genderNonBinaryPercent']) / 100.0
                unspecified[year-startYear] += (money * row['genderNotProvidedPercent']) / 100.0

    fundingTotalPerYear = []

    # at the end:
    for n in range(0, numYears):

        # sum of all sums
        sumOfAllSums = male[n] + female[n] + nonbinary[n] + unspecified[n]

        if sumOfAllSums > 0:    # division by zero error because the year is 0
            male[n] = male[n] / sumOfAllSums
            female[n] = female[n] / sumOfAllSums
            nonbinary[n] = nonbinary[n] / sumOfAllSums
            unspecified[n] = unspecified[n] / sumOfAllSums

        fundingTotalPerYear.append(sumOfAllSums)


    d = {'years': years,                        # x axis
        'fundingTotal': fundingTotalPerYear,    # y axis
        'male': male,                           # y axis
        'female': female,                       # y axis
        'nonbinary': nonbinary,                 # y axis
        'unspecified': unspecified}             # y axis

    
    df = pd.DataFrame.from_dict(d, orient='index').transpose()
    return df


if __name__ == '__main__':

    orgDF = pd.read_pickle('datasets/org_funding.csv.pickle')
    
    orgDF = prepareData(orgDF)
    yearsFundedDF = binFundingByYear(orgDF)
    # plotData(yearsFundedDF)
    writeToCSV(yearsFundedDF)




