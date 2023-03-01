import requests
import pandas as pd
from IPython.display import display
import pprint
import time
import pytz
from datetime import date, datetime

from auth import key


'''
reviewing what map() does in the makeRequest() function:
    # ans['entities'] is an iterable object bc its a list
    # first parameter in map() acts upon one item in that list
    # map() puts this in a new list
    # this is going to flatten that list
    # then we are returning the result of that map() operation in a pandas dataframe
'''


def programsProgress():
    '''
    this prints the time so you can track the program's progress
    '''
    zoneLA = pytz.timezone('America/Los_Angeles')
    nextTimeInstance = datetime.now(zoneLA)
    nextTimeFormatted = nextTimeInstance.strftime('%I:%M %p (%H:%M)')
    return print(f'Progress: {nextTimeFormatted}')


userKey = {'user_key': key}                                 # json object, not a string

def makeRequest(url: str, query: dict, columnHeaders: str, flattenFunction = None):
    '''
    this is a low-level attempt to access the url for any given collection
    deals with all the error codes that can occur, such as the rate limit
    the request to the API is contained within a loop
    this loop does a retry if there are errors and enforces delays
    it looks to see if there is a custom flatten function specified -- as in from the organizations.py program
    for a given collection & applies it
    otherwise, it just does a json_normalize() from pandas
    '''


    time.sleep(0.25)                                       # rate limit is a float val

    while True:
        try:
            resp = requests.post(url, params = userKey, json = query)
        except:
            # ConnectionResetError: [Errno 54] Connection reset by peer
            print(f'An exception occurred during the POST request. Sleeping for 10 seconds before program resumes.')
            time.sleep(10)  # 10 seconds
            continue

        if resp.status_code == 409 or resp.status_code == 429:
            print('Exceeded the rate limit.')
            time.sleep(60)                              # sleeps for 60 seconds
            continue
        else:
            break
    
    if resp.status_code != 200:                         # error handling
        # print(f'{dir(resp)=}')
        print(f'{resp.status_code=}')
        print(f'{resp.reason=}')

        ans = resp.json()
        if not 'entities' in ans:
            print('entities not found in answer')
            print(f'{dir(resp)=}')
            print(f'{ans=}')
            # print(f'{dir(ans)=}')
        exit()

    ans = resp.json()    

    if len(ans['entities']) == 0:
        print('Empty query result. Not necessariy an error.')
        return [0, pd.DataFrame(columns = columnHeaders)]

    count = int(ans['count'])                       # this extracts the limit to how many records you can retrieve from collection
    if count > 10:
        print(f'Total records expected: {count}')

    if flattenFunction is None:
        answerDF = pd.json_normalize(ans['entities'])
    else:
        # map itself is a loop in a sense
        answerDF = pd.DataFrame(map(flattenFunction, ans['entities']), columns = columnHeaders)

    return [count, answerDF]

# order of parameters matters
def pullData(filename: str, url: str, query: dict, stopRecord: int = 1000, flattenFunction = None, columnHeaders: list = [], postProcessFunc = None):
    '''
    purpose: provides the ability to query a url with a pre-defined query either from organizations, people, funding rounds, and so on....
    
    steps:
    stamps the dataframe with some column headers
    writes this out to a file
    has exception handling to restart program for cases like server disconnection
    has a while loop because we are restricted to return only a certain amount of data at a time
    then it does a post-process with a function that flattens the data
    to achieve this, we pass a function called postProcess() to this function
    
    keep in mind:
    this function is called from within other programs, such as organizations.py & funding_rounds.py
    '''  
    
    
    try: # reading in file
        finalDF = pd.read_csv(filename)
        
        # instantiate variables
        uuidLast = finalDF['uuid'].tolist()[-1]
        currentRecordCount = len(finalDF)

        print(f'Recovering with a {currentRecordCount} number of records. \n Last uuid: {uuidLast}')
        # cannot do exit() from within the try

    except:
        print('Starting with a fresh dataset.')

        # declare/initialize variables for the first time
        uuidLast = None
        finalDF = pd.DataFrame(columns = columnHeaders) # initialize columns in the constructor
        currentRecordCount = 0
    

    while currentRecordCount < stopRecord:
        if uuidLast is not None:    # assumes this is not the first pass through
            query['after_id'] = uuidLast
        # for first pass through when querying, we don't need the 'after_id' field


        # totalNumQueryResults
        # is the total results that are available as determined by the first query; default value is 0
        totalNumQueryResults, answerDF = makeRequest(url, query, columnHeaders, flattenFunction = flattenFunction)

        if answerDF.empty:
            print('answerDF is empty')
            break

        if postProcessFunc is not None:
            postProcessFunc(answerDF)

        finalDF = pd.concat([finalDF, answerDF])

        # make sure you get some data
        if len(filename) != 0:                          # writes every time
            print(f'Making sure we get some data...')
            finalDF.to_csv(filename, index = False)
            finalDF.to_pickle(filename + '.pickle')
        
        # column needs to be named at this point to access it within answerDF[]
        if 'uuid' not in answerDF:
            print('Error: uuid not in answerDF -- did not get a uuid back')
            display(answerDF)
            break
        
        else:
            uuidLast = answerDF['uuid'].tolist()[-1]    # the previous uuidLast, when you make the next query, this is the last page number you left off
            recordsThisQuery = len(answerDF['uuid'])
            currentRecordCount += recordsThisQuery
            print('Should always come here after n number of organization queries')
            print(f'\n{currentRecordCount=}')

            if currentRecordCount >= stopRecord or currentRecordCount >= totalNumQueryResults:
                break

        print(f'\n{currentRecordCount} of {totalNumQueryResults} ... about to do a follow up query')

    if not finalDF.empty:
        print(f'\nResults from loop: {currentRecordCount}')               # confirm the number of results from the loop
        programsProgress()
        # finalDF.info()                                                # using the .info() function to inspect the final dataframe

    if len(filename) != 0:
        finalDF.to_csv(filename, index = False)
    
    return finalDF
