import requests
import pandas as pd
import pprint
import pickle

from auth import key
from crunch_library import pullData
import people                           # makes everything in people available as an object


url = 'https://api.crunchbase.com/api/v4/searches/organizations/'

userKey = {'user_key': key}

def organizationsQuery(limit: int = 1000) -> dict:
    '''
    this produced the JSON to send to the Crunchbase server
    '''

    query = {
        "field_ids": [
            "acquirer_identifier",                      #  1
            "categories",                               #  2
            "category_groups",                          #  3
            "closed_on",                                #  4    # date that the organization was closed on... 
            "company_type",                             #  5
            "delisted_on",                              #  6
            "description",                              #  7
            "diversity_spotlights",                     #  8
            "exited_on",                                #  9
            "facet_ids",                                # 10
            "founded_on",                               # 11
            "founder_identifiers",                      # 12
            "funding_stage",                            # 13
            "funding_total",                            # 14
            "funds_total",                              # 15
            "hub_tags",                                 # 16
            "identifier",                               # 17
            "last_funding_at",                          # 18
            "last_funding_total",                       # 19
            "last_funding_type",                        # 20
            "location_identifiers",                     # 21
            "name",                                     # 22
            "num_diversity_spotlight_investments",      # 23
            "num_founders",                             # 24
            "num_funding_rounds",                       # 25
            "num_investments",                          # 26
            "num_investors",                            # 27
            "operating_status",                         # 28
            "owner_identifier",                         # 29
            "rank_org",                                 # 30
            "revenue_range",                            # 31
            "short_description",                        # 32
            "status",                                   # 33
            "stock_exchange_symbol",                    # 34
            "stock_symbol",                             # 35
            "updated_at",                               # 36
            "uuid",                                     # 37
            "valuation",                                # 38
            "valuation_date",                           # 39
            "went_public_on"                            # 40
        ],
        "query": [ 
            {
                "type": "predicate",
                "field_id": "facet_ids",
                "operator_id": "includes",
                "values": [
                    "company"                       # possible values: company, investor, school
                ]
            },
            {
                "type": "predicate",
                "field_id": "location_identifiers",
                "operator_id": "includes",
                "values": [
                    "f110fca2-1055-99f6-996d-011c198b3928"
                ]
            },
            {
                "type": "predicate",
                "field_id": "founded_on",
                "operator_id": "gte",
                "values": [
                    "1990"
                ]
            }
        ], 
        "limit": limit
    }
    return query


orgFoundersDict = dict()    # mapping one value to another between collections
founderGender = dict()
def flattenHelper(orgUuid: str, foundersList: list) -> None:
    '''
    caching the founders list in a dictionary
    '''
    
    global orgFoundersDict
    orgFoundersDict[orgUuid] = foundersList     # allows us to look up founders list for any given organization


def flattenOrgsEntity(entity: dict) -> list:
    '''
    custom flattening function that acts upon the organizations data
    calls flattenHelper() function defined above to stash away the founders uuid list for a given organization
    '''

    # custom variables
    genderMale = 0      # meaning the gender of a start-up founder...
    genderFemale = 0
    genderNonBinary = 0
    genderNotProvided = 0
    genderDiversity = 0
    genderMalePercent = 0
    genderFemalePercent = 0
    genderNonBinaryPercent = 0
    genderNotProvidedPercent = 0
    genderDiversityPercent = 0



    try:
        acquirerIDList = entity['properties']['acquirer_identifier']
        acquirerIdentifier = ', '.join(map(lambda x: x['value'], acquirerIDList))
    except:
        acquirerIdentifier = 'N/A'

    try:
        categoriesList = entity['properties']['categories']
        categories = ', '.join(map(lambda x: x['value'], categoriesList))
    except:
        categories = 'N/A'

    try:
        categoryGroupsList = entity['properties']['category_groups']
        categoryGroups = ', '.join(map(lambda x: x['value'], categoryGroupsList))
    except:
        categoryGroups = 'N/A'

    try:
        closedOn = entity['properties']['closed_on']['value']   # the date when the organization is closed
    except:
        closedOn = 'N/A'

    try:
        companyType = entity['properties']['company_type']
    except:
        companyType = 'N/A'

    try:
        delistedOn = entity['properties']['delisted_on']['value']
    except:
        delistedOn = 'N/A'

    try:
        description = entity['properties']['description']
    except:
        description = 'N/A'

    try:
        diversitySpotlightsList = entity['properties']['diversity_spotlights']
        diversitySpotlights = ', '.join(map(lambda x: x['value'], diversitySpotlightsList))
    except:
        diversitySpotlights = 'N/A'

    try:
        exitedOn = entity['properties']['exited_on']['value']
    except:
        exitedOn = 'N/A'

    try:
        facetIdsList = entity['properties']['facet_ids']
        facetIds = ', '.join(facetIdsList)
    except:
        facetIds = 'N/A'

    try:
        foundedOn = entity['properties']['founded_on']['value']
    except:
        foundedOn = 'N/A'

    try:
        founders = entity['properties']['founder_identifiers']
        founderUuidList = [founder['uuid'] for founder in founders]
        flattenHelper(entity['uuid'], founderUuidList)              # to stash away the founder list for the organization as its not in the org table
    except:
        flattenHelper(entity['uuid'], [])

    try:
        fundingStage = entity['properties']['funding_stage']
    except:
        fundingStage = 'N/A'

    try:
        fundingTotal = entity['properties']['funding_total']['value_usd']
    except:
        fundingTotal = 'N/A'

    try:
        fundsTotal = entity['properties']['funds_total']['value_usd']
    except:
        fundsTotal = 'N/A'

    try:
        hubTagsList = entity['properties']['hub_tags']
        hubTags = ', '.join(hubTagsList)
    except:
        hubTags = 'N/A'

    try:
        identifierValue = entity['properties']['identifier']['value']
    except:
        identifierValue = 'N/A'

    try:
        lastFundingAt = entity['properties']['last_funding_at']
    except:
        lastFundingAt = 'N/A'

    try:
        lastFundingTotal = entity['properties']['last_funding_total']['value_usd']
    except:
        lastFundingTotal = 'N/A'
    
    try:
        lastFundingType = entity['properties']['last_funding_type']
    except:
        lastFundingType = 'N/A'

    try:
        locationIdentifiersList = entity['properties']['location_identifiers']
        locationIdentifiers = ', '.join(map(lambda x: x['value'], locationIdentifiersList))
    except:
        locationIdentifiers = 'N/A'

    try:
        locationIdentifiersUuidList = entity['properties']['location_identifiers']
        locationIdentifiersUuid = ', '.join(map(lambda x: x['uuid'], locationIdentifiersUuidList))
    except:
        locationIdentifiersUuid = 'N/A'
        
    try:
        name = entity['properties']['name']
    except:
        name = 'N/A'

    try:
        numDiversitySpotlightInvestments = entity['properties']['num_diversity_spotlight_investments']
    except:
        numDiversitySpotlightInvestments = 'N/A'

    # if no founders, genders will returns 0
    try:
        numFounders = entity['properties']['num_founders']
    except:
        numFounders = 'N/A'

    try:
        numFundingRounds = entity['properties']['num_funding_rounds']
    except:
        numFundingRounds = 'N/A'

    try:
        numInvestments = entity['properties']['num_investments']
    except:
        numInvestments = 'N/A'
    
    try:
        numInvestors = entity['properties']['num_investors']
    except:
        numInvestors = 'N/A'

    try:
        operatingStatus = entity['properties']['operating_status']
    except:
        operatingStatus = 'N/A'

    try:
        ownerIdentifer = entity['properties']['owner_identifier']['value']
    except:
        ownerIdentifer = 'N/A'

    try:
        rankOrg = entity['properties']['rank_org']
    except:
        rankOrg = 'N/A'

    try:
        revenueRange = entity['properties']['revenue_range']
    except:
        revenueRange = 'N/A'
    
    try:
        shortDescription = entity['properties']['short_description']
    except:
        shortDescription = 'N/A'

    try:
        status = entity['properties']['status']
    except:
        status = 'N/A'

    try:
        stockExchangeSymbol = entity['properties']['stock_exchange_symbol']
    except:
        stockExchangeSymbol = 'N/A'

    try:
        stockSymbol = entity['properties']['stock_symbol']['value']
    except:
        stockSymbol = 'N/A'

    try:
        updatedAt = entity['properties']['updated_at']
    except:
        updatedAt = 'N/A'
    
    try:
        valuation = entity['properties']['valuation']['value_usd']
    except:
        valuation = 'N/A'

    try:
        valuationDate = entity['properties']['valuation_date']
    except:
        valuationDate = 'N/A'
        
    try:
        wentPublicOn = entity['properties']['went_public_on']
    except:   
        wentPublicOn = 'N/A'

    try:
        city = locationIdentifiers.split(', ')[0]
    except:
        city = 'N/A'

    try:
        state = locationIdentifiers.split(', ')[1]
    except:
        state = 'N/A'
    

    entityList = [entity['uuid'],                       #  1
                identifierValue,                        #  2
                name,                                   #  3
                categories,                             #  4
                categoryGroups,                         #  5
                facetIds,                               #  6
                companyType,                            #  7
                shortDescription,                       #  8
                description,                            #  9
                status,                                 # 10
                operatingStatus,                        # 11
                acquirerIdentifier,                     # 12
                ownerIdentifer,                         # 13
                foundedOn,                              # 14
                wentPublicOn,                           # 15
                lastFundingAt,                          # 16
                lastFundingTotal,                       # 17
                lastFundingType,                        # 18
                delistedOn,                             # 19
                exitedOn,                               # 20
                closedOn,                               # 21
                fundingStage,                           # 22
                fundingTotal,                           # 23
                fundsTotal,                             # 24
                numFundingRounds,                       # 25
                numInvestments,                         # 26
                numInvestors,                           # 27
                numFounders,                            # 28
                genderMale,                             # 29
                genderFemale,                           # 30
                genderNonBinary,                        # 31
                genderNotProvided,                      # 32
                genderDiversity,                        # 33
                genderMalePercent,                      # 34
                genderFemalePercent,                    # 35
                genderNonBinaryPercent,                 # 36
                genderNotProvidedPercent,               # 37
                genderDiversityPercent,                 # 38
                numDiversitySpotlightInvestments,       # 39
                diversitySpotlights,                    # 40
                rankOrg,                                # 41
                revenueRange,                           # 42
                valuation,                              # 43
                valuationDate,                          # 44
                stockExchangeSymbol,                    # 45
                stockSymbol,                            # 46
                hubTags,                                # 47
                updatedAt,                              # 48
                locationIdentifiers,                    # 49
                city,                                   # 50
                state,                                  # 51
                locationIdentifiersUuid]                # 52
    
    return entityList


columnHeaders = ['uuid',                                #  1
                'identifierValue',                      #  2
                'name',                                 #  3
                'categories',                           #  4
                'categoryGroups',                       #  5
                'facetIds',                             #  6
                'companyType',                          #  7
                'shortDescription',                     #  8
                'description',                          #  9
                'status',                               # 10
                'operatingStatus',                      # 11
                'acquirerIdentifier',                   # 12
                'ownerIdentifer',                       # 13
                'foundedOn',                            # 14
                'wentPublicOn',                         # 15
                'lastFundingAt',                        # 16
                'lastFundingTotal',                     # 17
                'lastFundingType',                      # 18
                'delistedOn',                           # 19
                'exitedOn',                             # 20
                'closedOn',                             # 21
                'fundingStage',                         # 22
                'fundingTotal',                         # 23
                'fundsTotal',                           # 24
                'numFundingRounds',                     # 25
                'numInvestments',                       # 26
                'numInvestors',                         # 27
                'numFounders',                          # 28
                'genderMale',                           # 29
                'genderFemale',                         # 30
                'genderNonBinary',                      # 31
                'genderNotProvided',                    # 32
                'genderDiversity',                      # 33
                'genderMalePercent',                    # 34
                'genderFemalePercent',                  # 35
                'genderNonBinaryPercent',               # 36
                'genderNotProvidedPercent',             # 37
                'genderDiversityPercent',               # 38
                'numDiversitySpotlightInvestments',     # 39
                'diversitySpotlights',                  # 40
                'rankOrg',                              # 41
                'revenueRange',                         # 42
                'valuation',                            # 43
                'valuationDate',                        # 44
                'stockExchangeSymbol',                  # 45
                'stockSymbol',                          # 46
                'hubTags',                              # 47
                'updatedAt',                            # 48
                'locationIdentifiers',                  # 49
                'city',                                 # 50
                'state',                                # 51
                'locationIdentifiersUuid']              # 52


def makePeopleQuery(founderGender: dict, foundersList: list) -> None:   # modifies not returning anything
    '''
    does a call to the API for the people collection using pullData()
    grabs the uuid & gender on founders & updates the dictionary called founderGender with this info
    '''

    print(f'\nDoing a people query from organizations. \n{len(foundersList)=}')
    founders = pullData('', people.url, people.peopleQuery(uuidList = foundersList), flattenFunction = people.flattenPeopleEntity, columnHeaders = people.columnHeaders)


    # lookup the gender for a founder
    for idx2, row2 in founders.iterrows():     # founders is a dataframe

        founderGender[row2['uuid']] = row2['gender']

    foundersList.clear()   # clear the list back to nothing


def postProcessOrgFunc(orgDF: pd.DataFrame) -> pd.DataFrame:
    '''
    purpose: to batch people queries to improve runtime
    
    what this does:
    loops over all the founders for the organization, so we can get the gender's fractions for an organization
    '''

    foundersList = [] # the list we are building to reach the max size 200
    global founderGender # stores the output of having mapped the founder uuid with founder gender

    for idx, row in orgDF.iterrows():   # iterrows is a method on a dataframe

        orgFoundersList = orgFoundersDict[row['uuid']]  # contains only founders for THIS ORG
        
        if len(orgFoundersList) + len(foundersList) > 200:  # to avoid 400 error: predicate values cannot exceed 200
            makePeopleQuery(founderGender, foundersList)
        
        for founder in orgFoundersList: # we are building towards 1000 no matter what
            foundersList.append(founder)

    # need to process the data here despite not having not reached 1000
    if len(foundersList) > 0:
        makePeopleQuery(founderGender, foundersList)
    

    for idx, row in orgDF.iterrows():

        orgFoundersList = orgFoundersDict[row['uuid']]

        # variables are unique to every organization so need to reset to zero for every process/iteration
        genderMale = 0
        genderFemale = 0
        genderNonBinary = 0
        genderNotProvided = 0   # description from Crunchbase for this is: "Prefer not to identify"
        genderDiversity = 0
        genderMalePercent = 0
        genderFemalePercent = 0
        genderNonBinaryPercent = 0
        genderNotProvidedPercent = 0
        genderDiversityPercent = 0


        for founder in orgFoundersList:

            gender = founderGender[founder]

            if gender == 'male':
                genderMale += 1
            elif gender == 'female':
                genderFemale += 1
            elif gender == 'non_binary':
                genderNonBinary += 1
            elif gender == 'not_provided':
                genderNotProvided += 1
        
        # once the FOR loop is done, combine the counts on these variables
        genderDiversity = genderFemale + genderNonBinary + genderNotProvided
        
                
        # founder gender ratios
        if len(orgFoundersList) > 0:  # condition: to avoid division by zero error
            genderMalePercent = (float(genderMale) * 100.0)/(len(orgFoundersList))
            genderFemalePercent = (float(genderFemale) * 100.0)/(len(orgFoundersList))
            genderNonBinaryPercent = (float(genderNonBinary) * 100.0)/(len(orgFoundersList))
            genderNotProvidedPercent = (float(genderNotProvided) * 100.0)/(len(orgFoundersList))
            genderDiversityPercent = (float(genderDiversity) * 100.0)/(len(orgFoundersList))
        

        orgDF.at[idx, 'genderMale'] = genderMale
        orgDF.at[idx, 'genderFemale'] = genderFemale
        orgDF.at[idx, 'genderNonBinary'] = genderNonBinary
        orgDF.at[idx, 'genderNotProvided'] = genderNotProvided
        orgDF.at[idx, 'genderDiversity'] = genderDiversity
        orgDF.at[idx, 'genderMalePercent'] = genderMalePercent
        orgDF.at[idx, 'genderFemalePercent'] = genderFemalePercent
        orgDF.at[idx, 'genderNonBinaryPercent'] = genderNonBinaryPercent
        orgDF.at[idx, 'genderNotProvidedPercent'] = genderNotProvidedPercent
        orgDF.at[idx, 'genderDiversityPercent'] = genderDiversityPercent

    return orgDF


if __name__ == '__main__':
    '''
    what this does:
    1. uses the organizations query to do a makeRequest() behind-the-scenes, which returns a dataframe & has 1000 orgs in it
    2. flattens that data & caches founder info away
    3. repeats those 2 steps until we get all the data
    4. calls postProcessOrgFunc() to batch queries 1000 organizations at a time whilst also grabbing founder info from people
    5. writes org data to a pickle (binary) file, which eliminates data type issues I had later on, in addition to producing a CSV
    6. writes a dictionary, where key = org & value = founder info to a pickle file to be used in another program
    7. writes a dictionary, where key = founder & value = gender info to a pickle file to be used in another program
    '''

    filename = 'datasets/organizations.csv'

    orgDF = pullData(filename, url, organizationsQuery(), stopRecord = 10e6, flattenFunction = flattenOrgsEntity, columnHeaders = columnHeaders, postProcessFunc = postProcessOrgFunc)
    orgDF.to_pickle(filename + '.pickle') # final write

    with open('datasets/orgFoundersDict.pickle', 'wb') as handle:
        pickle.dump(orgFoundersDict, handle) # org uuid & founders uuid

    with open('datasets/founderGenderDict.pickle', 'wb') as handle:
        pickle.dump(founderGender, handle) # founder uuid & genders
