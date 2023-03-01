import requests
import pandas as pd
import pprint

from auth import key
from crunch_library import pullData


url = 'https://api.crunchbase.com/api/v4/searches/funding_rounds/'

userKey = {'user_key': key}


def fundingRoundsQuery(limit: int = 1000, uuidLast: str = '', orgList: list = []):
    query = {
                "field_ids": ["funded_organization_identifier",
                                "announced_on",
                                "closed_on",
                                "identifier",
                                "money_raised",
                                "permalink",
                                "updated_at",
                                "uuid"
                            ],
                "query": [
                            {
                                "type": "predicate",
                                "field_id": "funded_organization_location",
                                "operator_id": "includes",
                                "values": 
                                    {"uuid": "f110fca2-1055-99f6-996d-011c198b3928"}
                            }
                        ], 
                "limit": limit
            }
    return query



def flattenFundingRoundsEntity(entity: dict):

    try:
        role = entity['properties']['funded_organization_identifier']['role']
    except:
        role = 'N/A'
    
    try:
        fundedOrganizationIdentifierUuid = entity['properties']['funded_organization_identifier']['uuid']
    except:
        fundedOrganizationIdentifierUuid = 'N/A'

    try:
        fundedOrganizationIdentifier = entity['properties']['funded_organization_identifier']['value']
    except:
        fundedOrganizationIdentifier = 'N/A'

    try:
        announcedOn = entity['properties']['announced_on']
    except:
        announcedOn = 'N/A'
    
    try:
        closedOn = entity['properties']['closed_on']['value']
    except:
        closedOn = 'N/A'

    try:
        identifierValue = entity['properties']['identifier']['value']
    except:
        identifierValue = 'N/A'

    try:
        moneyRaisedUSD = entity['properties']['money_raised']['value_usd']
    except:
        moneyRaisedUSD = 'N/A'

    try:
        updatedAt = entity['properties']['updated_at']
    except:
        updatedAt = 'N/A'
    

    entityList = [entity['uuid'],
                role,
                fundedOrganizationIdentifierUuid,
                fundedOrganizationIdentifier,
                announcedOn,
                closedOn,
                identifierValue,
                moneyRaisedUSD,
                updatedAt]

    return entityList



if __name__ == '__main__':

    filename = 'datasets/funding_rounds.csv'

    columnHeaders = ['uuid',
                    'role',
                    'fundedOrganizationIdentifierUuid',
                    'fundedOrganizationIdentifier',
                    'announcedOn',
                    'closedOn',                     # date that the funding round was closed -- not the same as "closed_on" in orgs table
                    'identifierValue',
                    'moneyRaisedUSD',
                    'updatedAt']
    
    
    fundingDF = pullData(filename, url, fundingRoundsQuery(), stopRecord = 10e6, flattenFunction = flattenFundingRoundsEntity, columnHeaders = columnHeaders)
    fundingDF.to_pickle(filename + '.pickle')
