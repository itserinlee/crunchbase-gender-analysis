import requests
import pandas as pd
import pprint

from auth import key
from crunch_library import pullData


url = 'https://api.crunchbase.com/api/v4/searches/investments/'

userKey = {'user_key': key}


def investmentsQuery(limit: int = 1000, uuidLast: str = '', orgList: list = []):
    query = {
                "field_ids": [
                                "funding_round_identifier",
                                "investor_identifier",
                                "investor_stage",
                                "investor_type",
                                "is_lead_investor",
                                "organization_diversity_spotlights",
                                "money_invested",
                                "name",
                                "organization_identifier",
                                "updated_at",
                                "uuid"
                            ],
                "query": [], 
                "limit": limit
            }
    return query



def flattenInvestmentsEntity(entity: dict):

    try:
        fundingRoundIdentifier = entity['properties']['funding_round_identifier']['value']
    except:
        fundingRoundIdentifier = 'N/A'

    try:
        investorUuid = entity['properties']['investor_identifier']['uuid']
    except:
        investorUuid = 'N/A'
      
    try:
        investorIdentifier = entity['properties']['investor_identifier']['value']
    except:
        investorIdentifier = 'N/A'

    try:
        investorStageList = entity['properties']['investor_stage']
        investorStage = ', '.join(investorStageList)
    except:
        investorStage = 'N/A'

    try:
        investorType = entity['properties']['investorType']
    except:
        investorType = 'N/A'
    
    try:
        diversitySpotlightsList = entity['properties']['diversity_spotlights']
        organizationDiversitySpotlights = ', '.join(map(lambda x: x['value'], diversitySpotlightsList))
    except:
        organizationDiversitySpotlights = 'N/A'
    
    try:
        moneyInvested = entity['properties']['money_invested']['value_usd']
    except:
        moneyInvested = 'N/A'
    
    try:
        organizationUuid = entity['properties']['organization_identifier']['uuid']
    except:
        organizationUuid = 'N/A'
    
    try:
        organizationName = entity['properties']['organization_identifier']['value']
    except:
        organizationName = 'N/A'

    try:
        investorName = entity['properties']['name']
    except:
        investorName = 'N/A'

    try:
        isLeadInvestor = entity['properties']['is_lead_investor']
    except:
        isLeadInvestor = 'N/A'  
    
    try:
        updatedAt = entity['properties']['updated_at']
    except:
        updatedAt = 'N/A'

    entityList = [entity['uuid'],
                fundingRoundIdentifier,
                investorUuid,
                investorIdentifier,
                moneyInvested,
                organizationUuid,
                organizationName,
                investorName,
                investorStage,
                investorType,
                isLeadInvestor,
                organizationDiversitySpotlights,
                updatedAt]

    return entityList



if __name__ == '__main__':

    filename = 'datasets/investments.csv'

    columnHeaders = ['uuid',
                    "fundingRoundIdentifier",
                    "investorUuid",
                    "investorIdentifier",
                    "moneyInvested",
                    "organizationUuid",
                    "organizationName",
                    "investorName",
                    "investorStage",
                    "investorType",
                    "isLeadInvestor",
                    "organizationDiversitySpotlights",
                    "updatedAt"]


    investmentsDF = pullData(filename, url, investmentsQuery(), stopRecord = 10e6, flattenFunction = flattenInvestmentsEntity, columnHeaders = columnHeaders)
    investmentsDF.to_pickle(filename + '.pickle')
