import requests
import pandas as pd
import pprint

from auth import key
from crunch_library import pullData


url = 'https://api.crunchbase.com/api/v4/searches/degrees/'

userKey = {'user_key': key}


def degreesQuery(limit: int = 1000, uuidLast: str = '', orgList: list = []):
    query = {
                "field_ids": ["started_on",
                                "completed_on",
                                "identifier",           # value: name of the degree
                                "person_identifier",    # person that received the degree
                                "school_identifier",
                                "subject",
                                "type_name",
                                "updated_at",
                                "uuid"],
                "query": [], 
                "limit": limit
            }
    return query



def flattenDegreesEntity(entity: dict):

    try:
        startedOn = entity['properties']['started_on']['value']
    except:
        startedOn = 'N/A'

    try:
        completedOn = entity['properties']['completed_on']['value']
    except:
        completedOn = 'N/A'

    try:
        fullDegree = entity['properties']['identifier']['value']
    except:
        fullDegree = 'N/A'
      
    try:
        personName = entity['properties']['person_identifier']['value']
    except:
        personName = 'N/A'
    
    try:
        personUuid = entity['properties']['person_identifier']['uuid']
    except:
        personUuid = 'N/A'

    try:
        schoolName = entity['properties']['school_identifier']['value']
    except:
        schoolName = 'N/A'
    
    try:
        schoolUuid = entity['properties']['school_identifier']['uuid']
    except:
        schoolUuid = 'N/A'

    try:
        subject = entity['properties']['subject']
    except:
        subject = 'N/A'

    try:
        degreeAttainment = entity['properties']['type_name']
    except:
        degreeAttainment = 'N/A'
    
    try:
        updatedAt = entity['properties']['updated_at']
    except:
        updatedAt = 'N/A'

    entityList = [entity['uuid'],
                startedOn,
                completedOn,
                fullDegree,
                personName,
                personUuid,
                schoolName,
                schoolUuid,
                subject,
                degreeAttainment,
                updatedAt]

    return entityList



if __name__ == '__main__':

    filename = 'datasets/degrees.csv'

    columnHeaders = ['uuid',
                    'startedOn',
                    'completedOn',
                    'fullDegree',
                    'personName',
                    'personUuid',
                    'schoolName',
                    'schoolUuid',
                    'subject',
                    'degreeAttainment',
                    "updatedAt"]


    degreesDF = pullData(filename, url, degreesQuery(), stopRecord = 10e6, flattenFunction = flattenDegreesEntity, columnHeaders = columnHeaders)
    degreesDF.to_pickle(filename + '.pickle')
