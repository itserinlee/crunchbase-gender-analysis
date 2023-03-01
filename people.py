import requests
import pandas as pd
import pprint

from auth import key
from crunch_library import pullData

url = 'https://api.crunchbase.com/api/v4/searches/people/'

def peopleQuery(limit: int = 1000, uuidLast: str = '', uuidList: list = None, gender: str = None):
    query = {
        "field_ids": [
            "entity_def_id",
            "facet_ids",
            "gender",
            "identifier",
            "name",
            "uuid"
            ],
            "query": [], # because a person does not need to be from the US to have a US company founded
        'limit': limit
    }
    if uuidList is not None:    # only if it is not None, we are going to specify the uuid
        query['query'].append({
                        "type": "predicate",
                        "field_id": "uuid",
                        # "operator_id": "eq",
                        "operator_id": "includes",
                        "values": uuidList
                    })
    if gender is not None:  # only if it is not None, we are going to specify the gender
        query['query'].append({
                        "type": "predicate",
                        "field_id": "gender",
                        "operator_id": "eq",
                        "values": [
                            gender
                        ]
                    })
    return query

def flattenPeopleEntity(entity: dict):


    if 'facet_ids' in entity['properties']:
        facetIds = entity['properties']['facet_ids']
        facetIdsString = ', '.join(facetIds)
    else:
        facetIdsString = 'null'

    if 'gender' not in entity['properties']:    # processes all the blanks
        gender = 'null'                         # currently, hard codes null
    else:
        gender = entity['properties']['gender']

    entityList = [entity['uuid'],
                entity['properties']['name'],
                facetIds,
                gender]

    # print(f'Entity list before return: {entityList=}')
    return entityList

columnHeaders = ['uuid',
                'name',
                'facetIds',
                'gender']

if __name__ == '__main__':
    
    # test values if you wanted to run only this program
    filename = 'datasets/people.csv'
    uuidList = []

    print(pullData(filename, url, peopleQuery(uuidList = uuidList), flattenFunction = flattenPeopleEntity, columnHeaders = columnHeaders))
