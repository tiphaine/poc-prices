import click
import json
import requests
import xmltodict

from collections import defaultdict
from copy import deepcopy
from elasticsearch import Elasticsearch, helpers
from io import BytesIO
from os import listdir, mkdir
from os.path import exists
from zipfile import ZipFile

URL_CARBURANTS_WEEK = 'http://donnees.roulez-eco.fr/opendata/jour'
URL_CARBURANTS_YEAR = 'http://donnees.roulez-eco.fr/opendata/annee'

ES_CARBURANT_MAPPING = {
    "mappings": {
        "pdv": {
            "properties": {
                "id": {"type": "string"},
                "ville": {"type": "string"},
                "adresse": {"type": "string"},
                "saufjour": {"type": "string"},
                "cp": {"type": "string"},
                "localisation": {"type": "geo_point"},
                "prix": { 
                    "dynamic": "true",
                    "properties": {
                        "sp98": {"type": "string"},
                        "sp95": {"type": "string"},
                        "gazole": {"type": "string"},
                        "e10": {"type": "string"}
                    }
                }
            }
        }
    }
}


@click.group()
def cli():
    '''Tools to download, extract, format, and index carburant data from France's opendata.'''
    pass


@cli.command()
@click.option('--data_type', default='week')
@click.option('--destination_dir', default='carburants')
@click.option('--carburants_dump', default='carburants.json')
@click.option('--es_host', default='localhost:9200')
@click.option('--index_name', default='carburants')
@click.option('--doc_type', default='pdv')
@click.option('--mapping', default=ES_CARBURANT_MAPPING)
def extract_and_index(data_type, destination_dir, carburants_dump, es_host, index_name, doc_type, mapping):
    '''Download, extract, format and index latest carburants data.'''
    carburants_pdvs = _get_carburants_xml(data_type, destination_dir)
    carburants_data = _extract_pdv_data(carburants_pdvs['pdv_liste']['pdv'])
    with open(carburants_dump, 'w') as fw:
        json.dump(carburants_data, fw)
    es = Elasticsearch([es_host])
    es.indices.create(index=index_name, ignore=400, body=mapping)
    actions = _format_for_bulk(index_name, doc_type)
    helpers.bulk(es, actions, stats_only=True)



@cli.command()
@click.option('--data_type', default='week')
@click.option('--destination_dir', default='carburants')
@click.option('--carburants_dump', default='carburants.json')
def get_latest_data(data_type, destination_dir, carburants_dump):
    '''Download and extract latest carburant data.'''
    carburants_pdvs = _get_carburants_xml(data_type, destination_dir)
    carburants_data = _extract_pdv_data(carburants_pdvs['pdv_liste']['pdv'])
    with open(carburants_dump, 'w') as fw:
        json.dump(carburants_data, fw)
    return carburants_data


@cli.command()
@click.option('--es_host', default='localhost:9200')
@click.option('--index_name', default='carburants')
@click.option('--doc_type', default='pdv')
@click.option('--mapping', default=ES_CARBURANT_MAPPING)
def index_in_es(es_host, index_name, mapping, doc_type):
    '''Create Elasticsearch index with specific mapping and bulk create data.'''
    es = Elasticsearch([es_host])
    es.indices.create(index=index_name, ignore=400, body=mapping)
    actions = _format_for_bulk(index_name, doc_type)
    helpers.bulk(es, actions, stats_only=True)



@cli.command()
@click.argument('carburant_data')
@click.argument('carburant_bulk')
@click.option('--index_name', default='carburants')
@click.option('--doc_type', default='pdv')
def write_actions_for_bulk(carburants_data, carburant_bulk, index_name, doc_type):
    '''Rewrite data for bulk creation using a command line in a terminal.'''
    with open(carburant_bulk, 'w') as fw:
        for pdv_id, pdv_infos in carburants_data.items():
            pdv_for_es = deepcopy(pdv_infos)
            pdv_for_es['id'] = pdv_id
            fw.write('{ "index" : { "_index" : "%s", "_type" : "%s", "_id" : "%s" } }\n' % (index_name, doc_type, pdv_id))
            fw.write(json.dumps(pdv_for_es) + '\n')
    return carburants_data


def _get_carburants_xml(data_type, destination_dir):
    '''Download and extract carburant data.'''
    if data_type.lower() == 'week':
        url = URL_CARBURANTS_WEEK
    elif data_type.lower() == 'year':
        url = URL_CARBURANTS_YEAR
    request = requests.get(url)
    zf = ZipFile(BytesIO(request.content))
    zf.extractall(destination_dir)
    xml_file = ['{}/{}'.format(destination_dir, file) for file in listdir(destination_dir)][0]
    with open(xml_file, encoding="ISO-8859-1") as fd:
        values = xmltodict.parse(fd.read())
    return values


def _extract_pdv_data(pdvs):
    '''Format carburant data.'''
    data_pdvs = defaultdict(dict)
    for index, pdv in enumerate(pdvs):
        pdv_id = pdv['@id']
        if 'fermeture' not in pdv.keys():
            data_pdvs[pdv_id]['cp'] = pdv['@cp']
            data_pdvs[pdv_id]['localisation'] = {}
            try:
                data_pdvs[pdv_id]['localisation']['lat'] =  float(pdv['@latitude']) / 100000
                data_pdvs[pdv_id]['localisation']['lon'] = float(pdv['@longitude']) / 100000
            except ValueError:
                pass
            data_pdvs[pdv_id]['adresse'] =  pdv['adresse'].lower()
            data_pdvs[pdv_id]['saufjour'] =  pdv['ouverture']['@saufjour'].lower()
            try:
                data_pdvs[pdv_id]['prix'] = _get_prices(pdv['prix'])
            except KeyError:
                pass
            data_pdvs[pdv_id]['ville'] = pdv['ville']
    return data_pdvs



def _format_for_bulk(index_name, doc_type, carburants_dump='carburants.json'):
    '''Obtain actions for bulk creation using elasticsearch-python helpers.'''
    actions = []
    with open(carburants_dump, 'r') as fp:
        for line in fp:
            carburants_data = json.loads(line)
    for pdv_id, pdv_infos in carburants_data.items():
        if len(pdv_infos['localisation'].keys()) == 2:
            action = {}
            action['_index'] = index_name
            action['_type'] = doc_type
            action['_id'] = pdv_id
            action['_source'] = deepcopy(pdv_infos)
            action['_source']['id'] = pdv_id
            actions.append(action)
    return actions
    
def _get_prices(prices):
    '''Format prices.'''
    clean_prices = {}
    if isinstance(prices, dict):
        prices['@nom'] = float(prices['@valeur']) / 1000.0
    else:
        for price in prices:
            clean_prices[price['@nom']] = float(price['@valeur']) / 1000.0
    return clean_prices

if __name__ == '__main__':
    cli()
    