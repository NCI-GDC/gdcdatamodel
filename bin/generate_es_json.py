#!/usr/bin/env python
import logging
import argparse
from psqlgraph import PsqlGraphDriver
from zug.datamodel.psqlgraph2json import PsqlGraph2JSON
from zug.gdc_elasticsearch import GDCElasticsearch
from cdisutils.log import get_logger
from elasticsearch import Elasticsearch

log = get_logger("json_generator")
logging.root.setLevel(level=logging.WARNING)
log.setLevel(level=logging.INFO)
args = None


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--database', default='gdc_datamodel', type=str,
                        help='the database to import to')
    parser.add_argument('-u', '--user', default='test', type=str,
                        help='the user to import as')
    parser.add_argument('-p', '--password', default='test', type=str,
                        help='the password for import user')
    parser.add_argument('-i', '--host', default='localhost', type=str,
                        help='the postgres server host')
    parser.add_argument('--alias', default='gdc_test', type=str,
                        help='elasticsearch index')
    parser.add_argument('--es-host', default='elasticsearch.service.consul',
                        type=str, help='elasticsearch host')
    args = parser.parse_args()

    # Get graph driver
    g = PsqlGraphDriver(
        host=args.host,
        user=args.user,
        password=args.password,
        database=args.database)

    # Get graph to json converter and load the database
    p2j = PsqlGraph2JSON(g)
    p2j.cache_database()

    # Get json to elasticsearch exporter and deploy the index
    es = GDCElasticsearch(
        Elasticsearch(hosts=[args.es_host], timeout=9999), p2j)
    es.deploy_alias(args.alias)
