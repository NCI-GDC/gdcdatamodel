#!/usr/bin/env python
import logging
import argparse
from psqlgraph import PsqlGraphDriver
from zug.datamodel import psqlgraph2json
from cdisutils.log import get_logger
from elasticsearch import Elasticsearch

log = get_logger("json_generator")
logging.root.setLevel(level=logging.WARNING)
log.setLevel(level=logging.INFO)
args = None


def get_converter():
    return psqlgraph2json.PsqlGraph2JSON(PsqlGraphDriver(
        host=args.host,
        user=args.user,
        password=args.password,
        database=args.database,
    ))


def insert_participants(conv, es):
    log.info('Loading participants')
    participants = list(conv.nodes_labeled('participant'))
    if args.limit:
        participants = participants[:args.limit]
    log.info('Transferring {} participants'.format(len(participants)))
    part_docs, file_docs = conv.denormalize_participants(participants)
    if es:
        conv.es_bulk_upload(es, args.es_index, 'participant', part_docs)
        conv.es_bulk_upload(es, args.es_index, 'file', file_docs)


def insert_projects(conv, es):
    log.info('Loading projects')
    project_docs = conv.denormalize_projects()
    if es:
        conv.es_bulk_upload(es, args.es_index, 'project', project_docs)


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
    parser.add_argument('-n', '--processes', default=8, type=int,
                        help='the number of processes')
    parser.add_argument('-l', '--limit', default=0, type=int,
                        help='limit no. of nodes')
    parser.add_argument('--no-projects', action='store_true',
                        help='do not parse projects')
    parser.add_argument('--no-participants', action='store_true',
                        help='do not parse participants')
    parser.add_argument('--es-index', default='gdc_from_graph', type=str,
                        help='elasticsearch index')
    parser.add_argument('--es-host', default='elasticsearch.service.consul',
                        type=str, help='elasticsearch host')
    parser.add_argument('--no-es', action='store_true',
                        help='do not post to elasticsearch')
    args = parser.parse_args()

    c = get_converter()
    with c.g.session_scope():
        c.cache_database()
        es = None if args.no_es else Elasticsearch(hosts=[args.es_host])
        insert_participants(c, es)
        insert_projects(c, es)
