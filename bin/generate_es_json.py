#!/usr/bin/env python
import logging
import argparse
from psqlgraph import PsqlGraphDriver
from zug.datamodel import psqlgraph2json
from cdisutils.log import get_logger
from pprint import pprint
from elasticsearch import Elasticsearch
from multiprocessing import Pool

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


def convert_participant(p):
    conv = get_converter()
    if not args.no_es:
        es = Elasticsearch(hosts=[args.es_host])
    with conv.g.session_scope():
        log.info(p)
        participant, files = conv.denormalize_participant(p)
    if not args.no_es:
        res = es.index(
            index=args.es_index, doc_type="participant", body=participant)
        log.info(res)
        for f in files:
            res = es.index(index=args.es_index, doc_type="file", body=f)
            log.info(res)


def convert_project(p):
    conv = get_converter()
    if not args.no_es:
        es = Elasticsearch(hosts=[args.es_host])
    with conv.g.session_scope():
        log.info(p)
        project = conv.denormalize_project(p)
        pprint(project)
    if not args.no_es:
        res = es.index(index=args.es_index, doc_type="project", body=project)
        log.info(res)


def convert_participants(conv):
    log.info('Loading participants')
    if args.limit:
        participants = conv.get_nodes('participant').limit(args.limit).all()
    else:
        participants = conv.get_nodes('participant').all()
    log.info('Found {} participants'.format(len(participants)))
    pool = Pool(args.processes)
    pool.map(convert_participant, participants)


def convert_projects(conv):
    log.info('Loading projects')
    if args.limit:
        projects = conv.get_nodes('project').limit(args.limit).all()
    else:
        projects = conv.get_nodes('project').all()
    log.info('Found {} projects'.format(len(projects)))
    pool = Pool(args.processes)
    pool.map(convert_project, projects)


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
        # if not args.no_projects:
        #     convert_projects(converter)
        # if not args.no_participants:
        #     convert_participants(converter)
        # project = c.g.nodes().ids('1334612b-3d2e-5941-a476-d455d71b458f').one()
        # pprint(convert_project(project))
        c.cache_dev_participant()
        p = c.nodes_labeled('participant').next()
        pprint(c.denormalize_participant(p)[1][0])
