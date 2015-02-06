#!/usr/bin/env python
import argparse
from zug.datamodel.tcga_annotations import TCGAAnnotationImporter
from gdcdatamodel import node_avsc_object, edge_avsc_object
from psqlgraph.validate import AvroNodeValidator, AvroEdgeValidator
from psqlgraph import PsqlGraphDriver
from cdisutils.log import get_logger
from multiprocessing import Pool
from os import getpid
import json
import logging

args = None
log = get_logger('tcga_annotation_importer')
logging.root.setLevel(level=logging.ERROR)


def get_driver():
    return PsqlGraphDriver(
        host=args.host,
        user=args.user,
        password=args.password,
        database=args.database,
        edge_validator=AvroEdgeValidator(edge_avsc_object),
        node_validator=AvroNodeValidator(node_avsc_object),
    )


def process(docs):
    log.info('Process {} handling {} annotations.'.format(getpid(), len(docs)))
    importer = TCGAAnnotationImporter(get_driver())
    map(importer.insert_annotation, docs)


def import_annotations(docs):
    log.info('Found {} annotations.'.format(len(docs)))
    chunksize = len(docs)/args.processes+1
    chunks = [
        docs[i:i+chunksize] for i in xrange(0, len(docs), chunksize)]
    assert sum([len(c) for c in chunks]) == len(docs)
    Pool(args.processes).map(process, chunks)
    log.info('Complete.')


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
    parser.add_argument('-f', '--file', default=None, type=str,
                        help='file to load from')

    args = parser.parse_args()
    importer = TCGAAnnotationImporter(get_driver())
    if args.file:
        with open(args.file, 'r') as f:
            docs = json.loads(f.read())
    else:
        docs = importer.download_annotations()
    import_annotations(docs['dccAnnotation'])
