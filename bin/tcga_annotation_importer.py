#!/usr/bin/env python
import argparse
from zug.datamodel.tcga_annotations import TCGAAnnotationImporter
from gdcdatamodel import node_avsc_object, edge_avsc_object
from psqlgraph.validate import AvroNodeValidator, AvroEdgeValidator
from psqlgraph import PsqlGraphDriver
from cdisutils.log import get_logger
import logging

args = None
log = get_logger("tcga_annotation_importer")
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
    parser.add_argument('-n', '--nproc', default=8, type=int,
                        help='the number of processes')
    args = parser.parse_args()
    TCGAAnnotationImporter(get_driver()).from_url({'item': 'TCGA-24-2027'})
