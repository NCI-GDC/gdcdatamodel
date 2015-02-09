#!/usr/bin/env python
import logging
import argparse
import json
from itertools import islice
from multiprocessing import Pool
from zug.datamodel import psqlgraph2json
from cdisutils.log import get_logger
from pprint import pprint

log = get_logger("json_generator")
logging.root.setLevel(level=logging.ERROR)
args = None


def get_converter():
    return psqlgraph2json.PsqlGraph2JSON(
        host=args.host,
        user=args.user,
        password=args.password,
        database=args.database,
    )


def print_samples(converter):
    with open('participant.json', 'w') as f:
        f.write(json.dumps([
            converter.denormalize_participant(n)
            for n in islice(converter.get_nodes('participant'), 10)]))
    with open('file.json', 'w') as f:
        f.write(json.dumps([
            converter.denormalize_file(n)
            for n in islice(converter.get_nodes('file'), 10)]))


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
    converter = get_converter()
    with converter.graph.session_scope():
        print_samples(converter)
