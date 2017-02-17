#!/usr/bin/env python

"""gdcdatamodel.migrations.node_shrouder
----------------------------------

Given requests to make certain nodes not visible, this code will take
a UUID or file with a list of UUIDs and make sure they're not visible
from esbuild or downloadable thru the API.

The easiest way to ensure these files are not used is to set their
acl to []. That effectively removes their acl, so the previous
acl will be stored in the system annotations, as well as a date
on which this node was made invisible.

Origin: https://jira.opensciencedatacloud.org/browse/DAT-586

"""

import os
import sys
import logging

from sqlalchemy import not_, or_, and_
from psqlgraph import Node, PsqlGraphDriver
from gdcdatamodel import models as md
from multiprocessing import Process, cpu_count, Queue
from collections import namedtuple
from argparse import ArgumentParser
import datetime

logger = logging.getLogger("node_shrouder")
logging.basicConfig(level=logging.INFO)

def load_data_file(file_name):
    """Loads data from a given file, getting it either from a tsv, csv, or
    json, returning it as a list of dicts"""
    key_data = []
    header = None
    delimiters = {  'tsv': '\t',
                    'csv': ','}
    delimiter = None

    # figure out what the delimiter might be, if one
    for ext in delimiters.keys():
        if ext in file_name:
            delimiter = delimiters[ext]

    with open(file_name, "r") as in_file:
        # load as json
        if 'json' in file_name:
            for line in in_file:
                line_data = json.loads(line)
                key_data.append(line_data)
        # load as tsv/csv, assuming the first row is the header
        # that provides keys for the dict
        else:
            if delimiter:
                for line in in_file:
                    if len(line.strip('\n').strip()):
                        if not header:
                            header = line.strip('\n').split(delimiter)
                        else:
                            line_data = dict(zip(header, line.strip('\n')\
                                                        .split(delimiter)))
                            key_data.append(line_data)
            else:
                log.warn("Unable to load %s, can't find delimiter" % file_name)

    return key_data

def shroud_nodes(db_kwargs=None,
                 uuid_list=None,
                 count=None,
                 dry_run=False):
    graph = PsqlGraphDriver(**db_kwargs)
    total_count = 0
    shrouded_count = 0
    with graph.session_scope() as session:
        shrouded_time = datetime.datetime.now().isoformat()
        for entry in uuid_list:
            node = graph.nodes().get(entry)
            if node:
                logger.info('Shrouding {}'.format(entry))
                # blank the acl so it's not visible
                node.acl = []

                # update the sysan
                node.sysan['shrouded'] = shrouded_time
                shrouded_count += 1

            else:
                logger.warn('Unable to find node {}'.format(entry))
            total_count += 1
            if count:
                if shrouded_count >= count:
                    break
        if dry_run:
            logger.info('dry_run requested, rolling back session')
            session.rollback()
    print '{}/{} nodes processed'.format(shrouded_count, total_count)

if __name__ == '__main__':
    parser = ArgumentParser()

    parser.add_argument("--uuids",
        help='list of UUIDs to make invisible',
        nargs='+')
    parser.add_argument("--uuid_file",
        help='file with list of UUIDs to make invisible')
    parser.add_argument("--dry_run",
        help='do not commit results',
        action='store_true')
    parser.add_argument('--count',
        help='how many of the given UUIDs to process (not counting any not found)',
        type=int)

    args = parser.parse_args()

    if not args.uuids and not args.uuid_file:
        print "Must provide a list of UUIDs or a file with UUIDs"

    else:
        db_kwargs = {
            'host':os.environ['PG_HOST'],
            'user':os.environ['PG_USER'],
            'database':os.environ['PG_NAME'],
            'password':os.environ['PG_PASS']
        }
        uuids = []
        if args.uuid_file:
            file_data = load_data_file(args.uuid_file)
            uuids = [line['file_id'] for line in file_data]
        else:
            uuids = args.uuids

        shroud_nodes(db_kwargs=db_kwargs,
                     uuid_list=uuids,
                     dry_run=args.dry_run,
                     count=args.count
        )
        

