#!/usr/bin/env python
import argparse
from psqlgraph import PsqlGraphDriver
from zug.datamodel.tcga_connect_bio_xml_nodes_to_case\
    import TCGABioXMLCaseConnector

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
    parser.add_argument('--dry-run', action='store_true',
                        help='rollback before commit')

    args = vars(parser.parse_args())
    dry_run = args.pop('dry_run')
    graph = PsqlGraphDriver(**args)
    connector = TCGABioXMLCaseConnector(graph)
    connector.run(dry_run)
