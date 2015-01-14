import logging
import os
import argparse
from gdcdatamodel import node_avsc_object, edge_avsc_object
from psqlgraph.validate import AvroNodeValidator, AvroEdgeValidator
from zug.datamodel import cghub2psqlgraph, cgquery


logging.basicConfig(level=logging.DEBUG)
current_dir = os.path.dirname(os.path.realpath(__file__))
data_dir = os.path.join(os.path.abspath(
    os.path.join(current_dir, os.path.pardir)), 'data')
mapping = os.path.join(data_dir, 'cghub.yaml')
center_csv_path = os.path.join(data_dir, 'centerCode.csv')
tss_csv_path = os.path.join(data_dir, 'tissueSourceSite.csv')


def initialize(host, user, password, database):

    node_validator = AvroNodeValidator(node_avsc_object)
    edge_validator = AvroEdgeValidator(edge_avsc_object)

    converter = cghub2psqlgraph.cghub2psqlgraph(
        translate_path=mapping,
        host=host,
        user=user,
        password=password,
        database=database,
        edge_validator=edge_validator,
        node_validator=node_validator,
        ignore_missing_properties=True,
    )
    return converter


def incremental_import(source, phsid, days, converter):
    if not days:
        print('Importing all files from TCGA...'.format(days))
        xml = cgquery.get_all(phsid)
    else:
        print('Rebasing past {} days from TCGA...'.format(days))
        xml = cgquery.get_changes_last_x_days(days, phsid)
    converter.parse(xml)
    converter.rebase(source)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--database', default='gdc_datamodel', type=str,
                        help='to odatabase to import to')
    parser.add_argument('-i', '--host', default='localhost', type=str,
                        help='host of the postgres server')
    parser.add_argument('-u', '--user', default='test', type=str,
                        help='the user to import as')
    parser.add_argument('-p', '--password', default='test', type=str,
                        help='the password for import user')
    parser.add_argument('--full_import', action='store_true',
                        help='import all the files')
    parser.add_argument('-t', '--days', default=1, type=int,
                        help='number of days for incremental import')
    args = parser.parse_args()
    converter = initialize(args.host, args.user, args.password, args.database)

    if args.full_import:
        incremental_import('cghub_tcga', 'phs000178', None, converter)
    else:
        incremental_import('cghub_tcga', 'phs000178', args.days, converter)
