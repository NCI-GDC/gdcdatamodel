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


def full_import(source, path, converter):
    logging.info("Reading import {} xml file".format(source))
    with open(path, 'r') as f:
        xml = f.read()

    logging.info("Converting import xml file")
    sa = {'source': source}
    converter.parse(xml)
    converter.purge_files(source)
    converter.export_file_nodes(system_annotations=sa)


def incremental_import(source, path, converter):
    days = 10
    print('Rebasing past {} days from TCGA...'.format(days))
    xml = cgquery.get_changes_last_x_days(days, 'phs000178')
    converter.parse(xml)
    converter.export(source)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--tcga', type=str, default=None,
                        help='tcga xml file to parse')
    parser.add_argument('-d', '--database', default='gdc_datamodel', type=str,
                        help='to odatabase to import to')
    parser.add_argument('-i', '--host', default='localhost', type=str,
                        help='host of the postgres server')
    parser.add_argument('-u', '--user', default='test', type=str,
                        help='the user to import as')
    parser.add_argument('-p', '--password', default='test', type=str,
                        help='the password for import user')
    parser.add_argument('--full_import', action='store_true',
                        help='import all the files, purge those absent. This '
                        'functionality requires that you have already '
                        'downloaded the full xml for all files in a single '
                        'cghub study. Any nodes not in your file will be '
                        'deleted.')
    args = parser.parse_args()

    converter = initialize(args.host, args.user, args.password, args.database)

    if args.full_import:

        if not args.tcga:
            raise Exception('No import files were specified (--tcga=path)')

        if args.tcga:
            full_import('cghub_tcga', args.tcga, args.host, args.user,
                        args.password, args.database)
    else:
        if args.tcga:
            raise Exception('Incremental update: no file paths (--tcga=)')

        incremental_import('cghub_tcga', args.tcga, converter)
