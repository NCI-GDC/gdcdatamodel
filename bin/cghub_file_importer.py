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
args = None


def setup():
    node_validator = AvroNodeValidator(node_avsc_object)
    edge_validator = AvroEdgeValidator(edge_avsc_object)
    converter = cghub2psqlgraph.cghub2psqlgraph(
        translate_path=mapping,
        host=args.host,
        user=args.user,
        password=args.password,
        database=args.database,
        edge_validator=edge_validator,
        node_validator=node_validator,
        ignore_missing_properties=True,
    )
    return converter


def import_files(source, phsid):
    converter = setup()

    if not args.days:
        print('Importing all files from TCGA...'.format(args.days))
        xml = cgquery.get_all(phsid)
    else:
        print('Rebasing past {} days from TCGA...'.format(args.days))
        xml = cgquery.get_changes_last_x_days(args.days, phsid)
    # converter.parse(xml)
    # converter.rebase(source)
    converter.initialize(xml)
    print converter.node_roots['file'][0]

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

    if args.full_import:
        import_files('cghub_tcga', 'phs000178')
    else:
        import_files('cghub_tcga', 'phs000178')
