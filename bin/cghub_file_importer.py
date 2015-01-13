import logging
import os
import argparse
from gdcdatamodel import node_avsc_object, edge_avsc_object
from psqlgraph.validate import AvroNodeValidator, AvroEdgeValidator
from zug.datamodel import xml2psqlgraph


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

    converter = xml2psqlgraph.xml2psqlgraph(
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


def start(source, path, *args, **kwargs):
    converter = initialize(*args)

    logging.info("Reading import xml file")
    with open(path, 'r') as f:
        xml = f.read()

    logging.info("Converting import xml file")
    sa = {'source': source}
    converter.xml2psqlgraph(xml)
    converter.purge_files(source)
    converter.export_file_nodes(system_annotations=sa)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--tcga', type=str, help='tcga xml file to parse')
    parser.add_argument('-d', '--database', default='gdc_datamodel', type=str,
                        help='to odatabase to import to')
    parser.add_argument('-i', '--host', default='localhost', type=str,
                        help='host of the postgres server')
    parser.add_argument('-u', '--user', default='test', type=str,
                        help='the user to import as')
    parser.add_argument('-p', '--password', default='test', type=str,
                        help='the password for import user')
    args = parser.parse_args()

    print('Importing TCGA...')
    start('cghub_tcga', args.tcga, args.host, args.user,
          args.password, args.database)
