import logging
import argparse
from multiprocessing import Pool
from gdcdatamodel import node_avsc_object, edge_avsc_object
from psqlgraph.validate import AvroNodeValidator, AvroEdgeValidator
from zug.datamodel import xml2psqlgraph, latest_urls,\
    extract_tar, bcr_xml_mapping, prelude

logging.basicConfig(level=logging.DEBUG)

args = None


def initialize():
    extractor = extract_tar.ExtractTar(
        regex=".*(bio).*(Level_1).*\\.xml")
    node_validator = AvroNodeValidator(node_avsc_object)
    edge_validator = AvroEdgeValidator(edge_avsc_object)

    converter = xml2psqlgraph.xml2psqlgraph(
        xml_mapping=bcr_xml_mapping,
        host=args.host,
        user=args.user,
        password=args.password,
        database=args.database,
        edge_validator=edge_validator,
        node_validator=node_validator,
    )
    return extractor, converter


def process(archive):
    extractor, converter = initialize()
    url = archive['dcc_archive_url']
    for xml in extractor(url):
        converter.xml2psqlgraph(xml)
        group_id = "{study}_{batch}".format(
            study=archive['disease_code'], batch=archive['batch'])
        version = archive['revision']
        converter.export(group_id=group_id, version=version)
        converter.purge_old_nodes(group_id, version)


def start():
    extractor, converter = initialize()

    logging.info("Importing prelude nodes")
    prelude.create_prelude_nodes(converter.graph)
    latest = latest_urls.LatestURLParser(
        constraints={'data_level': 'Level_1', 'platform': 'bio'})

    logging.info("Importing latest xml archives")
    p = Pool(8)
    p.map(process, list(latest))
    # process(list(latest)[0])

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--datatype', default='biospecimen', type=str,
                        help='the datatype to filter')
    parser.add_argument('-d', '--database', default='gdc_datamodel', type=str,
                        help='to odatabase to import to')
    parser.add_argument('-u', '--user', default='test', type=str,
                        help='the user to import as')
    parser.add_argument('-p', '--password', default='test', type=str,
                        help='the password for import user')
    parser.add_argument('-i', '--host', default='localhost', type=str,
                        help='the postgres server host')
    args = parser.parse_args()
    start()
