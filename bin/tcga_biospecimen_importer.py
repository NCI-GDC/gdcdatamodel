import logging
import os
import argparse
from gdcdatamodel import node_avsc_object, edge_avsc_object
from psqlgraph.validate import AvroNodeValidator, AvroEdgeValidator
from zug.datamodel import xml2psqlgraph, latest_urls,\
    extract_tar
from zug.datamodel.import_tcga_code_tables import \
    import_center_codes, import_tissue_source_site_codes

logging.basicConfig(level=logging.DEBUG)

current_dir = os.path.dirname(os.path.realpath(__file__))
data_dir = os.path.join(os.path.abspath(
    os.path.join(current_dir, os.path.pardir)), 'data')
mapping = os.path.join(data_dir, 'bcr.yaml')
center_csv_path = os.path.join(data_dir, 'centerCode.csv')
tss_csv_path = os.path.join(data_dir, 'tissueSourceSite.csv')


def initialize(datatype, host, user, password, database):
    parser = latest_urls.LatestURLParser(
        constraints={'data_level': 'Level_1', 'platform': 'bio'})
    extractor = extract_tar.ExtractTar(
        regex=".*(bio).*(Level_1).*\\.xml")
    node_validator = AvroNodeValidator(node_avsc_object)
    edge_validator = AvroEdgeValidator(edge_avsc_object)

    converter = xml2psqlgraph.xml2psqlgraph(
        translate_path=mapping,
        data_type=datatype,
        host=host,
        user=user,
        password=password,
        database=database,
        edge_validator=edge_validator,
        node_validator=node_validator,
        ignore_missing_properties=True,
    )
    return parser, extractor, converter


def purge_old_nodes(graph, group_id, version):
    with graph.session_scope() as s:
        old_nodes = graph.node_lookup(
            session=s, system_annotation_matches={'group_id': group_id})
        for n in old_nodes:
            if n.system_annotations['version'] < version:
                graph.edge_delete_by_node_id(n.node_id, session=s)
                graph.edge_delete(node=n, session=s)


def start(*args):
    parser, extractor, converter = initialize(*args)

    logging.info("Importing table codes")
    import_center_codes(converter.graph, center_csv_path)
    import_tissue_source_site_codes(converter.graph, tss_csv_path)
    converter.export()

    logging.info("Importing latest xml archives")
    for archive in parser:
        url = archive['dcc_archive_url']
        for xml in extractor(url):
            converter.xml2psqlgraph(xml)
            group_id = "{study}_{batch}".format(
                study=archive['disease_code'], batch=archive['batch'])
            version = archive['revision']
            converter.export(group_id=group_id, version=version)
            purge_old_nodes(converter.graph, group_id, version)

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
    start(args.datatype, args.host, args.user, args.password, args.database)
