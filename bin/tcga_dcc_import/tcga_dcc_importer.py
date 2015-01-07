import logging
import csv
from zug.datamodel import xml2psqlgraph, latest_urls, extract_tar
from psqlgraph.validate import AvroNodeValidator, AvroEdgeValidator
from gdcdatamodel import node_avsc_object, edge_avsc_object

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

mapping = 'bcr.yaml'
datatype = 'biospecimen'
host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'


def initialize():
    parser = latest_urls.LatestURLParser(
        constraints={'data_level': 'Level_1', 'platform': 'bio'},
        url_key='dcc_archive_url',
    )
    extractor = extract_tar.ExtractTar(
        regex=".*(bio).*(Level_1).*\\.xml"
    )
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


def import_table_csv(graph, path):
    with open(path, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            graph.node_merge(
                node_id=row[0],
                label='center',
                properties={
                    'center_name': row[1],
                    'full_name': row[3],
                    'short_name': row[4],
                    'center_type': row[2],
                })


def start():
    parser, extractor, converter = initialize()
    graph = converter.graph
    import_table_csv(graph, 'centerCode.csv')

    for url in parser:
        for xml in extractor(url):
            converter.xml2psqlgraph(xml)
    converter.export()

if __name__ == '__main__':
    start()
