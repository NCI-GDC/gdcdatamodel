import logging
from zug.datamodel import xml2psqlgraph, latest_urls, \
    extract_tar, import_tcga_code_tables
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


def start():
    parser, extractor, converter = initialize()
    graph = converter.graph
    import_tcga_code_tables(graph, 'centerCode.csv')

    for url in parser:
        for xml in extractor(url):
            converter.xml2psqlgraph(xml)
    converter.export()

if __name__ == '__main__':
    start()
