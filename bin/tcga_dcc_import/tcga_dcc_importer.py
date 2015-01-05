import logging
import json
from zug.datamodel import xml2psqlgraph, latest_urls, extract_tar
from psqlgraph.validate import AvroNodeValidator, AvroEdgeValidator
from avro.schema import make_avsc_object

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

mapping = 'bcr.yaml'
node_schema_file = 'schema_nodes.avsc'
edge_schema_file = 'schema_edges.avsc'
datatype = 'biospecimen'
host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'


def make_validators():
    node_validator = None
    edge_validator = None

    with open(node_schema_file, 'r') as f:
        node_schema = json.loads(f.read())
    with open(edge_schema_file, 'r') as f:
        edge_schema = json.loads(f.read())

    node_validator = AvroNodeValidator(make_avsc_object(node_schema))
    edge_validator = AvroEdgeValidator(make_avsc_object(edge_schema))

    return node_validator, edge_validator


def initialize():
    parser = latest_urls.LatestURLParser(
        constraints={'data_level': 'Level_1', 'platform': 'bio'},
        url_key='dcc_archive_url',
    )
    extractor = extract_tar.ExtractTar(
        regex=".*(bio).*(Level_1).*\\.xml"
    )
    node_validator, edge_validator = make_validators()

    converter = xml2psqlgraph.xml2psqlgraph(
        translate_path=mapping,
        data_type=datatype,
        host=host,
        user=user,
        password=password,
        database=database,
        edge_validator=edge_validator,
        node_validator=node_validator,
    )
    return parser, extractor, converter


def start():
    parser, extractor, converter = initialize()
    for url in parser:
        for xml in extractor(url):
            converter.xml2psqlgraph(xml)

if __name__ == '__main__':
    start()
