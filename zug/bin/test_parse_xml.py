import logging
from zug.datamodel import xml2psqlgraph

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

mapping = 'bcr.yaml'
datatype = 'biospecimen'
host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'


def initialize(path):
    with open(path) as f:
        xml = f.read()
    converter = xml2psqlgraph.xml2psqlgraph(
        translate_path=mapping,
        data_type=datatype,
        host=host,
        user=user,
        password=password,
        database=database
    )
    return xml, converter


def start():
    xml, converter = initialize('sample.bio.xml')
    converter.add_to_graph(xml)

if __name__ == '__main__':
    start()
