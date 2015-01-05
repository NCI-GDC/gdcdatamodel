import logging
from zug.datamodel import xml2psqlgraph, latest_urls, extract_tar

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
    converter = xml2psqlgraph.xml2psqlgraph(
        translate_path=mapping,
        data_type=datatype,
        host=host,
        user=user,
        password=password,
        database=database
    )
    return parser, extractor, converter


def start():
    parser, extractor, converter = initialize()
    for url in parser:
        for xml in extractor(url):
            converter.xml2psqlgraph(xml)

if __name__ == '__main__':
    start()
