import logging
import argparse
from multiprocessing import Pool
from gdcdatamodel import node_avsc_object, edge_avsc_object
from psqlgraph.validate import AvroNodeValidator, AvroEdgeValidator
from zug.datamodel import xml2psqlgraph, latest_urls,\
    extract_tar, bcr_xml_mapping, clinical_xml_mapping, prelude
from cdisutils.log import get_logger

log = get_logger("dcc_bio_importer")
logging.root.setLevel(level=logging.INFO)

args = None


def get_converter(mapping):
    return xml2psqlgraph.xml2psqlgraph(
        xml_mapping=mapping,
        host=args.host,
        user=args.user,
        password=args.password,
        database=args.database,
        edge_validator=AvroEdgeValidator(edge_avsc_object),
        node_validator=AvroNodeValidator(node_avsc_object),
    )


def process(args):
    archive, mapping, datatype = args
    converter = get_converter(mapping)
    url = archive['dcc_archive_url']
    extractor = extract_tar.ExtractTar(
        regex=".*(bio).*(Level_1).*({datatype}).*\\.xml".format(
            datatype=datatype))
    for xml in extractor(url):
        converter.xml2psqlgraph(xml)
        group_id = "{study}_{batch}".format(
            study=archive['disease_code'], batch=archive['batch'])
        version = archive['revision']
        converter.export(group_id=group_id, version=version)
        converter.purge_old_nodes(group_id, version)


def import_datatype(mapping, datatype):
    logging.info('Importing {} data'.format(datatype))
    latest = list(latest_urls.LatestURLParser(
        constraints={'data_level': 'Level_1', 'platform': 'bio'}))
    process((latest[0], mapping, datatype))
    # p = Pool(args.nproc)
    # p.map(process, zip(latest, [datatype]*len(latest)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--no-biospecimen', action='store_true',
                        help='import biospecimen')
    parser.add_argument('--no-clinical', action='store_true',
                        help='import clinical')
    parser.add_argument('-d', '--database', default='gdc_datamodel', type=str,
                        help='the database to import to')
    parser.add_argument('-u', '--user', default='test', type=str,
                        help='the user to import as')
    parser.add_argument('-p', '--password', default='test', type=str,
                        help='the password for import user')
    parser.add_argument('-i', '--host', default='localhost', type=str,
                        help='the postgres server host')
    parser.add_argument('-n', '--nproc', default=8, type=int,
                        help='the number of processes')
    args = parser.parse_args()

    # logging.info("Importing prelude nodes")
    # prelude.create_prelude_nodes(get_converter(None).graph)

    if args.no_biospecimen and args.no_clinical:
        raise RuntimeWarning(
            'Specifying these options leaves no work to be done')
    if not args.no_biospecimen:
        import_datatype(bcr_xml_mapping, 'biospecimen')
    if not args.no_clinical:
        import_datatype(clinical_xml_mapping, 'clinical')
