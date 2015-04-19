#!/usr/bin/env python
import logging
import argparse
import re
from multiprocessing import Pool
from gdcdatamodel import node_avsc_object, edge_avsc_object
from gdcdatamodel import models
from zug.datamodel import xml2psqlgraph, latest_urls,\
    extract_tar, bcr_xml_mapping, clinical_xml_mapping, prelude
from cdisutils.log import get_logger

log = get_logger("dcc_bio_importer")
logging.root.setLevel(level=logging.ERROR)

re_biospecimen = re.compile(".*(biospecimen|control).*\\.xml")
re_clinical = re.compile(".*(clinical).*\\.xml")
all_reg = ".*(bio).*(Level_1).*\\.xml"
args = None

# These will be set when the process each initialized
biospecimen_converter, clinical_converter = None, None


def get_converter(mapping):
    return xml2psqlgraph.xml2psqlgraph(
        xml_mapping=mapping,
        host=args.host,
        user=args.user,
        password=args.password,
        database=args.database,
    )


def process(archive):

    biospecimen_converter = get_converter(bcr_xml_mapping)
    clinical_converter = get_converter(clinical_xml_mapping)

    url = archive['dcc_archive_url']
    group_id = "{}_{}".format(archive['disease_code'], archive['batch'])
    version = archive['revision']
    log.info(url)

    extractor = extract_tar.ExtractTar(regex=all_reg)
    for xml, name in extractor(url, return_name=True):
        # multiplex on clinical or biospecimen
        if not args.no_biospecimen and re_biospecimen.match(name):
            converter = biospecimen_converter
        elif not args.no_clinical and re_clinical.match(name):
            converter = clinical_converter
        else:
            continue
        converter.xml2psqlgraph(xml)

    # export
    biospecimen_converter.export(group_id=group_id, version=version)
    clinical_converter.export(group_id=group_id, version=version)

    # purge (independent of datatype, just use biospecimen_converter)
    biospecimen_converter.purge_old_nodes(group_id, version)


def import_datatypes():
    log.info('Downloading list of latest files...')
    latest = list(latest_urls.LatestURLParser(
        constraints={'data_level': 'Level_1', 'platform': 'bio'}))
    log.info('Found {} latest files.'.format(len(latest)))
    # p = Pool(args.nproc)
    map(process, latest)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--no-prelude', action='store_true',
                        help='do not import prelude nodes')
    parser.add_argument('--no-biospecimen', action='store_true',
                        help='do not import biospecimen')
    parser.add_argument('--no-clinical', action='store_true',
                        help='do not import clinical')
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

    if args.no_biospecimen and args.no_clinical and args.no_prelude:
        raise RuntimeWarning(
            'Specifying these options leaves no work to be done')

    if not args.no_prelude:
        logging.info("Importing prelude nodes")
        prelude.create_prelude_nodes(get_converter(None).graph)

    if not args.no_biospecimen or not args.no_clinical:
        import_datatypes()
