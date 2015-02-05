#!/usr/bin/env python
import logging
import argparse
import uuid
from gdcdatamodel import node_avsc_object, edge_avsc_object
from psqlgraph.validate import AvroNodeValidator, AvroEdgeValidator
from zug.datamodel import cghub2psqlgraph, cgquery, cghub_xml_mapping
from multiprocessing import Pool
from lxml import etree
from signpostclient import SignpostClient
from cdisutils.log import get_logger

log = get_logger("cghub_file_importer")
logging.root.setLevel(level=logging.INFO)

args, source, phsid = None, None, None


class TestSignpostClient(object):

    def create(self):
        self.did = str(uuid.uuid4())
        return self


def setup():
    if args.no_signpost:
        signpost = TestSignpostClient()
    else:
        signpost = SignpostClient(
            "http://{}:{}".format(args.signpost_host, args.signpost_port),
            version=args.signpost_version)

    node_validator = AvroNodeValidator(node_avsc_object)
    edge_validator = AvroEdgeValidator(edge_avsc_object)
    converter = cghub2psqlgraph.cghub2psqlgraph(
        xml_mapping=cghub_xml_mapping,
        host=args.host,
        user=args.user,
        password=args.password,
        database=args.db,
        edge_validator=edge_validator,
        node_validator=node_validator,
        signpost=signpost,
    )
    return converter


def process(roots):
    converter = setup()
    for root in roots:
        root = etree.fromstring(root)
        converter.parse('file', root)
    converter.rebase(source)


def open_xml():
    log.info('Loading xml from {}...'.format(args.file))
    with open(args.file, 'r') as f:
        xml = f.read()
    return xml


def download_xml():
    # Download the file list
    if args.all:
        log.info('Importing all files from TCGA...'.format(args.days))
        xml = cgquery.get_all(phsid)
    else:
        log.info('Rebasing past {} days from TCGA...'.format(args.days))
        xml = cgquery.get_changes_last_x_days(args.days, phsid)

    if not xml:
        raise Exception('No xml found')
    else:
        log.info('File list downloaded.')
    return xml


def import_files(xml):
    # Split the file into results
    # print xml
    root = etree.fromstring(str(xml)).getroottree()
    roots = [etree.tostring(r) for r in root.xpath('/ResultSet/Result')]
    log.info('Found {} result(s)'.format(len(roots)))
    if not roots:
        log.warn('No results found for past {} days'.format(args.days))
        return

    # Chunk the results and distribute to process pool
    chunksize = len(roots)/args.processes+1
    chunks = [roots[i:i+chunksize]
              for i in xrange(0, len(roots), chunksize)]
    assert sum([len(c) for c in chunks]) == len(roots)
    Pool(args.processes).map(process, chunks)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default='gdc_datamodel', type=str,
                        help='to odatabase to import to')
    parser.add_argument('-i', '--host', default='localhost', type=str,
                        help='host of the postgres server')
    parser.add_argument('-u', '--user', default='test', type=str,
                        help='the user to import as')
    parser.add_argument('-p', '--password', default='test', type=str,
                        help='the password for import user')
    parser.add_argument('--all', action='store_true',
                        help='import all the files')
    parser.add_argument('-d', '--days', default=1, type=int,
                        help='time in days days for incremental import')
    parser.add_argument('-n', '--processes', default=8, type=int,
                        help='number of processes to run import with')
    parser.add_argument('-f', '--file', default=None, type=str,
                        help='file to load from')
    parser.add_argument('-H', '--signpost-host',
                        default='signpost.service.consul', type=str,
                        help='signpost server host')
    parser.add_argument('-P', '--signpost-port', default=80,
                        help='signpost server port')
    parser.add_argument('-V', '--signpost-version', default='v0',
                        help='the version of signpost API')
    parser.add_argument('--no-signpost', action='store_true',
                        help='do not use signpost instance, use random ids')
    args = parser.parse_args()

    source, phsid = 'tcga_cghub', 'phs000178'
    if args.file:
        xml = open_xml()
    else:
        xml = download_xml()
    import_files(xml)
