#!/usr/bin/env python
import os
import logging
import argparse
import uuid
from gdcdatamodel import models
from zug.datamodel import cghub2psqlgraph, cgquery, cghub_xml_mapping, cghub_xml_metadata
from multiprocessing import Pool
from lxml import etree
from signpostclient import SignpostClient
from cdisutils.log import get_logger
from boto import connect_s3
import boto.s3.connection

log = get_logger("cghub_file_importer")
logging.root.setLevel(level=logging.INFO)
all_phsids = '(phs000218 OR phs0004* OR phs000515 OR phs000178)'

args = None


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


    converter = cghub2psqlgraph.cghub2psqlgraph(
        xml_mapping=cghub_xml_mapping,
        host=args.host,
        user=args.user,
        password=args.password,
        database=args.db,
        signpost=signpost,
    )

    return converter


def process(roots):
    converter = setup()

    s3conn = connect_s3(
                aws_access_key_id=args.s3access,
                aws_secret_access_key=args.s3secret,
                host=args.s3host,
                is_secure=True,
                calling_format=boto.s3.connection.OrdinaryCallingFormat()
    )

    
    extractor = cghub_xml_metadata.Extractor( 
        signpost=converter.signpost,
        s3=s3conn,
        bucket=args.s3bucket,
        graph=converter.graph
    )

    with converter.graph.session_scope() as session:
        for root in roots:
            root = etree.fromstring(root)
            converter.parse('file', root)
        converter.rebase()

        for root in roots:
            root = etree.fromstring(root)
            extractor.process(root)

        if args.dry_run:
            log.warn('Rolling back session as requested.')
            session.rollback()

def open_xml():
    log.info('Loading xml from {}...'.format(args.file))
    with open(args.file, 'r') as f:
        xml = f.read()
    return xml


def download_xml():
    # Download the file list
    if args.all:
        log.info('Importing all files from TCGA...'.format(args.days))
        xml = cgquery.get_all(all_phsids)
    else:
        log.info('Rebasing past {} days from TCGA...'.format(args.days))
        xml = cgquery.get_changes_last_x_days(args.days, all_phsids)

    if not xml:
        raise Exception('No xml found')
    else:
        log.info('File list downloaded.')
    return xml


def import_files(xml):
    # Split the file into results
    root = etree.fromstring(str(xml)).getroottree()
    roots = [etree.tostring(r) for r in root.xpath('/ResultSet/Result')]
    log.info('Found {} result(s)'.format(len(roots)))
    if not roots:
        log.warn('No results found for past {} days or from file.'.format(
            args.days))
        return

    # Chunk the results and distribute to process pool
    chunksize = len(roots)/args.processes+1
    chunks = [roots[i:i+chunksize]
              for i in xrange(0, len(roots), chunksize)]
    assert sum([len(c) for c in chunks]) == len(roots)
    if args.processes == 1:
        log.info('Processing serially')
        map(process, chunks)
    else:
        log.info('Processing with pool size {}'.format(args.processes))
        res = Pool(args.processes).map_async(process, chunks)
        res.get(int(1e9))
    log.info('Complete.')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default=os.environ.get('DB_NAME','gdc_datamodel'),
                        type=str, help='the database to import to')
    parser.add_argument('-i', '--host', default=os.environ.get('DB_HOST', 'localhost'),
                        type=str, help='host of the postgres server')
    parser.add_argument('-u', '--user', default=os.environ.get('DB_USER','test'),
                        type=str, help='the user to import as')
    parser.add_argument('-p', '--password', default=os.environ.get('DB_PASS','test'),
                        type=str, help='the password for import user')
    parser.add_argument('--s3host', default=os.environ.get('S3_HOST','gdc-accessor6.osdc.io'),
                        type=str, help='the host for s3')
    parser.add_argument('--s3access', default=os.environ.get('S3_ACC_KEY','test'),
                        type=str, help='the access key for s3')
    parser.add_argument('--s3secret', default=os.environ.get('S3_SEC_KEY','test'),
                        type=str, help='the secret key for s3')
    parser.add_argument('--s3bucket', default=os.environ.get('S3_BUCKET','test_dev_pool1'),
                        type=str, help='the bucket for s3')
    parser.add_argument('--all', action='store_true',
                        help='import all the files')
    parser.add_argument('-d', '--days', default=1, type=int,
                        help='time in days days for incremental import')
    parser.add_argument('-n', '--processes', default=8, type=int,
                        help='number of processes to run import with')
    parser.add_argument('-f', '--file', default=None, type=str,
                        help='file to load from')
    parser.add_argument('-H', '--signpost-host',
                        default=os.environ.get('SIGNPOST_HOST','http://signpost.service.consul'),
                        type=str, help='signpost server host')
    parser.add_argument('-P', '--signpost-port', default=os.environ.get('SIGNPOST_PORT',80),
                        type=int, help='signpost server port')
    parser.add_argument('-V', '--signpost-version', default='v0',
                        help='the version of signpost API')
    parser.add_argument('--no-signpost', action='store_true',
                        help='do not add the files to signpost')
    parser.add_argument('--dry-run', action='store_true',
                        help='Do not commit any sessions')

    args = parser.parse_args()

    if args.dry_run:
        log.warn('Dry run: forcing --no-signpost')
        args.no_signpost = True

    if args.file:
        xml = open_xml()
    else:
        xml = download_xml()
    import_files(xml)

