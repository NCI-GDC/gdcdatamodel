import logging
import os
import argparse
from gdcdatamodel import node_avsc_object, edge_avsc_object
from psqlgraph.validate import AvroNodeValidator, AvroEdgeValidator
from zug.datamodel import cghub2psqlgraph, cgquery
from multiprocessing import Pool
from lxml import etree

logging.root.setLevel(level=logging.INFO)
log = logging.getLogger(name="cghub_file_importer")

current_dir = os.path.dirname(os.path.realpath(__file__))
data_dir = os.path.join(os.path.abspath(
    os.path.join(current_dir, os.path.pardir)), 'data')
mapping = os.path.join(data_dir, 'cghub.yaml')
center_csv_path = os.path.join(data_dir, 'centerCode.csv')
tss_csv_path = os.path.join(data_dir, 'tissueSourceSite.csv')
args, source, phsid, xml = [None]*4

poolsize = 10


def setup():
    node_validator = AvroNodeValidator(node_avsc_object)
    edge_validator = AvroEdgeValidator(edge_avsc_object)
    converter = cghub2psqlgraph.cghub2psqlgraph(
        translate_path=mapping,
        host=args.host,
        user=args.user,
        password=args.password,
        database=args.db,
        edge_validator=edge_validator,
        node_validator=node_validator,
        ignore_missing_properties=True,
    )
    return converter


def process(roots):
    converter = setup()
    for root in roots:
        root = etree.fromstring(root)
        converter.parse('file', root)
    log.info('Merging {} nodes'.format(
        len(converter.files_to_add)))
    converter.rebase(source)


def import_files():
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

    # Split the file into results
    root = etree.fromstring(str(xml)).getroottree()
    roots = [etree.tostring(r) for r in root.xpath('/ResultSet/Result')]
    log.info('Found {} results'.format(len(roots)))

    # Chunk the results and distribute to process pool
    chunksize = len(roots)/poolsize
    chunks = [roots[i:i+chunksize] for i in xrange(0, len(roots), chunksize)]
    assert sum([len(c) for c in chunks]) == len(roots)
    Pool(poolsize).map(process, chunks)

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
    args = parser.parse_args()

    source, phsid = 'cghub_tcga', 'phs000178'
    import_files()
