import os
import logging
import argparse
from lxml import etree
from cdisutils.log import get_logger
from zug.datamodel import cgquery
from zug.datamodel.ccle_importer import CCLEImporter

ccle_physids = '*Other_Sequencing_Multiisolate'

log = get_logger('ccle_import')
logging.root.setLevel(level=logging.INFO)

def download_xml():
    log.info('Downloading all ccle data from cghub')
    xml = cgquery.get_changes_last_x_days(args.days, all_phsids)
    if not xml:
        raise Exception('No xml found')
    else:
        log.info('Data downloaded')
    return xml

def import_chunk(importer, xml):
    root = etree.fromstring(str(xml)).getroottree()

    for r in root.xpath('/ResultSet/Result'):
        importer.import_xml_node(r)

def import_data():
    importer = CCLEImporter(args.host, args.user, args.password, args.db)
    # Bit of a hack to modify the api endpoint to avoid large downloads
    cgquery.url = 'https://cghub.ucsc.edu/cghub/metadata/analysisDetail'

    with importer.g.session_scope() as session:
        # Load in and process by chunk
        row = 0
        xml = cgquery.get_n_rows(row, args.chunk_size, ccle_physids)
        while '<downloadable_file_count>0</downloadable_file_count>' not in xml:
            import_chunk(importer, xml)
            row += args.chunk_size
            xml = cgquery.get_n_rows(row, args.chunk_size, ccle_physids)

        if args.dry_run:
            log.info('Rolling back session changes')
            session.rollback()

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
    parser.add_argument('--dry-run', action='store_true',
                        help='Do not commit any sessions')
    parser.add_argument('-n', '--processes', default=4, type=int,
                        help='Number of processes to use')
    parser.add_argument('-c', '--chunk-size', default=128, type=int,
                        help='Number of rows to query for at a time')

    args = parser.parse_args()

    if args.dry_run:
        log.warn('Running in dry mode, No changes will be commited')

    import_data()

