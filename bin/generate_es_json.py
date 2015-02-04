import logging
import argparse
import re
from itertools import islice
from pprint import pprint
from multiprocessing import Pool
from zug.datamodel import psqlgraph2json
from cdisutils.log import get_logger

log = get_logger("dcc_bio_importer")
logging.root.setLevel(level=logging.ERROR)

re_biospecimen = re.compile(".*(biospecimen).*\\.xml")
re_clinical = re.compile(".*(clinical).*\\.xml")
all_reg = ".*(bio).*(Level_1).*\\.xml"
args = None

# These will be set when the process each initialized
converter = None


def get_converter():
    return psqlgraph2json.PsqlGraph2JSON(
        host=args.host,
        user=args.user,
        password=args.password,
        database=args.database,
    )


def start_conversion():
    converter = get_converter()
    # for f in islice(converter.get_files(), 20):
    #     pprint(converter.denormalize_file(f))
    for f in islice(converter.get_participants(), 20):
        pprint(converter.denormalize_participant(f))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
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
    start_conversion()
