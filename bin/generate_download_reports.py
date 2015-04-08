#!/usr/bin/env python
from psqlgraph import PsqlGraphDriver
import argparse


def print_source(g, source):
    """Print the totals of downloaded and total data

    """

    files = g.nodes().sysan(dict(source=source))\
                     .props(dict(state='live')).all()

    header = "\t".join(["Filename", "Source", "Protected", "Status", "Digital ID", "Integrity Check md5checksum", "Validity-UUID", "Validity-Barcodes", "Metadata Status", "Data Level Check", "Comments/Notes"])
    with open('{}_report.tsv'.format(source), 'w') as tsv:
        tsv.write(header+'\n')
        for f in files:
            fname = '{}/{}'.format(
                f.system_annotations['analysis_id'], f['file_name'])
            tsv.write('\t'.join([
                fname,
                source.upper().replace('_', ' '),
                'FALSE' if f.acl == ['open'] else 'TRUE',  # protected
                'IMPORTED',                                # import state
                # TODO: Parse for actual status!
                'PASSED',                                  # mdsumed
                'N/A', 'N/A', 'N/A', 'N/A', 'NONE']) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--database', required=True, type=str,
                        help='name of the database to connect to')
    parser.add_argument('-i', '--host', required=True, type=str,
                        help='host of the postgres server')
    parser.add_argument('-u', '--user', required=True, type=str,
                        help='user to connect to postgres as')
    parser.add_argument('-p', '--password', required=True, type=str,
                        help='password for given user. If no '
                        'password given, one will be prompted.')
    args = parser.parse_args()
    g = PsqlGraphDriver(**args.__dict__)

    with g.session_scope() as session:
        for source in {'target_cghub', 'tcga_cghub'}:
            print_source(g, source)
