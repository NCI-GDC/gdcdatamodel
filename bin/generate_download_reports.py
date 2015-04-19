#!/usr/bin/env python
from psqlgraph import PsqlGraphDriver
import argparse
import json

from zug.datamodel.latest_urls import LatestURLParser
from zug.datamodel.tcga_magetab_sync import get_submitter_id_and_rev


def find_node(archive, nodes):
    submitter_id, rev = get_submitter_id_and_rev(archive["archive_name"])
    for node in nodes:
        if node["submitter_id"] == submitter_id:
            return node
    return None


def print_dcc(g):
    results = ["\t".join(["Archive name", "Source", "Protected", "Status", "Digital ID", "Integrity Check", "md5checksum", "Metadata Status", "Comments/Notes"])]
    archives = list(LatestURLParser())
    nodes = g.nodes().labels("archive").all()
    for archive in archives:
        node = find_node(archive, nodes)
        if not node:
            results.append("\t".join([archive["archive_name"],
                                     "TCGA_DCC",
                                      str(archive["protected"]).upper(),
                                      "NOT_STARTED",
                                      "N/A",
                                      "N/A", "N/A", "N/A", "NONE"]))
        else:
            status = "IMPORTED" if node["revision"] == archive["revision"] else "OLD_REVISION"
            results.append("\t".join([archive["archive_name"],
                                     "TCGA_DCC",
                                      str(archive["protected"]).upper(),
                                      status,
                                      node.node_id,
                                      "N/A", "N/A", "N/A", "NONE"]))
        tsv_content = "\n".join(results)
    with open('{}_report.tsv'.format('tcga_dcc'), 'w') as tsv:
        tsv.write(tsv_content)


def print_target_dcc(g):
    results = ["\t".join(["File name", "Source", "Protected", "Status", "Digital ID", "Integrity Check", "md5checksum", "Metadata Status", "Comments/Notes"])]
    nodes = g.nodes().labels("file").sysan(dict(source='target_dcc')).all()
    for node in nodes:
        results.append("\t".join([node["file_name"],
                                  "TARGET_DCC",
                                  'TRUE',
                                  "IMPORTED",
                                  "N/A",
                                  "N/A", "N/A", "N/A", "NONE"]))
    tsv_content = "\n".join(results)
    with open('{}_report.tsv'.format('target_dcc'), 'w') as tsv:
        tsv.write(tsv_content)


def get_bad_files(path):
    with open(path, 'r') as f:
        text = f.read().strip().split('\n')
    rows = {json.loads(':'.join(r.split(':')[1:]))['file_name'] for r in text}
    return rows


def is_bad_file(fname, bad_files):
    if fname in bad_files:
        return True
    return False


def print_cghub(g, source):
    """Print the totals of downloaded and total data

    """

    bad_files = get_bad_files('actual_bad_files.log')
    files = g.nodes().sysan(dict(source=source))\
                     .props(dict(state='live')).all()

    header = "\t".join(["Filename", "Source", "Protected", "Status", "Digital ID", "Integrity Check md5checksum", "Validity-UUID", "Validity-Barcodes", "Metadata Status", "Data Level Check", "Comments/Notes"])
    with open('{}_report.tsv'.format(source), 'w') as tsv:
        tsv.write(header+'\n')
        for f in files:
            fname = '{}/{}'.format(
                f.system_annotations['analysis_id'], f['file_name'])
            m5 = 'NOT STARTED' if is_bad_file(fname, bad_files) else 'PASSED'
            tsv.write('\t'.join([
                fname,
                source.upper().replace('_', ' '),
                'FALSE' if f.acl == ['open'] else 'TRUE',  # protected
                'IMPORTED',                                # import state
                f.node_id,
                m5,  # md5summed
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
        print_dcc(g)
        print_target_dcc(g)
        for source in {'target_cghub', 'tcga_cghub'}:
            print_cghub(g, source)
