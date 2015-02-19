#!/usr/bin/env python
import logging
import argparse
from gdcdatamodel import node_avsc_object, edge_avsc_object
from psqlgraph.validate import AvroNodeValidator, AvroEdgeValidator
from psqlgraph import PsqlGraphDriver, Node
from cdisutils.log import get_logger
from progressbar import ProgressBar, Percentage, Bar, ETA

log = get_logger("tcga_connect_bio_xml_nodes_to_participant")
logging.root.setLevel(level=logging.ERROR)

biospecimen_base = 'nationwidechildrens.org_biospecimen.{barcode}.xml'
control_base = 'nationwidechildrens.org_control.{barcode}.xml'
clinical_base = 'nationwidechildrens.org_clinical.{barcode}.xml'

args = None


def get_driver():
    return PsqlGraphDriver(
        host=args.host,
        user=args.user,
        password=args.password,
        database=args.database,
        edge_validator=AvroEdgeValidator(edge_avsc_object),
        node_validator=AvroNodeValidator(node_avsc_object),
    )


def get_pbar(title, maxval):
    pbar = ProgressBar(widgets=[
        title, Percentage(), ' ',
        Bar(marker='#', left='[', right=']'), ' ',
        ETA(), ' '], maxval=maxval)
    pbar.update(0)
    return pbar


def connect_all(g):
    log.info('Loading participants')
    participants = g.nodes().labels('participant').all()
    log.info('Found {} participants'.format(len(participants)))

    log.info('Loading xml files')
    xmls = {
        n['file_name']: n
        for n in g.nodes()
        .labels('file').sysan({'source': 'tcga_dcc'})
        .filter(Node.properties['file_name'].astext.endswith('.xml'))
        .all()
    }
    log.info('Found {} xml files'.format(len(xmls)))

    pbar = get_pbar('Connecting participants ', len(participants))
    for participant in participants:
        barcode = participant['submitter_id']
        biospecimen_name = biospecimen_base.format(barcode=barcode)
        control_name = control_base.format(barcode=barcode)
        clinical_name = clinical_base.format(barcode=barcode)

        clinical = xmls.get(clinical_name, None)
        biospecimen = xmls.get(biospecimen_name, None)
        if not biospecimen:
            biospecimen = xmls.get(control_name, None)
        # if not biospecimen:
        #     log.warn('Missing biospecimen file for {} {}'.format(
        #         participant, participant.system_annotations))
        pbar.update(pbar.currval+1)

    pbar.finish()

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

    g = get_driver()
    with g.session_scope():
        connect_all(g)
