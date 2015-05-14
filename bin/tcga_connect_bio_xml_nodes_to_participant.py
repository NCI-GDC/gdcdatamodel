#!/usr/bin/env python
import logging
import argparse
from gdcdatamodel import node_avsc_object, edge_avsc_object
from psqlgraph.validate import AvroNodeValidator, AvroEdgeValidator
from psqlgraph import PsqlGraphDriver, Node, Edge
from cdisutils.log import get_logger
from progressbar import ProgressBar, Percentage, Bar, ETA
from sqlalchemy.orm import joinedload

from gdcdatamodel.models import File, Participant

# NOTE this file still will not work, needs to be updated for ORM

log = get_logger("tcga_connect_bio_xml_nodes_to_participant")
logging.root.setLevel(level=logging.ERROR)

biospecimen_base = 'nationwidechildrens.org_biospecimen.{barcode}.xml'
control_base = 'nationwidechildrens.org_control.{barcode}.xml'
clinical_base = 'nationwidechildrens.org_clinical.{barcode}.xml'

biospecimen_base2 = 'genome.wustl.edu_biospecimen.{barcode}.xml'
control_base2 = 'genome.wustl.edu_control.{barcode}.xml'
clinical_base2 = 'genome.wustl.edu_clinical.{barcode}.xml'

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
    participants = g.nodes(Participant).options(joinedload(Node.edges_in)).all()
    log.info('Found {} participants'.format(len(participants)))
    pbar = get_pbar('Connecting participants ', len(participants))

    log.info('Loading xml files')
    xmls = {
        n['file_name']: n
        for n in g.nodes(File)
        .sysan({'source': 'tcga_dcc'})
        .filter(Node.properties['file_name'].astext.endswith('.xml'))
        .all()
    }
    log.info('Found {} xml files'.format(len(xmls)))

    for participant in participants:
        barcode = participant['submitter_id']
        p_neighbor_ids = [e.src_id for e in participant.edges_in]
        biospecimen_name = biospecimen_base.format(barcode=barcode)
        control_name = control_base.format(barcode=barcode)
        clinical_name = clinical_base.format(barcode=barcode)
        biospecimen_name2 = biospecimen_base2.format(barcode=barcode)
        control_name2 = control_base2.format(barcode=barcode)
        clinical_name2 = clinical_base2.format(barcode=barcode)

        clinical = xmls.get(clinical_name, None)
        clinical = clinical if clinical else xmls.get(clinical_name2, None)
        if clinical:
            if clinical.node_id not in p_neighbor_ids:
                g.edge_insert(Edge(
                    src_id=clinical.node_id,
                    dst_id=participant.node_id,
                    label='describes'))

        biospecimen = xmls.get(biospecimen_name, None)
        biospecimen = biospecimen if biospecimen else xmls.get(
            biospecimen_name2, None)
        biospecimen = biospecimen if biospecimen else xmls.get(
            control_name, None)
        biospecimen = biospecimen if biospecimen else xmls.get(
            control_name2, None)

        if biospecimen:
            if biospecimen.node_id not in p_neighbor_ids:
                g.edge_insert(Edge(
                    src_id=biospecimen.node_id,
                    dst_id=participant.node_id,
                    label='describes'))
        else:
            log.warn('Missing biospecimen file for {} {} {}'.format(
                participant, participant.system_annotations, barcode))
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
