from unittest import TestCase
import tempfile,uuid
from zug.datamodel.tcga_dcc_sync import TCGADCCArchiveSyncer
from zug.datamodel.tcga_magetab_sync import get_submitter_id_and_rev
import random,time
from libcloud.storage.providers import get_driver
from libcloud.storage.types import Provider
from multiprocessing import Process
from signpost import Signpost
from psqlgraph import PsqlGraphDriver, Node,PsqlNode,PsqlEdge
from signpostclient import SignpostClient
from zug.datamodel.prelude import create_prelude_nodes
from zug.datamodel.latest_urls import LatestURLParser
from zug.datamodel.tcga_dcc_to_biospecimen import TCGADCCToBiospecimen
import os
import pandas as pd


TEST_DIR = os.path.dirname(os.path.realpath(__file__))
FIXTURES_DIR = os.path.join(TEST_DIR,"fixtures", "magetabs")

def run_signpost(port):
    Signpost({"driver": "inmemory", "layers": ["validator"]}).run(host="localhost",
                                                                  port=port)


class TCGADCCToBiospecimenTest(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.port = random.randint(5000,6000)
        cls.signpost = Process(target=run_signpost, args=[cls.port])
        cls.signpost.start()
        time.sleep(1)

    @classmethod
    def tearDownClass(cls):
        cls.signpost.terminate()

    def setUp(self):
        self.pg_driver = PsqlGraphDriver('localhost', 'test',
                                         'test', 'automated_test')
        self.scratch_dir = tempfile.mkdtemp()
        Local = get_driver(Provider.LOCAL)
        self.storage_client = Local(tempfile.mkdtemp())
        self.storage_client.create_container("tcga_dcc_public")
        self.storage_client.create_container("tcga_dcc_protected")
        self.signpost_client = SignpostClient("http://localhost:{}".format(self.port),
                                              version="v0")
        self.parser = LatestURLParser()
        create_prelude_nodes(self.pg_driver)

    def tearDown(self):
        with self.pg_driver.engine.begin() as conn:
            conn.execute('delete from edges')
            conn.execute('delete from nodes')
            conn.execute('delete from voided_edges')
            conn.execute('delete from voided_nodes')
        self.pg_driver.engine.dispose()
        for container in self.storage_client.list_containers():
            for obj in container.list_objects():
                obj.delete()

    def syncer_for(self, archive, **kwargs):
        return TCGADCCArchiveSyncer(
            archive,
            signpost=self.signpost_client,
            pg_driver=self.pg_driver,
            scratch_dir=self.scratch_dir,
            storage_client=self.storage_client,
            **kwargs
        )

    def fake_biospecimen(self, node_id, props, label):
        bio = Node(label=label,
                   properties=props,
                   node_id=node_id)
        self.pg_driver.node_insert(bio)
        return bio

    def create_archive(self, archive):
        submitter_id, rev = get_submitter_id_and_rev(archive)
        return self.pg_driver.node_merge(
            node_id=str(uuid.uuid4()),
            label="archive",
            properties={"submitter_id": submitter_id,
                        "revision": rev}
        )


    def create_file(self, file, archive=None,sys_ann={}):
        file = self.pg_driver.node_merge(
            node_id=str(uuid.uuid4()),
            label="file",
            properties={
                "file_name": file,
                "md5sum": "bogus",
                "file_size": 0,
                "file_state": "live"
            },
            system_annotations=sys_ann
        )
        if archive:
            edge = PsqlEdge(
                label="member_of",
                src_id=file.node_id,
                dst_id=archive.node_id
            )
            self.pg_driver.edge_insert(edge)
        return file


    def specimen_edge_builder_for(self,node):
        return TCGADCCToBiospecimen(node,self.pg_driver)

    def fake_archive_for(self, fixture, rev=1):
        # TODO this is a total hack, come back and make it better at some point
        node = PsqlNode(
            node_id=str(uuid.uuid4()),
            label="archive",
            properties={
                "submitter_id": fixture + "fake_test_archive.1",
                "revision": rev
            }
        )
        self.pg_driver.node_insert(node)
        return {
            "archive_name": fixture + "fake_test_archive.1." + str(rev) +".0",
            "disease_code": "FAKE",
            "batch": 1,
            "revision": rev
        }, node

    def test_tie_to_participant_without_magetab(self):
        barcode = '2c03e8b9-8856-43a7-853d-5ec51a6e5330'
        self.create_file("nationwidechildrens.org_clinical.TCGA-DH-5140.xml",
                         sys_ann={"_participant_barcode": barcode})
        self.fake_biospecimen(barcode,
                              {'submitter_id': barcode,
                               'days_to_index': 0},
                              'participant')
        with self.pg_driver.session_scope():
            file_node = self.pg_driver.node_lookup(
                label='file',
                property_matches={
                    'file_name':
                    'nationwidechildrens.org_clinical.TCGA-DH-5140.xml'
                }).one()

            barcode = file_node.system_annotations['_participant_barcode']
            builder = self.specimen_edge_builder_for(file_node)
            builder.build()
            participant=self.pg_driver.nodes().labels('participant').\
                with_edge_from_node('data_from',file_node).one()
            self.assertEqual(participant['submitter_id'],barcode)
            edge = self.pg_driver.edges().labels('data_from').\
                src(file_node.node_id).\
                dst(participant.node_id).one()
            self.assertEqual(edge.system_annotations['source'],'filename')

    def test_file_without_sysan(self):
        self.create_file("nationwidechildrens.org_biospecimen_analyte_laml.txt")
       # make sure archive gets tied to project
        with self.pg_driver.session_scope():
            # make sure the files get ties to center

            file_node = self.pg_driver.node_lookup(
                label="file",
                property_matches={"file_name":"nationwidechildrens.org_biospecimen_analyte_laml.txt"}
                ).one()
            builder = self.specimen_edge_builder_for(file_node)
            builder.build()

    def load_file_with_aliquot(self):
        aliquot = self.fake_biospecimen("290f101e-ff47-4aeb-ad71-11cb6e6b9dde",
                                        {'submitter_id':"TCGA-OR-A5J5-01A-11R-A29W-13",
                                         u'amount': 20.0,
                                         u'concentration': 0.17,
                                         u'source_center': u'23'},'aliquot')
        archive = self.create_archive("bcgsc.ca_ACC.IlluminaHiSeq_miRNASeq.Level_3.1.1.0")
        self.create_file("TCGA-OR-A5J5-01A-11R-A29W-13_mirna.bam")
        file = self.create_file("TCGA-OR-A5J5-01A-11R-A29W-13.isoform.quantification.txt",
                                archive=archive,
                                sys_ann={'_aliquot_barcode': u'TCGA-OR-A5J5-01A-11R-A29W-13',
                                         'source':'tcga_dcc'})
        self.create_file("TCGA-OR-A5J5-01A-11R-A29W-13.mirna.quantification.txt",
                         archive=archive,
                         sys_ann={'_aliquot_barcode': u'TCGA-OR-A5J5-01A-11R-A29W-13',
                                  'source':'tcga_dcc'})
        return file, aliquot

    def test_tie_to_aliquot_with_magetab_first(self):
        file, aliquot = self.load_file_with_aliquot()
        self.pg_driver.edge_insert(PsqlEdge(
            label="data_from",
            src_id=file.node_id,
            dst_id=aliquot.node_id,
            system_annotations={"source": "tcga_magetab"}
        ))
        self.fake_archive_for("basic.sdrf.txt")
        with self.pg_driver.session_scope():
            file_node = self.pg_driver.node_lookup(
                label='file',
                property_matches={'file_name':
                    'TCGA-OR-A5J5-01A-11R-A29W-13.isoform.quantification.txt'}
                ).one()

            barcode = file_node.system_annotations['_aliquot_barcode']
            builder = self.specimen_edge_builder_for(file_node)
            builder.build()
            aliquot = self.pg_driver.nodes().labels('aliquot').\
                with_edge_from_node('data_from',file_node).one()
            self.assertEqual(aliquot['submitter_id'],barcode)
            edge = self.pg_driver.edges().labels('data_from').\
                src(file_node.node_id).\
                dst(aliquot.node_id).one()
            self.assertEqual(edge.system_annotations['source'],'tcga_magetab')
