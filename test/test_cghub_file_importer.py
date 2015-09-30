import unittest
import uuid
import os
from lxml import etree
from gdcdatamodel.models import (
    File,
    Center,
    Aliquot,
    Platform,
    ExperimentalStrategy,
    DataFormat,
    DataSubtype,
)
from psqlgraph import Node, Edge
from base import ZugTestBase, PreludeMixin, TEST_DIR
from zug.datamodel import cghub2psqlgraph, cghub_xml_mapping


class TestSignpostClient(object):
    def create(self):
        self.did = str(uuid.uuid4())
        return self


analysis_idA = '00007994-abeb-4b16-a6ad-7230300a29e9'
analysis_idB = '000dbac5-2f8c-48d9-9121-c84421e70381'
bamA = 'UNCID_1620885.c18465ae-447d-46c8-8b54-0156ab502265.sorted_genome_alignments.bam'
bamB = 'TCGA-BF-A1PZ-01A-11D-A18Z_120612_SN590_0162_BC0VNGACXX_s_5_rg.sorted.bam'
baiA = bamA + '.bai'
baiB = bamB + '.bai'
host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'


FIXTURES_DIR = os.path.join(TEST_DIR, "fixtures", "cghub_xml")
TCGA_TEST_DATA = [
    open(os.path.join(FIXTURES_DIR, "tcga1.xml")).read(),
    open(os.path.join(FIXTURES_DIR, "tcga2.xml")).read(),
]

TARGET_CGI_XML = open(os.path.join(FIXTURES_DIR, "target_cgi.xml")).read()
TARGET_BCCAGSC_XML = open(os.path.join(FIXTURES_DIR, "target_bccagsc.xml")).read()


class TestCGHubFileImporter(PreludeMixin, ZugTestBase):

    def setUp(self):
        super(TestCGHubFileImporter, self).setUp()
        self.converter = cghub2psqlgraph.cghub2psqlgraph(
            xml_mapping=cghub_xml_mapping,
            host=host,
            user=user,
            password=password,
            database=database,
            signpost=TestSignpostClient(),
        )
        self._add_required_nodes()

    def create_file(self, analysis_id, file_name):
        with self.converter.graph.session_scope():
            self.converter.graph.node_merge(
                str(uuid.uuid4()),
                label="file",
                properties={
                    "file_name": file_name,
                    "submitter_id": analysis_id,
                    "md5sum": "bogus",
                    "file_size": 0,
                    "state_comment": None,
                    "state": "submitted"
                },
                system_annotations={
                    "analysis_id": analysis_id
                }
            )

    def _add_required_nodes(self):
        with self.converter.graph.session_scope():
            self.converter.graph.node_merge(
                'c18465ae-447d-46c8-8b54-0156ab502265', label='aliquot',
                properties={
                    u'amount': 0.0, u'concentration': 0.0,
                    u'source_center': u'test', u'submitter_id': u'test'})

    def test_simple_parse(self):
        graph = self.converter.graph
        with graph.session_scope():
            to_add = [(analysis_idA, bamA), (analysis_idA, baiA)]
            to_delete = [(analysis_idB, bamB), (analysis_idB, baiB)]
            for root in TCGA_TEST_DATA:
                self.converter.parse('file', etree.fromstring(root))

    def insert_test_files(self):
        with self.converter.graph.session_scope():
            self.to_add = [(analysis_idA, bamA), (analysis_idA, baiA)]
            self.to_delete = [(analysis_idB, bamB), (analysis_idB, baiB)]

            # pre-insert files to delete
            for file_key in self.to_delete:
                self.create_file(*file_key)

    def run_convert(self):
        for root in TCGA_TEST_DATA:
            self.converter.parse('file', etree.fromstring(root))
        self.assertEqual(len(self.converter.files_to_add), 2)
        for file_key in self.to_add:
            self.assertTrue(file_key in self.converter.files_to_add)
        self.assertEqual(len(self.converter.files_to_delete), 2)
        for file_key in self.to_delete:
            self.assertTrue(file_key in self.converter.files_to_delete)
        self.converter.rebase()

    def test_simple_parse(self):
        graph = self.converter.graph
        self.insert_test_files()
        with graph.session_scope():
            self.run_convert()
            for file_key in self.to_add:
                node = graph.nodes().props(
                    {'file_name': file_key[1]}).one()
            for file_key in self.to_delete:
                self.assertEqual(graph.nodes()\
                                 .props({'file_name': file_key[1]})\
                                 .sysan({"to_delete": True})\
                                 .count(), 1)
            bam = graph.nodes().props({'file_name': bamA}).one()
            bai = graph.nodes().props({'file_name': baiA}).one()
            self.assertEqual(bam.sysan["cghub_state"], "live")
            self.converter.graph.nodes().ids('b9aec23b-5d6a-585f-aa04-80e86962f097').one()
            # there are two files uploaded on this date, the bam and the bai
            self.assertEqual(self.converter.graph.nodes()
                             .sysan(cghub_upload_date=1368401409).count(), 2)
            self.assertEqual(self.converter.graph.nodes()
                             .sysan(cghub_disease_abbr="COAD").count(), 2)
            self.assertEqual(self.converter.graph.nodes()
                             .sysan(cghub_legacy_sample_id="TCGA-AA-3495-01A-01R-1410-07").count(), 2)

    def test_missing_aliquot(self):
        graph = self.converter.graph
        self.insert_test_files()
        with graph.session_scope() as s:
            s.delete(graph.nodes(Aliquot).one())
            s.commit()
            self.run_convert()
            for file_key in self.to_add:
                node = graph.nodes().props(
                    {'file_name': file_key[1]}).one()
            for file_key in self.to_delete:
                self.assertEqual(graph.nodes()\
                                 .props({'file_name': file_key[1]})\
                                 .sysan({"to_delete": True})\
                                 .count(), 1)
            bam = graph.nodes().props({'file_name': bamA}).one()
            bai = graph.nodes().props({'file_name': baiA}).one()
            self.converter.graph.nodes().ids('b9aec23b-5d6a-585f-aa04-80e86962f097').one()

    def test_related_to(self):
        graph = self.converter.graph
        self.insert_test_files()
        with graph.session_scope():
            self.run_convert()
            bam = graph.nodes().props({'file_name': bamA}).one()
            bai = graph.nodes().props({'file_name': baiA}).one()
            self.assertEqual(len(list(bai.get_edges())), 1)
            self.assertEqual(
                len(self.converter.graph.nodes().ids(bai.node_id).one()\
                    .parent_files), 1)

    def test_categorization(self):
        graph = self.converter.graph
        self.insert_test_files()
        with graph.session_scope():
            self.run_convert()
            bam = graph.nodes().props({'file_name': bamA}).one()
            bai = graph.nodes().props({'file_name': baiA}).one()
            self.assertEqual(len(list(bam.get_edges())), 7)
            base = graph.nodes(File).ids(bam.node_id)
            base.path('centers').props(code='07').one()
            base.path('platforms').props(name='Illumina GA').one()
            base.path('data_subtypes').props(name='Aligned reads').one()
            base.path('data_formats').props(name='BAM').one()
            base.path('experimental_strategies').props(name='RNA-Seq').one()

    def test_idempotency(self):
        graph = self.converter.graph
        self.insert_test_files()
        for i in range(5):
            self.run_convert()
            with graph.session_scope() as s:
                f = graph.nodes(File).first()
                f['state'] = 'live'
                graph.node_merge(node_id=f.node_id, properties=f.properties)
            self.run_convert()
            with graph.session_scope():
                self.assertEqual(
                    graph.nodes().ids(f.node_id).one()['state'], 'live')
                bam = graph.nodes().props({'file_name': bamA}).one()
                bai = graph.nodes().props({'file_name': baiA}).one()
                self.assertEqual(len(list(bam.get_edges())), 7)
                base = graph.nodes(File).ids(bam.node_id)
                base.path('centers').props(code='07').one()
                base.path('platforms').props(name='Illumina GA').one()
                base.path('data_subtypes').props(name='Aligned reads').one()
                base.path('data_formats').props(name='BAM').one()
                base.path('experimental_strategies').props(name='RNA-Seq').one()
                self.assertEqual(len(list(bai.get_edges())), 1)

    def test_datetime_system_annotations(self):
        graph = self.converter.graph
        self.insert_test_files()
        with graph.session_scope() as s:
            # Insert a file without the sysans to simulate being run on
            # existing nodes without the correct sysasn
            s.add(File(str(uuid.uuid4()), file_name=bamA, state='submitted',
                       file_size=1, md5sum='test',
                       system_annotations={'analysis_id': analysis_idA}))
        self.run_convert()
        with graph.session_scope() as s:
            f = graph.nodes().props(file_name=bamA).one()
            for key in ["last_modified", "upload_date", "published_date"]:
                self.assertIn("cghub_"+key, f.sysan)

    def test_target_file_with_cgi_center(self):
        graph = self.converter.graph
        self.converter.parse('file', etree.fromstring(TARGET_CGI_XML))
        self.converter.rebase()
        with graph.session_scope():
            bam = graph.nodes(File).props(file_name="GS00826-DNA_G08.bam").one()
            self.assertEqual(bam.centers[0].short_name, "CGI")
            self.assertEqual(bam.acl, ["phs000464", "phs000218"])


    def test_target_file_with_bccagsc_center(self):
        graph = self.converter.graph
        self.converter.parse('file', etree.fromstring(TARGET_BCCAGSC_XML))
        self.converter.rebase()
        with graph.session_scope():
            bam = graph.nodes(File).props(file_name="HS1600_29_lanes_dupsFlagged.bam").one()
            self.assertEqual(bam.centers[0].short_name, "BCGSC")
