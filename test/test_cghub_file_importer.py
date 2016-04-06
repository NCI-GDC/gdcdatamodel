import unittest
import uuid
import os
import hashlib
from lxml import etree
from gdcdatamodel.models import (
    File,
    Center,
    Aliquot,
    Platform,
    ExperimentalStrategy,
    DataFormat,
    DataSubtype,
    AnalysisMetadata,
    ExperimentMetadata,
    RunMetadata
)
import boto
from moto import mock_s3
from psqlgraph import Node, Edge
from base import ZugTestBase, PreludeMixin, SignpostMixin, TEST_DIR
from zug.datamodel import cghub2psqlgraph, cghub_xml_mapping, cghub_xml_metadata


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
bucket = 'test_bucket'


FIXTURES_DIR = os.path.join(TEST_DIR, "fixtures", "cghub_xml")
TCGA_TEST_DATA = [
    open(os.path.join(FIXTURES_DIR, "tcga1.xml")).read(),
    open(os.path.join(FIXTURES_DIR, "tcga2.xml")).read(),
]

TARGET_CGI_XML = open(os.path.join(FIXTURES_DIR, "target_cgi.xml")).read()
TARGET_BCCAGSC_XML = open(os.path.join(FIXTURES_DIR, "target_bccagsc.xml")).read()


class TestCGHubFileImporter(PreludeMixin, SignpostMixin, ZugTestBase):

    def setUp(self):
        super(TestCGHubFileImporter, self).setUp()
        self.mock = mock_s3()
        self.mock.start()
        self.s3conn = boto.connect_s3()
        self.s3conn.create_bucket(bucket)

        self.converter = cghub2psqlgraph.cghub2psqlgraph(
            xml_mapping=cghub_xml_mapping,
            host=host,
            user=user,
            password=password,
            database=database,
            signpost=self.signpost_client,
        )
        self._add_required_nodes()
        
        self.extractor = cghub_xml_metadata.Extractor( 
            signpost=self.signpost_client,
            s3=self.s3conn,
            bucket=bucket,
        )
        self.extractor.g = self.converter.graph

    def tearDown(self):
        self.mock.stop()

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


    def test_extract_graph(self):
        '''
        Tests that metadata nodes are being inserted correctly
        '''
        graph = self.converter.graph
        self.insert_test_files()
        with graph.session_scope():
            self.run_convert()
            for root in TCGA_TEST_DATA:
                self.extractor.process(etree.fromstring(root))
            # Check that each metadata file was extracted
            n_nodes = graph.nodes(AnalysisMetadata).count()
            self.assertEqual(n_nodes, 2)
            n_nodes = graph.nodes(ExperimentMetadata).count()
            self.assertEqual(n_nodes, 2)
            n_nodes = graph.nodes(RunMetadata).count()
            self.assertEqual(n_nodes, 2)

            for T in [AnalysisMetadata, ExperimentMetadata, RunMetadata]:
                bamA_node = graph.nodes(T).sysan(
                          {'analysis_id':analysis_idA}).one()
                self.assertEqual(bamA_node.data_format, 'SRA XML')
                self.assertEqual(bamA_node.data_category, 'Sequencing Data')
                self.assertGreater(bamA_node.file_size, 0)
                self.assertGreater(len(bamA_node.files), 0)
                self.assertEqual(bamA_node.files[0].props['file_name'], bamA)

    def test_extract_signpost(self):
        '''
        Test that s3 urls are put into signpost
        '''
        graph = self.converter.graph
        self.insert_test_files()
        with graph.session_scope():
            self.run_convert()
            for root in TCGA_TEST_DATA:
                self.extractor.process(etree.fromstring(root))
            # Check every type of metadata node
            for T in [AnalysisMetadata, ExperimentMetadata, RunMetadata]:
                bamA_node = graph.nodes(T).sysan(
                          {'analysis_id':analysis_idA}).one()
                # returns 'analysis', 'experiment', or 'run' depending on T
                t = bamA_node.data_type.split(' ')[0].lower()
                file_name = '{}_{}.xml'.format(analysis_idA, t)
                bamA_url = self.signpost_client.get(bamA_node.node_id).urls[0]
                self.assertEqual(bamA_url.split('/')[-1], file_name)


    def test_extract_s3(self):
        '''
        Check that metadata files are stored in s3 properly
        '''
        graph = self.converter.graph
        self.insert_test_files()
        with graph.session_scope():
            self.run_convert()
            for root in TCGA_TEST_DATA:
                self.extractor.process(etree.fromstring(root))
            # Check every type of metadata node
            for T in [AnalysisMetadata, ExperimentMetadata, RunMetadata]:
                bamA_node = graph.nodes(T).sysan(
                          {'analysis_id':analysis_idA}).one()

                bamA_url = self.signpost_client.get(bamA_node.node_id).urls[0]
                t = bamA_node.data_type.split(' ')[0].lower()
                file_name = '{}_{}.xml'.format(analysis_idA, t)
                # Get file from s3 and check its hash against the node's
                s3_key_name = '/'.join([
                    bamA_node.node_id,
                    file_name
                ])
                bamA_from_s3 = self.s3conn.get_bucket(bucket)\
                                  .get_key(s3_key_name)\
                                  .get_contents_as_string()
                self.assertEqual(hashlib.md5(bamA_from_s3).hexdigest(),
                                    bamA_node.props['md5sum'])
                # TODO: Check size?

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

    def test_center_name_system_annotation(self):
        graph = self.converter.graph
        self.insert_test_files()
        self.run_convert()
        with graph.session_scope():
            f = graph.nodes().props(file_name=bamA).one()
            self.assertEqual(f.sysan["cghub_center_name"], "UNC-LCCC")

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
