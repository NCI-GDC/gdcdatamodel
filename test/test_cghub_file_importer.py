import logging
import unittest
import uuid
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
from base import ZugTestBase, PreludeMixin
from zug.datamodel import cghub2psqlgraph, cghub_xml_mapping
from cdisutils.log import get_logger

log = get_logger("cghub_file_importer")
logging.root.setLevel(level=logging.INFO)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


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
center_id = str(uuid.uuid4())


class TestCGHubFileImporter(PreludeMixin, ZugTestBase):

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)
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
            for root in TEST_DATA:
                self.converter.parse('file', etree.fromstring(root))

    def insert_test_files(self):
        with self.converter.graph.session_scope():
            self.to_add = [(analysis_idA, bamA), (analysis_idA, baiA)]
            self.to_delete = [(analysis_idB, bamB), (analysis_idB, baiB)]

            # pre-insert files to delete
            for file_key in self.to_delete:
                self.create_file(*file_key)

    def run_convert(self):
        for root in TEST_DATA:
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
            self.converter.graph.nodes().ids('b9aec23b-5d6a-585f-aa04-80e86962f097').one()
            # there are two files uploaded on this date, the bam and the bai
            self.assertEqual(self.converter.graph.nodes().sysan(cghub_upload_date=1368401409).count(), 2)
            self.assertEqual(self.converter.graph.nodes().sysan(cghub_project_code="COAD").count(), 2)

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



TEST_DATA = ["""
<Result id="1">
		<analysis_id>00007994-abeb-4b16-a6ad-7230300a29e9</analysis_id>
		<state>live</state>
		<reason></reason>
		<last_modified>2013-05-16T20:43:36Z</last_modified>
		<upload_date>2013-05-12T23:30:09Z</upload_date>
		<published_date>2013-05-12T23:40:33Z</published_date>
		<center_name>UNC-LCCC</center_name>
		<study>phs000178</study>
		<aliquot_id>c18465ae-447d-46c8-8b54-0156ab502265</aliquot_id>
		<files>
			<file>
				<filename>UNCID_1620885.c18465ae-447d-46c8-8b54-0156ab502265.sorted_genome_alignments.bam</filename>
				<filesize>1972948726</filesize>
				<checksum type="md5">401ce370ba2b641d3bfa41ae2b5ca932</checksum>
			</file>
			<file>
				<filename>UNCID_1620885.c18465ae-447d-46c8-8b54-0156ab502265.sorted_genome_alignments.bam.bai</filename>
				<filesize>4874968</filesize>
				<checksum type="md5">a592e6299b11eb2ad39fb2dcc56f94f5</checksum>
			</file>
		</files>
		<sample_accession>SRS156860</sample_accession>
		<legacy_sample_id>TCGA-AA-3495-01A-01R-1410-07</legacy_sample_id>
		<disease_abbr>COAD</disease_abbr>
		<tss_id>AA</tss_id>
		<participant_id>15b987ba-77ab-477b-a54a-65ec5c7c399e</participant_id>
		<sample_id>57733d46-9726-4cc0-825f-77185bc7bea9</sample_id>
		<analyte_code>R</analyte_code>
		<sample_type>01</sample_type>
		<library_strategy>RNA-Seq</library_strategy>
		<platform>ILLUMINA</platform>
		<refassem_short_name>HG19</refassem_short_name>
		<analysis_xml>
<ANALYSIS_SET xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="http://www.ncbi.nlm.nih.gov/viewvc/v1/trunk/sra/doc/SRA_1-5/SRA.analysis.xsd?view=co">
	<ANALYSIS center_name="UNC-LCCC" alias="2001231" analysis_date="2012-10-10T02:58:54.471" analysis_center="UNC-LCCC">
		<TITLE>Mapsplice Alignment to Genome: 1476030</TITLE>
		<STUDY_REF accession="SRP000677" refcenter="NHGRI" refname="phs000178"/>
		<DESCRIPTION>UNC RNA-Seq Workflow V2 - Mapsplice Alignment to Genome - c18465ae-447d-46c8-8b54-0156ab502265</DESCRIPTION>
		<ANALYSIS_TYPE>
			<REFERENCE_ALIGNMENT>
				<ASSEMBLY>
					<STANDARD short_name="HG19"/>
				</ASSEMBLY>
				<RUN_LABELS>
					<RUN refcenter="UNC-LCCC" refname="UNCID:125510" read_group_label="110215_UNC3-RDR300156_00072_FC_62J8HAAXX_7_" data_block_name="sorted_genome_alignments"/>
				</RUN_LABELS>
				<SEQ_LABELS>
					<SEQUENCE seq_label="chr1" data_block_name="sorted_genome_alignments" accession="NC_000001.10"/>
					<SEQUENCE seq_label="chr2" data_block_name="sorted_genome_alignments" accession="NC_000002.11"/>
					<SEQUENCE seq_label="chr3" data_block_name="sorted_genome_alignments" accession="NC_000003.11"/>
					<SEQUENCE seq_label="chr4" data_block_name="sorted_genome_alignments" accession="NC_000004.11"/>
					<SEQUENCE seq_label="chr5" data_block_name="sorted_genome_alignments" accession="NC_000005.9"/>
					<SEQUENCE seq_label="chr6" data_block_name="sorted_genome_alignments" accession="NC_000006.11"/>
					<SEQUENCE seq_label="chr7" data_block_name="sorted_genome_alignments" accession="NC_000007.13"/>
					<SEQUENCE seq_label="chr8" data_block_name="sorted_genome_alignments" accession="NC_000008.10"/>
					<SEQUENCE seq_label="chr9" data_block_name="sorted_genome_alignments" accession="NC_000009.11"/>
					<SEQUENCE seq_label="chr10" data_block_name="sorted_genome_alignments" accession="NC_000010.10"/>
					<SEQUENCE seq_label="chr11" data_block_name="sorted_genome_alignments" accession="NC_000011.9"/>
					<SEQUENCE seq_label="chr12" data_block_name="sorted_genome_alignments" accession="NC_000012.11"/>
					<SEQUENCE seq_label="chr13" data_block_name="sorted_genome_alignments" accession="NC_000013.10"/>
					<SEQUENCE seq_label="chr14" data_block_name="sorted_genome_alignments" accession="NC_000014.8"/>
					<SEQUENCE seq_label="chr15" data_block_name="sorted_genome_alignments" accession="NC_000015.9"/>
					<SEQUENCE seq_label="chr16" data_block_name="sorted_genome_alignments" accession="NC_000016.9"/>
					<SEQUENCE seq_label="chr17" data_block_name="sorted_genome_alignments" accession="NC_000017.10"/>
					<SEQUENCE seq_label="chr18" data_block_name="sorted_genome_alignments" accession="NC_000018.9"/>
					<SEQUENCE seq_label="chr19" data_block_name="sorted_genome_alignments" accession="NC_000019.9"/>
					<SEQUENCE seq_label="chr20" data_block_name="sorted_genome_alignments" accession="NC_000020.10"/>
					<SEQUENCE seq_label="chr21" data_block_name="sorted_genome_alignments" accession="NC_000021.8"/>
					<SEQUENCE seq_label="chr22" data_block_name="sorted_genome_alignments" accession="NC_000022.10"/>
					<SEQUENCE seq_label="chrX" data_block_name="sorted_genome_alignments" accession="NC_000023.10"/>
					<SEQUENCE seq_label="chrY" data_block_name="sorted_genome_alignments" accession="NC_000024.9"/>
					<SEQUENCE seq_label="chrM_rCRS" data_block_name="sorted_genome_alignments" accession="NC_012920.1"/>
				</SEQ_LABELS>
				<PROCESSING>
					<PIPELINE>
						<PIPE_SECTION section_name="MapspliceRSEM">
							<STEP_INDEX>1476030</STEP_INDEX>
							<PREV_STEP_INDEX>N/A</PREV_STEP_INDEX>
							<PROGRAM>MapspliceRSEM</PROGRAM>
							<VERSION>0.7.5</VERSION>
							<NOTES>samtools-sort-genome; UNCID:1620885</NOTES>
						</PIPE_SECTION>
					</PIPELINE>
					<DIRECTIVES>
						<alignment_includes_unaligned_reads>true</alignment_includes_unaligned_reads>
						<alignment_marks_duplicate_reads>false</alignment_marks_duplicate_reads>
						<alignment_includes_failed_reads>false</alignment_includes_failed_reads>
					</DIRECTIVES>
				</PROCESSING>
			</REFERENCE_ALIGNMENT>
		</ANALYSIS_TYPE>
		<TARGETS>
			<TARGET sra_object_type="SAMPLE" refcenter="TCGA" refname="c18465ae-447d-46c8-8b54-0156ab502265"/>
		</TARGETS>
		<DATA_BLOCK name="sorted_genome_alignments">
			<FILES>
				<FILE checksum="401ce370ba2b641d3bfa41ae2b5ca932" checksum_method="MD5" filetype="bam" filename="UNCID_1620885.c18465ae-447d-46c8-8b54-0156ab502265.sorted_genome_alignments.bam"/>
			</FILES>
		</DATA_BLOCK>
	</ANALYSIS>
</ANALYSIS_SET>
</analysis_xml>
		<experiment_xml>
<EXPERIMENT_SET xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="http://www.ncbi.nlm.nih.gov/viewvc/v1/trunk/sra/doc/SRA_1-5/SRA.experiment.xsd?view=co">
	<EXPERIMENT center_name="UNC-LCCC" alias="UNCID:7-125509">
		<STUDY_REF accession="SRP000677" refcenter="NHGRI" refname="phs000178"/>
		<DESIGN>
			<DESIGN_DESCRIPTION>TCGA RNA-Seq Single-End Experiment</DESIGN_DESCRIPTION>
			<SAMPLE_DESCRIPTOR refcenter="TCGA" refname="c18465ae-447d-46c8-8b54-0156ab502265"/>
			<LIBRARY_DESCRIPTOR>
				<LIBRARY_NAME>Illumina TruSeq for c18465ae-447d-46c8-8b54-0156ab502265</LIBRARY_NAME>
				<LIBRARY_STRATEGY>RNA-Seq</LIBRARY_STRATEGY>
				<LIBRARY_SOURCE>GENOMIC</LIBRARY_SOURCE>
				<LIBRARY_SELECTION>RANDOM</LIBRARY_SELECTION>
				<LIBRARY_LAYOUT>
					<SINGLE/>
				</LIBRARY_LAYOUT>
			</LIBRARY_DESCRIPTOR>
			<SPOT_DESCRIPTOR>
				<SPOT_DECODE_SPEC>
					<READ_SPEC>
						<READ_INDEX>0</READ_INDEX>
						<READ_CLASS>Application Read</READ_CLASS>
						<READ_TYPE>Forward</READ_TYPE>
						<BASE_COORD>1</BASE_COORD>
					</READ_SPEC>
				</SPOT_DECODE_SPEC>
			</SPOT_DESCRIPTOR>
		</DESIGN>
		<PLATFORM>
			<ILLUMINA>
				<INSTRUMENT_MODEL>Illumina Genome Analyzer II</INSTRUMENT_MODEL>
			</ILLUMINA>
		</PLATFORM>
		<PROCESSING>
			<PIPELINE>
				<PIPE_SECTION section_name="BASE_CALLS">
					<STEP_INDEX>N/A</STEP_INDEX>
					<PREV_STEP_INDEX>NIL</PREV_STEP_INDEX>
					<PROGRAM>Illumina RTA</PROGRAM>
					<VERSION/>
					<NOTES>SEQUENCE_SPACE=Base Space</NOTES>
				</PIPE_SECTION>
				<PIPE_SECTION section_name="QUALITY_SCORES">
					<STEP_INDEX>N/A</STEP_INDEX>
					<PREV_STEP_INDEX>NIL</PREV_STEP_INDEX>
					<PROGRAM>Illumina RTA</PROGRAM>
					<VERSION/>
					<NOTES>qtype=phred</NOTES>
				</PIPE_SECTION>
			</PIPELINE>
		</PROCESSING>
	</EXPERIMENT>
</EXPERIMENT_SET>
</experiment_xml>
		<run_xml>
<RUN_SET xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="http://www.ncbi.nlm.nih.gov/viewvc/v1/trunk/sra/doc/SRA_1-5/SRA.run.xsd?view=co">
	<RUN center_name="UNC-LCCC" alias="UNCID:125510">
		<EXPERIMENT_REF refcenter="UNC-LCCC" refname="UNCID:7-125509"/>
	</RUN>
</RUN_SET>
</run_xml>
		<analysis_detail_uri>https://cghub.ucsc.edu/cghub/metadata/analysisDetail/00007994-abeb-4b16-a6ad-7230300a29e9</analysis_detail_uri>
		<analysis_submission_uri>https://cghub.ucsc.edu/cghub/metadata/analysisSubmission/00007994-abeb-4b16-a6ad-7230300a29e9</analysis_submission_uri>
		<analysis_data_uri>https://cghub.ucsc.edu/cghub/data/analysis/download/00007994-abeb-4b16-a6ad-7230300a29e9</analysis_data_uri>
	</Result>
""", """
	<Result id="2">
		<analysis_id>000dbac5-2f8c-48d9-9121-c84421e70381</analysis_id>
		<state>redacted</state>
		<reason></reason>
		<last_modified>2013-05-16T20:43:36Z</last_modified>
		<upload_date>2012-08-06T22:25:27Z</upload_date>
		<published_date>2012-08-07T03:11:43Z</published_date>
		<center_name>HMS-RK</center_name>
		<study>phs000178</study>
		<aliquot_id>25d65d8b-ed75-47d6-b7f6-95cf26ccbe06</aliquot_id>
		<files>
			<file>
				<filename>TCGA-BF-A1PZ-01A-11D-A18Z_120612_SN590_0162_BC0VNGACXX_s_5_rg.sorted.bam</filename>
				<filesize>17405291593</filesize>
				<checksum type="MD5">503386a6bf3b32925a6e878ba7811e03</checksum>
			</file>
			<file>
				<filename>TCGA-BF-A1PZ-01A-11D-A18Z_120612_SN590_0162_BC0VNGACXX_s_5_rg.sorted.bam.bai</filename>
				<filesize>8616976</filesize>
				<checksum type="md5">683b249e253fa1cf07c10766b6992183</checksum>
			</file>
		</files>
		<sample_accession></sample_accession>
		<legacy_sample_id>TCGA-BF-A1PZ-01A-11D-A18Z-02</legacy_sample_id>
		<disease_abbr>SKCM</disease_abbr>
		<tss_id>BF</tss_id>
		<participant_id>455f982c-a067-46ac-bf89-3e535d0ffca0</participant_id>
		<sample_id>2a5ee782-122e-432b-98e0-0849b6f9a3d9</sample_id>
		<analyte_code>D</analyte_code>
		<sample_type>01</sample_type>
		<library_strategy>WGS</library_strategy>
		<platform>ILLUMINA</platform>
		<refassem_short_name>HG19_Broad_variant</refassem_short_name>
		<analysis_xml>
<ANALYSIS_SET xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="http://www.ncbi.nlm.nih.gov/viewvc/v1/trunk/sra/doc/SRA_1-5/SRA.analysis.xsd?view=co">
	<ANALYSIS alias="TCGA-BF-A1PZ-01A-11D-A18Z-02" center_name="HMS-RK" analysis_center="HMS-RK" analysis_date="2012-08-06T15:31:28">
		<TITLE>Low Pass Sequencing of TCGA SKCM Samples</TITLE>
		<STUDY_REF accession="SRP000677" refcenter="NHGRI" refname="phs000178"/>
		<DESCRIPTION>Low pass whole genome sequencing  of sample:  TCGA-BF-A1PZ-01A-11D-A18Z-02</DESCRIPTION>
		<ANALYSIS_TYPE>
			<REFERENCE_ALIGNMENT>
				<ASSEMBLY>
					<STANDARD short_name="HG19_Broad_variant"/>
				</ASSEMBLY>
				<RUN_LABELS>
					<RUN data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" read_group_label="120612_SN590_0162_BC0VNGACXX_5" refname="TCGA-BF-A1PZ-01A-11D-A18Z_120612_SN590_0162_BC0VNGACXX_s_5_rg.sorted.bam" refcenter="HMS-RK"/>
				</RUN_LABELS>
				<SEQ_LABELS>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="NC_000001.10" seq_label="1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="NC_000002.11" seq_label="2"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="NC_000003.11" seq_label="3"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="NC_000004.11" seq_label="4"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="NC_000005.9" seq_label="5"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="NC_000006.11" seq_label="6"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="NC_000007.13" seq_label="7"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="NC_000008.10" seq_label="8"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="NC_000009.11" seq_label="9"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="NC_000010.10" seq_label="10"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="NC_000011.9" seq_label="11"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="NC_000012.11" seq_label="12"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="NC_000013.10" seq_label="13"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="NC_000014.8" seq_label="14"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="NC_000015.9" seq_label="15"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="NC_000016.9" seq_label="16"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="NC_000017.10" seq_label="17"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="NC_000018.9" seq_label="18"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="NC_000019.9" seq_label="19"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="NC_000020.10" seq_label="20"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="NC_000021.8" seq_label="21"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="NC_000022.10" seq_label="22"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="NC_000023.10" seq_label="X"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="NC_000024.9" seq_label="Y"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="NC_012920.1" seq_label="MT"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000207.1" seq_label="GL000207.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000226.1" seq_label="GL000226.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000229.1" seq_label="GL000229.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000231.1" seq_label="GL000231.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000210.1" seq_label="GL000210.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000239.1" seq_label="GL000239.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000235.1" seq_label="GL000235.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000201.1" seq_label="GL000201.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000247.1" seq_label="GL000247.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000245.1" seq_label="GL000245.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000197.1" seq_label="GL000197.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000203.1" seq_label="GL000203.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000246.1" seq_label="GL000246.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000249.1" seq_label="GL000249.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000196.1" seq_label="GL000196.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000248.1" seq_label="GL000248.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000244.1" seq_label="GL000244.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000238.1" seq_label="GL000238.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000202.1" seq_label="GL000202.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000234.1" seq_label="GL000234.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000232.1" seq_label="GL000232.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000206.1" seq_label="GL000206.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000240.1" seq_label="GL000240.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000236.1" seq_label="GL000236.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000241.1" seq_label="GL000241.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000243.1" seq_label="GL000243.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000242.1" seq_label="GL000242.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000230.1" seq_label="GL000230.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000237.1" seq_label="GL000237.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000233.1" seq_label="GL000233.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000204.1" seq_label="GL000204.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000198.1" seq_label="GL000198.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000208.1" seq_label="GL000208.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000191.1" seq_label="GL000191.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000227.1" seq_label="GL000227.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000228.1" seq_label="GL000228.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000214.1" seq_label="GL000214.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000221.1" seq_label="GL000221.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000209.1" seq_label="GL000209.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000218.1" seq_label="GL000218.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000220.1" seq_label="GL000220.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000213.1" seq_label="GL000213.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000211.1" seq_label="GL000211.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000199.1" seq_label="GL000199.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000217.1" seq_label="GL000217.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000216.1" seq_label="GL000216.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000215.1" seq_label="GL000215.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000205.1" seq_label="GL000205.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000219.1" seq_label="GL000219.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000224.1" seq_label="GL000224.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000223.1" seq_label="GL000223.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000195.1" seq_label="GL000195.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000212.1" seq_label="GL000212.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000222.1" seq_label="GL000222.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000200.1" seq_label="GL000200.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000193.1" seq_label="GL000193.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000194.1" seq_label="GL000194.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000225.1" seq_label="GL000225.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="GL000192.1" seq_label="GL000192.1"/>
					<SEQUENCE data_block_name="TCGA-BF-A1PZ-01A-11D-A18Z-02" accession="NC_007605.1" seq_label="NC_007605"/>
				</SEQ_LABELS>
				<PROCESSING>
					<PIPELINE>
						<PIPE_SECTION section_name="Mapping">
							<STEP_INDEX>1</STEP_INDEX>
							<PREV_STEP_INDEX>NIL</PREV_STEP_INDEX>
							<PROGRAM>bwa</PROGRAM>
							<VERSION>0.5.9-r16</VERSION>
							<NOTES>bwa aln parameter = OLB</NOTES>
						</PIPE_SECTION>
						<PIPE_SECTION section_name="Mapping">
							<STEP_INDEX>2</STEP_INDEX>
							<PREV_STEP_INDEX>1</PREV_STEP_INDEX>
							<PROGRAM>bwa</PROGRAM>
							<VERSION>0.5.9-r16</VERSION>
							<NOTES>bwa sampe</NOTES>
						</PIPE_SECTION>
						<PIPE_SECTION section_name="addRG">
							<STEP_INDEX>3</STEP_INDEX>
							<PREV_STEP_INDEX>2</PREV_STEP_INDEX>
							<PROGRAM>addRGTags</PROGRAM>
							<VERSION>1.0</VERSION>
							<NOTES>add RG tags</NOTES>
						</PIPE_SECTION>
						<PIPE_SECTION section_name="sort">
							<STEP_INDEX>4</STEP_INDEX>
							<PREV_STEP_INDEX>3</PREV_STEP_INDEX>
							<PROGRAM>samtools</PROGRAM>
							<VERSION>1.4</VERSION>
							<NOTES>sort BAM</NOTES>
						</PIPE_SECTION>
					</PIPELINE>
					<DIRECTIVES>
						<alignment_includes_unaligned_reads>true</alignment_includes_unaligned_reads>
						<alignment_marks_duplicate_reads>false</alignment_marks_duplicate_reads>
						<alignment_includes_failed_reads>true</alignment_includes_failed_reads>
					</DIRECTIVES>
				</PROCESSING>
			</REFERENCE_ALIGNMENT>
		</ANALYSIS_TYPE>
		<TARGETS>
			<TARGET sra_object_type="SAMPLE" refcenter="TCGA" refname="25d65d8b-ed75-47d6-b7f6-95cf26ccbe06"/>
		</TARGETS>
		<DATA_BLOCK name="TCGA-BF-A1PZ-01A-11D-A18Z-02">
			<FILES>
				<FILE filename="TCGA-BF-A1PZ-01A-11D-A18Z_120612_SN590_0162_BC0VNGACXX_s_5_rg.sorted.bam" filetype="bam" checksum_method="MD5" checksum="503386a6bf3b32925a6e878ba7811e03"/>
			</FILES>
		</DATA_BLOCK>
	</ANALYSIS>
</ANALYSIS_SET>
</analysis_xml>
		<experiment_xml>
<EXPERIMENT_SET xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="http://www.ncbi.nlm.nih.gov/viewvc/v1/trunk/sra/doc/SRA_1-5/SRA.experiment.xsd?view=co">
	<EXPERIMENT alias="TCGA-BF-A1PZ-01A-11D-A18Z_120612_SN590_0162_BC0VNGACXX_s_5_rg.sorted.bam" center_name="HMS-RK">
		<TITLE>Low Pass Sequencing of TCGA SKCM Samples</TITLE>
		<STUDY_REF accession="SRP000677" refcenter="NHGRI" refname="phs000178"/>
		<DESIGN>
			<DESIGN_DESCRIPTION>Low pass whole genome sequencing of sample TCGA-BF-A1PZ-01A-11D-A18Z-02</DESIGN_DESCRIPTION>
			<SAMPLE_DESCRIPTOR refcenter="TCGA" refname="25d65d8b-ed75-47d6-b7f6-95cf26ccbe06"/>
			<LIBRARY_DESCRIPTOR>
				<LIBRARY_NAME>HM_A_HiFi</LIBRARY_NAME>
				<LIBRARY_STRATEGY>WGS</LIBRARY_STRATEGY>
				<LIBRARY_SOURCE>GENOMIC</LIBRARY_SOURCE>
				<LIBRARY_SELECTION>RANDOM</LIBRARY_SELECTION>
				<LIBRARY_LAYOUT>
					<PAIRED NOMINAL_LENGTH="322" NOMINAL_SDEV="82"/>
				</LIBRARY_LAYOUT>
			</LIBRARY_DESCRIPTOR>
			<SPOT_DESCRIPTOR>
				<SPOT_DECODE_SPEC>
					<SPOT_LENGTH>102</SPOT_LENGTH>
					<READ_SPEC>
						<READ_INDEX>0</READ_INDEX>
						<READ_CLASS>Application Read</READ_CLASS>
						<READ_TYPE>Forward</READ_TYPE>
						<BASE_COORD>1</BASE_COORD>
					</READ_SPEC>
					<READ_SPEC>
						<READ_INDEX>1</READ_INDEX>
						<READ_CLASS>Application Read</READ_CLASS>
						<READ_TYPE>Reverse</READ_TYPE>
						<BASE_COORD>52</BASE_COORD>
					</READ_SPEC>
				</SPOT_DECODE_SPEC>
			</SPOT_DESCRIPTOR>
		</DESIGN>
		<PLATFORM>
			<ILLUMINA>
				<INSTRUMENT_MODEL>Illumina HiSeq 2000</INSTRUMENT_MODEL>
			</ILLUMINA>
		</PLATFORM>
		<PROCESSING>
			<PIPELINE>
				<PIPE_SECTION section_name="Base Caller">
					<STEP_INDEX>1</STEP_INDEX>
					<PREV_STEP_INDEX>NIL</PREV_STEP_INDEX>
					<PROGRAM>OLB</PROGRAM>
					<VERSION>1.9.3</VERSION>
				</PIPE_SECTION>
			</PIPELINE>
		</PROCESSING>
		<EXPERIMENT_ATTRIBUTES>
			<EXPERIMENT_ATTRIBUTE>
				<TAG>/DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_LAYOUT/PAIRED/@ORIENTATION</TAG>
				<VALUE>5'3-3'5'</VALUE>
			</EXPERIMENT_ATTRIBUTE>
		</EXPERIMENT_ATTRIBUTES>
	</EXPERIMENT>
</EXPERIMENT_SET>
</experiment_xml>
		<run_xml>
<RUN_SET xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="http://www.ncbi.nlm.nih.gov/viewvc/v1/trunk/sra/doc/SRA_1-5/SRA.run.xsd?view=co">
	<RUN alias="TCGA-BF-A1PZ-01A-11D-A18Z_120612_SN590_0162_BC0VNGACXX_s_5_rg.sorted.bam" center_name="HMS-RK">
		<EXPERIMENT_REF refname="TCGA-BF-A1PZ-01A-11D-A18Z_120612_SN590_0162_BC0VNGACXX_s_5_rg.sorted.bam" refcenter="HMS-RK"/>
		<DATA_BLOCK>
			<FILES>
				<FILE filename="TCGA-BF-A1PZ-01A-11D-A18Z_120612_SN590_0162_BC0VNGACXX_s_5_rg.sorted.bam" filetype="bam" checksum_method="MD5" checksum="503386a6bf3b32925a6e878ba7811e03"/>
			</FILES>
		</DATA_BLOCK>
		<RUN_ATTRIBUTES>
			<RUN_ATTRIBUTE>
				<TAG>/DATA_BLOCK/@name</TAG>
				<VALUE>TCGA-BF-A1PZ-01A-11D-A18Z-02</VALUE>
			</RUN_ATTRIBUTE>
		</RUN_ATTRIBUTES>
	</RUN>
</RUN_SET>
</run_xml>
		<analysis_detail_uri>https://cghub.ucsc.edu/cghub/metadata/analysisDetail/000dbac5-2f8c-48d9-9121-c84421e70381</analysis_detail_uri>
		<analysis_submission_uri>https://cghub.ucsc.edu/cghub/metadata/analysisSubmission/000dbac5-2f8c-48d9-9121-c84421e70381</analysis_submission_uri>
		<analysis_data_uri>https://cghub.ucsc.edu/cghub/data/analysis/download/000dbac5-2f8c-48d9-9121-c84421e70381</analysis_data_uri>
</Result>
"""]
