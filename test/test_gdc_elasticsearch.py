from base import ZugTestBase, PreludeMixin
from elasticsearch import Elasticsearch
from gdcdatamodel.models import File, Aliquot
from elasticsearch.exceptions import AuthorizationException
from zug.gdc_elasticsearch import GDCElasticsearch
import es_fixtures
import os


class GDCElasticsearchTest(PreludeMixin, ZugTestBase):

    def setUp(self):
        super(GDCElasticsearchTest, self).setUp()
        es_fixtures.insert(self.graph)
        self.add_file_nodes()
        os.environ["ELASTICSEARCH_HOST"] = "localhost"
        self.es = Elasticsearch("localhost")

    def add_file_nodes(self):
        file = File(
            node_id='file1',
            file_name='TCGA-WR-A838-01A-12R-A406-31_rnaseq_fastq.tar',
            file_size=12916551680,
            md5sum='d7e6cbd40ef2f5b6607cb4af982280a9',
            state='live',
            state_comment=None,
            submitter_id='5cb6bc65-9cd5-45ac-9078-551bc7408906'
        )
        to_delete_file = File(
            node_id='file2',
            file_name='a_file_to_be_deleted.txt',
            file_size=5,
            md5sum='foobar',
            state='live',
            state_comment=None,
            submitter_id='5cb6bc65-9cd5-45ac-9078-551bc7408906'
        )
        to_delete_file.system_annotations["to_delete"] = True
        with self.graph.session_scope():
            aliquot = self.graph.nodes(Aliquot).ids('84df0f82-69c4-4cd3-a4bd-f40d2d6ef916').one()
            aliquot.files.append(file)
            aliquot.files.append(to_delete_file)

    def tearDown(self):
        super(GDCElasticsearchTest, self).tearDown()
        indices = self.es.indices.get_aliases()
        for index, info in indices.items():
            if info.get("aliases"):
                for alias in info["aliases"].keys():
                    self.es.indices.delete_alias(index=index, name=alias)
            self.es.indices.delete(index)

    def make_gdc_es(self):
        return GDCElasticsearch(index_base="gdc_es_test")

    def get_es_indices(self):
        return self.es.indices.get_aliases().keys()

    def test_basic_es_generate(self):
        gdces = self.make_gdc_es()
        gdces.go()
        self.assertEqual(len(self.get_es_indices()), 1)
        # also verify that the to_delete file is not in the index and
        # got deleted
        with self.graph.session_scope():
            self.assertIsNone(self.graph.nodes(File).get('file2'))
            self.assertFalse(self.es.exists(index="gdc_es_test",
                                            doc_type="file",
                                            id="file2"))

    def test_doesnt_delete_file_with_derived_files(self):
        gdces = self.make_gdc_es()
        with self.graph.session_scope():
            to_delete_file = self.graph.nodes(File).ids("file2").one()
            derived_file = self.get_fuzzed_node(File, state="live")
            to_delete_file.derived_files = [derived_file]
        gdces.go()
        self.assertEqual(len(self.get_es_indices()), 1)
        # verify that th eto_delete file did not get deleted
        with self.graph.session_scope():
            self.assertEqual(self.graph.nodes(File).get('file2').file_name,
                             "a_file_to_be_deleted.txt")

    def test_old_index_cleanup(self):
        for i in range(7):
            gdces = self.make_gdc_es()
            gdces.go()
        indices = self.get_es_indices()
        # running the index build seven times should delete indicies 1 and 2
        self.assertEqual(set(indices), {"gdc_es_test_3",
                                        "gdc_es_test_4",
                                        "gdc_es_test_5",
                                        "gdc_es_test_6",
                                        "gdc_es_test_7"})
        for i in xrange(3, 6):
            with self.assertRaises(AuthorizationException):
                self.es.indices.stats('gdc_es_test_'+str(i))
