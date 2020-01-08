from gdcdatamodel import models as md
from psqlgraph import Node

from test.conftest import BaseTestCase


class TestCacheRelatedCases(BaseTestCase):

    def test_insert_single_association_proxy(self):
        with self.g.session_scope() as s:
            case = md.Case('case_id_1')
            sample = md.Sample('sample_id_1')
            sample.cases = [case]
            s.merge(sample)

        with self.g.session_scope() as s:
            sample = self.g.nodes(md.Sample).subq_path('cases').one()
            assert sample._related_cases_from_cache == [case]

    def test_insert_single_edge(self):
        with self.g.session_scope() as s:
            case = s.merge(md.Case('case_id_1'))
            sample = s.merge(md.Sample('sample_id_1'))
            edge = md.SampleDerivedFromCase(sample.node_id, case.node_id)
            s.merge(edge)

        with self.g.session_scope() as s:
            sample = self.g.nodes(md.Sample).subq_path('cases').one()
            assert sample._related_cases_from_cache == [case]

    def test_insert_double_edge_in(self):
        with self.g.session_scope() as s:
            case = md.Case('case_id_1')
            sample1 = md.Sample('sample_id_1')
            sample2 = md.Sample('sample_id_2')
            case.samples = [sample1, sample2]
            s.merge(case)

        with self.g.session_scope() as s:
            samples = self.g.nodes(md.Sample).subq_path('cases').all()
            self.assertEqual(len(samples), 2)
            for sample in samples:
                assert sample._related_cases_from_cache == [case]

    def test_insert_double_edge_out(self):
        with self.g.session_scope() as s:
            case1 = md.Case('case_id_1')
            case2 = md.Case('case_id_2')
            sample = md.Sample('sample_id_1')
            sample.cases = [case1, case2]
            s.merge(sample)

        with self.g.session_scope() as s:
            sample = self.g.nodes(md.Sample).subq_path('cases').one()
            assert {c.node_id for c in sample._related_cases} == \
                   {c.node_id for c in [case1, case2]}

    def test_insert_multiple_edges(self):
        with self.g.session_scope() as s:
            case = md.Case('case_id_1')
            sample = md.Sample('sample_id_1')
            portion = md.Portion('portion_id_1')
            analyte = md.Analyte('analyte_id_1')
            aliquot = md.Aliquot('aliquot_id_1')
            general_file = md.File('file_id_1')

            sample.cases = [case]
            portion.samples = [sample]
            analyte.portions = [portion]
            aliquot.analytes = [analyte]
            general_file.aliquots = [aliquot]
            s.merge(case)

        with self.g.session_scope() as s:
            nodes = self.g.nodes(Node).all()
            nodes = [n for n in nodes if n.label not in ['case']]

            for node in nodes:
                assert node._related_cases == [case]

    def test_insert_update_children(self):
        with self.g.session_scope() as s:
            aliquot = s.merge(md.Aliquot('aliquot_id_1'))
            sample = s.merge(md.Sample('sample_id_1'))
            aliquot.samples = [sample]
            s.merge(md.Case('case_id_1'))

        with self.g.session_scope() as s:
            case = self.g.nodes(md.Case).one()
            sample = self.g.nodes(md.Sample).one()
            sample.cases = [case]

        with self.g.session_scope() as s:
            aliquot = self.g.nodes(md.Aliquot).one()
            sample = self.g.nodes(md.Sample).one()

            assert sample._related_cases == [case]
            assert aliquot._related_cases == [case]

    def test_delete_dst_association_proxy(self):
        with self.g.session_scope() as s:
            case = md.Case('case_id_1')
            aliquot = md.Aliquot('aliquot_id_1')
            sample = md.Sample('sample_id_1')
            aliquot.samples = [sample]
            sample.cases = [case]
            s.merge(case)

        with self.g.session_scope() as s:
            case = self.g.nodes(md.Case).one()
            case.samples = []

        with self.g.session_scope() as s:
            assert not self.g.nodes(md.Sample).subq_path('cases').count()

        with self.g.session_scope() as s:
            sample = self.g.nodes(md.Sample).one()
            aliquot = self.g.nodes(md.Aliquot).one()

            sample._related_cases = []
            aliquot._related_cases = []

    def test_delete_src_association_proxy(self):
        with self.g.session_scope() as s:
            case = md.Case('case_id_1')
            aliquot = md.Aliquot('aliquot_id_1')
            sample = md.Sample('sample_id_1')
            aliquot.samples = [sample]
            sample.cases = [case]
            s.merge(case)

        with self.g.session_scope() as s:
            sample = self.g.nodes(md.Sample).one()
            sample.cases = []

        with self.g.session_scope() as s:
            assert not self.g.nodes(md.Sample).subq_path('cases').count()

        with self.g.session_scope() as s:
            sample = self.g.nodes(md.Sample).one()
            aliquot = self.g.nodes(md.Aliquot).one()

            assert sample._related_cases == []
            assert aliquot._related_cases == []

    def test_delete_edge(self):
        with self.g.session_scope() as s:
            case = md.Case('case_id_1')
            aliquot = md.Aliquot('aliquot_id_1')
            sample = md.Sample('sample_id_1')
            aliquot.samples = [sample]
            sample.cases = [case]
            s.merge(case)

        with self.g.session_scope() as s:
            case = self.g.nodes(md.Case).one()
            edge = [
                e for e in case.edges_in
                if e.label != 'relates_to' and e.src.label == 'sample'
            ][0]
            s.delete(edge)

        with self.g.session_scope() as s:
            assert not self.g.nodes(md.Sample).subq_path('cases').count()

        with self.g.session_scope() as s:
            sample = self.g.nodes(md.Sample).one()
            aliquot = self.g.nodes(md.Aliquot).one()

            sample._related_cases == []
            aliquot._related_cases == []

    def test_delete_parent(self):
        with self.g.session_scope() as s:
            case = md.Case('case_id_1')
            sample = md.Sample('sample_id_1')
            sample.cases = [case]
            s.merge(case)

        with self.g.session_scope() as s:
            case = self.g.nodes(md.Case).one()
            s.delete(case)

        with self.g.session_scope() as s:
            sample = self.g.nodes(md.Sample).one()

            assert sample._related_cases == []

    def test_delete_one_parent(self):
        with self.g.session_scope() as s:
            case1 = md.Case('case_id_1')
            case2 = md.Case('case_id_2')
            sample = md.Sample('sample_id_1')
            sample.cases = [case1, case2]
            s.merge(sample)

        with self.g.session_scope() as s:
            case1 = self.g.nodes(md.Case).ids('case_id_1').one()
            s.delete(case1)

        with self.g.session_scope() as s:
            sample = self.g.nodes(md.Sample).one()
            assert sample._related_cases == [case2]

    def test_preserve_timestamps(self):
        """Confirm cache changes do not affect the case's timestamps."""
        with self.g.session_scope() as s:
            s.merge(md.Case('case_id_1'))

        with self.g.session_scope():
            case = self.g.nodes(md.Case).one()
            old_created_datetime = case.created_datetime
            old_updated_datetime = case.updated_datetime

            # Test addition of cache edges.
            sample = md.Sample('sample_id_1')
            portion = md.Portion('portion_id_1')
            analyte = md.Analyte('analyte_id_1')
            aliquot = md.Aliquot('aliquot_id_1')
            sample.cases = [case]
            portion.samples = [sample]
            analyte.portions = [portion]
            aliquot.analytes = [analyte]

            sample2 = md.Sample('sample_id_2')
            sample2.cases = [case]

        with self.g.session_scope() as s:
            case = self.g.nodes(md.Case).one()

            # Exercise a few cache edge removal use cases as well.
            analyte = self.g.nodes(md.Analyte).one()
            sample2 = self.g.nodes(md.Sample).get('sample_id_2')
            s.delete(analyte)
            sample2.cases = []

        with self.g.session_scope():
            case = self.g.nodes(md.Case).one()
            assert case.created_datetime == old_created_datetime
            assert case.updated_datetime == old_updated_datetime
