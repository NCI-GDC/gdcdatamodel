from gdcdatamodel import models as md
from psqlgraph import Node, Edge, PsqlGraphDriver

import unittest

host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'
g = PsqlGraphDriver(host, user, password, database)


class TestCacheRelatedCases(unittest.TestCase):

    def setUp(self):
        self._clear_tables()

    def tearDown(self):
        self._clear_tables()

    def _clear_tables(self):
        conn = g.engine.connect()
        conn.execute('commit')
        for table in Node().get_subclass_table_names():
            if table != Node.__tablename__:
                conn.execute('delete from {}'.format(table))
        for table in Edge.get_subclass_table_names():
            if table != Edge.__tablename__:
                conn.execute('delete from {}'.format(table))
        conn.execute('delete from versioned_nodes')
        conn.execute('delete from _voided_nodes')
        conn.execute('delete from _voided_edges')
        conn.close()

    def test_insert_single_association_proxy(self):
        with g.session_scope() as s:
            case = md.Case('case_id_1')
            sample = md.Sample('sample_id_1')
            sample.cases = [case]
            s.merge(sample)

        with g.session_scope() as s:
            sample = g.nodes(md.Sample).subq_path('cases').one()
            assert sample._related_cases_from_cache == [case]

    def test_insert_single_edge(self):
        with g.session_scope() as s:
            case = s.merge(md.Case('case_id_1'))
            sample = s.merge(md.Sample('sample_id_1'))
            edge = md.SampleDerivedFromCase(sample.node_id, case.node_id)
            s.merge(edge)

        with g.session_scope() as s:
            sample = g.nodes(md.Sample).subq_path('cases').one()
            assert sample._related_cases_from_cache == [case]

    def test_insert_double_edge_in(self):
        with g.session_scope() as s:
            case = md.Case('case_id_1')
            sample1 = md.Sample('sample_id_1')
            sample2 = md.Sample('sample_id_2')
            case.samples = [sample1, sample2]
            s.merge(case)

        with g.session_scope() as s:
            samples = g.nodes(md.Sample).subq_path('cases').all()
            self.assertEqual(len(samples), 2)
            for sample in samples:
                assert sample._related_cases_from_cache == [case]

    def test_insert_double_edge_out(self):
        with g.session_scope() as s:
            case1 = md.Case('case_id_1')
            case2 = md.Case('case_id_2')
            sample = md.Sample('sample_id_1')
            sample.cases = [case1, case2]
            s.merge(sample)

        with g.session_scope() as s:
            sample = g.nodes(md.Sample).subq_path('cases').one()
            assert sample._related_cases == [case1, case2]

    def test_insert_multiple_edges(self):
        with g.session_scope() as s:
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

        with g.session_scope() as s:
            nodes = g.nodes(Node).all()
            nodes = [n for n in nodes if n.label not in ['case']]

            for node in nodes:
                assert node._related_cases == [case]

    def test_insert_update_children(self):
        with g.session_scope() as s:
            aliquot = s.merge(md.Aliquot('aliquot_id_1'))
            sample = s.merge(md.Sample('sample_id_1'))
            aliquot.samples = [sample]
            s.merge(md.Case('case_id_1'))

        with g.session_scope() as s:
            case = g.nodes(md.Case).one()
            sample = g.nodes(md.Sample).one()
            sample.cases = [case]

        with g.session_scope() as s:
            aliquot = g.nodes(md.Aliquot).one()
            sample = g.nodes(md.Sample).one()

            assert sample._related_cases == [case]
            assert aliquot._related_cases == [case]

    def test_delete_dst_association_proxy(self):
        with g.session_scope() as s:
            case = md.Case('case_id_1')
            aliquot = md.Aliquot('aliquot_id_1')
            sample = md.Sample('sample_id_1')
            aliquot.samples = [sample]
            sample.cases = [case]
            s.merge(case)

        with g.session_scope() as s:
            case = g.nodes(md.Case).one()
            case.samples = []

        with g.session_scope() as s:
            assert not g.nodes(md.Sample).subq_path('cases').count()

        with g.session_scope() as s:
            sample = g.nodes(md.Sample).one()
            aliquot = g.nodes(md.Aliquot).one()

            sample._related_cases = []
            aliquot._related_cases = []

    def test_delete_src_association_proxy(self):
        with g.session_scope() as s:
            case = md.Case('case_id_1')
            aliquot = md.Aliquot('aliquot_id_1')
            sample = md.Sample('sample_id_1')
            aliquot.samples = [sample]
            sample.cases = [case]
            s.merge(case)

        with g.session_scope() as s:
            sample = g.nodes(md.Sample).one()
            sample.cases = []

        with g.session_scope() as s:
            assert not g.nodes(md.Sample).subq_path('cases').count()

        with g.session_scope() as s:
            sample = g.nodes(md.Sample).one()
            aliquot = g.nodes(md.Aliquot).one()

            assert sample._related_cases == []
            assert aliquot._related_cases == []

    def test_delete_edge(self):
        with g.session_scope() as s:
            case = md.Case('case_id_1')
            aliquot = md.Aliquot('aliquot_id_1')
            sample = md.Sample('sample_id_1')
            aliquot.samples = [sample]
            sample.cases = [case]
            s.merge(case)

        with g.session_scope() as s:
            case = g.nodes(md.Case).one()
            edge = case.edges_in[0]
            s.delete(edge)

        with g.session_scope() as s:
            assert not g.nodes(md.Sample).subq_path('cases').count()

        with g.session_scope() as s:
            sample = g.nodes(md.Sample).one()
            aliquot = g.nodes(md.Aliquot).one()

            sample._related_cases == []
            aliquot._related_cases == []

    def test_delete_parent(self):
        with g.session_scope() as s:
            case = md.Case('case_id_1')
            sample = md.Sample('sample_id_1')
            sample.cases = [case]
            s.merge(case)

        with g.session_scope() as s:
            case = g.nodes(md.Case).one()
            s.delete(case)

        with g.session_scope() as s:
            sample = g.nodes(md.Sample).one()

            assert sample._related_cases == []

    def test_delete_one_parent(self):
        with g.session_scope() as s:
            case1 = md.Case('case_id_1')
            case2 = md.Case('case_id_2')
            sample = md.Sample('sample_id_1')
            sample.cases = [case1, case2]
            s.merge(sample)

        with g.session_scope() as s:
            case1 = g.nodes(md.Case).ids('case_id_1').one()
            s.delete(case1)

        with g.session_scope() as s:
            sample = g.nodes(md.Sample).one()
            assert sample._related_cases == [case2]
