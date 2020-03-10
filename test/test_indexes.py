# -*- coding: utf-8 -*-
"""
gdcdatamodel.test.conftest
----------------------------------

Test GDC specific index creation.

"""


def test_secondary_key_indexes(indexes):
    assert 'index_node_datasubtype_name_lower' in indexes
    assert 'index_node_analyte_project_id' in indexes
    assert 'index_4df72441_famihist_submitte_id_lower' in indexes
    assert 'transaction_logs_project_id_idx' in indexes
