import uuid

from gdcdatamodel import models


def test_log_redaction(g, redacted_fixture):

    log_id, log_entries_count = redacted_fixture

    # query for just created nodes
    with g.session_scope() as sxn:
        xlog = sxn.query(models.redaction.RedactionLog).get(log_id)
        assert xlog is not None
        assert len(xlog.entries) == log_entries_count


def test_all_rescind(g, redacted_fixture):

    log_id, log_entries_count = redacted_fixture

    # rescind
    with g.session_scope() as sxn:
        xlog = sxn.query(models.redaction.RedactionLog).get(log_id)
        xlog.rescind_all("TEST")
        sxn.commit()

    # verify
    with g.session_scope() as sxn:
        xlog = sxn.query(models.redaction.RedactionLog).get(log_id)
        assert len(xlog.entries) == log_entries_count
        assert xlog.is_rescinded is True


def test_single_rescind(g, redacted_fixture):

    log_id, log_entries_count = redacted_fixture

    # rescind
    with g.session_scope() as sxn:
        xlog = sxn.query(models.redaction.RedactionLog).get(log_id)
        entry = xlog.entries[0]
        entry.rescind("TEST")
        rescinded_entry_id = entry.node_id
        sxn.commit()

    # verify
    with g.session_scope() as sxn:
        xlog = sxn.query(models.redaction.RedactionLog).get(log_id)
        assert len(xlog.entries) == log_entries_count
        assert xlog.is_rescinded is False

        for entry in xlog.entries:
            if entry.node_id == rescinded_entry_id:
                assert entry.rescinded is True
            else:
                assert entry.rescinded is False
