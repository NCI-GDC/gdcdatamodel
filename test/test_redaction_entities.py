import uuid

from gdcdatamodel import models


def test_log_redaction(g):
    """
    Args:
        g (psqlgraph.PsqlGraphDriver):
    :return:
    """
    log = models.redaction.RedactionLog()
    log.initiated_by = "Rest"
    log.program = "AB"
    log.project = "BQ"
    log.reason = "Err"

    # redaction entry
    entry = models.redaction.RedactionEntry(node_id=str(uuid.uuid4()))

    with g.session_scope() as sxn:
        sxn.add(log)
        entry.redaction_log = log
        sxn.add(entry)
        sxn.commit()

    # query for just created nodes
    with g.session_scope() as sxn:
        xlog = sxn.query(models.redaction.RedactionLog).get(log.id)
        assert xlog is not None
        assert len(xlog.entries) == 1
