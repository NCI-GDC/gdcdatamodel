# -*- coding: utf-8 -*-
"""
migrations.async_transactions
----------------------------------

Migrates up/down between states A -> B
A: without
B: with
the following indexes per secondary key
- lower(_props ->> key)
- _props ->> key

"""

from psqlgraph import Node
from gdcdatamodel.models import get_secondary_key_indexes
from gdcdatamodel.models.submission import TransactionLog
from sqlalchemy import Index


import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


TX_LOG_PROJECT_ID_IDX = Index(
    "transaction_logs_project_id_idx",
    TransactionLog.program + "_" + TransactionLog.project,
)


def up_transaction(connection):
    logger.info("Migrating async-transactions: up")

    for cls in Node.get_subclasses():
        for index in get_secondary_key_indexes(cls):
            logger.info("Creating %s", index.name)
            index.create(connection)
    TX_LOG_PROJECT_ID_IDX.create(connection)


def down_transaction(connection):
    logger.info("Migrating async-transactions: down")

    for cls in Node.get_subclasses():
        for index in get_secondary_key_indexes(cls):
            logger.info("Dropping %s", index.name)
            index.drop(connection)
    TX_LOG_PROJECT_ID_IDX.drop(connection)


def up(connection):
    transaction = connection.begin()
    try:
        up_transaction(connection)
        transaction.commit()
    except Exception:
        transaction.rollback()
        raise


def down(connection):
    transaction = connection.begin()
    try:
        down_transaction(connection)
        transaction.commit()
    except Exception:
        transaction.rollback()
        raise
