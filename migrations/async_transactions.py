# -*- coding: utf-8 -*-
"""
migrations.async_transactions
----------------------------------

Migrates up/down between states A -> B
A: without
B: with
the following columns
- transaction_log.is_complete
- transatcion_log.is_dry_run
- transatcion_log.success

"""

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def up_transaction(connection):
    logger.info('Migrating async-transactions: up')

    connection.execute("""
    ALTER TABLE transaction_logs ADD COLUMN state        TEXT;
    ALTER TABLE transaction_logs ADD COLUMN committed_by INTEGER;
    ALTER TABLE transaction_logs ADD COLUMN is_dry_run   BOOLEAN;

    UPDATE transaction_logs SET state      = 'SUCCEEDED'  WHERE state       IS NULL;
    UPDATE transaction_logs SET is_dry_run = FALSE        WHERE is_dry_run  IS NULL;
    """)


def down_transaction(connection):
    logger.info('Migrating async-transactions: down')

    connection.execute("""
    ALTER TABLE transaction_logs DROP COLUMN state;
    ALTER TABLE transaction_logs DROP COLUMN committed_by;
    ALTER TABLE transaction_logs DROP COLUMN is_dry_run;
    """)


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
