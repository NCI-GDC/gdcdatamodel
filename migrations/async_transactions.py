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
    logger.info("Migrating async-transactions: up")

    connection.execute(
        """
    ALTER TABLE transaction_logs ADD COLUMN state        TEXT;
    ALTER TABLE transaction_logs ADD COLUMN committed_by INTEGER;
    ALTER TABLE transaction_logs ADD COLUMN is_dry_run   BOOLEAN;
    ALTER TABLE transaction_logs ADD COLUMN closed       BOOLEAN NOT NULL DEFAULT FALSE;

    CREATE INDEX transaction_logs_program_idx on transaction_logs (program);
    CREATE INDEX transaction_logs_project_idx on transaction_logs (project);
    CREATE INDEX transaction_logs_is_dry_run_idx on transaction_logs  (is_dry_run);
    CREATE INDEX transaction_logs_committed_by_idx on transaction_logs  (committed_by);
    CREATE INDEX transaction_logs_closed_idx on transaction_logs (closed);
    CREATE INDEX transaction_logs_state_idx on transaction_logs (state);
    CREATE INDEX transaction_logs_submitter_idx on transaction_logs (submitter);

    UPDATE transaction_logs SET state      = 'SUCCEEDED'  WHERE state       IS NULL;
    UPDATE transaction_logs SET is_dry_run = FALSE        WHERE is_dry_run  IS NULL;
    """
    )


def down_transaction(connection):
    logger.info("Migrating async-transactions: down")

    connection.execute(
        """
    DROP INDEX transaction_logs_program_idx;
    DROP INDEX transaction_logs_project_idx;
    DROP INDEX transaction_logs_is_dry_run_idx;
    DROP INDEX transaction_logs_committed_by_idx;
    DROP INDEX transaction_logs_closed_idx;
    DROP INDEX transaction_logs_state_idx;
    DROP INDEX transaction_logs_submitter_idx;

    ALTER TABLE transaction_logs DROP COLUMN state;
    ALTER TABLE transaction_logs DROP COLUMN closed;
    ALTER TABLE transaction_logs DROP COLUMN committed_by;
    ALTER TABLE transaction_logs DROP COLUMN is_dry_run;
    """
    )


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
