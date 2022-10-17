"""
migrations.notifications
----------------------------------

Create `notifications` table.
"""

import logging

from gdcdatamodel import models

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def up(connection):
    logger.info("Migrating notifications: up")

    models.notifications.Base.metadata.create_all(connection)
    models.redaction.Base.metadata.create_all(connection)
    models.qcreport.Base.metadata.create_all(connection)


def down(connection):
    logger.info("Migrating notifications: down")

    models.notifications.Base.metadata.drop_all(connection)
    models.redaction.Base.metadata.drop_all(connection)
    models.qcreport.Base.metadata.drop_all(connection)
