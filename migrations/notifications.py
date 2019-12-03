# -*- coding: utf-8 -*-
"""
migrations.notifications
----------------------------------

Create `notifications` table.
"""

from gdcdatamodel.models.notifications import Base, Notification

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def up(connection):
    logger.info("Migrating notifications: up")

    Base.metadata.create_all(connection)


def down(connection):
    logger.info("Migrating notifications: down")

    sql_cmd = "DROP TABLE {}".format(Notification.__tablename__)
    connection.execute(sql_cmd)
