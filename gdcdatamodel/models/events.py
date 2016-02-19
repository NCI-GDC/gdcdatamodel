# -*- coding: utf-8 -*-
"""
gdcdatamodel.models.events
----------------------------------

Defines a model to log GDC events.  These events can be anything from
reporting an ETL failure to noting that a scheduled MD5 check has
succeeded.

"""

from sqlalchemy.ext.declarative import declarative_base

import enum

from sqlalchemy.dialects.postgres import (
    ARRAY,
    JSONB,
)

from sqlalchemy import (
    Column,
    Text,
    DateTime,
    BigInteger,
    text,
    Index,
    Enum,
    Boolean,
)


Base = declarative_base()


class GDCEventPriority(enum.Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class GDCEventTypes(enum.Enum):
    #: An event that resulted in or was trigged by an error
    ERROR = "ERROR"

    #: A non-user event that occurred externally.
    EXTERNAL = "EXTERNAL"

    #: A low priority event that might be useful later to look at
    INFO = "INFO"

    #: An event done manually and recorded by a superuser
    MANUAL_ADMIN_ACTION = "MANUAL_ADMIN_ACTION"

    #: An automated process reporting completion
    PROCESS_COMPLETED = "PROCESS_COMPLETED"

    #: An automated process reporting initiation
    PROCESS_STARTED = "PROCESS_STARTED"

    #: An automated process reporting a state that is not initiation
    #: or commpletion
    PROCESS_UPDATED = "PROCESS_UPDATED"

    #: Resolves another event that needed resolution
    RESOLUTION = "RESOLUTION"

    #: An event triggered by an external user
    USER_ACTION = "USER_ACTION"

    #: An event that resulted in or was triggered by a warning
    WARNING = "WARNING"

    #: An event of unknown classification.  Try not to use this one.
    UNKNOWN = "UNKNOWN"


class GDCEvent(Base):

    __tablename__ = 'gdc_events'
    __table_args__ = (
        Index('gdc_events_node_ids_idx', 'node_ids'),
        Index('gdc_events_node_ids_idx', 'type'),
        Index('gdc_events_node_ids_idx', 'tags'),
        Index('gdc_events_node_ids_idx', 'summary'),
        Index('gdc_events_node_ids_idx', 'hidden'),
        Index('gdc_events_node_ids_idx', 'needs_resolution'),
        Index('gdc_events_node_ids_idx', 'created'),
        Index('gdc_events_node_ids_idx', 'creator'),
    )

    def __repr__(self):
        return (
            "<GDCEvent(id={}, type='{}', tabs='{}')>"
            .format(self.id, self.type, len(self.tags))
        )

    #: The id of the entry, sequential and self incrementing
    id = Column(
        BigInteger,
        primary_key=True,
        nullable=False
    )

    #: The type of event, if you need a new type, add it to the
    #: GDCEventTypes enum
    type = Column(Enum(
        GDCEventTypes,
        # Set native_enum to false to prevent schema being stored at
        # the database level.  This allows us to specify new types as
        # needed at the software level without a table lock at the
        # cost of using a VARCHAR instead of a native enum.
        native_enum=False,
        nullable=False,
    ))

    #: How important it is that this event is seen/resolved.
    priority = Column(Enum(
        GDCEventPriority,
        # Set native_enum to false to prevent schema being stored at
        # the database level.  This allows us to specify new types as
        # needed at the software level without a table lock at the
        # cost of using a VARCHAR instead of a native enum.
        native_enum=False,
        default=GDCEventPriority.MEDIUM,
    ))

    #: The project ids required for external viewing of this event
    project_ids = Column(
        ARRAY(Text),
        nullable=True,
    )

    #: Should be False if this event intended to be hidden from ALL
    #: external users.
    hidden = Column(
        Boolean,
        nullable=False,
    )

    #: A place to note which (if any) nodes were affected by this
    #: event
    node_ids = Column(
        ARRAY(Text),
        nullable=True,
    )

    #: NoSQL key/values specific to different events.  These CAN be
    #: displayed externally.
    tags = Column(
        JSONB,
        nullable=True,
    )

    #: Sort description: Free form text for the event creator to
    #: describe the event
    summary = Column(
        Text,
        nullable=False,
    )

    #: Sort description: Free form text for the event creator to
    #: describe the event
    description = Column(
        Text,
        nullable=False,
    )

    #: The application name that created this event
    creator = Column(
        Text,
        nullable=False,
    )

    #: The ISO 8601 datetime when this event was logged
    created = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text('now()'),
    )

    #: Should this event log trigger any sort of future action?
    needs_resolution = Column(
        Boolean,
        default=False,
    )

    #: Free form JSONB. These should NOT be displayed externally.
    system_annotations = Column(
        JSONB,
        default={},
    )
