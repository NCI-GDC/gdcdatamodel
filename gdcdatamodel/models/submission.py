# -*- coding: utf-8 -*-
"""
gdcdatamodel.models.submission
----------------------------------

Models for submission TransactionLogs
"""

from datetime import datetime
from json import loads, dumps
from sqlalchemy import func
from sqlalchemy.dialects.postgres import JSONB
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship, deferred

import pytz

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Index,
    Text,
    text,
)

Base = declarative_base()


def datetime_to_unix(dt):
    return (dt - datetime(1970, 1, 1, tzinfo=pytz.utc)).total_seconds()


class TransactionLog(Base):
    __tablename__ = 'transaction_logs'

    @declared_attr
    def __table_args__(cls):
        tbl = cls.__tablename__
        return (
            Index('{}_program_idx'.format(tbl), 'program'),
            Index('{}_project_idx'.format(tbl), 'project'),
            Index('{}_is_dry_run_idx'.format(tbl), 'is_dry_run'),
            Index('{}_committed_by_idx'.format(tbl), 'committed_by'),
            Index('{}_closed_idx'.format(tbl), 'closed'),
            Index('{}_state_idx'.format(tbl), 'state'),
            Index('{}_submitter_idx'.format(tbl), 'submitter'),
            Index('{}_created_datetime_idx'.format(tbl), 'created_datetime'),
            Index('{}_project_id_idx'.format(tbl), cls.program+'-'+cls.project),
        )

    def __repr__(self):
        return "<TransactionLog({}, {})>".format(
            self.id, self.created_datetime)

    def to_json(self, fields=set()):
        # Source fields
        existing_fields = [c.name for c in self.__table__.c]+[
            'entities', 'documents']
        custom_fields = {'created_datetime', 'entities', 'documents'}

        # Pull out child fields
        entity_fields = {f for f in fields if f.startswith('entities.')}
        document_fields = {f for f in fields if f.startswith('documents.')}
        fields = fields - entity_fields - document_fields

        # Reformat child fields
        entity_fields = {
            f.replace('entities.', '') for f in entity_fields}
        document_fields = {
            f.replace('documents.', '') for f in document_fields}

        # Default fields
        if not fields:
            fields = {'id', 'submitter', 'role', 'program', 'created_datetime'}

        # Check for field existence
        if set(fields) - set(existing_fields):
            raise RuntimeError('Fields do not exist: {}'.format(
                ', '.join((set(fields) - set(existing_fields)))))

        # Set standard fields
        doc = {key: getattr(self, key) for key in fields
               if key not in custom_fields}

        # Add custom fields
        if 'entities' in fields or entity_fields:
            doc['entities'] = [
                n.to_json(entity_fields) for n in self.entities]
        if 'documents' in fields or document_fields:
            doc['documents'] = [
                n.to_json(document_fields) for n in self.documents]
        if 'created_datetime' in fields:
            doc['created_datetime'] = self.created_datetime.isoformat("T")

        return doc

    id = Column(
        Integer,
        primary_key=True,
    )

    submitter = Column(
        Text,
    )

    role = Column(
        Text,
        nullable=False,
    )

    program = Column(
        Text,
        nullable=False,
    )

    project = Column(
        Text,
        nullable=False,
    )

    #: Specifies a non-dry_run transaction that repeated this
    #: transaction in an attempt to write to the database
    committed_by = Column(
        Integer,
    )

    #: Was this transaction a dry_run (for validation)
    is_dry_run = Column(
        Boolean,
        nullable=False,
    )

    #: Has this transaction succeeded, errored, failed, etc.
    state = Column(
        Text,
        nullable=False,
    )

    closed = Column(
        Boolean,
        default=False,
        nullable=False,
    )

    @hybrid_property
    def project_id(self):
        return self.program + '-' + self.project

    @project_id.expression
    def project_id(cls):
        return func.concat(cls.program, '-', cls.project)

    created_datetime = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text('now()'),
    )

    canonical_json = deferred(Column(
        JSONB,
        nullable=False,
    ))


class TransactionSnapshot(Base):
    __tablename__ = 'transaction_snapshots'

    def __repr__(self):
        return "<TransactionSnapshot({}, {})>".format(
            self.id, self.transaction_id)

    def to_json(self, fields=set()):
        fields = set(fields)
        existing_fields = [c.name for c in self.__table__.c]
        if not fields:
            fields = existing_fields
        if set(fields) - set(existing_fields):
            raise RuntimeError('Entity fields do not exist: {}'.format(
                ', '.join((set(fields) - set(existing_fields)))))
        doc = {key: getattr(self, key) for key in fields}
        return doc

    id = Column(
        Integer,
        primary_key=True,
    )

    entity_id = Column(
        Text,
        nullable=False,
        index=True,
    )

    transaction_id = Column(
        Integer,
        ForeignKey('transaction_logs.id'),
        index=True,
    )

    action = Column(
        Text,
        nullable=False,
    )

    old_props = Column(
        JSONB,
        nullable=False,
    )

    new_props = Column(
        JSONB,
        nullable=False,
    )

    transaction = relationship(
        "TransactionLog",
        backref="entities"
    )
    
    __table_args__ = (
        Index('snapshot_transaction_idx', 'transaction_id'),
    )


class TransactionDocument(Base):
    __tablename__ = 'transaction_documents'

    def to_json(self, fields=set()):
        # Source fields
        fields = set(fields)
        existing_fields = {c.name for c in self.__table__.c}

        # Default fields
        if not fields:
            fields = existing_fields

        # Check field existence
        if set(fields) - set(existing_fields):
            raise RuntimeError('Entity fields do not exist: {}'.format(
                ', '.join(fields - existing_fields)))

        # Generate doc
        doc = {key: getattr(self, key) for key in fields}
        return doc

    id = Column(
        Integer,
        primary_key=True,
        nullable=False,
    )

    transaction_id = Column(
        Integer,
        ForeignKey('transaction_logs.id'),
        primary_key=True,
    )

    name = Column(
        Text,
    )

    doc_format = Column(
        Text,
        nullable=False,
    )

    doc = deferred(Column(
        Text,
        nullable=False,
    ))

    response_json = deferred(Column(
        JSONB,
    ))

    transaction = relationship(
        "TransactionLog",
        backref="documents"
    )
    
    __table_args__ = (
        Index('document_transaction_idx', 'transaction_id'),
    )

    @property
    def is_json(self):
        if self.doc_format.upper() != 'JSON':
            return False
        else:
            return True

    @property
    def is_xml(self):
        if self.doc_format.upper() != 'XML':
            return False
        else:
            return True

    @property
    def json(self):
        if not self.is_json:
            return None
        return loads(self.doc)

    @json.setter
    def json(self, doc):
        self.doc_format = 'JSON'
        self.doc = dumps(doc)

    @property
    def xml(self):
        if not self.is_xml:
            return None
        return self.doc

    @xml.setter
    def xml(self, doc):
        self.doc_format = 'XML'
        self.doc = doc
