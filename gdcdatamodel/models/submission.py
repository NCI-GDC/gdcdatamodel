from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, Text, DateTime, BigInteger
from sqlalchemy import Column, Text, DateTime, text, event
from sqlalchemy.dialects.postgres import ARRAY, JSONB
from sqlalchemy import Table, Column, Integer, ForeignKey
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.hybrid import hybrid_property
from json import loads, dumps
from datetime import datetime
import pytz

Base = declarative_base()


def datetime_to_unix(dt):
    return (dt - datetime(1970, 1, 1, tzinfo=pytz.utc)).total_seconds()


class TransactionLog(Base):
    __tablename__ = 'transaction_logs'

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

    project = Column(
        Text,
        nullable=False,
    )

    @hybrid_property
    def project_id(self):
        return self.program + '-' + self.project

    created_datetime = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text('now()'),
    )

    canonical_json = Column(
        JSONB,
        nullable=False,
    )


class TransactionSnapshot(Base):
    __tablename__ = 'transaction_snapshots'

    def __repr__(self):
        return "<TransactionSnapshot({}, {})>".format(self.node_id, self.tid)

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
        Text,
        primary_key=True,
        nullable=False,
    )

    transaction_id = Column(
        Integer,
        ForeignKey('transaction_logs.id'),
        primary_key=True,
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

    doc = Column(
        Text,
        nullable=False,
    )

    response_json = Column(
        JSONB,
    )

    transaction = relationship(
        "TransactionLog",
        backref="documents"
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
