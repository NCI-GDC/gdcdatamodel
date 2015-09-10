from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, Text, DateTime, BigInteger
from sqlalchemy import Column, Text, DateTime, text, event
from sqlalchemy.dialects.postgres import ARRAY, JSONB
from sqlalchemy import Table, Column, Integer, ForeignKey
from sqlalchemy.orm import relationship, backref
from json import loads, dumps
from datetime import datetime
import pytz

Base = declarative_base()


def datetime_to_unix(dt):
    return (dt - datetime(1970, 1, 1, tzinfo=pytz.utc)).total_seconds()


class TransactionLog(Base):
    __tablename__ = 'transaction_logs'

    def __repr__(self):
        return "<TransactionLog({}, {})>".format(self.id, self.timestamp)

    def to_json(self, fields=set()):
        existing_fields = [c.name for c in self.__table__.c]+['entities']
        custom_fields = {'timestamp', 'entities'}
        fields = fields or existing_fields

        if set(fields) - set(existing_fields):
            raise RuntimeError('Fields do not exist: {}'.format(
                set(fields) - set(existing_fields)))

        doc = {key: getattr(self, key) for key in fields
               if key not in custom_fields}

        if 'entities' in fields:
            doc['entities'] = [n.to_json() for n in self.entities]
        if 'timestamp' in fields:
            doc['timestamp'] = datetime_to_unix(self.timestamp)

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

    timestamp = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text('now()'),
    )

    canonical_json = Column(
        JSONB,
        nullable=False,
    )

    doc_format = Column(
        Text,
        nullable=False,
    )

    doc = Column(
        Text,
        nullable=False,
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


class TransactionSnapshot(Base):
    __tablename__ = 'transaction_snapshots'

    def __repr__(self):
        return "<TransactionSnapshot({}, {})>".format(self.node_id, self.tid)

    def to_json(self, fields=set()):
        fields = set(fields)
        existing_fields = [c.name for c in self.__table__.c]
        fields = fields or existing_fields
        if set(fields) - set(existing_fields):
            raise RuntimeError('Fields do not exist: {}'.format(
                set(fields) - set(existing_fields)))
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
