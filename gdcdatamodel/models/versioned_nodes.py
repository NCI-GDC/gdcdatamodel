from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgres import ARRAY, JSONB
from sqlalchemy import Column, Text, DateTime, BigInteger, text, Index
from copy import copy


Base = declarative_base()


class VersionedNode(Base):

    __tablename__ = 'versioned_nodes'
    __table_args__ = (
        Index('submitted_node_id_idx', 'node_id'),
        Index('submitted_node_gdc_versions_idx', 'node_id'),
    )

    def __repr__(self):
        return ("<VersionedNode(key={}, label='{}', node_id='{}')>"
                .format(self.key, self.label, self.node_id))

    key = Column(
        BigInteger,
        primary_key=True,
        nullable=False
    )

    label = Column(
        Text,
        nullable=False,
    )

    node_id = Column(
        Text,
        nullable=False,
    )

    project_id = Column(
        Text,
        nullable=False,
    )

    gdc_versions = Column(
        ARRAY(Text),
    )

    created = Column(
        DateTime(timezone=True),
        nullable=False,
    )

    versioned = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text('now()'),
    )

    acl = Column(
        ARRAY(Text),
        default=list(),
    )

    system_annotations = Column(
        JSONB,
        default={},
    )

    properties = Column(
        JSONB,
        default={},
    )

    neighbors = Column(
        ARRAY(Text),
    )

    @staticmethod
    def clone(node):
        return VersionedNode(
            label=copy(node.label),
            node_id=copy(node.node_id),
            project_id=copy(node._props.get('project_id')),
            created=copy(node.created),
            acl=copy(node.acl),
            system_annotations=copy(node.system_annotations),
            properties=copy(node.properties),
            neighbors=copy([
                edge.dst_id for edge in node.edges_out
            ] + [
                edge.src_id for edge in node.edges_in
            ])
        )
