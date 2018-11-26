from sqlalchemy import Column, DateTime, ForeignKey, Integer, Text, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

Base = declarative_base()


class RedactionLog(Base):

    __tablename__ = 'redaction_log'

    id = Column(
        Integer,
        primary_key=True,
        nullable=False
    )

    initiated_by = Column(Text, nullable=False)

    reason = Column(
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

    created_datetime = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text('now()'),
    )

    @hybrid_property
    def project_id(self):
        return self.program + '-' + self.project


class RedactionEntry(Base):

    node_id = Column(Text, nullable=False, primary_key=True)
    version = Column(Text)
    file_name = Column(Text)
    release_number = Column(Text, nullable=False, primary_key=True)

    redaction_id = Column(Integer, ForeignKey("redaction_log.id"), nullable=False)
    redaction_log = relationship("RedactionLog", backref="entries")
