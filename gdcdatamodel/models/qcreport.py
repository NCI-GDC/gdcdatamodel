from sqlalchemy import Boolean, Integer, Column, DateTime, String, Text, Enum, ForeignKey, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, validates


Base = declarative_base()
SEVERITY = Enum("CRITICAL", "WARNING", "PASSED", name="error_severity")
TEST_RUN_STATUS = Enum("PENDING", "RUNNING", "SUCCESS", "ERROR", "FAILED", name="test_run_status")


class TestRun(Base):

    __tablename__ = 'qc_test_runs'

    id = Column(Integer, primary_key=True)
    project_id = Column(String(64), nullable=False, index=True)

    entity_id = Column(String(64), nullable=False)
    test_type = Column(String(64), nullable=False, index=True)
    is_stale = Column(Boolean(64), nullable=False, default=False)

    # e.g. pending/running/finished
    status = Column(TEST_RUN_STATUS, default="PENDING", nullable=False, index=True)

    test_results = relationship('ValidationResult',
                                back_populates='test_run',
                                cascade='all, delete, delete-orphan')

    date_created = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text('now()'),
    )

    last_updated = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text('now()'),
    )

    def __repr__(self):
        return "<TestRun(id='%d', test_type='%s', status='%s')>" % (
            self.id, self.test_type, self.status
        )

    def to_json(self):
        return {
            'id': self.id,
            'project_id': self.project_id,
            'entity_id': self.entity_id,
            'test_type': self.test_type,
            'status': self.status,
            'date_created': self.date_created.isoformat(),
            'last_updated': self.last_updated.isoformat()
        }


class ValidationResult(Base):

    __tablename__ = 'qc_validation_results'

    id = Column(Integer, primary_key=True)

    node_id = Column(String(64), nullable=False)
    submitter_id = Column(String(128))

    error_type = Column(String(128), nullable=True, default='', index=True)

    # from Node.label
    node_type = Column(String(128), nullable=False, index=True)
    message = Column(Text, nullable=False)

    severity = Column(SEVERITY, nullable=True, index=True)

    related_nodes = Column(JSONB, nullable=True)

    test_run_id = Column(Integer, ForeignKey("qc_test_runs.id"), nullable=False, primary_key=True)
    test_run = relationship('TestRun', back_populates='test_results')

    date_created = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text('now()'),
    )

    last_updated = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text('now()'),
    )

    @validates("severity")
    def validate_severity(self, key, severity):

        if not severity:
            return severity

        # ensure all upper cases
        severity = severity.upper()

        # map fatal to failed
        if severity == "FATAL":
            severity = "CRITICAL"

        return severity

    def __repr__(self):
        return "<ValidationResult(id=%d, error='%s')>" % (
            self.id, self.error_type
        )

    def to_json(self):
        return {
            'node_id': self.node_id,
            'submitter_id': self.submitter_id,
            'error': self.error_type,
            'severity': self.severity,
            'message': self.message,
            'related_nodes': self.related_nodes,
            'date_created': self.date_created.isoformat(),
            'last_updated': self.last_updated.isoformat()
        }
