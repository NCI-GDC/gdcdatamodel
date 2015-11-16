from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Text, DateTime, text, Integer, Index
from sqlalchemy.dialects.postgres import JSONB

Base = declarative_base()


class GDCReport(Base):
    __tablename__ = 'gdc_reports'
    __table_args__ = (
        Index('{}_report_idx'.format(__tablename__),
              'report', postgresql_using='gin'),
        Index('{}_report_type_idx'.format(__tablename__),
              'report_type'),
        Index('{}_created_datetime_idx'.format(__tablename__),
              'created_datetime'),
        Index('{}_id_idx'.format(__tablename__),
              'id'),
    )

    def __repr__(self):
        return "<Report({}, {})>".format(self.id, self.report_type)

    id = Column(
        Integer,
        primary_key=True,
    )

    report_type = Column(
        Text,
        nullable=False,
    )

    created_datetime = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text('now()'),
    )

    report = Column(JSONB)
