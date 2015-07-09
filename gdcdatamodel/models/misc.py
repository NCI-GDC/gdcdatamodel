from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, Text, DateTime, BigInteger


Base = declarative_base()


class FileReport(Base):
    __tablename__ = 'filereport'
    id = Column('id', Integer, primary_key=True)
    node_id = Column('node_id', Text, index=True)
    ip = Column('ip', String)
    country_code = Column('country_code', String, index=True)
    timestamp = Column('timestamp', DateTime, server_default="now()")
    streamed_bytes = Column('streamed_bytes', BigInteger)
    username = Column('username', String, index=True)
    requested_bytes = Column('requested_bytes', BigInteger)
