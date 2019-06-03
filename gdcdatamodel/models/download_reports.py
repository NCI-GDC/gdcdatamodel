import json

from sqlalchemy import (
    Column, Date, DateTime, Float, Integer, String, text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


DEFAULT_USAGE_REPORT = dict(
    visits=0,
    visitors=0,
    requests=0,
    network_usage=0
)


class DataUsageReport(Base):

    __tablename__ = "data_usage_report"

    report_period = Column(
        Date, primary_key=True, nullable=False)  # MM/YYYY 01/31/2019

    api_report = Column(
        JSONB, nullable=False, default=DEFAULT_USAGE_REPORT)

    portal_report = Column(
        JSONB, nullable=False, default=DEFAULT_USAGE_REPORT)

    website_report = Column(
        JSONB, nullable=False, default=DEFAULT_USAGE_REPORT)

    doc_site_report = Column(
        JSONB, nullable=False, default=DEFAULT_USAGE_REPORT)

    date_created = Column(
        DateTime(timezone=True), nullable=False, server_default=text('now()'))
    last_updated = Column(
        DateTime(timezone=True), nullable=False, server_default=text('now()'))

    def set_api_report(self, visits, visitors, requests, network_usage):
        self.api_report = dict(
            visits=visits,
            visitors=visitors,
            requests=requests,
            network_usage=network_usage
        )

    def set_portal_report(self, visits, visitors, requests, network_usage):
        self.portal_report = dict(
            visits=visits,
            visitors=visitors,
            requests=requests,
            network_usage=network_usage
        )

    def set_website_report(self, visits, visitors, requests, network_usage):
        self.website_report = dict(
            visits=visits,
            visitors=visitors,
            requests=requests,
            network_usage=network_usage
        )

    def set_doc_site_report(self, visits, visitors, requests, network_usage):
        self.doc_site_report = dict(
            visits=visits,
            visitors=visitors,
            requests=requests,
            network_usage=network_usage
        )

    def to_json(self):
        """Returns a JSON safe representation of :class:`DataUsageReport`"""

        return json.loads(json.dumps({
            'report_period': str(self.report_period),
            'api_report': self.api_report,
            'portal_report': self.portal_report,
            'website_report': self.website_report,
            'doc_site_report': self.doc_site_report,
            'date_created': str(self.date_created),
            'last_updated': str(self.last_updated)
        }))


class DataDownloadReport(Base):

    __tablename__ = "data_download_report"

    report_period = Column(Date, primary_key=True, nullable=False)

    project_id_report = Column(JSONB, nullable=False, server_default='{}')

    experimental_strategy_report = Column(
        JSONB, nullable=False, server_default='{}')

    access_type_report = Column(JSONB, nullable=False, server_default='{}')

    access_location_report = Column(JSONB, nullable=False, server_default='{}')

    date_created = Column(
        DateTime(timezone=True), nullable=False, server_default=text('now()'))
    last_updated = Column(
        DateTime(timezone=True), nullable=False, server_default=text('now()'))

    def add_access_type(self, access_type, size):
        """
        Args:
            access_type (str): open/closed
            size (double): size in GB
        """
        if not self.access_type_report:
            self.access_type_report = {}
        self.access_type_report[access_type] = size

    def add_experimental_strategy(self, strategy, size):
        """
        Args:
            strategy (str): strategy name
            size (double): size in GB
        """
        if not self.experimental_strategy_report:
            self.experimental_strategy_report = {}
        self.experimental_strategy_report[strategy] = size

    def add_project_id(self, project, size):
        """
        Args:
            project id (str): project's name
            size (double): size in GB
        """
        if not self.project_id_report:
            self.project_id_report = {}
        self.project_id_report[project] = size

    def add_access_location(self, location, size):
        """
        Args:
            location (str): location name (country code)
            size (double): size in GB
        """
        if not self.access_location_report:
            self.access_location_report = {}
        self.access_location_report[location] = size

    def to_json(self):
        """Returns a JSON safe representation of :class:`DataDownloadReport`"""

        return json.loads(json.dumps({
            'report_period': str(self.report_period),
            'project_id_report': self.project_id_report,
            'experimental_strategy_report': self.experimental_strategy_report,
            'access_type_report': self.access_type_report,
            'access_location_report': self.access_location_report,
            'date_created': str(self.date_created),
            'last_updated': str(self.last_updated)
        }))


class MonthlyAwstats(Base):
    __tablename__ = 'monthly_awstats'
    report_date = Column('report_date', Date, primary_key=True)
    site = Column('site', String(length=50), primary_key=True)
    # TODO: many of these are currently Integers, should they be BigInts?
    unique_visitors = Column('unique_visitors', Integer)
    number_of_visits = Column('number_of_visits', Integer)
    viewed_pages = Column('viewed_pages', Integer)
    viewed_hits = Column('viewed_hits', Integer)
    viewed_bw_gb = Column('viewed_bw_gb', Float)
    unviewed_pages = Column('unviewed_pages', Integer)
    unviewed_hits = Column('unviewed_hits', Integer)
    unviewed_bw_gb = Column('unviewed_bw_gb', Float)
    observium_bw_in_gb = Column('observium_bw_in_gb', Float)
    observium_bw_out_gb = Column('observium_bw_out_gb', Float)
