from datetime import date

from gdcdatamodel.models.download_reports import *


def test_create_usage_reports(pg_driver_reports):

    with pg_driver_reports.session_scope() as sxn:

        report = DataUsageReport()
        report.set_api_report(visits=10, visitors=1, requests=100, network_usage=100.0)
        report.set_portal_report(visits=10, visitors=3, requests=100, network_usage=120.0)
        report.set_doc_site_report(visits=10, visitors=4, requests=100, network_usage=10.0)
        report.set_website_report(visits=10, visitors=1, requests=100, network_usage=100.0)
        report.report_period = date.today()

        sxn.add(report)

    # check if persisted
    with pg_driver_reports.session_scope() as _:
        rp = pg_driver_reports\
            .nodes(DataUsageReport).get(report.report_period)  # type: DataUsageReport

        assert rp.api_report == report.api_report
        assert rp.report_period == report.report_period
        assert rp.portal_report == report.portal_report


def test_create_download_report(pg_driver_reports):

    with pg_driver_reports.session_scope() as sxn:
        report = DataDownloadReport()

        report.add_access_type("open", 100.0)
        report.add_access_type("closed", 33.0)
        report.add_experimental_strategy("WXS", 100.0)
        report.add_access_location("San Francisco, CA, USA", 300)
        report.add_project_id("TCGA-YYY", 330)
        report.report_period = date.today()

        sxn.add(report)

    # check if persisted
    with pg_driver_reports.session_scope() as _:
        rp = pg_driver_reports\
            .nodes(DataDownloadReport).get(report.report_period)  # type: DataDownloadReport

        assert rp.access_location_report == report.access_location_report
        assert rp.report_period == report.report_period
        assert rp.project_id_report == report.project_id_report
