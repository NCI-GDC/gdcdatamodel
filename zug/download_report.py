import os
import calendar
import time

from sqlalchemy import func
from elasticsearch import Elasticsearch, TransportError

from gdcdatamodel.models import (
    File, Tag, ExperimentalStrategy, Center, Project,
    DataType, DataSubtype, DataFormat, Platform,
    FileMemeberOfTag, FileMemberOfDataSubtype,
    FileMemberOfDataFormat, FileMemberOfExperimentalStrategy,
    FileSubmittedByCenter, FileGeneratedFromPlatform
)

from gdcdatamodel.models.misc import FileReport
from psqlgraph import PsqlGraphDriver
from cdisutils.log import get_logger


def user_access_type(logged_in, open):
    if logged_in and open:
        return "authenticated_open"
    elif logged_in and not open:
        return "authenticated_protected"
    else:
        return "anonymous"


CONTINENTS = {
    'Africa': [
        'AO', 'BF', 'BI', 'BJ', 'BW', 'CD', 'CG', 'CI', 'CM',
        'CV', 'DJ', 'EG', 'ER', 'ET', 'GA', 'GH', 'GM', 'GN', 'GW', 'KE',
        'LR', 'LS', 'LY', 'MG', 'ML', 'MR', 'MU', 'MW', 'MZ', 'NA', 'NE',
        'NG', 'RW', 'SC', 'SD', 'SL', 'SN', 'SO', 'ST', 'TG', 'TN', 'TZ',
        'UG', 'ZM', 'ZW', 'DZ', 'CF', 'TD', 'KM', 'GQ', 'MA', 'ZA', 'SZ'
    ],
    'Asia': [
        'AF', 'AM', 'AZ', 'BD', 'BH', 'BN', 'BT', 'CN', 'CY', 'GE',
        'ID', 'IL', 'IN', 'IQ', 'IR', 'JO', 'JP', 'KG', 'KP', 'KR', 'KW',
        'LB', 'MM', 'MN', 'MV', 'MY', 'NP', 'OM', 'PH', 'PK', 'QA', 'SA',
        'SG', 'SY', 'TH', 'TJ', 'TM', 'TR', 'UZ', 'VN', 'YE', 'KH', 'TL',
        'KZ', 'LA', 'LK', 'AE'
    ],
    'Europe': [
        'AD', 'AL', 'AT', 'BE', 'BG', 'BY', 'CZ', 'DE', 'DK',
        'EE', 'FI', 'FR', 'GR', 'HU', 'IE', 'IS', 'IT', 'LI', 'LT', 'LU',
        'LV', 'MK', 'MT', 'NL', 'NO', 'PL', 'PT', 'RO', 'RU', 'SE', 'SI',
        'SK', 'SM', 'UA', 'VA', 'BA', 'HR', 'MD', 'MC', 'ME', 'RS', 'ES',
        'CH', 'GB'
    ],
    'North America': [
        'AG', 'BB', 'BS', 'BZ', 'CA', 'CR', 'CU', 'DM', 'DO', 'GT', 'GT',
        'HN', 'JM', 'MX', 'NI', 'PA', 'TT', 'US', 'SV', 'GD', 'KN', 'LC',
        'VC'
    ],
    'Oceania': [
        'AU', 'FJ', 'KI', 'MH', 'NR', 'NZ', 'PG', 'PW', 'SB', 'TO', 'TV',
        'VU', 'FM', 'WS'
    ],
    'South America': [
        'AR', 'BO', 'BR', 'CL', 'CO', 'EC', 'GY', 'PE',
        'PY', 'SR', 'UY', 'VE'
    ]
}


class DownloadStatsIndexBuilder(object):
    """
    This class is used to build the download-stats
    index from the FileReport table.
    """

    # TODO include archives in all of this, shouldn't be tooooo hard
    # since the table is pretty small

    MAPPING = {
        '_id': {'path': 'project_id'},
        '_all': {'enabled': False}
    }

    def __init__(self, graph=None, es=None,
                 index_name="download_stats",
                 doc_type="project_stats"):
        if graph:
            self.graph = graph
        else:
            self.graph = PsqlGraphDriver(
                os.environ["PG_HOST"],
                os.environ["PG_USER"],
                os.environ["PG_PASS"],
                os.environ["PG_NAME"],
            )
        if es:
            self.es = es
        else:
            self.es = Elasticsearch([os.environ["ELASTICSEARCH_HOST"]])
        self.index_name = index_name
        self.doc_type = doc_type
        self.log = get_logger("download_stats_index_build")

    def create_es_index(self):
        try:
            self.log.info("Attempting to create %s index", self.index_name)
            self.es.indices.create(self.index_name)
        except TransportError as e:
            if e.status_code == 400 and "IndexAlreadyExistsException" in e.error:
                self.log.info("Index %s appears to already exist")
            else:
                raise e
        self.log.info("Creating mapping for %s doc type", self.doc_type)
        self.es.indices.put_mapping(
            index=self.index_name,
            doc_type=self.doc_type,
            body=self.MAPPING
        )

    def go(self, projects=None):
        self.log.info("Loading all projects from database")
        if not projects:
            projects = self.graph.nodes(Project).all()
        self.log.info("Loaded %s projects", len(projects))
        for project in projects:
            self.log.info("Producing json for %s (%s)", project, project.code)
            body = self.produce_json(project)
            self.log.info("ES indexing %s (%s)", project, project.code)
            self.es.index(
                index=self.index_name,
                doc_type=self.doc_type,
                body=body
            )

    def produce_json(self, project):
        code = project.code
        self.log.info("Producing json for project %s", code)
        program = project.programs[0]
        self.log.info("Computing overall breakdown")
        total_size, total_count = self.overall_breakdown(code)
        self.log.info("Computing data subtype breakdown")

        subtype_breakdown = self.data_subtype_breakdown(code)
        self.log.info("Computing data type breakdown")
        type_breakdown = self.data_type_breakdown(subtype_breakdown)

        self.log.info("Computing experimental strategy breakdown")
        strategy_breakdown = self.experimental_strategy_breakdown(code)

        self.log.info("Computing format breakdown")
        format_breakdown = self.data_format_breakdown(code)

        self.log.info("Computing access breakdown")
        access_breakdown = self.data_access_breakdown(code)

        self.log.info("Computing tag breakdown")
        tag_breakdown = self.tag_breakdown(code)

        self.log.info("Computing platform breakdown")
        platform_breakdown = self.platform_breakdown(code)

        self.log.info("Computing center breakdown")
        center_breakdown = self.center_breakdown(code)

        self.log.info("Computing user access breakdown")
        user_access_breakdown = self.user_access_type_breakdown(code)

        self.log.info("Computing country access breakdown")
        country_breakdown = self.country_breakdown(code)
        self.log.info("Computing continent breakdown")
        continent_breakdown = self.continent_breakdown(country_breakdown)

        return {
            "timestamp": calendar.timegm(time.gmtime()),
            "project_id": "{}-{}".format(program.name, project.code),
            "program": program.name,
            "primary_site": project.primary_site,
            "disease_type": project.disease_type,
            "size": total_size,
            "count": total_count,
            "data_types": type_breakdown,
            "data_subtypes": subtype_breakdown,
            "experimental_strategies": strategy_breakdown,
            "data_formats": format_breakdown,
            "data_access": access_breakdown,
            "tags": tag_breakdown,
            "platforms": platform_breakdown,
            "centers": center_breakdown,
            "user_access_types": user_access_breakdown,
            "countries": country_breakdown,
            "continents": continent_breakdown,
        }

    def file_ids_in_project(self, code):
        """Return a query that will find all files in a project, works by
        unioning the various paths by which that can happen.
        """
        tcga_path = self.graph.nodes(File.node_id)\
                              .path("aliquots.analytes.portions.samples.participants.projects").props(code=code)
        shipped_portion_path = self.graph.nodes(File.node_id)\
                                         .path("portions.samples.participants.projects").props(code=code)
        target_path = self.graph.nodes(File.node_id)\
                                .path("aliquots.samples.participants.projects").props(code=code)
        direct_participant_path = self.graph.nodes(File.node_id)\
                                            .path("participants.projects").props(code=code)
        # TODO others?
        return tcga_path.union(shipped_portion_path,
                               target_path,
                               direct_participant_path)

    def overall_breakdown(self, code):
        size, count = self.graph.nodes((func.sum(FileReport.streamed_bytes), func.count("*")))\
                                .filter(FileReport.node_id.in_(self.file_ids_in_project(code)))\
                                .one()
        if not count:
            return 0, 0
        else:
            return int(size), count

    def data_subtype_breakdown(self, code):
        q = self.graph.nodes((func.sum(FileReport.streamed_bytes), func.count("*"), DataSubtype.name))\
                      .join(FileMemberOfDataSubtype, FileMemberOfDataSubtype.src_id == FileReport.node_id)\
                      .join(DataSubtype, FileMemberOfDataSubtype.dst_id == DataSubtype.node_id)\
                      .filter(FileReport.node_id.in_(self.file_ids_in_project(code)))\
                      .group_by(DataSubtype.name)
        return [{"data_subtype": row[2], "size": int(row[0]), "count": row[1]}
                for row in q.all()]

    def data_type_breakdown(self, subtype_breakdown):
        data_types = self.graph.nodes(DataType).all()
        res = []
        for data_type in data_types:
            subtype_names = [subtype.name for subtype in data_type.data_subtypes]
            size = sum([desc["size"] for desc in subtype_breakdown
                        if desc["data_subtype"] in subtype_names])
            count = sum([desc["count"] for desc in subtype_breakdown
                         if desc["data_subtype"] in subtype_names])
            if count:
                res.append({"data_type": data_type.name,
                            "count": count, "size": size})
        return res

    def experimental_strategy_breakdown(self, code):
        q = self.graph.nodes((func.sum(FileReport.streamed_bytes), func.count("*"), ExperimentalStrategy.name))\
                      .join(FileMemberOfExperimentalStrategy, FileMemberOfExperimentalStrategy.src_id == FileReport.node_id)\
                      .join(ExperimentalStrategy, FileMemberOfExperimentalStrategy.dst_id == ExperimentalStrategy.node_id)\
                      .filter(FileReport.node_id.in_(self.file_ids_in_project(code)))\
                      .group_by(ExperimentalStrategy.name)
        return [{"experimental_strategy": row[2], "size": int(row[0]), "count": row[1]}
                for row in q.all()]

    def data_format_breakdown(self, code):
        q = self.graph.nodes((func.sum(FileReport.streamed_bytes), func.count("*"), DataFormat.name))\
                      .join(FileMemberOfDataFormat, FileMemberOfDataFormat.src_id == FileReport.node_id)\
                      .join(DataFormat, FileMemberOfDataFormat.dst_id == DataFormat.node_id)\
                      .filter(FileReport.node_id.in_(self.file_ids_in_project(code)))\
                      .group_by(DataFormat.name)
        return [{"data_format": row[2], "size": int(row[0]), "count": row[1]}
                for row in q.all()]

    def tag_breakdown(self, code):
        q = self.graph.nodes((func.sum(FileReport.streamed_bytes), func.count("*"), Tag.name))\
                      .join(FileMemeberOfTag, FileMemeberOfTag.src_id == FileReport.node_id)\
                      .join(Tag, FileMemeberOfTag.dst_id == Tag.node_id)\
                      .filter(FileReport.node_id.in_(self.file_ids_in_project(code)))\
                      .group_by(Tag.name)
        return [{"tag": row[2], "size": int(row[0]), "count": row[1]}
                for row in q.all()]

    def platform_breakdown(self, code):
        q = self.graph.nodes((func.sum(FileReport.streamed_bytes), func.count("*"), Platform.name))\
                      .join(FileGeneratedFromPlatform, FileGeneratedFromPlatform.src_id == FileReport.node_id)\
                      .join(Platform, FileGeneratedFromPlatform.dst_id == Platform.node_id)\
                      .filter(FileReport.node_id.in_(self.file_ids_in_project(code)))\
                      .group_by(Platform.name)
        return [{"platform": row[2], "size": int(row[0]), "count": row[1]}
                for row in q.all()]

    def center_breakdown(self, code):
        q = self.graph.nodes((func.sum(FileReport.streamed_bytes), func.count("*"), Center.code))\
                      .join(FileSubmittedByCenter, FileSubmittedByCenter.src_id == FileReport.node_id)\
                      .join(Center, FileSubmittedByCenter.dst_id == Center.node_id)\
                      .filter(FileReport.node_id.in_(self.file_ids_in_project(code)))\
                      .group_by(Center.code)
        return [{"center": row[2], "size": int(row[0]), "count": row[1]}
                for row in q.all()]

    def user_access_type_breakdown(self, code):
        open_data = File.acl == ["open"]
        # empty string is a sentinel for not logged in, probably this
        # should really be NULL
        logged_in_user = FileReport.username != ''
        q = self.graph.nodes((func.sum(FileReport.streamed_bytes), func.count("*"), open_data, logged_in_user))\
                      .join(File, FileReport.node_id == File.node_id)\
                      .filter(FileReport.node_id.in_(self.file_ids_in_project(code)))\
                      .group_by(open_data, logged_in_user)
        return [{"user_access_type": user_access_type(row[3], row[2]), "size": int(row[0]), "count": row[1]}
                for row in q.all()]

    def data_access_breakdown(self, code):
        open_data = File.acl == ["open"]
        q = self.graph.nodes((func.sum(FileReport.streamed_bytes), func.count("*"), open_data))\
                      .join(File, FileReport.node_id == File.node_id)\
                      .filter(FileReport.node_id.in_(self.file_ids_in_project(code).subquery()))\
                      .group_by(open_data)
        return [{"access": "open" if row[2] else "protected", "size": int(row[0]), "count": row[1]}
                for row in q.all()]

    def country_breakdown(self, code):
        q = self.graph.nodes((func.sum(FileReport.streamed_bytes), func.count("*"), FileReport.country_code))\
                      .join(File, FileReport.node_id == File.node_id)\
                      .filter(FileReport.node_id.in_(self.file_ids_in_project(code).subquery()))\
                      .group_by(FileReport.country_code)
        return [{"country": row[2] if row[2] else "unknown", "size": int(row[0]), "count": row[1]}
                for row in q.all()]

    def continent_breakdown(self, country_breakdown):
        res = []
        for continent, countries in CONTINENTS.iteritems():
            size = sum([desc["size"] for desc in country_breakdown
                        if desc["country"] in countries])
            count = sum([desc["count"] for desc in country_breakdown
                         if desc["country"] in countries])
            if count:
                res.append({"continent": continent,
                            "count": count, "size": size})
        return res

    # TODO
    #
    # is_data_file, probably the best way to do this is to look at the
    # related_to edge
    #
    # protocol, requires adding an X-GDC-Orig-Protocol header to UDT
    # requests made by parcel, more code in the API, as well as
    # another column to the table
    #
    # client type, requires giving parcel it's own user agent,
    # detecting this in the API, and adding another column to the
    # report table
    #
    # also need to respect X-Forwarded-For when setting client IP,
    # just requires an API change
