from gdcdatamodel.models import (
    File, ExperimentalStrategy,
    Platform, DataFormat,
)
from sqlalchemy import desc, func, Boolean, BigInteger


SORT_ORDER = (
    File._sysan["alignment_seen_docker_error"].cast(Boolean).nullsfirst(),
    func.random()
)


def exome(graph, source):
    wxs = ExperimentalStrategy.name.astext == "WXS"
    illumina = Platform.name.astext.contains("Illumina")
    bam = DataFormat.name.astext == "BAM"
    all_file_ids_sq = graph.nodes(File.node_id)\
                           .sysan(source=source)\
                           .distinct(File._sysan["cghub_legacy_sample_id"].astext)\
                           .filter(File.experimental_strategies.any(wxs))\
                           .filter(File.platforms.any(illumina))\
                           .filter(File.data_formats.any(bam))\
                           .order_by(File._sysan["cghub_legacy_sample_id"].astext,
                                     desc(File._sysan["cghub_upload_date"].cast(BigInteger)))\
                           .subquery()
    return graph.nodes(File).filter(File.node_id == all_file_ids_sq.c.node_id)


def wgs(graph, source):
    wgs = ExperimentalStrategy.name.astext == "WGS"
    illumina_plus_hiseq_x_ten = Platform.name.astext.contains("Illumina") | (Platform.name.astext == "HiSeq X Ten")
    bam = DataFormat.name.astext == "BAM"
    all_file_ids_sq = graph.nodes(File.node_id)\
                           .sysan(source=source)\
                           .distinct(File._sysan["cghub_legacy_sample_id"].astext)\
                           .filter(File.experimental_strategies.any(wgs))\
                           .filter(File.platforms.any(illumina_plus_hiseq_x_ten))\
                           .filter(File.data_formats.any(bam))\
                           .order_by(File._sysan["cghub_legacy_sample_id"].astext,
                                     desc(File._sysan["cghub_upload_date"].cast(BigInteger)))\
                           .subquery()
    return graph.nodes(File).filter(File.node_id == all_file_ids_sq.c.node_id)


def mirnaseq(graph, source):
    strategy = ExperimentalStrategy.name.astext == 'miRNA-Seq'
    platform = Platform.name.astext.contains('Illumina')
    dataformat = DataFormat.name.astext == 'BAM'

    subquery = graph.nodes(File.node_id)\
        .sysan(source=source)\
        .distinct(File._sysan['cghub_legacy_sample_id'].astext)\
        .filter(File.experimental_strategies.any(strategy))\
        .filter(File.platforms.any(platform))\
        .filter(File.data_formats.any(dataformat))\
        .order_by(
            File._sysan['cghub_legacy_sample_id'].astext,
            desc(File._sysan['cghub_upload_date'].cast(BigInteger)),
        )\
        .subquery()
    return graph.nodes(File).filter(File.node_id == subquery.c.node_id)


def rnaseq(graph, source):
    strategy = ExperimentalStrategy.name.astext == 'RNA-Seq'
    platform = Platform.name.astext.contains('Illumina')
    dataformat = DataFormat.name.astext.in_(['TAR', 'TARGZ'])

    subquery = graph.nodes(File.node_id)\
        .sysan(source=source)\
        .distinct(File._sysan['cghub_legacy_sample_id'].astext)\
        .filter(File.experimental_strategies.any(strategy))\
        .filter(File.platforms.any(platform))\
        .filter(File.data_formats.any(dataformat))\
        .order_by(
            File._sysan['cghub_legacy_sample_id'].astext,
            desc(File._sysan['cghub_upload_date'].cast(BigInteger)),
        )\
        .subquery()

    return graph.nodes(File).filter(File.node_id == subquery.c.node_id)
