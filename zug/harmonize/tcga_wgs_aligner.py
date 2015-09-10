from sqlalchemy import desc, BigInteger

from gdcdatamodel.models import (
    File, ExperimentalStrategy,
    Platform, Center, DataFormat
)

from zug.harmonize.tcga_bwa_aligner import TCGABWAAligner


class TCGAWGSAligner(TCGABWAAligner):

    @property
    def name(self):
        return "tcga_wgs_aligner"

    @property
    def source(self):
        return "tcga_wgs_alignment"

    def choose_bam_by_forced_id(self):
        input_bam = self.graph.nodes(File).ids(self.config["force_input_id"]).one()
        assert input_bam.sysan["source"] == "tcga_cghub"
        assert input_bam.file_name.endswith(".bam")
        assert input_bam.data_formats[0].name == "BAM"
        assert input_bam.experimental_strategies[0].name == "WGS"
        return input_bam

    @property
    def bam_files(self):
        '''targeted bam files query'''
        wgs = ExperimentalStrategy.name.astext == "WGS"
        illumina_plus_hiseq_x_ten = Platform.name.astext.contains("Illumina") | (Platform.name.astext == "HiSeq X Ten")
        bam = DataFormat.name.astext == "BAM"
        all_file_ids_sq = self.graph.nodes(File.node_id)\
                                    .sysan(source="tcga_cghub")\
                                    .distinct(File._sysan["cghub_legacy_sample_id"].astext)\
                                    .filter(File.experimental_strategies.any(wgs))\
                                    .filter(File.platforms.any(illumina_plus_hiseq_x_ten))\
                                    .filter(File.data_formats.any(bam))\
                                    .order_by(File._sysan["cghub_legacy_sample_id"].astext,
                                              desc(File._sysan["cghub_upload_date"].cast(BigInteger)))\
                                    .subquery()
        return self.graph.nodes(File).filter(File.node_id == all_file_ids_sq.c.node_id)

    @property
    def alignable_files(self):
        '''bam files that are not aligned'''
        currently_being_aligned = self.consul.list_locked_keys()
        alignable = self.bam_files\
                        .props(state="live")\
                        .filter(~File.derived_files.any())\
                        .filter(~File.node_id.in_(currently_being_aligned))
        if self.config["size_limit"]:
            alignable = alignable.filter(
                File.file_size.cast(BigInteger) < self.config["size_limit"]
            )
        if self.config["size_min"]:
            alignable = alignable.filter(
                File.file_size.cast(BigInteger) > self.config["size_min"]
            )
        if self.config["center_limit"]:
            # limit to just HMS for now
            hms = Center.short_name.astext == "HMS"
            alignable = alignable.filter(File.centers.any(hms))
        return alignable
