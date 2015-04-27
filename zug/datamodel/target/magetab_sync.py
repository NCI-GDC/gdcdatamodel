import os
from cStringIO import StringIO
import requests
import pandas as pd
from collections import defaultdict
from datetime import datetime

from sqlalchemy.orm.exc import NoResultFound
from gdcdatamodel import models as md

from cdisutils.log import get_logger

from zug.datamodel.target.dcc_sync import tree_walk
from zug.datamodel.tcga_magetab_sync import group_by_protocol, cleanup_list,\
    cleanup_row, FILE_COL_NAMES


def is_file_group(group):
    group = cleanup_list(group)
    if "Derived Data File REF" in group:
        return True
    if "Comment [Derived Data File REF]" in group:
        return True
    for name in FILE_COL_NAMES:
        if name in group:
            return True
    return False


def get_file(subrow):
    ret = None
    for name in FILE_COL_NAMES:
        if subrow.get(name):
            if not ret:
                ret = subrow[name]
            else:
                raise RuntimeError("subrow %s has more than one file column", subrow)
    return ret


def get_name_and_version(sdrf):
    parts = sdrf.replace(".sdrf.txt", "").split("_")
    date = parts.pop()
    return "_".join(parts), datetime.strptime(date, "%Y%m%d").toordinal()


class TARGETMAGETABSyncer(object):

    def __init__(self, project, graph=None, dcc_auth=None):
        self.project = project
        self.url = "https://target-data.nci.nih.gov/{}/".format(project)
        assert self.url.startswith("https://target-data.nci.nih.gov")
        self.dcc_auth = dcc_auth
        self.graph = graph
        self.log = get_logger("target_magetab_sync_{}_{}".format(
            os.getpid(), self.project))

    def magetab_links(self):
        for link in tree_walk(self.url, auth=self.dcc_auth):
            if link.endswith(".sdrf.txt"):
                yield link

    def df_for_link(self, link):
        resp = requests.get(link, auth=self.dcc_auth)
        return pd.read_table(StringIO(resp.content))

    def aliquot_for(self, row):
        """Target magetabs appear fairly well organized, each seems to have a
        'Source Name' column, which has the participant barcode, a
        'Sample Name' column, which has the sample barcode, and an
        'Extract Name' column, which has the aliquot barcode. This
        method parses that information out.
        """
        participant = row["Source Name"]
        sample = row["Sample Name"]
        aliquot = row["Extract Name"]
        assert aliquot.startswith("TARGET-")
        assert aliquot.startswith(sample)
        assert aliquot.startswith(participant)
        return aliquot

    def compute_mappings(self):
        """Returns a dict, the keys of which are links to sdrf files that
        produced a mapping, the values ("mappings") are dicts from
        filename to aliquot barcode
        """
        self.log.info("computing mapping")
        ret = {}
        for link in self.magetab_links():
            mapping = defaultdict(lambda: set())
            self.log.info("processing %s", link)
            df = self.df_for_link(link)
            for _, row in df.iterrows():
                aliquot = self.aliquot_for(row)
                groups = group_by_protocol(df)
                file_groups = [group for group in groups
                               if is_file_group(group)]
                for group in file_groups:
                    subrow = row[group]
                    cleanup_row(subrow)
                    filename = get_file(subrow)
                    if not pd.notnull(filename):
                        continue
                    else:
                        mapping[filename].add(aliquot)
            ret[link] = mapping
        return ret

    def sync(self):
        mappings = self.compute_mappings()
        self.insert_mappings_in_graph(mappings)

    def insert_mappings_in_graph(self, mappings):
        with self.graph.session_scope():
            for link, mapping in mappings.iteritems():
                self.log.info("inserting mapping for %s", link)
                self.insert_mapping_in_graph(link, mapping)

    def insert_mapping_in_graph(self, link, mapping):
        sdrf_name = link.split("/")[-1]
        sdrf = self.graph.nodes().labels("file")\
                                 .sysan({"source": "target_dcc"})\
                                 .props({"file_name": sdrf_name}).scalar()
        if not sdrf:
            self.log.warning("sdrf %s not found", sdrf_name)
            return
        for file_name, aliquot_barcodes in mapping.iteritems():
            files = self.graph.nodes().labels("file")\
                                      .sysan({"source": "target_dcc"})\
                                      .props({"file_name": file_name}).all()
            if len(files) == 0:
                self.log.warning("file %s not found", file_name)
                continue
            elif len(files) == 1:
                file = files[0]
                self.log.info("found file %s as %s", file_name, file)
            elif len(files) > 1:
                self.log.info("multiple files with name %s found, attempting to disambiguate based on url", file_name)
                # a file that has the url
                # https://target-data.nci.nih.gov/WT/Discovery/miRNA-seq/L3/TARGET-50-CAAAAO-01A-01R.isoform.quantification.txt
                # will have a MAGETAB at
                # https://target-data.nci.nih.gov/WT/Discovery/miRNA-seq/METADATA/MAGE-TAB_TARGET_WT_miRNA-Seq_Illumina_20141223.sdrf.txt
                #
                # a file that has the url
                # https://target-data.nci.nih.gov/WT/Discovery/mRNA-seq/L3/expression/TARGET-50-CAAAAO-01A-01R.isoform.quantification.txt
                # will have a MAGETAB at the url
                # https://target-data.nci.nih.gov/WT/Discovery/mRNA-seq/METADATA/MAGE-TAB_TARGET_WT_RNA-seq_Illumina_20150305.sdrf.txt
                # here we extract that part of the url that should match and then filter based on it
                part_of_url_that_should_match = link.split("METADATA")[0]
                filtered_files = [file for file in files
                                  if part_of_url_that_should_match
                                  in file.system_annotations["url"]]
                assert len(filtered_files) == 1
                file = filtered_files[0]
            self.tie_file_to_sdrf(file, sdrf)
            for barcode in aliquot_barcodes:
                self.log.info("attempting to tie file %s to aliquot %s",
                              file_name, barcode)
                try:
                    aliquot = self.graph.nodes(md.Aliquot)\
                                        .labels("aliquot")\
                                        .sysan({"source": "target_sample_matrices"})\
                                        .props({"submitter_id": barcode})\
                                        .one()
                    self.log.info("found aliquot %s", barcode)
                except NoResultFound:
                    self.log.warning("aliquot %s not found in graph", barcode)
                    continue
                self.tie_file_to_aliquot(file, aliquot, sdrf)
            # TODO add code to delete edges from old versions of this
            # sdrf when we insert a new one

    def tie_file_to_aliquot(self, file, aliquot, sdrf):
        maybe_edge_to_aliquot = self.graph.edges().labels("data_from")\
                                                  .src(file.node_id)\
                                                  .dst(aliquot.node_id)\
                                                  .scalar()
        sdrf_name, sdrf_version = get_name_and_version(sdrf["file_name"])
        if not maybe_edge_to_aliquot:
            edge_to_aliquot = md.FileDataFromAliquot(
                src_id=file.node_id,
                dst_id=aliquot.node_id,
                system_annotations={
                    "source": "target_magetab",
                    "sdrf_name": sdrf_name,
                    "sdrf_version": sdrf_version,
                }
            )
            with self.graph.session_scope() as s:
                s.merge(edge_to_aliquot)

    def tie_file_to_sdrf(self, file, sdrf):
        maybe_edge = self.graph.edges().labels("related_to")\
                                       .src(sdrf.node_id)\
                                       .dst(file.node_id)\
                                       .scalar()
        if not maybe_edge:
            sdrf_name, sdrf_version = get_name_and_version(sdrf["file_name"])
            edge = md.FileRelatedToFile(
                label="related_to",
                src_id=sdrf.node_id,
                dst_id=file.node_id,
                system_annotations={
                    "source": "target_magetab",
                    "sdrf_name": sdrf_name,
                    "sdrf_version": sdrf_version,
                }
            )
            with self.graph.session_scope() as s:
                s.merge(edge)
