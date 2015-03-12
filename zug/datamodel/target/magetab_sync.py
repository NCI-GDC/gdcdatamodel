import os
from cStringIO import StringIO
import requests
import pandas as pd
from collections import defaultdict

from sqlalchemy.orm.exc import NoResultFound
from psqlgraph import PsqlEdge
from psqlgraph.validate import AvroNodeValidator, AvroEdgeValidator
from gdcdatamodel import node_avsc_object, edge_avsc_object

from cdisutils.log import get_logger

from zug.datamodel.target.dcc_sync import tree_walk
from zug.datamodel.tcga_magetab_sync import group_by_protocol, cleanup_list, cleanup_row, FILE_COL_NAMES


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


class TARGETMAGETABSyncer(object):

    def __init__(self, url, graph=None, dcc_auth=None):
        assert url.startswith("https://target-data.nci.nih.gov")
        self.url = url
        self.project = url.split("/")[3]
        self.dcc_auth = dcc_auth
        self.graph = graph
        if self.graph:
            self.graph.node_validator = AvroNodeValidator(node_avsc_object)
            self.graph.edge_validator = AvroEdgeValidator(edge_avsc_object)
        self.log = get_logger("target_magetab_sync_{}_{}".format(os.getpid(), self.project))

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

    def compute_mapping(self):
        self.log.info("computing mapping")
        mapping = defaultdict(lambda: set())
        for link in self.magetab_links():
            self.log.info("processing %s", link)
            df = self.df_for_link(link)
            for _, row in df.iterrows():
                aliquot = self.aliquot_for(row)
                groups = group_by_protocol(df)
                file_groups = [group for group in groups if is_file_group(group)]
                for group in file_groups:
                    subrow = row[group]
                    cleanup_row(subrow)
                    filename = get_file(subrow)
                    if not pd.notnull(filename):
                        continue
                    else:
                        mapping[filename].add(aliquot)
        return mapping

    def insert_mapping_in_graph(self, mapping):
        with self.graph.session_scope():
            for file_name, aliquot_barcodes in mapping.iteritems():
                try:
                    # note that for now this assumes filenames are
                    # unique (this is the case in WT), it will blow up
                    # with MultipleResultsFound exception and fail to
                    # insert anything if this is not the case
                    # presumably this will need to be revisited in the future
                    file = self.graph.nodes().labels("file")\
                                             .sysan({"source": "target_dcc"})\
                                             .props({"file_name": file_name}).one()
                    self.log.info("found file %s as %s", file_name, file)
                except NoResultFound:
                    self.log.warning("file %s not found in graph", file_name)
                    continue
                for barcode in aliquot_barcodes:
                    self.log.info("attempting to tie file %s to aliquot %s", file_name, barcode)
                    try:
                        aliquot = self.graph.nodes().labels("aliquot")\
                                                    .sysan({"source": "target_sample_matrices"})\
                                                    .props({"submitter_id": barcode}).one()
                        self.log.info("found aliquot %s", barcode)
                    except NoResultFound:
                        self.log.warning("aliquot %s not found in graph", barcode)
                        continue
                    self.tie_file_to_aliquot(file, aliquot)

    def tie_file_to_aliquot(self, file, aliquot):
        maybe_edge_to_aliquot = self.graph.edges().labels("data_from")\
                                                  .src(file).dst(aliquot).scalar()
        if not maybe_edge_to_aliquot:
            edge_to_aliquot = PsqlEdge(
                label="data_from",
                src_id=file.node_id,
                dst_id=aliquot.node_id
            )
            self.graph.edge_insert(edge_to_aliquot)
