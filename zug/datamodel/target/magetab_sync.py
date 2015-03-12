import os
from cStringIO import StringIO
import requests
import pandas as pd
from collections import defaultdict

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
