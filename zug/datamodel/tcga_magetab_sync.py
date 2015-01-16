import pandas as pd
from lxml import html
import requests
import os
import re

from cdisutils.log import get_logger


def sdrf_from_folder(url):
    tree = html.fromstring(requests.get(url).content)
    links = tree.cssselect("a")
    for link in links:
        if "sdrf" in link.attrib["href"]:
            return os.path.join(url, link.attrib["href"])


def is_reference(s):
    REF_NAMES = [
        "Promega ref DNA", "Stratagene Univeral Reference",
        "Human male genomic",
        "Stratagene_Cell_Line_Hum_Ref_RNA_Extract",
        "BD_Human_Tissue_Ref_RNA_Extract",
        "BioChain_RtHanded_Total_Brain_RNA_Extract",
        "BioChain_RtHanded_Total_Ovary_RNA_Extract"
    ]
    return s in REF_NAMES or "Control" in s


def is_uuid(s):
    uuid_re = re.compile("^[0-9A-F]{8}-[0-9A-F]{4}-4[0-9A-F]{3}-[89AB][0-9A-F]{3}-[0-9A-F]{12}$",re.IGNORECASE)
    return bool(re.match(uuid_re, s))


def is_barcode(s):
    any_number_of_dashes_caps_numbers_re = "[\-A-Z0-9]*"
    return s.startswith("TCGA-") and re.match(any_number_of_dashes_caps_numbers_re, s)


def is_empty(s):
    return s == "->"


def strip_dot_trailer(s):
    return re.sub("\..*$", "", s)


def cleanup(subrow):
    subrow.index = [re.sub("\.[0-9]*$", "", name) for name in subrow.index]

FILE_COL_NAMES = [
    'Array Data File',
    'Derived Array Data File',
    'Derived Array Data Matrix File',
    'Derived Data File',
    'Image File'
]


def is_file_group(group):
    if "Derived Data File REF" in group:
        return True
    if "Comment [Derived Data File REF]" in group:
        return True
    for name in FILE_COL_NAMES:
        if name in group and "Comment [TCGA Archive Name]" in group:
            return True
    return False


def get_file_and_archive(row):
    # first deal with cghub files
    if row.get("Derived Data File REF"):
        return row["Derived Data File REF"], None
    elif row.get("Comment [Derived Data File REF]"):
        return row["Comment [Derived Data File REF]"], None
    for name in FILE_COL_NAMES:
        if row.get(name):
            return row[name], row["Comment [TCGA Archive Name]"]
    return None, None


def group_by_protocol(df):
    """Split a dataframe into groups based on protocol REF and return a
    list of them"""
    groups = [[]]
    for col in df.columns:
        if "Protocol REF" in col:
            groups.append([])
        groups[-1].append(col)
    return groups


def is_reference_row(row):
    return is_reference(row["Extract Name"])


class TCGAMAGETABSyncer(object):

    def __init__(self, archive, pg_driver=None, dcc_auth=None):
        self.archive = archive
        self.pg_driver = pg_driver
        self.dcc_auth = dcc_auth
        folder_url = self.archive["dcc_archive_url"].replace(".tar.gz", "")
        self.df = pd.read_table(sdrf_from_folder(folder_url))
        self.log = get_logger("tcga_magetab_sync")

    def sample_for(self, row):
        extract_name = "Extract Name"
        tcga_barcode = "Comment [TCGA Barcode]"
        if is_uuid(row[extract_name]):
            if is_barcode(row[tcga_barcode]):
                # the most common case
                return row[extract_name], row[tcga_barcode]
        elif is_barcode(row[extract_name]):
            if not row.get(tcga_barcode) or is_empty(row[tcga_barcode]):
                # second most common, Extract Name is the barcode
                return None, row[extract_name]
        # if we get here, the other possibility is that the Extract Names
        # are the weird barcodes with dots, e.g. TCGA-LN-A9FP-01A-41-A41Y-20.P
        else:
            fixed = strip_dot_trailer(row[extract_name])
            if is_barcode(fixed) and not row.get(tcga_barcode):
                return None, fixed
        raise RuntimeError("Can't compute uuid/barcode for {}".format(row))

    def compute_mapping(self):
        groups = [group for group in group_by_protocol(self.df)]
        result = {}  # a dict from (archive, filename) pairs to (uuid,
                     # barcode) pairs
        file_groups = [group for group in groups if is_file_group(group)]
        for _, row in self.df.iterrows():
            if is_reference_row(row):
                self.log.debug("skipping row because Extract Name (%s) is a reference",
                               row["Extract Name"])
                continue
            uuid, barcode = self.sample_for(row)
            for group in file_groups:
                subrow = row[group]
                cleanup(subrow)
                file, archive = get_file_and_archive(subrow)
                if file is None and archive is None:
                    self.log.debug("couldnt extract file from row %s", row)
                    continue
                result[(archive, file)] = (uuid, barcode)
        return result
