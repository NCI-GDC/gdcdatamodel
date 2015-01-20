import pandas as pd
from lxml import html
import requests
import os
import re

from psqlgraph import PsqlEdge

from cdisutils.log import get_logger


def sdrf_from_folder(url):
    tree = html.fromstring(requests.get(url).content)
    links = tree.cssselect("a")
    for link in links:
        if "sdrf" in link.attrib["href"]:
            return os.path.join(url, link.attrib["href"])


REF_NAMES = {
    '231 Control.P',
    '231 IGF.P',
    '468 Control.P',
    '468 EGF.P',
    'BD_Human_Tissue_Ref_RNA_Extract',
    'BioChain_RtHanded_Total_Brain_RNA_Extract',
    'BioChain_RtHanded_Total_Ovary_RNA_Extract',
    'Control_Jurkat-Control-1.P',
    'Control_Jurkat-Control-2.P',
    'Control_Jurkat-Control-3.P',
    'Control_Jurkat-Control-4.P',
    'Control_Jurkat-Control-5.P',
    'Control_Jurkat-Control-6.P',
    'Control_Jurkat-Fas-1.P',
    'Control_Jurkat-Fas-2.P',
    'Control_Jurkat-Fas-3.P',
    'Control_Jurkat-Fas-4.P',
    'Control_Jurkat-Fas-5.P',
    'Control_Jurkat-Fas-6.P',
    'Control_MDA-MB-231-Control-1.P',
    'Control_MDA-MB-231-Control-2.P',
    'Control_MDA-MB-231-Control-3.P',
    'Control_MDA-MB-231-Control-4.P',
    'Control_MDA-MB-231-Control-5.P',
    'Control_MDA-MB-231-Control-6.P',
    'Control_MDA-MB-231-IGF-1.P',
    'Control_MDA-MB-231-IGF-2.P',
    'Control_MDA-MB-231-IGF-3.P',
    'Control_MDA-MB-231-IGF-4.P',
    'Control_MDA-MB-231-IGF-5.P',
    'Control_MDA-MB-231-IGF-6.P',
    'Control_MDA-MB-468-Control-1.P',
    'Control_MDA-MB-468-Control-2.P',
    'Control_MDA-MB-468-Control-3.P',
    'Control_MDA-MB-468-Control-4.P',
    'Control_MDA-MB-468-Control-5.P',
    'Control_MDA-MB-468-Control-6.P',
    'Control_MDA-MB-468-EGF-1.P',
    'Control_MDA-MB-468-EGF-2.P',
    'Control_MDA-MB-468-EGF-3.P',
    'Control_MDA-MB-468-EGF-4.P',
    'Control_MDA-MB-468-EGF-5.P',
    'Control_MDA-MB-468-EGF-6.P',
    'Control_Mixed-Cell-Lysate-1.P',
    'Control_Mixed-Cell-Lysate-2.P',
    'Control_Mixed-Cell-Lysate-3.P',
    'Control_Mixed-Cell-Lysate-4.P',
    'Control_Mixed-Cell-Lysate-5.P',
    'Control_Mixed-Cell-Lysate-6.P',
    'Control_Mixed-Cell-Lysate-7.P',
    'Control_Mixed-Lysate-1.P',
    'Control_Mixed-Lysate-10.P',
    'Control_Mixed-Lysate-11.P',
    'Control_Mixed-Lysate-12.P',
    'Control_Mixed-Lysate-13.P',
    'Control_Mixed-Lysate-14.P',
    'Control_Mixed-Lysate-15.P',
    'Control_Mixed-Lysate-2.P',
    'Control_Mixed-Lysate-3.P',
    'Control_Mixed-Lysate-4.P',
    'Control_Mixed-Lysate-5.P',
    'Control_Mixed-Lysate-6.P',
    'Control_Mixed-Lysate-7.P',
    'Control_Mixed-Lysate-8.P',
    'Control_Mixed-Lysate-9.P',
    'Human male genomic',
    'Jurkat Control.P',
    'Jurkat Fas.P',
    'Mixed Lysate.P',
    'Promega ref DNA',
    'Stratagene Univeral Reference',
    'Stratagene_Cell_Line_Hum_Ref_RNA_Extract'
}


def is_reference(s):
    return s in REF_NAMES


def is_uuid(s):
    uuid_re = re.compile("^[0-9A-F]{8}-[0-9A-F]{4}-4[0-9A-F]{3}-[89AB][0-9A-F]{3}-[0-9A-F]{12}$",re.IGNORECASE)
    return re.match(uuid_re, s)


def is_barcode(s):
    barcode_re = "^TCGA(-[0-9A-Za-z]{1,5})*$"
    return re.match(barcode_re, s)


def is_empty(s):
    return s == "->"


def strip_dot_trailer(s):
    return re.sub("\..*$", "", s)


def cleanup_row(row):
    row.index = cleanup_list(row.index)


def cleanup_list(xs):
    return [re.sub("\.[0-9]*$", "", x) for x in xs]


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


def get_legacy_id_and_rev(archive):
    return re.sub("\.(\d+?)\.(\d+)$", "", archive["archive_name"]), archive["revision"]


class TCGAMAGETABSyncer(object):

    def __init__(self, archive, pg_driver=None, dcc_auth=None):
        self.archive = archive
        self.pg_driver = pg_driver
        self.dcc_auth = dcc_auth
        folder_url = self.archive["dcc_archive_url"].replace(".tar.gz", "")
        self.df = pd.read_table(sdrf_from_folder(folder_url))
        self.log = get_logger("tcga_magetab_sync_{}".format(
            self.archive["archive_name"]))

    def sample_for(self, row):
        extract_name = "Extract Name"
        tcga_barcode = "Comment [TCGA Barcode]"
        if is_uuid(row[extract_name]):
            if is_barcode(row[tcga_barcode]):
                # the most common case
                return row[extract_name], row[tcga_barcode]
        elif is_barcode(row[extract_name]):
            if not row.get(tcga_barcode) or is_empty(row[tcga_barcode]):
                # second most common, Extract Name is the barcode, there is
                # no tcga barcode column
                return None, row[extract_name]
            elif (is_barcode(row[tcga_barcode])
                    and row[tcga_barcode] == row[extract_name]):
                # sometimes the barcode and extract_name are both
                # identical barcodes
                return None, row[extract_name]
        # if we get here, the other possibility is that the Extract Names
        # are the weird barcodes with dots, e.g. TCGA-LN-A9FP-01A-41-A41Y-20.P
        else:
            fixed = strip_dot_trailer(row[extract_name])
            # the `and not` bit is there because in all cases where the
            # extract name is a wonky barcode, there shouldn't be a barcode
            # column; if there is, something is wrong
            if is_barcode(fixed) and not row.get(tcga_barcode):
                return None, fixed
        raise RuntimeError("Can't compute uuid/barcode for {}".format(row))

    def compute_mapping(self, force=False):
        if self._mapping and not force:
            self.log.info("cached mapping present, not computing")
            return self._mapping
        self.log.info("computing mappings . . .")
        groups = [cleanup_list(group) for group in group_by_protocol(self.df)]
        result = {}  # a dict from (archive, filename) pairs to (uuid,
                     # barcode) pairs
        file_groups = [group for group in groups if is_file_group(group)]
        self.log.debug("file groups are %s", file_groups)
        for _, row in self.df.iterrows():
            if is_reference_row(row):
                self.log.debug("skipping row because Extract Name (%s) is a reference",
                               row["Extract Name"])
                continue
            uuid, barcode = self.sample_for(row)
            for group in file_groups:
                subrow = row[group]
                cleanup_row(subrow)
                file, archive = get_file_and_archive(subrow)
                if file is None and archive is None:
                    self.log.debug("couldnt extract file from row %s", row)
                    continue
                if result.get((archive, file)):
                    assert result[(archive, file)] == (uuid, barcode)
                else:
                    result[(archive, file)] = (uuid, barcode)
        return result

    def get_file_node(self, archive_name, file_name, session):
            if archive_name is None:
                # this is a cghub file
                file_node = self.pg_driver.node_lookup(
                    label="file",
                    property_matches={"file_name": file_name},
                    system_annotations_matches={"source": "cghub"},
                    session=session
                ).one()
            else:
                # dcc file
                legacy_id, revision = get_legacy_id_and_rev(self.archive)
                archive_node = self.pg_driver.node_lookup(
                    label="archive",
                    property_matches={"legacy_id": legacy_id,
                                      "revision": revision},
                    session=session
                ).one()
                # dcc file
                file_node = self.pg_driver.node_lookup(
                    label="file",
                    property_matches={"file_name": file_name},
                    session=session
                ).with_edge_to_node("member_of", archive_node).one()
            # TODO might need to explicitly free the archive node here
            return file_node

    def tie_to_biospecemin(self, file, uuid, barcode, session):
        """You would think that this function would be called 'tie to
        aliquot', but sadly no. It appears that in some rare cases,
        the 'Extract Name' column of an sdrf actually refers to some
        other component of the biospecemin pathway (e.g. a sample or
        portion), so this function in most cases will tie files back
        to aliquots, but sometimes other things as well.
        """
        # TODO it seems like in many cases it's possible to figure out
        # what kind of biospecemin a barcode is supposed to correspond
        # to from another column of the magetab (e.g. Comment [TCGA
        # Biospecimen Type])
        if uuid:
            # this should always be an aliquot (I think?)
            bio = self.pg_driver.node_lookup(node_id=uuid)
            assert bio.label == "aliquot"
        else:
            for label in ["aliquot", "portion", "sample"]:
                bio = self.pg_driver.node_lookup(label="portion")
                if bio:
                    break
        edge = PsqlEdge(
            label="data_from",
            src_id=file.node_id,
            dst_id=bio.node_id
        )
        self.driver.edge_insert(edge, session=session)

    def put_mapping_in_pg(self, mapping):
        for (archive, filename), (uuid, barcode) in mapping.iteritems():
            with self.pg_driver.session_scope() as session:
                file = self.get_file_node(archive, filename, session)
                self.tie_to_biospecemin(self, file, session)
