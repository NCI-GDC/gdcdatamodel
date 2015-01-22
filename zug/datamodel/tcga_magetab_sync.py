import pandas as pd
from lxml import html
import requests
import os
import re
from collections import defaultdict

from psqlgraph import PsqlEdge
from sqlalchemy.orm.exc import NoResultFound

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


def is_uuid4(s):
    # note that this matches uuid4s, which it seems like everything in
    # tcga is. some of ours are going to be uuid5s, so if you are
    # looking at this wondering why it isn't matching, that might be
    # why
    uuid_re = re.compile("^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
    return re.match(uuid_re, s)


def is_truncated_shipped_portion_barcode(s):
    # some shipped portion barcodes appear to have been truncated,
    # e.g. TCGA-04-1342-01A-21-20. as far as I can tell they are
    # totally useless, but the sample name for these should always
    # have a useful uuid
    barcode_re = re.compile("^TCGA-[A-Z0-9]{2}-[0-9A-Za-z]{4}-[0-9]{2}[A-Z]-[0-9]{2}-[0-9]{2}$")
    return re.match(barcode_re, s)


def is_fat_fingered_shipped_portion_barcode(s):
    # some shipped portion barcodes appear to have been fat fingered, e.g.
    # e.g. TCGA-D9-A1X3-06A21-A20M-20, which should be TCGA-D9-A1X3-06-A21-A20M-20
    barcode_re = re.compile("^TCGA-[A-Z0-9]{2}-[0-9A-Za-z]{4}-[0-9]{2}[A-Z][0-9]{2}-[0-9A-Za-z]{4}-[0-9]{2}$")
    return re.match(barcode_re, s)


def fix_fat_fingered_barcode(s):
    # what happened above is fairly clear, so we can fix it programmatically
    return re.sub("([0-9]{2}[A-Z])([0-9]{2})", "\g<1>-\g<2>", s)


def is_shipped_portion_barcode(s):
    # e.g. TCGA-OR-A5LP-01A-21-A39K-20
    #      TCGA-CM-5341-01A-21-1933-20
    barcode_re = re.compile("^TCGA-[A-Z0-9]{2}-[0-9A-Za-z]{4}-[0-9]{2}[A-Z]-[0-9]{2}-[0-9A-Za-z]{4}-[0-9]{2}$")
    return re.match(barcode_re, s)


def is_aliquot_barcode(s):
    # e.g. TCGA-02-0001-01C-01D-0182-01
    barcode_re = re.compile("^TCGA-[A-Z0-9]{2}-[0-9A-Za-z]{4}-[0-9]{2}[A-Z]-[0-9]{2}[A-Z]-[0-9A-Za-z]{4}-[0-9]{2}$")
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
    group = cleanup_list(group)
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
    # now dcc files
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


def get_submitter_id_and_rev(archive):
    pat = "\.(\d+?)\.(\d+)$"
    return re.sub(pat, "", archive), re.search(pat, archive).group(1)


class TCGAMAGETABSyncer(object):

    def __init__(self, archive, pg_driver=None, cache_path=None, lazy=False):
        self.archive = archive
        self.pg_driver = pg_driver
        self.log = get_logger("tcga_magetab_sync_{}".format(
            self.archive["archive_name"]))
        self._cache_path = cache_path
        if not lazy:
            self.fetch_sdrf()
        self._mapping = None

    def fetch_sdrf(self):
        folder_url = self.archive["dcc_archive_url"].replace(".tar.gz", "")
        if self._cache_path:
            pickle_path = os.path.join(self._cache_path, "{}.pickle".format(self.archive["archive_name"]))
            self.log.info("reading sdrf from cache path %s", pickle_path)
            self.df = pd.read_pickle(pickle_path)
        else:
            self.log.info("downloading sdrf from %s", folder_url)
            self.df = pd.read_table(sdrf_from_folder(folder_url))

    def cache_df_to_file(self, path):
        self.df.to_pickle(os.path.join(path, "{}.pickle".format(
            self.archive["archive_name"])))

    def sample_for(self, row):
        EXTRACT_NAME = "Extract Name"
        TCGA_BARCODE = "Comment [TCGA Barcode]"
        SAMPLE_NAME = "Sample Name"
        ALIQUOT = "aliquot"
        PORTION = "portion"
        SPECEMIN_TYPE = "Comment [TCGA Biospecimen Type]"
        if is_uuid4(row[EXTRACT_NAME].lower()):
            if is_aliquot_barcode(row[TCGA_BARCODE]):
                # the most common case
                return ALIQUOT, row[EXTRACT_NAME].lower(), row[TCGA_BARCODE]
        elif (is_aliquot_barcode(row[EXTRACT_NAME])
                and is_aliquot_barcode(row[TCGA_BARCODE])
                and row[TCGA_BARCODE] == row[EXTRACT_NAME]):
                # sometimes the barcode and EXTRACT_NAME are both
                # identical barcodes
                return ALIQUOT, None, row[EXTRACT_NAME]
        # if we get here, the other possibility is that the Extract
        # Names are the shipped portion barcodes with dots,
        # e.g. TCGA-LN-A9FP-01A-41-A41Y-20.P
        else:
            # we don't expect there to be a barcode column for the
            # shipped portion magetabs
            assert not row.get(TCGA_BARCODE)
            # we expect the biospecemin type to be Shipped portion
            assert row[SPECEMIN_TYPE] in ["Shipped portion", "Shipped Portion"]
            fixed = strip_dot_trailer(row[EXTRACT_NAME])
            if is_shipped_portion_barcode(fixed):
                barcode = fixed
            elif is_fat_fingered_shipped_portion_barcode(fixed):
                barcode = fix_fat_fingered_barcode(fixed)
                assert is_shipped_portion_barcode(barcode)
            elif is_truncated_shipped_portion_barcode(fixed):
                # in this case, the barcode is totally useless, so we just
                # set it to None
                barcode = None
            # the sample name here should always be the uuid
            uuid = row[SAMPLE_NAME].lower()
            assert is_uuid4(uuid)
            return PORTION, uuid, barcode
        raise RuntimeError("Can't compute uuid/barcode for {}".format(row))

    def compute_mapping(self, force=False):
        if self._mapping and not force:
            self.log.info("cached mapping present, not computing")
            return self._mapping
        self.log.info("computing mappings . . .")
        groups = [group for group in group_by_protocol(self.df)]
        result = defaultdict(lambda: set())  # a dict from (archive,
                                             # filename) pairs to (label,
                                             # uuid, barcode) triples
        file_groups = [group for group in groups if is_file_group(group)]
        self.log.debug("file groups are %s", file_groups)
        for _, row in self.df.iterrows():
            if is_reference_row(row):
                self.log.debug("skipping row because Extract Name (%s) is a reference",
                               row["Extract Name"])
                continue
            sample = self.sample_for(row)
            for group in file_groups:
                subrow = row[group]
                cleanup_row(subrow)
                file, archive = get_file_and_archive(subrow)
                if is_empty(file):
                    self.log.debug("file is empty in row %s", row)
                    continue
                if file is None and archive is None:
                    self.log.debug("couldnt extract file from row %s", row)
                    continue
                else:
                    result[(archive, file)].add(sample)
        self. _mapping = result
        return result

    def get_file_node(self, archive_name, file_name, session):
            if archive_name is None:
                # this is a cghub file
                file_node = self.pg_driver.node_lookup(
                    label="file",
                    property_matches={"file_name": file_name},
                    system_annotation_matches={"source": "cghub"},
                    session=session
                ).one()
            else:
                # dcc file
                submitter_id, revision = get_submitter_id_and_rev(archive_name)
                archive_node = self.pg_driver.node_lookup(
                    label="archive",
                    property_matches={"submitter_id": submitter_id,
                                      "revision": revision},
                    session=session
                ).one()
                file_node = self.pg_driver.node_lookup(
                    label="file",
                    property_matches={"file_name": file_name},
                    session=session
                ).with_edge_to_node("member_of", archive_node).one()
            # TODO might need to explicitly free the archive node here
            # to avoid connection leaks
            return file_node

    def tie_to_biospecemin(self, file, label, uuid, barcode, session):
        if uuid:
            bio = self.pg_driver.node_lookup(node_id=uuid).one()
            assert bio.label == label
            assert bio["submitter_id"] == barcode
        elif barcode:
            bio = self.pg_driver.node_lookup(
                label=label,
                property_matches={"submitter_id": barcode},
            ).one()
        edge = PsqlEdge(
            label="data_from",
            src_id=file.node_id,
            dst_id=bio.node_id
        )
        self.pg_driver.edge_insert(edge, session=session)

    def put_mapping_in_pg(self, mapping):
        for (archive, filename), specemins in mapping.iteritems():
            for (label, uuid, barcode) in specemins:
                with self.pg_driver.session_scope() as session:
                    try:
                        file = self.get_file_node(archive, filename, session)
                    except NoResultFound:
                        session.rollback()
                        self.log.warning("Couldn't find file %s in archive %s", filename, archive)
                        continue
                    try:
                        self.tie_to_biospecemin(file, label, uuid,
                                                barcode, session)
                    except NoResultFound:
                        session.rollback()
                        self.log.warning("Couldn't find bionspecemin (%s, %s, %s)", label, uuid, barcode)

    def sync(self):
        mapping = self.compute_mapping()
        self.put_mapping_in_pg(mapping)
