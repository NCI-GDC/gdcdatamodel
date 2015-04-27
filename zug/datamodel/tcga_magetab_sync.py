import time
import pandas as pd
from collections import defaultdict

import os
from lxml import html
import re
from psqlgraph import PsqlGraphDriver, PsqlEdge
from sqlalchemy import func
from sqlalchemy.types import Integer
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm.exc import NoResultFound
import requests
from cdisutils.log import get_logger

from gdcdatamodel.models import (
    ArchiveRelatedToFile,
    FileMemberOfArchive,
)


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
    return re.sub(pat, "", archive), int(re.search(pat, archive).group(1))


class TCGAMAGETABSyncer(object):

    def __init__(self, cache_path=None, archive_id=None):
        self.log = get_logger("tcga_magetab_sync_{}".format(os.getpid()))
        self.graph = PsqlGraphDriver(
            os.environ["PG_HOST"],
            os.environ["PG_USER"],
            os.environ["PG_PASS"],
            os.environ["PG_NAME"],
        )
        self.archive_id = archive_id
        # this is to keep track of the number of edges we got out of
        # this magetab so we can record it on system_annotations and
        # manually investigate anything that looks fishy (e.g. zero,
        # a billion)
        self.edges_from = 0
        self._cache_path = cache_path
        self._mapping = None

    @property
    def revision(self):
        return self.archive["revision"]

    @property
    def submitter_id(self):
        return self.archive["submitter_id"]

    def fetch_sdrf(self):
        # TODO (jjp) this is somewhat janky and it would probably be
        # faster to fetch the archive from our object store since
        # we've already downloaded it, however this would require
        # adding a bunch more complicated configuration info for
        # connecting to various object stores to this class, which is
        # why I'm not doing it right now
        folder_url = self.archive.system_annotations["dcc_archive_url"].replace(".tar.gz", "")
        if self._cache_path:
            pickle_path = os.path.join(self._cache_path, "{}.pickle".format(
                self.archive.system_annotations["archive_name"])
            )
            self.log.info("reading sdrf from cache path %s", pickle_path)
            return pd.read_pickle(pickle_path)
        else:
            self.log.info("downloading sdrf from %s", folder_url)
            return pd.read_table(sdrf_from_folder(folder_url))

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
        self._mapping = result
        return result

    def get_file_node(self, archive_name, file_name):
            if archive_name is None:
                # this is a cghub file
                file_node = self.graph.nodes()\
                                      .labels("file")\
                                      .props({"file_name": file_name})\
                                      .sysan({"source": "tcga_cghub"})\
                                      .one()
            else:
                # dcc file
                submitter_id, revision = get_submitter_id_and_rev(archive_name)
                archive_node = self.graph.nodes()\
                                         .labels("archive")\
                                         .props({"submitter_id": submitter_id,
                                                 "revision": revision})\
                                         .one()
                file_node = self.graph.nodes()\
                                      .labels("file")\
                                      .props({"file_name": file_name})\
                                      .with_edge_to_node(
                                          FileMemberOfArchive, archive_node)\
                                      .one()
            # TODO might need to explicitly free the archive node here
            # to avoid connection leaks
            return file_node

    def tie_to_biospecemin(self, file, label, uuid, barcode):
        if uuid:
            bio = self.graph.nodes().ids(uuid).one()
            self.log.info("found biospecemin by uuid: %s", bio)
            assert bio.label == label
            if barcode:
                assert bio["submitter_id"] == barcode
        elif barcode:
            bio = self.graph.nodes()\
                            .labels(label)\
                            .props({"submitter_id": barcode})\
                            .one()
            self.log.info("found biospecemin by barcode: %s", bio)

        maybe_edge = self.graph.edges()\
                               .labels("data_from")\
                               .src(file.node_id)\
                               .dst(bio.node_id).scalar()
        self.edges_from += 1
        if maybe_edge:
            self.log.info("edge already exists: %s", maybe_edge)
        else:
            if file.system_annotations["source"] == "tcga_cghub":
                self.log.warning("cghub file %s should be tied to %s %s but is not",
                                 file, label, (uuid, barcode))
                return
            edge = self.pg_driver.get_PsqlEdge(
                label="data_from",
                src_id=file.node_id,
                dst_id=bio.node_id,
                system_annotations={
                    "submitter_id": self.submitter_id,
                    "revision": self.revision,
                    "source": "tcga_magetab",
                },
                src_label='file',
                dst_label=bio.label,
            )
            self.log.info("tieing file to biospecemin: %s", edge)
            with self.graph.session_scope() as s:
                s.merge(edge)

    def delete_old_edges(self):
        """We need to first find all the edges produced by previous runs of
        this archive and delete them."""
        to_delete = self.graph.edges()\
                              .sysan({"source": "tcga_magetab"})\
                              .sysan({"submitter_id": self.submitter_id})\
                              .filter(PsqlEdge.system_annotations["revision"].cast(Integer) < self.revision)\
                              .all()
        self.log.info("found %s edges to delete from previous revisions", len(to_delete))
        for edge in to_delete:
            self.log.info("deleting old edge %s", edge)
            self.graph.edge_delete(edge)

    def tie_to_archive(self, file):
        maybe_edge_to_archive = self.graph.edges()\
                                          .labels("related_to")\
                                          .src(self.archive.node_id)\
                                          .dst(file.node_id).scalar()
        if not maybe_edge_to_archive:
            edge_to_archive = ArchiveRelatedToFile(
                label="related_to",
                src_id=self.archive.node_id,
                dst_id=file.node_id,
                system_annotations={
                    "submitter_id": self.submitter_id,
                    "revision": self.revision,
                    "source": "tcga_magetab",
                }
            )
            self.log.info("relating file to magetab archive %s",
                          edge_to_archive)
            with self.edge.session_scope() as s:
                s.merge(edge_to_archive)

    def put_mapping_in_pg(self, mapping):
        self.delete_old_edges()
        for (archive, filename), specemins in mapping.iteritems():
            if archive is None:
                self.log.info("%s is a cghub file, skipping", filename)
                continue
            for (label, uuid, barcode) in specemins:
                self.log.info("attempting to tie file %s to specemin %s",
                              (archive, filename),
                              (label, uuid, barcode))
                try:
                    file = self.get_file_node(archive, filename)
                    self.log.info("found file node %s", file)
                except NoResultFound:
                    self.log.warning("Couldn't find file %s in archive %s", filename, archive)
                    continue
                try:
                    self.tie_to_biospecemin(file, label, uuid, barcode)
                    self.tie_to_archive(file)
                except NoResultFound:
                    self.log.warning("Couldn't find biospecemin (%s, %s, %s)", label, uuid, barcode)

    def get_archive_to_workon(self):
        """
        Find an unsynced mage-tab archive in the graph to download, parse, and insert metadata from
        """
        lock_tries = 0
        while lock_tries < 5:
            lock_tries += 1
            try:
                if self.archive_id:
                    # if we were passed an id, just grab that
                    try_archive = self.graph.nodes.ids(self.archive_id).one()
                    assert try_archive.label == "archive"
                else:
                    self.log.info("Searching for archive to work on")
                    # find an archive that we want to try to work on
                    try_archive = self.graph.nodes()\
                                            .labels("archive")\
                                            .sysan({"data_level": "mage-tab"})\
                                            .not_sysan({"magetab_synced": True})\
                                            .order_by(func.random())\
                                            .first()
                    if not try_archive:
                        self.log.info("No unsynced magetab archives found, we're all caught up!")
                        return
                self.log.info("Attempting to lock %s in postgres", try_archive)
                archive = self.graph.nodes()\
                                    .ids(try_archive.node_id)\
                                    .with_for_update(nowait=True).one()
                self.archive = archive
                return self.archive
            except OperationalError:
                self.graph.current_session().rollback()
                self.log.exception("Couldn't lock archive %s, try number %s, retrying",
                                   try_archive.node_id, lock_tries)
                time.sleep(3)
        raise RuntimeError("Couldn't lock archive to sync in 5 tries")

    def mark_synced(self):
        self.log.info("marking %s as magetab_synced and resulting in %s edges ",
                      self.archive, self.edges_from)
        # TODO record how many edges we got from this so we can investigate
        # anything suspicious (i.e. 0)
        self.graph.node_update(
            self.archive,
            system_annotations={
                "magetab_synced": True,
                "magetab_edges_from": self.edges_from,
            }
        )

    def sync(self):
        # we have to use one session at the top level because we need
        # to hold the lock on the magetab archive the whole time we're
        # working on it
        with self.graph.session_scope():
            if not self.get_archive_to_workon():
                return
            self.df = self.fetch_sdrf()
            # the "mapping" is a dictionary from (archive, filename) pairs
            # to (label, uuid, barcode) triples
            mapping = self.compute_mapping()
            self.put_mapping_in_pg(mapping)
            self.mark_synced()
