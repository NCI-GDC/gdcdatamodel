import os
from boto.s3.connection import OrdinaryCallingFormat
import pandas as pd
import numpy as np

from boto.s3.key import Key
Key.BufferSize = 10 * 1024 * 1024

from psqlgraph import PsqlGraphDriver
from cdisutils.net import BotoManager
from cdisutils.log import get_logger
from signpostclient import SignpostClient
from gdcdatamodel.models import File, Aliquot, Analyte, DataFormat, Center

from cStringIO import StringIO


def find_first_col(df, cols):
    """Given a dataframe and a list of column names, find the first column
    that exists in the dataframe and return it, otherwise return None.
    """
    for col in cols:
        if col in df.columns:
            return col
    return None


def barcode_is_for_analyte(barcode):
    return len(barcode) == 20


# these are the aliquots we try to tie to in the case that multiple
# aliquots are found for an analyte.
#
# see here for more context on this:
# https://jira.opensciencedatacloud.org/browse/GDC-635?focusedCommentId=15456&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-15456
TO_INCLUDE_FROM_MULTIPLE_UUIDS = [
    "c27d2b85-fef3-4aeb-88dd-40b846d1db18",
    "a731f292-c9a1-4802-9e0a-90a9a2f938ab",
    "70c21e45-6606-4815-8df0-ac276b6980d5",
    "817a1679-eb11-4566-9a85-a2ab7601c5b0",
    "dd8be7f7-4514-48dc-9085-07b70bfefb15",
    "d27e7a9b-e407-44f4-8a2c-91d3eb5db3cb",
    "79fa14ed-bc6b-444f-a2d0-b69ec117defd",
    "8aa636a7-a6a8-4a11-b9bd-225d17711a2d",
    "6f9e5a76-5d2a-4bb0-babf-3f365a177236",
    "36ffe4e2-1f55-4b64-ad4b-e568032f0f05",
]

class MAFReconciler(object):
    """
    Parses MAF files and connects them to the aliquots that were
    used to produce them, only TCGA for now.
    """

    def __init__(self, graph=None, s3=None, signpost=None):
        if not graph:
            self.graph = PsqlGraphDriver(
                os.environ["PG_HOST"],
                os.environ["PG_USER"],
                os.environ["PG_PASS"],
                os.environ["PG_NAME"],
            )
        else:
            self.graph = graph
        if not s3:
            self.s3 = BotoManager({
                "cleversafe.service.consul": {
                    "aws_access_key_id": os.environ["CLEV_ACCESS_KEY"],
                    "aws_secret_access_key": os.environ["CLEV_SECRET_KEY"],
                    "is_secure": True,
                    "calling_format": OrdinaryCallingFormat()
                },
                "ceph.service.consul": {
                    "aws_access_key_id": os.environ["CEPH_ACCESS_KEY"],
                    "aws_secret_access_key": os.environ["CEPH_SECRET_KEY"],
                    "is_secure": True,
                    "calling_format": OrdinaryCallingFormat()
                },
            })
            self.s3.connect()
        else:
            self.s3 = s3
        if not signpost:
            self.signpost = SignpostClient(os.environ["SIGNPOST_URL"])
        else:
            self.signpost = signpost
        self.log = get_logger("maf_reconciler")

    def get_file_data(self, file):
        """
        Get the bytes corresponding to a File (from the object store).
        """
        self.log.info("Getting url from signpost")
        url = self.signpost.get(file.node_id).urls[0]
        self.log.info("Getting file data from s3 for %s" % url)
        if "gdc-accessor" in url:
            offset_loc = 0
            parts = filter(None, url.split('/'))
            for part in parts:
                if "gdc-accessor" in part:
                    break
                else:
                    offset_loc += 1
            if offset_loc < len(parts):
                parts[offset_loc] = "cleversafe.service.consul"
                new_url = '/'.join(parts)
                url = new_url[:new_url.find('/')] + '/' + new_url[new_url.find('/'):]
                self.log.info("Fixed url to %s" % url)
            else:
                self.log.error("Ok, weird, we found the substring in the url %s, but not in the parts" % url)
                raise RuntimeError("Ok, weird, we found the substring in the url %s, but not in the parts" % url)
        try:
            key = self.s3.get_url(url)
        except Exception as e:
            self.log.error("Unable to retrieve %s" % url)
            self.log.error(e)
            raise RuntimeError(e)

        return key.get_contents_as_string()

    def df_from_fileobj(self, file_obj):
        """
        Given a python `file` object, create a pandas dataframe
        """
        self.log
        # index_col = False is necessary because there are some MAFs
        # (e.g. genome.wustl.edu_OV.IlluminaGA_DNASeq.Level_2.2.0.0.somatic.maf)
        # that have an extra column without a corresponding header in
        # the entry row. without passing index_col=False, pandas
        # assumes that the first column is actually names for each row
        # and parses the second column as the first header so all of
        # the headers are on the incorrect column
        file_df = pd.read_table(file_obj, comment="#", index_col=False)
        # normalize column names to lowercase
        file_df.columns = [c.lower() for c in file_df.columns]
        return file_df

    def clean_df(self, df):
        """Do some basic sanity checking on a dataframe parsed from a MAF,
        then normlize it by selecting only the columns we're
        interested in, giving them consistent names, and deduping the
        result.

        """
        self.log.info("Cleaning dataframe")
        POSSIBLE_COL_NAMES = {
            "tumor": [
                "tumor_sample_barcode",
                "tumor_sample_id"
            ],
            "norm": [
                "matched_norm_sample_barcode",
                "match_normal_sample_id",
                "match_norm_sample_id"
            ],
        }
        res = {}
        for kind, col_list in POSSIBLE_COL_NAMES.items():
            barcode_column = find_first_col(df, col_list)
            if not barcode_column:
                raise RuntimeError("couldnt find column barcode for {} in column list {}"
                                   .format(kind, col_list))
            values = df[barcode_column]
            # assert that they're all either strings or NA
            assert all([isinstance(v, basestring) or np.isnan(v) for v in values])
            # check for a uuid column, if it exists, validate that
            # it's strings / NaNs correspond to those in the barcode
            # column
            maybe_uuid_column = [col for col in df.columns
                                 if "uuid" in col and kind in col]
            if maybe_uuid_column:
                uuid_column = maybe_uuid_column[0]
                cleaned = df[[barcode_column, uuid_column]]
                # normalize names
                cleaned.columns = ["barcode", "uuid"]
            else:
                cleaned = df[[barcode_column]]
                cleaned.columns = ["barcode"]
            # some uuids are uppercase so we .lower all of them
            if "uuid" in cleaned.columns:
                cleaned["uuid"] = cleaned["uuid"].str.lower()
            res[kind] = cleaned
        combined = res["tumor"].append(res["norm"], ignore_index=True)
        without_nas = combined.dropna(how="all")
        without_dups = without_nas.drop_duplicates()
        # this next line is effectively a fast way of asserting
        # that there's no barcode that maps to multiple uuids
        assert len(without_dups["barcode"]) == len(without_dups["barcode"].drop_duplicates())
        return without_dups

    def find_aliquot_with_analyte_barcode(self, file, barcode):
        self.log.info("barcode %s is for analyte", barcode)
        # Some really old MAFs have analyte barcodes rather than
        # aliquot barcodes. In these cases we look for the aliquot
        # that comes from that analyte and comes from the same center
        # as this maf file. there should only be one such aliquot
        #
        # see here for more context:
        # https://jira.opensciencedatacloud.org/browse/GDC-635
        try:
            analyte = self.graph.nodes(Analyte)\
                            .props(submitter_id=barcode)\
                            .scalar()
        except Exception as e:
            self.log.error("Unable to get one value for barcode %s" % barcode)
            self.log.error(e)
            raise

        if not analyte:
            if barcode == "TCGA-06-0133-01A-01W":
                # per MAJ here: https://jira.opensciencedatacloud.org/browse/GDC-635?focusedCommentId=15451&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-15451
                return self.graph.nodes(Aliquot)\
                                 .ids("b966fb9b-b173-4406-b00a-8359f38b2ace")\
                                 .one()
            self.log.warning("analyte with barcode %s not found",
                             barcode)
            return None
        this_analyte = Analyte.submitter_id.astext == barcode
        this_center = Center.short_name.astext == file.centers[0].short_name
        maybe_aliquots = self.graph.nodes(Aliquot)\
                                   .filter(Aliquot.analytes.any(this_analyte))\
                                   .filter(Aliquot.centers.any(this_center))\
                                   .all()
        if len(maybe_aliquots) == 0:
            self.log.warning("no aliquot found for analyte %s", analyte)
            return None
        elif len(maybe_aliquots) == 1:
            return maybe_aliquots[0]
        elif len(maybe_aliquots) > 1:
            # first we check if it's one of the aliquots from here:
            # https://jira.opensciencedatacloud.org/browse/GDC-635?focusedCommentId=15456&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-15456
            for aliquot in maybe_aliquots:
                if aliquot.node_id in TO_INCLUDE_FROM_MULTIPLE_UUIDS:
                    return aliquot
            self.log.warning("multiple aliquots found for analyte %s", analyte)
            return None

    def find_aliquot_with_aliquot_barcode(self, barcode, uuid):
        # first try to lookup by barcode
        try:
            aliquot = self.graph.nodes(Aliquot)\
                            .props(submitter_id=barcode).scalar()
        except Exception as e:
            self.log.error("Unable to get one result for %s, %s" % 
                (barcode, uuid)
            )
            self.log.error(e)
            aliquot = None

        if not aliquot:
            # if that fails, try by uuid
            if uuid:
                self.log.info("trying to lookup by uuid %s", uuid)
                aliquot = self.graph.nodes(Aliquot).ids(uuid).one()
                if aliquot:
                    return aliquot
                else:
                    self.log.warning("couldn't find aliquot by uuid %s", uuid)
                    return None
            else:
                self.log.warning("Couldn't find aliqout barcode %s and no uuid in MAF",
                                 barcode)
                return None
        else:
            return aliquot

    def tie_to_aliquots(self, file, cleaned_df):
        """Given a `cleaned_df` parsed from a MAF file, find the relevant
        aliquots in the database and connect the file to them

        """
        self.log.info("Tieing %s to relevant aliquots parsed from dataframe", file)
        with self.graph.session_scope() as session:
            session.add(file)
            aliquots = []
            for _, row in cleaned_df.iterrows():
                barcode = row["barcode"]
                uuid = row.get("uuid")
                barcode_inconsistent = False
                uuid_inconsistent = False
                if barcode_is_for_analyte(barcode):
                    aliquot = self.find_aliquot_with_analyte_barcode(
                        file, barcode
                    )
                    if aliquot:
                        barcode_inconsistent = not aliquot.submitter_id.startswith(barcode)
                else:
                    aliquot = self.find_aliquot_with_aliquot_barcode(
                        barcode, uuid
                    )
                    if aliquot:
                        barcode_inconsistent = aliquot.submitter_id != barcode
                if aliquot:
                    uuid_inconsistent = uuid and aliquot.node_id != uuid
                    if barcode_inconsistent or uuid_inconsistent:
                        self.log.warning(
                            "Inconsistency detected: MAF claims (%s, %s), "
                            "but found %s with barcode %s",
                            barcode, uuid, aliquot, aliquot.submitter_id
                        )
                    self.log.info("Tieing %s to %s", file, aliquot)
                    aliquots.append(aliquot)
            self.log.info("Flushing aliquots for %s to db", file)
            file.aliquots = aliquots

    def reconcile(self, file):
        self.log.info("Reconciling %s", file)
        try:
            data = self.get_file_data(file)
        except Exception as e:
            self.log.error("Unable to get file data for %s" % file)
            self.log.error(e)
            raise RuntimeError(e)
        else:
            df = self.df_from_fileobj(StringIO(data))
            cleaned_df = self.clean_df(df)
            self.tie_to_aliquots(file, cleaned_df)

    def reconcile_all(self):
        """Reconcile all MAF files that don't already have aliquots, for now
        just tcga.

        """
        with self.graph.session_scope():
            self.log.info("Loading MAF files to reconcile")
            is_maf = File.data_formats.any(DataFormat.name.astext == "MAF")
            maf_files = self.graph.nodes(File)\
                                  .sysan(source="tcga_dcc")\
                                  .props(state="live")\
                                  .filter(is_maf)\
                                  .filter(~File.aliquots.any())\
                                  .all()
        self.log.info("Reconciling %s MAFs", len(maf_files))
        for file in maf_files:
            self.reconcile(file)
