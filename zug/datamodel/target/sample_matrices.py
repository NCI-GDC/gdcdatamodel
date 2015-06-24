from urlparse import urljoin
import re
import os
import itertools
import requests
import xlrd
import pandas as pd
from lxml import html
from collections import defaultdict
from cdisutils.log import get_logger
from uuid import UUID, uuid5
from datetime import datetime

from sqlalchemy import Integer
from psqlgraph import PsqlGraphDriver
from gdcdatamodel.models import Aliquot, Participant, Sample, Project

from zug.datamodel.target import barcode_to_aliquot_id_dict
from zug.datamodel.target import PROJECTS

NAMESPACE_PARTICIPANTS = UUID('6e201b2f-d528-411c-bc21-d5ffb6aa8edb')
NAMESPACE_SAMPLES = UUID('90383d9f-5124-4087-8d13-5548da118d68')

TUMOR_CODE_TO_DESCRIPTION = {
    "00": "Non cancerous tissue",
    "01": "Diffuse Large B-Cell Lymphoma (DLBCL)",
    "02": "Lung Cancer (all types)",
    "03": "Cervical Cancer (all types)",
    "04": "Anal Cancer (all types)",
    "10": "Acute lymphoblastic leukemia (ALL)",
    "20": "Acute myeloid leukemia (AML)",
    "21": "Induction Failure AML (AML-IF)",
    "30": "Neuroblastoma (NBL)",
    "40": "Osteosarcoma (OS)",
    "41": "Ewing sarcoma",
    "50": "Wilms tumor (WT)",
    "51": "Clear cell sarcoma of the kidney (CCSK)",
    "52": "Rhabdoid tumor (kidney) (RT)",
    "60": "CNS, ependymoma",
    "61": "CNS, glioblastoma (GBM)",
    "62": "CNS, rhabdoid tumor",
    "63": "CNS, low grade glioma (LGG)",
    "64": "CNS, medulloblastoma",
    "65": "CNS, other",
    "70": "NHL, anaplastic large cell lymphoma",
    "71": "NHL, Burkitt lymphoma (BL)",
    "80": "Rhabdomyosarcoma",
    "81": "Soft tissue sarcoma, non-rhabdomyosarcoma",
}

SAMPLE_TYPE_TO_DESCRIPTION = {
    "01": "Primary Tumor",
    "02": "Recurrent Tumor",
    "03": "Primary Blood Derived Cancer - Peripheral Blood",
    "04": "Recurrent Blood Derived Cancer - Peripheral Blood",
    "05": "Additional - New Primary",
    "06": "Metastatic",
    "07": "Additional Metastatic",
    "08": "Post neo-adjuvant therapy",
    "09": "Primary Blood Derived Cancer - Bone Marrow",
    "10": "Blood Derived Normal",
    "11": "Solid Tissue Normal",
    "12": "Buccal Cell Normal",
    "13": "EBV Immortalized Normal",
    "14": "Bone Marrow Normal",
    "15": "Fibroblasts from Bone Marrow Normal",
    "20": "Control Analyte",
    "40": "Recurrent Blood Derived Cancer - Peripheral Blood",
    "41": "Blood Derived Cancer - Bone Marrow, Post-treatment",
    "42": "Blood Derived Cancer - Peripheral Blood, Post-treatment",
    "50": "Cell Lines",
    "60": "Primary Xenograft Tissue",
    "61": "Cell Line Derived Xenograft Tissue",
    "99": "Granulocytes",
}


def split_seq(iterable, size):
    it = iter(iterable)
    item = list(itertools.islice(it, size))
    while item:
        yield item
        item = list(itertools.islice(it, size))


def sample_for(aliquot):
    """Return the sample barcode for an aliquot."""
    parts = aliquot.split("-")
    assert len(parts) == 5
    return "-".join(parts[0:-1])


def parse_metadata(sample):
    _, tumor_code, _, tissue = sample.split("-")
    tissue_code = tissue[0:2]
    return tumor_code, tissue_code


def filter_pending(aliquots):
    """Some aliquots are of the form TARGET-52-NAAELV-50A-01R [P-SM],
    which apparently means that the target folks are still waiting on
    data about this aliquot, so we should skip these for now.
    """
    return [aliquot for aliquot in aliquots
            if not re.search("\[P-[A-Z]{2}\]", aliquot)]


class TARGETSampleMatrixSyncer(object):

    def __init__(self, project, graph=None, dcc_auth=None):
        self.project = project
        if graph:
            self.graph = graph
        else:
            self.graph = PsqlGraphDriver(
                os.environ["PG_HOST"],
                os.environ["PG_USER"],
                os.environ["PG_PASS"],
                os.environ["PG_NAME"],
            )
        if dcc_auth:
            self.dcc_auth = dcc_auth
        else:
            self.dcc_auth = (os.environ["DCC_USER"], os.environ["DCC_PASS"])
        self.log = get_logger("target_sample_matrix_import_{}".format(self.project))

    def locate_sample_matrix(self):
        """Given a project, return the url to it's latest sample matrix."""
        if self.project == "ALL-P1":
            search_url = "https://target-data.nci.nih.gov/ALL/Phase_I/Discovery/SAMPLE_MATRIX/"
        elif self.project == "ALL-P2":
            search_url = "https://target-data.nci.nih.gov/ALL/Phase_II/Discovery/SAMPLE_MATRIX/"
        elif self.project in PROJECTS:
            search_url = "https://target-data.nci.nih.gov/{}/Discovery/SAMPLE_MATRIX/".format(self.project)
        else:
            raise RuntimeError("project {} is not known".format(self.project))
        resp = requests.get(search_url, auth=self.dcc_auth)
        resp.raise_for_status()
        search_html = html.fromstring(resp.content)
        links = search_html.cssselect('a')
        for link in links:
            maybe_match = re.search("SampleMatrix_([0-9]{8})", link.attrib["href"])
            if maybe_match:
                self.url = urljoin(search_url, link.attrib["href"])
                self.version = datetime.strptime(maybe_match.group(1), "%Y%m%d").toordinal()
                return
        raise RuntimeError("Could not find sample matrix at url {}".format(search_url))

    def load_sample_matrix(self, data):
        """Given a url, load a sample matrix from it into a pandas DataFrame"""
        book = xlrd.open_workbook(file_contents=data)
        sheet_names = [sheet.name for sheet in book.sheets()]
        if "Sample Names" in sheet_names:
            SHEET = "Sample Names"
        elif "SampleNames" in sheet_names:
            SHEET = "SampleNames"
        else:
            raise RuntimeError("no sheet name in {} recongized".format(sheet_names))
        df = pd.read_excel(book, engine="xlrd", sheetname=SHEET)
        # this is what you have to do to replace NaN with None. note
        # that I just generated this uuid on my laptop, it's value is
        # not important at all. it's just there to make sure there are
        # no collisions
        return df.fillna("71a48a66-1846-4ed0-9e62-bf65a3daf955")\
                 .replace(["71a48a66-1846-4ed0-9e62-bf65a3daf955"], [None])

    def group_by_sample(self, aliquots):
        """Given a list of aliquots, group them into samples, extracting
        metadata from samples as we go"""
        res = defaultdict(lambda: {"aliquots": set()})
        for aliquot in aliquots:
            res[sample_for(aliquot)]["aliquots"].add(aliquot)
        for sample, info in res.iteritems():
            info["tumor_code"], info["tissue_code"] = parse_metadata(sample)
            info["tumor_desc"] = TUMOR_CODE_TO_DESCRIPTION[info["tumor_code"]]
            # tissue code and sample type mean the same thing
            info["tissue_desc"] = SAMPLE_TYPE_TO_DESCRIPTION[info["tissue_code"]]
        return dict(res)

    def participant_mapping(self, participant_id, row):
        """Given a row, extract all the aliquots from it, group them into
        samples, add metadata, and assert various sanity checks"""

        aliquot_lists = [row[col].split(",") for col in row.index
                         if col.endswith("Sample ID") and row.get(col)]
        # this amounts to flattening the nested list
        aliquots = list(itertools.chain(*aliquot_lists))
        aliquots = [aliquot.strip() for aliquot in aliquots]  # some of them have suprious whitespace on the front
        aliquots = filter_pending(aliquots)
        for aliquot in aliquots:
            assert aliquot.startswith(participant_id)
            assert len(aliquot.split("-")) == 5
        sample_groups = self.group_by_sample(aliquots)
        for sample in sample_groups:
            assert sample.startswith(participant_id)
        return sample_groups

    def compute_mapping_from_df(self, df):
        """Computes a dict of the information we need to put in the database
        from a dataframe of the project's sample matrix"""
        CASE_USI = "Case USI"
        mapping = {}
        for _, row in df.iterrows():
            participant_id = row[CASE_USI]
            # TODO filter the row if the "TARGET Case" column is "N".
            # we're not sure this is actually the right column, which
            # is why I haven't done it yet
            if participant_id:
                participant_id = participant_id.strip()
                if not participant_id.startswith("TARGET"):
                    self.log.info("Skipping Case USI: %s because it doesn't start with 'TARGET-'", participant_id)
                    continue
                assert len(participant_id.split("-")) == 3
            else:
                continue
            if mapping.get(participant_id):
                self.log.warning("%s is duplicated in this sample matrix", participant_id)
                continue
            else:
                mapping[participant_id] = self.participant_mapping(participant_id, row)
                if not mapping[participant_id]:
                    self.log.warning("mapping for participant %s is empty", participant_id)
        return mapping

    def sanity_check(self, mapping):
        """Sanity check the data: all samples derived from a participant should
        start with that participant's barcode, all aliquots derived
        from a sample should start with that sample's barcode."""
        for participant, samples in mapping.iteritems():
            for sample, contents in samples.iteritems():
                assert sample.startswith(participant)
                for aliquot in contents["aliquots"]:
                    assert aliquot.startswith(sample)

    def compute_mapping(self):
        self.locate_sample_matrix()
        resp = requests.get(self.url, auth=self.dcc_auth)
        df = self.load_sample_matrix(resp.content)
        mapping = self.compute_mapping_from_df(df)
        self.sanity_check(mapping)
        return mapping

    def sync(self):
        self.log.info("Fetching and extracting info from sample matrix.")
        mapping = self.compute_mapping()
        self.log.info("Storing sample matrix data in database.")
        with self.graph.session_scope():
            self.put_mapping_in_pg(mapping)
            self.log.info("Removing old versions of data from this matrix.")
            self.remove_old_versions()

    def remove_old_versions(self):
        models_to_remove = [Aliquot, Participant, Sample]
        for model in models_to_remove:
            q = self.graph.nodes(model)\
                          .sysan({"group_id": self.project})\
                          .filter(model._sysan["version"].cast(Integer) < self.version)
            self.log.info("Found %s old %s to remove.", q.count(), model.__name__)
            for node in q.all():
                self.log.info("Deleting node %s", node)
                self.graph.node_delete(node=node)

    def put_mapping_in_pg(self, mapping):
        self.log.info("Constructing dict from aliquot barcode -> cghub uuid")
        aliquot_id_map = barcode_to_aliquot_id_dict()
        sysans = {
            "source": "target_sample_matrices",
            "group_id": self.project,
            "version": self.version,
        }
        project_node = self.graph.nodes(Project)\
                                 .props(code=self.project)\
                                 .one()
        for participant, samples in mapping.iteritems():
            part_node = Participant(
                node_id=str(uuid5(NAMESPACE_PARTICIPANTS, str(participant))),
                submitter_id=participant,
                days_to_index=None,
            )
            part_node.system_annotations = sysans
            self.log.info("creating participant %s as %s",
                          participant, part_node)
            for sample, contents in samples.iteritems():
                sample_node = Sample(
                    node_id=str(uuid5(NAMESPACE_SAMPLES, str(sample))),
                    submitter_id=sample,
                    sample_type_id=contents["tissue_code"],
                    sample_type=contents["tissue_desc"],
                    tumor_code_id=contents["tumor_code"],
                    tumor_code=contents["tumor_desc"],
                    longest_dimension=None,
                    intermediate_dimension=None,
                    shortest_dimension=None,
                    initial_weight=None,
                    current_weight=None,
                    freezing_method=None,
                    oct_embedded=None,
                    time_between_clamping_and_freezing=None,
                    time_between_excision_and_freezing=None,
                    days_to_collection=None,
                    days_to_sample_procurement=None,
                    is_ffpe=None,
                    pathology_report_uuid=None,
                )
                sample_node.system_annotations = sysans
                self.log.info("creating sample %s as %s", sample, sample_node)
                self.log.info("tieing sample to participant %s", part_node)
                part_node.samples.append(sample_node)
                for aliquot in contents["aliquots"]:
                    if not aliquot_id_map.get(aliquot):
                        self.log.info("Not inserting aliquot %s because it doesn't have an id in cghub", aliquot)
                    else:
                        aliquot_node = Aliquot(
                            node_id=aliquot_id_map[aliquot],
                            submitter_id=aliquot,
                            source_center=None,
                            amount=None,
                            concentration=None,
                        )
                        aliquot_node.system_annotations = sysans
                        self.log.info("creating aliquot %s as %s",
                                      aliquot, aliquot_node)
                        self.log.info("tieing aliquot %s to sample %s",
                                      aliquot_node, sample_node)
                        sample_node.aliquots.append(aliquot_node)
            self.log.info("inserting participant %s", part_node)
            part_node = self.graph.current_session().merge(part_node)
            self.log.info("tieing %s to project", part_node)
            part_node.projects = [project_node]
