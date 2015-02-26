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

PROJECTS = ["ALL-P1", "ALL-P2", "AML", "CCSK", "NBL", "OS", "RT", "WT"]

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
    "03": "Primary Blood Cancer",
    "04": "Recurrent Blood Cancer",
    "05": "Additional - New Primary",
    "06": "Metastatic",
    "07": "Additional Metastatic",
    "08": "Post neo-adjuvant therapy",
    "09": "Primary Blood Cancer BM",
    "10": "Blood Derived Normal",
    "11": "Solid Tissue Normal",
    "12": "Buccal Cell Normal",
    "13": "EBV Normal",
    "14": "BM Normal",
    "15": "Fibroblast Normal",
    "20": "Cell Line Control",
    "40": "Recurrent Blood Cancer",
    "41": "Post treatment Blood Cancer Bone Marrow ",
    "42": "Post treatment Blood Cancer Blood",
    "50": "Cancer cell line",
    "60": "Xenograft, primary",
    "61": "Xenograft, cell-line derived",
    "99": "Granulocytes",
}


def sample_for(aliquot):
    """Return the sample barcode for an aliquot."""
    parts = aliquot.split("-")
    assert len(parts) == 5
    return "-".join(parts[0:-1])


def parse_metadata(sample):
    _, tumor_code, _, tissue = sample.split("-")
    tissue_code = tissue[0:2]
    return tumor_code, tissue_code


class TARGETSampleMatrixImporter(object):

    def __init__(self, project, graph=None, dcc_auth=None):
        self.project = project
        self.graph = graph
        self.dcc_auth = dcc_auth
        self.log = get_logger("target_sample_matrix_import_{}_{}".format(os.getpid(), self.project))

    def locate_sample_matrix_for_project(self):
        """Given a project, return the url to it's latest sample matrix."""
        if self.project == "ALL-P1":
            search_url = "https://target-data.nci.nih.gov/ALL/Phase_I/Discovery/SAMPLE_MATRIX/"
            template = "TARGET_ALLP1_SampleMatrix_([0-9]*)\.xlsx"
        elif self.project == "ALL-P2":
            search_url = "https://target-data.nci.nih.gov/ALL/Phase_II/Discovery/SAMPLE_MATRIX/"
            template = "TARGET_ALLP2_SampleMatrix_([0-9]*)\.xlsx"
        elif self.project in ["AML", "CCSK", "NBL", "OS", "RT", "WT"]:
            search_url = "https://target-data.nci.nih.gov/{}/Discovery/SAMPLE_MATRIX/".format(self.project)
            template = "(TARGET_)?{}_SampleMatrix_([0-9]*)\.xlsx".format(self.project)
        else:
            raise RuntimeError("project {} is not known".format(self.project))
        resp = requests.get(search_url, auth=self.dcc_auth)
        resp.raise_for_status()
        search_html = html.fromstring(resp.content)
        links = search_html.cssselect('a')
        for link in links:
            maybe_match = re.match(template, link.attrib["href"])
            if maybe_match:
                self.url = urljoin(search_url, link.attrib["href"])
                self.version = maybe_match.groups()[-1]
                return

    def load_sample_matrix(self, url):
        """Given a url, load a sample matrix from it into a pandas DataFrame"""
        resp = requests.get(url, auth=self.dcc_auth)
        book = xlrd.open_workbook(file_contents=resp.content)
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
            if participant_id:
                participant_id = participant_id.strip()
                assert len(participant_id.split("-")) == 3
                assert participant_id.startswith("TARGET")
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

    def compute_mapping(self):
        self.locate_sample_matrix_for_project()
        df = self.load_sample_matrix(self.url)
        return self.compute_mapping_from_df(df)
