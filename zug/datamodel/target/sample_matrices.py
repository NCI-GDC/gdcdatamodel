from urlparse import urljoin
import re
import os
import itertools
import requests
from copy import deepcopy
import xlrd
import pandas as pd
from lxml import html
from collections import defaultdict
from cdisutils.log import get_logger
from uuid import UUID, uuid5
from datetime import datetime
from bs4 import BeautifulSoup

from sqlalchemy import Integer
from psqlgraph import PsqlGraphDriver
from gdcdatamodel.models import Aliquot, Case, Sample, Project

from zug.datamodel.target import barcode_to_aliquot_id_dict
from zug.datamodel.target import PROJECTS

requests.packages.urllib3.disable_warnings()

NAMESPACE_CASES = UUID('6e201b2f-d528-411c-bc21-d5ffb6aa8edb')
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


IGNORE_BARCODES = [
    "TARGET-50-PAJLUJ-01A-01D",  # this appears to be an old name for TARGET-50-PAJLUJ-06A-01D
]


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

def convert_index_to_row_name(index):
    row_name = ""

    while index:
        row_name += chr((index % 26) + ord('A'))
        index /= 26

    row_name = row_name[::-1]
    return row_name


def is_potential_aliquot_col(col):
    """Aliquot names can be in columns that either end in "Sample ID" or
    start with "L". The ones that start with L sometimes show up as
    unnamed after parsing becuse there's a fake row above them, but
    sometimes they show up as having the name of that fake
    row. Consider all options here to deicde if a column could have an
    aliquot in it.
    """
    return (col.endswith("Sample ID")
            or col.startswith("Unnamed")
            or col in ["Gene Expression", "mRNA-seq",
                       "miRNA-Seq", "Copy Number Analysis",
                       "Methylation", "Whole Exome Sequencing",
                       "Targeted Capture Sequencing (BCCA)"])


def split_into_aliquots(cell):
    """Almost always this just amounts to splitting on commas, but some
    are missing commas, e.g.: TARGET-20-PAEEYP-14A-01DTARGET-20-PAEEYP-03A-01D.
    """
    if cell.count("TARGET") > 1 and "," not in cell:
        # split into barcodes
        return ["TARGET" + s for s in cell.split("TARGET")[1:]]
    else:
        return cell.split(",")


def fix_suprious_dash(aliquot):
    """Some aliquots have a spurious dash on the end
    ('TARGET-20-PANVGE-09A-02R -'), remove it
    """
    if aliquot and aliquot.endswith(" -"):
        return aliquot.replace(" -", "")
    else:
        return aliquot


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
        self.ROW_CLASSES = [ "even", "odd" ]

    def find_sample_matrix_directories(self, url, url_list):
        """Walk the given url and recursively find all the sample matrix directories."""
        self.log.info("Walking %s" % url)
        r = requests.get(url, auth=self.dcc_auth, verify=False)
        if not r.raise_for_status():
            soup = BeautifulSoup(r.text, "lxml")
            file_table = soup.find('table', attrs={'id':'indexlist'})
            rows = file_table.find_all('tr')
            for row in rows:
                if row['class'][0] in self.ROW_CLASSES:
                    image = row.find('img')
                    # directory
                    if "[DIR]" in image['alt']:
                        dir_name = row.find('td', class_="indexcolname").get_text().strip()
                        # don't walk too deep
                        if (dir_name != "SAMPLE_MATRIX/") & (len(url.split('/')) < 7):
                            self.find_sample_matrix_directories(url + dir_name, url_list)
                        else:
                            if "SAMPLE_MATRIX" in dir_name:
                                url_list.append(url + dir_name)

    def locate_sample_matrices(self):
        """Given a project, set self.urls to a list of urls for it's sample
        matrices. Some projects have two sample matrices, one in
        Discovery and one in Validation.
        """
        self.urls = []
        url_list = []
        self.version = 0

        base_url = "https://target-data.nci.nih.gov/{proj}/".format(proj=self.project)
        self.find_sample_matrix_directories(base_url, url_list)
        self.log.info("%s: Found %d directories" % (self.project, len(url_list)))

        for dir in url_list:
            search_url = dir
            resp = requests.get(search_url, auth=self.dcc_auth)
            if resp.status_code == 404:
                self.log.info("No sample matrix found at %s", search_url)
                continue
            resp.raise_for_status()
            search_html = html.fromstring(resp.content)
            links = search_html.cssselect('a')
            for link in links:
                maybe_match = re.search("SampleMatrix_([0-9]{8})", link.attrib["href"])
                if maybe_match:
                    url = urljoin(search_url, link.attrib["href"])
                    version = datetime.strptime(maybe_match.group(1), "%Y%m%d").toordinal()
                    self.urls.append(url)
                    if version > self.version:
                        # the logic here is that since what we have in
                        # the database can be updated by a new version
                        # of either sample matrix, the version of the
                        # biospecemin data for this project is the
                        # later of the two sample matrix versions
                        self.version = version
        if not self.urls:
            raise RuntimeError("Could not find any sample matrices")
        if not self.version:
            raise RuntimeError("Problem parsing versions")

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

    def case_mapping(self, case_id, row, row_index):
        """Given a row, extract all the aliquots from it, group them into
        samples, add metadata, and assert various sanity checks"""

        aliquot_lists = [
            split_into_aliquots(row[col]) for col in row.index
            if is_potential_aliquot_col(col) and row.get(col)
            # this is to avoid trying to process whitespace
            and row.get(col).strip()
            and row.get(col).strip().lower() != "failed"
            and "[BCCA]" not in row.get(col)  # TODO don't know what this means, skiping it for now
        ]
        # this amounts to flattening the nested list
        aliquots = list(itertools.chain(*aliquot_lists))
        # some of them have suprious whitespace
        aliquots = [aliquot.strip() for aliquot in aliquots if aliquot.strip()]
        aliquots = filter_pending(aliquots)
        aliquots = [fix_suprious_dash(a) for a in aliquots]
        row_ok = True
        for aliquot in aliquots:
            assert len(aliquot.split("-")) == 5
            if not aliquot.startswith(case_id):
                # adding two here because that's the header offset
                self.log.warning("In Row %s: Aliquot: %s, case_id: %s" %
                    (row_index + 2, aliquot, case_id)
                )
                row_ok = False
        if row_ok:
            aliquots = [a for a in aliquots if a not in IGNORE_BARCODES]
            sample_groups = self.group_by_sample(aliquots)
            for sample in sample_groups:
                assert sample.startswith(case_id)
        else:
            sample_groups = {}
        return sample_groups, row_ok

    def compute_mapping_from_df(self, df):
        """Computes a dict of the information we need to put in the database
        from a dataframe of the project's sample matrix"""
        CASE_USI = "Case USI"
        mapping = {}
        errors_found = False
        for i, row in df.iterrows():
            case_id = row[CASE_USI]
            # TODO filter the row if the "TARGET Case" column is "N".
            # we're not sure this is actually the right column, which
            # is why I haven't done it yet
            if case_id:
                case_id = case_id.strip()
                if not case_id.startswith("TARGET"):
                    self.log.info("Skipping Case USI: %s because it doesn't start with 'TARGET-'", case_id)
                    continue
                # cursory check of format of ID
                assert len(case_id.split("-")) == 3
            else:
                continue
            if mapping.get(case_id):
                errors_found = True
                self.log.warning("{} is duplicated in this sample matrix".format(case_id))
            else:
                mapping[case_id], row_ok = self.case_mapping(case_id, row, i)
                if not row_ok:
                    errors_found = True
        return mapping, errors_found

    def sanity_check(self, mapping):
        """Sanity check the data: all samples derived from a case should
        start with that case's barcode, all aliquots derived
        from a sample should start with that sample's barcode."""
        for case, samples in mapping.iteritems():
            for sample, contents in samples.iteritems():
                assert sample.startswith(case)
                for aliquot in contents["aliquots"]:
                    assert aliquot.startswith(sample)

    def merge_samples(self, sample1, sample2):
        if sample1 is None:
            return deepcopy(sample2)
        if sample2 is None:
            return deepcopy(sample1)
        merged = {}
        merged["aliquots"] = sample1["aliquots"].union(sample2["aliquots"])
        for key in ["tissue_code", "tumor_code", "tissue_desc", "tumor_desc"]:
            assert sample1[key] == sample2[key]
            merged[key] = sample1[key]
        return merged

    def merge_mappings(self, mappings):
        final = {}
        for mapping in mappings:
            for case, samples in mapping.iteritems():
                if case not in final:
                    final[case] = deepcopy(samples)
                else:
                    # this case is duplicated
                    for sample_name, info in samples.iteritems():
                        final[case][sample_name] = self.merge_samples(
                            final[case].get(sample_name), info
                        )
        return final

    def compute_mapping(self):
        self.locate_sample_matrices()
        mappings = []
        spreadsheet_errors = False
        for url in self.urls:
            self.log.info("Retrieving %s" % url)
            resp = requests.get(url, auth=self.dcc_auth)
            df = self.load_sample_matrix(resp.content)
            mapping, errors = self.compute_mapping_from_df(df)
            if errors:
                self.log.warning("Errors found in %s" % url)
                spreadsheet_errors = True
            mappings.append(mapping)
        if spreadsheet_errors:
            self.log.error("Errors found in spreadsheets, aborting merge")
            raise RuntimeError("Errors found in spreadsheets, aborting merge")

        final_mapping = self.merge_mappings(mappings)
        self.sanity_check(final_mapping)
        return final_mapping

    def sync(self):
        self.log.info("Fetching and extracting info from sample matrix.")
        mapping = self.compute_mapping()
        self.log.info("Storing sample matrix data in database.")
        with self.graph.session_scope():
            self.put_mapping_in_pg(mapping)
            self.log.info("Removing old versions of data from this matrix.")
            self.remove_old_versions()

    def remove_old_versions(self):
        models_to_remove = [Aliquot, Case, Sample]
        for model in models_to_remove:
            q = self.graph.nodes(model)\
                          .sysan({"group_id": self.project})\
                          .filter(model._sysan["version"].cast(Integer) < self.version)
            self.log.info("Found %s old %s to remove.", q.count(), model.__name__)
            to_delete = q.all()
            for node in to_delete:
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
        for case, samples in mapping.iteritems():
            case_node = Case(
                node_id=str(uuid5(NAMESPACE_CASES, str(case))),
                submitter_id=case,
                days_to_index=None,
            )
            case_node.system_annotations = sysans
            self.log.info("creating case %s as %s",
                          case, case_node)
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
                self.log.info("tieing sample to case %s", case_node)
                case_node.samples.append(sample_node)
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
            self.log.info("inserting case %s", case_node)
            case_node = self.graph.current_session().merge(case_node)
            self.log.info("tieing %s to project", case_node)
            case_node.projects = [project_node]
