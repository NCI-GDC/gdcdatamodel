from cStringIO import StringIO
import tempfile
import tarfile
import re
import hashlib
import uuid
from contextlib import contextmanager
from urlparse import urlparse
from functools import partial
import copy
import os

from lxml import html

import requests

from libcloud.storage.drivers.s3 import S3StorageDriver
from libcloud.storage.drivers.cloudfiles import OpenStackSwiftStorageDriver
from libcloud.storage.drivers.local import LocalStorageDriver

from psqlgraph import PsqlNode, PsqlEdge, session_scope
from psqlgraph.validate import AvroNodeValidator, AvroEdgeValidator

from gdcdatamodel import node_avsc_object, edge_avsc_object

from cdisutils.log import get_logger

from zug.datamodel import classification


def md5sum(iterable):
    md5 = hashlib.md5()
    for chunk in iterable:
        md5.update(chunk)
    return md5.hexdigest()


def iterable_from_file(fileobj, chunk_size=8192):
    return iter(partial(fileobj.read, chunk_size), '')


def fix_barcode(s):
    """Munge barcodes matched from filenames into correct format"""
    return s.replace("_", "-").upper()


def fix_uuid(s):
    """Munge uuids matched from filenames into correct format"""
    return s.replace("_", "-").lower()

CLASSIFICATION_ATTRS = ["data_subtype", "data_format", "platform",
                        "experimental_strategy", "tag"]

DATA_TYPES = {
    "Copy number variation": [
        "Copy number segmentation", "Copy number estimate",
        "Normalized intensities", "Copy number germline variation",
        "LOH", "Copy number QC metrics"
    ],
    "Simple nucleotide variation": [
        "Simple germline variation", "Genotypes", "Simple somatic mutation",
        "Simple nucleotide variation"
    ],
    "Gene expression": [
        "Gene expression quantification", "miRNA quantification",
        "Isoform expression quantification", "Exon junction quantification",
        "Exon quantification"
    ],
    "Protein expression": [
        "Protein expression quantification", "Expression control"
    ],
    "Raw microarray data": [
        "Raw intensities", "Normalized intensities", "CGH Array QC",
        "Intensities Log2Ratio", "Expression control"
    ],
    "DNA methylation": [
        "Methylation beta value", "Bisulfite sequence alignment",
        "Methylation percentage"
    ],
    "Clinical": [
        "Tissue slide image", "Clinical data", "Biospecimen data",
        "Diagnostic image", "Pathology report"
    ],
    "Structural rearrangement": [
        "Structural germline variation"
    ],
    "Raw sequencing data": [
        "Coverage WIG", "Sequencing tag", "Sequencing tag counts"
    ],
    "Other": [
        "Microsattelite instability", "ABI sequence trace", "HPV Test",
        "Test description", "Auxiliary test"
    ]
}

TAGS = [
    "meth", "hg18", "hg19", "germline", "snv", "exon", "image",
    "miRNA", "ismpolish", "byallele", "raw", "tangent", "isoform",
    "coverage", "junction", "cnv", "msi", "cgh", "qc", "sif",
    "auxiliary", "QA", "lowess_normalized_smoothed", "BioSizing",
    "segmentation", "omf", "sv", "control", "tr", "alleleSpecificCN",
    "pairedcn", "segmented", "somatic", "DGE", "Tag", "bisulfite",
    "indel", "portion", "sample", "cqcf", "follow_up", "aliquot",
    "analyte", "protocol", "diagnostic_slides", "slide", "FIRMA",
    "gene", "patient", "nte", "radiation", "drug", "B_Allele_Freq",
    "Delta_B_Allele_Freq", "Genotypes", "LOH", "Normal_LogR",
    "Paired_LogR", "seg", "segnormal", "Unpaired_LogR", "MSI", "hpv",
    "cov"
]

DATA_FORMATS = [
    "TXT", "VCF", "SVS", "idat", "CEL", "XML", "WIG",
    "PDF", "TIF", "TSV", "FSA", "SIF", "JPG", "PNG",
    "dat", "Biotab", "FA", "TR", "MAF", "BED", "DGE-Tag",
    "HTML", "MAGE-Tab"
]

EXPERIMENTAL_STRATEGIES = [
    "Genotyping array", "RNA-Seq", "Methylation array", "DNA-Seq",
    "CGH array", "miRNA-Seq", "Protein expression array",
    "Gene expression array", "MSI-Mono-Dinucleotide Assay", "miRNA expression array",
    "WXS", "WGS", "Exon array", "Total RNA-Seq", "Mixed strategies",
    "Capillary sequencing", "Bisulfite-Seq"
]

PLATFORMS = [
    "Affymetrix SNP Array 6.0", "Illumina HiSeq", "Illumina GA",
    "Hospital Record", "Illumina Human Methylation 450",
    "MDA_RPPA_Core", "BCR Record", "HG-CGH-415K_G4124A",
    "Illumina Human Methylation 27", "HG-CGH-244A", "ABI capillary sequencer",
    "AgilentG4502A_07_3", "CGH-1x1M_G4447A", "HT_HG-U133A",
    "Illumina Human 1M Duo", "H-miRNA_8x15Kv2", "Illumina HumanHap550",
    "H-miRNA_8x15K", "AgilentG4502A_07_2", "HuEx-1_0-st-v2",
    "HG-U133_Plus_2", "Illumina DNA Methylation OMA003 CPI",
    "Illumina DNA Methylation OMA002 CPI", "AgilentG4502A_07_1", "ABI SOLiD",
    "Mixed platforms"
]


def idempotent_insert(driver, label, name, session):
        node = driver.node_lookup_one(label=label,
                                      property_matches={"name": name},
                                      session=session)
        if not node:
            node = driver.node_insert(PsqlNode(node_id=str(uuid.uuid4()),
                                               label=label,
                                               properties={"name": name}),
                                      session=session)
        return node


def insert_classification_nodes(driver):
    with driver.session_scope() as session:
        for data_type, subtypes in DATA_TYPES.iteritems():
            type_node = idempotent_insert(driver, "data_type", data_type, session)
            for subtype in subtypes:
                subtype_node = idempotent_insert(driver, "data_subtype", subtype,
                                                 session)
                edge = driver.edge_lookup_one(src_id=subtype_node.node_id,
                                              dst_id=type_node.node_id,
                                              label="member_of",
                                              session=session)
                if not edge:
                    edge = PsqlEdge(src_id=subtype_node.node_id,
                                    dst_id=type_node.node_id,
                                    label="member_of")
                    driver.edge_insert(edge, session=session)
        for tag in TAGS:
            idempotent_insert(driver, "tag", tag, session)
        for platform in PLATFORMS:
            idempotent_insert(driver, "platform", platform, session)
        for strat in EXPERIMENTAL_STRATEGIES:
            idempotent_insert(driver, "experimental_strategy", strat, session)
        for format in DATA_FORMATS:
            idempotent_insert(driver, "data_format", format, session)


def classify(archive, filename):
    """Given a filename and an archive that it came from, attempt to
    classify it. Return a dictionary representing the
    classification.
    """
    data_type = archive["data_type_in_url"]
    data_level = str(archive["data_level"])
    platform = archive["platform"]
    potential_classifications = classification[data_type][data_level][platform]
    for possibility in potential_classifications:
        match = re.match(possibility["pattern"], filename)
        if match:
            result = copy.deepcopy(possibility["category"])
            result["data_format"] = possibility["data_format"]
            if possibility.get("captured_fields"):
                for i, field in enumerate(possibility["captured_fields"]):
                    if field not in ['_', '-']:
                        if field.endswith("barcode"):
                            result[field] = fix_barcode(match.groups()[i])
                        elif field.endswith("uuid"):
                            result[field] = fix_uuid(match.groups()[i])
                        else:
                            result[field] = match.groups()[i]
            return result
    raise RuntimeError("file {}/{} failed to classify".format(archive["archive_name"], filename))


class TCGADCCArchiveSyncer(object):

    MAX_BYTES_IN_MEMORY = 2 * (10**9)  # 2GB TODO make this configurable
    SIGNPOST_VERSION = "v0"

    def __init__(self, signpost_url, pg_driver, dcc_auth, scratch_dir):
        self.signpost_url = signpost_url
        self.pg_driver = pg_driver
        self.pg_driver.node_validator = AvroNodeValidator(node_avsc_object)
        self.pg_driver.edge_validator = AvroEdgeValidator(edge_avsc_object)
        self.dcc_auth = dcc_auth
        self.scratch_dir = scratch_dir
        self.log = get_logger("tcga_dcc_sync_" + str(os.getpid()))

    def put_archive_in_pg(self, archive, session):
        # legacy_id is just the name without the revision or series
        # this will be identical between different versions of an archive as new
        # versions are submitted
        legacy_id = re.sub("\.(\d+?)\.(\d+)$", "", archive["archive_name"])
        self.log.info("looking for archive %s in postgres", archive["archive_name"])
        maybe_this_archive = self.pg_driver.node_lookup_one(label="archive",
                                                            property_matches={"legacy_id": legacy_id,
                                                                              "revision": archive["revision"]})
        if maybe_this_archive:
            self.log.info("found archive %s in postgres, not inserting", archive["archive_name"])
            return maybe_this_archive
        self.log.info("looking up old versions of archive %s in postgres", legacy_id)
        old_versions = self.pg_driver.node_lookup(label="archive",
                                                  property_matches={"legacy_id": legacy_id},
                                                  session=session).all()
        if len(old_versions) > 1:
            # since we void all old versions of an archive when we add a new one,
            # there should never be more than one old version in the database
            raise ValueError("multiple old versions of an archive found")
        if old_versions:
            old_archive = old_versions[0]
            self.log.info("old revision (%s) of archive %s found, voiding it and associated files",
                          old_archive.properties["revision"],
                          legacy_id)
            # TODO it would be awesome to verify that the changes we make actually match what's in
            # CHANGES_DCC.txt,
            # first get all the files related to this archive and void them
            for file in self.pg_driver.node_lookup(label="file", session=session)\
                                      .with_edge_to_node("member_of", old_archive)\
                                      .all():
                self.log.info("voiding file %s", str(file))
                self.pg_driver.node_delete(node=file, session=session)
            self.pg_driver.node_delete(node=old_archive, session=session)
        new_archive_node = PsqlNode(
            node_id=self.allocate_id_from_signpost(),
            label="archive",
            properties={"legacy_id": legacy_id,
                        "revision": archive["revision"]})
        self.log.info("inserting new archive node in postgres: %s", str(new_archive_node))
        session.add(new_archive_node)
        return new_archive_node

    def sync_archives(self, archives):
        for i, archive in enumerate(archives):
            self.log.info("syncing archive %s of %s", i+1, len(archives))
            self.sync_archive(archive)

    def lookup_file_in_pg(self, archive_node, filename, session):
        q = self.pg_driver.node_lookup(label="file",
                                       property_matches={"file_name": filename},
                                       session=session)\
                          .with_edge_to_node("member_of", archive_node)
        file_nodes = q.all()
        if not file_nodes:
            return None
        if len(file_nodes) > 1:
            raise ValueError("multiple files with the same name found in archive {}".format(archive_node))
        else:
            return file_nodes[0]

    def allocate_id_from_signpost(self):
        """Retrieve a new empty did from signpost."""
        resp = requests.post("/".join([self.signpost_url,
                                       self.SIGNPOST_VERSION,
                                       "did"]),
                             json={"urls": []})
        resp.raise_for_status()
        return resp.json()["did"]

    def tie_file_to_atribute(self, file_node, attr, value, session):
        LABEL_MAP = {
            "platform": "generated_from",
            "data_subtype": "member_of",
            "data_format": "member_of",
            "tag": "member_of",
            "experimental_strategy": "member_of"
        }
        if not isinstance(value, list):
            # this is to handle the thing where tag is
            # sometimes a list and sometimes a string
            value = [value]
        for val in value:
            attr_node = self.pg_driver.node_lookup_one(label=attr,
                                                       property_matches={"name": val},
                                                       session=session)
            if not attr_node:
                self.log.error("attr_node with label %s and name %s not found (trying to tie for file %s) ", attr, val, file_node["file_name"])
            edge_to_attr_node = PsqlEdge(label=LABEL_MAP[attr],
                                         src_id=file_node.node_id,
                                         dst_id=attr_node.node_id)
            self.pg_driver.edge_insert(edge_to_attr_node, session=session)

    def store_file_in_pg(self, archive_node, filename, md5, md5_source,
                         file_classification, session):
        # not there, need to get id from signpost and store it.
        did = self.allocate_id_from_signpost()
        acl = ["phs000178"] if file_classification["data_access"] == "protected" else []
        system_annotations = {"md5_source": md5_source,
                              "file_source": "tcga_dcc"}
        for k, v in file_classification.iteritems():
            if k.startswith("_"):
                system_annotations[k] = v
        file_node = PsqlNode(node_id=did, label="file", acl=acl,
                             properties={"file_name": filename,
                                         "md5sum": md5,
                                         "state": "submitted",
                                         "state_comment": None},
                             system_annotations={"md5_source": md5_source,
                                                 "file_source": "tcga_dcc"})
        edge_to_archive = PsqlEdge(label="member_of",
                                   src_id=file_node.node_id,
                                   dst_id=archive_node.node_id,
                                   properties={})
        self.log.info("inserting file %s as node %s", filename, file_node)
        self.pg_driver.node_insert(file_node, session=session)
        self.pg_driver.edge_insert(edge_to_archive, session=session)
        # ok, classification
        #
        # we need to create edges to: data_subtype, data_format,
        # platform, experimental_strategy, tag.
        for attribute in CLASSIFICATION_ATTRS:
            if file_classification.get(attribute):
                self.tie_file_to_atribute(file_node, attribute,
                                          file_classification[attribute],
                                          session)
            else:
                self.log.warning("not tieing %s (node %s) to a %s", filename, file_node, attribute)
        return file_node

    def set_file_state(self, file_node, state):
        self.pg_driver.node_update(file_node, properties={"state": state})

    def sync_file(self, archive, archive_node, filename, dcc_md5, session):
        """Sync this file in the database, classifying it and"""
        file_classification = classify(archive, filename)
        if ("to_be_determined" in file_classification.values() or
            "data_access" not in file_classification.keys()):
            # we shouldn't insert this file
            self.log.info("file %s/%s classified as %s, not inserting",
                          archive["archive_name"],
                          filename,
                          file_classification)
            return
        file_node = self.lookup_file_in_pg(archive_node, filename, session)
        if not file_node:
            md5_source = "tcga_dcc"
            file_node = self.store_file_in_pg(archive_node, filename,
                                              dcc_md5, md5_source,
                                              file_classification,
                                              session)
        else:
            self.log.info("file %s in archive %s already in postgres, not inserting",
                          filename,
                          archive_node.properties["legacy_id"])

    def get_manifest(self, archive):
        resp = self.get_with_auth("/".join([archive["non_tar_url"], "MANIFEST.txt"]))
        manifest_data = resp.content
        res = {}
        for line in manifest_data.splitlines():
            md5, filename = line.split()
            res[filename] = md5
        return res

    def get_with_auth(self, url, **kwargs):
        resp = requests.get(url, auth=self.dcc_auth,
                            allow_redirects=False, **kwargs)
        tries = 0
        while resp.is_redirect and tries < 5:
            # sometimes it redirects, try again. normally requests
            # does this automatically, but this doesn't work with auth
            tries += 1
            resp = requests.get(resp.headers["location"], auth=self.dcc_auth,
                                allow_redirects=False, **kwargs)
        # ENTERING GROSS HACK ZONE
        #
        # sometimes it just returns a 401 (no redirect) when you're
        # trying to hit tcga-data but you want
        # tcga-data-secure. somehow Chrome manages to figure this out,
        # I think it has something to do with cookies. In any case I
        # do it manually here.
        if resp.status_code == 401:
            fixed_url = re.sub("tcga-data", "tcga-data-secure", url)
            resp = requests.get(fixed_url, auth=self.dcc_auth,
                                allow_redirects=False, **kwargs)
        # EXITING GROSS HACK ZONE
        return resp

    def fetch_files(self, archive):
        NOT_PART_OF_ARCHIVE = ["Name", "Last modified", "Size", "Parent Directory"]
        resp = self.get_with_auth(archive["non_tar_url"])
        archives_html = html.fromstring(resp.content)
        return [elem.text for elem in archives_html.cssselect('a')
                if elem.text not in NOT_PART_OF_ARCHIVE]

    def sync_archive(self, archive):
        if archive["disease_code"] == "FPPP":
            self.log.info("%s is an FPPP archive, skipping", archive["archive_name"])
        self.log.info("syncing archive %s", archive["archive_name"])
        archive["non_tar_url"] = re.sub("\.tar\.gz$", "", archive["dcc_archive_url"])
        with self.pg_driver.session_scope() as session:
            archive_node = self.put_archive_in_pg(archive, session)
            try:
                manifest = self.get_manifest(archive)
            except:
                self.log.exception("error while parsing manifest on archive %s, marking as such and moving on", archive["archive_name"])
                self.pg_driver.node_update(archive_node,
                                           system_annotations={"manifest_problems": True},
                                           session=session)
                return
            filenames = self.fetch_files(archive)
            for filename in filenames:
                if filename != "MANIFEST.txt":
                    if manifest.get(filename):
                        self.sync_file(archive, archive_node, filename, manifest[filename], session)
                    else:
                        self.log.warning("manifest in archive %s does not have md5 for file %s, marking archive has having problems",
                                         archive["archive_name"], filename)
                        self.pg_driver.node_update(archive_node,
                                                   system_annotations={"manifest_problems": True},
                                                   session=session)
