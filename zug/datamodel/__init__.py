import os
import yaml
import xml2psqlgraph
from pkg_resources import resource_string


PKG_DIR = os.path.dirname(os.path.abspath(__file__))

tcga_classification = yaml.load(open(os.path.join(PKG_DIR, 'tcga_classification.yaml')).read())

bcr_xml_mapping_path = os.path.join(PKG_DIR, 'bcr.yaml')
cghub_xml_mapping_path = os.path.join(PKG_DIR, 'cghub.yaml')
clinical_xml_mapping_path = os.path.join(PKG_DIR, 'clinical.yaml')
cghub_categorization_xml_mapping_path = os.path.join(PKG_DIR, 'cghub_file_categorization.yaml')

with open(bcr_xml_mapping_path, 'r') as f:
    bcr_xml_mapping = yaml.load(f.read())
with open(cghub_xml_mapping_path, 'r') as f:
    cghub_xml_mapping = yaml.load(f.read())

clinical_xml_mapping = yaml.load(resource_string('gdcdatamodel', 'xml_mappings/tcga_clinical.yaml'))

with open(cghub_categorization_xml_mapping_path, 'r') as f:
    cghub_categorization_mapping = yaml.load(f.read())
