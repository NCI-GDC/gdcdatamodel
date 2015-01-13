import os
import yaml
import xml2psqlgraph

PKG_DIR = os.path.dirname(os.path.abspath(__file__))

classification = yaml.load(open(os.path.join(PKG_DIR, 'classification.yaml')).read())
