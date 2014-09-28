import os
import yaml

__PACKAGE = os.path.dirname(__file__)

# Load settings file.
settings = os.path.join(__PACKAGE,'settings.yaml')
settings = yaml.load(open(settings).read())

from bam_validator import validate
