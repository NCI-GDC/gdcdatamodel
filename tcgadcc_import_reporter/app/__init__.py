import os
import logging
import yaml

baseDir = os.path.dirname(os.path.realpath(__file__))

#TODO use the Settings module
settingsDir = os.path.dirname(baseDir)
settingsFile = os.path.join(settingsDir, 'settings.yaml')
settings = yaml.load(open(settingsFile).read())
