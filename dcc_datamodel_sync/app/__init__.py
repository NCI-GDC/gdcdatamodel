import os

from settings import Settings

baseDir   = os.path.dirname(os.path.realpath(__file__))
settingsDir  = os.path.dirname(baseDir)
settingsFile = 'settings.yaml'
settingsPath = os.path.join(settingsDir, settingsFile)

settings = Settings(settingsPath)
