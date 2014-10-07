import yaml, pprint, os, logging, json
logger = logging.getLogger(name = "[{name}]".format(name = __name__))

class Settings:

    """
    This class is used to contain the global settings.  The
    settings will be imported on the load of the module from the
    default path.

    """

    settings = {}

    default_path = "settings.yaml"

    def get(self, key, default = None):
        """ 
        Insert any indirect lookups in this function
        """

        if key not in self.settings:
            logger.debug("Settings: Key [{key}] was not in settings dictionary".format(key = key))
            return default

        return self.settings[key]

    def __init__(self, path = None):
        self.path = self.default_path
        self.load(path)
        
    def __call__(self, key, default = None):
        return self.get(key, defailt = None)

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        self.settings[key] = value
        return self

    def __repr__(self):
        return str(self.settings)

    def load(self, path = None):

        if path is None and self.path is None:
            logger.error("Unable to load settings, no path specified.")
            return self
        
        if path is not None: 
            logger.debug("Updating settings file path {path}".format(path = path))
            self.path = path

        logger.info("Loading settings file {path}".format(path = path))

        try:
            with open(self.path, 'r') as yaml_file:
                self.settings = yaml.load(yaml_file)
        except Exception, msg:
            logger.error("Unable to load settings from {path}: {msg}".format(path = path, msg = str(msg)))
            logger.info("Proceeding with no settings")
        else:
            logger.info("SUCCESS: loaded settings from {path}.".format(path = path))
            logger.debug(self)
            
        return self
