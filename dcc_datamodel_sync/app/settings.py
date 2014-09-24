import yaml, pprint, os, logging, json

class Settings:

    """
    This class is used to contain the global settings.  The
    settings will be imported on the load of the module from the
    default path.

    """

    settings = {}

    default_path = "settings.yaml"

    def lookup(self, key):
        """ 
        Insert any indirect lookups in this function
        """

        if key not in self.settings:
            logging.error("Key [{key}] was not in settings dictionary".format(key = key))
            return None

        return self.settings[key]

    def __init__(self, path = None):
        self.path = self.default_path
        self.load(path)
        
    def __call__(self, key):
        return self.lookup(key)

    def __getitem__(self, key):
        return self.lookup(key)

    def __setitem__(self, key, value):
        self.settings[key] = value
        return self

    def __repr__(self):
        return str(self.settings)

    def load(self, path = None):

        if path is None and self.path is None:
            logging.error("Unable to load settings, no path specified.")
            return self
        
        if path is not None: 
            logging.debug("Updating settings file path {path}".format(path = path))
            self.path = path

        logging.info("Loading settings file {path}".format(path = path))

        try:
            with open(self.path, 'r') as yaml_file:
                self.settings = yaml.load(yaml_file)
        except Exception, msg:
            logging.error("Unable to load settings from {path}: {msg}".format(path = path, msg = str(msg)))
            logging.info("Proceeding with no settings")
        else:
            logging.info("Successfully loaded settings from {path}.".format(path = path))
            logging.debug(self)
            
        return self
