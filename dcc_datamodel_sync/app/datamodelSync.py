import logging, imp, os, sys

from app import settings, baseDir

class DatamodelSync:

    def __init__(self):
        logging.debug("New DatamodelSync instance")

        self.plugins = settings['plugins']

        # Caching for imported and converted docs
        self.imported = {}
        self.converted = {}

        # Load the plugins
        self.schedulers  = self.loadPlugins('schedulers')
        self.conversions = self.loadPlugins('conversions')
        self.exports     = self.loadPlugins('exports')

        assert len(self.schedulers) > 0, "No scheduler plugins were set"
        assert len(self.conversions) > 0, "No conversion plugins were set"
        assert len(self.exports) > 0, "No export plugins were set"

        logging.info("Running with schedulers {plugins}".format(plugins = self.schedulers))
        logging.info("Running with conversions {plugins}".format(plugins = self.conversions))
        logging.info("Running with exports {plugins}".format(plugins = self.exports))

    def loadPlugins(self, pluginType):

        assert pluginType in self.plugins, "No plugins of type [{ptype}] defined!".format(ptype = pluginType)
        assert pluginType in self.plugins['paths'], "No path to plugins of type [{ptype}] defined!".format(ptype = pluginType)

        plugins = []

        for plugin in self.plugins[pluginType]:

            logging.info("Loading plugin: [{ptype}] {plugin}".format(ptype = pluginType, plugin = plugin))

            # Build plugin file path
            pluginDir = self.plugins['paths']['schedulers']
            pluginPath = os.path.join(baseDir, pluginDir, "{plugin}.py".format(plugin = plugin))
            logging.info("Looking for plugin at {path}".format(path = pluginPath))

            # Attempt to import the plugin and add to the list
            try:
                plugins.append(imp.load_source(plugin, pluginPath))
            except Exception, msg:
                logging.error("Unable to load plugin: [{ptype}] {plugin}: {msg}".format(ptype = pluginType, plugin = plugin, msg = str(msg)))
            else:
                logging.info("Plugin loaded successfully: [{ptype}] {plugin}".format(ptype = pluginType, plugin = plugin))

        # Returns a list of plugin objects
        return plugins

    def schedule(self, scheduler):
        logging.info("Scheduling work using {plugin}".format(plugin = scheduler))
        
        
        
        # for conversion in self.conversions:
            
        

    def start(self):
        logging.info("Starting DatamodelSync")
        
        for scheduler in self.schedulers:
            self.schedule(scheduler)
        
            
            
