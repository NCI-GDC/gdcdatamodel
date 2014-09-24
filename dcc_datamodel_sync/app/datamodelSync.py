import logging, imp, os, sys

from app import settings, baseDir

class DatamodelSync:

    def __init__(self):
        logging.debug("New DatamodelSync instance")

        self.plugins = settings['plugins']

        # Load the plugins
        self.schedulers  = self.loadPlugins('schedulers')
        self.conversions = self.loadPlugins('conversions')
        self.exports     = self.loadPlugins('exports')

        assert len(self.schedulers) > 0, "No scheduler plugins were loaded."
        assert len(self.conversions) > 0, "No conversion plugins were loaded."
        assert len(self.exports) > 0, "No export plugins were loaded."

        logging.info("Running with schedulers {plugins}".format(plugins = self.schedulers))
        logging.info("Running with conversions {plugins}".format(plugins = self.conversions))
        logging.info("Running with exports {plugins}".format(plugins = self.exports))

    def initializePlugin(self, module, name, pluginType):
        
        kwargs = {}
        if name in settings.settings:
            kwargs = settings[name]
            
        if pluginType == "schedulers":
            return module.Scheduler(**kwargs)
        elif pluginType == "conversions":
            return module.Conversion(**kwargs)
        elif pluginType == "exports":
            return module.Export(**kwargs)

        return None

    def loadPlugins(self, pluginType):

        assert pluginType in self.plugins, "No plugins of type [{ptype}] defined!".format(ptype = pluginType)
        assert pluginType in self.plugins['paths'], "No path to plugins of type [{ptype}] defined!".format(ptype = pluginType)

        plugins = []

        # Loop over all plugins for given type
        for plugin in self.plugins[pluginType]:

            logging.info("Loading plugin: [{ptype}] {plugin}".format(ptype = pluginType, plugin = plugin))

            # Build plugin file path
            pluginDir = self.plugins['paths'][pluginType]
            pluginPath = os.path.join(baseDir, pluginDir, "{plugin}.py".format(plugin = plugin))
            logging.info("Looking for plugin at {path}".format(path = pluginPath))

            # Attempt to import the plugin and add to the list
            try:
                module = imp.load_source(plugin, pluginPath)
                plugins.append(self.initializePlugin(module, plugin, pluginType))
            except Exception, msg:
                logging.error("Unable to load plugin: [{ptype}] {plugin}: {msg}".format(ptype = pluginType, plugin = plugin, msg = str(msg)))
            else:
                logging.info("SUCCESS: Loaded plugin: [{ptype}] {plugin}".format(ptype = pluginType, plugin = plugin))

        # Returns a list of plugin objects
        return plugins

    def run(self):
        logging.info("Starting DatamodelSync")
        
        for scheduler in self.schedulers:
            self.schedule(scheduler)
        
    def schedule(self, scheduler):
        logging.info("Scheduling work with {scheduler}".format(scheduler = scheduler))

        scheduler.load()

        for doc in scheduler:
            for conversion in self.conversions:
                self.convert(conversion, doc)
            
    def convert(self, conversion, doc):
        logging.debug("Starting conversion with {conversion}".format(conversion = conversion))

        converted = conversion.convert(doc)

        # print converted
        for export in self.exports:
            self.export(export, converted)

    def export(self, exporter, doc):
        logging.debug("Starting export with {exporter}".format(exporter = exporter))
        exporter.export(doc)
