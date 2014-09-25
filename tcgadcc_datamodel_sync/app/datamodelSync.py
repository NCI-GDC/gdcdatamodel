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

        plugins = {}

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
                plugins[plugin] = self.initializePlugin(module, plugin, pluginType)
            except Exception, msg:
                logging.error("Unable to load plugin: [{ptype}] {plugin}: {msg}".format(ptype = pluginType, plugin = plugin, msg = str(msg)))
            else:
                logging.info("SUCCESS: Loaded plugin: [{ptype}] {plugin}".format(ptype = pluginType, plugin = plugin))

        # Returns a list of plugin objects
        return plugins

    def run(self):
        logging.info("Starting DatamodelSync")

        # Pass to schedulers
        for schedulerPlugin, scheduler in self.schedulers.iteritems():
            self.schedule(scheduler, schedulerPlugin = schedulerPlugin)
        
    def schedule(self, scheduler, **kwargs):
        logging.info("Scheduling work with {scheduler}".format(scheduler = scheduler))

        # Tell the scheduler to start 
        scheduler.load()

        # Convert all docs
        for doc, schedulerDetails in scheduler:
            for conversionPlugin, conversion in self.conversions.iteritems():
                self.convert(conversion, doc, schedulerDetails, conversionPlugin = conversionPlugin, **kwargs)
            
    def convert(self, conversion, doc, schedulerDetails, **kwargs):
        logging.debug("Starting conversion with {conversion}".format(conversion = conversion))

        # Convert the doc
        converted, conversionDetails = conversion._convert(doc, schedulerDetails, **kwargs)

        # Pass doc to exporters
        for exportPlugin, export in self.exports.iteritems():
            self.export(export, converted, schedulerDetails, conversionDetails, exportPlugin = exportPlugin, **kwargs)

    def export(self, exporter, doc, schedulerDetails, conversionDetails, **kwargs):
        logging.debug("Starting export with {exporter}".format(exporter = exporter))

        # Export the doc
        exporter._export(doc, schedulerDetails, conversionDetails, **kwargs)
