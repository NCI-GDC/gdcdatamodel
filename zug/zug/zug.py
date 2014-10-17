import logging
import imp
import os
import sys
import pprint
import copy

import exceptions

baseDir = os.path.dirname(os.path.realpath(__file__))
logger = logging.getLogger(name = "[{name}]".format(name=__name__))
pluginBasePath = os.path.join(os.path.dirname(baseDir), 'plugins', 'base.py')
basePlugin = imp.load_source('ZugPluginBase', pluginBasePath).ZugPluginBase

decoratables = ['next', 'initialize', '__iter__', 'start']
DECORATOR_PREFIX = 'zug_'

class Zug:

    def __init__(self, settings):
        """
        A new instance to extract, transform, and load data
        """
        
        logger.info("New ETL instance: " + str(self))

        self.settings = settings
        self.pluginTree = self.settings.get('plugins', [])
        self.plugins = []

        logger.info("Loading plugins ... ")
        self.loadPluginTree()

        logger.info("Loading modules ...")
        self.loadModules()

    def loadPluginTree(self):
        """ 
        Walk the plugin tree provided in the settings and create TreeLevel PluginTreeLevel objects from them
        """

        logger.info("Loading Plugins... ")
        for plugin in self.pluginTree:
            self.plugins.append(PluginTreeLevel(self, plugin))

        return self

    def loadModules(self):
        """ 
        Walk the PluginTreeLevel tree create instances of PiplinePlugins objects from them
        """

        logger.info("Loading Modules... ")
        for plugin in self.plugins:
            plugin.loadModules()
        return self

    def run(self):
        logger.info("Starting ETL...")

        for plugin in self.plugins:
            logger.info('Running plugin {plugin}'.format(plugin = plugin))
            plugin.run(None)

class PluginTreeLevel:

    """ 
    Represents a node in a plugin tree
    """

    def __init__(self, zug, tree = None):
        self.zug = zug
        self.name = self.zug.settings.get('name', None)
        self.children = []
        self.module = None
        self.current = None
        self.plugin = None

        if self.name: logger.info("Initializing ETL: {name}".format(name = self.name))

        if isinstance(tree, str):
            self.name = tree
        else:
            self.name = tree.keys()[0]
            self.loadTree(tree[self.name])

    def addChild(self, TreeLevel):
        """ 
        Adds a child node to current level 
        """

        self.children.append(TreeLevel)

    def __repr__(self):
        """
        String representation
        """

        return "<PluginTreeLevel '{name}'>".format(name = self.name)

    def dump(self, level = 0):
        """
        Print text representation of tree to stdout
        """

        print '| '*level + '+ ' + self.name
        for child in self.children:
            child.dump(level + 1)

    def run(self, __doc__, **__state__):
        """
        Passes a document down the tree, loading it into each of TreeLevel in the next level
        """

        logger.debug('Running ' + str(self))
        if self.plugin is None: return self

        # Give the plugin a document and start it
        self.plugin.load(__doc__, **__state__)
        self.plugin.start()
 
        # Iterate through plugins __doc__uments
        for __doc__ in self.plugin:
            # Pass each document to the next TreeLevel
            child_doc = copy.deepcopy(__doc__)
            child_state = copy.deepcopy(self.plugin.state)

            for child in self.children:
                logger.debug('Passing to ' + child.name)
                child.run(child_doc, **child_state)

        self.plugin.close()
        

    def loadTree(self, tree, root = None):
        """
        Loads a python pluginstructure, assuming that it is a list,
        dictionary (or single name) containing the names of plugins
        """

        # Will be none if first level
        if root is None: root = self

        # End of recursion
        if isinstance(tree, str):
            root.addChild(PluginTreeLevel(self.zug, tree))
            return self

        # Handle the case where we were passed a list
        elif isinstance(tree, list):
            for node in tree:
                root.addChild(PluginTreeLevel(self.zug, node))
            return self

        elif not isinstance(tree, dict):
            logger.error("Unable to load tree from type {type}".format(type=type(tree)))

        # Recurse over dictionary
        for node, children in tree.iteritems():
            for child in children:
                curr = PluginTreeLevel(self.zug, node)
                root.addChild(curr)
                self.loadTree(child, curr)

        return self

    def loadModule(self, name, pluginPath = None, directory = None):
        """
        Attempts to load a module of a given name
        """

        if directory and not os.path.isabs(directory):
            pluginPath = os.path.join(baseDir, directory, '{name}.py'.format(name=name))
        elif directory:
            pluginPath = os.path.join(directory, '{name}.py'.format(name=name))

        logger.info("Looking for {plugin} at: {path}".format(plugin=name, path=pluginPath))

        # Attempt to import and add the plugin
        try:
            module = imp.load_source(name, pluginPath)
        except Exception, msg:
            logger.warn("Unable to load plugin: {plugin}: {msg}".format(plugin=name, msg=str(msg)))
            return None
        else:
            logger.info("SUCCESS: Loaded plugin: {plugin}".format(plugin=name))

        return module


    def initializeModule(self):
        """
        Attempt to load class from module and initialize plugin
        """

        logger.info('Initializing {name}'.format(name = self.name))

        # Attempt to load as class
        try:
            className = self.zug.settings.get('plugin_class_names', {}).get(self.name, self.name)
            pluginClass = getattr(self.module, className)
            self.plugin = pluginClass(**self.zug.settings.get('plugin_kwargs', {}).get(self.name, {}))
            return 

        except Exception, msg:
            logger.info("Unable to load plugin as class. Attempting to use decorators: " + str(msg))

        # Attempt to load by decorators
        try:
            self.plugin = basePlugin(**self.zug.settings.get('plugin_kwargs', {}).get(self.name, {}))

            # Check the module for functions handled by decorators
            for dec in decoratables:
                attr = DECORATOR_PREFIX + dec
                decs = [a for a in dir(self.module) if hasattr(self.module.__dict__[a], attr)]

                # Override the base class functions
                if len(decs) > 0: self.plugin.__dict__[dec] = self.module.__dict__[decs[-1]]
            return 

        except Exception, msg:
            logger.info("Unable to load plugin by decorators: " + str(msg))
            raise

        else:
            return

        raise Exception("Unable to load plugin by class or decorators")

    def loadModules(self):
        """
        Attempts to load all modules within Plugin tree
        """

        pluginPath = self.zug.settings.get('plugin_paths', None).get(self.name, None)
        pluginDirs = ['../plugins'] + self.zug.settings.get('plugin_directories', []) 

        if pluginPath is not None:
            self.module = self.loadModule(self.name, pluginPath)

        else:
            # Try and find the module in any provided directory
            for pluginDir in pluginDirs:
                logger.info("Looking for plugin {name} in {dir}".format(name=self.name, dir=pluginDir))
                if not os.path.isabs(pluginDir): pluginDir = os.path.join(baseDir, pluginDir)
                self.module = self.loadModule(self.name, directory = pluginDir)
                if self.module is not None: break

        # Bail if we failed to load the module
        if self.module is None:
            raise Exception("Unable to load plugin: {plugin}.".format(plugin=self.name))
            return self

        # Walk tree for children modules
        for child in self.children:
            child.loadModules()
        try:
            self.initializeModule()
        except Exception, msg:
            raise Exception('Initialization of {name} failed: {msg}'.format(name=self.name, msg=str(msg)))
        else:
            logger.info('Initialization of {name} complete.'.format(name=self.name))

        return self
