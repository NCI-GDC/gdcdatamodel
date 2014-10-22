import logging
import imp
import os
import sys
import pprint
import copy
import time

import exceptions
from threading import Thread
from multiprocessing import Process
from multiprocessing import JoinableQueue as Queue
import threading

baseDir = os.path.dirname(os.path.realpath(__file__))
logger = logging.getLogger(name = "[{name}]".format(name=__name__))
pluginBasePath = os.path.join(os.path.dirname(baseDir), 'plugins', 'base.py')
basePlugin = imp.load_source('ZugPluginBase', pluginBasePath).ZugPluginBase

decoratables = ['process', 'initialize']
DECORATOR_PREFIX = 'zug_'


class PluginTreeLevel:

    """ 
    Represents a node in a plugin tree
    """

    def __init__(self, zug, tree = None):

        self.zug  = zug

        self.processes = []
        self.children  = []

        self.parent    = None
        self.module    = None
        self.current   = None
        self.plugin    = None
        self.block     = True

        if isinstance(tree, str):
            self.name = tree
        else:
            self.name = tree.keys()[0]
            self.loadTree(tree[self.name])

        self.q_new_work = Queue(zug.settings.get('queue_len', {}).get(self.name, 10))
        self.qs_finished_work = []

        if self.name: logger.info("Initializing ETL: {name}".format(name = self.name))

    def loadTree(self, tree, root = None):
        """
        Loads a python pluginstructure, assuming that it is a list,
        dictionary (or single name) containing the names of plugins
        """

        # Will be none if first level
        if root is None: root = self

        # End of recursion
        if isinstance(tree, str):
            root.children.append(PluginTreeLevel(self.zug, tree))
            return self

        # Handle the case where we were passed a list
        elif isinstance(tree, list):
            for node in tree:
                root.children.append(PluginTreeLevel(self.zug, node))
            return self

        elif not isinstance(tree, dict):
            logger.error("Unable to load tree from type {type}".format(type=type(tree)))

        # Recurse over dictionary
        for node, children in tree.iteritems():
            for child in children:
                child.parent = self
                curr = PluginTreeLevel(self.zug, node)
                root.addChild(curr)
                self.loadTree(child, curr)

        return self

    def loadModules(self):
        """
        Attempts to load all modules within Plugin tree
        """

        pluginPath = self.zug.settings.get('plugin_paths', {}).get(self.name, None)
        defaultDir = os.path.join(os.path.dirname(baseDir), 'plugins')
        pluginDirs = self.zug.settings.get('plugin_directories', []) + [os.getcwd(), defaultDir]

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

        # Walk tree for children modules
        for child in self.children:
            child.parent = self
            child.loadModules()

        if self.parent:
            self.parent.qs_finished_work.append(self.q_new_work)

        numProcs = int(self.zug.settings.get('processors', {}).get(self.name, 1))

        logger.info("Launching [{num}] proc(s) for [{name}]".format(num=numProcs, name=self.name))

        try:
            for i in range(numProcs):
                self.initializeModule()
        except Exception, msg:
            raise Exception('Initialization of {name} failed: {msg}'.format(name=self.name, msg=str(msg)))
        else:
            logger.info('Initialization of {name} complete.'.format(name=self.name))

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

        logger.info("Attempting to initialize [{name}] as class.".format(name=self.name))

        # Attempt to load as class
        try:
            className = self.zug.settings.get('plugin_class_names', {}).get(self.name, self.name)
            pluginClass = getattr(self.module, className)
            kwargs = self.zug.settings.get('plugin_kwargs', {}).get(self.name, {})
            self.plugin = pluginClass(self.q_new_work, self.qs_finished_work, __pluginName__=self.name, **kwargs)
            self.startDaemon()
            logger.info("SUCCESS: Initialized class: {plugin}".format(plugin=self.name))
            return 

        except Exception, msg:
            logger.warn("Unable to initialize plugin as class: " + str(msg))
            
        logger.info("Attempting to initialize [{name}] using decorators.".format(name=self.name))

        # Attempt to load by decorators
        try:
            kwargs = self.zug.settings.get('plugin_kwargs', {}).get(self.name, {})
            self.plugin = basePlugin(self.q_new_work, self.qs_finished_work, __pluginName__=self.name, **kwargs)

            numDecorators = 0
            # Check the module for functions handled by decorators
            for dec in decoratables:
                attr = DECORATOR_PREFIX + dec
                decs = [a for a in dir(self.module) if hasattr(self.module.__dict__[a], attr)]

                # Override the base class functions
                if len(decs) > 0: 
                    numDecorators += 1
                    logging.debug("Injecting function into base class instance: {func}".format(func=decs[-1]))
                    self.plugin.__dict__[dec] = self.module.__dict__[decs[-1]]

            if numDecorators == 0:
                raise Exception('No decorators found')

            self.startDaemon()
            logger.info("SUCCESS: Initialized injected class: {plugin}".format(plugin=self.name))
            return

        except Exception, msg:
            logger.info("Unable to load plugin by decorators: " + str(msg))
            raise

    def startDaemon(self):
        p = Process(target=self.plugin.start)
        p.daemon = True
        p.start()
        self.processes.append(p)

    def joinTree(self):

        for child in self.children:
            child.joinTree()

        for process in self.processes:
            process.join()

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

        logger.info("Starting zug manager...")
        for plugin in self.plugins:
            docs = self.settings.get('plugin_kwargs', {}).get(plugin.name, {}).get('docs', [])

            for doc in docs: 
                plugin.q_new_work.put(doc)

            if not plugin.plugin.__dict__.get('isDaemon', False):
                plugin.q_new_work.put(exceptions.EndOfQueue())

        for plugin in self.plugins:
            plugin.joinTree()

