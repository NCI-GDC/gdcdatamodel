import logging, imp, os, sys, pprint

from app import settings, baseDir

    
class ETL:

    def __init__(self):
        """
        A new instance to extract, transform, and load data
        """

        logging.debug("New ETL instance: " + str(self))
        self.pipelineTree = settings.get('pipelines', [])
        self.pipelines = []

        logging.info("Loading pipelines ... ")
        self.loadPipelines()

        logging.info("Loading modules ...")
        self.loadModules()

    def loadPipelines(self):
        """ 
        Walk the plugin tree provided in the settings and create stage PipelineStage objects from them
        """

        logging.info("Loading Pipelines... ")
        for pipeline in self.pipelineTree:
            self.pipelines.append(PipelineStage(pipeline))
        return self

    def loadModules(self):
        """ 
        Walk the PipelineStage tree create instances of PiplinePlugins objects from them
        """

        logging.info("Loading Modules... ")
        for pipeline in self.pipelines:
            pipeline.loadModules()
        return self

    def run(self):
        logging.info("Starting ETL...")

        for pipeline in self.pipelines:
            logging.info('Running pipeline {pipeline}'.format(pipeline = pipeline))
            pipeline.run()

class PipelineStage:

    """ 
    Represents a node in a pipeline tree
    """

    def __init__(self, tree = None):
        self.name = settings.get('name', None)
        self.children = []
        self.module = None
        self.current = None
        self.plugin = None

        if self.name: logging.info("Initializing ETL: {name}".format(name = self.name))

        if isinstance(tree, str):
            self.name = tree
        else:
            self.name = tree.keys()[0]
            self.loadTree(tree[self.name])

    def addChild(self, stage):
        """ 
        Adds a child node to current level 
        """

        self.children.append(stage)

    def __repr__(self):
        """
        String representation
        """

        return "<PipelineStage '{name}'>".format(name = self.name)

    def dump(self, level = 0):
        """
        Print text representation of tree to stdout
        """

        print '| '*level + '+ ' + self.name
        for child in self.children:
            child.dump(level + 1)

    def run(self, doc = None):
        """
        Passes a document down the tree, loading it into each of stage in the next level
        """

        logging.debug('Running ' + str(self))
        if self.plugin is None: return self

        # Give the plugin a document
        self.plugin.start(doc)

        # Iterate through plugins documents
        for doc in self.plugin:
            # Pass each document to the next stage
            for child in self.children:
                child.run(doc)

        self.plugin.close()

    def loadTree(self, tree, root = None):
        """
        Loads a python datastructure, assuming that it is a list,
        dictionary (or single name) containing the names of plugins
        """

        # Will be none if first level
        if root is None: root = self

        # End of recursion
        if isinstance(tree, str):
            root.addChild(PipelineStage(tree))
            return self

        # Handle the case where we were passed a list
        elif isinstance(tree, list):
            for node in tree:
                root.addChild(PipelineStage(node))
            return self

        elif not isinstance(tree, dict):
            logging.error("Unable to load tree from type {type}".format(type = type(tree)))

        # Recurse over dictionary
        for node, children in tree.iteritems():
            for child in children:
                curr = PipelineStage(node)
                root.addChild(curr)
                self.loadTree(child, curr)

        return self

    def loadModule(self, name):
        """
        Attempts to load a module of a given name
        """

        logging.info("Loading plugin: {plugin}".format(plugin = name))

        # Build plugin file path
        pluginDir = settings.get('plugin_directory', 'plugins')
        pluginPath = os.path.join(baseDir, pluginDir, "{plugin}.py".format(plugin = name))
        pluginPath = settings.get('plugin_paths', {}).get(name, pluginPath)
        logging.info("Looking for plugin at {path}".format(path = pluginPath))

        # Attempt to import and add the plugin
        try:
            module = imp.load_source(name, pluginPath)
        except Exception, msg:
            logging.error("Unable to load plugin: {plugin}: {msg}".format(plugin = name, msg = str(msg)))
            module = None
        else:
            logging.info("SUCCESS: Loaded plugin: {plugin}".format(plugin = name))

        return module

    def loadModules(self):
        """
        Attempts to load aall modules within Pipeline tree
        """

        self.module = self.loadModule(self.name)

        for child in self.children:
            child.loadModules()
            
        if self.module is None:
            return self

        logging.info('Initializing {name}'.format(name = self.name))
        try:
            self.plugin = self.module.PipelinePlugin(**settings.get('plugin_kwargs', {}).get(self.name, {}))
        except Exception, msg:
            logging.error('Initialization of {name} failed: {msg}'.format(name = self.name, msg = str(msg)))
        else:
            logging.info('Initialization of {name} complete.'.format(name = self.name))

        return self

