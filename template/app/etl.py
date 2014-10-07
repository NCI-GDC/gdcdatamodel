import logging, imp, os, sys, pprint, copy
from app import settings, baseDir

logger = logging.getLogger(name = "[{name}]".format(name = __name__))

class ETL:

    def __init__(self):
        """
        A new instance to extract, transform, and load data
        """

        logger.debug("New ETL instance: " + str(self))
        self.pipelineTree = settings.get('pipelines', [])
        self.pipelines = []

        logger.info("Loading pipelines ... ")
        self.loadPipelines()

        logger.info("Loading modules ...")
        self.loadModules()

    def loadPipelines(self):
        """ 
        Walk the plugin tree provided in the settings and create stage PipelineStage objects from them
        """

        logger.info("Loading Pipelines... ")
        for pipeline in self.pipelineTree:
            self.pipelines.append(PipelineStage(pipeline))
        return self

    def loadModules(self):
        """ 
        Walk the PipelineStage tree create instances of PiplinePlugins objects from them
        """

        logger.info("Loading Modules... ")
        for pipeline in self.pipelines:
            pipeline.loadModules()
        return self

    def run(self):
        logger.info("Starting ETL...")

        for pipeline in self.pipelines:
            logger.info('Running pipeline {pipeline}'.format(pipeline = pipeline))
            pipeline.run(None)

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

        if self.name: logger.info("Initializing ETL: {name}".format(name = self.name))

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

    def run(self, __doc__, **__state__):
        """
        Passes a document down the tree, loading it into each of stage in the next level
        """

        logger.debug('Running ' + str(self))
        if self.plugin is None: return self

        # Give the plugin a document and start it
        self.plugin.load(__doc__, **__state__)
        self.plugin.start()
 
        # Iterate through plugins __doc__uments
        for __doc__ in self.plugin:
            # Pass each document to the next stage
            child_doc = copy.deepcopy(__doc__)
            child_state = copy.deepcopy(self.plugin.state)

            for child in self.children:
                logger.debug('Passing to ' + child.name)
                child.run(child_doc, **child_state)

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
            logger.error("Unable to load tree from type {type}".format(type = type(tree)))

        # Recurse over dictionary
        for node, children in tree.iteritems():
            for child in children:
                curr = PipelineStage(node)
                root.addChild(curr)
                self.loadTree(child, curr)

        return self

    def loadModule(self, name, pluginPath = None, directory = None):
        """
        Attempts to load a module of a given name
        """

        logger.info("Loading plugin: {plugin}".format(plugin = name))

        if directory and not os.path.isabs(directory):
            pluginPath = os.path.join(baseDir, directory, '{name}.py'.format(name = name))
        elif directory:
            pluginPath = os.path.join(directory, '{name}.py'.format(name = name))

        # Attempt to import and add the plugin
        try:
            module = imp.load_source(name, pluginPath)
        except Exception, msg:
            logger.info("Unable to load plugin: {plugin}: {msg}".format(plugin = name, msg = str(msg)))
            return None
        else:
            logger.info("SUCCESS: Loaded plugin: {plugin}".format(plugin = name))

        return module

    def loadModules(self):
        """
        Attempts to load all modules within Pipeline tree
        """

        pluginPath = settings.get('plugin_paths', None).get(self.name, None)
        pluginDirs = settings.get('plugin_directories', []) 

        if pluginPath is not None:
            logger.info("Looking for plugin {name} at {path}".format(name = self.name, path = pluginPath))
            self.module = self.loadModule(self.name, pluginPath)

        else:
            for pluginDir in pluginDirs:
                logger.info("Looking for plugin {name} in {dir}".format(name = self.name, dir = pluginDir))
                self.module = self.loadModule(self.name, directory = pluginDir)
                if self.module is not None: break

        for child in self.children:
            child.loadModules()
            
        if self.module is None:
            logger.error("Unable to load plugin: {plugin}. Is plugin in a 'plugin_directories' directory?".format(plugin = self.name))
            return self

        logger.info('Initializing {name}'.format(name = self.name))
        try:
            self.plugin = self.module.PipelinePlugin(**settings.get('plugin_kwargs', {}).get(self.name, {}))
        except Exception, msg:
            logger.error('Initialization of {name} failed: {msg}'.format(name = self.name, msg = str(msg)))
        else:
            logger.info('Initialization of {name} complete.'.format(name = self.name))

        return self

