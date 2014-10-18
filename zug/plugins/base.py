
from zug.exceptions import IgnoreDocumentException, EndOfQueue
import zug
import logging

def overrideWarn(f):
    f.overrideWarn = True
    return f

def checkoverride(bases):
    ret = []
    for base in bases:
        ret +=  [name for name, attr in base.__dict__.items() if getattr(attr, "overrideWarn", False)]
        ret.extend(checkoverride(base.__bases__))
    return ret

class OverrideWarned(type):
    def __new__(cls, name, bases, dct):
        for name in [name for name in dct if name in checkoverride(bases)]:
            logging.error("You really shouldn't override the function {name}()!!!".format(name=name))
            logging.error("But I'll assume you know what you're doing!")
        return type.__new__(cls, name, bases, dct)

class ZugPluginBase(object):

    __metaclass__ = OverrideWarned

    @overrideWarn
    def __init__(self, **kwargs):
        self.name = kwargs.pop('__pluginName__', 'plugin')
        self.docs = []
        self.state = {}
        self.kwargs = kwargs
        self.initialize(**kwargs)
        self.q_new_work = None
        self.q_finished_work = None
        self.logger = logging.getLogger(name = "[{name}]".format(name=self.name))

    def process(self, doc):
        """Override this"""
        return doc

    def initialize(self, **kwargs):
        """Override this"""
        pass

    @overrideWarn
    def __iter__(self):
        yield self.q_out.get()

    @overrideWarn
    def start(self):
        self.logger.info("Starting plugin daemon")

        while True:

            self.logger.debug("Waiting for new document")
            doc = self.q_new_work.get()

            if isinstance(doc, zug.exceptions.EndOfQueue):
                self.logger.warn("Closing queue")
                self.q_new_work.close()
                self.q_finished_work.put(doc)
                break

            # if isinstance(doc, EndOfQueue): break
            self.logger.debug("Processing new document: " + str(type(doc)))
            try:
                self.q_finished_work.put(self.process(doc))
                self.logger.debug("Sucessfully processed: " + str(type(doc)))
            except Exception, msg:
                self.logger.error("Exception: " + str(msg))
                pass
                
    def close(self):
        pass
        
