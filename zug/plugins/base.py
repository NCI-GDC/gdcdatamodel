
from zug.exceptions import IgnoreDocumentException, EndOfQueue
import zug
import logging
import copy
import time

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
            logging.error("{cls}: You really shouldn't override the function {name}()!!!".format(
                    name=name, cls=str(cls)))
            logging.error("But I'll assume you know what you're doing!")
            time.sleep(3)
        return type.__new__(cls, name, bases, dct)

class ZugPluginBase(object):

    __metaclass__ = OverrideWarned

    @overrideWarn
    def __init__(self, q_new_work, qs_finished_work, **kwargs):
        self.name = kwargs.pop('__pluginName__', 'plugin')
        self.docs = []
        self.state = {}
        self.kwargs = kwargs
        self.q_new_work = q_new_work
        self.qs_finished_work = qs_finished_work
        self.logger = logging.getLogger(name = "[{name}]".format(name=self.name))

        self.initialize(**kwargs)

    def process(self, doc):
        """Override this"""
        return doc

    def initialize(self, **kwargs):
        """Override this"""
        pass

    def enqueue(self, doc):
        self.q_new_work.put(doc)

    def yieldDoc(self, doc):
        for q in self.qs_finished_work:
            q.put(doc)

    @overrideWarn
    def start(self):
        self.logger.info("Starting plugin daemon")

        while True:

            self.logger.debug("Waiting for new document")
            doc = self.q_new_work.get()

            doc = copy.copy(doc)

            if isinstance(doc, zug.exceptions.EndOfQueue):
                self.logger.warn("Closing queue")
                self.q_new_work.close()
                self.yieldDoc(doc)
                return

            try:
                self.logger.debug("Processing new document: " + str(type(doc)))

                processed = self.process(doc)

                if processed is not None:
                    self.yieldDoc(copy.deepcopy(processed))

                self.logger.debug("Sucessfully processed: " + str(type(doc)))

            except IgnoreDocumentException:
                pass

            except EndOfQueue:
                self.logger.info("End of queue")
                return
        
