import pprint
import zug

@zug.process
def stdout(doc):
    pprint.pprint(doc)
    return doc
