import pprint
import zug

@zug.next
def stdout(doc):
    pprint.pprint(doc)
    return doc
