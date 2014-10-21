
import zug

@zug.process
def count(doc):
    return str(len(doc))

