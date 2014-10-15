
import zug

@zug.callables.register()
def count(doc):
    print len(doc)
    return len(doc)

