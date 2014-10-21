
import zug
import urllib

@zug.process
def uri(doc):
    return urllib.urlopen(doc).read()

