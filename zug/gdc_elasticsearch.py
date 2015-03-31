from gdcdatamodel.mappings import (
    get_project_es_mapping, index_settings,
    annotation_tree, get_annotation_es_mapping,
    participant_tree, participant_traversal, get_participant_es_mapping,
    file_tree, file_traversal, get_file_es_mapping,
    ONE_TO_ONE, ONE_TO_MANY
)
import re
import json
import logging
from cdisutils.log import get_logger
from progressbar import ProgressBar, Percentage, Bar, ETA
from elasticsearch import NotFoundError

log = get_logger("gdc_elasticsearch")
log.setLevel(level=logging.INFO)

BATCH_SIZE = 16


class GDCElasticsearch(object):

    """
    """

    def __init__(self, es=None, c=None):
        """Walks the graph to produce elasticsearch json documents.

        :param es: An instance of Elasticsearch class

        """
        self.index_pattern = '{base}_{n}'
        self.es = es

    def pbar(self, title, maxval):
        """Create and initialize a custom progressbar

        :param str title: The text of the progress bar
        "param int maxva': The maximumum value of the progress bar

        """
        pbar = ProgressBar(widgets=[
            title, Percentage(), ' ',
            Bar(marker='#', left='[', right=']'), ' ',
            ETA(), ' '], maxval=maxval)
        pbar.update(0)
        return pbar

    def bulk_upload(self, index, doc_type, docs, batch_size=BATCH_SIZE):
        """Chunk and upload docs to Elasticsearch.  This function will raise
        an exception of there were errors inserting any of the
        documents

        :param str index: The index to upload documents to
        :param str doc_type: The type of document to pload as
        :param list docs: The documents to upload
        :param int batch_size: The number of docs per batch

        """
        if not docs:
            return
        if not self.es:
            log.error('No elasticsearch driver initialized')
        instruction = {"index": {"_index": index, "_type": doc_type}}
        pbar = self.pbar('{} upload '.format(doc_type), len(docs))

        def body():
            start = pbar.currval
            for doc in docs[start:start+batch_size]:
                yield instruction
                yield doc
                pbar.update(pbar.currval+1)
        while pbar.currval < len(docs):
            res = self.es.bulk(body=body())
            if res['errors']:
                raise RuntimeError(json.dumps([
                    d for d in res['items'] if d['index']['status'] != 100
                ], indent=2))
        pbar.finish()

    def put_mappings(self, index):
        """Add mappings to index.

        :param str index: The elasticsearch index

        """
        if not self.es:
            log.error('No elasticsearch driver initialized')
        return [
            self.es.indices.put_mapping(
                index=index,
                doc_type="project",
                body=get_project_es_mapping()),
            self.es.indices.put_mapping(
                index=index,
                doc_type="file",
                body=get_file_es_mapping()),
            self.es.indices.put_mapping(
                index=index,
                doc_type="participant",
                body=get_participant_es_mapping()),
            self.es.indices.put_mapping(
                index=index,
                doc_type="annotation",
                body=get_annotation_es_mapping()),
        ]

    def index_populate(self, index, part_docs=[], file_docs=[],
                       ann_docs=[], project_docs=[],
                       batch_size=BATCH_SIZE):
        self.bulk_upload(index, 'project', project_docs, batch_size)
        self.bulk_upload(index, 'annotation', ann_docs, batch_size)
        self.bulk_upload(index, 'participant', part_docs, batch_size)
        self.bulk_upload(index, 'file', file_docs, batch_size)

    def index_create_and_populate(self, index, part_docs=[],
                                  file_docs=[], ann_docs=[], project_docs=[],
                                  batch_size=BATCH_SIZE):
        """Create a new index with name `index` and add given documents to it.
        `part_docs` or `project_docs` are empty, the will be generated
        automatically.

        :param list part_docs: The participant docs to upload.
        :param list file_docs:
            The file docs to upload. If part_docs is empty,
            `file_docs` will be overwritten when part_docs are
            produced.
        :param list ann_docs:
            The annotation docs to upload. If part_docs is empty,
            `ann_docs` will be overwritten when part_docs are
            produced.
        :param list project_docs: The project docs to upload.

        """

        self.es.indices.create(index=index, body=index_settings())
        self.put_mappings(index)
        if not part_docs:
            print("There were no participant docs passed to populate with!")
        if not project_docs:
            print("There were no participant docs passed to populate with!")
        self.index_populate(index, part_docs, file_docs, ann_docs,
                            project_docs, batch_size)

    def swap_index(self, old_index, new_index, alias):
        """Atomically switch the resolution of `alias` from `old_index` to
        `new_index`

        :param str old_index: Old resolution
        :param str new_index: New resolution. `alias` will point here.
        :param str alias: Alias name to swap

        """

        self.es.indices.update_aliases({'actions': [
            {'remove': {'index': old_index, 'alias': alias}},
            {'add': {'index': new_index, 'alias': alias}}]})

    def get_next_index(self, base):
        """Using this class's `index_pattern` (1) find any old indices with

        matching bases (2) take the maximum

        """

        indices = set(self.es.indices.get_aliases().keys())
        p = re.compile(self.index_pattern.format(base=base, n='(\d+)')+'$')
        matches = [p.match(index) for index in indices if p.match(index)]
        next_n = max(sorted([int(m.group(1)) for m in matches]+[0]))+1
        return self.index_pattern.format(base=base, n=next_n)

    def lookup_index_by_alias(self, alias):
        """Find the index that an Elasticsearch alias is poiting to. Return
        None if the index doesn't exist.

        """

        try:
            keys = self.es.indices.get_alias(alias).keys()
            if not keys:
                return None
            return keys[0]
        except NotFoundError:
            return None

    def deploy_alias(self, alias, part_docs=[], file_docs=[],
                     ann_docs=[], project_docs=[], batch_size=BATCH_SIZE):
        """Create a new index with an incremented name, populate it with
        :func index_create_and_populate: and atomically switch the
        alias to point to the new index

        """
        new_index = self.get_next_index(alias)
        self.index_create_and_populate(new_index, part_docs,
                                       file_docs, ann_docs,
                                       project_docs, batch_size)
        old_index = self.lookup_index_by_alias(alias)
        if old_index:
            self.swap_index(old_index, new_index, alias)
        else:
            self.es.indices.put_alias(index=new_index, name=alias)
