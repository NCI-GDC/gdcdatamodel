import os
import tempfile
import hashlib
from boto.s3.key import Key
from sqlalchemy.orm.exc import NoResultFound
from lxml import etree
from cdisutils.log import get_logger
from cdisutils.net import url_for_boto_key
from gdcdatamodel import models as md


class Extractor(object):

    """
    Extracts sra xml files stored within the cghub xml file
    """

    def __init__(self, signpost=None, s3=None, bucket=None, graph=None):
        """

        """

        self.signpost = signpost
        self.bucket_name = bucket
        self.s3 = s3
        self.g = graph
        self.log = get_logger("cghub_file_sync")
        self.metadata_types = { 'analysis' : md.AnalysisMetadata,
                                'experiment' : md.ExperimentMetadata,
                                'run' : md.RunMetadata }

        self.sysan_to_copy = ['cghub_center_name',
             'cghub_last_modified',
             'analysis_id',
             'cghub_published_date',
             'import_took',
             'cghub_disease_abbr',
             'cghub_state',
             'source',
             'cghub_legacy_sample_id',
             'import_completed',
             'cghub_upload_date']
    
    def process(self, root):
        try:
            analysis_id = root.xpath('//Result/analysis_id')[0].text
            parent_name = root.xpath('//Result/files/file/filename')[0].text
        except IndexError:
            self.log.warn("Could not find the required parameters in the XML file, skipping")
            return

        try:
            parent_node = self.g.nodes(md.File).props({'file_name': parent_name})\
                                     .sysan({'analysis_id': analysis_id})\
                                     .one()
            # Add it to both bam and bai files?
        except NoResultFound:
            self.log.warn("Could not find parent node for {}, skipping"
                .format(parent_name))
            return
        # Extract, upload, insert, update
        self.create_update_node(root, parent_node, 'analysis')
        self.create_update_node(root, parent_node, 'experiment')
        self.create_update_node(root, parent_node, 'run')

    def resolve_type(self, type_str):
        '''
        Resolves a string to a metadata type
        '''
        try:
            return self.metadata_types[type_str]
        except KeyError:
            self.log.warn("Invalid node type '{}'".format(type_str))
            return None

    def create_update_node(self, root, parent, metadata_type):
        '''
        Returns the new node or the updated, existing node

        @param root: the xml document root
        @param parent: the parent file node to be linked
        @param metadata_type: string indicating what type of node is being created:
            one of: ('analysis','experiment','run')
        '''

        analysis_id = parent.sysan['analysis_id']

        md_type = self.resolve_type(metadata_type)
        if md_type is None: return
        
        file_name = '{}_{}.xml'.format(analysis_id, metadata_type)
        # Check if it exists already
        node = self.g.nodes(md_type).props({'file_name': file_name})\
                                    .sysan({'analysis_id': analysis_id})\
                                    .scalar()
        # Create a new node
        if not node:
            node = self.new_node(root, parent, metadata_type, file_name)
        # Update node if it exists
        else:
            self.log.info("Updating node {}".format(file_name))
            node = self.inherit_from_parent(node, parent)
            try:
                self.g.current_session().add(node)
            except:
                self.log.error("Problem updating node {}".format(file_name))
                raise

        return node

    def new_node(self, root, parent, metadata_type, file_name):
        '''
        Creates a new metadata node in the graph, creates a signpost id,
        and uploads it to s3

        @param root: the xml document root
        @param parent: the parent file node to be linked
        @param metadata_type: string indicating what type of node is being created:
            one of: ('analysis','experiment','run')
        @param file_name: the name of the file
        '''
        self.log.info("Creating new node {}".format(file_name))
        # Get signpost id
        nid = self.signpost.create()

        md_type = self.resolve_type(metadata_type)
        if md_type is None: return
        # Extract and save xml metadata file
        file_params = self.extract_xml_file(root, metadata_type+'_xml')

        if file_params is None:
            self.log.warn("No {} metadata found".format(metadata_type))
            return

        abs_path, file_size, file_md5 = file_params
        s3_key_name = '/'.join([nid.did, file_name])

        # Upload file
        s3_key = self.upload_file(abs_path, s3_key_name)
        # Remove temp file
        os.remove(abs_path)
            
        url = url_for_boto_key(s3_key)
        nid.urls = [url]
        nid.patch()
        node = md_type(node_id=nid.did,
                       file_name=file_name,
                       file_size=file_size,
                       md5sum=file_md5,
                       data_format='SRA XML',
                       data_category='Sequencing Data',
                       data_type='{} Metadata'.format(metadata_type.capitalize()))

        node = self.inherit_from_parent(node, parent)

        return node

    def inherit_from_parent(self, node, parent):
        for key in self.sysan_to_copy:
            if key in parent.sysan:
                node.sysan[key] = parent.sysan[key]
        node.acl = parent.acl
        if parent not in node.files:
            node.files.append(parent)
        
        return node

    def extract_xml_file(self, root, tag):
        '''
        Extracts a file and saves it
        Returns the absolute path of the file, its size, and its hash
        
        @param root: the xml document root
        @param tag: the tag to extract and save as a file
        '''

        doc = root.xpath(tag)
        # TODO: Support multiple?
        if len(doc) > 0:
            doc = doc[0]
            if len(doc) > 1:
                self.log.warn("Found multiple metadata tags with the same name")
        else:
            return None
        f, file_path = tempfile.mkstemp()
        os.write(f, etree.tostring(doc, pretty_print=True))
        os.close(f)
        # Md5 + filesize
        file_md5 = hashlib.md5(open(file_path, 'rb').read()).hexdigest()
        file_size = os.stat(file_path).st_size

        return file_path, file_size, file_md5

    def upload_file(self, path, s3_key_name):
        '''
        Uploads an xml metadata file to s3
        
        @param path: absolute path to the file to be uploaded
        @param s3_key_name: the name to use for the key
        '''
        # Maybe multipart this?
        self.log.info('Uploading {}'.format(s3_key_name))
        b = self.s3.get_bucket(self.bucket_name)
        k = Key(b)
        k.key = s3_key_name
        k.set_contents_from_filename(path)
        return k
        
