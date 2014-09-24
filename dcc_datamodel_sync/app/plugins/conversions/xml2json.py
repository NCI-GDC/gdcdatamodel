from lxml import etree
import json, os, glob, re
from pprint import pprint
import psycopg2, copy, logging

try:
    from local_settings import DB, DB_USER, DB_PASSWORD, DB_PORT
except:
    logging.warn("xml2json: unable to import local_settings.py")

class xml2json:

    recurse = True
    full_namespaces = False
    full_attributes = True
    use_namespaces = False
    flatten = False
    includes = []

    #dict because should map to the column name that this data is stored in
    accepted_xml_types = {
        "biospecimen" : "biospecimen", 
        "clinical" : "clinical",
        "auxiliary" : "auxiliary",
        "control" : "control",
        "omf" : "omf"
    }

    tree, root, conn, pscur = None, None, None, None

    def __init__(self, path = None):
        """the class specifies a single xml document at a time"""
        self.path = path

        self.table = None
        self.table_index = 0
        self.include_fieldless = False
        self.attrNamespacePattern = re.compile("\{.+\}(.+)")

        if self.path:
            self.loadFromFile()

    def __iter__(self):
        return self

    def loadFromFile(self, path = None):
        """load the xml document from path file"""
        logging.debug("importing xml from path {path}".format(path = path))
        
        if path:
            self.tree = etree.parse(path)
        else:
            self.tree = etree.parse(self.path)
        self.root = self.tree.getroot()
        logging.debug("imported xml from path {path}".format(path = path))

    def loadFromString(self, xml_string):
        """load the xml document from path file"""
        self.tree = None
        self.root = etree.fromstring(xml_string)
        
    def tagnons(self, element):
        """return the tag of the element without the namespace"""
        return element.xpath('local-name()')
 
    def tagns(self, element):
        """return the namespace of the tag of the element """
        return element.xpath('namespace-uri()')
       
    def add_nsmap(self, doc):
        """add the xml root namespace map to the document root"""
        doc['@xmlns'] = self.root.nsmap

    def add_element(self, root, element, 
                    recurse = True, 
                    full_namespaces = False,
                    full_attributes = True, 
                    use_namespaces = False):
        
        local = self.tagnons(element)
        prefix = element.prefix

        if use_namespaces and prefix:
            local = "%s:%s" % (prefix, local)

        if local not in root:
            """If there is no multiplicity of current element tag"""

            text = element.text

            if text:
                text = text.strip()

                if text != "":
                    """ Insert a non-empty element""" 
                    if isinstance(root, list):
                        root[-1][local] = {'$': text}
                        if full_namespaces:
                            root[-1][local]['@xmlns'] = element.nsmap    
                    else:
                        root[local] = {'$': text}
                        if full_namespaces:
                            root[local]['@xmlns'] = element.nsmap    

                else: 
                    """ Insert an empty element""" 
                    if isinstance(root, list):
                        root.append({local : {}})
                        if full_namespaces:
                            root[-1][local]['@xmlns'] = element.nsmap    
                    else:
                        root[local] = {}
                        if full_namespaces:
                            root[local]['@xmlns'] = element.nsmap    


                if full_attributes:
                    """add attributes"""
                    if isinstance(root, list):
                        for attr, value in element.items():
                            root[-1][local]['@%s' % attr] = value
                    else:
                        for attr, value in element.items():
                            root[local]['@%s' % attr] = value


            else:
                """Insert an element with no value"""
                if isinstance(root, list):
                    root.append({local : {'$':''}})
                else:
                    root[local] = {'$':''}

                if full_attributes:
                    """add attributes"""
                    if isinstance(root, list):
                        for attr, value in element.items():
                            root[-1][local]['@%s' % attr] = value
                    else:
                        for attr, value in element.items():
                            root[local]['@%s' % attr] = value


        else: 
            """If there is multiplicity of current element tag"""
            # is this the first tag conflict -> turn element list
            if not isinstance(root[local], list):
                root[local] = [root[local]]

            # append element to enter recursively
            root[local].append({})

        if recurse:
            """add sub elements recursively"""

            for child in element.findall("*"):
                # recurse over children

                if isinstance(root, list):
                    # if the current root is a list, use last empty 
                    self.add_element(root[-1], child, recurse, full_namespaces, full_attributes, use_namespaces)

                else:
                    # recurse using the element we just added as root
                    self.add_element(root[local], child, recurse, full_namespaces, full_attributes, use_namespaces)


    def flatten_element(self, root, element, fields = {}, parent_key = None, includes = None, include_fieldless = False):
        
        xml_leaf = False
        local    = self.tagnons(element)
        prefix   = element.prefix
        children = element.findall("*")

        """ 
        Define a leaf node as any leaf that has children but no
        grandchildren
        """
        for child in children:
            grandchildren = child.findall("*")
            if len(grandchildren) == 0:
                xml_leaf = True

        if xml_leaf:
            """ 
            If this node is a leaf node, then add it directly to the final
            flattened document
            """
            
            leaf = {'_type': local}
    
            # Add all of the values we accumulated from parent nodes
            for key, value in fields.iteritems():
                leaf[key] = value

            root.append(leaf)
        
        # Copy the fields dict to avoid breaking references
        fields = copy.copy(fields)

        # Add the element text
        if element.text is not None  and element.text.strip() != "":
            fields[local] = element.text.strip()
        elif include_fieldless:
            fields[local] = None

        # Add add the element attributes to the fields
        for attr, value in element.items():
            nsmatch = self.attrNamespacePattern.match(attr)
            if nsmatch: attr = nsmatch.group(1)
            key = "{local}.{attribute}".format(local = local, attribute = attr)
            fields[key] = value

        # Add each child to the fields
        for child in children:
            child_name = self.tagnons(child)

            if child.text is not None and child.text.strip() != "":
                fields[child_name] = child.text.strip()
            elif include_fieldless:
                fields[child_name] = None

            for attr, value in child.items():
                nsmatch = self.attrNamespacePattern.match(attr)
                if nsmatch: attr = nsmatch.group(1)
                fields["%s.%s" % (child_name, attr)] = value

        for child in children:
            self.flatten_element(root, child, includes = includes, fields = fields, parent_key = local)

                
    def toJSON(self, recurse = True, full_namespaces = False, full_attributes = True, use_namespaces = False, flatten = False, includes = [], include_fieldless = False):

        """convert an xml document to json, load tree from path if specified

        :param recurse: add all children recursively
        :param full_namespaces: use the badgerfish standard of including the nested namespace map in every element
        :param full_attributes: include the attributes (including empty attributes) for each elementn
        :param use_namespaces: whether or not to append the namespace to the element name i.e. "nmsp1:elmnt1"

        """        
        logging.debug("converting xml to json")

        doc = []

        if flatten:
            self.flatten_element(doc, self.root, includes = includes, include_fieldless = include_fieldless)
        else:
            self.add_element(doc, self.root, recurse, full_namespaces, full_attributes, use_namespaces)

        logging.debug("completed conversion from xml to json")
        return doc

    def connectToPostgres(self, DB, DB_USER, DB_PASSWORD, DB_PORT, DB_HOST):
        """connect the instance to a postgres database"""
        self.conn = psycopg2.connect(database=DB, user=DB_USER, password=DB_PASSWORD, port=DB_PORT)

    def loadTable(self, table_name, xml_field, recurse = True, full_namespaces = False,
                  full_attributes = True, use_namespaces = False, flatten = False, includes = [], include_fieldless = False):
        """Execute query on database for use with iterator
        
        :param table_name: is the relation to pull the data from 
        :param xml_field: is the name of the column that is the actual xml file
        :param includes: a list of fields to aggregate down to the aliquot level if you are flattening
        :returns: the number of documents available

        """

        self.recurse = recurse
        self.full_namespaces = full_namespaces
        self.full_attributes = full_attributes
        self.use_namespaces = use_namespaces
        self.flatten = flatten
        self.includes = includes 
        self.include_fieldless = include_fieldless

        self.pscur = self.conn.cursor()
        self.pscur.execute("SELECT count(%s) FROM %s" % (xml_field, table_name))
        total = self.pscur.fetchone()[0]

        print("Loading %s documents %s from table %s ..." % (total, xml_field, table_name)),
        self.pscur.execute("SELECT %s FROM %s" % (xml_field, table_name))
        print("done")

        return total

    def loadAllDocs(self):
        self.table = self.pscur.fetchall()
        self.table_index = 0

    def next(self):
        """Iterator to grab a single entry from postgres at a time. Example:

        ``
        xml2json_instance.loadTable("tcga_biospecimen", "biospecimen")
        for doc in xml2json_instance: 
              print doc
        ``
        
        """

        doc = {}

        if not self.pscur:
            raise Exception("postgres cursor not defined, maybe .loadTable()?")

        if self.table:
            if self.table_index < len(self.table):
                row = self.table[self.table_index]
                self.table_index += 1
            else:
                row = None

        else:
            row = self.pscur.fetchone()

        # End condition
        if not row or len(row) <= 0: 
            raise StopIteration


        # Convert the current entry
        self.loadFromString(row[0])
        if self.flatten:
            doc = self.toJSON(self.recurse, self.full_namespaces, self.full_attributes, 
                              self.use_namespaces, flatten = True, includes = self.includes, 
                              include_fieldless = self.include_fieldless)
        else:
            doc = self.toJSON(self.recurse, self.full_namespaces, self.full_attributes, 
                              self.use_namespaces)

        return doc

    def convertDatabase(self, project_name):
        """for each table in the database that is of the format <project_name>_<accepted_xml_type>, 
        convert the xml documents to json"""

        cur = self.conn.cursor()

        # loop through xml types
        for key in self.accepted_xml_types:
            
            # Grab the info about the xml_type data 
            xml_type = self.accepted_xml_types[key]
            table_name = "%s_%s" % (project_name, xml_type)
            total = self.loadTable(table_name, xml_type)

            print "Converting %d files in %s" % (total, key)

            count = 0
            for doc in self:
                print "converting %d/%d\r" % (count, total),
                count += 1

    def generateElementMapping(self, element, mapping):

        local = "%s" % (self.tagnons(element))

        if element.prefix and element.prefix != "":
            local = "%s:%s" % (element.prefix, local)

        text = element.text
        children = element.findall("*")
        if not children: children = []

        if local not in mapping:
            mapping[local] = { "properties": {} }

            # if len(children) > 0:
            mapping[local]['type'] = "nested"
            
            if text and  text != "":
                mapping[local]['properties']["$"] = {'type': 'nested'}

            for attr, value in element.items():
                mapping[local]['properties']['@%s' % attr] = {'type' : 'nested'} 

        for child in children:
            self.generateElementMapping(child, mapping[local]['properties'])


    def createMapping(self, table_name, xml_field, elasticsearch_type):

        cur = self.conn.cursor()        
        cur.execute("SELECT %s FROM %s" % (xml_field, table_name))
        xml = cur.fetchone()[0]
        self.loadFromString(xml)
        mapping = {elasticsearch_type: {"properties": {}}}
        self.generateElementMapping(self.root, mapping[elasticsearch_type]["properties"])
        return mapping

    def generateElementNesting(self, element, mapping):

        local = "%s" % (self.tagnons(element))

        if element.prefix and element.prefix != "":
            if self.use_namespaces:
                local = "%s:%s" % (element.prefix, local)
            else:
                local = "%s" % (local)

        text = element.text
        children = element.findall("*")
        if not children: children = []

        if local not in mapping:
            mapping[local] = {"attr":[]}

            for attr, value in element.items():
                mapping[local]['attr'].append('@%s' % attr)

        for child in children:
            self.generateElementNesting(child, mapping[local])


    def createNesting(self, table_name, xml_field, use_namespaces = True):
        self.use_namespaces = use_namespaces

        cur = self.conn.cursor()        
        cur.execute("SELECT %s FROM %s" % (xml_field, table_name))
        xml = cur.fetchone()[0]
        self.loadFromString(xml)
        nesting = {}
        self.generateElementNesting(self.root, nesting)
        return nesting


def main():
    """take all the xml files in xmlroot and convert them to json documents in jsonroot"""

    conv = xml2json()
    conn = conv.connectToPostgres(DB, DB_USER, DB_PASSWORD, DB_PORT, 'localhost')
    # conv.createMapping("tcga_biospecimen", "biospecimen", "analysis")
    # conv.loadTable("tcga_biospecimen", "biospecimen", flatten = True)
    # for doc in conv:
    #     pprint(doc)
    #     break

if "__main__" in __name__:
    main()
