'''
Created on Apr 17, 2014

@author: martin
'''
import os

from subprocess import check_output, check_call, CalledProcessError
from lxml import etree  # @UnresolvedImport

java    = "/usr/bin/java"
jparams = "-jar /usr/local/bin/diffx-0.7.4.jar -o /tmp/out.xml -A guano -F convenient".split(' ')

# This XSL will return only the change nodes from diffx-0.7.4.jar
xslt_root = etree.XML('''
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:strip-space elements="*"/>
    <xsl:output method = "xml" indent="yes"/>
    <xsl:template match="node()|@*">
        <xsl:copy>
            <xsl:apply-templates select="node()|@*" />
        </xsl:copy>
    </xsl:template>
    <xsl:template match="*[not(descendant-or-self::*[substring(name(), 1, 3) = 'dfx'])]" />
</xsl:stylesheet>''')
diff_transform = etree.XSLT(xslt_root)

xslt_del_attributes = etree.XML('''
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:strip-space elements="*"/>
    <xsl:output method = "xml" indent="yes"/>
    <xsl:template match="node()|@*">
        <xsl:copy>
            <xsl:apply-templates select="node()|@*" />
        </xsl:copy>
    </xsl:template>
    <xsl:template match="@cde" />
    <xsl:template match="@cde_ver" />
    <xsl:template match="@xsd_ver" />
    <xsl:template match="@preferred_name" />
    <xsl:template match="@display_order" />
    <xsl:template match="@owner" />
    <xsl:template match="@restricted" />
    <xsl:template match="@tier" />
    <xsl:template match="@source_system_identifier" />
    <xsl:template match="@precision" />
    <xsl:template match="@sequence" />
</xsl:stylesheet>
        ''')
strip_attr_transform = etree.XSLT(xslt_del_attributes)

class XmlDiffClass(object):
    '''
    classdocs
    '''

    def __init__(self, xml_cur, xml_prior):
        '''
        The shell command to diff xml requires files. So save the input xml text to /tmp/1.xml and /tmp/2.xml
        '''
        
        def get_etree(xml):
            try:    # Try reading as file
                new_tree = etree.parse(xml)
            except OSError:
                # Now try reading as XML string
                from io import BytesIO
                try:    # Try as string
                    #xml = str(string(xml, encoding='utf-8'))
                    #print("%s" % xml)
                    new_tree = etree.parse(BytesIO(xml.encode()))
                    #parser = etree.XMLParser(recover=True, encoding='utf-8')
                except Exception as e:
                    raise (e)
                return new_tree
        # Do XSLT to remove most attributes
        cur_root   = get_etree(xml_cur)
        xml_cur_tree  = strip_attr_transform(cur_root)
        xml_cur_tree.write('/tmp/cur.xml')

        prior_root = get_etree(xml_prior)
        xml_prior_tree  = strip_attr_transform(prior_root)

        # Run the difference engine. Just skip this case if a bad execution. This program sucks - it does not
        # return non-zero exit status on dump.
        # This is a hack: writing XMLs to files for java program. Should be able to do it in memory.
        xml_cur_tree.write('/tmp/cur.xml')
        xml_prior_tree.write('/tmp/prior.xml')
        
        stderr_fh = open("/tmp/stderr.txt", 'w')
        try:
            check_call([ '/usr/bin/java'] + jparams + ['/tmp/cur.xml', '/tmp/prior.xml'], stderr = stderr_fh)
        except Exception as e:
            print ("Java diffx return: %s" % e)
            raise (e)
        finally:
            # Check big stderr file size
            stderr_size = stderr_fh.tell()
            stderr_fh.close()
            if (stderr_size > 100):
                new_e = CalledProcessError(1, "java diffx")
                raise (new_e)
              
        # Get the diffx output xml into an etree
        try: 
            tmp_tree = etree.parse('/tmp/out.xml')
        except OSError as e:
            # If not a valid filename
            raise(e)
        except Exception as e:
            print ("out.xml parse error: %s" % e)
            raise (e)
        
        # Transform the result
        tmp_root = tmp_tree.getroot()
        self.result = diff_transform(tmp_root)
        #print(type(result))
        
    def get_diff_tree(self):
        return self.result
    
    def get_diff_nodelist(self):
        
        diff_tree = self.result.xpath("//dfx:ins/..",   namespaces = self.result.getroot().nsmap)
        
        return diff_tree
    
    def something_else(self):
        pass
        #diff_node_list = result.xpath("//*[local-name() = 'ins']/..")
#         for diff_node in diff_node_list:
#             print("Diff: %s: %s" % (diff_node.tag, diff_node.text))
#             #print(etree.SubElement(diff_node.tag, "child").text)
#             for node in diff_node:
#                 print("\tNode: %s: %s" % (node.tag, node.text))

if __name__ == '__main__':
    
    # Test files
#     latest_xml_file = "./nationwidechildrens.org_clinical.TCGA-G2-A2EF.2.xml"
#     prior_xml_file  = "./nationwidechildrens.org_clinical.TCGA-G2-A2EF.1.xml"
   
    latest_xml_file = "/tmp/nationwidechildrens.org_clinical.TCGA-BR-8679.32.xml"
    prior_xml_file  = "/tmp/nationwidechildrens.org_clinical.TCGA-BR-8679.28.xml"
    # Test xmls
    latest_xml_file_handle = open(latest_xml_file ,'r')
    latest_xml             = latest_xml_file_handle.read()
    latest_xml_file_handle.close()
     
    prior_xml_file_handle  = open(prior_xml_file ,'r')
    prior_xml              = prior_xml_file_handle.read()
    prior_xml_file_handle.close()
    
    # Test DB retrieval
#     from bcr_xml_postgres_interface import PostgresClinXmlGetter
#     getter = PostgresClinXmlGetter(True)
# 
#     xmls = getter.get_case_last2_rev('A2EF')
#     prior_xml  = xmls[1].get_xml()
#     latest_xml = xmls[0].get_xml()
    
    # Instantiate class
    differ = XmlDiffClass(latest_xml, prior_xml)
    
    # Print test results
    print ("Diff XML: %s" % differ.get_diff_tree())
    print ("Diff nodelist: %s" % differ.get_diff_nodelist())