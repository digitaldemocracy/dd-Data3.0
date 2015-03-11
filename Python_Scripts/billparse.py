import mysql.connector
from lxml import etree 
import re

def traverse(root):
   for node in root:
      traverse(node)
      if "caml" in node.tag:
         node.attrib['class'] = node.tag.split('}')[1]
         node.tag = 'span'

def billparse():
   print "got here"
   conn = mysql.connector.connect(user="root", database="DDDB2015AprTest", password="", buffered=True)
   get = conn.cursor()
   put = conn.cursor()

   print "got here"
   get.execute("SELECT bill_version_id, bill_xml FROM capublic.bill_version_tbl")
   for (vid, xml) in get:
      try:	
	 xml = ''.join('{:02x}'.format(x) for x in xml.strip())
	 xml = xml.decode("hex")
	 #print "got here1"
         xml = re.sub(r'<\?xm-(insertion|deletion)_mark\?>', r'', xml, flags=re.DOTALL)
         xml = re.sub(r'<\?xm-(insertion|deletion)_mark (?:data="(.*?)")\?>', r'<span class="\1">\2</span>', xml, flags=re.DOTALL)

         xml = re.sub(r'<\?xm-(insertion|deletion)_mark_start\?>', r'<span class="\1">', xml, flags=re.DOTALL)
         xml = re.sub(r'<\?xm-(insertion|deletion)_mark_end\?>', r'</span>', xml, flags=re.DOTALL)

         root = etree.fromstring(xml)
         namespace = {'caml': 'http://lc.ca.gov/legalservices/schemas/caml.1#'}

	 #print vid
	 #print "creating title"
         title = root.xpath('//caml:Title', namespaces=namespace)[0]

	 #print "creating digest"
	 digest = ""
	 if (len(root.xpath('//caml:DigestText', method="text", namespaces=namespace)) > 0):
	    digest = root.xpath('//caml:DigestText', method="text", namespaces=namespace)[0]
            digest = etree.tostring(digest)

	 #print "creating body"
	 body = ""
	 if (len(root.xpath('//caml:Bill', namespaces=namespace)) > 0):
            body = root.xpath('//caml:Bill', namespaces=namespace)[0]
            traverse(body)
	    body = etree.tostring(body)


         put.execute("UPDATE BillVersion SET title = %s, digest= %s, text = %s WHERE vid = %s", (title.text, digest, body, vid))
      except Exception as e:
         print(e)
         #print"problems/" + vid + ".err", "w"

   conn.commit()
   get.close()
   return

if __name__ == "__main__":
   billparse()
