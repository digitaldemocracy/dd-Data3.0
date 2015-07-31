#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-
'''
File: billparse.py
Author: ???
Modified By: Daniel Mangin & Mandy Chan
Date: 6/11/2015

Description:
- Takes the bill_xml column from the capublic.bill_version_tbl and inserts it into the appropriate columns in DDDB2015Apr.BillVersion
- This script runs under the update script

Sources:
- Leginfo (capublic)
  - Pubinfo_2015.zip
  - Pubinfo_Mon.zip
  - Pubinfo_Tue.zip
  - Pubinfo_Wed.zip
  - Pubinfo_Thu.zip
  - Pubinfo_Fri.zip
  - Pubinfo_Sat.zip

- capublic
  - bill_version_tbl

Populates:
  - BillVersion (title, digest, text)
'''

import loggingdb
from lxml import etree 
import re
import binascii

def traverse(root):
   for node in root:
      traverse(node)
      if "caml" in node.tag:
         node.attrib['class'] = node.tag.split('}')[1]
         node.tag = 'span'

def billparse():
   # MUST SPECIFY charset='utf8' OR BAD THINGS WILL HAPPEN.
   with loggingdb.connect(host='transcription.digitaldemocracy.org',
                        user='monty',
                        db='DDDB2015July',
                        passwd='python',
                        charset='utf8') as dd_cursor:
     print('Opened connection')
     dd_cursor.execute('''SELECT bill_version_id, bill_xml
                          FROM capublic.bill_version_tbl;''')
     print('Executed select query')
     rows = dd_cursor.fetchall()
     for (vid, xml) in rows:
         xml = xml.strip()
         flags=re.DOTALL
         xml = re.sub(r'<\?xm-(insertion|deletion)_mark\?>', r'', xml, flags)
         xml = re.sub(r'<\?xm-(insertion|deletion)_mark (?:data="(.*?)")\?>', r'<span class="\1">\2</span>', xml, flags)
         '''
         xml = re.sub(r'<\?xm-(insertion|deletion)_mark_start\?>', r'<span class="\1">', xml, flags)
         xml = re.sub(r'<\?xm-(insertion|deletion)_mark_end\?>', r'</span>', xml, flags)
         '''
         
         root = etree.fromstring(xml)

         namespace = {'caml': 'http://lc.ca.gov/legalservices/schemas/caml.1#'}

         #print "creating title"
         title = root.xpath('//caml:Title', namespaces=namespace)[0]
         titleText = re.sub(r'<.+?>', r'', etree.tostring(title), flags)

         #print "creating digest"
         digest = ""
         if (len(root.xpath('//caml:DigestText', method="text", namespaces=namespace)) > 0):
            digest = root.xpath('//caml:DigestText', method="text", namespaces=namespace)[0]
            digest = etree.tostring(digest)

         #print "creating body"
         body = ""
         if (len(root.xpath('//caml:Bill', namespaces=namespace)) > 0):
            body = root.xpath('//caml:Bill', namespaces=namespace)[0]
            #traverse(body)
            body = etree.tostring(body)
         elif (len(root.xpath('//caml:Content', namespaces=namespace)) > 0):
            temp = ""
            for x in range(0, len(root.xpath('//caml:Content', namespaces=namespace))):
               temp2 = root.xpath('//caml:Content', namespaces=namespace)[x]
               body = body + etree.tostring(temp2)
         else:
            print vid
            print root.xpath('//caml:Bill', namespaces=namespace)
         #print body

         dd_cursor.execute('''UPDATE BillVersion
                              SET title = %s, digest= %s, text = %s
                              WHERE vid = %s;''', (titleText, digest, body, vid))
         '''
         except Exception as e:
          print(e)
          print vid
          #print"problems/" + vid + ".err", "w"
         '''

if __name__ == "__main__":
   billparse()
