from glob import iglob
import os
from io import StringIO
from zipfile import ZipFile
import xml.etree.ElementTree as ET
from pprint import pprint
from lxml import objectify
from tqdm import tqdm

from es import es
from elasticsearch import helpers
from collections import deque

ET.register_namespace('', 'http://www.rechtspraak.nl/schema/rechtspraak-1.0')
file_list = [f for f in iglob('OpenDataUitspraken/201*/*.zip', recursive=True) if os.path.isfile(f)]

def process_xml(tree):
    docs = []
    doc = {}
    # RDF
    rdf = tree.find('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}RDF')
    if rdf:
        desc_1 = rdf[0]
        identifier = desc_1.find('{http://purl.org/dc/terms/}identifier')
        modified = desc_1.find('{http://purl.org/dc/terms/}modified')
        published = desc_1.find('{http://purl.org/dc/terms/}issued')
        creator = desc_1.find('{http://purl.org/dc/terms/}creator')
        final_date = desc_1.find('{http://purl.org/dc/terms/}date')
        zaaknummer = desc_1.find('{http://psi.rechtspraak.nl/}zaaknummer')
        procedure = desc_1.find('{http://psi.rechtspraak.nl/}procedure')
        spatial = desc_1.find('{http://purl.org/dc/terms/}spatial')
        subject = desc_1.find('{http://purl.org/dc/terms/}subject')
        if len(rdf) > 1:
            deep_link = rdf[1].attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about']
            doc['link'] = deep_link

        doc['modified'] = modified.text
        doc['published'] = published.text
        doc['creator'] = creator.text
        doc['final_date'] = final_date.text
        if zaaknummer is not None:
            doc['case_number'] = zaaknummer.text
        if procedure is not None:
            doc['procedure'] = procedure.text
        if spatial is not None:
            doc['spatial'] = spatial.text
        if subject is not None:
            doc['subject'] = subject.text
        doc['_index'] = 'dutch_jurisdiction'
        doc['_type'] = 'verdict'
        doc['_id'] = identifier.text
        # res = es.index(index='rdf', doc_type='rdf', id=identifier.text, body=doc)

    # INHOUDS INDICATIE
    ih = tree.find('{http://www.rechtspraak.nl/schema/rechtspraak-1.0}inhoudsindicatie')
    if ih:
        ih_id = ih.attrib['id']
        ih_text = ET.tostring(ih).decode()
        doc['content_indication'] = ih_text
        # res = es.index(index='content_indication', doc_type='indication', id=identifier.text, body=doc)

    # UITSPRAAK
    uitspraak = tree.find('{http://www.rechtspraak.nl/schema/rechtspraak-1.0}uitspraak')
    if uitspraak:
        uitspraak_id = uitspraak.attrib['id']
        uitspraak_text = ET.tostring(uitspraak).decode()
        doc['verdict'] = uitspraak_text
        # res = es.index(index='verdict', doc_type='verdict', id=identifier.text, body=doc)
    return doc


if __name__ == '__main__':
    for f in file_list:
        handle = open(f, 'r')
        z = ZipFile(f, 'r')
        xmls = z.namelist()
        print("### {} ###".format(f))
        d = []
        for x in tqdm(xmls):
            zh = z.read(x)
            tree = ET.fromstring(zh)
            doc = process_xml(tree)
            d.append(doc)
        deque(helpers.parallel_bulk(es, d), maxlen=0)
        '''
        for success, info in helpers.parallel_bulk(es, d):
            if not success:
                print(success, info)
        '''
