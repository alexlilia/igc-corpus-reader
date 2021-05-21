import os, os.path, sys
from pathlib import Path
import glob
from xml.etree import ElementTree

def run(files):
    xml_files = list(map(str,Path(files).rglob('*.xml')))
    for xml_file in xml_files:
        data = ElementTree.fromstring(open(xml_file,encoding='utf-8').read())
        for s in data.iter('{http://www.tei-c.org/ns/1.0}s'):
            print(s.text)
            print(s.attrib)
# run('F:\\Academics\\PhD\\Icelandic\\MalfongCorpus')
