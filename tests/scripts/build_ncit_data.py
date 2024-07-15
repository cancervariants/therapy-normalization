"""Construct test data for NCIt source."""

import xml.etree.ElementTree as XETree
from collections.abc import Generator
from pathlib import Path

import lxml.etree as etr
import owlready2 as owl
import xmlformatter

from therapy.database import create_db
from therapy.etl import NCIt

# define captured ids in `test_classes` variable

NCIT_PREFIX = "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl"
OWL_PREFIX = "{http://www.w3.org/2002/07/owl#}"
RDFS_PREFIX = "{http://www.w3.org/2000/01/rdf-schema#}"
RDF_PREFIX = "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}"

ANNOTATION_PROPERTY_TAG = f"{OWL_PREFIX}AnnotationProperty"
DESCRIPTION_TAG = f"{RDF_PREFIX}Description"
ABOUT_ATTRIB = f"{RDF_PREFIX}about"
AXIOM_TAG = f"{OWL_PREFIX}Axiom"
DATATYPE_TAG = f"{RDFS_PREFIX}Datatype"
OBJECT_PROPERTY_TAG = f"{OWL_PREFIX}ObjectProperty"
CLASS_TAG = f"{OWL_PREFIX}Class"


ncit = NCIt(create_db())
ncit._extract_data(False)
TEST_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "ncit"
outfile_path = TEST_DATA_DIR / ncit._data_file.name


def ncit_parser() -> Generator:
    """Get unique XML elements."""
    context = iter(
        etr.iterparse(ncit._data_file, events=("start", "end"), huge_tree=True)
    )
    for event, elem in context:
        if event == "start":
            yield elem


parser = ncit_parser()

# make root element
root = next(parser)
nsmap = root.nsmap
new_root = etr.Element(f"{RDF_PREFIX}RDF", nsmap=nsmap)
new_root.set(
    "{http://www.w3.org/XML/1998/namespace}base",
    "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl",
)

# add ontology element
element = next(parser)
new_root.append(element)

# get annotation property elements
element = next(parser)
while element.tag != ANNOTATION_PROPERTY_TAG:
    element = next(parser)

while element.tag != DESCRIPTION_TAG:
    new_root.append(element)
    element = next(parser)
    while element.tag != ANNOTATION_PROPERTY_TAG and element.tag != DESCRIPTION_TAG:
        if element.tag == AXIOM_TAG:
            new_root.append(element)
        element = next(parser)

# get description elements
descriptions = {}
while element.tag != DATATYPE_TAG:
    new_root.append(element)
    element = next(parser)
    while element.tag != DESCRIPTION_TAG and element.tag != DATATYPE_TAG:
        if element.tag == AXIOM_TAG:
            new_root.append(element)
        element = next(parser)

# get datatype elements
while element.tag != OBJECT_PROPERTY_TAG:
    new_root.append(element)
    element = next(parser)
    while (
        element.tag != DATATYPE_TAG or len(element.attrib) == 0
    ) and element.tag != OBJECT_PROPERTY_TAG:
        if element.tag == AXIOM_TAG:
            new_root.append(element)
        element = next(parser)

# get object property elements
while element.tag != CLASS_TAG:
    new_root.append(element)
    element = next(parser)
    while element.tag != OBJECT_PROPERTY_TAG and element.tag != CLASS_TAG:
        if element.tag == AXIOM_TAG:
            new_root.append(element)
        element = next(parser)

# get classes
onto = owl.get_ontology(ncit._data_file.absolute().as_uri())
onto.load()
test_classes = {
    onto.C95221,
    onto.C74021,
    onto.C1647,
    onto.C49236,
    onto.C61796,
    onto.C376,
    onto.C739,
    onto.C839,
    onto.C107245,
    onto.C488,
    onto.C66724,
}

parent_concepts = set()
for c in test_classes:
    parent_concepts |= c.ancestors()  # type: ignore
parent_concepts.remove(owl.Thing)
parent_concept_iris = {p.iri for p in parent_concepts}

while element.attrib.get(ABOUT_ATTRIB) != f"{NCIT_PREFIX}#term-group":
    elements = [element]
    element = next(parser)
    while (
        element.tag != CLASS_TAG or ABOUT_ATTRIB not in element.attrib
    ) and element.attrib.get(ABOUT_ATTRIB) != f"{NCIT_PREFIX}#term-group":
        if element.tag == AXIOM_TAG:
            elements.append(element)
        element = next(parser)
    if elements[0].attrib.get(ABOUT_ATTRIB) in parent_concept_iris:
        for e in elements:
            new_root.append(e)
    else:
        for e in elements:
            e.clear()

# get annotations
EOF = False
while not EOF:
    new_root.append(element)
    element = next(parser)
    while True:
        if element.tag == AXIOM_TAG:
            new_root.append(element)
        try:
            element = next(parser)
        except StopIteration:
            EOF = True
            break
        if element.tag == DESCRIPTION_TAG:
            break

etr.ElementTree(new_root).write(outfile_path, pretty_print=True)

pi = XETree.ProcessingInstruction(  # TODO get encoding attrib out
    target='xml version="1.0"'
)
pi_string = XETree.tostring(pi).decode("ASCII")
with outfile_path.open("r+") as f:
    content = f.read()
    f.seek(0, 0)
    f.write(pi_string.rstrip("\r\n") + "\n" + content)

formatter = xmlformatter.Formatter(indent=2)
formatter.format_file(outfile_path)
