"""Build chemidplus test data."""
from typing import Generator
from pathlib import Path
import xml.etree.ElementTree as ET

from therapy.etl import ChemIDplus
from therapy.database import Database

ch = ChemIDplus(Database())
ch._extract_data()
TEST_DATA_DIR = Path(__file__).resolve().parents[1] / "data"
outfile_path = TEST_DATA_DIR / ch._src_file.name

root = ET.Element("file")
root.set("name", ch._src_file.name)
root.set("date", ch._src_file.stem.split("chemidplus_")[1])


def parse_xml(path: Path) -> Generator:
    """Parse XML file and retrieve elements with matching tag value.
    :param Path path: path to XML file
    :param str tag: XML tag
    :return: generator yielding elements of corresponding tag
    """
    context = iter(ET.iterparse(path, events=("start", "end")))
    _, root = next(context)
    for event, elem in context:
        if event == "end" and elem.tag == "Chemical":
            yield elem
            root.clear()


concept_id_list = [
    "87-08-1",
    "152459-95-5",
    "220127-57-1",
    "15663-27-1",
    "50-06-6",
    "51186-83-5",
    "8025-81-8",
]

parser = parse_xml(ch._src_file)
for chemical in parser:
    regno = chemical.find("NumberList").find("CASRegistryNumber")
    if not regno:
        continue

    if regno.text in concept_id_list:
        root.append(chemical)

with open(outfile_path, "w") as f:
    ET.ElementTree(root).write(f, encoding="unicode")

pi = ET.ProcessingInstruction(
    target='xml version="1.0" encoding="UTF-8" standalone="yes"'
)
pi_string = ET.tostring(pi).decode("UTF8")
with open(outfile_path, "r+") as f:
    content = f.read()
    f.seek(0, 0)
    f.write(pi_string.rstrip("\r\n") + "\n" + content)
