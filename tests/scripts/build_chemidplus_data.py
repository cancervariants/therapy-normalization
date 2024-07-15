"""Build chemidplus test data."""

from collections.abc import Generator
from pathlib import Path
from xml.etree import ElementTree

from therapy.database import create_db
from therapy.etl import ChemIDplus

TEST_IDS = [
    "87-08-1",
    "152459-95-5",
    "220127-57-1",
    "15663-27-1",
    "50-06-6",
    "51186-83-5",
    "8025-81-8",
    "112901-68-5",
    "20537-88-6",
]

ch = ChemIDplus(create_db())
ch._extract_data(False)
TEST_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "chemidplus"
outfile_path = TEST_DATA_DIR / ch._data_file.name

root = ElementTree.Element("file")
root.set("name", ch._data_file.name)
root.set("date", ch._data_file.stem.split("chemidplus_")[1])


def parse_xml(path: Path) -> Generator:
    """Parse XML file and retrieve elements with matching tag value.
    :param Path path: path to XML file
    :param str tag: XML tag
    :return: generator yielding elements of corresponding tag
    """
    context = iter(ElementTree.iterparse(path, events=("start", "end")))  # noqa: S314
    _, root = next(context)
    for event, elem in context:
        if event == "end" and elem.tag == "Chemical":
            yield elem
            root.clear()


parser = parse_xml(ch._data_file)
for chemical in parser:
    regno = chemical.find("NumberList").find("CASRegistryNumber")
    if not regno:
        continue

    if regno.text in TEST_IDS:
        root.append(chemical)

with outfile_path.open("w") as f:
    ElementTree.ElementTree(root).write(f, encoding="unicode")

pi = ElementTree.ProcessingInstruction(
    target='xml version="1.0" encoding="UTF-8" standalone="yes"'
)
pi_string = ElementTree.tostring(pi).decode("UTF8")
with outfile_path.open("r+") as f:
    content = f.read()
    f.seek(0, 0)
    f.write(pi_string.rstrip("\r\n") + "\n" + content)
