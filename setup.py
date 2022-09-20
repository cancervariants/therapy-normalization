"""Defines how metakb is packaged and distributed."""
from setuptools import setup

exec(open("therapy/version.py").read())
setup(version=__version__)  # noqa: F821
