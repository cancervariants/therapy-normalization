"""Defines how metakb is packaged and distributed."""
from setuptools import setup, find_packages
from therapy import __version__

setup(version=__version__,
      packages=find_packages(),
      zip_safe=False)
