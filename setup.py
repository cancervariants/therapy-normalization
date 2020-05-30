"""Defines how metakb is packaged and distributed."""
from setuptools import setup

setup(name='thera-py',
      version='0.0.1',
      description='VICC normalization routine for therapies',
      url='https://github.com/cancervariants/therapy-normalization',
      author='VICC',
      author_email='help@cancervariants.org',
      license='MIT',
      packages=['therapy'],
      zip_safe=False)
