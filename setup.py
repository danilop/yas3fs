from setuptools import setup, find_packages

import sys

execfile('yas3fs/_version.py')

requires = ['setuptools>=2.2', 'boto>=2.25.0', 'fusepy>=2.0.2']

# Versions of Python pre-2.7 require argparse separately. 2.7+ and 3+ all
# include this as the replacement for optparse.
if sys.version_info[:2] < (2, 7):
    requires.append("argparse")

setup(
    name='yas3fs',
    version=__version__,
    description='YAS3FS (Yet Another S3-backed File System) is a Filesystem in Userspace (FUSE) interface to Amazon S3.',
    packages=find_packages(),
    author='Danilo Poccia',
    author_email='dpoccia@gmail.com',
    url='https://github.com/danilop/yas3fs',
    install_requires=requires,
    entry_points = { 'console_scripts': ['yas3fs = yas3fs:main'] },
    )
