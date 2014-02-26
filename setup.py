from setuptools import setup
import sys

# Versions of Python pre-2.7 require argparse separately. 2.7+ and 3+ all
# include this as the replacement for optparse.
if sys.version_info[:2] < (2, 7):
    install_requires.append("argparse")

setup(
    name='yas3fs',
    version='2.1.0',
    description='YAS3FS (Yet Another S3-backed File System) is a Filesystem in Userspace (FUSE) interface to Amazon S3.',
    packages=['yas3fs'],
    author='Danilo Poccia',
    author_email='dpoccia@gmail.com',
    install_requires=['boto>=2.25.0', 'fusepy>=2.0.2'],
    entry_points = { 'console_scripts': ['yas3fs = yas3fs:main'] },
    )
