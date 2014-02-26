from setuptools import setup, find_packages
import sys

install_requires = ['boto', 'fusepy']

# Versions of Python pre-2.7 require argparse separately. 2.7+ and 3+ all
# include this as the replacement for optparse.
#if sys.version_info[:2] < (2, 7):
#    install_requires.append("argparse")

setup(
  name="yas3fs",
  version="2.1.0",
  packages = find_packages(),
  author="Danilo Poccia",
  install_requires=install_requires,
  entry_points = { 'console_scripts': ['yas3fs = yas3fs:main'] },
);
