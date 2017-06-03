"""
Lightflow-Epics
-----

An epics extension for Lightflow.

It adds tasks for common epics operations as well as a trigger task that monitors
a PV and starts one or more dags.

"""

from setuptools import setup, find_packages
import re

with open('lightflow_epics/__init__.py') as file:
    version = re.search(r"__version__ = '(.*)'", file.read()).group(1)

setup(
    name='Lightflow-Epics',
    version=version,
    description='An epics extension for Lightflow.',
    long_description=__doc__,
    url='https://bitbucket.synchrotron.org.au/projects/DR/repos/lightflow-epics/browse',

    author='The Australian Synchrotron Python Group',
    author_email='python@synchrotron.org.au',

    classifiers=[
        'Development Status :: 3 - Alpha',
        'Topic :: Scientific/Engineering',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Natural Language :: English',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
    ],

    packages=find_packages(exclude=['tests', 'examples']),

    install_requires=[
        'lightflow>=1.6.1',
        'pyepics>=3.2.6'
    ],

)
