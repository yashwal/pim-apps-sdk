# -*- coding: utf-8 -*-
#
# This file were created by Python Boilerplate. Use boilerplate to start simple
# usable and best-practices compliant Python projects.
#
# Learn more about it at: http://github.com/fabiommendes/python-boilerplate/
#

import os
import codecs
from setuptools import setup, find_packages

# Save version and author to __meta__.py
version = open('VERSION').read().strip()
dirname = os.path.dirname(__file__)
path = os.path.join(dirname, 'src', 'pim_apps', '__meta__.py')
meta = '''# Automatically created. Please do not edit.
__version__ = '%s'
__author__ = 'Yashaskara Jois'
''' % version
with open(path, 'w') as F:
    F.write(meta)

setup(
    # Basic info
    name='pim-apps',
    version=version,
    author='Yashaskara Jois',
    author_email='yashaskara.jois@unbxd.com',
    url='',
    description='A short description for your project.',
    long_description=codecs.open('README.rst', 'rb', 'utf8').read(),

    # Classifiers (see https://pypi.python.org/pypi?%3Aaction=list_classifiers)
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Topic :: Software Development :: Libraries',
    ],

    # Packages and dependencies
    package_dir={'': 'src'},
    packages=find_packages('src'),
    install_requires=[
        "requests", "boto3==1.21.38", "botocore==1.24.38", "certifi==2021.10.8", "charset-normalizer==2.0.12",
        "idna==3.3", "jmespath==1.0.0", "numpy==1.22.3", "pandas==1.4.2", "python-dateutil==2.8.2", "pytz==2022.1",
        "requests==2.27.1", "s3transfer==0.5.2", "six==1.16.0", "urllib3==1.26.9",
    ],
    extras_require={
        'dev': [
            'python-boilerplate[dev]',
        ],
    },

    # Other configurations
    zip_safe=False,
    platforms='any',
)
