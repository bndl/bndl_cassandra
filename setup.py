#!/usr/bin/env python

import os.path

from setuptools import setup, find_packages


with open(os.path.join(os.path.dirname(__file__), 'bndl_cassandra', '__version__.py')) as f:
    version = {}
    exec(f.read(), version)
    version = version['version']

setup(
    name='bndl_cassandra',
    version=version,
    url='https://stash.tgho.nl/projects/THCLUSTER/repos/bndl_cassandra/browse',
    description='Read from and write to Apache Cassandra with BNDL',
    long_description=open('README.rst').read(),
    author='Frens Jan Rumph',
    author_email='mail@frensjan.nl',

    packages=(
        find_packages()
    ),

    include_package_data=True,
    zip_safe=False,

    install_requires=[
        'bndl>=0.3.1',
        'cassandra-driver',
        'lz4',
        'scales',
    ],

    extras_require=dict(
        dev=[
            'bndl[dev]',
            'ccm',
        ],
    ),

    entry_points={
        'bndl.plugin':[
            'bndl_cassandra=bndl_cassandra',
        ]
    },

    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
)
