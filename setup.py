#!/usr/bin/env python

import os
import re

from setuptools import setup, find_packages, Extension


ext = re.compile(r'\.pyx$')

extensions = [
    Extension(
        '.'.join((root.replace(os.sep, '.'), ext.sub('', f))),
        [os.path.join(root, f)]
    )
    for root, dirs, files in os.walk('bndl_cassandra')
    for f in files
    if not root.endswith('tests')
    if ext.search(f)
]

try:
    from Cython.Build.Dependencies import cythonize
    extensions = cythonize(extensions)
except ImportError:
    pass


setup(
    name='bndl_cassandra',
    version='0.3.2',
    url='https://stash.tgho.nl/projects/THCLUSTER/repos/bndl_cassandra/browse',
    description='Read from and write to Apache Cassandra with BNDL',
    long_description=open('README.md').read(),
    author='Frens Jan Rumph',
    author_email='mail@frensjan.nl',

    packages=(
        find_packages(exclude=["*.tests", "*.tests.*"])
    ),

    include_package_data=True,
    zip_safe=False,

    install_requires=[
        'bndl>=0.2.0',
        'cassandra-driver',
        'lz4',
        'scales',
    ],

    extras_require=dict(
        dev=[
            'bndl[dev]',
        ],
    ),

    ext_modules=extensions,

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
