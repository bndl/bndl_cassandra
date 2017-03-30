#!/usr/bin/env python

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
        'bndl>=0.5.3',
        'cassandra-driver>=3.7.0',
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
