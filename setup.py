# Copyright 2018 Spotify AB. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import setuptools

__location__ = os.path.realpath(os.path.dirname(__file__))

with open(os.path.join(__location__, 'requirements.txt')) as f:
    requirements = f.read().splitlines()

git_requirements = [r for r in requirements if r.startswith('-e')]
requirements = [r for r in requirements if not r.startswith('-e')]

setuptools.setup(
    name="comet-core",
    version="2.0.0",
    url="https://github.com/spotify/comet-core",

    author="Spotify Platform Security",
    author_email="wasabi@spotify.com",

    description="Comet Distributed Security Notification Framework",
    long_description=open('README.md', 'r+', encoding='utf-8').read(),

    packages=['comet_core'],

    install_requires=requirements,
    dependency_links=git_requirements,

    include_package_data=True,

    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6'
    ],
)
