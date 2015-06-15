#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import re
import ckanutils

from os import system, path as p

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages


def read(filename, parent=None):
    parent = (parent or __file__)

    try:
        with open(p.join(p.dirname(parent), filename)) as f:
            return f.read()
    except IOError:
        return ''


def parse_requirements(filename, parent=None, dep=False):
    parent = (parent or __file__)
    filepath = p.join(p.dirname(parent), filename)
    content = read(filename, parent)

    for line_number, line in enumerate(content.splitlines(), 1):
        candidate = line.strip()

        if candidate.startswith('-r'):
            for item in parse_requirements(candidate[2:].strip(), filepath, dep):
                yield item
        elif not dep and '#egg=' in candidate:
            yield re.sub('.*#egg=(.*)-(.*)', r'\1==\2', candidate)
        elif dep and '#egg=' in candidate:
            yield candidate.replace('-e ', '')
        elif not dep:
            yield candidate

# Avoid byte-compiling the shipped template
sys.dont_write_bytecode = True

if sys.argv[-1] == 'publish':
    system('python setup.py sdist upload')
    sys.exit()

# if sys.argv[-1] == 'info':
#     for k, v in ckanutils.items():
#         print('%s: %s' % (k, v))

#     sys.exit()

requirements = parse_requirements('requirements.txt')
requirements += ['argparse'] if sys.version_info < (2, 7) else []
dependencies = list(parse_requirements('requirements.txt', dep=True))
readme = read('README.rst')
history = read('HISTORY.rst').replace('.. :changelog:', '')
license = ckanutils.__license__
classifier = {'GPL': 'GNU General Public', 'MIT': 'The MIT', 'BSD': 'The BSD'}

# [metadata]
# classifier = Development Status :: 1 - Planning
# classifier = Development Status :: 2 - Pre-Alpha
# classifier = Development Status :: 3 - Alpha
# classifier = Development Status :: 4 - Beta
# classifier = Development Status :: 5 - Production/Stable
# classifier = Development Status :: 6 - Mature
# classifier = Development Status :: 7 - Inactive
# classifier = License :: OSI Approved :: GNU General Public License (GPL)
# classifier = License :: OSI Approved :: The MIT License (MIT)
# classifier = License :: OSI Approved :: The BSD License (BSD)
# classifier = Environment :: Console
# classifier = Environment :: Web Environment
# classifier = Intended Audience :: End Users/Desktop
# classifier = Intended Audience :: Developers
# classifier = Intended Audience :: System Administrators
# classifier = Operating System :: MacOS :: MacOS X
# classifier = Operating System :: Microsoft :: Windows
# classifier = Operating System :: POSIX
#
# [files]
# packages = someprogram
# resources =

setup(
    name=ckanutils.__title__,
    version=ckanutils.__version__,
    description=ckanutils.__description__,
    long_description=readme + '\n\n' + history,
    author=ckanutils.__author__,
    author_email=ckanutils.__email__,
    url='https://github.com/reubano/ckanutils',
    packages=find_packages(exclude=['docs', 'tests']),
    include_package_data=True,
    install_requires=requirements,
    dependency_links=dependencies,
    tests_require=['nose', 'scripttest'],
    license=license,
    zip_safe=False,
    keywords=ckanutils.__title__,
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'License :: OSI Approved :: %s License (%s)' % (classifier[license], license),
        'Natural Language :: English',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
    ],
    platforms=['MacOS X', 'Windows', 'Linux'],
    scripts=[p.join('bin', ckanutils)],
)
