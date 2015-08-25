#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import re
import ckanutils

from os import path as p

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
            args = [candidate[2:].strip(), filepath, dep]

            for item in parse_requirements(*args):
                yield item
        elif not dep and '#egg=' in candidate:
            yield re.sub('.*#egg=(.*)-(.*)', r'\1==\2', candidate)
        elif dep and '#egg=' in candidate:
            yield candidate.replace('-e ', '')
        elif not dep:
            yield candidate

# Avoid byte-compiling the shipped template
sys.dont_write_bytecode = True

requirements = list(parse_requirements('requirements.txt'))
dev_requirements = list(parse_requirements('dev-requirements.txt'))
dependencies = list(parse_requirements('requirements.txt', dep=True))
readme = read('README.rst')
changes = read('CHANGES.rst').replace('.. :changelog:', '')
license = ckanutils.__license__

classifier = {
    'GPL': 'GNU General Public License (GPL)',
    'MIT': 'MIT License',
    'BSD': 'BSD License'
}

setup(
    name=ckanutils.__title__,
    version=ckanutils.__version__,
    description=ckanutils.__description__,
    long_description=readme + '\n\n' + changes,
    author=ckanutils.__author__,
    author_email=ckanutils.__email__,
    url='https://github.com/reubano/ckanutils',
    py_modules=['ckanutils'],
    include_package_data=True,
    install_requires=requirements,
    dependency_links=dependencies,
    tests_require=dev_requirements,
    license=license,
    zip_safe=False,
    keywords=ckanutils.__title__,
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: %s' % classifier[license],
        'Natural Language :: English',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: End Users/Desktop',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Operating System :: POSIX :: Linux',
    ],
    platforms=['MacOS X', 'Windows', 'Linux'],
)
