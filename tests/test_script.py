#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: sw=4:ts=4:expandtab

""" A script to test CKAN Utils functionality """

from __future__ import (
    absolute_import, division, print_function, with_statement,
    unicode_literals)

from sys import exit
from os import path as p
from scripttest import TestFileEnvironment
from ckanutils import __version__ as version

parent_dir = p.abspath(p.dirname(p.dirname(__file__)))
script = p.join(parent_dir, 'bin', 'ckanny')


def main(verbose=False):
    env = TestFileEnvironment('.scripttest')
    test_num = 1

    # Test main usage
    result = env.run('%s --help' % script)

    if verbose:
        print(result.stdout)

    assert result.stdout.split('\n')[0] == 'usage: ckanny <command> [<args>]'
    print('\nScripttest: #%i ... ok' % test_num)
    test_num += 1

    # Test dsdelete usage
    result = env.run('%s dsdelete --help' % script)

    if verbose:
        print(result.stdout)

    assert ' '.join(result.stdout.split(' ')[:3]) == 'usage: ckanny [-h]'
    print('Scripttest: #%i ... ok' % test_num)
    test_num += 1

    # Test dsupdate usage
    result = env.run('%s dsupdate --help' % script)

    if verbose:
        print(result.stdout)

    assert ' '.join(result.stdout.split(' ')[:3]) == 'usage: ckanny [-h]'
    print('Scripttest: #%i ... ok' % test_num)
    test_num += 1

    # Test version
    result = env.run('%s ver' % script)

    if verbose:
        print(result.stdout)

    assert result.stdout.split('\n')[0] == 'v%s' % version
    print('Scripttest: #%i ... ok' % test_num)
    print('-----------------------------')
    print('Ran %i tests\n\nOK' % test_num)
    exit(0)


if __name__ == '__main__':
    main()
