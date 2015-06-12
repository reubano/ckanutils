#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" A script to test CKAN Utils functionality """

from sys import exit, stderr
from scripttest import TestFileEnvironment


def main():
    try:
        env = TestFileEnvironment('.scripttest')
        result = env.run('ckanutils --help')
        print('%s' % result.stdout)

    except Exception as err:
        stderr.write('ERROR: %s\n' % str(err))

    exit(0)


if __name__ == '__main__':
    main()
