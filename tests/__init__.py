# -*- coding: utf-8 -*-
"""
    ckanutils.tests
    ~~~~~~~~~~~~~~~~

    Provides application unit tests
"""

from sys import stderr

initialized = False


def setup_package():
    """database context creation"""
    global initialized
    initialized = True
    print('Test Package Setup\n')


def teardown_package():
    """database context removal"""
    global initialized
    initialized = False
    print('Test Package Teardown\n')
