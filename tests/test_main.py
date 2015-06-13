# -*- coding: utf-8 -*-
# vim: sw=4:ts=4:expandtab

"""
tests.test_main
~~~~~~~~~~~~~~~

Provides unit tests for the website.
"""

import nose.tools as nt

from . import stderr
from pprint import pprint


def setup_module():
    """site initialization"""
    global initialized
    initialized = True
    print('Site Module Setup\n')


class TestMain:
    """Main unit tests"""
    def __init__(self):
        self.cls_initialized = False

    def test_home(self):
        nt.assert_equal(self.cls_initialized, False)
