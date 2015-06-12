# -*- coding: utf-8 -*-
"""
    app.tests.test_site
    ~~~~~~~~~~~~~~

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