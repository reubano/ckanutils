#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: sw=4:ts=4:expandtab

""" Miscellaneous CKAN Datastore scripts """

from __future__ import (
    absolute_import, division, print_function, with_statement,
    unicode_literals)

from manager import Manager
from . import datastorer
from . import filestorer

manager = Manager()
manager.merge(datastorer.manager, namespace='ds')
manager.merge(filestorer.manager, namespace='fs')


@manager.command
def ver():
    """Show ckanny version"""
    from . import __version__ as version
    print('v%s' % version)


if __name__ == '__main__':
    manager.main()
