#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: sw=4:ts=4:expandtab

""" Miscellaneous CKAN Filestore scripts """

from __future__ import (
    absolute_import, division, print_function, with_statement,
    unicode_literals)

import traceback
import sys

from os import getcwd, environ
from manager import Manager
from xattr import xattr
from . import api

manager = Manager()


@manager.arg(
    'resource_id', help='the resource id', nargs='?', default=sys.stdin)
@manager.arg(
    'destination', 'd', help='the destination folder or file path',
    default=getcwd())
@manager.arg(
    'remote', 'r', help='the remote ckan url (uses `%s` ENV if available)' %
    api.REMOTE_ENV, default=environ.get(api.REMOTE_ENV))
@manager.arg(
    'api_key', 'k', help='the api key (uses `%s` ENV if available)' %
    api.API_KEY_ENV, default=environ.get(api.API_KEY_ENV))
@manager.arg(
    'ua', 'u', help='the user agent (uses `%s` ENV if available)' % api.UA_ENV,
    default=environ.get(api.UA_ENV))
@manager.arg(
    'chunksize_bytes', 'c', help='number of bytes to read/write at a time',
    type=int, default=api.CHUNKSIZE_BYTES)
@manager.arg(
    'quiet', 'q', help='suppress debug statements', type=bool, default=False)
@manager.command
def fetch(resource_id, **kwargs):
    """Downloads a filestore resource"""
    verbose = not kwargs['quiet']
    ckan_kwargs = {k: v for k, v in kwargs.items() if k in api.CKAN_KEYS}
    fetch_kwargs = {
        'filepath': kwargs['destination'],
        'chunksize': kwargs['chunksize_bytes']
    }

    try:
        ckan = api.CKAN(**ckan_kwargs)
        r, filepath = ckan.fetch_resource(resource_id, **fetch_kwargs)

        # save encoding to extended attributes
        x = xattr(filepath)

        if verbose and r.encoding:
            print('saving encoding %s to extended attributes' % r.encoding)

        if r.encoding:
            x['com.ckanutils.encoding'] = r.encoding

        print(filepath)
    except Exception as err:
        sys.stderr.write('ERROR: %s\n' % str(err))
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)


if __name__ == '__main__':
    manager.main()
