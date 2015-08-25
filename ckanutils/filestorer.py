#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: sw=4:ts=4:expandtab

""" Miscellaneous CKAN Filestore scripts """

from __future__ import (
    absolute_import, division, print_function, with_statement,
    unicode_literals)

import traceback
import sys

from os import unlink, getcwd, environ, path as p
from tempfile import NamedTemporaryFile

from manager import Manager
from xattr import xattr
from tabutils import process as tup, io as tio

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
    'name_from_id', 'n', help='Use resource id for filename', type=bool,
    default=False)
@manager.arg(
    'quiet', 'q', help='suppress debug statements', type=bool, default=False)
@manager.command
def fetch(resource_id, **kwargs):
    """Downloads a filestore resource"""
    verbose = not kwargs['quiet']
    filepath = kwargs['destination']
    name_from_id = kwargs.get('name_from_id')
    chunksize = kwargs.get('chunksize_bytes')
    ckan_kwargs = {k: v for k, v in kwargs.items() if k in api.CKAN_KEYS}

    try:
        ckan = api.CKAN(**ckan_kwargs)
        r = ckan.fetch_resource(resource_id)
        fkwargs = {
            'headers': r.headers,
            'name_from_id': name_from_id,
            'resource_id': resource_id}

        filepath = tup.make_filepath(filepath, **fkwargs)
        tio.write_file(filepath, r.iter_content, chunksize=chunksize)

        # save encoding to extended attributes
        x = xattr(filepath)

        if verbose and r.encoding:
            print('saving encoding %s to extended attributes' % r.encoding)

        if r.encoding:
            x['com.ckanutils.encoding'] = r.encoding

        print(filepath)
    except api.NotAuthorized as err:
        sys.stderr.write('ERROR: %s\n' % str(err))
        sys.exit(1)
    except Exception as err:
        sys.stderr.write('ERROR: %s\n' % str(err))
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)


@manager.arg(
    'resource_id', help='the resource id', nargs='?', default=sys.stdin)
@manager.arg(
    'src_remote', 's', help=('the source remote ckan url (uses `%s` ENV'
    ' if available)') % api.REMOTE_ENV, default=environ.get(api.REMOTE_ENV))
@manager.arg(
    'dest_remote', 'd', help=('the destination remote ckan url (uses `%s` ENV'
    ' if available)') % api.REMOTE_ENV, default=environ.get(api.REMOTE_ENV))
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
def migrate(resource_id, **kwargs):
    """Copies a filestore resource from one ckan instance to another"""
    src_remote, dest_remote = kwargs['src_remote'], kwargs['dest_remote']

    if src_remote == dest_remote:
        sys.stderr.write(
            'ERROR: `dest-remote` of %s is the same as `src-remote` of %s.\n'
            'The dest and src remotes must be different.\n' % (src_remote,
            dest_remote))

        sys.exit(1)

    verbose = not kwargs['quiet']
    chunksize = kwargs['chunksize_bytes']
    ckan_kwargs = {k: v for k, v in kwargs.items() if k in api.CKAN_KEYS}

    try:
        src_ckan = api.CKAN(remote=src_remote, **ckan_kwargs)
        dest_ckan = api.CKAN(remote=dest_remote, **ckan_kwargs)
        r = src_ckan.fetch_resource(resource_id)
        filepath = NamedTemporaryFile(delete=False).name
        tio.write_file(filepath, r.raw.read(), chunksize=chunksize)
    except api.NotAuthorized as err:
        sys.stderr.write('ERROR: %s\n' % str(err))
        sys.exit(1)
    except Exception as err:
        sys.stderr.write('ERROR: %s\n' % str(err))
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)
    else:
        resource = dest_ckan.update_filestore(resource_id, filepath=filepath)

        if resource and verbose:
            print('Success! Resource %s updated.' % resource_id)
        elif not resource:
            sys.exit('Error uploading file!')
    finally:
        if verbose:
            print('Removing tempfile...')

        unlink(filepath)


@manager.arg(
    'source', help='the source file path', nargs='?', default=sys.stdin)
@manager.arg(
    'resource_id', 'R', help=('the resource id to update (default: source file'
        ' name)'))
@manager.arg(
    'package_id', 'p', help='the package id (used to create a new resource)')
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
    'url', 'U', help='treat source as a url', type=bool, default=False)
@manager.arg(
    'quiet', 'q', help='suppress debug statements', type=bool, default=False)
@manager.command
def upload(source, resource_id=None, package_id=None, **kwargs):
    """Updates the filestore of an existing resource or creates a new one"""
    verbose = not kwargs['quiet']
    resource_id = resource_id or p.splitext(p.basename(source))[0]
    ckan_kwargs = {k: v for k, v in kwargs.items() if k in api.CKAN_KEYS}

    if package_id and verbose:
        print(
            'Creating filestore resource %s in dataset %s...' %
            (source, package_id))
    elif verbose:
        print(
            'Uploading %s to filestore resource %s...' % (source, resource_id))

    try:
        ckan = api.CKAN(**ckan_kwargs)
    except Exception as err:
        sys.stderr.write('ERROR: %s\n' % str(err))
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    resource_kwargs = {'url' if kwargs.get('url') else 'filepath': source}
    if package_id:
        resource = ckan.create_resource(package_id, **resource_kwargs)
    else:
        resource = ckan.update_filestore(resource_id, **resource_kwargs)

    if package_id and resource and verbose:
        infix = '%s ' % resource['id'] if resource.get('id') else ''
        print('Success! Resource %screated.' % infix)
    elif resource and verbose:
        print('Success! Resource %s updated.' % resource_id)
    elif not resource:
        sys.exit('Error uploading file!')


if __name__ == '__main__':
    manager.main()
