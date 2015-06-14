#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: sw=4:ts=4:expandtab

""" Miscellaneous CKAN Datastore scripts """

from __future__ import (
    absolute_import, division, print_function, with_statement,
    unicode_literals)

import traceback
import sys

from os import unlink, environ, path as p
from manager import Manager
from . import utils
from . import api

manager = Manager()

CHUNKSIZE_ROWS = 10 ** 3
CHUNKSIZE_BYTES = 2 ** 20


def update_resource(ckan, resource_id, filepath, **kwargs):
    chunk_rows = kwargs.get('chunksize_rows')
    primary_key = kwargs.get('primary_key')
    method = 'upsert' if primary_key else 'insert'
    create_keys = ['aliases', 'primary_key', 'indexes']

    extension = p.splitext(filepath)[1].split('.')[1]
    switch = {'xls': 'read_xls', 'xlsx': 'read_xls', 'csv': 'read_csv'}
    parser = getattr(utils, switch.get(extension))
    records = iter(parser(filepath, encoding=kwargs.get('encoding')))
    fields = list(utils.gen_fields(records.next().keys()))
    create_kwargs = dict((k, v) for k, v in kwargs.items() if k in create_keys)

    if not primary_key:
        ckan.delete_table(resource_id)

    insert_kwargs = {'chunksize': chunk_rows, 'method': method}
    ckan.create_table(resource_id, fields, **create_kwargs)
    ckan.insert_records(resource_id, records, **insert_kwargs)


def update_hash_table(ckan, resource_id, resource_hash):
    create_kwargs = {
        'resource_id': ckan.hash_table_id,
        'fields': [
            {'id': 'datastore_id', 'type': 'text'},
            {'id': 'hash', 'type': 'text'}],
        'primary_key': 'datastore_id'
    }

    ckan.create_table(**create_kwargs)
    records = [{'datastore_id': resource_id, 'hash': resource_hash}]
    ckan.insert_records(ckan.hash_table_id, records, method='upsert')


@manager.command
def ver():
    """Show ckanny version"""
    from . import __version__ as version
    print('v%s' % version)


@manager.arg('resource_id', help='the resource id')
@manager.arg('remote', 'r', help=('the remote ckan url (uses %s ENV if '
    'available)' % api.REMOTE_ENV), default=environ.get(api.REMOTE_ENV))
@manager.arg('api_key', 'k', help='the api key (uses %s ENV if available)' % (
    api.API_KEY_ENV), default=environ.get(api.API_KEY_ENV))
@manager.arg(
    'hash_table_id', 'H', help=('the hash table resource id (uses %s ENV if '
    'available)' % api.HASH_TABLE_ENV), default=environ.get(api.HASH_TABLE_ENV))
@manager.arg('ua', 'u', help=('the user agent (uses %s ENV if '
    'available)' % api.UA_ENV), default=environ.get(api.UA_ENV))
@manager.arg('chunksize_rows', 'c', help='number of rows to write at a time',
    type=int, default=CHUNKSIZE_ROWS)
@manager.arg('chunksize_bytes', 'C', help=('number of bytes to read/write at a'
    ' time'), type=int, default=CHUNKSIZE_BYTES)
@manager.arg('primary_key', 'p', help="Unique field(s), e.g., 'field1,field2'")
@manager.arg(
    'quiet', 'q', help='suppress debug statements', type=bool, default=False)
@manager.arg(
    'force', 'f', help="update resource even if it hasn't changed.",
    type=bool, default=False)
@manager.command
def dsupdate(resource_id, **kwargs):
    """Update a datastore table"""
    verbose = not kwargs.get('quiet')
    chunk_bytes = kwargs.get('chunksize_bytes')
    force = kwargs.get('force')
    ckan_kwargs = dict((k, v) for k, v in kwargs.items() if k in api.CKAN_KEYS)

    try:
        ckan = api.CKAN(**ckan_kwargs)
        r, filepath = ckan.fetch_resource(resource_id, chunksize=chunk_bytes)

        if ckan.hash_table_id and not force:
            old_hash = ckan.get_hash(resource_id)
            hash_kwargs = {'chunksize': chunk_bytes, 'verbose': verbose}
            new_hash = utils.hash_file(filepath, **hash_kwargs)
            doesnt_need_update = new_hash == old_hash
        elif force:
            doesnt_need_update = False

        if ckan.hash_table_id and doesnt_need_update and not force:
            if verbose:
                print('No new data found. Not updating datastore.')
            sys.exit(0)
        elif force and verbose:
            print('Hashes not checked due to forced update. Updating datastore...')
        elif ckan.hash_table_id and verbose:
            print('New data found. Updating datastore...')
        elif verbose:
            print('`hash_table_id` not set. Updating datastore...')

        kwargs['encoding'] = r.encoding
        update_resource(ckan, resource_id, filepath, **kwargs)

        if ckan.hash_table_id:
            update_hash_table(ckan, resource_id, new_hash)
    except Exception as err:
        sys.stderr.write('ERROR: %s\n' % str(err))
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)
    finally:
        print('Removing tempfile...')
        unlink(filepath)


@manager.arg('resource_id', help='the resource id')
@manager.arg('remote', 'r', help='the remote ckan url')
@manager.arg('api_key', 'k', help='the api key (uses %s ENV if available)' % (
    api.API_KEY_ENV), default=environ.get(api.API_KEY_ENV))
@manager.arg('ua', 'u', help='the user agent',
    default=api.DEF_USER_AGENT)
@manager.arg(
    'filters', 'f', help=('the filters to apply before deleting, e.g., {"name"'
    ': "fred"}'))
@manager.command
def dsdelete(resource_id, **kwargs):
    """Delete a datastore table"""
    ckan_kwargs = dict((k, v) for k, v in kwargs.items() if k in api.CKAN_KEYS)

    try:
        ckan = api.CKAN(**ckan_kwargs)
        ckan.delete_table(resource_id, filters=kwargs.get('filters'))
    except Exception as err:
        sys.stderr.write('ERROR: %s\n' % str(err))
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)


if __name__ == '__main__':
    manager.main()
