#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: sw=4:ts=4:expandtab

""" Miscellaneous CKAN Datastore scripts """

from __future__ import (
    absolute_import, division, print_function, with_statement,
    unicode_literals)

import traceback
import sys

from pprint import pprint
from os import unlink, environ, path as p
from manager import Manager
from xattr import xattr
from . import utils
from . import api

manager = Manager()


def get_message(unchanged, force, old_hash):
    needs_update = not unchanged

    if unchanged and not force:
        message = 'No new data found. Not updating datastore.'
    elif unchanged and force:
        message = 'No new data found, but update forced.'
        message += ' Updating datastore...'
    elif needs_update and old_hash:
        message = 'New data found. Updating datastore...'
    elif not old_hash:
        message = '`hash_table_id` not set or table not found.'
        message += ' Updating datastore...'

    return message


def update_resource(ckan, resource_id, filepath, **kwargs):
    verbose = not kwargs.get('quiet')
    chunk_rows = kwargs.get('chunksize_rows')
    primary_key = kwargs.get('primary_key')
    content_type = kwargs.get('content_type')
    method = 'upsert' if primary_key else 'insert'
    create_keys = ['aliases', 'primary_key', 'indexes']

    try:
        extension = p.splitext(filepath)[1].split('.')[1]
    # no file extension given, e.g., a tempfile
    except IndexError:
        extension = content_type.split('/')[1]

    xlsx_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml'
    xlsx_type += '.sheet'
    switch = {'xls': 'read_xls', 'csv': 'read_csv'}
    switch[xlsx_type] = 'read_xls'

    try:
        parser = getattr(utils, switch[extension])
    except IndexError:
        print("Error: plugin for extension '%s' not found!" % extension)
        return False
    else:
        records = iter(parser(filepath, encoding=kwargs.get('encoding')))
        fields = list(utils.gen_fields(records.next().keys()))

        if verbose:
            print('Parsed fields:')
            pprint(fields)

        create_kwargs = {k: v for k, v in kwargs.items() if k in create_keys}

        if not primary_key:
            ckan.delete_table(resource_id)

        insert_kwargs = {'chunksize': chunk_rows, 'method': method}
        ckan.create_table(resource_id, fields, **create_kwargs)
        ckan.insert_records(resource_id, records, **insert_kwargs)
        return True


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


@manager.arg(
    'resource_id', help='the resource id', nargs='?', default=sys.stdin)
@manager.arg(
    'remote', 'r', help='the remote ckan url (uses `%s` ENV if available)' %
    api.REMOTE_ENV, default=environ.get(api.REMOTE_ENV))
@manager.arg(
    'api_key', 'k', help='the api key (uses `%s` ENV if available)' %
    api.API_KEY_ENV, default=environ.get(api.API_KEY_ENV))
@manager.arg(
    'hash_table_id', 'H', help=('the hash table resource id (uses `%s` ENV if '
    'available)' % api.HASH_TABLE_ENV), default=environ.get(api.HASH_TABLE_ENV))
@manager.arg(
    'ua', 'u', help='the user agent (uses `%s` ENV if available)' % api.UA_ENV,
    default=environ.get(api.UA_ENV))
@manager.arg(
    'chunksize_rows', 'c', help='number of rows to write at a time',
    type=int, default=api.CHUNKSIZE_ROWS)
@manager.arg(
    'chunksize_bytes', 'C', help='number of bytes to read/write at a time',
    type=int, default=api.CHUNKSIZE_BYTES)
@manager.arg('primary_key', 'p', help="Unique field(s), e.g., 'field1,field2'")
@manager.arg(
    'quiet', 'q', help='suppress debug statements', type=bool, default=False)
@manager.arg(
    'force', 'f', help="update resource even if it hasn't changed.",
    type=bool, default=False)
@manager.command
def update(resource_id, **kwargs):
    """Updates a datastore table based on the current filestore resource"""
    verbose = not kwargs['quiet']
    chunk_bytes = kwargs['chunksize_bytes']
    force = kwargs.pop('force')
    ckan_kwargs = {k: v for k, v in kwargs.items() if k in api.CKAN_KEYS}
    hash_kwargs = {'chunksize': chunk_bytes, 'verbose': verbose}

    try:
        ckan = api.CKAN(**ckan_kwargs)
        r, filepath = ckan.fetch_resource(resource_id, chunksize=chunk_bytes)
        old_hash = ckan.get_hash(resource_id) if ckan.hash_table_id else None

    except Exception as err:
        sys.stderr.write('ERROR: %s\n' % str(err))
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)
    else:
        if old_hash:
            new_hash = utils.hash_file(filepath, **hash_kwargs)
            unchanged = new_hash == old_hash
        else:
            unchanged = None

        if verbose:
            print(get_message(unchanged, force, old_hash))

        if unchanged and not force:
            sys.exit(0)

        kwargs['encoding'] = r.encoding
        kwargs['content_type'] = r.headers['content-type']
        updated = update_resource(ckan, resource_id, filepath, **kwargs)

        if updated and verbose:
            print('Success! Resource %s updated.' % resource_id)

        if updated and not unchanged and old_hash:
            update_hash_table(ckan, resource_id, new_hash)
        elif not updated:
            sys.stderr.write('ERROR: resource %s not updated.' % resource_id)
            traceback.print_exc(file=sys.stdout)
            sys.exit(1)
    finally:
        if verbose:
            print('Removing tempfile...')

        unlink(filepath)


@manager.arg(
    'source', help='the source file path', nargs='?', default=sys.stdin)
@manager.arg(
    'resource_id', 'R', help='the resource id (default: source file name)')
@manager.arg(
    'encoding', 'e', help=("the file encoding (read from the file's extended "
    'attributes if uploading a file downloaded with `fs.fetch` on Mac or'
    ' Linux'), default='utf-8')
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
    'chunksize_rows', 'c', help='number of rows to write at a time',
    type=int, default=api.CHUNKSIZE_ROWS)
@manager.arg('primary_key', 'p', help="Unique field(s), e.g., 'field1,field2'")
@manager.arg(
    'quiet', 'q', help='suppress debug statements', type=bool, default=False)
@manager.command
def upload(source, **kwargs):
    """Uploads a file to a datastore table"""
    verbose = not kwargs['quiet']
    def_resource_id = p.splitext(p.basename(source))[0]
    resource_id = kwargs.pop('resource_id', None) or def_resource_id
    ckan_kwargs = {k: v for k, v in kwargs.items() if k in api.CKAN_KEYS}

    if verbose:
        print(
            'Uploading %s to datastore resource %s...' % (source, resource_id))

    # read encoding from extended attributes
    x = xattr(source)

    try:
        kwargs['encoding'] = x.get('com.ckanutils.encoding')
    except IOError:
        pass

    if verbose and kwargs['encoding']:
        print('Using encoding %s' % kwargs['encoding'])

    try:
        ckan = api.CKAN(**ckan_kwargs)
    except Exception as err:
        sys.stderr.write('ERROR: %s\n' % str(err))
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    if update_resource(ckan, resource_id, source, **kwargs):
        print('Success! Resource %s uploaded.' % resource_id)
    else:
        sys.stderr.write('ERROR: resource %s not uploaded.' % resource_id)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)


@manager.arg(
    'resource_id', help='the resource id', nargs='?', default=sys.stdin)
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
    'filters', 'f', help=('the filters to apply before deleting, e.g., {"name"'
    ': "fred"}'))
@manager.command
def delete(resource_id, **kwargs):
    """Deletes a datastore table"""
    ckan_kwargs = {k: v for k, v in kwargs.items() if k in api.CKAN_KEYS}

    try:
        ckan = api.CKAN(**ckan_kwargs)
        ckan.delete_table(resource_id, filters=kwargs.get('filters'))
    except Exception as err:
        sys.stderr.write('ERROR: %s\n' % str(err))
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)


if __name__ == '__main__':
    manager.main()
