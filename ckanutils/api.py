# -*- coding: utf-8 -*-
# vim: sw=4:ts=4:expandtab

"""
ckanutils.api
~~~~~~~~~~~~~

Provides methods for interacting with a CKAN instance

Examples:
    literal blocks::

        python example_google.py

Attributes:
    CKAN_KEYS (List[str]): available CKAN keyword arguments.
"""

from __future__ import (
    absolute_import, division, print_function, with_statement,
    unicode_literals)

import requests
import ckanapi

from os import environ
from . import utils

CKAN_KEYS = ['hash_table_id', 'remote', 'api_key', 'ua', 'force', 'quiet']
DEF_USER_AGENT = 'ckanapiexample/1.0'
API_KEY_ENV = 'CKAN_API_KEY'
HASH_TABLE_ENV = 'CKAN_HASH_TABLE_ID'
REMOTE_ENV = 'CKAN_REMOTE_URL'
UA_ENV = 'CKAN_USER_AGENT'


class CKAN(object):
    """This is a description of the class.

    Attributes:
        force (bool): Force.
        verbose (bool): Print debug statements.
        quiet (bool): Suppress debug statements.
        address (str): CKAN url.
        hash_table_id (str): The datastore hash table resource id.
        keys (List[str]):
    """

    def __init__(self, **kwargs):
        """Initialization method.

        Args:
            **kwargs: Keyword arguments.

        Kwargs:
            hash_table_id (str): The datastore hash table resource id.
            remote (str): The remote ckan url.
            api_key (str): The ckan api key.
            ua (str): The user agent.
            force (bool): Force (default: True).
            quiet (Optional[bool]): Suppress debug statements (default: False).

        Returns:
            New instance of :class:`CKAN`

        Examples:
            >>> CKAN()  #doctest: +ELLIPSIS
            <ckanutils.api.CKAN object at 0x...>
        """
        remote = kwargs.get('remote', environ.get(REMOTE_ENV))
        default_ua = environ.get(UA_ENV, DEF_USER_AGENT)
        user_agent = kwargs.get('ua', default_ua)
        hash_tbl_id = kwargs.get('hash_table_id', environ.get(HASH_TABLE_ENV))

        self.force = kwargs.get('force', True)
        self.quiet = kwargs.get('quiet')
        self.user_agent = user_agent
        self.verbose = not self.quiet
        # print('verbose', self.verbose)
        self.hash_table_id = hash_tbl_id

        ckan_kwargs = {
            'apikey': kwargs.get('api_key', environ.get(API_KEY_ENV)),
            'user_agent': user_agent
        }

        attr = 'RemoteCKAN' if remote else 'LocalCKAN'
        ckan = getattr(ckanapi, attr)(remote, **ckan_kwargs)

        self.address = ckan.address

        # shortcuts
        self.datastore_search = ckan.action.datastore_search
        self.datastore_create = ckan.action.datastore_create
        self.datastore_delete = ckan.action.datastore_delete
        self.datastore_upsert = ckan.action.datastore_upsert
        self.datastore_search = ckan.action.datastore_search
        self.resource_show = ckan.action.resource_show

    def create_table(self, resource_id, fields, **kwargs):
        """Create a datastore table.

        Args:
            resource_id (str): The filestore resource id.
            fields (List[dict]): fields/columns and their extra metadata.
            **kwargs: Keyword arguments that are passed to datastore_create.

        Kwargs:
            force (bool): Create resource even if read-only.
            aliases (List[str]): name(s) for read only alias(es) of the
                resource.
            primary_key (List[str]): field(s) that represent a unique key.
            indexes (List[str]): index(es) on table.

        Returns:
            dict: The newly created data object.

        Raises:
            ValidationError: If unable to validate user on ckan site.
            NotFound: If unable to find resource.

        Examples:
        >>> CKAN().create_table('rid', fields=[{'id': 'field', 'type': \
'text'}])
        Traceback (most recent call last):
        NotFound: Resource "rid" was not found.
        """
        kwargs.setdefault('force', self.force)
        kwargs['resource_id'] = resource_id
        kwargs['fields'] = fields

        if self.verbose:
            print('Creating table for datastore resource %s...' % resource_id)

        try:
            return self.datastore_create(**kwargs)
        except ckanapi.ValidationError as err:
            if err.error_dict.get('resource_id') == [u'Not found: Resource']:
                raise ckanapi.NotFound(
                    'Resource "%s" was not found.' % resource_id)
            else:
                raise

    def delete_table(self, resource_id, **kwargs):
        """Delete a datastore table.

        Args:
            resource_id (str): The datastore resource id.
            **kwargs: Keyword arguments that are passed to datastore_create.

        Kwargs:
            force (bool): Delete resource even if read-only.
            filters (dict): Filters to apply before deleting, e.g.,
                {"name": "fred"}. If missing delete whole table and all
                dependent views.

        Returns:
            dict: Original filters sent if table was found, `None` otherwise.

        Raises:
            ValidationError: If unable to validate user on ckan site.

        Examples:
            >>> CKAN().delete_table('rid')
        """
        kwargs.setdefault('force', self.force)
        kwargs['resource_id'] = resource_id

        if self.verbose:
            print('Deleting table for datastore resource %s...' % resource_id)

        try:
            result = self.datastore_delete(**kwargs)
        except ckanapi.NotFound:
            result = None

            if self.verbose:
                print("Can't delete, datastore resource %s. Table not found." % resource_id)

        return result

    def insert_records(self, resource_id, records, **kwargs):
        """Insert records into a datastore table.

        Args:
            resource_id (str): The datastore resource id.
            records (List[dict]): The records to insert.
            **kwargs: Keyword arguments that are passed to datastore_create.

        Kwargs:
            method (str): Insert method. One of ['update, 'insert', 'upsert']
                (default: 'insert').
            force (bool): Create resource even if read-only.
            start (int): Row number to start from (zero indexed).
            stop (int): Row number to stop at (zero indexed).
            chunksize (int): Number of rows to write at a time.

        Returns:
            int: Number of records inserted.

        Raises:
            NotFound: If unable to find the resource.

        Examples:
            >>> CKAN(quiet=True).insert_records('rid', [{'field': 'value'}])
            Traceback (most recent call last):
            NotFound: Resource "rid" was not found.
        """
        chunksize = kwargs.pop('chunksize', 0)
        start = kwargs.pop('start', 0)
        stop = kwargs.pop('stop', None)

        kwargs.setdefault('force', self.force)
        kwargs.setdefault('method', 'insert')
        kwargs['resource_id'] = resource_id
        count = 1

        for chunk in utils.chunk(records, chunksize, start=start, stop=stop):
            length = len(chunk)

            if self.verbose:
                print(
                    'Adding records %i - %i to resource %s...' % (
                        count, count + length - 1, resource_id))

            kwargs['records'] = chunk
            self.datastore_upsert(**kwargs)
            count += length

        return count

    def get_hash(self, resource_id):
        """Gets the hash of a datastore table.

        Args:
            resource_id (str): The datastore resource id.

        Returns:
            str: The datastore resource hash.

        Raises:
            Exception: If `hash_table_id` isn't set.
            NotAuthorized: If unable to authorize ckan user.
            NotFound: If unable to find the hash table resource.

        Examples:
            >>> CKAN(quiet=True).get_hash('rid')
        """
        if not self.hash_table_id:
            raise Exception('`hash_table_id` not set!')

        kwargs = {
            'resource_id': self.hash_table_id,
            'filters': {'datastore_id': resource_id},
            'fields': 'hash',
            'limit': 1
        }

        result = self.datastore_search(**kwargs)

        try:
            resource_hash = result['records'][0]['hash']
        except IndexError:
            if self.verbose:
                print('Resource %s not found in hash table.' % resource_id)

            resource_hash = None

        if self.verbose:
            print('Resource %s hash is %s.' % (resource_id, resource_hash))

        return resource_hash

    def fetch_resource(self, resource_id, **kwargs):
        """Fetch a single resource from filestore.

        Args:
            resource_id (str): The filestore resource id.
            **kwargs: Keyword arguments that are passed to datastore_create.

        Kwargs:
            filepath (str): Output file path.
            chunksize (int): Number of bytes to write at a time.
            user_agent (str): The user agent.

        Returns:
            Tuple(obj, str): Tuple of (requests.Response object, filepath).

        Raises:
            NotFound: If unable to find the resource.

        Examples:
            >>> CKAN().fetch_resource('rid')
            Traceback (most recent call last):
            NotFound: Resource "rid" was not found.
        """
        user_agent = kwargs.pop('user_agent', self.user_agent)
        filepath = kwargs.pop('filepath', utils.get_temp_filepath())

        try:
            resource = self.resource_show(id=resource_id)
        except ckanapi.NotFound:
            # Keep exception message consistent with the others
            raise ckanapi.NotFound('Resource "%s" was not found.' % resource_id)

        if self.verbose:
            print('Downloading resource %s...' % resource_id)

        headers = {'User-Agent': user_agent}
        r = requests.get(resource['url'], stream=True, headers=headers)
        utils.write_file(filepath, r, **kwargs)
        return (r, filepath)
