# -*- coding: utf-8 -*-
# vim: sw=4:ts=4:expandtab

"""
ckanutils.ckan
~~~~~~~~~~~~~~

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

import os
import requests
import ckanapi

CKAN_KEYS = [
    'hash_table_id', 'remote', 'api_env', 'api_key', 'user_agent', 'force',
    'quiet'
]

class CKAN(object):
    """This is a description of the class.

    Attributes:
        force (bool): Force.
        verbose (bool): Print debug statements.
        quiet (bool): Suppress debug statements.
        address (str): CKAN url.
        hash_table_id (str): The datastore hash table resource id.
        keys List([str]):
    """

    def __init__(self, **kwargs):
        """Initialization method.

        Args:
            **kwargs: Keyword arguments.

        Kwargs:
            hash_table_id (str): The datastore hash table resource id.
            remote (str): The remote ckan url.
            api_key (str): The ckan api key.
            user_agent (str): The user agent.
            force (bool): Force (defaults to True).
            quiet (Optional[bool]): Suppress debug statements (defaults to False).

        Returns:
            New instance of :class:`CKAN`

        Examples:
            >>> from ckanutils.ckan import CKAN
            >>> ckan = CKAN()
            >>> ckan  #doctest: +ELLIPSIS
            <class object CKAN at 0x...>
        """
        remote = kwargs.get('remote')
        api_key = kwargs.get('api_key')
        user_agent = kwargs.get(user_agent)
        ckan_kwargs = {'apikey': api_key, 'user_agent': user_agent}

        self.force = kwargs.get('force', True)
        self.quiet = kwargs.get('quiet')
        self.verbose = not self.quiet
        self.hash_table_id = kwargs.get('hash_table_id')

        instance = 'RemoteCKAN' if remote else 'LocalCKAN'
        ckan = getattr(ckanapi, instance)(remote, **ckan_kwargs)

        self.address = ckan.address

        # shortcut so we can write, e.g., self.datastore_search instead of
        # self.action.datastore_search
        self.update(ckan.action)

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

        Examples:
            >>> CKAN().create_table('rid', fields=[{'id': 'field', 'type': 'text'}])
        """
        kwargs.setdefault('force', self.force)
        kwargs['resource_id'] = resource_id
        kwargs['fields'] = fields

        try:
            print('Creating datastore table for resource %s...' % resource_id)
            return self.datastore_create(**kwargs)
        except ckanapi.ValidationError as err:
            print(err)
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
            dict: Original filters sent.

        Raises:
            ValidationError: If unable to validate user on ckan site.

        Examples:
            >>> CKAN().delete_table('rid')
        """
        kwargs.setdefault('force', self.force)
        kwargs['resource_id'] = resource_id

        try:
            print('Deleting datastore table for resource %s...' % resource_id)
            return self.datastore_delete(**kwargs)
        except ckanapi.ValidationError as err:
            print(err)
            raise

    def insert_records(self, resource_id, records, **kwargs):
        """Insert records into a datastore table.

        Args:
            resource_id (str): The datastore resource id.
            records (List[dict]): The records to insert.
            **kwargs: Keyword arguments that are passed to datastore_create.

        Kwargs:
            method (str): Insert method. One of ['update, 'insert', 'upsert']
                (defaults to 'insert').
            force (bool): Create resource even if read-only.
            start (int): Row number to start from (zero indexed).
            stop (int): Row number to stop at (zero indexed).
            chunksize (int): Number of rows to write at a time.

        Returns:
            int: Number of records inserted.

        Raises:
            NotFound: If unable to find the resource.

        Examples:
            >>> CKAN().insert_records('rid', records, [{'field', 'value'}])
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

            kwargs['records'] = records
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
            Exception: If hash_table_id isn't set.
            NotAuthorized: If unable to authorize ckan user.
            NotFound: If unable to find the hash table resource.

        Examples:
            >>> CKAN().get_hash('rid')
        """
        if not self.hash_table_id:
            raise Exception('hash_table_id not set!')

        kwargs = {
            'resource_id': self.hash_table_id,
            'filters':  {'datastore_id': resource_id},
            'fields': 'hash',
            'limit': 1
        }

        try:
            result = self.datastore_search(**kwargs)
        except ckanapi.NotAuthorized:
            print('Access denied. Check your api key.')
            raise
        except ckanapi.NotFound:
            print(
                'Hash table %s not found on datastore at %s.' % (
                    self.hash_table_id, self.address))
            raise

        try:
            resource_hash = result['records'][0]['hash']
        except IndexError:
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
            obj: requests.Response object.

        Raises:
            NotFound: If unable to find the resource.

        Examples:
            >>> CKAN().fetch_resource('rid')
        """
        user_agent = kwargs.pop('user_agent', self.user_agent)
        chunksize = kwargs.get('chunksize')
        kwargs.setdefault('filepath', utils.get_temp_filepath())

        try:
            resource = self.resource_show(id=resource_id)
        except ckanapi.NotFound:
            print(
                'Resource %s not found on filestore at %s.' % (
                    resource_id, ckan.address))
            raise

        if self.verbose:
            print('Downloading resource %s...' % resource_id)

        headers = {'User-Agent': user_agent}
        r = requests.get(resource['url'], stream=chunksize, headers=headers)
        length = int(r.headers.get('content-length'))

        if chunksize:
            utils.write_file(filepath, r.iter_content(chunksize), **kwargs)
        else:
            utils.write_file(filepath, r.raw, **kwargs)

        return (r, filepath)
