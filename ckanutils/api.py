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

from os import environ, path as p
from . import utils

CKAN_KEYS = ['hash_table_id', 'remote', 'api_key', 'ua', 'force', 'quiet']
DEF_USER_AGENT = 'ckanapiexample/1.0'
API_KEY_ENV = 'CKAN_API_KEY'
HASH_TABLE_ENV = 'CKAN_HASH_TABLE_ID'
REMOTE_ENV = 'CKAN_REMOTE_URL'
UA_ENV = 'CKAN_USER_AGENT'
CHUNKSIZE_ROWS = 10 ** 3
CHUNKSIZE_BYTES = 2 ** 20


class CKAN(object):
    """Interacts with a CKAN instance.

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
            quiet (bool): Suppress debug statements (default: False).

        Returns:
            New instance of :class:`CKAN`

        Examples:
            >>> CKAN()  #doctest: +ELLIPSIS
            <ckanutils.api.CKAN object at 0x...>
        """
        remote = kwargs.get('remote', environ.get(REMOTE_ENV))
        api_key = kwargs.get('api_key', environ.get(API_KEY_ENV))
        default_ua = environ.get(UA_ENV, DEF_USER_AGENT)
        user_agent = kwargs.get('ua', default_ua)
        hash_tbl_id = kwargs.get('hash_table_id', environ.get(HASH_TABLE_ENV))

        self.force = kwargs.get('force', True)
        self.quiet = kwargs.get('quiet')
        self.user_agent = user_agent
        self.verbose = not self.quiet
        # print('verbose', self.verbose)
        self.hash_table_id = hash_tbl_id


        ckan_kwargs = {'apikey': api_key, 'user_agent': user_agent}
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
        self.resource_create = ckan.action.resource_create
        self.revision_show = ckan.action.revision_show

    def create_table(self, resource_id, fields, **kwargs):
        """Creates a datastore table.

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
        >>> CKAN(quiet=True).create_table('rid', fields=[{'id': 'field', \
'type': 'text'}])
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
        """Deletes a datastore table.

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
            >>> CKAN(quiet=True).delete_table('rid')
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
                print("Can't delete. Datastore table not found.")
        except ckanapi.ValidationError as err:
            if 'read-only' in err.error_dict:
                print(
                    "Can't delete. Datastore table is read only table. Set "
                    "'force' to True and try again.")

                result = None

        return result

    def insert_records(self, resource_id, records, **kwargs):
        """Inserts records into a datastore table.

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
            >>> CKAN(hash_table_id='hid').get_hash('rid')
            `hash_table_id` "hid" was not found!
            Resource rid hash is None.
        """
        if not self.hash_table_id:
            raise Exception('`hash_table_id` not set!')

        kwargs = {
            'resource_id': self.hash_table_id,
            'filters': {'datastore_id': resource_id},
            'fields': 'hash',
            'limit': 1
        }

        try:
            result = self.datastore_search(**kwargs)
            resource_hash = result['records'][0]['hash']
        except ckanapi.NotFound:
            if self.verbose:
                print(
                    '`hash_table_id` "%s" was not found!' % self.hash_table_id)

            resource_hash = None
        except IndexError:
            if self.verbose:
                print('Resource "%s" not found in hash table.' % resource_id)

            resource_hash = None

        if self.verbose:
            print('Resource %s hash is %s.' % (resource_id, resource_hash))

        return resource_hash

    def fetch_resource(self, resource_id, **kwargs):
        """Fetches a single resource from filestore.

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
            >>> CKAN(quiet=True).fetch_resource('rid')
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

        url = resource['url']

        if self.verbose:
            print('Downloading resource %s...' % resource_id)

        if p.isdir(filepath):
            basename = p.basename(url)

            if basename.startswith('export?format='):
                basename = '%s.%s' % (resource_id, basename.split('=')[1])

            filepath = p.join(filepath, basename)

        headers = {'User-Agent': user_agent}
        r = requests.get(url, stream=True, headers=headers)
        utils.write_file(filepath, r, **kwargs)
        return (r, filepath)

    def update_resource(self, resource_id, **kwargs):
        """Create or update a single resource on filestore.

        Args:
            resource_id (str): The filestore resource id.
            **kwargs: Keyword arguments that are passed to resource_create.

        Kwargs:
            name (str): The resource name.
            filepath (str): New file path.
            description (str): The resource description.
            hash (str): The resource hash.

        Returns:
            bool: True if successful, False otherwise.

        Examples:
            >>> CKAN(quiet=True).update_resource('rid')
            Resource "rid" was not found.
            False
        """
        try:
            resource = self.resource_show(id=resource_id)
        except ckanapi.NotFound:
            # Keep exception message consistent with the others
            print('Resource "%s" was not found.' % resource_id)
            return False
        else:
            filepath = kwargs.pop('filepath', None)
            f = open(filepath, 'rb') if filepath else None
            revision = self.revision_show(id=resource['revision_id'])

            resource.update(kwargs)
            resource.update({'upload': f}) if f else None
            resource['package_id'] = revision['packages'][0]

            if self.verbose:
                print('Updating resource %s...' % resource_id)

            data = {k: v for k, v in resource.items() if not isinstance(v, dict)}

            try:
                r = self.resource_create(**data)
            finally:
                f.close() if f else None

            return r
