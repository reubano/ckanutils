# -*- coding: utf-8 -*-
# vim: sw=4:ts=4:expandtab

"""
ckanutils
~~~~~~~~~

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
import itertools as it

from os import environ, path as p
from datetime import datetime as dt
from operator import itemgetter
from pprint import pprint

from ckanapi import NotFound, NotAuthorized, ValidationError
from tabutils import process as pr, io, fntools as ft, convert as cv

__version__ = '0.14.9'

__title__ = 'ckanutils'
__author__ = 'Reuben Cummings'
__description__ = 'Miscellaneous CKAN utility library'
__email__ = 'reubano@gmail.com'
__license__ = 'MIT'
__copyright__ = 'Copyright 2015 Reuben Cummings'

CKAN_KEYS = ['hash_table', 'remote', 'api_key', 'ua', 'force', 'quiet']
API_KEY_ENV = 'CKAN_API_KEY'
REMOTE_ENV = 'CKAN_REMOTE_URL'
UA_ENV = 'CKAN_USER_AGENT'
DEF_USER_AGENT = 'ckanutils/%s' % __version__
DEF_HASH_PACK = 'hash-table'
DEF_HASH_RES = 'hash-table.csv'
CHUNKSIZE_ROWS = 10 ** 3
CHUNKSIZE_BYTES = 2 ** 20
ENCODING = 'utf-8'


class CKAN(object):
    """Interacts with a CKAN instance.

    Attributes:
        force (bool): Force.
        verbose (bool): Print debug statements.
        quiet (bool): Suppress debug statements.
        address (str): CKAN url.
        hash_table (str): The hash table package id.
        keys (List[str]):
    """

    def __init__(self, **kwargs):
        """Initialization method.

        Args:
            **kwargs: Keyword arguments.

        Kwargs:
            hash_table (str): The hash table package id.
            remote (str): The remote ckan url.
            api_key (str): The ckan api key.
            ua (str): The user agent.
            force (bool): Force (default: True).
            quiet (bool): Suppress debug statements (default: False).

        Returns:
            New instance of :class:`CKAN`

        Examples:
            >>> CKAN()  #doctest: +ELLIPSIS
            <ckanutils.CKAN object at 0x...>
        """
        default_ua = environ.get(UA_ENV, DEF_USER_AGENT)
        def_remote = environ.get(REMOTE_ENV)
        def_api_key = environ.get(API_KEY_ENV)
        remote = kwargs.get('remote', def_remote)

        self.api_key = kwargs.get('api_key', def_api_key)
        self.force = kwargs.get('force', True)
        self.quiet = kwargs.get('quiet')
        self.user_agent = kwargs.get('ua', default_ua)
        self.verbose = not self.quiet
        self.hash_table = kwargs.get('hash_table', DEF_HASH_PACK)

        ckan_kwargs = {'apikey': self.api_key, 'user_agent': self.user_agent}
        attr = 'RemoteCKAN' if remote else 'LocalCKAN'
        ckan = getattr(ckanapi, attr)(remote, **ckan_kwargs)

        self.address = ckan.address
        self.package_show = ckan.action.package_show

        try:
            self.hash_table_pack = self.package_show(id=self.hash_table)
        except NotFound:
            self.hash_table_pack = None
        except ValidationError as err:
            if err.error_dict.get('resource_id') == ['Not found: Resource']:
                self.hash_table_pack = None
            else:
                raise err

        try:
            self.hash_table_id = self.hash_table_pack['resources'][0]['id']
        except (IndexError, TypeError):
            self.hash_table_id = None

        # shortcuts
        self.datastore_search = ckan.action.datastore_search
        self.datastore_create = ckan.action.datastore_create
        self.datastore_delete = ckan.action.datastore_delete
        self.datastore_upsert = ckan.action.datastore_upsert
        self.datastore_search = ckan.action.datastore_search
        self.resource_show = ckan.action.resource_show
        self.resource_create = ckan.action.resource_create
        self.package_create = ckan.action.package_create
        self.package_update = ckan.action.package_update
        self.package_privatize = ckan.action.bulk_update_private
        self.revision_show = ckan.action.revision_show
        self.organization_list = ckan.action.organization_list_for_user
        self.organization_show = ckan.action.organization_show
        self.license_list = ckan.action.license_list
        self.group_list = ckan.action.group_list
        self.user = ckan.action.get_site_user()

    def create_table(self, resource_id, fields, **kwargs):
        """Creates a datastore table for an existing filestore resource.

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
        NotFound: Resource `rid` was not found in filestore.
        """
        kwargs.setdefault('force', self.force)
        kwargs['resource_id'] = resource_id
        kwargs['fields'] = fields
        err_msg = 'Resource `%s` was not found in filestore.' % resource_id

        if self.verbose:
            print('Creating table `%s` in datastore...' % resource_id)

        try:
            return self.datastore_create(**kwargs)
        except ValidationError as err:
            if err.error_dict.get('resource_id') == ['Not found: Resource']:
                raise NotFound(err_msg)
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
            Can't delete. Table `rid` was not found in datastore.
        """
        kwargs.setdefault('force', self.force)
        kwargs['resource_id'] = resource_id
        init_msg = "Can't delete. Table `%s`" % resource_id
        err_msg = '%s was not found in datastore.' % init_msg
        read_msg = '%s is read only.' % init_msg

        if self.verbose:
            print('Deleting table `%s` from datastore...' % resource_id)

        try:
            result = self.datastore_delete(**kwargs)
        except NotFound:
            print(err_msg)
            result = None
        except ValidationError as err:
            if 'read-only' in err.error_dict:
                print(read_msg)
                print("Set 'force' to True and try again.")
                result = None
            elif err.error_dict.get('resource_id') == ['Not found: Resource']:
                print(err_msg)
                result = None
            else:
                raise err

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
            NotFound: Resource `rid` was not found in filestore.
        """
        recoded = pr.json_recode(records)
        chunksize = kwargs.pop('chunksize', 0)
        start = kwargs.pop('start', 0)
        stop = kwargs.pop('stop', None)

        kwargs.setdefault('force', self.force)
        kwargs.setdefault('method', 'insert')
        kwargs['resource_id'] = resource_id
        count = 1

        for chunk in ft.chunk(recoded, chunksize, start=start, stop=stop):
            length = len(chunk)

            if self.verbose:
                print(
                    'Adding records %i - %i to resource %s...' % (
                        count, count + length - 1, resource_id))

            kwargs['records'] = chunk
            err_msg = 'Resource `%s` was not found in filestore.' % resource_id

            try:
                self.datastore_upsert(**kwargs)
            except requests.exceptions.ConnectionError as err:
                if 'Broken pipe' in err.message[1]:
                    print('Chunksize too large. Try using a smaller chunksize.')
                    return 0
                else:
                    raise err
            except NotFound:
                # Keep exception message consistent with the others
                raise NotFound(err_msg)
            except ValidationError as err:
                if err.error_dict.get('resource_id') == ['Not found: Resource']:
                    raise NotFound(err_msg)
                else:
                    raise err

            count += length

        return count

    def get_hash(self, resource_id):
        """Gets the hash of a datastore table.

        Args:
            resource_id (str): The datastore resource id.

        Returns:
            str: The datastore resource hash.

        Raises:
            NotFound: If `hash_table_id` isn't set or not in datastore.
            NotAuthorized: If unable to authorize ckan user.

        Examples:
            >>> CKAN(hash_table='hash_jhb34rtj34t').get_hash('rid')
            Traceback (most recent call last):
            NotFound: {u'item': u'package', u'message': u'Package \
`hash_jhb34rtj34t` was not found!'}
        """
        if not self.hash_table_pack:
            message = 'Package `%s` was not found!' % self.hash_table
            raise NotFound({'message': message, 'item': 'package'})

        if not self.hash_table_id:
            message = 'No resources found in package `%s`!' % self.hash_table
            raise NotFound({'message': message, 'item': 'resource'})

        kwargs = {
            'resource_id': self.hash_table_id,
            'filters': {'datastore_id': resource_id},
            'fields': 'hash',
            'limit': 1
        }

        err_msg = 'Resource `%s` was not found' % resource_id
        alt_msg = 'Hash table `%s` was not found' % self.hash_table_id

        try:
            result = self.datastore_search(**kwargs)
            resource_hash = result['records'][0]['hash']
        except NotFound:
            message = '%s in datastore!' % alt_msg
            raise NotFound({'message': message, 'item': 'datastore'})
        except ValidationError as err:
            if err.error_dict.get('resource_id') == ['Not found: Resource']:
                raise NotFound('%s in filestore.' % err_msg)
            else:
                raise err
        except IndexError:
            print('%s in hash table.' % err_msg)
            resource_hash = None

        if self.verbose:
            print('Resource `%s` hash is `%s`.' % (resource_id, resource_hash))

        return resource_hash

    def fetch_resource(self, resource_id, user_agent=None, stream=True):
        """Fetches a single resource from filestore.

        Args:
            resource_id (str): The filestore resource id.

        Kwargs:
            user_agent (str): The user agent.
            stream (bool): Stream content (default: True).

        Returns:
            obj: requests.Response object.

        Raises:
            NotFound: If unable to find the resource.
            NotAuthorized: If access to fetch resource is denied.

        Examples:
            >>> CKAN(quiet=True).fetch_resource('rid')
            Traceback (most recent call last):
            NotFound: Resource `rid` was not found in filestore.
        """
        user_agent = user_agent or self.user_agent
        err_msg = 'Resource `%s` was not found in filestore.' % resource_id

        try:
            resource = self.resource_show(id=resource_id)
        except NotFound:
            raise NotFound(err_msg)
        except ValidationError as err:
            if err.error_dict.get('resource_id') == ['Not found: Resource']:
                raise NotFound(err_msg)
            else:
                raise err

        url = resource.get('perma_link') or resource.get('url')

        if self.verbose:
            print('Downloading url %s...' % url)

        headers = {'User-Agent': user_agent}
        r = requests.get(url, stream=stream, headers=headers)
        err_msg = 'Access to fetch resource %s was denied.' % resource_id

        if any('403' in h.headers.get('x-ckan-error', '') for h in r.history):
            raise NotAuthorized(err_msg)
        elif r.status_code == 401:
            raise NotAuthorized(err_msg)
        else:
            return r

    def get_filestore_update_func(self, resource, **kwargs):
        """Returns the function to create or update a single resource on
        filestore. To create a resource, you must supply either `url`,
        `filepath`, or `fileobj`.

        Args:
            resource (dict): The resource passed to resource_create.
            **kwargs: Keyword arguments that are passed to resource_create.

        Kwargs:
            url (str): New file url (for file link, requires `format`).
            format (str): New file format (for file link, requires `url`).
            fileobj (obj): New file like object (for file upload).
            filepath (str): New file path (for file upload).
            post (bool): Post data using requests instead of ckanapi.
            name (str): The resource name.
            description (str): The resource description.
            hash (str): The resource hash.

        Returns:
            tuple: (func, args, data)
                where func is `requests.post` if `post` option is specified,
                `self.resource_create` otherwise. `args` and `data` should be
                passed as *args and **kwargs respectively.

        See also:
            ckanutils._update_filestore

        Examples:
            >>> ckan = CKAN(quiet=True)
            >>> resource = {
            ...     'name': 'name', 'package_id': 'pid', 'resource_id': 'rid',
            ...     'description': 'description', 'hash': 'hash'}
            >>> kwargs = {'url': 'http://example.com/file', 'format': 'csv'}
            >>> res = ckan.get_filestore_update_func(resource, **kwargs)
            >>> func, args, kwargs = res
            >>> func(*args, **kwargs)
            Traceback (most recent call last):
            NotFound: Not found
        """
        post = kwargs.pop('post', None)
        filepath = kwargs.pop('filepath', None)
        fileobj = kwargs.pop('fileobj', None)
        f = open(filepath, 'rb') if filepath else fileobj
        resource.update(kwargs)

        if post:
            args = ['%s/api/action/resource_create' % self.address]
            hdrs = {
                'X-CKAN-API-Key': self.api_key, 'User-Agent': self.user_agent}

            data = {'data': resource, 'headers': hdrs}
            data.update({'files': {'upload': f}}) if f else None
            func = requests.post
        else:
            args = []
            resource.update({'upload': f}) if f else None
            data = {
                k: v for k, v in resource.items() if not isinstance(v, dict)}
            func = self.resource_create

        return (func, args, data)

    def _update_filestore(self, func, *args, **kwargs):
        """Helps create or update a single resource on filestore.
        To create a resource, you must supply either `url`, `filepath`, or
        `fileobj`.

        Args:
            func (func): The resource passed to resource_create.
            *args: Postional arguments that are passed to `func`
            **kwargs: Keyword arguments that are passed to `func`.

        Kwargs:
            url (str): New file url (for file link).
            fileobj (obj): New file like object (for file upload).
            filepath (str): New file path (for file upload).
            name (str): The resource name.
            description (str): The resource description.
            hash (str): The resource hash.

        Returns:
            obj: requests.Response object if `post` option is specified,
                ckan resource object otherwise.

        See also:
            ckanutils.get_filestore_update_func

        Examples:
            >>> ckan = CKAN(quiet=True)
            >>> url = 'http://example.com/file'
            >>> resource = {'package_id': 'pid'}
            >>> kwargs = {'name': 'name', 'url': url, 'format': 'csv'}
            >>> res = ckan.get_filestore_update_func(resource, **kwargs)
            >>> ckan._update_filestore(res[0], *res[1], **res[2])
            Package `pid` was not found.
            >>> resource['resource_id'] = 'rid'
            >>> res = ckan.get_filestore_update_func(resource, **kwargs)
            >>> ckan._update_filestore(res[0], *res[1], **res[2])
            Resource `rid` was not found in filestore.
        """
        data = kwargs.get('data', {})
        files = kwargs.get('files', {})
        resource_id = kwargs.get('resource_id', data.get('resource_id'))
        package_id = kwargs.get('package_id', data.get('package_id'))
        f = kwargs.get('upload', files.get('upload'))
        err_msg = 'Resource `%s` was not found in filestore.' % resource_id

        try:
            r = func(*args, **kwargs) or {'id': None}
        except NotFound:
            pck_msg = 'Package `%s` was not found.' % package_id
            print(err_msg if resource_id else pck_msg)
        except ValidationError as err:
            if err.error_dict.get('resource_id') == ['Not found: Resource']:
                print(err_msg)
                r = None
            else:
                raise err
        except requests.exceptions.ConnectionError as err:
            if 'Broken pipe' in err.message[1]:
                print('File size too large. Try uploading a smaller file.')
                r = None
            else:
                raise err
        else:
            return r
        finally:
            f.close() if f else None

    def create_resource(self, package_id, **kwargs):
        """Creates a single resource on filestore. You must supply either
        `url`, `filepath`, or `fileobj`.

        Args:
            package_id (str): The filestore package id.
            **kwargs: Keyword arguments that are passed to resource_create.

        Kwargs:
            url (str): New file url (for file link).
            filepath (str): New file path (for file upload).
            fileobj (obj): New file like object (for file upload).
            post (bool): Post data using requests instead of ckanapi.
            name (str): The resource name (defaults to the filename).
            description (str): The resource description.
            hash (str): The resource hash.

        Returns:
            obj: requests.Response object if `post` option is specified,
                ckan resource object otherwise.

        Raises:
            TypeError: If neither `url`, `filepath`, nor `fileobj` are supplied.

        Examples:
            >>> ckan = CKAN(quiet=True)
            >>> ckan.create_resource('pid')
            Traceback (most recent call last):
            TypeError: You must specify either a `url`, `filepath`, or `fileobj`
            >>> ckan.create_resource('pid', url='http://example.com/file')
            Package `pid` was not found.
        """
        if not any(map(kwargs.get, ['url', 'filepath', 'fileobj'])):
            raise TypeError(
                'You must specify either a `url`, `filepath`, or `fileobj`')

        path = filter(None, map(kwargs.get, ['url', 'filepath', 'fileobj']))[0]

        try:
            if 'docs.google.com' in path:
                def_name = path.split('gid=')[1].split('&')[0]
            else:
                def_name = p.basename(path)
        except AttributeError:
            def_name = None
            file_format = 'csv'
        else:
            # copy/pasted from utils... fix later
            if 'format=' in path:
                file_format = path.split('format=')[1].split('&')[0]
            else:
                file_format = p.splitext(path)[1].lstrip('.')

        kwargs.setdefault('name', def_name)

        # Will get `ckan.logic.ValidationError` if url isn't set
        kwargs.setdefault('url', 'http://example.com')
        kwargs['format'] = file_format
        resource = {'package_id': package_id}

        if self.verbose:
            print('Creating new resource in package %s...' % package_id)

        func, args, data = self.get_filestore_update_func(resource, **kwargs)
        return self._update_filestore(func, *args, **data)

    def update_filestore(self, resource_id, **kwargs):
        """Updates a single resource on filestore.

        Args:
            resource_id (str): The filestore resource id.
            **kwargs: Keyword arguments that are passed to resource_create.

        Kwargs:
            url (str): New file url (for file link).
            filepath (str): New file path (for file upload).
            fileobj (obj): New file like object (for file upload).
            post (bool): Post data using requests instead of ckanapi.
            name (str): The resource name.
            description (str): The resource description.
            hash (str): The resource hash.

        Returns:
            obj: requests.Response object if `post` option is specified,
                ckan resource object otherwise.

        Examples:
            >>> CKAN(quiet=True).update_filestore('rid')
            Resource `rid` was not found in filestore.
        """
        err_msg = 'Resource `%s` was not found in filestore.' % resource_id

        try:
            resource = self.resource_show(id=resource_id)
        except NotFound:
            print(err_msg)
            return None
        except ValidationError as err:
            if err.error_dict.get('resource_id') == ['Not found: Resource']:
                raise NotFound(err_msg)
            else:
                raise err
        else:
            resource['package_id'] = self.get_package_id(resource_id)

            if self.verbose:
                print('Updating resource %s...' % resource_id)

            f, args, data = self.get_filestore_update_func(resource, **kwargs)
            return self._update_filestore(f, *args, **data)

    def update_datastore(self, resource_id, filepath, **kwargs):
        verbose = not kwargs.get('quiet')
        chunk_rows = kwargs.get('chunksize_rows')
        primary_key = kwargs.get('primary_key')
        content_type = kwargs.get('content_type')
        type_cast = kwargs.get('type_cast')
        method = 'upsert' if primary_key else 'insert'
        keys = ['aliases', 'primary_key', 'indexes']

        try:
            extension = p.splitext(filepath)[1].split('.')[1]
        except (IndexError, AttributeError):
            # no file extension given, e.g., a tempfile
            extension = cv.ctype2ext(content_type)

        try:
            reader = io.get_reader(extension)
        except TypeError:
            print('Error: plugin for extension `%s` not found!' % extension)
            return False
        else:
            records = reader(filepath, **kwargs)
            first = records.next()
            keys = first.keys()
            records = it.chain([first], records)

            if type_cast:
                records, results = pr.detect_types(records)
                types = results['types']
                casted_records = pr.type_cast(records, types)
            else:
                types = [{'id': key, 'type': 'text'} for key in keys]
                casted_records = records

            if verbose:
                print('Parsed types:')
                pprint(types)

            create_kwargs = {k: v for k, v in kwargs.items() if k in keys}

            if not primary_key:
                self.delete_table(resource_id)

            insert_kwargs = {'chunksize': chunk_rows, 'method': method}
            self.create_table(resource_id, types, **create_kwargs)
            args = [resource_id, casted_records]
            return self.insert_records(*args, **insert_kwargs)

    def find_ids(self, packages, **kwargs):
        default = {'rid': '', 'pname': ''}
        kwargs.update({'method': self.query, 'default': default})
        return pr.find(packages, **kwargs)

    def get_package_id(self, resource_id):
        """Gets the package id of a single resource on filestore.

        Args:
            resource_id (str): The filestore resource id.

        Returns:
            str: The package id.

        Examples:
            >>> CKAN(quiet=True).get_package_id('rid')
            Resource `rid` was not found in filestore.
        """
        err_msg = 'Resource `%s` was not found in filestore.' % resource_id

        try:
            resource = self.resource_show(id=resource_id)
        except NotFound:
            print(err_msg)
            return None
        except ValidationError as err:
            if err.error_dict.get('resource_id') == ['Not found: Resource']:
                raise NotFound(err_msg)
            else:
                raise err
        else:
            revision = self.revision_show(id=resource['revision_id'])
            return revision['packages'][0]

    def create_hash_table(self, verbose=False):
        kwargs = {
            'resource_id': self.hash_table_id,
            'fields': [
                {'id': 'datastore_id', 'type': 'text'},
                {'id': 'hash', 'type': 'text'}],
            'primary_key': 'datastore_id'
        }

        if verbose:
            print('Creating hash table...')

        self.create_table(**kwargs)

    def update_hash_table(self, resource_id, resource_hash, verbose=False):
        records = [{'datastore_id': resource_id, 'hash': resource_hash}]

        if verbose:
            print('Updating hash table...')

        self.insert_records(self.hash_table_id, records, method='upsert')

    def get_update_date(self, item):
        timestamps = {
            'revision_timestamp': 'revision',
            'last_modified': 'resource',
            'metadata_modified': 'package'
        }

        for key, value in timestamps.items():
            if key in item:
                timestamp = item[key]
                item_type = value
                break
        else:
            keys = timestamps.keys()
            msg = 'None of the following keys found in item: %s' % keys
            raise TypeError(msg)

        if not timestamp and item_type == 'resource':
            # print('Resource timestamp is empty. Querying revision.')
            timestamp = self.revision_show(id=item['revision_id'])['timestamp']

        return dt.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f')

    def filter(self, items, tagged=None, named=None, updated=None):
        for i in items:
            if i['state'] != 'active':
                continue

            if updated and updated(self.get_update_date(i)):
                yield i
                continue

            if named and named.lower() in i['name'].lower():
                yield i
                continue

            tags = it.imap(itemgetter('name'), i['tags'])
            is_tagged = tagged and 'tags' in i

            if is_tagged and any(it.ifilter(lambda t: t == tagged, tags)):
                yield i
                continue

            if not (named or tagged or updated):
                yield i

    def query(self, packages, **kwargs):
        pkwargs = {
            'named': kwargs.get('pnamed'),
            'tagged': kwargs.get('ptagged')}

        rkwargs = {
            'named': kwargs.get('rnamed'),
            'tagged': kwargs.get('rtagged')}

        skwargs = {'key': self.get_update_date, 'reverse': True}
        filtered_packages = self.filter(packages, **pkwargs)

        for pack in sorted(filtered_packages, **skwargs):
            package = self.package_show(id=pack['name'])
            resources = self.filter(package['resources'], **rkwargs)

            for resource in sorted(resources, **skwargs):
                yield {'rid': resource['id'], 'pname': package['name']}
