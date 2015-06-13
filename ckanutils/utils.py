#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: sw=4:ts=4:expandtab

"""
ckanutils.ckan
~~~~~~~~~~~~~~

Provides miscellaneous utility methods

Examples:
    literal blocks::

        python example_google.py

Attributes:
    ENCODING (str): Default file encoding.
"""

from __future__ import (
    absolute_import, division, print_function, with_statement,
    unicode_literals)

import sys
import hashlib
import itertools as it
import unicodecsv as csv

from tempfile import NamedTemporaryFile
from slugify import slugify

ENCODING = 'utf-8'


def gen_fields(names):
    """Tries to determine field types based on field names.

    Args:
        names (List[str]): Field name.

    Yields:
        dict: The parsed field with type

    Examples:
        >>> gen_fields(['date', 'raw_value', 'text']).next()
        {'id': 'date', 'type': 'timestamp'}
    """
    for name in names:
        if 'date' in name:
            yield {'id': name, 'type': 'timestamp'}
        elif 'value' in name:
            yield {'id': name, 'type': 'float'}
        else:
            yield {'id': name, 'type': 'text'}


def read_csv(csv_filepath, mode='rb', **kwargs):
    """Reads a csv.

    Args:
        csv_filepath (str): The csv file path.
        mode (Optional[str]): The file open mode (defaults to 'rb').
        **kwargs: Keyword arguments that are passed to the csv reader.

    Kwargs:
        delimiter (str): Field delimiter (defaults to ',').
        quotechar (str): Quote character (defaults to '"').
        encoding (str): File encoding.

    Returns:
        List[dicts]: The csv rows.

    Raises:
        NotFound: If unable to the resource.

    Examples:
        >>> read_csv('path/to/file')
    """
    with open(csv_filepath, mode) as f:
        encoding = kwargs.get('encoding', ENCODING)
        header = csv.reader(f, encoding=encoding, **kwargs).next()

        # Remove empty field names and slugify the rest
        names = [slugify(name, separator='_') for name in header if name]
        f.seek(0)
        reader = csv.DictReader(f, names, encoding=encoding)

        # Remove empty columns
        rows = (dict(it.ifilter(lambda x: x[0], r.iteritems())) for r in reader)

        # Remove empty rows
        rows = it.ifilter(lambda r: any(r.strip() for r in r.values()), rows)
        return list(rows)


def get_temp_filepath(delete=False):
    """Creates a named temporary file.

    Args:
        delete (Optional[bool]): Delete file after closing (defaults to False).

    Returns:
        str: The file path.

    Examples:
        >>> get_temp_filepath()
    """
    tmpfile = NamedTemporaryFile(delete=delete)
    return tmpfile.name


def write_file(filepath, r, mode='wb', chunksize=0, bar_len=50):
    """Writes content to a named file.

    Args:
        filepath (str): The path of the file to write to.
        r (obj): Requests object.
        mode (Optional[str]): The file open mode (defaults to 'wb').
        chunksize (Optional[int]): Number of bytes to write at a time (defaults
            to 0, i.e., all).
        bar_len (Optional[int]): Length of progress bar (defaults to 50).

    Returns:
        bool: True

    Examples:
        >>> import requests
        >>> r = requests.get('url')
        >>> write_file('path/to/file', r)
        True
    """
    length = int(r.headers.get('content-length'))

    with open(filepath, mode) as f:
        if chunksize:
            progress = 0

            for chunk in r.iter_content(chunksize):
                f.write(chunk)
                progress += chunksize
                bars = min(int(bar_len * progress / length), bar_len)
                print('\r[%s%s]' % ('=' * bars, ' ' * (bar_len - bars)))
                sys.stdout.flush()
        else:
            f.write(r.raw)

    return True


def chunk(iterable, chunksize=0, start=0, stop=None):
    """Groups data into fixed-length chunks.
    http://stackoverflow.com/a/22919323/408556

    Args:
        iterable (iterable): Content to group into chunks.
        chunksize (Optional[int]): Number of chunks to include in a group (
            defaults to 0, i.e., all).

        start (Optional[int]): Starting item (zero indexed, defaults to 0).
        stop (Optional[int]): Ending item (zero indexed).

    Returns:
        Iter[List]: Chunked content.

    Examples:
        >>> chunk([1,2,3,4,5,6], 2, 1).next()
        [2,3]
    """
    i = it.islice(iter(iterable), start, stop)

    if chunksize:
        generator = (list(it.islice(i, chunksize)) for _ in it.count())
        chunked = it.takewhile(bool, generator)
    else:
        chunked = [list(i)]

    return chunked


def hash_file(filepath, hasher='sha1', chunksize=0):
    """Hashes a file.
    http://stackoverflow.com/a/1131255/408556

    Args:
        filepath (str): The path of the file to write to.
        hasher (str): The hashlib hashing algorithm to use.
        chunksize (Optional[int]): Number of bytes to write at a time (defaults
            to 0, i.e., all).

    Returns:
        List[dict]: Fields

    Examples:
        >>> hash_file('path/to/file')
    """
    hasher = getattr(hashlib, hasher)()

    with open(filepath, 'rb') as f:
        if chunksize:
            while True:
                data = f.read(chunksize)
                if not data:
                    break

                hasher.update(data)
        else:
            hasher.update(f.read())

    return hasher.hexdigest()
