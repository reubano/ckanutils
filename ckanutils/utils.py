#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: sw=4:ts=4:expandtab

"""
ckanutils.utils
~~~~~~~~~~~~~~~

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
import xlrd
import itertools as it
import unicodecsv as csv

from xlrd.xldate import xldate_as_datetime
from chardet.universaldetector import UniversalDetector
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
        >>> gen_fields(['date', 'raw_value', 'text']).next()['id']
        u'date'
    """
    for name in names:
        if 'date' in name:
            yield {'id': name, 'type': 'timestamp'}
        elif 'value' in name:
            yield {'id': name, 'type': 'float'}
        else:
            yield {'id': name, 'type': 'text'}


def _read_csv(f, encoding, names):
    # Read data
    f.seek(0)
    reader = csv.DictReader(f, names, encoding=encoding)

    # Remove `None` keys
    rows = (dict(it.ifilter(lambda x: x[0], r.iteritems())) for r in reader)

    # Remove empty rows
    return [r for r in rows if any(v.strip() for v in r.values())]


def detect_encoding(f):
    f.seek(0)
    detector = UniversalDetector()

    for line in f:
        detector.feed(line)

        if detector.done:
            break

    detector.close()
    # print('detector.result', detector.result)
    return detector.result


def read_csv(csv_filepath, mode='rU', **kwargs):
    """Reads a csv file.

    Args:
        csv_filepath (str): The csv file path.
        mode (Optional[str]): The file open mode (defaults to 'rU').
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
        >>> from os import unlink, path as p
        >>> filepath = get_temp_filepath()
        >>> read_csv(filepath)
        Traceback (most recent call last):
        StopIteration
        >>> unlink(filepath)
        >>> parent_dir = p.abspath(p.dirname(p.dirname(__file__)))
        >>> filepath = p.join(parent_dir, 'data', 'test.csv')
        >>> rows = read_csv(filepath)
        >>> len(rows)
        4
        >>> keys = sorted(rows[1].keys())
        >>> keys
        [u'some_date', u'some_value', u'sparse_data', u'unicode_test']
        >>> [rows[1][k] for k in keys] == [u'05/04/82', u'234', \
u'Iñtërnâtiônàližætiøn', u'Ādam']
        True
        >>> [r['some_date'] for r in rows[1:]]
        [u'05/04/82', u'01-Jan-15', u'December 31, 1995']
    """
    with open(csv_filepath, mode) as f:
        encoding = kwargs.get('encoding', ENCODING)
        header = csv.reader(f, encoding=encoding, **kwargs).next()

        # Slugify field names and remove empty columns
        names = [slugify(n, separator='_') for n in header if n.strip()]

        try:
            rows = _read_csv(f, encoding, names)
        except UnicodeDecodeError:
            # Try to detect the encoding
            result = detect_encoding(f)
            rows = _read_csv(f, result['encoding'], names)

        return rows


def _datize_sheet(sheet, mode, dformat):
    for i in xrange(sheet.nrows):
        row = it.izip(sheet.row_types(i), sheet.row_values(i))

        for cell in row:
            ctype, value = cell

            if ctype == xlrd.XL_CELL_DATE:
                value = xldate_as_datetime(value, mode).strftime(dformat)

            yield (i, value)


def read_xls(xls_filepath, **kwargs):
    """Reads an xls/xlsx file.

    Args:
        xls_filepath (str): The xls file path.
        **kwargs: Keyword arguments that are passed to the xls reader.

    Kwargs:
        date_format (str): Date format passed to `strftime()` (defaults to
            '%B %d, %Y').

        encoding (str): File encoding. By default, the encoding is derived from
            the file's `CODEPAGE` number, e.g., 1252 translates to `cp1252`.

        on_demand (bool): open_workbook() loads global data and returns without
            releasing resources. At this stage, the only information available
            about sheets is Book.nsheets and Book.sheet_names() (defaults to
            False).

        pad_rows (bool): Add empty cells so that all rows have the number of
            columns `Sheet.ncols` (defaults to False).

    Yields:
        dict: An xls row.

    Raises:
        NotFound: If unable to the resource.

    Examples:
        >>> from os import unlink, path as p
        >>> filepath = get_temp_filepath()
        >>> read_xls(filepath).next()
        Traceback (most recent call last):
        XLRDError: File size is 0 bytes
        >>> unlink(filepath)
        >>> parent_dir = p.abspath(p.dirname(p.dirname(__file__)))
        >>> filepath = p.join(parent_dir, 'data', 'test.xls')
        >>> rows = list(read_xls(filepath))
        >>> len(rows)
        4
        >>> keys = sorted(rows[1].keys())
        >>> keys
        [u'some_date', u'some_value', u'sparse_data', u'unicode_test']
        >>> [rows[1][k] for k in keys] == ['May 04, 1982', 234.0, \
u'Iñtërnâtiônàližætiøn', u'Ādam']
        True
        >>> [r['some_date'] for r in rows[1:]]
        ['May 04, 1982', 'January 01, 2015', 'December 31, 1995']
        >>> filepath = p.join(parent_dir, 'data', 'test.xlsx')
        >>> rows = list(read_xls(filepath))
        >>> len(rows)
        4
        >>> keys = sorted(rows[1].keys())
        >>> keys
        [u'some_date', u'some_value', u'sparse_data', u'unicode_test']
        >>> [rows[1][k] for k in keys] == ['May 04, 1982', 234.0, \
u'Iñtërnâtiônàližætiøn', u'Ādam']
        True
        >>> [r['some_date'] for r in rows[1:]]
        ['May 04, 1982', 'January 01, 2015', 'December 31, 1995']
    """
    date_format = kwargs.get('date_format', '%B %d, %Y')

    xlrd_kwargs = {
        'on_demand': kwargs.get('on_demand'),
        'ragged_rows': not kwargs.get('pad_rows'),
        'encoding_override': kwargs.get('encoding', True)
    }

    book = xlrd.open_workbook(xls_filepath, **xlrd_kwargs)
    sheet = book.sheet_by_index(0)
    header = sheet.row_values(0)

    # Slugify field names and remove empty columns
    names = [slugify(name, separator='_') for name in header if name.strip()]

    # Convert dates
    dated = _datize_sheet(sheet, book.datemode, date_format)

    for key, group in it.groupby(dated, lambda v: v[0]):
        values = [g[1] for g in group]

        # Remove empty rows
        try:
            if any(v.strip() for v in values):
                yield dict(zip(names, values))
        except AttributeError:
            yield dict(zip(names, values))


def get_temp_filepath(delete=False):
    """Creates a named temporary file.

    Args:
        delete (Optional[bool]): Delete file after closing (defaults to False).

    Returns:
        str: The file path.

    Examples:
        >>> get_temp_filepath(delete=True)  #doctest: +ELLIPSIS
        '/var/folders/...'
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
        >>> filepath = get_temp_filepath(delete=True)
        >>> r = requests.get('http://google.com')
        >>> write_file(filepath, r)
        True
    """
    length = int(r.headers.get('content-length') or 0)

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
            f.write(r.raw.read())

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
        >>> chunk([1, 2, 3, 4, 5, 6], 2, 1).next()
        [2, 3]
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
        str: File hash.

    Examples:
        >>> import os
        >>> filepath = get_temp_filepath()
        >>> hash_file(filepath)
        'da39a3ee5e6b4b0d3255bfef95601890afd80709'
        >>> os.unlink(filepath)
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
