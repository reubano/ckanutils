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
import httplib
import hashlib
import xlrd
import itertools as it
import unicodecsv as csv

from xlrd.xldate import xldate_as_datetime
from chardet.universaldetector import UniversalDetector
from tempfile import NamedTemporaryFile
from slugify import slugify

ENCODING = 'utf-8'


def patch_http_response_read(func):
    """Patches httplib to read poorly encoded chunked data.

    http://stackoverflow.com/a/14206036/408556
    """
    def inner(*args):
        try:
            return func(*args)
        except httplib.IncompleteRead, e:
            return e.partial

    return inner

httplib.HTTPResponse.read = patch_http_response_read(httplib.HTTPResponse.read)


def _read_csv(f, encoding, names):
    """Helps read a csv file.

    Args:
        f (obj): The csv file like object.
        encoding (str): File encoding.
        names (List[str]): The header names.

    Returns:
        List[dicts]: The csv rows.

    Examples:
        >>> from os import path as p
        >>> parent_dir = p.abspath(p.dirname(p.dirname(__file__)))
        >>> filepath = p.join(parent_dir, 'data', 'test.csv')
        >>> f = open(filepath, 'rU')
        >>> names = ['some_date', 'sparse_data', 'some_value', 'unicode_test']
        >>> rows = _read_csv(f, 'utf-8', names)
        >>> f.close()
        >>> rows[2]['some_date']
        u'01-Jan-15'
    """
    # Read data
    f.seek(0)
    reader = csv.DictReader(f, names, encoding=encoding)

    # Remove `None` keys
    rows = (dict(it.ifilter(lambda x: x[0], r.iteritems())) for r in reader)

    # Remove empty rows
    return [r for r in rows if any(v.strip() for v in r.values())]


def _sanitize_sheet(sheet, mode, dformat, from_fieldname=False):
    """Formats numbers and date values (from xls/xslx file) as strings.

    Args:
        book (obj): `xlrd` workbook object.
        mode (str): `xlrd` workbook datemode property.
        dformat (str): `strftime()` date format.
        from_fieldname (Optional[bool]): Interpret as date if 'date' is in
            fieldname even if cell type isn't `date`. Also, convert cell type
            `number` into text unless 'value' is in the fieldname
            (default: False).

    Yields:
        Tuple[int, str]: A tuple of (row_number, value).

    Examples:
        >>> from os import path as p
        >>> parent_dir = p.abspath(p.dirname(p.dirname(__file__)))
        >>> filepath = p.join(parent_dir, 'data', 'test.xls')
        >>> book = xlrd.open_workbook(filepath)
        >>> sheet = book.sheet_by_index(0)
        >>> dated = _sanitize_sheet(sheet, book.datemode, '%B %d, %Y')
        >>> it.islice(dated, 5, 6).next()
        (1, 'May 04, 1982')
    """
    names = [n.lower() for n in sheet.row_values(0)]

    for i in xrange(sheet.nrows):
        row = it.izip(sheet.row_types(i), sheet.row_values(i))

        for col, cell in enumerate(row):
            ctype, value = cell

            # if it's a date
            if (ctype == 3) or (from_fieldname and 'date' in names[col]):
                try:
                    value = xldate_as_datetime(value, mode).strftime(dformat)
                except ValueError:
                    pass
            # if it's a number
            elif (ctype == 2) and from_fieldname and 'value' not in names[col]:
                value = str(value)

            yield (i, value)


def gen_fields(names):
    """Tries to determine field types based on field names.

    Args:
        names (List[str]): Field names.

    Yields:
        dict: The parsed field with type

    Examples:
        >>> gen_fields(['date', 'raw_value', 'text']).next()['id']
        u'date'
    """
    for name in names:
        # You can't insert a empty string into a timestamp, so skip this step
        # until a work-a-around is inplace to convert empty strings to `null`s
        # if 'date' in name:
        #     yield {'id': name, 'type': 'timestamp'}
        if 'value' in name:
            yield {'id': name, 'type': 'float'}
        else:
            yield {'id': name, 'type': 'text'}


def detect_encoding(f):
    """Detects a file's encoding.

    Args:
        f (obj): The file like object to detect.

    Returns:
        dict: The encoding result

    Examples:
        >>> from os import path as p
        >>> parent_dir = p.abspath(p.dirname(p.dirname(__file__)))
        >>> filepath = p.join(parent_dir, 'data', 'test.csv')
        >>> f = open(filepath, 'rU')
        >>> result = detect_encoding(f)
        >>> f.close()
        >>> result
        {'confidence': 0.99, 'encoding': 'utf-8'}
    """
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
        mode (Optional[str]): The file open mode (default: 'rU').
        **kwargs: Keyword arguments that are passed to the csv reader.

    Kwargs:
        delimiter (str): Field delimiter (default: ',').
        quotechar (str): Quote character (default: '"').
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
        encoding = kwargs.pop('encoding', ENCODING)
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


def read_xls(xls_filepath, **kwargs):
    """Reads an xls/xlsx file.

    Args:
        xls_filepath (str): The xls file path.
        **kwargs: Keyword arguments that are passed to the xls reader.

    Kwargs:
        date_format (str): Date format passed to `strftime()` (default:
            '%B %d, %Y').

        encoding (str): File encoding. By default, the encoding is derived from
            the file's `CODEPAGE` number, e.g., 1252 translates to `cp1252`.

        on_demand (bool): open_workbook() loads global data and returns without
            releasing resources. At this stage, the only information available
            about sheets is Book.nsheets and Book.sheet_names() (default:
            False).

        pad_rows (bool): Add empty cells so that all rows have the number of
            columns `Sheet.ncols` (default: False).

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
    dated = _sanitize_sheet(sheet, book.datemode, date_format, True)

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
        delete (Optional[bool]): Delete file after closing (default: False).

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
        mode (Optional[str]): The file open mode (default: 'wb').
        chunksize (Optional[int]): Number of bytes to write at a time (defaults
            to 0, i.e., all).
        bar_len (Optional[int]): Length of progress bar (default: 50).

    Returns:
        bool: True

    Examples:
        >>> import requests
        >>> filepath = get_temp_filepath(delete=True)
        >>> r = requests.get('http://google.com')
        >>> write_file(filepath, r)
        True
    """
    with open(filepath, mode) as f:
        if chunksize and r.headers.get('transfer-encoding') == 'chunked':
            length = int(r.headers.get('content-length') or 0)
            progress = 0

            for chunk in r.iter_content(chunksize):
                f.write(chunk)
                progress += chunksize

                if length:
                    bars = min(int(bar_len * progress / length), bar_len)
                else:
                    bars = bar_len

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
            default: 0, i.e., all).

        start (Optional[int]): Starting item (zero indexed, default: 0).
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


def hash_file(filepath, hasher='sha1', chunksize=0, verbose=False):
    """Hashes a file.
    http://stackoverflow.com/a/1131255/408556

    Args:
        filepath (str): The path of the file to write to.
        hasher (str): The hashlib hashing algorithm to use.
        chunksize (Optional[int]): Number of bytes to write at a time (default:
            0, i.e., all).
        verbose (Optional[bool]): Print debug statements (default: False).

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

    file_hash = hasher.hexdigest()

    if verbose:
        print('File %s hash is %s.' % (filepath, file_hash))

    return file_hash
