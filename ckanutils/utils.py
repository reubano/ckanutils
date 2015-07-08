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

from dateutil.parser import parse
from functools import partial
from xlrd.xldate import xldate_as_datetime as xl2dt
from xlrd import (
    XL_CELL_DATE, XL_CELL_EMPTY, XL_CELL_NUMBER, XL_CELL_BOOLEAN,
    XL_CELL_ERROR)

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

    Yields:
        dict: A csv record.

    Examples:
        >>> from os import path as p
        >>> parent_dir = p.abspath(p.dirname(p.dirname(__file__)))
        >>> filepath = p.join(parent_dir, 'data', 'test.csv')
        >>> f = open(filepath, 'rU')
        >>> names = ['some_date', 'sparse_data', 'some_value', 'unicode_test']
        >>> records = _read_csv(f, 'utf-8', names)
        >>> it.islice(records, 2, 3).next()['some_date']
        u'01-Jan-15'
        >>> f.close()
    """
    # Read data
    f.seek(0)
    reader = csv.DictReader(f, names, encoding=encoding)

    # Remove `None` keys
    records = (dict(it.ifilter(lambda x: x[0], r.iteritems())) for r in reader)

    # Remove empty rows
    for row in records:
        if any(v.strip() for v in row.values()):
            yield row


def _sanitize_sheet(sheet, mode, date_format):
    """Formats xlrd cell types (from xls/xslx file) as strings.

    Args:
        book (obj): `xlrd` workbook object.
        mode (str): `xlrd` workbook datemode property.
        date_format (str): `strftime()` date format.

    Yields:
        Tuple[int, str]: A tuple of (row_number, value).

    Examples:
        >>> from os import path as p
        >>> parent_dir = p.abspath(p.dirname(p.dirname(__file__)))
        >>> filepath = p.join(parent_dir, 'data', 'test.xls')
        >>> book = xlrd.open_workbook(filepath)
        >>> sheet = book.sheet_by_index(0)
        >>> sanitized = _sanitize_sheet(sheet, book.datemode, '%Y-%m-%d')
        >>> it.islice(sanitized, 5, 6).next()
        (1, '1982-05-04')
    """
    switch = {
        XL_CELL_DATE: lambda v: xl2dt(v, mode).strftime(date_format),
        XL_CELL_EMPTY: lambda v: None,
        XL_CELL_NUMBER: lambda v: unicode(v),
        XL_CELL_BOOLEAN: lambda v: unicode(bool(v)),
        XL_CELL_ERROR: lambda v: xlrd.error_text_from_code[v],
    }

    for i in xrange(sheet.nrows):
        for ctype, value in it.izip(sheet.row_types(i), sheet.row_values(i)):
            yield (i, switch.get(ctype, lambda v: v)(value))


def make_float(value):
    """Parses and formats numbers.

    Args:
        value (str): The number to parse.

    Returns:
        flt: The parsed number.

    Examples:
        >>> make_float('1')
        1.0
        >>> make_float('1f')
    """
    try:
        if value and value.strip():
            value = float(value.replace(',', ''))
        else:
            value = None
    except ValueError:
        value = None

    return value


def _make_date(value, date_format):
    """Parses and formats date strings.

    Args:
        value (str): The date to parse.
        date_format (str): Date format passed to `strftime()`.

    Returns:
        str: The formatted date string.

    Examples:
        >>> _make_date('5/4/82', '%Y-%m-%d')
        ('1982-05-04', False)
        >>> _make_date('2/32/82', '%Y-%m-%d')
        (u'2/32/82', True)
    """
    try:
        if value and value.strip():
            value = parse(value).strftime(date_format)

        retry = False
    # impossible date, e.g., 2/31/15
    except ValueError:
        retry = True
    # unparseable date, e.g., Novmbr 4
    except TypeError:
        value = None
        retry = False

    return (value, retry)


def make_date(value, date_format):
    """Parses and formats date strings.

    Args:
        value (str): The date to parse.
        date_format (str): Date format passed to `strftime()`.

    Returns:
        str: The formatted date string.

    Examples:
        >>> make_date('5/4/82', '%Y-%m-%d')
        '1982-05-04'
        >>> make_date('2/32/82', '%Y-%m-%d')
        '1982-02-28'
    """
    value, retry = _make_date(value, date_format)

    # Fix impossible dates, e.g., 2/31/15
    if retry:
        bad_num = [x for x in ['29', '30', '31', '32'] if x in value][0]
        possibilities = [value.replace(bad_num, x) for x in ['30', '29', '28']]

        for p in possibilities:
            value, retry = _make_date(p, date_format)

            if retry:
                continue
            else:
                break

    return value


def ctype2ext(content_type):
    ctype = content_type.split('/')[1].split(';')[0]
    xlsx_type = 'vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    switch = {'xls': 'xls', 'csv': 'csv'}
    switch[xlsx_type] = 'xlsx'

    if ctype not in switch:
        print(
            'Content-Type %s not found in dictionary. Using default value.'
            % ctype)

    return switch.get(ctype, 'csv')


def gen_type_cast(records, fields, date_format='%Y-%m-%d'):
    """Casts record entries based on field types.

    Args:
        records (List[dicts]): Record entries (`read_csv` output).
        fields (List[dicts]): Field types (`gen_fields` output).
        date_format (str): Date format passed to `strftime()` (default:
            '%Y-%m-%d', i.e, 'YYYY-MM-DD').

    Yields:
        dict: The type casted record entry.

    Examples:
        >>> from os import path as p
        >>> parent_dir = p.abspath(p.dirname(p.dirname(__file__)))
        >>> csv_filepath = p.join(parent_dir, 'data', 'test.csv')
        >>> csv_records = read_csv(csv_filepath, sanitize=True)
        >>> csv_header = sorted(csv_records.next().keys())
        >>> csv_fields = gen_fields(csv_header, True)
        >>> csv_records.next()['some_date']
        u'05/04/82'
        >>> casted_csv_row = gen_type_cast(csv_records, csv_fields).next()
        >>> casted_csv_values = [casted_csv_row[h] for h in csv_header]
        >>>
        >>> xls_filepath = p.join(parent_dir, 'data', 'test.xls')
        >>> xls_records = read_xls(xls_filepath, sanitize=True)
        >>> xls_header = sorted(xls_records.next().keys())
        >>> xls_fields = gen_fields(xls_header, True)
        >>> xls_records.next()['some_date']
        '1982-05-04'
        >>> casted_xls_row = gen_type_cast(xls_records, xls_fields).next()
        >>> casted_xls_values = [casted_xls_row[h] for h in xls_header]
        >>>
        >>> casted_csv_values == casted_xls_values
        True
        >>> casted_csv_values
        ['2015-01-01', 100.0, None, None]
    """
    make_date_p = partial(make_date, date_format=date_format)
    make_unicode = lambda v: unicode(v) if v and v.trim() else None
    switch = {'float': make_float, 'date': make_date_p, 'text': make_unicode}
    field_types = {f['id']: f['type'] for f in fields}

    for row in records:
        yield {k: switch.get(field_types[k])(v) for k, v in row.items()}


def gen_fields(names, type_cast=False):
    """Tries to determine field types based on field names.

    Args:
        names (List[str]): Field names.

    Yields:
        dict: The parsed field with type

    Examples:
        >>> gen_fields(['date', 'raw_value', 'text']).next()
        {u'type': u'text', u'id': u'date'}
    """
    for name in names:
        if type_cast and 'date' in name:
            yield {'id': name, 'type': 'date'}
        elif type_cast and 'value' in name:
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
        sanitize (bool): Underscorify and lowercase field names
            (default: False).

    Yields:
        dict: A csv row.

    Raises:
        NotFound: If unable to find the resource.

    Examples:
        >>> from os import unlink, path as p
        >>> filepath = get_temp_filepath()
        >>> read_csv(filepath).next()
        Traceback (most recent call last):
        StopIteration
        >>> unlink(filepath)
        >>> parent_dir = p.abspath(p.dirname(p.dirname(__file__)))
        >>> filepath = p.join(parent_dir, 'data', 'test.csv')
        >>> records = read_csv(filepath, sanitize=True)
        >>> header = sorted(records.next().keys())
        >>> header
        [u'some_date', u'some_value', u'sparse_data', u'unicode_test']
        >>> row = records.next()
        >>> [row[h] for h in header] == [ \
u'05/04/82', u'234', u'Iñtërnâtiônàližætiøn', u'Ādam']
        True
        >>> [r['some_date'] for r in records]
        [u'01-Jan-15', u'December 31, 1995']
    """
    with open(csv_filepath, mode) as f:
        encoding = kwargs.pop('encoding', ENCODING)
        sanitize = kwargs.pop('sanitize', False)
        header = csv.reader(f, encoding=encoding, **kwargs).next()

        # Remove empty columns
        names = [name for name in header if name.strip()]

        # Underscorify field names
        if sanitize:
            names = [slugify(name, separator='_') for name in names]

        try:
            records = _read_csv(f, encoding, names)
        except UnicodeDecodeError:
            # Try to detect the encoding
            result = detect_encoding(f)
            records = _read_csv(f, result['encoding'], names)

        for row in records:
            yield row


def read_xls(xls_filepath, **kwargs):
    """Reads an xls/xlsx file.

    Args:
        xls_filepath (str): The xls file path.
        **kwargs: Keyword arguments that are passed to the xls reader.

    Kwargs:
        date_format (str): Date format passed to `strftime()` (default:
            '%Y-%m-%d', i.e, 'YYYY-MM-DD').

        encoding (str): File encoding. By default, the encoding is derived from
            the file's `CODEPAGE` number, e.g., 1252 translates to `cp1252`.

        sanitize (bool): Underscorify and lowercase field names
            (default: False).

        on_demand (bool): open_workbook() loads global data and returns without
            releasing resources. At this stage, the only information available
            about sheets is Book.nsheets and Book.sheet_names() (default:
            False).

        pad_rows (bool): Add empty cells so that all rows have the number of
            columns `Sheet.ncols` (default: False).

    Yields:
        dict: An xls row.

    Raises:
        NotFound: If unable to find the resource.

    Examples:
        >>> from os import unlink, path as p
        >>> filepath = get_temp_filepath()
        >>> read_xls(filepath).next()
        Traceback (most recent call last):
        XLRDError: File size is 0 bytes
        >>> unlink(filepath)
        >>> parent_dir = p.abspath(p.dirname(p.dirname(__file__)))
        >>> filepath = p.join(parent_dir, 'data', 'test.xls')
        >>> records = read_xls(filepath, sanitize=True)
        >>> header = sorted(records.next().keys())
        >>> header
        [u'some_date', u'some_value', u'sparse_data', u'unicode_test']
        >>> row = records.next()
        >>> [row[h] for h in header] == [ \
'1982-05-04', u'234.0', u'Iñtërnâtiônàližætiøn', u'Ādam']
        True
        >>> [r['some_date'] for r in records]
        ['2015-01-01', '1995-12-31']
        >>> filepath = p.join(parent_dir, 'data', 'test.xlsx')
        >>> records = read_xls(filepath, sanitize=True)
        >>> header = sorted(records.next().keys())
        >>> header
        [u'some_date', u'some_value', u'sparse_data', u'unicode_test']
        >>> row = records.next()
        >>> [row[h] for h in header] == [ \
'1982-05-04', u'234.0', u'Iñtërnâtiônàližætiøn', u'Ādam']
        True
        >>> [r['some_date'] for r in records]
        ['2015-01-01', '1995-12-31']
    """
    date_format = kwargs.get('date_format', '%Y-%m-%d')

    xlrd_kwargs = {
        'on_demand': kwargs.get('on_demand'),
        'ragged_rows': not kwargs.get('pad_rows'),
        'encoding_override': kwargs.get('encoding', True)
    }

    book = xlrd.open_workbook(xls_filepath, **xlrd_kwargs)
    sheet = book.sheet_by_index(0)
    header = sheet.row_values(0)

    # Remove empty columns
    names = [name for name in header if name.strip()]

    # Underscorify field names
    if kwargs.get('sanitize'):
        names = [slugify(name, separator='_') for name in names]

    # Convert dates
    sanitized = _sanitize_sheet(sheet, book.datemode, date_format)

    for key, group in it.groupby(sanitized, lambda v: v[0]):
        values = [g[1] for g in group]

        # Remove empty rows
        if any(v and v.strip() for v in values):
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
