#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Miscellaneous CKAN utility scripts """

from __future__ import print_function

import traceback
import sys
import unicodecsv

from os import path as p
from manager import Manager

manager = Manager()
_homedir = p.expanduser('~')
ENCODING = 'utf-8-sig'


def readCSV(csv_file):
    with open(csv_file, 'r') as f:
        kwargs = {'delimiter': ',', 'quotechar': '"', 'encoding': ENCODING}
        reader = unicodecsv.reader(f, **kwargs)
        header = reader.next()
        return [{k: v for k, v in zip(header, row)} for row in reader]


@manager.option(
    '-v', '--verbose', help='increase output verbosity',
    action='store_true')
@manager.option(
    '-f', '--cfile',
    help="the file to use (defaults to '~/file')")
@manager.option(
    '-d', '--cdir',
    help="the project directory (defaults to current directory)")
@manager.option(
    '-V', '--version', help='display version and exit',
    action='store_true')
def run(cfile=None, cdir=None, version=None):
    """Create html"""
    if version:
        from . import __version__ as version
        print 'v%s' % version
        sys.exit(0)

    try:
        cfile = (cfile or p.join(_homedir, 'file'))
        cdir = (cdir or os.curdir)
        data = []
        raw = readCSV(csv_file)
        headers = ['Image', 'Title', 'End Date', 'Price (GBP)']

        for row in raw:
            img = '<img src="%s">' % row['ebay_img_url']
            title = '[%s](%s)' % (row['ebay_title'], row['ebay_url'])
            date = '%s, %s' % (row['ebay_end_date'], row['ebay_end_time'])
            price = row['ebay_price_and_shipping']
            attrs = [img, title, date, price]
            data.append(attrs)

        md = tabulate(data, headers=headers, floatfmt=',.2f', tablefmt='pipe')
        title = '%s results' % p.splitext(p.basename(html_file))[0].capitalize()
        export_md(md, title, html_file)
        sys.exit(0)
    except Exception as err:
        sys.stderr.write('ERROR: %s\n' % str(err))
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)


if __name__ == '__main__':
    manager.run()