#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: sw=4:ts=4:expandtab

""" Miscellaneous HDX scripts """

from __future__ import (
    absolute_import, division, print_function, with_statement,
    unicode_literals)

import traceback
import sys

from pprint import pprint
from operator import itemgetter
from time import time, strptime
from os import unlink, getcwd, environ, path as p
from manager import Manager
from . import api, utils, datastorer as ds

manager = Manager()


def get_field(fields, field):
    if field:
        return {f.lower(): f for f in fields if f}[field.lower()]
    else:
        return ''


def gen_fuzzies(fields, possible):
    for f in fields:
        for p in possible:
            if p in f.lower():
                yield f


def gen_items(items, tagged=None, named=None):
    for i in items:
        try:
            timestamp = i['metadata_modified']
        except KeyError:
            timestamp = i['revision_timestamp']

        if i['state'] != 'active':
            continue

        i['updated'] = strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f')

        if named and named.lower() in i['name'].lower():
            yield i
        elif tagged:
            for t in i['tags']:
                if t['name'] == tagged:
                    yield i
        elif not (named or tagged):
            yield i


def find_first_common(*args):
    sets = (set(i.lower() for i in arg) for arg in args)
    intersect = reduce(lambda x, y: x.intersection(y), sets)

    try:
        return intersect.pop()
    except KeyError:
        return ''


def find_who(fields):
    try:
        return gen_fuzzies(fields, ['org', 'partner']).next()
    except StopIteration:
        return ''


def find_what(fields):
    try:
        return gen_fuzzies(fields, ['sector', 'cluster']).next()
    except StopIteration:
        return ''


def find_where(fields):
    possible = ['code', 'state', 'region', 'province', 'township']

    try:
        return gen_fuzzies(fields, possible).next()
    except StopIteration:
        return ''


@manager.arg(
    'org_id', help='the organization id', nargs='?', default=sys.stdin)
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
    'image_sq', 's', help='logo 75x75 (url or google doc id)',
    default='0B01Bdplw4VkCNG5HLXowNzV4WGM')
@manager.arg(
    'image_rect', 'R', help='logo 300x125 (url or google doc id)',
    default='0B01Bdplw4VkCZC1vQWxJVlVGZWM')
@manager.arg('color', 'c', help='the base color', default='#026bb5')
@manager.arg('topline', 't', help='topline figures resource id')
@manager.arg('3w', 'w', help='3w data resource id (default: most recently updated resource containing `3w`)')
@manager.arg('geojson', 'g', help='the map boundaries geojson resource id (default: most recently updated resource matching the org country)')
@manager.arg('where', 'W', help='The `where` field (case insensitive) (default: first column name found matching a `3w` field).')
@manager.arg(
    'quiet', 'q', help='suppress debug statements', type=bool, default=False)
@manager.command
def customize(org_id, **kwargs):
    """Introspects custom organization values"""
    verbose = not kwargs['quiet']
    ckan_kwargs = {k: v for k, v in kwargs.items() if k in api.CKAN_KEYS}
    image_sq = kwargs.get('image_sq')
    image_rect = kwargs.get('image_rect')
    three_dub_id = kwargs.get('3w')
    geojson_id = kwargs.get('geojson_id')
    topline_id = kwargs.get('topline') or ''

    ckan = api.CKAN(**ckan_kwargs)
    organization = ckan.organization_show(id=org_id)
    extras = {e['key']: e['value'] for e in organization['extras']}

    if geojson_id:
        geojson_set_id = ckan.get_package_id(geojson_id)
    else:
        country = org_id.split('-')[1]
        geojson_set_id = 'json-repository'
        geojson_set = ckan.package_show(id=geojson_set_id)
        resources = list(gen_items(geojson_set['resources'], named=country))
        # print([(r['name'], r['id']) for r in resources])
        sorted_kwargs = {'key': itemgetter('updated'), 'reverse': True}
        geojson = sorted(resources, **sorted_kwargs)[0]
        geojson_id = geojson['id']

    if three_dub_id:
        three_dub_set_id = ckan.get_package_id(three_dub_id)
    else:
        packages = list(gen_items(organization['packages'], '3w'))
        sorted_kwargs = {'key': itemgetter('updated'), 'reverse': True}

        try:
            three_dub_set = sorted(packages, **sorted_kwargs)[0]
        except IndexError:
            print('Error: %s has no packages tagged `3w`' % org_id)
            sys.exit(1)

        three_dub_set_id = three_dub_set['name']
        resources = list(gen_items(three_dub_set['resources']))
        three_dub = sorted(resources, **sorted_kwargs)[0]
        three_dub_id = three_dub['id']

    viz_url = '%s/dataset/%s' % (kwargs['remote'], three_dub_set_id)
    # three_dub_fields = ckan.get_field_names(three_dub_id)
    three_dub_fields = ['Organization','SectorCluster','Status','Township','StateRegion']
    # shape = ckan.fetch_shape(geojson_id)
    # geojson_fields = shape['features'][0]['properties'].keys()
    geojson_fields = ['ST_PCODE', 'ts', 'st', 'dt_pcode', 'gid', 'dt', 'TS_PCODE']

    if verbose:
        print('3w fields: %s' % three_dub_fields)
        print('geojson fields: %s' % geojson_fields)

    def_where = find_first_common(three_dub_fields, geojson_fields)
    who_column = kwargs.get('who_column') or find_who(three_dub_fields)
    what_column = kwargs.get('what_column') or find_what(three_dub_fields)

    if def_where:
        where_column = kwargs.get('where') or def_where
        where_column_2 = kwargs.get('where') or def_where
        name_column = kwargs.get('where') or def_where
    else:
        where_column = kwargs.get('where') or find_where(three_dub_fields)
        where_column_2 = kwargs.get('where') or find_where(geojson_fields)
        name_column = kwargs.get('where') or ''

    if 'http' not in image_sq:
        gdocs = 'https://docs.google.com'
        image_sq = '%s/uc?id=%s&export=download' % (gdocs, image_sq)

    if 'http' not in image_rect:
        gdocs = 'https://docs.google.com'
        image_rect = '%s/uc?id=%s&export=download' % (gdocs, image_rect)

    data = {
        'name': org_id,
        'resource_id_1': three_dub_id,
        'resource_id_2': geojson_id,
        'topline_resource': topline_id,
        'datatype_1': kwargs.get('datatype_1') or 'datastore',
        'datatype_2': kwargs.get('datatype_2') or 'filestore',
        'org_url': extras['org_url'],
        'description': organization['description'],
        'title': organization['title'],
        'image_sq': image_sq,
        'image_rect': image_rect,
        'highlight_color': kwargs.get('color'),
        'dataset_id_1': three_dub_set_id,
        'dataset_id_2': geojson_set_id,
        'who_column': get_field(three_dub_fields, who_column),
        'what_column': get_field(three_dub_fields, what_column),
        'where_column': get_field(three_dub_fields, where_column),
        'where_column_2': get_field(geojson_fields, where_column_2),
        'map_district_name_column': get_field(geojson_fields, name_column),
        'viz_data_link_url': viz_url,
        'visualization_select': kwargs.get('viz_type', '3W-dashboard'),
        'viz_title': kwargs.get('viz_title', "Who's doing what and where?"),
        'colors': [
            '#c6d5ed', '#95b5df', '#659ad2', '#026bb5',
            '#659ad2', '#213b68', '#101d4e', '#000035'],
        'use_org_color': True,
        'modified_at': int(time()),
    }

    control_sheet_keys = [
        'highlight_color', 'image_rect', 'image_sq', 'dataset_id_1',
        'datatype_1', 'resource_id_1', 'where_column', 'description',
        'org_url', 'title', 'dataset_id_2', 'datatype_2',
        'map_district_name_column', 'resource_id_2', 'where_column_2',
        'name', 'topline_resource', 'visualization_select',
        'viz_data_link_url', 'viz_title', 'what_column', 'who_column']

    if verbose:
        print('\nCustom pages control sheet data:')
        [print(data[k]) for k in control_sheet_keys]


def update(org_id, **kwargs):
    ckan = api.CKAN(**ckan_kwargs)
    ds.update(topline_id, ckan=ckan) if topline_id else None
    ds.update(three_dub_id, ckan=ckan)


if __name__ == '__main__':
    manager.main()
