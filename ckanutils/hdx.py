#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: sw=4:ts=4:expandtab

""" Miscellaneous HDX scripts """

from __future__ import (
    absolute_import, division, print_function, with_statement,
    unicode_literals)

import sys

from pprint import pprint
from os import environ
from manager import Manager
from tabutils import process as tup

from . import api, datastorer as ds

manager = Manager()


def find_field(fields, ftype='who', default=None, **kwargs):
    possibilities = {
        'who': ['org', 'partner'],
        'what': ['sector', 'cluster'],
        'where': ['code', 'state', 'region', 'province', 'township'],
    }

    field = kwargs.get(ftype) or default
    return field or tup.find(fields, possibilities[ftype], method='fuzzy')


def deref_field(fields, field):
    if field:
        return {f.lower(): f for f in fields if f}[field.lower()]
    else:
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
    'image_sq', 'S', help='logo 75x75 (url or google doc id)',
    default='0B01Bdplw4VkCNG5HLXowNzV4WGM')
@manager.arg(
    'image_rect', 'R', help='logo 300x125 (url or google doc id)',
    default='0B01Bdplw4VkCZC1vQWxJVlVGZWM')
@manager.arg('color', 'c', help='the base color', default='#026bb5')
@manager.arg(
    'topline', 't', help=(
        'topline figures resource id (default: most recently updated resource'
        ' containing `topline`)'))
@manager.arg(
    '3w', 'w', help=(
        '3w data resource id (default: most recently updated resource tagged'
        ' `3w`)'))
@manager.arg(
    'geojson', 'g', help=(
        'the map boundaries geojson resource id (default: most recently '
        'updated resource matching the org country)'))
@manager.arg(
    'where', 'W', help=(
        'The `where` field (case insensitive) (default: first column name'
        ' found matching a `3w` field).'))
@manager.arg(
    'sanitize', 's', help='underscorify and lowercase field names', type=bool,
    default=False)
@manager.arg(
    'quiet', 'q', help='suppress debug statements', type=bool, default=False)
@manager.command
def customize(org_id, **kwargs):
    """Introspects custom organization values"""
    verbose = not kwargs['quiet']
    ckan_kwargs = {k: v for k, v in kwargs.items() if k in api.CKAN_KEYS}
    image_sq = kwargs.get('image_sq')
    image_rect = kwargs.get('image_rect')
    sanitize = kwargs.get('sanitize')
    three_dub_id = kwargs.get('3w')
    geojson_id = kwargs.get('geojson')
    topline_id = kwargs.get('topline')

    ckan = api.CKAN(**ckan_kwargs)
    organization = ckan.organization_show(id=org_id, include_datasets=True)
    hdx = ckan.organization_show(id='hdx', include_datasets=True)
    extras = {e['key']: e['value'] for e in organization['extras']}

    if three_dub_id:
        three_dub_set_id = ckan.get_package_id(three_dub_id)
    else:
        ids = find_ids(organization, ckan, '3w', '3w')
        three_dub_set_id = ids['set_id']
        three_dub_id = ids['resource_id']

    if not three_dub_id:
        sys.exit(1)

    if not topline_id:
        topline_id = find_ids(organization, ckan, 'topline')['resource_id']

    if geojson_id:
        geojson_set_id = ckan.get_package_id(geojson_id)
    else:
        country = org_id.split('-')[1]
        ids = find_ids(hdx, ckan, 'json-repository', rnamed=country)
        geojson_set_id = ids['set_id']
        geojson_id = ids['resource_id']

    viz_url = '%s/dataset/%s' % (kwargs['remote'], three_dub_set_id)
    three_dub_r = ckan.fetch_resource(three_dub_id)
    _fields = three_dub_r.iter_lines().next().split(',')
    three_dub_fields = tup.underscorify(_fields) if sanitize else _fields

    if geojson_id:
        geojson_r = ckan.fetch_resource(geojson_id)
        geojson_fields = geojson_r.json()['features'][0]['properties'].keys()
    else:
        geojson_fields = []

    if verbose:
        print('3w fields:')
        pprint(three_dub_fields)
        print('geojson fields:')
        pprint(geojson_fields)

    def_where = tup.find(three_dub_fields, geojson_fields) or ''
    who_column = find_field(three_dub_fields, 'who', **kwargs)
    what_column = find_field(three_dub_fields, 'what', **kwargs)
    where_column = find_field(three_dub_fields, 'where', def_where, **kwargs)

    where_column_2 = find_field(geojson_fields, 'where', def_where, **kwargs)
    name_column = kwargs.get('where') or def_where

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
        'who_column': deref_field(three_dub_fields, who_column),
        'what_column': deref_field(three_dub_fields, what_column),
        'where_column': deref_field(three_dub_fields, where_column),
        'where_column_2': deref_field(geojson_fields, where_column_2),
        'map_district_name_column': deref_field(geojson_fields, name_column),
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


def update(three_dub_id, topline_id=None, **kwargs):
    ckan_kwargs = {k: v for k, v in kwargs.items() if k in api.CKAN_KEYS}
    ckan = api.CKAN(**ckan_kwargs)
    ds.update(topline_id, ckan=ckan) if topline_id else None
    ds.update(three_dub_id, ckan=ckan)


if __name__ == '__main__':
    manager.main()
