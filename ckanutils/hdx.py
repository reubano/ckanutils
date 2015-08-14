#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: sw=4:ts=4:expandtab

""" Miscellaneous HDX scripts """

from __future__ import (
    absolute_import, division, print_function, with_statement,
    unicode_literals)

import sys

from pprint import pprint
from operator import itemgetter
from time import time, strptime
from datetime import datetime
from os import environ
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
        for i in possible:
            if i in f.lower():
                yield f


def gen_items(items, tagged=None, named=None):
    keys = ['metadata_modified', 'revision_timestamp', 'last_modified']

    for i in items:
        try:
            timestamp = (i[key] for key in keys if key in i).next()
        except StopIteration:
            raise KeyError(
                'None of the following keys found: %s in item' % keys)
        else:
            timestamp = timestamp or i.get('created')

        if i['state'] != 'active':
            continue

        if timestamp:
            i['updated'] = strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f')
        else:
            i['updated'] = datetime.now()

        if named and named.lower() in i['name'].lower():
            yield i
        elif tagged and 'tags' in i:
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


def find_ids(organization, ckan, pnamed=None, ptagged=None, rnamed=None):
    all_packages = organization['packages']
    packages = list(gen_items(all_packages, named=pnamed, tagged=ptagged))
    sorted_kwargs = {'key': itemgetter('updated'), 'reverse': True}

    if pnamed:
        verb, word = 'named', pnamed
    elif ptagged:
        verb, word = 'tagged', ptagged
    else:
        verb, word = 'named', '*'

    print('Searching for resources %s `%s`...' % (verb, word))

    try:
        set_id = sorted(packages, **sorted_kwargs)[0]['name']
    except IndexError:
        org_id = organization['id']
        print('Error: %s has no packages %s `%s`' % (org_id, verb, word))
        return {'resource_id': '', 'set_id': ''}
    else:
        found_set = ckan.package_show(id=set_id)
        resources = list(gen_items(found_set['resources'], named=rnamed))
        found_resource = sorted(resources, **sorted_kwargs)[0]
        print('Selected resource: %s.' % found_resource['name'])
        return {'resource_id': found_resource['id'], 'set_id': set_id}


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
    three_dub_fields = utils.underscorify(_fields) if sanitize else _fields

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

    def_where = find_first_common(three_dub_fields, geojson_fields) or ''
    who_column = kwargs.get('who') or find_who(three_dub_fields)
    what_column = kwargs.get('what') or find_what(three_dub_fields)
    where_column = kwargs.get('where') or def_where or find_where(
        three_dub_fields)
    where_column_2 = kwargs.get('where') or def_where or find_where(
        geojson_fields)
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


def update(three_dub_id, topline_id=None, **kwargs):
    ckan_kwargs = {k: v for k, v in kwargs.items() if k in api.CKAN_KEYS}
    ckan = api.CKAN(**ckan_kwargs)
    ds.update(topline_id, ckan=ckan) if topline_id else None
    ds.update(three_dub_id, ckan=ckan)


if __name__ == '__main__':
    manager.main()
