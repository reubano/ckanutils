# CKAN Utils

## Introduction

CKAN Utils is a [Python library](##library) and [command line interface](##cli) for interacting with remote and local [CKAN](http://ckan.org/) instances. It uses [ckanapi](https://github.com/ckan/ckanapi) under the hood, and is essentially a high level wrapper for it.

With CKAN Utils, you can

- Download a CKAN resource
- Parse structured CSV/Excel files and push them into a CKAN DataStore

If you have configured a [hash_table](#hash_table) in your CKAN instance, CKAN Utils will compute the hash of a file and only update the datastore if the file has changed.

This allows you to schedule a script to run on a frequent basis, e.g., `@hourly` via a cron job, without updating the CKAN instance unnecessarily.

## Requirements

CKAN Utils has been tested on the following configuration:

- MacOS X 10.9.5
- Python 2.7.9

Proposer requires the following in order to run properly:

- [Python >= 2.7](http://www.python.org/download) (MacOS X comes with python preinstalled)

## Installation

(You are using a [virtualenv](http://www.virtualenv.org/en/latest/index.html), right?)

     sudo pip install -e git+https://github.com/reubano/ckanutils@master#egg=ckanutils

## CLI

CKAN Utils comes with a built in command line interface `ckanny`. 

### Usage

     ckanny [<namespace>.]<command> [<args>]


### Examples

*show help*

    ckanny -h

```bash
usage: ckanny [<namespace>.]<command> [<args>]

positional arguments:
  command     the command to run

optional arguments:
  -h, --help  show this help message and exit

available commands:
  ver                      Show ckanny version
  
  [ds]
    delete                 Delete a datastore table
    update                 Update a datastore table based on the current filestore resource
    upload                 Upload a file to a datastore table
  
  [fs]
    fetch                  Download a filestore resource
```

*show version*

    ckanny ver

*fetch a resource*

    ckanny fs.fetch -k <CKAN_API_KEY> -r <CKAN_URL> <resource_id>

*show fs.fetch help*

    ckanny fs.fetch -h


```bash
usage: bin/ckanny fs.fetch [-h] [-q] [-C CHUNKSIZE_BYTES] [-c CHUNKSIZE_ROWS]
                           [-u UA] [-k API_KEY] [-r REMOTE] [-d DESTINATION]
                           [resource_id]

Download a filestore resource

positional arguments:
  resource_id           the resource id

optional arguments:
  -h, --help            show this help message and exit
  -q, --quiet           suppress debug statements
  -C CHUNKSIZE_BYTES, --chunksize-bytes CHUNKSIZE_BYTES
                        number of bytes to read/write at a time (default:
                        1048576)
  -c CHUNKSIZE_ROWS, --chunksize-rows CHUNKSIZE_ROWS
                        number of rows to write at a time (default: 1000)
  -u UA, --ua UA        the user agent (uses `CKAN_USER_AGENT` ENV if
                        available) (default: '')
  -k API_KEY, --api-key API_KEY
                        the api key (uses `CKAN_API_KEY` ENV if available)
                        (default: '')
  -r REMOTE, --remote REMOTE
                        the remote ckan url (uses `CKAN_REMOTE_URL` ENV if
                        available) (default: '')
  -d DESTINATION, --destination DESTINATION
                        the destination folder or file path (default: .)
```

## Library

CKAN Utils may also be used directly from Python.

### Examples

*Fetch a remote resource*

```python
from ckanutils import api

kwargs = {'filepath': '~/test.csv', 'api_key': 'mykey', 'remote': 'http://demo.ckan.org'}
ckan = api.CKAN(**kwargs)
r, filepath = ckan.fetch_resource('36f33846-cb43-438e-95fd-f518104a32ed')
print(r.encoding)
```

```python
kwargs = {'filepath': '~/test.csv', 'api_key': 'mykey', 'remote': None}
ckan = api.CKAN(**kwargs)
r, filepath = ckan.fetch_resource('36f33846-cb43-438e-95fd-f518104a32ed')
print(r.encoding)
```

## Configuration

CKAN Utils will use the following [Environment Variables](http://www.cyberciti.biz/faq/set-environment-variable-linux/) if set:

Environment Variable|Description
--------------------|-----------
CKAN_API_KEY|Your CKAN API Key
CKAN_HASH_TABLE_ID|Your CKAN instance hash table resource id
CKAN_REMOTE_URL|Your CKAN instance remote url
CKAN_USER_AGENT|Your user agent

## Hash Table

In order to enable file hashing, you must first load a csv to the filestore with the following structure:

datastore_id|hash
------------|----
|

Get the table's `resource_id` and use it to set the `CKAN_HASH_TABLE_ID` environment variable, in the command line for the `--hash-table-id` option, or in a Python file as the `hash_table_id` keyword argument to `api.CKAN`.

## Scripts

CKAN Utils comes with a built in task manager `manage.py` and a `Makefile`. 

### Examples

*Run python linter and noses tests*

```bash
manage lint
manage test
```

Or if `make` is more your speed...

```bash
make lint
make test
```

## Contributing

View [CONTRIBUTING.rst](https://github.com/reubano/ckanutils/blob/master/CONTRIBUTING.rst)

## License

CKAN Utils is distributed under the [MIT License](http://opensource.org/licenses/MIT), the same as [ckanapi](https://github.com/ckan/ckanapi).
