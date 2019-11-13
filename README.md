# Storages

## Overview

This library provides a generic interface for multiple file storages:

* local storage
* remote storage via SSH
* AWS S3 (https://aws.amazon.com/s3/)
* OpenStack Object Storage service Swift (https://docs.openstack.org/api-ref/object-store/)
* custom HTTP storage

## Installation

```
pip install https://github.com/SYSTRAN/storages/archive/master.tar.gz
```

This installs the package `systran-storages` and command line utility `systran-storages-cli` allowing to easily test the connections to the different services.

## Features

For each service, the library provides the following functions:

* `get_file(remote_path, local_path)`: download a remote file to a local file. If the file is already present (based on checksum and/or date and/or file size), the local file is not modified.
* `get_directory(remote_path, local_path)`: recursively download all the files from a given directory.
* `stream(remote_path, buffer_size=1024)`: stream the content of the file through a generator function in `buffer_size` packets. If the service does not support streaming, file is first downloaded locally to a temporary directory then streamed.
* `push(local_path, remote_path)`: push a local file to a remote location
* `mkdir(local_path, remote_path)`: create a remote directory
* `listdir(remote_path, recursive=False)`: list of the files and directory at the `remote_path` location. Returns dictionary where keys are the file/directory name and values are stat of files/directories.
* `delete(remote_path, recursive=False)`: delete a file or a directory
* `rename(remote_path, new_remote_path)`: rename a remote_file
* `stat(remote_path)`: statistic on file or directory - returns `{'is_dir': True}` for directory, and `{'size': SIZE, 'last_modified': TIMESTAMP}` for files
* `exists(remote_path)`: check if a remote file or directory exists

## Configuration

Configuration of the services is done with a JSON dictionary: `{"key1": DEF1, "key2": DEF2, ..., "keyN": DEFN}`, where `key1`...`keyN` are the identifiers of each storage, and `DEF1`...`DEFN` their configuration.

Configurations `DEFI` are JSON dictionary with following fields:
```json
{
	"description": "description of the storage (optional)",
	"type": "local|ssh|s3|swift|http",
	[STORAGE_OPTIONS]
}
```

The dictionary is passed to the `StorageClient` constructor:

```python
from systran_storages import StorageClient

client = StorageClient(services)
```

`systran_storages/bin/storages_cli.py` gives a comprehensive usage example.

## Usage

The different services are method of the `client` object where `remote_path` is a string in the format `storage:path`:

* `storage` is the identifier of the storage as defined in the configuration dictionary
* `path` is a path relative to the storage referenced by `storage`

Paths use `/` delimiter. Path starts with `/` and no relative path accessor like `.`, `..` are supported. For the storages with actual directory structure (S3, Swift, ...), a hierarchical file organization is simulated.

## Services

### Local

Type of the service is `local`. 

_Storage options_:

* (optional, default `'/'`) `basedir`: defines the base directory where files are stored.

### SSH

Type of the service is `ssh`.

_Storage options_:

* (required) `server`: name of IP of the remote server
* (option, default 22) `port`: name of the port to connect to
* (required) `user`: login to connect with on the server
* (optional) `password`: user password, preferably use `pkey`
* (optional) `pkey`: private key used to connect on the service. The value does not include header (`-----BEGIN RSA PRIVATE KEY-----`) and tail (`-----END RSA PRIVATE KEY-----`)
* (optional, default user home) `basedir`: defines the base directory where files are stored.

### S3

Type of the service is `s3`.

_Storage options_:

* (required) `bucket_name`
* (required) `access_key_id`
* (required) `secret_access_key`
* (optional) `region_name`
* (optional) `assume_role`: arn of assume_role
* (optional) `transfer_config`: additional transfer configuration parameters as defined here: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html.

### Swift

Type of the service is `swift`.

_Storage options_:

* (required) `container_name`
* (optional) `auth_config`: if provided, defines the configuration for authentifying to the Open Stack service (https://docs.openstack.org/python-swiftclient/latest/service-api.html#authentication).
* (optional) `transfer_config`: https://docs.openstack.org/python-swiftclient/latest/service-api.html#configuration

### HTTP

Type of the service is `http`.

_Storage options_:

* (required) `pattern_get`
* (required) `pattern_push`
* (required) `pattern_list`

Note HTTP storages do not define all the possible functions:

* `rename` is not defined
* `exists` and `listdir` are defined only if `pattern_list` option is defined.
