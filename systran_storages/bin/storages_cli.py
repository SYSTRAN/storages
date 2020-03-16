#!/usr/bin/env python
# coding: utf-8
import argparse
import json
import os
import logging
from datetime import datetime

from systran_storages import StorageClient


def resolvedpath(path):
    fields = path.split(':')
    if not len(fields) == 2 or not fields[1].startswith('/'):
        raise argparse.ArgumentError(None, "incorrect storage path: %s" % path)
    return path


def resolvedjson(path):
    with open(path) as jsonf:
        content = jsonf.read()
        data = json.loads(content)
        if 'filter' in data:
            filter_query = data['filter']
    return filter_query


def check_format(corpus_format):
    if corpus_format not in ['application/x-tmx+xml', 'text/bitext']:
        raise argparse.ArgumentError(None, "incorrect format: %s" % corpus_format)
    return corpus_format


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('-c', '--config', default=None, required=True,
                        help='Storages configuration file.')
    parser.add_argument('--info', '-v', action='store_true', help='info mode')
    parser.add_argument('--verbose', '-vv', action='store_true', help='verbose mode')

    subparsers = parser.add_subparsers(help='command help', dest='cmd')
    subparsers.required = True

    parser_list = subparsers.add_parser('list', help='list file on a storage')
    parser_list.add_argument('--recursive', '-r', action='store_true', help='recursive listing')
    parser_list.add_argument('storage', type=resolvedpath, help='path to list')

    parser_get = subparsers.add_parser('get', help='download a file or directory')
    parser_get.add_argument('storage', type=resolvedpath,
                            help='path to file or directory to download, directory must ends with /')
    parser_get.add_argument('local', type=str, help='local path')

    parser_get = subparsers.add_parser('push', help='upload a file or directory')
    parser_get.add_argument('local', type=str, help='local path to file or directory to upload')
    parser_get.add_argument('storage', type=resolvedpath,
                            help='path to file or directory to download, directory must ends with /')

    parser_get = subparsers.add_parser('delete', help='delete a corpus')
    parser_get.add_argument('storage', type=resolvedpath,
                            help='path to file or directory to download, directory must ends with /')
    parser_get.add_argument('corpusId', type=str, help='corpus id')

    parser_stat = subparsers.add_parser('stat', help='returns stat on a remote file/directory')
    parser_stat.add_argument('storage', type=resolvedpath,
                             help='path to file or directory to download, directory must ends with /')

    parser_get = subparsers.add_parser('download', help='Export a corpus in TMX(default) or biText')
    parser_get.add_argument('storage', type=resolvedpath,
                            help='path to file or directory to download, directory must ends with /')
    parser_get.add_argument('corpusId', type=str, help='corpus id')
    parser_get.add_argument('format', type=check_format,
                            help='Format of the corpus (application/x-tmx+xml, text/bitext)')

    parser_search = subparsers.add_parser('search', help='list corpus segments identified '
                                                         'by corpus id')
    parser_search.add_argument('storage', type=resolvedpath, help='remote path')
    parser_search.add_argument('id', help='remote id')
    parser_search.add_argument('search_query', type=resolvedjson,
                               help='query text for search')
    parser_search.add_argument('skip', default=0,
                               help='number of segments skip (default 0)')
    parser_search.add_argument('limit', default=0,
                               help='number of segments returned (default 0 meaning all)')

    parser_search = subparsers.add_parser('seg_delete', help='Delete segments identified by id')
    parser_search.add_argument('storage', type=resolvedpath, help='remote path')
    parser_search.add_argument('corpus_id', help='corpus id')
    parser_search.add_argument('ids', help='list segment id')

    args = parser.parse_args()
    if args.info:
        logging.basicConfig(level=logging.INFO)
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    with open(args.config) as jsonf:
        config = json.load(jsonf)
        # support configuration from automatic tests
        if 'storages' in config:
            config = config['storages']
        client = StorageClient(config=config)

    if args.cmd == "list":
        listdir = client.listdir(args.storage, args.recursive)
        for k in sorted(listdir.keys()):
            if listdir[k].get("is_dir"):
                print("dir", k)
            else:
                date = datetime.fromtimestamp(listdir[k]["last_modified"])
                print("   ", "%10d" % listdir[k]["entries"], date.strftime("%Y-%m-%dT%H:%M:%S"), k)
    elif args.cmd == "get":
        directory = args.storage.endswith('/')
        if directory:
            if os.path.isfile(args.local):
                raise ValueError("%s should be a directory" % args.local)
            client.get_directory(args.storage, args.local)
        else:
            client.get_file(args.storage, args.local)
    elif args.cmd == "push":
        client.push(args.local, args.storage)
    elif args.cmd == "delete":
        client.delete_corpus_manager(args.storage, args.corpusId)
    elif args.cmd == "stat":
        print(client.stat(args.storage))
    elif args.cmd == "download":
        client.stream_corpus_manager(args.storage, args.corpusId, args.format)
    elif args.cmd == "search":
        print(client.search(args.storage, args.id, args.search_query, args.skip, args.limit))
    elif args.cmd == "seg_delete":
        print(client.seg_delete(args.storage, args.corpus_id, args.ids))
    elif args.cmd == "seg_add":
        print(client.seg_add(args.storage, args.corpus_id, args.ids))


if __name__ == "__main__":
    main()
