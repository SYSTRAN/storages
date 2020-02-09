"""Definition of `http` storage class"""
import glob
import re
from datetime import datetime
import logging
import shutil
import tempfile
import os

import requests

from systran_storages.storages import Storage
from systran_storages.storages.utils import datetime_to_timestamp

LOGGER = logging.getLogger(__name__)


class CMStorages(Storage):
    """Simple http file-only storage."""

    def __init__(self, storage_id, hostURL, accountID=None, root_folder=None):
        super(CMStorages, self).__init__(storage_id)
        self.hostURL = hostURL
        self.accountID = accountID
        self.root_folder = root_folder

    def _get_file_safe(self, remote_path, local_path):
        list_objects = requests.get(self.hostURL + "/corpus/list?accountId=" + self.accountID)
        list_objects = list_objects.json()["files"]
        list_corpus = [item["filename"] for item in list_objects]
        if remote_path in [item["filename"] for item in list_objects]:
            with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
                params = (
                    ('accountId', self.accountID),
                    ('id', local_path),
                )
                res = requests.get(f'{self.hostURL}/corpus/export', params=params)
                if res.status_code != 200:
                    raise RuntimeError(
                        'cannot not get %s (response code %d)' % (remote_path, res.status_code))
                tmpfile.write(res.content)
                shutil.move(tmpfile.name, local_path)

    def _check_existing_file(self, remote_path, local_path):
        # not optimized for http download yet
        return False

    def stream(self, remote_path, buffer_size=1024):
        res = requests.get(self._pattern_get % remote_path, stream=True)
        if res.status_code != 200:
            raise RuntimeError(
                'cannot not get %s (response code %d)' % (remote_path, res.status_code))

        def generate():
            for chunk in res.iter_content(chunk_size=buffer_size, decode_unicode=None):
                yield chunk

        return generate()

    def push_file(self, local_path, remote_path, format_path):
        if self.hostURL is None:
            raise ValueError('http storage %s can not handle hostURL' % self._storage_id)

        if remote_path == "":
            remote_path = '/' + local_path.split("/")[-1]

        if not remote_path.startswith('/'):
            remote_path = "/" + remote_path

        parameters = {
            'accountId': (None, self.accountID),
            'filename': (None, remote_path),
            'format': (None, format_path),
            'corpus': (local_path, open(local_path, 'rb')),
        }

        requests.post(f'{self.hostURL}/corpus/import', files=parameters)

    def listdir(self, remote_path, recursive=False, is_file=False):
        if self.hostURL is None:
            raise ValueError('http storage %s can not handle hostURL' % self._storage_id)

        listdir = {}

        list_objects = requests.get(self.hostURL + "/corpus/list?accountId=" + self.accountID)
        list_objects = list_objects.json()
        if 'directories' in list_objects:
            for key in list_objects['directories']:
                listdir[key['Prefix']] = {'is_dir': True}
        if 'files' in list_objects:
            for key in list_objects['files']:
                if remote_path in key['filename']:
                    if remote_path == "":
                        remote_path = "/"
                    if not remote_path.startswith('/'):
                        remote_path = '/' + remote_path
                    date_time = datetime.strptime(key["createdAt"].strip(), "%a %b %d %H:%M:%S %Y")
                    basename = os.path.basename(key["filename"])
                    folder = os.path.dirname(key['filename'])
                    if not folder.startswith('/'):
                        folder = "/" + folder
                    listdir[basename] = {'entries': int(key['nbSegments']),
                                         'format': key['format'],
                                         'id': key['id'],
                                         'sourceLanguage': key['sourceLanguage'],
                                         'targetLanguages': key['targetLanguages'],
                                         'last_modified': datetime_to_timestamp(date_time)}
                    if remote_path + "/" in folder or remote_path == '/':
                        sub_dir = folder.split(remote_path)[1]
                        internal_sub_dir = self._internal_path(sub_dir)
                        if len(internal_sub_dir) > 0:
                            listdir[internal_sub_dir] = {"is_dir": True}
        return listdir

    def _delete_single(self, remote_path, isdir):
        raise NotImplementedError()

    def rename(self, old_remote_path, new_remote_path):
        raise NotImplementedError()

    def mkdir(self, remote_path):
        pass

    def isdir(self, remote_path):
        if remote_path.endswith('/'):
            return True
        return False

    def exists(self, remote_path):
        params = {
            'accountId': (None, self.accountID),
            'filename': (None, remote_path),
        }

        response = requests.post(f'{self.hostURL}/corpus/exists', files=params)
        if 'true' in response.text:
            return True
        return False

    def _internal_path(self, remote_path):
        if remote_path.startswith('/'):
            return remote_path[1:]
        return remote_path

    def stat(self, remote_path):
        pass
