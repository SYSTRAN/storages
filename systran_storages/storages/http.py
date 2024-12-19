"""Definition of `http` storage class"""

import os
import tempfile
import shutil
import logging

import requests

from systran_storages.storages import Storage

LOGGER = logging.getLogger(__name__)


class HTTPStorage(Storage):
    """Simple http file-only storage."""

    def __init__(self, storage_id, pattern_get, pattern_push=None, pattern_list=None):
        super().__init__(storage_id)
        self._pattern_get = pattern_get
        self._pattern_push = pattern_push
        self._pattern_list = pattern_list

    def _get_file_safe(self, remote_path, local_path):
        with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
            res = requests.get(self._pattern_get % remote_path)
            if res.status_code != 200:
                raise RuntimeError(
                    'cannot not get %s (response code %d)' % (remote_path, res.status_code))
            tmpfile.write(res.content)
            shutil.move(tmpfile.name, local_path)

    def _check_existing_file(self, remote_path, local_path):
        # not optimized for http download yet
        return False

    def _get_checksum_file(self, local_path):
        return None

    def stream(self, remote_path, buffer_size=1024, stream_format=None):
        res = requests.get(self._pattern_get % remote_path, stream=True)
        if res.status_code != 200:
            raise RuntimeError(
                'cannot not get %s (response code %d)' % (remote_path, res.status_code))

        def generate():
            for chunk in res.iter_content(chunk_size=buffer_size, decode_unicode=None):
                yield chunk

        return generate()

    def push_file(self, local_path, remote_path, lp=None, is_advanced=False):
        if self._pattern_push is None:
            raise ValueError('http storage %s can not handle post requests' % self._storage_id)
        with open(local_path, "rb") as f:
            data = f.read()
            res = requests.post(url=self._pattern_push % remote_path,
                                data=data,
                                headers={'Content-Type': 'application/octet-stream'})
            if res.status_code != 200:
                raise RuntimeError('cannot not post %s to %s (response code %d)' % (
                    local_path,
                    remote_path,
                    res.status_code))

    def listdir(self, remote_path, recursive=False, is_file=False, options=None):
        if self._pattern_list is None:
            raise ValueError('http storage %s can not handle list request' % self._storage_id)

        res = requests.get(self._pattern_list % remote_path)
        if res.status_code != 200:
            raise RuntimeError('Error when listing remote directory %s (status %d)' % (
                remote_path, res.status_code))
        data = res.json()
        listdir = {}
        for f in data:
            path = os.path.join(remote_path, f["path"])
            listdir[path] = {'path': path}
        return listdir

    def _delete_single(self, remote_path, isdir):
        raise NotImplementedError()

    def rename(self, old_remote_path, new_remote_path):
        raise NotImplementedError()

    def mkdir(self, remote_path):
        return

    def isdir(self, remote_path):
        if remote_path.endswith('/'):
            return True
        return False

    def exists(self, remote_path):
        raise NotImplementedError()

    def _internal_path(self, path):
        return path

    def check_for_aliases(self, local_path):
        return None