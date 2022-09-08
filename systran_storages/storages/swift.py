"""Definition of `swift` storage class"""

import os
import tempfile
import shutil
import logging
from datetime import datetime

from swiftclient.service import SwiftService, SwiftUploadObject, SwiftCopyObject
import six

from systran_storages.storages.utils import datetime_to_timestamp
from systran_storages.storages import Storage

logging.getLogger("requests").setLevel(logging.CRITICAL)
logging.getLogger("swiftclient").setLevel(logging.CRITICAL)

LOGGER = logging.getLogger(__name__)


class SwiftStorage(Storage):
    """Storage on OpenStack swift service."""

    def __init__(self, storage_id, container_name, auth_config=None, transfer_config=None):
        super().__init__(storage_id)
        opts = transfer_config or {}
        if auth_config:
            for k, v in six.iteritems(auth_config):
                opts[k] = v
        self._client = SwiftService(opts)
        self._container = container_name

    def _get_file_safe(self, remote_path, local_path):
        tmpdir = tempfile.mkdtemp()
        results = self._client.download(container=self._container,
                                        objects=[remote_path],
                                        options={"out_directory": tmpdir})
        has_results = False
        for r in results:
            has_results = True
            if not r["success"]:
                raise RuntimeError("Cannot download [%s]: %s" % (remote_path, r["error"]))
            timestamp = float(r["response_dict"]["headers"]["x-timestamp"])
            os.utime(os.path.join(tmpdir, remote_path), (timestamp, timestamp))
        if not has_results:
            raise RuntimeError("Cannot copy download [%s]: NO RESULT" % remote_path)
        shutil.move(os.path.join(tmpdir, remote_path), local_path)
        shutil.rmtree(tmpdir, ignore_errors=True)

    def _check_existing_file(self, remote_path, local_path):
        if os.path.exists(local_path):
            results = self._client.stat(self._container, objects=[remote_path])
            local_stat = os.stat(local_path)
            for r in results:
                if r['success']:
                    if int(r['headers']['content-length']) != local_stat.st_size:
                        return False
                    timestamp = float(r["headers"]["x-timestamp"])
                    if int(local_stat.st_mtime) == int(timestamp):
                        return True
        else:
            LOGGER.debug('Cannot find %s', local_path)
        return False

    def stat(self, remote_path):
        if not remote_path.endswith('/'):
            results = self._client.stat(self._container, objects=[remote_path])
            for r in results:
                if r['success']:
                    return {'is_dir': False,
                            'size': int(r['headers']['content-length']),
                            'last_modified': float(r['headers']['x-timestamp']),
                            'etag': r['headers']['etag']}
            remote_path += '/'
        results = self._client.list(container=self._container, options={"prefix": remote_path,
                                                                        "delimiter": "/"})
        for r in results:
            if r['success']:
                return {'is_dir': True}
        return False

    def push_file(self, local_path, remote_path):
        (_, basename) = os.path.split(local_path)
        if not remote_path:
            remote_path = basename
        obj = SwiftUploadObject(local_path, object_name=remote_path)
        results = self._client.upload(self._container, [obj])
        has_results = False
        for r in results:
            has_results = True
            if not r["success"]:
                raise RuntimeError("Cannot push file [%s]>[%s]: %s" % (local_path, remote_path,
                                                                       r["error"]))
        if not has_results:
            raise RuntimeError("Cannot push file [%s]>[%s]: %s" % (local_path, remote_path,
                                                                   "NO RESULTS"))

    def stream(self, remote_path, buffer_size=1024, stream_format=None):
        def generate():
            tmpdir = tempfile.mkdtemp()
            results = self._client.download(container=self._container,
                                            objects=[remote_path],
                                            options={"out_directory": tmpdir})
            has_results = False
            for r in results:
                has_results = True
                if not r["success"]:
                    raise RuntimeError("Cannot download file [%s]: %s" % (remote_path, r["error"]))
            if not has_results:
                raise RuntimeError("Cannot download file [%s]: NO RESULTS" % remote_path)

            with open(os.path.join(tmpdir, remote_path), "rb") as f:
                for chunk in iter(lambda: f.read(buffer_size), b''):
                    yield chunk

            shutil.rmtree(tmpdir, ignore_errors=True)

        return generate()

    def listdir(self, remote_path, recursive=False, is_file=False, options=None):
        if not is_file and remote_path != '' and not remote_path.endswith('/'):
            remote_path += '/'
        options = {"prefix": remote_path}
        if not recursive:
            options["delimiter"] = "/"
        list_parts_gen = self._client.list(container=self._container,
                                           options=options)
        lsdir = {}
        for page in list_parts_gen:
            if page["success"]:
                for item in page["listing"]:
                    if "subdir" in item:
                        lsdir[item["subdir"]] = {'is_dir': True}
                    else:
                        path = item["name"]
                        last_modified = datetime.strptime(item["last_modified"],
                                                          '%Y-%m-%dT%H:%M:%S.%f')
                        lsdir[path] = {'size': item["bytes"],
                                       'last_modified': datetime_to_timestamp(last_modified)}
        return lsdir

    def mkdir(self, remote_path):
        if not remote_path.endswith("/") and remote_path:
            remote_path += "/"
        if self.exists(remote_path):
            return
        obj = SwiftUploadObject(None, object_name=remote_path)
        results = self._client.upload(self._container, [obj])
        has_results = False
        for r in results:
            has_results = True
            if not r["success"]:
                raise RuntimeError("cannot create the directory %s: %s" % (remote_path, r["error"]))
        if not has_results:
            raise RuntimeError("cannot create the directory %s: %s" % (remote_path, "No results"))

    def _delete_single(self, remote_path, isdir):
        if not isdir:
            results = self._client.delete(container=self._container, objects=[remote_path])
            has_results = False
            for r in results:
                has_results = True
                if not r["success"]:
                    raise RuntimeError("Cannot delete file [%s]: %s" % (remote_path, r["error"]))
            if not has_results:
                raise RuntimeError("Cannot delete file [%s]: NO RESULT" % remote_path)

    def rename(self, old_remote_path, new_remote_path):
        listfiles = self.listdir(old_remote_path, True, not self.isdir(old_remote_path))
        for f in listfiles:
            assert f[:len(old_remote_path)] == old_remote_path, "inconsistent listdir result"
            obj = SwiftCopyObject(f, {"destination": "/%s/%s%s" % (self._container, new_remote_path,
                                                                   f[len(old_remote_path):])})
            results = self._client.copy(self._container, [obj])
            has_results = False
            for r in results:
                has_results = True
                if not r["success"]:
                    raise RuntimeError("Cannot copy file [%s]: %s" % (old_remote_path, r["error"]))
            if not has_results:
                raise RuntimeError("Cannot copy file [%s]: NO RESULT" % old_remote_path)
            self._delete_single(f, False)

    def exists(self, remote_path):
        result = self._client.list(container=self._container, options={"prefix": remote_path,
                                                                       "delimiter": "/"})
        for page in result:
            if page["success"]:
                for item in page["listing"]:
                    if "subdir" in item:
                        return True
                    if (item["name"] == remote_path or
                            remote_path == '' or
                            remote_path.endswith('/') or
                            item["name"].startswith(remote_path + '/')):
                        return True
        return False

    def isdir(self, remote_path):
        if not remote_path.endswith('/'):
            return self.exists(remote_path+'/')
        return self.exists(remote_path)

    def _internal_path(self, path):
        # OpenStack does not work with paths but keys. This function possibly adapts a
        # path-like representation to a OpenStack key.
        if path.startswith('/'):
            return path[1:]
        return path

    def _get_checksum_file(self, local_path):
        pass

    def delete_corpus_manager(self, corpus_id):
        pass

    def push_corpus_manager(self, local_path, remote_path, corpus_id, user_data):
        pass

    def search(self, remote_ids, search_query, nb_skip, nb_limit):
        pass

    def seg_add(self, corpus_id, segments):
        pass

    def seg_delete(self, corpus_id, seg_ids):
        pass

    def seg_modify(self, corpus_id, seg_id, tgt_id, tgt_seg, src_seg):
        pass

    def stream_corpus_manager(self, remote_id, remote_format, buffer_size=1024):
        pass
