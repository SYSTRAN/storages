"""Definition of `local` storage class"""

import shutil
import os
import tempfile
import logging

from systran_storages.storages import Storage

LOGGER = logging.getLogger(__name__)


class LocalStorage(Storage):
    """Storage using the local filesystem."""

    def __init__(self, storage_id=None, basedir=None):
        super().__init__(storage_id or "local")
        self._basedir = basedir

    def _get_file_safe(self, remote_path, local_path):
        with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
            LOGGER.debug('Copy remote_path %s to local_path %s via %s', remote_path, local_path,
                         tmpfile.name)
            tmpfile.close()
            shutil.copy2(remote_path, tmpfile.name)
            shutil.move(tmpfile.name, local_path)

    def _check_existing_file(self, remote_path, local_path):
        if not os.path.exists(local_path):
            return False
        stat_remote = os.stat(remote_path)
        stat_local = os.stat(local_path)
        return stat_remote.st_mtime == stat_local.st_mtime and\
            stat_remote.st_size == stat_local.st_size

    def stream(self, remote_path, buffer_size=1024, stream_format=None):
        def generate():
            """generator function to stream local file"""
            with open(remote_path, "rb") as f:
                for chunk in iter(lambda: f.read(buffer_size), b''):
                    yield chunk

        return generate()

    def push_file(self, local_path, remote_path):
        shutil.copy2(local_path, remote_path)

    def mkdir(self, remote_path):
        if not os.path.exists(remote_path):
            os.makedirs(remote_path)

    def _delete_single(self, remote_path, isdir):
        if not os.path.isdir(remote_path):
            os.remove(remote_path)
        else:
            shutil.rmtree(remote_path, ignore_errors=True)

    def stat(self, remote_path):
        if os.path.isdir(remote_path):
            return {'is_dir': True}
        if os.path.isfile(remote_path):
            stat = os.stat(remote_path)
            return {'size': stat.st_size, 'last_modified': stat.st_mtime}
        return False

    def listdir(self, remote_path, recursive=False, is_file=False, options=None):
        listfile = {}

        if is_file:
            stat = os.stat(remote_path)
            return {remote_path: {'size': stat.st_size, 'last_modified': stat.st_mtime}}

        if not os.path.isdir(remote_path):
            raise ValueError("%s is not a directory" % remote_path)

        def getfiles_rec(path):
            """recursive listdir"""
            for f in os.listdir(path):
                fullpath = os.path.join(path, f)
                if self._basedir:
                    rel_fullpath = self._external_path(fullpath)
                else:
                    rel_fullpath = fullpath
                if os.path.isdir(fullpath):
                    if recursive:
                        getfiles_rec(fullpath)
                    else:
                        listfile[rel_fullpath + '/'] = {'is_dir': True}
                else:
                    if os.path.isfile(fullpath):
                        stat = os.stat(fullpath)
                        listfile[rel_fullpath] = {'size': stat.st_size,
                                                  'last_modified': stat.st_mtime}

        getfiles_rec(remote_path)

        return listfile

    def rename(self, old_remote_path, new_remote_path):
        os.rename(old_remote_path, new_remote_path)

    def exists(self, remote_path):
        return os.path.exists(remote_path)

    def isdir(self, remote_path):
        return os.path.isdir(remote_path)

    def _internal_path(self, path):
        if self._basedir:
            if path.startswith('/'):
                path = path[1:]
            path = os.path.join(self._basedir, path)
        return path

    def _external_path(self, path):
        if self._basedir:
            return os.path.relpath(path, self._basedir)
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
