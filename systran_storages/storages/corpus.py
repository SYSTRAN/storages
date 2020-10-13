"""Definition of `Corpus Manager` storage class"""
import json
from datetime import datetime
import logging
import shutil
import tempfile
import os

import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder
from requests_toolbelt.multipart import decoder

from systran_storages.storages import Storage
from systran_storages.storages.utils import datetime_to_timestamp

LOGGER = logging.getLogger(__name__)


class CMStorages(Storage):
    """Corpus Manager storage."""

    def __init__(self, storage_id, host_url, resource_type, root_folder, account_id=None):
        super(CMStorages, self).__init__(storage_id)
        self.host_url = host_url
        self.account_id = account_id
        self.resource_type = resource_type
        if root_folder is None:
            self.root_folder = ''
        else:
            self.root_folder = root_folder
        self.root_folder = '/' + self.path_without_starting_slash(self.root_folder)
        if self.root_folder.endswith('/'):
            self.root_folder = self.root_folder[:-1]
        if self.host_url is None:
            raise ValueError('http storage %s can not handle host url' % self._storage_id)

    def _get_file_safe(self, remote_path, local_path):
        corpus = self._get_corpus_info_from_remote_path(remote_path)
        params = {
            'accountId': self.account_id,
            'id': corpus.get("id"),
            'format': "text/monolingual"
        }

        response = requests.get(f'{self.host_url}/corpus/export', params=params)

        if response.status_code != 200:
            raise RuntimeError(
                'cannot not get %s (response code %d)' % (remote_path, response.status_code))
        if "multipart/mixed" not in response.headers.get("Content-Type"):
            with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
                tmpfile.write(response.content)
                shutil.move(tmpfile.name, local_path)
                return
        multipart_data = decoder.MultipartDecoder.from_response(response)
        for part_index, part in enumerate(multipart_data.parts):
            with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
                tmpfile.write(part.content)
                if part_index == 0:
                    shutil.move(tmpfile.name, local_path + "." + corpus.get("sourceLanguage"))
                else:
                    shutil.move(tmpfile.name, local_path + "." + corpus.get("targetLanguages")[0])

    def _check_existing_file(self, remote_path, local_path):
        # not optimized for http download yet
        return False

    def stream_corpus_manager(self, remote_id, remote_format, buffer_size=1024):
        if remote_format == "" or remote_format is None:
            remote_format = "text/bitext"
        if remote_format not in ['application/x-tmx+xml', 'text/bitext']:
            raise RuntimeError(
                'Error format file %s, only support format of the corpus (application/x-tmx+xml, '
                'text/bitext)' % remote_format)
        params = (
            ('accountId', self.account_id),
            ('id', remote_id),
            ('format', remote_format),
        )

        response = requests.get(f'{self.host_url}/corpus/export', params=params)
        if response.status_code != 200:
            raise RuntimeError(
                'cannot get %s (response code %d)' % response.status_code)

        def generate():
            for chunk in response.iter_content(chunk_size=buffer_size, decode_unicode=None):
                yield chunk

        return generate()

    def stream(self, remote_path, buffer_size=1024):
        corpus = self._get_corpus_info_from_remote_path(remote_path)
        return self.stream_corpus_manager(corpus.get("id"), corpus.get("format"), buffer_size)

    def push_corpus_manager(self, local_path, remote_path, corpus_id, user_data):

        if local_path.endswith(".txt"):
            format_path = 'text/bitext'
        elif local_path.endswith(".tmx"):
            format_path = 'application/x-tmx+xml'
        else:
            raise ValueError(
                'cannot push %s, only support format of the corpus (application/x-tmx+xml, '
                'text/bitext)' % local_path)

        remote_path = f"/{self.root_folder}/" + remote_path + os.path.basename(local_path)
        files = {
            'filename': (None, remote_path),
            'accountId': (None, self.account_id),
            'format': (None, format_path),
            'id': (None, corpus_id),
            'corpus': (remote_path, open(local_path, 'rb')),
            'data': (None, user_data)
        }

        response = requests.post(f'{self.host_url}/corpus/import', files=files)
        if response.status_code != 200:
            raise RuntimeError(
                'cannot push %s (response code %d)' % (remote_path, response.status_code))
        status = "id" in response.json()
        return status

    def listdir(self, remote_path, recursive=False, is_file=False):
        if not is_file and not remote_path.endswith('/'):
            remote_path += '/'
        listdir = {}

        data = {
            'directory': self._create_path_from_root(remote_path),
            'accountId': self.account_id
        }
        if recursive:
            data = {
                'prefix': self._create_path_from_root(remote_path),
                'accountId': self.account_id
            }

        response = requests.get(f'{self.host_url}/corpus/list', params=data)

        list_objects = response.json()
        if 'directories' in list_objects:
            for key in list_objects['directories']:
                new_dir = os.path.join(remote_path, key) + '/'
                if new_dir.startswith('/'):
                    new_dir = new_dir[1:]
                if new_dir != '':
                    listdir[new_dir] = {'is_dir': True, 'type': self.resource_type}
        if 'files' in list_objects:
            for key in list_objects['files']:
                if remote_path in key['filename']:
                    date_time = datetime.strptime(key["createdAt"].strip(), "%a %b %d %H:%M:%S %Y")
                    filename = key["filename"][len(self.root_folder) + 1:]
                    listdir[filename] = {'entries': int(key.get('nbSegments')),
                                         'format': key.get('format'),
                                         'id': key.get('id'),
                                         "type": self.resource_type,
                                         'sourceLanguage': key.get('sourceLanguage'),
                                         'targetLanguages': key.get('targetLanguages'),
                                         'last_modified': datetime_to_timestamp(
                                             date_time)}
                    if recursive:
                        folder = os.path.dirname(key['filename'][len(self.root_folder) + 1:])
                        all_dirs = folder.split("/")
                        for folder_index, folder in enumerate(all_dirs):
                            new_dir = "/".join(all_dirs[:folder_index + 1])
                            if new_dir != '':
                                listdir[new_dir + '/'] = {'is_dir': True, 'type': self.resource_type}

        return listdir

    def _delete_single(self, remote_path, isdir):
        corpus = self._get_corpus_info_from_remote_path(remote_path)
        self.delete_corpus_manager(corpus.get("id"))

    def delete_corpus_manager(self, corpus_id):
        params = (
            ('accountId', self.account_id),
            ('id', corpus_id),
        )

        response = requests.get(f'{self.host_url}/corpus/delete', params=params)
        if response.status_code != 200:
            raise RuntimeError(
                'cannot delete %s (response code %d)' % response.status_code)
        status = response.ok
        return status

    def _get_corpus_info_from_remote_path(self, remote_path):
        data = {
            'prefix': self._create_path_from_root(remote_path),
            'accountId': self.account_id
        }
        response = requests.get(f'{self.host_url}/corpus/list', data=data)
        list_objects = response.json()
        if "files" in list_objects:
            for key in list_objects["files"]:
                if self._create_path_from_root(remote_path) == key.get("filename"):
                    return key
        raise ValueError(f"corpus not found from remote_path: {remote_path}")

    def rename(self, old_remote_path, new_remote_path):
        raise NotImplementedError()

    def mkdir(self, remote_path):
        return True

    def search(self, remote_ids, search_query, nb_skip, nb_returns):
        params = {
            'skip': int(nb_skip),
            'limit': int(nb_returns)
        }
        data = {
            'accountId': self.account_id,
            'ids': remote_ids,
            'search': {}
        }

        if search_query['source']['keyword']:
            data['search']['srcQuery'] = search_query['source']['keyword']
        if search_query['target']['keyword']:
            data['search']['tgtQuery'] = search_query['target']['keyword']

        response = requests.post(f'{self.host_url}/corpus/segment/list', json=data, params=params)
        if response.status_code != 200:
            raise ValueError("Cannot list segment '%s' in '%s'." % (search_query, remote_ids))
        list_segment = response.json()
        for segment in list_segment["segments"]:
            for corpus in segment["corpus"]:
                corpus["filename"] = corpus["filename"].replace("/" + self.root_folder, "")
        if "error" in list_segment:
            raise ValueError("Cannot list segment '%s' in '%s'." % (remote_ids,
                                                                    list_segment['error']))
        return list_segment['segments'], list_segment['total']

    def seg_delete(self, corpus_id, list_seg_id):
        deleted_seg = 0
        for seg_id in list_seg_id:
            params = {
                'accountId': self.account_id,
                'id': corpus_id,
                'segId': seg_id,
            }
            response = requests.post(f'{self.host_url}/corpus/segment/delete', data=params)
            if response.status_code == 200:
                deleted_seg += response.json()["segmentDeleted"]
            else:
                raise ValueError(
                    "Cannot delete '%s' in '%s'." % (seg_id, corpus_id))
        return deleted_seg

    def seg_modify(self, corpus_id, seg_id, tgt_id, tgt_seg, src_seg):
        params = {
            'accountId': self.account_id,
            'id': corpus_id,
            'segId': seg_id,
            'tgtId': tgt_id,
            'tgtSeg': tgt_seg,
            'srcSeg': src_seg,
        }

        response = requests.post(f'{self.host_url}/corpus/segment/modify', data=params)
        if response.status_code != 200:
            raise ValueError(
                "Cannot modify segment '%s' in '%s'." % (seg_id, corpus_id))
        status = response.json()["status"] == 'ok'
        return status

    def seg_add(self, corpus_id, segments):
        data = {
            'accountId': self.account_id,
            'id': corpus_id,
            'segments': segments,
        }
        data = json.dumps(data)
        with tempfile.NamedTemporaryFile(delete=False, mode="w") as tmpfile:
            tmpfile.write(data)

        command = f"http {self.host_url}/corpus/segment/add < {tmpfile.name}"
        return_code = os.system(command)
        if return_code != 0:
            raise ValueError(
                "Cannot add segment '%s' in '%s'." % (segments, corpus_id))
        return True

    def isdir(self, remote_path):
        full_path_to_check = self._create_path_from_root(remote_path)
        if full_path_to_check.endswith('/'):
            full_path_to_check = full_path_to_check[:-1]
        if full_path_to_check.startswith('/'):
            full_path_to_check = full_path_to_check[1:]
        directoryArray = full_path_to_check.split("/")
        if len(directoryArray) == 0:
            raise ValueError(f"Bad remote_path: {remote_path}")
        if len(directoryArray) == 1 and directoryArray[0] == '':
            return True
        else:
            parentDirectory = '/' + '/'.join(directoryArray[:-1])
            data = {
                'directory': parentDirectory,
                'accountId': self.account_id
            }
            response = requests.get(f'{self.host_url}/corpus/list', params=data)
            if "directories" in response.json():
                if directoryArray[-1] in response.json()["directories"]:
                    return True
        return False

    def exists(self, remote_path):
        if self.isdir(remote_path):
            return True
        data = {
            'filename': self._create_path_from_root(remote_path),
            'accountId': self.account_id
        }
        response = requests.get(f'{self.host_url}/corpus/exists', params=data)
        return response.status_code == 200 and "true" in str(response.content)

    def push_file(self, local_path, remote_path):
        data = {
            'filename': self._create_path_from_root(remote_path),
            'accountId': self.account_id
        }

        response = requests.get(f'{self.host_url}/corpus/exists', params=data)
        if response.status_code == 200 and "true" in str(response.content):
            raise RuntimeError("Cannot push file: %s already exists"
                               % remote_path)
        with open(local_path, "rb") as f:
            data = f.read()
            if local_path.endswith(".txt"):
                format_path = 'text/bitext'
            elif local_path.endswith(".tmx"):
                format_path = 'application/x-tmx+xml'
            mp_encoder = MultipartEncoder(
                [
                    ('accountId', self.account_id),
                    ('format', format_path),
                    ('importOptions', '{"cleanFormatting": true}'),
                    ('filename', self._create_path_from_root(remote_path)),
                    ('corpus', data)
                ]
            )
            response = requests.post(f'{self.host_url}/corpus/import', data=mp_encoder,
                                     headers={'Content-Type': mp_encoder.content_type})
            if response.status_code != 200:
                raise ValueError(
                    "Cannot import file '%s' in '%s'." % (local_path, remote_path))

    def _create_path_from_root(self, remote_path):
        return_value = ''
        if self.root_folder is not '':
            return_value = '/' + self.path_without_starting_slash(self.root_folder)
        if remote_path is not '':
            return_value += '/' + self.path_without_starting_slash(remote_path)
        if return_value is '':
            return '/'
        return return_value

    def _internal_path(self, remote_path):
        return self.path_without_starting_slash(remote_path)

    def path_without_starting_slash(self, remote_path):
        if remote_path.startswith('/'):
            return remote_path[1:]
        return remote_path

    def stat(self, remote_path):
        pass
