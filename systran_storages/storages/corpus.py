"""Definition of `Corpus Manager` storage class"""
import json
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
    """Corpus Manager storage."""

    def __init__(self, storage_id, hostURL, accountID=None):
        super(CMStorages, self).__init__(storage_id)
        self.hostURL = hostURL
        self.accountID = accountID

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

    def listdir(self, remote_path, recursive=False, is_file=False):
        if self.hostURL is None:
            raise ValueError('http storage %s can not handle hostURL' % self._storage_id)

        if not remote_path.endswith('/'):
            remote_path = remote_path + "/"

        if not remote_path.startswith('/'):
            remote_path = '/' + remote_path

        listdir = {}

        list_objects = requests.get(self.hostURL + "/corpus/list?accountId=" + self.accountID)
        list_objects = list_objects.json()
        if 'directories' in list_objects:
            for key in list_objects['directories']:
                listdir[key['Prefix']] = {'is_dir': True}
        if 'files' in list_objects:
            for key in list_objects['files']:
                if remote_path in key['filename']:
                    date_time = datetime.strptime(key["createdAt"].strip(), "%a %b %d %H:%M:%S %Y")
                    basename = os.path.basename(key["filename"])
                    folder = os.path.dirname(key['filename'])
                    if not folder.startswith('/'):
                        folder = "/" + folder
                    if remote_path + basename == key['filename']:
                        listdir[key['filename']] = {'entries': int(key.get('nbSegments')),
                                                    'format': key.get('format'),
                                                    'id': key.get('id'),
                                                    'sourceLanguage': key.get('sourceLanguage'),
                                                    'targetLanguages': key.get('targetLanguages'),
                                                    'last_modified': datetime_to_timestamp(
                                                        date_time)}
                    if remote_path in folder or remote_path == '/':
                        sub_dir = folder.split(remote_path)[1]
                        internal_sub_dir = self._internal_path(sub_dir)
                        if len(internal_sub_dir) > 0:
                            listdir[os.path.join(remote_path, internal_sub_dir)] = {"is_dir": True}
        return listdir

    def _delete_single_corpus_manager(self, remote_path, corpus_id, isdir):
        if not isdir:
            params = (
                ('accountId', self.accountID),
                ('id', corpus_id),
            )

            response = requests.get(f'{self.hostURL}/corpus/delete', params=params)
            if response.status_code != 200:
                raise RuntimeError(
                    'cannot delete %s (response code %d)' % response.status_code)

            return True

    def rename(self, old_remote_path, new_remote_path):
        raise NotImplementedError()

    def mkdir(self, remote_path):
        pass

    def segment_list(self, remote_id):
        if self.hostURL is None:
            raise ValueError('http storage %s can not handle hostURL' % self._storage_id)
        params = {
            'accountId': self.accountID,
            'id': remote_id
        }
        response = requests.post(f'{self.hostURL}/corpus/segment/list', data=params)
        list_segment = response.json()
        return list_segment

    def search(self, remote_ids, search_query, nb_skip, nb_returns):
        skip = int(nb_skip)
        limit = int(nb_returns)

        list_segment = self.segment_list(remote_ids[0])['segments']
        matched_source = [x for x in list_segment if
                          re.search(search_query["source"]['keyword'], x['seg'])]
        matched_target = [x for x in matched_source if
                          re.search(search_query["target"]['keyword'], x['tgt']['seg'])]
        result = matched_target[skip:skip + limit]
        for row in result:
            row.update({"corpusId": remote_ids[0]})
        return result, len(matched_target)

    def seg_delete(self, corpus_id, list_seg_id):
        deleted_seg = 0
        for seg_id in list_seg_id:
            params = {
                'accountId': self.accountID,
                'id': corpus_id,
                'segId': seg_id,
            }
            response = requests.post(f'{self.hostURL}/corpus/segment/delete', data=params)
            if response.status_code == 200:
                deleted_seg += response.json()["segmentDeleted"]
            else:
                raise ValueError(
                    "Cannot delete '%s' in '%s'." % (seg_id, corpus_id))
        return deleted_seg

    def seg_modify(self, corpus_id, seg_id, tgt_id, tgt_seg, src_seg):
        params = {
            'accountId': self.accountID,
            'id': corpus_id,
            'segId': seg_id,
            'tgtId': tgt_id,
            'tgtSeg': tgt_seg,
            'srcSeg': src_seg,
        }

        response = requests.post(f'{self.hostURL}/corpus/segment/modify', data=params)
        if response.status_code == 200:
            status = True if response.json()["status"] == 'ok' else False
        else:
            raise ValueError(
                "Cannot modify segment '%s' in '%s'." % (seg_id, corpus_id))
        return status

    def isdir(self, remote_path):
        if remote_path.endswith('/'):
            return True
        return False

    def exists(self, remote_path):
        if not remote_path.startswith('/'):
            remote_path = '/' + remote_path
        params = {
            'accountId': self.accountID,
            'filename': remote_path,
        }

        if '.' not in remote_path:
            return True
        if '.' in remote_path:
            response = requests.post(f'{self.hostURL}/corpus/exists', data=params)
            if "true" in response.text:
                return True
        return False

    def _internal_path(self, remote_path):
        if remote_path.startswith('/'):
            return remote_path[1:]
        return remote_path

    def stat(self, remote_path):
        pass
