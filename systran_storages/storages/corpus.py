"""Definition of `Corpus Manager` storage class"""
import json
from datetime import datetime
import logging
import os
import uuid

import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder
from requests_toolbelt.multipart import decoder

from systran_storages.storages import Storage
from systran_storages.storages.utils import datetime_to_timestamp

LOGGER = logging.getLogger(__name__)


class CMStorages(Storage):
    """Corpus Manager storage."""

    def __init__(self, storage_id, host_url, root_folder, account_id=None):
        super().__init__(storage_id)
        self.host_url = host_url
        self.account_id = account_id
        self.resource_type = "corpusmanager"
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
        self._get_main_file_safe(remote_path, local_path)
        self._get_checksum_file_safe(remote_path, local_path)

    @staticmethod
    def _get_checksum_file_name(local_path):
        (local_dir, basename) = os.path.split(local_path)
        return os.path.join(local_dir, "." + basename + ".md5")

    def _get_checksum_file(self, local_path):
        if not local_path.endswith(".txt") and not local_path.endswith(".tmx"):
            local_path = local_path[:-3]
        return self._get_checksum_file_name(local_path)

    def _get_main_file_safe(self, remote_path, local_path):
        corpus = self._get_corpus_info_from_remote_path(remote_path)
        params = {
            'accountId': self.account_id,
            'id': corpus.get("id"),
            'format': "text/monolingual"
        }

        response = requests.get(self.host_url + '/corpus/export', params=params)

        if response.status_code != 200:
            raise RuntimeError(
                'cannot not get %s (response code %d)' % (remote_path, response.status_code))
        if "multipart/mixed" not in response.headers.get("Content-Type"):
            with open(local_path, "wb") as file_writer:
                file_writer.write(response.content)
                return
        multipart_data = decoder.MultipartDecoder.from_response(response)
        for part_index, part in enumerate(multipart_data.parts):
            filename = local_path
            src_extension = "." + corpus.get("sourceLanguageCode")
            tgt_extension = "." + corpus.get("targetLanguageCodes")[0]
            if local_path.endswith('.tmx') or local_path.endswith('.txt'):
                if part_index == 1:
                    filename += tgt_extension
                else:
                    filename += src_extension
            if (filename.endswith(src_extension) and part_index == 0) or\
                    (filename.endswith(tgt_extension) and part_index == 1):
                with open(filename, "wb") as file_writer:
                    file_writer.write(part.content)

    def _get_checksum_from_database(self, remote_path):
        corpus = self._get_corpus_info_from_remote_path(remote_path)
        params = {
            'accountId': self.account_id,
            'id': corpus.get("id")
        }
        response = requests.get(self.host_url + '/corpus/details', params=params)

        if response.status_code != 200:
            raise RuntimeError(
                'cannot not get checksum of %s (response code %d)' % (remote_path, response.status_code))

        list_of_one_object = response.json()
        if "files" not in list_of_one_object or len(list_of_one_object["files"]) != 1:
            raise RuntimeError(
                'cannot not get checksum of %s (response badly formatted %s)' % (remote_path, response.content))
        file_checksum = list_of_one_object["files"][0].get("checksum")
        if file_checksum is None or file_checksum == "":
            LOGGER.warning("checksum was not part of the current corpus, generating random one")
            file_checksum = str(uuid.uuid1())
        return file_checksum

    def _get_checksum_file_safe(self, remote_path, local_path):
        file_checksum = self._get_checksum_from_database(remote_path)
        with open(self._get_checksum_file_name(local_path), "w") as file_writer:
            file_writer.write(file_checksum)

    @staticmethod
    def _alias_files_exist(local_path):
        dirname = os.path.dirname(local_path)
        number_of_files = 0
        for filename in os.listdir(dirname):
            complete_filename = os.path.join(dirname, filename)
            if complete_filename.startswith(local_path) and len(complete_filename) == len(local_path) + 3:
                number_of_files += 1
        return number_of_files == 2

    def _check_existing_file(self, remote_path, local_path):
        checksum_path = self._get_checksum_file(local_path)
        if self._alias_files_exist(local_path) and os.path.exists(checksum_path):
            with open(checksum_path) as f:
                checksum_from_file = f.read()
            checksum_from_database = self._get_checksum_from_database(remote_path)
            if checksum_from_database == checksum_from_file:
                return True
            LOGGER.debug('checksum has changed for file %s (%s/%s)', local_path,
                         checksum_from_file, checksum_from_database)
        else:
            LOGGER.debug('Cannot find %s or %s', local_path, checksum_path)
        return False

    def stream_corpus_manager(self, remote_id, remote_format, buffer_size=1024):
        if remote_format == "" or remote_format is None:
            remote_format = "text/bitext"
        if remote_format not in ['application/x-tmx+xml', 'text/bitext', 'systran/edition-corpus']:
            raise RuntimeError(
                'Error format file %s, only support format of the corpus (application/x-tmx+xml, '
                'text/bitext)' % remote_format)
        params = (
            ('accountId', self.account_id),
            ('id', remote_id),
            ('format', remote_format),
        )

        response = requests.get(self.host_url + '/corpus/export', params=params)
        if response.status_code != 200:
            raise RuntimeError(
                'cannot get %s (response code %d)' % response.status_code)

        def generate():
            for chunk in response.iter_content(chunk_size=buffer_size, decode_unicode=None):
                yield chunk

        return generate()

    def stream(self, remote_path, buffer_size=1024, stream_format=None):
        corpus = self._get_corpus_info_from_remote_path(remote_path)
        return self.stream_corpus_manager(corpus.get("id"), stream_format if stream_format else corpus.get("format"),
                                          buffer_size)

    def push_corpus_manager(self, local_path, remote_path, corpus_id, user_data):

        if local_path.endswith(".txt"):
            format_path = 'text/bitext'
        elif local_path.endswith(".tmx"):
            format_path = 'application/x-tmx+xml'
        else:
            raise ValueError(
                'Cannot push %s, only support format of the corpus (application/x-tmx+xml, '
                'text/bitext)' % local_path)

        remote_path = '/' + self.root_folder + '/' + remote_path + os.path.basename(local_path)
        files = {
            'filename': (None, remote_path),
            'accountId': (None, self.account_id),
            'format': (None, format_path),
            'id': (None, corpus_id),
            'corpus': (remote_path, open(local_path, 'rb')),
            'data': (None, user_data)
        }

        response = requests.post(self.host_url + '/corpus/import', files=files)
        if response.status_code != 200:
            raise RuntimeError(
                'cannot push %s (response code %d)' % (remote_path, response.status_code))
        status = "id" in response.json()
        return status

    def listdir(self, remote_path, recursive=False, is_file=False, options=None):
        if not is_file and not remote_path.endswith('/'):
            remote_path += '/'
        listdir = {}

        data = {
            'directory': self._create_path_from_root(remote_path),
            'accountId': self.account_id
        }
        if recursive or is_file:
            data = {
                'prefix': self._create_path_from_root(remote_path),
                'accountId': self.account_id
            }

        if options:
            data.update(options)

        response = requests.get(self.host_url + '/corpus/list', params=data)

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
                    listdir[filename] = {'entries': int(key.get('nbSegments')) if key.get('nbSegments') else None,
                                         'format': key.get('format'),
                                         'id': key.get('id'),
                                         'type': self.resource_type,
                                         'status': key.get('status'),
                                         'errorDesc': key.get('errorDesc', ''),
                                         'tags': key.get('tags'),
                                         'sourceLanguage': key.get('sourceLanguage'),
                                         'targetLanguages': key.get('targetLanguages'),
                                         'last_modified': datetime_to_timestamp(
                                             date_time),
                                         'alias_names': [filename + "." + key.get('sourceLanguageCode', ''),
                                                         filename + "." + (key.get('targetLanguageCodes')[0]
                                                                           if key.get('targetLanguageCodes') else '')]}
                    if recursive:
                        folder = os.path.dirname(key['filename'][len(self.root_folder) + 1:])
                        all_dirs = folder.split("/")
                        for folder_index, folder in enumerate(all_dirs):
                            new_dir = "/".join(all_dirs[:folder_index + 1]) + "/"
                            if new_dir != '' and remote_path in new_dir:
                                listdir[new_dir] = {'is_dir': True, 'type': self.resource_type}

        return listdir

    def _delete_single(self, remote_path, isdir):
        # CM only support delete file, folder does not really exist
        if not isdir:
            corpus = self._get_corpus_info_from_remote_path(remote_path)
            self.delete_corpus_manager(corpus.get("id"))

    def delete_corpus_manager(self, corpus_id):
        params = (
            ('accountId', self.account_id),
            ('id', corpus_id),
        )

        response = requests.get(self.host_url + '/corpus/delete', params=params)
        if response.status_code != 200:
            raise RuntimeError(
                'cannot delete the corpus "%s" (response code %d)' % (corpus_id, response.status_code))
        status = response.ok
        return status

    def _get_corpus_info_from_remote_path(self, remote_path):
        data = {
            'prefix': self._create_path_from_root(remote_path),
            'accountId': self.account_id
        }
        response = requests.get(self.host_url + '/corpus/list', data=data)
        list_objects = response.json()
        if "files" in list_objects:
            for key in list_objects["files"]:
                if self._create_path_from_root(remote_path) == key.get("filename"):
                    return key
        raise ValueError("Corpus not found from remote_path: " + remote_path)

    def rename(self, old_remote_path, new_remote_path):
        raise NotImplementedError()

    def mkdir(self, remote_path):
        return True

    def search(self, remote_ids, search_query=None, nb_skip=0, nb_limit=0):
        mp_encoder_content = [
            ('skip', str(int(nb_skip))),
            ('limit', str(int(nb_limit))),
            ('accountId', self.account_id)
        ]
        for rid in remote_ids:
            mp_encoder_content.append(('id', rid))
        is_async_mode = False
        if search_query:
            if search_query.get('searchMode'):
                mp_encoder_content.append(('searchMode', search_query['searchMode']))
            if search_query.get('xmlEscape'):
                mp_encoder_content.append(('xmlEscape', search_query['xmlEscape']))
            if search_query.get('filename'):
                is_async_mode = True
                mp_encoder_content.append(('filename', search_query['filename']))
                if search_query.get('accountId'):
                    mp_encoder_content.remove(('accountId', self.account_id))
                    mp_encoder_content.append(('readOnlyAccountId', self.account_id))
                    mp_encoder_content.append(('accountId', search_query['accountId']))
            if search_query.get('source_language'):
                mp_encoder_content.append(('srcLang', search_query['source_language']))
            if search_query.get('target_language'):
                mp_encoder_content.append(('tgtLang', search_query['target_language']))

            data = {
                'search': {}
            }
            if search_query.get('source') and search_query['source'].get('keyword'):
                data['search']['srcQuery'] = search_query['source']['keyword']
                if search_query['source'].get('is_regex_search'):
                    data['search'].update({'srcIsRegex': True})
                    data['search']['srcIsCaseInsensitive'] = search_query['source'].get('isCaseInsensitive')
            if search_query.get('target') and search_query['target'].get('keyword'):
                data['search']['tgtQuery'] = search_query['target']['keyword']
                if search_query['target'].get('is_regex_search'):
                    data['search'].update({'tgtIsRegex': True})
                    data['search']['tgtIsCaseInsensitive'] = search_query['target'].get('isCaseInsensitive')

            if data.get('search'):
                mp_encoder_content.append(('query', json.dumps(data)))

        mp_encoder = MultipartEncoder(mp_encoder_content)
        response = requests.request('POST', self.host_url + '/corpus/segment/list', data=mp_encoder,
                                    headers={'Content-Type': mp_encoder.content_type})
        if response.status_code != 200:
            raise ValueError("Cannot list segment '%s' in '%s'." % (search_query, remote_ids))

        json_response = response.json()
        if "error" in json_response:
            raise ValueError("Cannot list segment '%s' in '%s'." % (remote_ids, json_response['error']))
        if is_async_mode:
            return json_response
        return json_response.get("segments"), json_response.get('total')

    def seg_delete(self, corpus_id, seg_ids):
        deleted_seg = 0
        for seg_id in seg_ids:
            params = {
                'accountId': self.account_id,
                'id': corpus_id,
                'segId': seg_id,
            }
            response = requests.post(self.host_url + '/corpus/segment/delete', data=params)
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

        response = requests.post(self.host_url + '/corpus/segment/modify', data=params)
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

        response = requests.post(self.host_url + '/corpus/segment/add', json=data)
        if response.status_code != 200:
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
            raise ValueError("Bad remote_path: " + remote_path)
        if len(directoryArray) == 1 and directoryArray[0] == '':
            return True

        parentDirectory = '/' + '/'.join(directoryArray[:-1])
        data = {
            'directory': parentDirectory,
            'accountId': self.account_id
        }
        response = requests.get(self.host_url + '/corpus/list', params=data)
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
        response = requests.get(self.host_url + '/corpus/exists', params=data)
        return response.status_code == 200 and "true" in str(response.content)

    def push_file(self, local_path, remote_path, lp=None):
        if lp is None:
            lp = {}
        data = {
            'filename': self._create_path_from_root(remote_path),
            'accountId': self.account_id
        }

        response = requests.get(self.host_url + '/corpus/exists', params=data)
        if response.status_code == 200 and "true" in str(response.content):
            raise RuntimeError("Cannot push file: %s already exists"
                               % remote_path)
        with open(local_path, "rb") as f:
            data = f.read()
            if local_path.endswith(".txt"):
                format_path = 'text/bitext'
            elif local_path.endswith(".tmx"):
                format_path = 'application/x-tmx+xml'
            else:
                raise ValueError(
                    'Cannot push %s, only support format of the corpus (application/x-tmx+xml, '
                    'text/bitext)' % local_path)
            source = lp.get('source', '')
            targets = lp.get('targets', [])
            import_options = {
                "cleanFormatting": True,
                "removeDuplicates": True,
                "expectedSourceLanguage": source,
                "expectedTargetLanguages": targets
            }

            mp_encoder = MultipartEncoder(
                [
                    ('accountId', self.account_id),
                    ('format', format_path),
                    ('importOptions', json.dumps(import_options)),
                    ('filename', self._create_path_from_root(remote_path)),
                    ('corpus', data)
                ]
            )
            response = requests.post(self.host_url + '/corpus/import', data=mp_encoder,
                                     headers={'Content-Type': mp_encoder.content_type})
            if response.status_code != 200:
                error_message = json.loads(response.content).get('error')
                if error_message:
                    raise ValueError("Cannot import file(s) : %s" % error_message)
                raise ValueError(
                    "Cannot import file '%s' in '%s'." % (local_path, remote_path))
            return response.json()

    def partition_auto(self, local_path, training_path, testing_path, partition_value, is_percent, lp):
        remote_path = training_path + os.path.basename(local_path)
        training_file = training_path + os.path.basename(local_path)
        testing_file = testing_path + os.path.basename(local_path)

        data = {
            'filename': self._create_path_from_root(remote_path),
            'accountId': self.account_id
        }

        if is_percent:
            data_partition = [
                                {'segments': str(100-partition_value), 'filename': str(training_file)},
                                {'segments': str(partition_value), 'filename': str(testing_file)}
            ]
        else:
            data_partition = {
                'usePercentage': False,
                'partition': [
                    {'segments': 'remains', 'filename': str(training_file)},
                    {'segments': str(partition_value), 'filename': str(testing_file)}
                ],
            }

        data_partition_str = json.dumps(data_partition)

        response = requests.get(self.host_url + '/corpus/exists', params=data)
        if response.status_code == 200 and "true" in str(response.content):
            raise RuntimeError("Cannot push file: %s already exists"
                               % remote_path)
        with open(local_path, "rb") as f:
            data = f.read()
            if local_path.endswith(".txt"):
                format_path = 'text/bitext'
            elif local_path.endswith(".tmx"):
                format_path = 'application/x-tmx+xml'
            else:
                raise ValueError(
                    'Cannot push %s, only support format of the corpus (application/x-tmx+xml, '
                    'text/bitext)' % local_path)
            source = lp.get('source', '')
            targets = lp.get('targets', [])
            import_options = {
                "cleanFormatting": True,
                "removeDuplicates": True,
                "expectedSourceLanguage": source,
                "expectedTargetLanguages": targets
            }

            mp_encoder = MultipartEncoder(
                [
                    ('accountId', self.account_id),
                    ('format', format_path),
                    ('importOptions', json.dumps(import_options)),
                    ('filename', self._create_path_from_root(remote_path)),
                    ('partition', data_partition_str),
                    ('corpus', data)
                ]
            )
            response = requests.post(self.host_url + '/corpus/import/partition', data=mp_encoder,
                                     headers={'Content-Type': mp_encoder.content_type})
            if response.status_code != 200:
                error_message = json.loads(response.content).get('error')
                if error_message:
                    raise ValueError("Cannot import file(s) : %s" % error_message)
                raise ValueError("Cannot import file '%s' in '%s'." % (local_path, remote_path))
            return response.json()

    def _create_path_from_root(self, remote_path):
        return_value = ''
        if self.root_folder != '':
            return_value = '/' + self.path_without_starting_slash(self.root_folder)
        if remote_path != '':
            return_value += '/' + self.path_without_starting_slash(remote_path)
        if return_value == '':
            return '/'
        if return_value.endswith('.tmx') or return_value.endswith('.txt') or return_value.endswith('/'):
            return return_value
        p = return_value.rfind('.tmx.')
        corpus_format = '.tmx'
        if return_value.rfind('.txt.') > p:
            p = return_value.rfind('.txt.')
            corpus_format = '.txt'
        if p > -1:
            return return_value[0:p+len(corpus_format)]
        return return_value

    def _internal_path(self, path):
        return self.path_without_starting_slash(path)

    @staticmethod
    def path_without_starting_slash(remote_path):
        if remote_path.startswith('/'):
            return remote_path[1:]
        return remote_path

    def stat(self, remote_path):
        pass

    def similar(self, corpus_ids, search_options, input_corpus, output_corpus_name):
        params = {
            **search_options,
            'filename': output_corpus_name
        }
        response = requests.post(self.host_url + '/corpus/similar', params=params, data={'id': corpus_ids},
                                 files=[('corpus', input_corpus)])
        if response.status_code != 200:
            raise RuntimeError(
                'Cannot start similar search. %s' % response.text)
        return response.json()['id']

    def tag_add(self, corpus_id, tag):
        data = {
            'accountId': self.account_id,
            'id': corpus_id
        }
        response = requests.post(self.host_url + '/corpus/tags/' + tag, data=data)
        if response.status_code != 200:
            raise ValueError(
                "Cannot add tag '%s' in '%s'." % (tag, corpus_id))
        return response.json()

    def tag_remove(self, corpus_id, tag):
        data = {
            'accountId': self.account_id,
            'id': corpus_id
        }
        response = requests.delete(self.host_url + '/corpus/tags/' + tag, data=data)
        if response.status_code != 200:
            raise ValueError(
                "Cannot remove tag '%s' in '%s'." % (tag, corpus_id))
        return response.json()

    def detail(self, corpus_id):
        params = {
            'accountId': self.account_id,
            'id': corpus_id
        }
        response = requests.get(self.host_url + '/corpus/details', params=params)
        return response.json().get('files')
