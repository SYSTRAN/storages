"""Client to abstract storage location: local, S3, SSH, etc."""
import os
import logging

from systran_storages import storages

LOGGER = logging.getLogger(__name__)


class StorageClient:
    """Client to get and push files to a storage."""

    def __init__(self, config=None):
        """Initializes the client.

        Args:
          config: A dictionary mapping storage identifiers to a storage type
            and its configuration.
        """
        self._config = config
        self._storages = {}

    def is_managed_path(self, path):
        """Returns True if the path references a storage managed by this client."""
        if self._config is None:
            return False
        fields = path.split(':', 1)
        return len(fields) == 2 and fields[0] in self._config

    @staticmethod
    def parse_managed_path(path):
        """Returns the storage ID and the full path from a managed path."""
        fields = path.split(':', 1)
        return fields[0], fields[1]

    def _get_storage(self, path, storage_id=None):
        """Returns the storage implementation based on storage_id or infer it
        from the path. Defaults to the local filesystem.
        """
        if storage_id is None:
            fields = path.split(':', 1)
            if len(fields) == 2:
                storage_id = fields[0]
                path = fields[1]

        if storage_id is not None:
            if storage_id not in self._storages:
                if self._config is None or storage_id not in self._config:
                    raise ValueError('unknown storage identifier %s' % storage_id)
                config = self._config[storage_id]
                if config['type'] == 's3':
                    credentials = config.get('aws_credentials', {})
                    client = storages.S3Storage(
                        storage_id,
                        config['bucket'],
                        access_key_id=credentials.get('access_key_id'),
                        secret_access_key=credentials.get('secret_access_key'),
                        region_name=credentials.get('region_name'),
                        assume_role=credentials.get('assume_role'),
                        transfer_config=credentials.get('transfer_config'))
                elif config['type'] == 'swift':
                    client = storages.SwiftStorage(
                        storage_id,
                        config['container'],
                        auth_config=config.get('auth_config'),
                        transfer_config=config.get('transfer_config'))
                elif config['type'] == 'ssh':
                    client = storages.RemoteStorage(
                        storage_id,
                        config['server'],
                        config['user'],
                        config.get('password'),
                        config.get('pkey'),
                        port=config.get('port', 22),
                        basedir=config.get('basedir'))
                elif config['type'] == 'http':
                    client = storages.HTTPStorage(  # pylint: disable=abstract-class-instantiated
                        storage_id,
                        config['get_pattern'],
                        pattern_push=config.get('post_pattern'),
                        pattern_list=config.get('list_pattern'))
                elif config['type'] == 'systran_corpusmanager':
                    client = storages.CMStorages(
                        storage_id,
                        config.get('host_url'),
                        account_id=config.get('account_id'),
                        root_folder=config.get('root_folder'))
                elif config['type'] == 'local':
                    client = storages.LocalStorage(
                        storage_id,
                        basedir=config.get("basedir"))
                else:
                    raise ValueError(
                        'unsupported storage type %s for %s' % (config['type'], storage_id))
                self._storages[storage_id] = client
            else:
                client = self._storages[storage_id]
        else:
            client = storages.LocalStorage()

        return client, client._internal_path(path)

    def join(self, path, *paths):
        """Joins the paths according to the storage implementation."""
        if not self.is_managed_path(path):
            return os.path.join(path, *paths)
        client, _ = self._get_storage(path)
        prefix, rel_path = self.parse_managed_path(path)
        return '%s:%s' % (prefix, client.join(rel_path, *paths))  # Only join the actual path.

    def split(self, path):
        """Splits the path according to the storage implementation."""
        if not self.is_managed_path(path):
            return os.path.split(path)
        client, _ = self._get_storage(path)
        prefix, rel_path = self.parse_managed_path(path)
        return ("%s:" % prefix,) + client.split(rel_path)

    # Simple wrappers around get().
    def get_file(self, remote_path, local_path, storage_id=None):
        """Retrieves a file from remote_path to local_path."""
        return self.get(remote_path, local_path, directory=False, storage_id=storage_id)

    def get_directory(self, remote_path, local_path, storage_id=None):
        """Retrieves a full directory from remote_path to local_path."""
        return self.get(remote_path, local_path, directory=True, storage_id=storage_id)

    def get(self,
            remote_path,
            local_path,
            directory=False,
            storage_id=None,
            check_integrity_fn=None):
        """Retrieves file or directory from remote_path to local_path."""
        LOGGER.info('Synchronizing %s to %s', remote_path, local_path)
        client, remote_path = self._get_storage(remote_path, storage_id=storage_id)
        client.get(
            remote_path,
            local_path,
            directory=directory,
            check_integrity_fn=check_integrity_fn)
        if not os.path.exists(local_path):
            raise RuntimeError('Failed to synchronize %s' % local_path)

    def stat(self, remote_path, storage_id=None):
        """Returns stat on remote_path file
        """
        client, remote_path = self._get_storage(remote_path, storage_id=storage_id)
        return client.stat(remote_path)

    def stream(self, remote_path, buffer_size=1024, storage_id=None, stream_format=None):
        """Returns a generator to stream a remote_path file.
        `buffer_size` is the maximal size of each chunk
        """
        client, remote_path = self._get_storage(remote_path, storage_id=storage_id)
        return client.stream(remote_path, buffer_size, stream_format)

    def stream_corpus_manager(self, remote_path, remote_id, remote_format,
                              buffer_size=1024, storage_id=None):
        """Returns a generator to stream a remote_path file for Corpus Manager storage.
        `buffer_size` is the maximal size of each chunk
        """
        client, remote_path = self._get_storage(remote_path, storage_id=storage_id)
        return client.stream_corpus_manager(remote_id, remote_format, buffer_size)

    def push(self, local_path, remote_path, storage_id=None, lp=None):
        """Pushes a local_path file or directory to storage."""
        if not os.path.exists(local_path):
            raise RuntimeError('%s not found' % local_path)
        if local_path == remote_path:
            return None
        LOGGER.info('Uploading %s to %s', local_path, remote_path)
        client, remote_path = self._get_storage(remote_path, storage_id=storage_id)
        return client.push(local_path, remote_path, lp=lp)

    def push_corpus_manager(self, local_path, remote_path, corpus_id, user_data, storage_id=None):
        """Pushes a local_path file or directory to storage."""
        if not os.path.exists(local_path):
            raise RuntimeError('%s not found' % local_path)
        if local_path == remote_path:
            return None
        LOGGER.info('Uploading %s to %s', local_path, remote_path)
        client, remote_path = self._get_storage(remote_path, storage_id=storage_id)
        client.push_corpus_manager(local_path, remote_path, corpus_id, user_data)
        return None

    def partition_auto(self, data, training_path, testing_path, remote_path, storage_id, partition_value, is_percent,
                       lp):
        LOGGER.info('Partitioning %s in %s to %s', str(data), training_path, testing_path)
        client, remote_path = self._get_storage(remote_path, storage_id=storage_id)
        return client.partition_auto(data, training_path, testing_path, partition_value, is_percent, lp)

    def mkdir(self, local_path, remote_path, storage_id=None):
        """Pushes a local_path file or directory to storage."""
        LOGGER.info('mkdir %s to %s', local_path, remote_path)
        client, remote_path = self._get_storage(remote_path, storage_id=storage_id)
        # Remove antislash from first and last character
        if local_path.startswith("/"):
            local_path = local_path[1:]
        if local_path.endswith("/"):
            local_path = local_path[:-1]
        if remote_path.endswith("/"):
            remote_path = remote_path[:-1]

        full_path = remote_path + "/" + local_path + "/"
        if self.exists(full_path, storage_id):
            raise ValueError(
                "the folder '%s' already exists in the storage '%s'." % (full_path, storage_id))

        client.mkdir(full_path)

    def listdir(self, remote_path, recursive=False, storage_id=None, options=None):
        """Lists of the files on a storage:
        * if `recursive` returns all of the files present recursively in the directory
        * if not `recursive` returns only first level, directory are indicated with trailing '/'
        """
        client, remote_path = self._get_storage(remote_path, storage_id=storage_id)
        return client.listdir(remote_path, recursive, False, options)

    def list(self, remote_path, recursive=False, storage_id=None):
        """Lists of the files on a storage:
        * if `recursive` returns all of the files present recursively in the directory
        * if not `recursive` returns only first level, directory are indicated with trailing '/'
        """
        client, remote_path = self._get_storage(remote_path, storage_id=storage_id)

        if client.isdir(remote_path):
            return client.listdir(remote_path, recursive)
        return client.listdir(remote_path, recursive, True)

    def delete(self, remote_path, recursive=False, storage_id=None):
        """Deletes a file or directory from a storage."""
        client, remote_path = self._get_storage(remote_path, storage_id=storage_id)
        return client.delete(remote_path, recursive)

    def delete_corpus_manager(self, remote_path, corpus_id, storage_id=None):
        """Deletes a file or directory from a storage."""
        client, remote_path = self._get_storage(remote_path, storage_id=storage_id)
        return client.delete_corpus_manager(corpus_id)

    def search(self, remote_path, remote_ids, search_query, nb_skip, nb_returns, storage_id=None):
        """List corpus segments from a storage."""
        client, remote_path = self._get_storage(remote_path, storage_id=storage_id)
        return client.search(remote_ids, search_query, nb_skip, nb_returns)

    def seg_delete(self, remote_path, corpus_id, seg_ids, storage_id=None):
        """Delete segments from a corpus in a storage."""
        client, remote_path = self._get_storage(remote_path, storage_id=storage_id)
        return client.seg_delete(corpus_id, seg_ids)

    def seg_modify(self, remote_path, corpus_id, seg_id, tgt_id, tgt_seg, src_seg, storage_id=None):
        """Modify segment from a corpus in a storage."""
        client, remote_path = self._get_storage(remote_path, storage_id=storage_id)
        return client.seg_modify(corpus_id, seg_id, tgt_id, tgt_seg, src_seg)

    def seg_add(self, remote_path, corpus_id, segments, storage_id=None):
        """Add segments from a corpus in a storage."""

        client, remote_path = self._get_storage(remote_path, storage_id=storage_id)
        return client.seg_add(corpus_id, segments)

    def rename(self, old_remote_path, new_remote_path, storage_id=None):
        """Renames a file or directory on storage from old_remote_path to new_remote_path."""
        client_old, old_remote_path = self._get_storage(old_remote_path, storage_id=storage_id)
        client_new, new_remote_path = self._get_storage(new_remote_path, storage_id=storage_id)

        if client_old._storage_id != client_new._storage_id:
            raise ValueError('rename on different storages')

        result = client_old.rename(old_remote_path, new_remote_path)
        if result is None:  # some storages return nothing when ok and raise exception when error
            return True

        return result

    def exists(self, remote_path, storage_id=None):
        """Checks if file or directory exists on storage."""
        client, remote_path = self._get_storage(remote_path, storage_id=storage_id)
        return client.exists(remote_path)

    def similar(self, remote_path, corpus_ids, search_options, input_corpus, output_corpus_name, storage_id=None):
        """Extract a similar corpus from a large corpus dataset using an input corpus."""
        client, remote_path = self._get_storage(remote_path, storage_id=storage_id)
        return client.similar(corpus_ids, search_options, input_corpus, output_corpus_name)

    def tag_add(self, remote_path, corpus_id, tag, storage_id=None):
        """Add a tag associated with a corpus."""
        client, remote_path = self._get_storage(remote_path, storage_id=storage_id)
        return client.tag_add(corpus_id, tag)

    def tag_remove(self, remote_path, corpus_id, tag, storage_id=None):
        """Remove a tag associated with a corpus."""
        client, remote_path = self._get_storage(remote_path, storage_id=storage_id)
        return client.tag_remove(corpus_id, tag)

    def detail(self, remote_path, corpus_id, storage_id=None):
        """Return the details of the corpus."""
        client, remote_path = self._get_storage(remote_path, storage_id=storage_id)
        return client.detail(corpus_id)
