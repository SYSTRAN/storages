"""Definition of `s3` storage class"""

import os
import boto3
import tempfile
import shutil
import logging
import botocore

from systran_storages.storages.utils import datetime_to_timestamp
from systran_storages.storages import Storage

LOGGER = logging.getLogger(__name__)

class S3Storage(Storage):
    """Storage on Amazon S3."""

    def __init__(self, storage_id, bucket_name, access_key_id=None, secret_access_key=None,
                 region_name=None, assume_role=None, transfer_config=None):
        super(S3Storage, self).__init__(storage_id)
        if assume_role is not None:
            if not assume_role.get('role_arn') or not assume_role.get('role_session_name'):
                raise ValueError('invalid "assume_role" configuration: "role_arn" and "role_session_name" are required')
            session_duration = 3600
            if assume_role.get('session_duration') is not None:
                session_duration = assume_role.get('session_duration')
            sts_client = boto3.client('sts',
                                      aws_access_key_id=access_key_id,
                                      aws_secret_access_key=secret_access_key,
                                      region_name=region_name)
            response_assume_role = sts_client.assume_role(RoleArn=assume_role.get('role_arn'),
                                                          RoleSessionName=assume_role.get('role_session_name'),
                                                          DurationSeconds=session_duration)
            session = boto3.Session(aws_access_key_id=response_assume_role['Credentials']['AccessKeyId'],
                                    aws_secret_access_key=response_assume_role['Credentials']['SecretAccessKey'],
                                    aws_session_token=response_assume_role['Credentials']['SessionToken'])
        else:
            session = boto3.Session(
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key)
        self._s3 = session.resource('s3')
        self._client = session.client('s3')
        self._bucket_name = bucket_name
        self._bucket = self._s3.Bucket(bucket_name)
        if transfer_config is not None:
            self._transfer_config = boto3.s3.transfer.TransferConfig(**transfer_config)
        else:
            self._transfer_config = None

    def _get_file_safe(self, remote_path, local_path):
        (local_dir, basename) = os.path.split(local_path)
        md5_path = os.path.join(local_dir, ".5dm#"+basename+"#md5")
        with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
            self._bucket.download_file(remote_path, tmpfile.name, Config=self._transfer_config)
            shutil.move(tmpfile.name, local_path)
            obj = self._bucket.Object(remote_path)
            with open(md5_path, "w") as fw:
                fw.write(obj.e_tag)

    def _check_existing_file(self, remote_path, local_path):
        (local_dir, basename) = os.path.split(local_path)
        md5_path = os.path.join(local_dir, ".5dm#"+basename+"#md5")
        if os.path.exists(local_path) and os.path.exists(md5_path):
            with open(md5_path) as f:
                md5 = f.read()
            obj = self._bucket.Object(remote_path)
            if obj.e_tag == md5:
                return True
            LOGGER.debug('checksum has changed for file %s (%s/%s)', local_path, md5, obj.e_tag)
        else:
            LOGGER.debug('Cannot find %s or %s', local_path, md5_path)
        return False

    def _get_checksum_file(self, local_path):
        """return checksum sum used by storage or None
        """
        (local_dir, basename) = os.path.split(local_path)
        return os.path.join(local_dir, ".5dm#"+basename+"#md5")

    def push_file(self, local_path, remote_path):
        (local_dir, basename) = os.path.split(local_path)
        md5_path = os.path.join(local_dir, ".5dm#"+basename+"#md5")
        if not remote_path:
            remote_path = basename
        self._bucket.upload_file(local_path, remote_path, Config=self._transfer_config)
        obj = self._bucket.Object(remote_path)
        with open(md5_path, "w") as fw:
            fw.write(obj.e_tag)

    def stat(self, remote_path):
        obj = self._bucket.Object(remote_path)
        try:
            return {'size': obj.content_length, 'last_modified': datetime_to_timestamp(obj.last_modified)}
        except botocore.exceptions.ClientError:
            return False

    def stream(self, remote_path, buffer_size=1024):
        body = self._s3.Object(self._bucket_name, remote_path).get()['Body']

        def generate():
            for chunk in iter(lambda: body.read(buffer_size), b''):
                yield chunk

        return generate()

    def listdir(self, remote_path, recursive=False, is_file=False):
        if not is_file and remote_path != '' and not remote_path.endswith('/'):
            remote_path += '/'
        listdir = {}
        delimiter = '/'
        if recursive:
            delimiter = ''
        list_objects = self._s3.meta.client.list_objects_v2(Bucket=self._bucket_name,
                                                            Delimiter=delimiter,
                                                            Prefix=remote_path)
        if 'CommonPrefixes' in list_objects:
            for key in list_objects['CommonPrefixes']:
                listdir[key['Prefix']] = {'is_dir': True}
        if 'Contents' in list_objects:
            for key in list_objects['Contents']:
                listdir[key['Key']] = {'size': key['Size'],
                                       'last_modified': datetime_to_timestamp(key['LastModified'])}
                if key['Key'].endswith('/'):
                    listdir[key['Key']]['is_dir'] = True
        return listdir

    def mkdir(self, remote_path):
        remote_path = remote_path.strip()
        result = self.exists(remote_path)
        if result:
            return

        if remote_path.startswith("/"):  # S3 create a empty directory if /
            remote_path = remote_path[1:]

        if not remote_path.endswith("/"):  # to simulate a directory in S3
            remote_path += "/"
        self._client.put_object(
            Bucket=self._bucket_name,
            Body='',
            Key=remote_path
        )

        if not self.exists(remote_path):
            raise ValueError("cannot create the directory %s" % remote_path)

    def _delete_single(self, remote_path, isdir):
        if not isdir:
            self._s3.meta.client.delete_object(Bucket=self._bucket_name, Key=remote_path)

    def rename(self, old_remote_path, new_remote_path):
        is_dir = self.isdir(old_remote_path)
        for obj in self._bucket.objects.filter(Prefix=old_remote_path):
            src_key = obj.key
            if not src_key.endswith('/'):
                copy_source = self._bucket_name + '/' + src_key
                if src_key == old_remote_path:
                    # it is a file that we are copying
                    dest_file_key = new_remote_path
                else:
                    filename = src_key.split('/')[-1]
                    dest_file_key = new_remote_path + '/' + filename
                self._s3.Object(self._bucket_name, dest_file_key).copy_from(CopySource=copy_source)
            self._s3.Object(self._bucket_name, src_key).delete()

        # Warning: create the new virtual directory. if not, an empty directory will be deleted instead of being renamed
        # important to do it at last because filter by prefix could delete the new directory
        if is_dir:
            self.mkdir(new_remote_path)

        result = self.exists(new_remote_path)
        return result

    def exists(self, remote_path):
        result = self._bucket.objects.filter(Prefix=remote_path)
        for obj in result:
            if (obj.key == remote_path or
                    remote_path == '' or
                    remote_path.endswith('/') or
                    obj.key.startswith(remote_path + '/')):
                return True
        return False

    def isdir(self, remote_path):
        if not remote_path.endswith('/'):
            return self.exists(remote_path+'/')
        return self.exists(remote_path)

    def _internal_path(self, path):
        # S3 does not work with paths but keys. This function possibly adapts a
        # path-like representation to a S3 key.
        if path.startswith('/'):
            return path[1:]
        return path
