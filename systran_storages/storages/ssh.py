"""Definition of `remote` storage class"""

import os
import io
from stat import S_ISDIR
from socket import timeout as SocketTimeout
import tempfile
import shutil
import logging

import paramiko
import scp

from systran_storages.storages import Storage

LOGGER = logging.getLogger(__name__)


class RemoteStorage(Storage):
    """Storage on a remote SSH server.
       Connect with user/password or user/privatekey
    """

    def __init__(self, storage_id, server, user, password, pkey=None, port=22, basedir=None):
        super().__init__(storage_id)
        self._allow_multithreading = False
        self._server = server
        self._user = user
        self._password = password
        if pkey is not None:
            private_key_file = io.StringIO()
            private_key_file.write('-----BEGIN RSA PRIVATE KEY-----\n%s\n'
                                   '-----END RSA PRIVATE KEY-----\n' % pkey)
            private_key_file.seek(0)
            try:
                pkey = paramiko.RSAKey.from_private_key(private_key_file)
            except Exception as err:
                raise RuntimeError("cannot parse private key (%s)" % str(err)) from err
        self._pkey = pkey
        self._port = port
        self._ssh_client = None
        self._scp_client = None
        self._sftp_client = None
        self._basedir = basedir

    def __del__(self):
        if self._scp_client:
            self._scp_client.close()
        if self._sftp_client:
            self._sftp_client.close()
        if self._ssh_client:
            self._ssh_client.close()

    def _connect(self):
        if self._ssh_client is None:
            self._ssh_client = paramiko.SSHClient()
            self._ssh_client.load_system_host_keys()
            self._ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self._ssh_client.connect(self._server,
                                     port=self._port,
                                     username=self._user,
                                     password=self._password,
                                     pkey=self._pkey,
                                     look_for_keys=False)
        return self._ssh_client

    def _connectSCPClient(self):
        if self._scp_client is None:
            ssh_client = self._connect()
            self._scp_client = scp.SCPClient(ssh_client.get_transport())
        return self._scp_client

    def _closeSCPClient(self):
        # in case of exception, SCP client does not seem to be reusable
        self._scp_client.close()
        self._scp_client = None

    def _connectSFTPClient(self):
        if self._sftp_client is None:
            ssh_client = self._connect()
            self._sftp_client = ssh_client.open_sftp()
        return self._sftp_client

    def _get_file_safe(self, remote_path, local_path):
        client = self._connectSCPClient()
        with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
            try:
                client.get(remote_path, tmpfile.name, preserve_times=True)
            except Exception as err:
                self._closeSCPClient()
                raise RuntimeError("cannot get file (%s)" % str(err)) from err
            shutil.move(tmpfile.name, local_path)

    def _check_existing_file(self, remote_path, local_path):
        if os.path.exists(local_path):
            local_stat = os.stat(local_path)
            remote_stat = self._connectSFTPClient().stat(remote_path)
            if int(local_stat.st_mtime) == remote_stat.st_mtime and \
                    local_stat.st_size == remote_stat.st_size:
                return True
        return False

    def stat(self, remote_path):
        try:
            remote_stat = self._connectSFTPClient().stat(remote_path)
            if S_ISDIR(remote_stat.st_mode):
                return {'is_dir': True}
            return {'size': remote_stat.st_size, 'last_modified': remote_stat.st_mtime}
        except IOError:
            return False

    def stream(self, remote_path, buffer_size=1024, stream_format=None):
        client = self._connectSCPClient()
        channel = client._open()
        channel.settimeout(client.socket_timeout)
        channel.exec_command(b"scp -f " + client.sanitize(scp.asbytes(remote_path)))  # nosec
        while not channel.closed:
            # wait for command as long as we're open
            channel.sendall('\x00')
            msg = channel.recv(1024)
            if not msg:  # chan closed while recving
                break
            assert msg[-1:] == b'\n'
            msg = msg[:-1]
            code = msg[0:1]
            # recv file
            if code == b"C":
                cmd = msg[1:]
                parts = cmd.strip().split(b' ', 2)
                size = int(parts[1])

                channel.send(b'\x00')
                try:
                    def generate():
                        buff_size = buffer_size
                        pos = 0
                        while pos < size:
                            # we have to make sure we don't read the final byte
                            if size - pos <= buff_size:
                                buff_size = size - pos
                            buf = channel.recv(buff_size)
                            pos += len(buf)
                            yield buf
                        msg = channel.recv(512)
                        channel.close()
                        if msg and msg[0:1] != b'\x00':
                            self._closeSCPClient()
                            raise scp.SCPException(scp.asunicode(msg[1:]))

                    return generate()
                except SocketTimeout as st:
                    channel.close()
                    self._closeSCPClient()
                    raise scp.SCPException('Error receiving, socket.timeout') from st

    def push_file(self, local_path, remote_path, lp=None, is_advanced=False):
        self._connectSFTPClient().put(local_path, remote_path)

    def mkdir(self, remote_path):
        client = self._connectSFTPClient()
        # build the full directory up to remote_path
        folders = remote_path.split(os.sep)
        full_path = []
        for f in folders:
            full_path.append(f)
            subpath = os.sep.join(full_path)
            if subpath != '' and not self.exists(subpath):
                client.mkdir(subpath)

    def _ls(self, client, remote_path, recursive=False, is_file=False):
        listfile = {}

        def getfiles_rec(path):
            if is_file:
                file_stat = client.stat(path)
                listfile[self._external_path(path)] = {'size': file_stat.st_size,
                                                       'last_modified': file_stat.st_mtime}
            else:
                for f in client.listdir_attr(path=path):
                    fullpath = os.path.join(path, f.filename)
                    if S_ISDIR(f.st_mode):
                        if recursive:
                            getfiles_rec(fullpath)
                        else:
                            listfile[self._external_path(fullpath) + '/'] = {'is_dir': True}
                    else:
                        listfile[self._external_path(fullpath)] = {'size': f.st_size,
                                                                   'last_modified': f.st_mtime}

        getfiles_rec(remote_path)
        return listfile

    def listdir(self, remote_path, recursive=False, is_file=False, options=None):
        client = self._connectSFTPClient()
        return self._ls(client, remote_path, recursive, is_file)

    def _delete_single(self, remote_path, isdir):
        client = self._connectSFTPClient()
        if isdir:
            client.rmdir(remote_path)
        else:
            client.remove(remote_path)

    def rename(self, old_remote_path, new_remote_path):
        client = self._connectSFTPClient()
        client.posix_rename(old_remote_path, new_remote_path)

    def exists(self, remote_path):
        client = self._connectSFTPClient()
        try:
            client.stat(remote_path)
        except IOError:
            return False
        return True

    def isdir(self, remote_path):
        client = self._connectSFTPClient()
        try:
            return S_ISDIR(client.stat(remote_path).st_mode)
        except IOError:
            return False

    def _internal_path(self, path):
        if path.startswith('/'):
            path = path[1:]
        if self._basedir:
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

    def check_for_aliases(self, local_path):
        return None
