# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Class for interacting with and downloading files from SFTP servers"""
import abc
import datetime
import logging
import os
import shutil
import tempfile

from typing import Optional, List

import pysftp
from paramiko import SFTPAttributes

from recidiviz.utils import secrets


class SftpAuth:
    """Handles authentication for a given SFTP server."""

    def __init__(self, hostname: str, username: Optional[str], password: Optional[str]):
        self.hostname = hostname
        self.username = username
        self.password = password

    @classmethod
    def for_region(cls, region_code: str) -> 'SftpAuth':
        prefix = f"us_{region_code}_sftp"
        host = secrets.get_secret(f"{prefix}_host")
        if host is None:
            raise ValueError(f"Unable to find host name for secret key {prefix}_host")
        username = secrets.get_secret(f"{prefix}_username")
        password = secrets.get_secret(f"{prefix}_password")
        return SftpAuth(host, username, password)


class BaseSftpDownloadDelegate(abc.ABC):
    """Contains the base class to handle region-specific SFTP downloading logic."""

    @abc.abstractmethod
    def root_directory(self, candidate_paths: List[str]) -> str:
        """Returns the path of the root directory new files should be searched in."""

    @abc.abstractmethod
    def filter_paths(self, candidate_paths: List[str]) -> List[str]:
        """Given a list of paths discovered in the SFTP, returns a filtered list including only the values
        that are to be downloaded."""

    @abc.abstractmethod
    def post_process_downloads(self, download_directory_path: str) -> None:
        """Should be implemented if any of the downloaded values need post-processing prior to uploading to
        GCS (e.g. unzipping a zip file)."""


class DownloadFilesFromSftpController:
    """Class for interacting with and downloading files from SFTP servers."""

    def __init__(self,
                 sftp_auth: SftpAuth,
                 delegate: BaseSftpDownloadDelegate,
                 lower_bound_update_datetime: Optional[datetime.datetime],
                 local_destination_path: Optional[str] = None):

        self.auth = sftp_auth
        self.delegate = delegate
        self.lower_bound_update_datetime = lower_bound_update_datetime

        tempdir = tempfile.gettempdir()
        self.localdir = local_destination_path if local_destination_path is not None else tempdir

    def _is_after_update_bound(self, sftp_attr: SFTPAttributes) -> bool:
        if self.lower_bound_update_datetime is None:
            return True
        update_time = datetime.datetime.fromtimestamp(sftp_attr.st_mtime)
        return update_time >= self.lower_bound_update_datetime

    def _fetch(self, connection: pysftp.Connection, path_to_fetch: str) -> None:
        try:
            logging.info("Downloading %s into %s", path_to_fetch, self.localdir)
            connection.get_r(remotedir=path_to_fetch, localdir=self.localdir)
            logging.info("Post processing %s in %s", path_to_fetch, self.localdir)
            self.delegate.post_process_downloads(os.path.join(self.localdir, path_to_fetch))
        except IOError:
            logging.info("Could not download %s into %s", path_to_fetch, self.localdir)

    def _fetch_recursively(self) -> Optional[int]:
        """Opens a connection to SFTP and based on the delegate, find and recursively download
        items that are after the update bound and match the delegate's criteria, returning number
        of items downloaded in response."""
        try:
            with pysftp.Connection(host=self.auth.hostname, username=self.auth.username,
                                password=self.auth.password) as connection:
                remote_dirs = connection.listdir()
                root = self.delegate.root_directory(remote_dirs)
                dirs_with_attributes = connection.listdir_attr(root)
                paths_post_timestamp = [sftp_attr.filename for sftp_attr in dirs_with_attributes
                                        if self._is_after_update_bound(sftp_attr)]
                items_to_download = self.delegate.filter_paths(paths_post_timestamp)

                logging.info("Attempting to download %s items from %s", len(items_to_download), root)

                for item in items_to_download:
                    self._fetch(connection, item)
            return len(os.listdir(self.localdir))
        except BaseException as e:
            logging.info("Error downloading items due to %s", e)
            return None

    def clean_up(self) -> bool:
        """Attempts to recursively remove any downloaded folders created as part of do_fetch."""
        try:
            logging.info("Cleaning up items in %s.", self.localdir)
            shutil.rmtree(self.localdir)
            return True
        except Exception as e:
            logging.info("%s could not be cleaned up due to an error %s.", self.localdir, e.args)
            return False

    def do_fetch(self) -> Optional[int]:
        """Attempts to open an SFTP connection and download items recursively."""
        items = self._fetch_recursively()
        if items is not None:
            logging.info("Successfully downloaded %s items into %s", items, self.localdir)
        else:
            logging.info("Downloaded no items into %s", self.localdir)
        return items
