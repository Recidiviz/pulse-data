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
import datetime
import io
import logging
import os
import stat
from collections import deque
from typing import List, Optional, Tuple

import paramiko
import pytz
from paramiko import SFTPAttributes
from paramiko.hostkeys import HostKeyEntry
from pysftp import CnOpts

from recidiviz.cloud_storage.gcs_file_system import BYTES_CONTENT_TYPE
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.common.io.sftp_file_contents_handle import SftpFileContentsHandle
from recidiviz.common.results import MultiRequestResultWithSkipped
from recidiviz.common.sftp_connection import RecidivizSftpConnection
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
    to_normalized_unprocessed_raw_file_path,
)
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_sftp_download_bucket_path_for_state,
)
from recidiviz.ingest.direct.metadata.postgres_direct_ingest_file_metadata_manager import (
    PostgresDirectIngestRawFileMetadataManager,
)
from recidiviz.ingest.direct.sftp.sftp_download_delegate_factory import (
    SftpDownloadDelegateFactory,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils import secrets

RAW_INGEST_DIRECTORY = "raw_data"


class SftpAuth:
    """Handles authentication for a given SFTP server."""

    def __init__(
        self,
        hostname: str,
        hostkey: str,
        hostkey_entry: HostKeyEntry,
        username: str,
        password: str,
        client_private_key: Optional[paramiko.RSAKey],
        port: Optional[int] = None,
    ):
        self.hostname = hostname
        # Define connection options needed in order to define a proper SFTP connection.
        # These are custom options necessary for machines that do not have ssh knownhosts.
        self.cnopts = CnOpts()
        _, keytype, _ = hostkey.split(" ")
        if hostkey_entry:
            self.cnopts.hostkeys.add(hostname, keytype, hostkey_entry.key)
        self.username = username
        self.password = password
        # If we need a client key, this variable contains the private part of the private/public
        # key pair.
        self.client_private_key = client_private_key
        # The default port for most SFTP servers is 22, but there may be cases where we
        # may need to specify a port
        self.port = 22 if port is None else port

    @classmethod
    def for_region(cls, region_code: str) -> "SftpAuth":
        """Creates a specific region's SFTP authentication method."""
        prefix = f"{region_code}_sftp"
        host = secrets.get_secret(f"{prefix}_host")
        if host is None:
            raise ValueError(f"Unable to find host name for secret key {prefix}_host")
        hostkey = secrets.get_secret(f"{prefix}_hostkey")
        if hostkey is None:
            raise ValueError(f"Unable to find hostkey for secret key {prefix}_hostkey")
        hostkey_entry = HostKeyEntry.from_line(hostkey)
        if hostkey_entry is None:
            raise ValueError(
                "Unable to convert to proper hostkey. Make sure it's "
                "in the format of hostname keytype key."
            )
        username = secrets.get_secret(f"{prefix}_username")
        if username is None:
            raise ValueError(
                f"Unable to find username for secret key {prefix}_username"
            )
        password = secrets.get_secret(f"{prefix}_password")
        if password is None:
            raise ValueError(
                f"Unable to find password for secret key {prefix}_password"
            )
        raw_client_private_key = secrets.get_secret(f"{prefix}_client_private_key")
        client_private_key = (
            None
            if not raw_client_private_key
            else paramiko.RSAKey.from_private_key(io.StringIO(raw_client_private_key))
        )
        port_secret = secrets.get_secret(f"{prefix}_port")
        port = None if not port_secret else int(port_secret)

        return SftpAuth(
            host, hostkey, hostkey_entry, username, password, client_private_key, port
        )


class DownloadFilesFromSftpController:
    """Class for interacting with and downloading files from SFTP servers."""

    def __init__(
        self,
        project_id: str,
        region_code: str,
        lower_bound_update_datetime: Optional[datetime.datetime],
        gcs_destination_path: Optional[GcsfsDirectoryPath] = None,
    ):
        self.project_id = project_id
        self.region_code = region_code.lower()

        self.auth = SftpAuth.for_region(region_code)
        self.delegate = SftpDownloadDelegateFactory.build(region_code=region_code)
        self.gcsfs = DirectIngestGCSFileSystem(GcsfsFactory.build())

        self.unable_to_download_items: List[str] = []
        self.downloaded_items: List[Tuple[str, datetime.datetime]] = []
        self.skipped_files: List[str] = []

        self.lower_bound_update_datetime = lower_bound_update_datetime
        self.bucket = (
            gcsfs_sftp_download_bucket_path_for_state(
                region_code, project_id=self.project_id
            )
            if gcs_destination_path is None
            else gcs_destination_path
        )
        self.download_dir = GcsfsDirectoryPath.from_dir_and_subdir(
            dir_path=self.bucket, subdir=RAW_INGEST_DIRECTORY
        )

        self.postgres_direct_ingest_file_metadata_manager = (
            PostgresDirectIngestRawFileMetadataManager(
                region_code,
                DirectIngestInstance.PRIMARY.database_key_for_state(
                    StateCode(region_code.upper())
                ).db_name,
            )
        )

    def _is_after_update_bound(self, sftp_attr: SFTPAttributes) -> bool:
        if self.lower_bound_update_datetime is None:
            return True
        if sftp_attr.st_mtime is None:
            # We want to error out noisily here because this is an unexpected
            # behavior. If it ends up happening, we need a more robust strategy
            # for determining timestamp bounds.
            raise ValueError("mtime for SFTP file was unexepctedly None")
        update_time = datetime.datetime.fromtimestamp(sftp_attr.st_mtime)
        if self.lower_bound_update_datetime.tzinfo:
            update_time = update_time.astimezone(pytz.UTC)
        return update_time >= self.lower_bound_update_datetime

    def _normalize_sftp_path(self, file_path: str) -> str:
        """Normalizes an SFTP path to download to GCS by first replacing dashes with
        underscores within the path, then setting up the path to be only the base directory
        and the base file name."""
        normalized_file_path = os.path.normpath(file_path).replace("-", "_")
        base_directory = os.path.basename(os.path.dirname(normalized_file_path))
        base_file_name = os.path.basename(normalized_file_path)
        return os.path.join(base_directory, base_file_name)

    def _fetch(
        self,
        connection: RecidivizSftpConnection,
        file_path: str,
        file_timestamp: datetime.datetime,
    ) -> None:
        """Fetches data files from the SFTP, tracking which items downloaded and failed to download."""
        normalized_sftp_path = self._normalize_sftp_path(file_path)
        normalized_upload_path = GcsfsFilePath.from_directory_and_file_name(
            dir_path=self.download_dir,
            file_name=os.path.basename(
                to_normalized_unprocessed_raw_file_path(
                    normalized_sftp_path, dt=file_timestamp
                )
            ),
        )
        if not self.postgres_direct_ingest_file_metadata_manager.has_raw_file_been_discovered(
            normalized_upload_path
        ) and not self.postgres_direct_ingest_file_metadata_manager.has_raw_file_been_processed(
            normalized_upload_path
        ):
            logging.info(
                "Downloading %s into %s", normalized_sftp_path, self.download_dir
            )
            try:
                path = GcsfsFilePath.from_directory_and_file_name(
                    dir_path=GcsfsDirectoryPath.from_dir_and_subdir(
                        dir_path=self.download_dir,
                        subdir=file_timestamp.strftime("%Y-%m-%dT%H:%M:%S:%f"),
                    ),
                    file_name=normalized_sftp_path,
                )
                self.gcsfs.upload_from_contents_handle_stream(
                    path=path,
                    contents_handle=SftpFileContentsHandle(
                        sftp_connection=connection, sftp_file_path=file_path
                    ),
                    content_type=BYTES_CONTENT_TYPE,
                )
                logging.info("Post processing %s", path.uri())
                self.downloaded_items.extend(
                    [
                        (
                            downloaded_file,
                            file_timestamp,
                        )
                        for downloaded_file in self.delegate.post_process_downloads(
                            path, self.gcsfs
                        )
                    ]
                )
            except IOError as e:
                logging.info(
                    "Could not download %s into %s: %s",
                    normalized_sftp_path,
                    self.download_dir,
                    e.args,
                )
                self.unable_to_download_items.append(file_path)
        else:
            logging.info(
                "Skipping downloading %s because it has already been previously downloaded for ingest.",
                normalized_sftp_path,
            )
            self.skipped_files.append(file_path)

    def get_paths_to_download(
        self, connection: RecidivizSftpConnection
    ) -> List[Tuple[str, datetime.datetime]]:
        """Opens a connection to SFTP and based on the delegate, find and recursively list items
        that are after the update bound and match the delegate's criteria, returning items and
        corresponding timestamps that are to be downloaded."""
        remote_dirs = connection.listdir()
        root = self.delegate.root_directory(remote_dirs)
        dirs_with_attributes = connection.listdir_attr(root)
        paths_post_timestamp = {}
        file_modes_of_paths = {}
        for sftp_attr in dirs_with_attributes:
            if not self._is_after_update_bound(sftp_attr):
                continue

            if sftp_attr.st_mtime is None:
                # We should never reach this point because we should have filtered out
                # None mtimes already.
                raise ValueError("mtime for SFTP file was unexpectedly None")

            paths_post_timestamp[sftp_attr.filename] = datetime.datetime.fromtimestamp(
                sftp_attr.st_mtime
            ).astimezone(pytz.UTC)
            file_modes_of_paths[sftp_attr.filename] = sftp_attr.st_mode

        paths_to_download = self.delegate.filter_paths(
            list(paths_post_timestamp.keys())
        )

        files_to_download_with_timestamps: List[Tuple[str, datetime.datetime]] = []

        for path in paths_to_download:
            file_timestamp = paths_post_timestamp[path]
            file_mode = file_modes_of_paths[path]
            if file_mode and stat.S_ISREG(file_mode):
                files_to_download_with_timestamps.append((path, file_timestamp))
            else:
                inner_paths = deque(
                    [
                        os.path.join(root, path, inner_path)
                        for inner_path in connection.listdir(path)
                    ]
                )
                while len(inner_paths) > 0:
                    current_path = inner_paths.popleft()
                    sftp_attr_of_current_path = connection.stat(current_path)
                    if sftp_attr_of_current_path.st_mode and stat.S_ISDIR(
                        sftp_attr_of_current_path.st_mode
                    ):
                        for entry in connection.listdir(current_path):
                            inner_paths.append(os.path.join(current_path, entry))
                    else:
                        files_to_download_with_timestamps.append(
                            (current_path, file_timestamp)
                        )

        return files_to_download_with_timestamps

    def fetch_files(
        self,
        connection: RecidivizSftpConnection,
        files_to_download_with_timestamps: List[Tuple[str, datetime.datetime]],
    ) -> None:
        """Opens up one connection and loops through all of the files with timestamps to upload
        to the GCS bucket."""
        for file_path, file_timestamp in files_to_download_with_timestamps:
            self._fetch(connection, file_path, file_timestamp)

    def do_fetch(
        self,
    ) -> MultiRequestResultWithSkipped[Tuple[str, datetime.datetime], str, str]:
        """Attempts to open an SFTP connection and download items, returning the corresponding paths
        and the timestamp associated, and also any unable to be downloaded."""
        with RecidivizSftpConnection(
            host=self.auth.hostname,
            username=self.auth.username,
            password=self.auth.password,
            port=self.auth.port,
            private_key=self.auth.client_private_key,
            cnopts=self.auth.cnopts,
        ) as connection:
            logging.info(
                "Downloading raw files from SFTP server [%s] to ingest bucket [%s] for project [%s]",
                self.auth.hostname,
                self.bucket.uri(),
                self.project_id,
            )

            files_to_download_with_timestamps = self.get_paths_to_download(connection)
            logging.info(
                "Found %s items to download from SFTP server [%s] to upload to ingest bucket [%s]",
                len(files_to_download_with_timestamps),
                self.auth.hostname,
                self.bucket,
            )

            self.fetch_files(connection, files_to_download_with_timestamps)

            logging.info(
                "Download complete, successfully downloaded %s files to ingest bucket [%s] "
                "could not download %s files and skipped %s files",
                len(self.downloaded_items),
                self.download_dir.uri(),
                len(self.unable_to_download_items),
                len(self.skipped_files),
            )
            return MultiRequestResultWithSkipped(
                successes=self.downloaded_items,
                failures=self.unable_to_download_items,
                skipped=self.skipped_files,
            )
