# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Defines a class that can be used to access contents of a file on an SFTP server."""
import logging
import math
import time
from contextlib import contextmanager
from datetime import timedelta
from tempfile import TemporaryFile
from typing import IO, Callable, Iterator, Optional, Union

from paramiko import SFTPClient, SFTPFile

from recidiviz.common.io.file_contents_handle import FileContentsHandle


def human_readable_size(size: float, decimal_places: Optional[int] = None) -> str:
    decimal_places = 2 if decimal_places is None else decimal_places
    for unit in ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]:
        if size < 1024.0 or unit == "PiB":
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f} {unit if unit is not None else 2}"


def create_progress_callback(
    reporting_interval: Optional[int] = None,
) -> Callable[[int, int], None]:
    """Creates a function which prints out download progress"""
    reporting_interval = 5 if reporting_interval is None else reporting_interval
    start_time = time.time()
    last_report = start_time
    last_processed_bytes = 0

    def progress_callback(processed_bytes: int, total_bytes: int) -> None:
        nonlocal last_report
        nonlocal last_processed_bytes
        current_time = time.time()
        elapsed = current_time - last_report
        remaining_bytes = total_bytes - processed_bytes
        if elapsed > reporting_interval:
            throughput = (processed_bytes - last_processed_bytes) / elapsed
            logging.info(
                "%s / %s %s/s ETA: %s",
                human_readable_size(processed_bytes),
                human_readable_size(total_bytes),
                human_readable_size(throughput),
                timedelta(seconds=math.floor(remaining_bytes / throughput)),
            )
            last_report = current_time
            last_processed_bytes = processed_bytes

    return progress_callback


class SftpFileContentsHandle(FileContentsHandle[bytes, Union[SFTPFile, IO]]):
    """A class that can be used to access contents of a file on an SFTP server."""

    def __init__(
        self,
        sftp_file_path: str,
        sftp_client: SFTPClient,
    ):
        self.sftp_file_path = sftp_file_path
        self.sftp_client = sftp_client

    @contextmanager
    def open(self, mode: str = "r") -> Iterator[Union[SFTPFile, IO]]:
        if "r" in mode:
            with TemporaryFile(mode="w+b") as f:
                # Perform a threaded download of the SFTP object to a temp file
                self.sftp_client.getfo(
                    self.sftp_file_path,
                    fl=f,
                    callback=create_progress_callback(),
                    prefetch=True,
                )

                # Rewind stream for reading
                f.seek(0)

                yield f
        else:
            with self.sftp_client.open(filename=self.sftp_file_path, mode=mode) as f:
                yield f
