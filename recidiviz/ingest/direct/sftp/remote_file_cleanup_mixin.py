# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Mixin class for deleting files on a remote SFTP server"""
import logging

import paramiko


class RemoteFileCleanupMixin:
    """Mixin that deletes files on a remote SFTP sever after validating that the calling
    SFTP delegate is only configured to run in a single environment to avoid race
    conditions.
    """

    @classmethod
    def remove_remote_file(
        cls,
        *,
        sftp_client: paramiko.SFTPClient,
        remote_path: str,
        supported_environments: list[str],
    ) -> None:
        if len(supported_environments) > 1:
            raise ValueError(
                f"The RemoteFileCleanupMixin cannot be used for a region that runs in "
                f"more than one project, as it can cause race conditions for file reading. "
                f"Please configure [{cls.__name__}] to only be supported in a single "
                f"environment"
            )

        logging.info("Removing [%s] from remote sftp server", remote_path)
        sftp_client.remove(remote_path)
