# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Airflow tasks for the clean up and storage step of the raw data import dag"""
import logging
from concurrent import futures
from typing import List

from airflow.decorators import task

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath

MAX_DELETE_THREADS = 16


@task
def clean_up_temporary_files(serialized_temporary_paths: List[str]) -> None:
    """Deletes all paths in |serialized_temporary_paths|, if they exist."""
    temporary_paths = [
        GcsfsFilePath.from_absolute_path(path) for path in serialized_temporary_paths
    ]

    fs = GcsfsFactory.build()

    with futures.ThreadPoolExecutor(max_workers=MAX_DELETE_THREADS) as executor:
        delete_futures = [executor.submit(fs.delete, path) for path in temporary_paths]

        failed: List[Exception] = []
        deleted = 0
        for f in futures.as_completed(delete_futures):
            try:
                f.result()
                deleted += 1
            except Exception as e:
                failed.append(e)

    logging.info(
        "Confirmed [%s/%s] paths are deleted",
        deleted,
        len(temporary_paths),
    )

    if failed:
        raise ExceptionGroup("Errors occurred during path deletion", failed)
