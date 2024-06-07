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
"""GCS file processing tasks"""

import concurrent.futures
import heapq
from typing import List, Tuple

from airflow.decorators import task

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath

MAX_THREADS = 16  # TODO(#29946) determine reasonable default


@task
def create_file_batches(file_paths: List[str], num_batches: int) -> List[List[str]]:
    fs = GcsfsFactory.build()
    return batch_files_by_size(fs, file_paths, num_batches)


def batch_files_by_size(
    fs: GCSFileSystem, file_paths: List[str], num_batches: int
) -> List[List[str]]:
    """Divide files into batches with approximately equal cumulative size"""
    # If get_file_size returns None, set size to 0 and don't worry about sorting correctly
    # If the file doesn't exist we'll return an error downstream
    files_with_sizes = _get_files_with_sizes_concurrently(fs, file_paths)
    files_with_sizes.sort(key=lambda x: x[1], reverse=True)

    num_batches = len(file_paths) if len(file_paths) < num_batches else num_batches
    batches: List[List[str]] = [[] for _ in range(num_batches)]
    heap = [(0, batch_index) for batch_index in range(num_batches)]
    heapq.heapify(heap)

    for file_path, file_size in files_with_sizes:
        batch_size, batch_index = heapq.heappop(heap)
        batches[batch_index].append(file_path)
        heapq.heappush(heap, (batch_size + file_size, batch_index))

    return batches


def _get_files_with_sizes_concurrently(
    fs: GCSFileSystem, file_paths: List[str]
) -> List[Tuple[str, int]]:
    files_with_sizes: List[Tuple[str, int]] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        future_to_file_path = {
            executor.submit(_get_file_size, fs, file_path): file_path
            for file_path in file_paths
        }

        for future in concurrent.futures.as_completed(future_to_file_path):
            file_path = future_to_file_path[future]
            try:
                size = future.result()
                files_with_sizes.append((file_path, size))
            except Exception:
                files_with_sizes.append((file_path, 0))

    return files_with_sizes


def _get_file_size(fs: GCSFileSystem, file_path: str) -> int:
    return fs.get_file_size(GcsfsFilePath.from_absolute_path(file_path)) or 0
