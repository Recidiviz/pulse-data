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
"""Tests for sequencing_tasks"""
from unittest import TestCase

from recidiviz.airflow.dags.raw_data.sequencing_tasks import (
    has_files_to_import,
    successfully_acquired_all_locks,
)
from recidiviz.common.constants.operations.direct_ingest_raw_data_resource_lock import (
    DirectIngestRawDataResourceLockResource,
)
from recidiviz.ingest.direct.types.raw_data_import_types import RawDataResourceLock


class SequencingTests(TestCase):
    """Tests for sequencing_tasks."""

    def test_has_files_to_import(self) -> None:
        assert not has_files_to_import.function(None)
        assert not has_files_to_import.function([])
        assert has_files_to_import.function(["a"])

    def test_successfully_acquired_all_locks(self) -> None:
        assert not successfully_acquired_all_locks.function(None, [])
        assert successfully_acquired_all_locks.function([], [])
        assert not successfully_acquired_all_locks.function(
            [], [DirectIngestRawDataResourceLockResource.BUCKET]
        )
        assert not successfully_acquired_all_locks.function(
            [
                RawDataResourceLock(
                    lock_id=1,
                    lock_resource=DirectIngestRawDataResourceLockResource.BIG_QUERY_RAW_DATA_DATASET,
                    released=False,
                ).serialize()
            ],
            [DirectIngestRawDataResourceLockResource.BUCKET],
        )
        assert not successfully_acquired_all_locks.function(
            [
                RawDataResourceLock(
                    lock_id=1,
                    lock_resource=DirectIngestRawDataResourceLockResource.BUCKET,
                    released=True,
                ).serialize()
            ],
            [DirectIngestRawDataResourceLockResource.BUCKET],
        )
        assert successfully_acquired_all_locks.function(
            [
                RawDataResourceLock(
                    lock_id=1,
                    lock_resource=DirectIngestRawDataResourceLockResource.BUCKET,
                    released=False,
                ).serialize()
            ],
            [DirectIngestRawDataResourceLockResource.BUCKET],
        )
