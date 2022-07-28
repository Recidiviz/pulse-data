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
"""Tests for the DirectIngestInstance."""
import unittest

from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_bucket_for_state,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.direct_ingest_instance_factory import (
    DirectIngestInstanceFactory,
)


class TestDirectIngestInstance(unittest.TestCase):
    """Tests for the DirectIngestInstance."""

    def test_from_state_ingest_bucket(self) -> None:
        ingest_bucket_path = gcsfs_direct_ingest_bucket_for_state(
            region_code="us_xx",
            ingest_instance=DirectIngestInstance.PRIMARY,
            project_id="recidiviz-456",
        )

        self.assertEqual(
            DirectIngestInstance.PRIMARY,
            DirectIngestInstanceFactory.for_ingest_bucket(ingest_bucket_path),
        )

        ingest_bucket_path = gcsfs_direct_ingest_bucket_for_state(
            region_code="us_xx",
            ingest_instance=DirectIngestInstance.SECONDARY,
            project_id="recidiviz-456",
        )

        self.assertEqual(
            DirectIngestInstance.SECONDARY,
            DirectIngestInstanceFactory.for_ingest_bucket(ingest_bucket_path),
        )
