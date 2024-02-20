# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for cloud_task_args.py."""
import datetime
from unittest import TestCase

from recidiviz.ingest.direct.types.cloud_task_args import IngestViewMaterializationArgs
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


# TODO(#20930): Move this class to another location when we move
#  IngestViewMaterializationArgs.
class TestCloudTaskArgs(TestCase):
    """Tests for cloud_task_args.py."""

    def test_bq_ingest_view_materialization_args(self) -> None:
        dt_lower = datetime.datetime(2019, 1, 22, 11, 22, 33, 444444)
        dt_upper = datetime.datetime(2019, 11, 22, 11, 22, 33, 444444)

        args = IngestViewMaterializationArgs(
            ingest_view_name="my_ingest_view_name",
            lower_bound_datetime_exclusive=None,
            upper_bound_datetime_inclusive=dt_upper,
            ingest_instance=DirectIngestInstance.PRIMARY,
        )

        self.assertEqual(args.ingest_instance, DirectIngestInstance.PRIMARY)

        args = IngestViewMaterializationArgs(
            ingest_view_name="my_ingest_view_name",
            lower_bound_datetime_exclusive=dt_lower,
            upper_bound_datetime_inclusive=dt_upper,
            ingest_instance=DirectIngestInstance.SECONDARY,
        )

        self.assertEqual(args.ingest_instance, DirectIngestInstance.SECONDARY)
