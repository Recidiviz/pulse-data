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

from recidiviz.ingest.direct.types.cloud_task_args import (
    BQIngestViewMaterializationArgs,
    GcsfsIngestViewExportArgs,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


class TestCloudTaskArgs(TestCase):
    """Tests for cloud_task_args.py."""

    # TODO(#11424): Delete this test once BQ materialization is enabled for all states.
    def test_gcsfs_ingest_view_export_args(self) -> None:
        dt_lower = datetime.datetime(2019, 1, 22, 11, 22, 33, 444444)
        dt_upper = datetime.datetime(2019, 11, 22, 11, 22, 33, 444444)

        args = GcsfsIngestViewExportArgs(
            ingest_view_name="my_file_tag",
            output_bucket_name="an_ingest_bucket",
            lower_bound_datetime_exclusive=None,
            upper_bound_datetime_inclusive=dt_upper,
        )

        self.assertEqual(
            "ingest_view_export_my_file_tag-an_ingest_bucket-None-2019_11_22_11_22_33_444444",
            args.task_id_tag(),
        )

        args = GcsfsIngestViewExportArgs(
            ingest_view_name="my_file_tag",
            output_bucket_name="an_ingest_bucket",
            lower_bound_datetime_exclusive=dt_lower,
            upper_bound_datetime_inclusive=dt_upper,
        )

        self.assertEqual(
            "ingest_view_export_my_file_tag-an_ingest_bucket-2019_01_22_11_22_33_444444-2019_11_22_11_22_33_444444",
            args.task_id_tag(),
        )

    def test_bq_ingest_view_materialization_args(self) -> None:
        dt_lower = datetime.datetime(2019, 1, 22, 11, 22, 33, 444444)
        dt_upper = datetime.datetime(2019, 11, 22, 11, 22, 33, 444444)

        args = BQIngestViewMaterializationArgs(
            ingest_view_name="my_ingest_view_name",
            lower_bound_datetime_exclusive=None,
            upper_bound_datetime_inclusive=dt_upper,
            ingest_instance=DirectIngestInstance.SECONDARY,
        )

        self.assertEqual(
            "ingest_view_materialization_my_ingest_view_name-SECONDARY-None-2019_11_22_11_22_33_444444",
            args.task_id_tag(),
        )

        args = BQIngestViewMaterializationArgs(
            ingest_view_name="my_ingest_view_name",
            lower_bound_datetime_exclusive=dt_lower,
            upper_bound_datetime_inclusive=dt_upper,
            ingest_instance=DirectIngestInstance.PRIMARY,
        )

        self.assertEqual(
            "ingest_view_materialization_my_ingest_view_name-PRIMARY-2019_01_22_11_22_33_444444-2019_11_22_11_22_33_444444",
            args.task_id_tag(),
        )
