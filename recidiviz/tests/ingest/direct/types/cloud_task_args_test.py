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

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    to_normalized_unprocessed_file_name,
)
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_bucket_for_region,
)
from recidiviz.ingest.direct.gcs.file_type import GcsfsDirectIngestFileType
from recidiviz.ingest.direct.types.cloud_task_args import (
    BQIngestViewMaterializationArgs,
    GcsfsIngestViewExportArgs,
    LegacyExtractAndMergeArgs,
    NewExtractAndMergeArgs,
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
            ingest_instance_=DirectIngestInstance.PRIMARY,
        )

        self.assertEqual(args.ingest_instance, DirectIngestInstance.PRIMARY)
        self.assertEqual(
            "ingest_view_materialization_my_ingest_view_name-PRIMARY-None-2019_11_22_11_22_33_444444",
            args.task_id_tag(),
        )

        args = BQIngestViewMaterializationArgs(
            ingest_view_name="my_ingest_view_name",
            lower_bound_datetime_exclusive=dt_lower,
            upper_bound_datetime_inclusive=dt_upper,
            ingest_instance_=DirectIngestInstance.SECONDARY,
        )

        self.assertEqual(args.ingest_instance, DirectIngestInstance.SECONDARY)
        self.assertEqual(
            "ingest_view_materialization_my_ingest_view_name-SECONDARY-2019_01_22_11_22_33_444444-2019_11_22_11_22_33_444444",
            args.task_id_tag(),
        )

    # TODO(#11424): Delete this test once BQ materialization is enabled for all states.
    def test_legacy_extract_and_merge_args(self) -> None:
        dt = datetime.datetime(2019, 11, 22, 11, 22, 33, 444444)
        bucket = gcsfs_direct_ingest_bucket_for_region(
            project_id="recidiviz-456",
            region_code="us_xx",
            system_level=SystemLevel.STATE,
            ingest_instance=DirectIngestInstance.PRIMARY,
        )
        ingest_view_file_path = GcsfsFilePath.from_directory_and_file_name(
            bucket,
            to_normalized_unprocessed_file_name(
                "my_ingest_view_name.csv", GcsfsDirectIngestFileType.INGEST_VIEW, dt=dt
            ),
        )

        args = LegacyExtractAndMergeArgs(
            ingest_time=datetime.datetime.now(), file_path=ingest_view_file_path
        )

        self.assertEqual(args.ingest_instance, DirectIngestInstance.PRIMARY)
        self.assertEqual(args.ingest_view_name, "my_ingest_view_name")
        self.assertEqual(
            "ingest_job_my_ingest_view_name_2019-11-22",
            args.task_id_tag(),
        )

    def test_extract_and_merge_args(self) -> None:
        dt = datetime.datetime(2019, 11, 22, 11, 22, 33, 444444)
        args = NewExtractAndMergeArgs(
            ingest_time=datetime.datetime.now(),
            ingest_view_name="my_ingest_view_name",
            ingest_instance=DirectIngestInstance.PRIMARY,
            upper_bound_datetime_inclusive=dt,
            batch_number=2,
        )

        self.assertEqual(args.ingest_instance, DirectIngestInstance.PRIMARY)
        self.assertEqual(args.ingest_view_name, "my_ingest_view_name")
        self.assertEqual(
            "extract_and_merge_my_ingest_view_name_2019-11-22_batch_2",
            args.task_id_tag(),
        )
