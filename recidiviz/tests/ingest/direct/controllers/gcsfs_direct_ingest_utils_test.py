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
"""Tests for gcsfs_direct_ingest_utils.py."""
import datetime
from unittest import TestCase

from mock import patch, Mock

from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import \
    gcsfs_direct_ingest_storage_directory_path_for_region, \
    gcsfs_direct_ingest_directory_path_for_region, filename_parts_from_path
from recidiviz.ingest.direct.errors import DirectIngestError


class GcsfsDirectIngestUtilsTest(TestCase):
    """Tests for gcsfs_direct_ingest_utils.py."""

    @patch('recidiviz.utils.metadata.project_id',
           Mock(return_value='recidiviz-123'))
    def test_get_county_storage_directory_path(self) -> None:
        self.assertEqual(
            gcsfs_direct_ingest_storage_directory_path_for_region(
                'us_tx_brazos', SystemLevel.COUNTY),
            'recidiviz-123-direct-ingest-county-storage/us_tx_brazos')

    @patch('recidiviz.utils.metadata.project_id',
           Mock(return_value='recidiviz-staging'))
    def test_get_state_storage_directory_path(self) -> None:
        self.assertEqual(
            gcsfs_direct_ingest_storage_directory_path_for_region(
                'us_nd', SystemLevel.STATE),
            'recidiviz-staging-direct-ingest-state-storage/us_nd')

    @patch('recidiviz.utils.metadata.project_id',
           Mock(return_value='recidiviz-123'))
    def test_get_county_ingest_directory_path_for_region(self):
        self.assertEqual(
            gcsfs_direct_ingest_directory_path_for_region(
                'us_tx_brazos', SystemLevel.COUNTY),
            'recidiviz-123-direct-ingest-county/us_tx_brazos')

    @patch('recidiviz.utils.metadata.project_id',
           Mock(return_value='recidiviz-staging'))
    def test_get_state_ingest_directory_path_for_region(self):
        self.assertEqual(
            gcsfs_direct_ingest_directory_path_for_region(
                'us_nd', SystemLevel.STATE),
            'recidiviz-staging-direct-ingest-state-us-nd')

    def test_filename_parts_from_path(self):
        with self.assertRaises(DirectIngestError):
            filename_parts_from_path('bucket/us_ca_sf/elite_offenders.csv')

        parts = filename_parts_from_path(
            'bucket-us-nd/unprocessed_2019-08-07T22:09:18:770655_'
            'elite_offenders.csv')

        self.assertEqual(parts.extension, 'csv')
        self.assertEqual(parts.file_tag, 'elite_offenders')
        self.assertEqual(parts.utc_upload_datetime,
                         datetime.datetime.fromisoformat(
                             '2019-08-07T22:09:18:770655'))
        self.assertEqual(parts.date_str, '2019-08-07')

        parts = filename_parts_from_path(
            'processed_2019-09-07T00:09:18:770655_elite_offenders.csv')

        self.assertEqual(parts.extension, 'csv')
        self.assertEqual(parts.file_tag, 'elite_offenders')
        self.assertEqual(parts.utc_upload_datetime,
                         datetime.datetime.fromisoformat(
                             '2019-09-07T00:09:18:770655'))
        self.assertEqual(parts.date_str, '2019-09-07')
