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
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import \
    gcsfs_direct_ingest_storage_directory_path_for_region, \
    gcsfs_direct_ingest_directory_path_for_region, filename_parts_from_path, GcsfsDirectIngestFileType, \
    GcsfsIngestViewExportArgs
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
           Mock(return_value='recidiviz-staging'))
    def test_get_state_storage_directory_path_file_type_raw(self) -> None:
        self.assertEqual(
            gcsfs_direct_ingest_storage_directory_path_for_region(
                'us_nd', SystemLevel.STATE, GcsfsDirectIngestFileType.RAW_DATA),
            'recidiviz-staging-direct-ingest-state-storage/us_nd/raw')

    @patch('recidiviz.utils.metadata.project_id',
           Mock(return_value='recidiviz-123'))
    def test_get_county_ingest_directory_path_for_region(self) -> None:
        self.assertEqual(
            gcsfs_direct_ingest_directory_path_for_region(
                'us_tx_brazos', SystemLevel.COUNTY),
            'recidiviz-123-direct-ingest-county/us_tx_brazos')

    @patch('recidiviz.utils.metadata.project_id',
           Mock(return_value='recidiviz-staging'))
    def test_get_state_ingest_directory_path_for_region(self) -> None:
        self.assertEqual(
            gcsfs_direct_ingest_directory_path_for_region(
                'us_nd', SystemLevel.STATE),
            'recidiviz-staging-direct-ingest-state-us-nd')

    def test_filename_parts_from_path_unspecified_file_type(self) -> None:
        with self.assertRaises(DirectIngestError):
            filename_parts_from_path(
                GcsfsFilePath.from_absolute_path(
                    'bucket/us_ca_sf/elite_offenders.csv'))

        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                'bucket-us-nd/unprocessed_2019-08-07T22:09:18:770655_'
                'elite_offenders.csv'))

        self.assertEqual(parts.processed_state, 'unprocessed')
        self.assertEqual(parts.extension, 'csv')
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.UNSPECIFIED)
        self.assertEqual(parts.file_tag, 'elite_offenders')
        self.assertEqual(parts.filename_suffix, None)
        self.assertEqual(parts.utc_upload_datetime,
                         datetime.datetime.fromisoformat(
                             '2019-08-07T22:09:18:770655'))
        self.assertEqual(parts.date_str, '2019-08-07')
        self.assertEqual(parts.is_file_split, False)
        self.assertEqual(parts.file_split_size, None)

        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                'bucket-us-nd/processed_2019-09-07T00:09:18:770655_'
                'elite_offenders.csv'))

        self.assertEqual(parts.processed_state, 'processed')
        self.assertEqual(parts.extension, 'csv')
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.UNSPECIFIED)
        self.assertEqual(parts.file_tag, 'elite_offenders')
        self.assertEqual(parts.filename_suffix, None)
        self.assertEqual(parts.utc_upload_datetime,
                         datetime.datetime.fromisoformat(
                             '2019-09-07T00:09:18:770655'))
        self.assertEqual(parts.date_str, '2019-09-07')
        self.assertEqual(parts.is_file_split, False)
        self.assertEqual(parts.file_split_size, None)

        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                'bucket-us-nd/processed_2019-09-07T00:09:18:770655_'
                'elite_offenders_1split.csv'))

        self.assertEqual(parts.processed_state, 'processed')
        self.assertEqual(parts.extension, 'csv')
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.UNSPECIFIED)
        self.assertEqual(parts.file_tag, 'elite_offenders')
        self.assertEqual(parts.filename_suffix, '1split')
        self.assertEqual(parts.utc_upload_datetime,
                         datetime.datetime.fromisoformat(
                             '2019-09-07T00:09:18:770655'))
        self.assertEqual(parts.date_str, '2019-09-07')

        # Needs the actual file_split suffix to be a file split
        self.assertEqual(parts.is_file_split, False)
        self.assertEqual(parts.file_split_size, None)

        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                'bucket-us-nd/processed_2019-09-07T00:09:18:770655_'
                'elite_offenders_002_file_split.csv'))

        self.assertEqual(parts.processed_state, 'processed')
        self.assertEqual(parts.extension, 'csv')
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.UNSPECIFIED)
        self.assertEqual(parts.file_tag, 'elite_offenders')
        self.assertEqual(parts.filename_suffix, '002_file_split')
        self.assertEqual(parts.utc_upload_datetime,
                         datetime.datetime.fromisoformat(
                             '2019-09-07T00:09:18:770655'))
        self.assertEqual(parts.date_str, '2019-09-07')

        self.assertEqual(parts.is_file_split, True)
        self.assertEqual(parts.file_split_size, None)

        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                'bucket-us-nd/processed_2019-09-07T00:09:18:770655_'
                'elite_offenders_002_file_split_size300.csv'))

        self.assertEqual(parts.processed_state, 'processed')
        self.assertEqual(parts.extension, 'csv')
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.UNSPECIFIED)
        self.assertEqual(parts.file_tag, 'elite_offenders')
        self.assertEqual(parts.filename_suffix, '002_file_split_size300')
        self.assertEqual(parts.utc_upload_datetime,
                         datetime.datetime.fromisoformat(
                             '2019-09-07T00:09:18:770655'))
        self.assertEqual(parts.date_str, '2019-09-07')

        self.assertEqual(parts.is_file_split, True)
        self.assertEqual(parts.file_split_size, 300)

        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                'bucket-us-nd/processed_2019-09-07T00:09:18:770655_'
                'BrazosCounty_2019_09_25.csv'))

        self.assertEqual(parts.processed_state, 'processed')
        self.assertEqual(parts.extension, 'csv')
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.UNSPECIFIED)
        self.assertEqual(parts.file_tag, 'BrazosCounty')
        self.assertEqual(parts.filename_suffix, '2019_09_25')
        self.assertEqual(parts.utc_upload_datetime,
                         datetime.datetime.fromisoformat(
                             '2019-09-07T00:09:18:770655'))
        self.assertEqual(parts.date_str, '2019-09-07')

        self.assertEqual(parts.is_file_split, False)
        self.assertEqual(parts.file_split_size, None)

        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                'bucket-us-nd/processed_2019-09-07T00:09:18:770655_'
                'BrazosCounty_2019_09_25_002_file_split_size300.csv'))

        self.assertEqual(parts.processed_state, 'processed')
        self.assertEqual(parts.extension, 'csv')
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.UNSPECIFIED)
        self.assertEqual(parts.file_tag, 'BrazosCounty')
        self.assertEqual(parts.filename_suffix,
                         '2019_09_25_002_file_split_size300')
        self.assertEqual(parts.utc_upload_datetime,
                         datetime.datetime.fromisoformat(
                             '2019-09-07T00:09:18:770655'))
        self.assertEqual(parts.date_str, '2019-09-07')

        self.assertEqual(parts.is_file_split, True)
        self.assertEqual(parts.file_split_size, 300)

        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                'bucket-us-mo/unprocessed_2019-09-07T00:09:18:770655_'
                'tak001_offender_identification.csv'))

        self.assertEqual(parts.processed_state, 'unprocessed')
        self.assertEqual(parts.extension, 'csv')
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.UNSPECIFIED)
        self.assertEqual(parts.file_tag, 'tak001_offender_identification')
        self.assertEqual(parts.filename_suffix, None)
        self.assertEqual(parts.utc_upload_datetime,
                         datetime.datetime.fromisoformat(
                             '2019-09-07T00:09:18:770655'))
        self.assertEqual(parts.date_str, '2019-09-07')

        self.assertEqual(parts.is_file_split, False)
        self.assertEqual(parts.file_split_size, None)

        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                'bucket-us-mo/unprocessed_2019-09-07T00:09:18:770655_'
                'tak001_offender_identification_002_file_split_size300.csv'))

        self.assertEqual(parts.processed_state, 'unprocessed')
        self.assertEqual(parts.extension, 'csv')
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.UNSPECIFIED)
        self.assertEqual(parts.file_tag, 'tak001_offender_identification')
        self.assertEqual(parts.filename_suffix, '002_file_split_size300')
        self.assertEqual(parts.utc_upload_datetime,
                         datetime.datetime.fromisoformat(
                             '2019-09-07T00:09:18:770655'))
        self.assertEqual(parts.date_str, '2019-09-07')

        self.assertEqual(parts.is_file_split, True)
        self.assertEqual(parts.file_split_size, 300)

        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                'storage_bucket/region_subdir/2020-04-29/processed_2020-04-29T18:02:41:789323_test_file-(1).csv')
        )

        self.assertEqual(parts.processed_state, 'processed')
        self.assertEqual(parts.extension, 'csv')
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.UNSPECIFIED)
        self.assertEqual(parts.file_tag, 'test_file')
        self.assertEqual(parts.filename_suffix, None)
        self.assertEqual(parts.utc_upload_datetime,
                         datetime.datetime.fromisoformat(
                             '2020-04-29T18:02:41:789323'))
        self.assertEqual(parts.date_str, '2020-04-29')

        self.assertEqual(parts.is_file_split, False)

    def test_filename_parts_from_path_with_file_type(self) -> None:
        with self.assertRaises(DirectIngestError):
            filename_parts_from_path(
                GcsfsFilePath.from_absolute_path(
                    'bucket/us_ca_sf/elite_offenders.csv'))

        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                'bucket-us-nd/unprocessed_2019-08-07T22:09:18:770655_'
                'raw_elite_offenders.csv'))

        self.assertEqual(parts.processed_state, 'unprocessed')
        self.assertEqual(parts.extension, 'csv')
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.RAW_DATA)
        self.assertEqual(parts.file_tag, 'elite_offenders')
        self.assertEqual(parts.filename_suffix, None)
        self.assertEqual(parts.utc_upload_datetime,
                         datetime.datetime.fromisoformat(
                             '2019-08-07T22:09:18:770655'))
        self.assertEqual(parts.date_str, '2019-08-07')
        self.assertEqual(parts.is_file_split, False)
        self.assertEqual(parts.file_split_size, None)

        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                'bucket-us-nd/processed_2019-09-07T00:09:18:770655_'
                'ingest_view_elite_offenders.csv'))

        self.assertEqual(parts.processed_state, 'processed')
        self.assertEqual(parts.extension, 'csv')
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.INGEST_VIEW)
        self.assertEqual(parts.file_tag, 'elite_offenders')
        self.assertEqual(parts.filename_suffix, None)
        self.assertEqual(parts.utc_upload_datetime,
                         datetime.datetime.fromisoformat(
                             '2019-09-07T00:09:18:770655'))
        self.assertEqual(parts.date_str, '2019-09-07')
        self.assertEqual(parts.is_file_split, False)
        self.assertEqual(parts.file_split_size, None)

        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                'bucket-us-nd/processed_2019-09-07T00:09:18:770655_'
                'raw_elite_offenders_1split.csv'))

        self.assertEqual(parts.processed_state, 'processed')
        self.assertEqual(parts.extension, 'csv')
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.RAW_DATA)
        self.assertEqual(parts.file_tag, 'elite_offenders')
        self.assertEqual(parts.filename_suffix, '1split')
        self.assertEqual(parts.utc_upload_datetime,
                         datetime.datetime.fromisoformat(
                             '2019-09-07T00:09:18:770655'))
        self.assertEqual(parts.date_str, '2019-09-07')

        # Needs the actual file_split suffix to be a file split
        self.assertEqual(parts.is_file_split, False)
        self.assertEqual(parts.file_split_size, None)

        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                'bucket-us-nd/processed_2019-09-07T00:09:18:770655_'
                'ingest_view_elite_offenders_002_file_split.csv'))

        self.assertEqual(parts.processed_state, 'processed')
        self.assertEqual(parts.extension, 'csv')
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.INGEST_VIEW)
        self.assertEqual(parts.file_tag, 'elite_offenders')
        self.assertEqual(parts.filename_suffix, '002_file_split')
        self.assertEqual(parts.utc_upload_datetime,
                         datetime.datetime.fromisoformat(
                             '2019-09-07T00:09:18:770655'))
        self.assertEqual(parts.date_str, '2019-09-07')

        self.assertEqual(parts.is_file_split, True)
        self.assertEqual(parts.file_split_size, None)

        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                'bucket-us-nd/processed_2019-09-07T00:09:18:770655_'
                'raw_elite_offenders_002_file_split_size300.csv'))

        self.assertEqual(parts.processed_state, 'processed')
        self.assertEqual(parts.extension, 'csv')
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.RAW_DATA)
        self.assertEqual(parts.file_tag, 'elite_offenders')
        self.assertEqual(parts.filename_suffix, '002_file_split_size300')
        self.assertEqual(parts.utc_upload_datetime,
                         datetime.datetime.fromisoformat(
                             '2019-09-07T00:09:18:770655'))
        self.assertEqual(parts.date_str, '2019-09-07')

        self.assertEqual(parts.is_file_split, True)
        self.assertEqual(parts.file_split_size, 300)

        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                'bucket-us-nd/processed_2019-09-07T00:09:18:770655_'
                'ingest_view_BrazosCounty_2019_09_25.csv'))

        self.assertEqual(parts.processed_state, 'processed')
        self.assertEqual(parts.extension, 'csv')
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.INGEST_VIEW)
        self.assertEqual(parts.file_tag, 'BrazosCounty')
        self.assertEqual(parts.filename_suffix, '2019_09_25')
        self.assertEqual(parts.utc_upload_datetime,
                         datetime.datetime.fromisoformat(
                             '2019-09-07T00:09:18:770655'))
        self.assertEqual(parts.date_str, '2019-09-07')

        self.assertEqual(parts.is_file_split, False)
        self.assertEqual(parts.file_split_size, None)

        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                'bucket-us-nd/processed_2019-09-07T00:09:18:770655_'
                'raw_BrazosCounty_2019_09_25_002_file_split_size300.csv'))

        self.assertEqual(parts.processed_state, 'processed')
        self.assertEqual(parts.extension, 'csv')
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.RAW_DATA)
        self.assertEqual(parts.file_tag, 'BrazosCounty')
        self.assertEqual(parts.filename_suffix,
                         '2019_09_25_002_file_split_size300')
        self.assertEqual(parts.utc_upload_datetime,
                         datetime.datetime.fromisoformat(
                             '2019-09-07T00:09:18:770655'))
        self.assertEqual(parts.date_str, '2019-09-07')

        self.assertEqual(parts.is_file_split, True)
        self.assertEqual(parts.file_split_size, 300)

        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                'bucket-us-mo/unprocessed_2019-09-07T00:09:18:770655_'
                'ingest_view_tak001_offender_identification.csv'))

        self.assertEqual(parts.processed_state, 'unprocessed')
        self.assertEqual(parts.extension, 'csv')
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.INGEST_VIEW)
        self.assertEqual(parts.file_tag, 'tak001_offender_identification')
        self.assertEqual(parts.filename_suffix, None)
        self.assertEqual(parts.utc_upload_datetime,
                         datetime.datetime.fromisoformat(
                             '2019-09-07T00:09:18:770655'))
        self.assertEqual(parts.date_str, '2019-09-07')

        self.assertEqual(parts.is_file_split, False)
        self.assertEqual(parts.file_split_size, None)

        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                'bucket-us-mo/unprocessed_2019-09-07T00:09:18:770655_'
                'raw_tak001_offender_identification_002_file_split_size300.csv'))

        self.assertEqual(parts.processed_state, 'unprocessed')
        self.assertEqual(parts.extension, 'csv')
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.RAW_DATA)
        self.assertEqual(parts.file_tag, 'tak001_offender_identification')
        self.assertEqual(parts.filename_suffix, '002_file_split_size300')
        self.assertEqual(parts.utc_upload_datetime,
                         datetime.datetime.fromisoformat(
                             '2019-09-07T00:09:18:770655'))
        self.assertEqual(parts.date_str, '2019-09-07')

        self.assertEqual(parts.is_file_split, True)
        self.assertEqual(parts.file_split_size, 300)

        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                'storage_bucket/raw/2020/04/29/processed_2020-04-29T18:02:41:789323_raw_test_file-(1).csv')
        )

        self.assertEqual(parts.processed_state, 'processed')
        self.assertEqual(parts.extension, 'csv')
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.RAW_DATA)
        self.assertEqual(parts.file_tag, 'test_file')
        self.assertEqual(parts.filename_suffix, None)
        self.assertEqual(parts.utc_upload_datetime,
                         datetime.datetime.fromisoformat(
                             '2020-04-29T18:02:41:789323'))
        self.assertEqual(parts.date_str, '2020-04-29')

        self.assertEqual(parts.is_file_split, False)

        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                'bucket-us-mo/unprocessed_2019-09-07T00:09:18:770655_'
                'raw_tak001_offender_identification_002_file_split_size300-(5).csv'))

        self.assertEqual(parts.processed_state, 'unprocessed')
        self.assertEqual(parts.extension, 'csv')
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.RAW_DATA)
        self.assertEqual(parts.file_tag, 'tak001_offender_identification')
        self.assertEqual(parts.filename_suffix, '002_file_split_size300')
        self.assertEqual(parts.utc_upload_datetime,
                         datetime.datetime.fromisoformat(
                             '2019-09-07T00:09:18:770655'))
        self.assertEqual(parts.date_str, '2019-09-07')

        self.assertEqual(parts.is_file_split, True)
        self.assertEqual(parts.file_split_size, 300)

    def test_gcsfs_ingest_view_export_args(self) -> None:
        dt_lower = datetime.datetime(2019, 1, 22, 11, 22, 33, 444444)
        dt_upper = datetime.datetime(2019, 11, 22, 11, 22, 33, 444444)

        args = GcsfsIngestViewExportArgs(
            ingest_view_name='my_file_tag',
            upper_bound_datetime_prev=None,
            upper_bound_datetime_to_export=dt_upper
        )

        self.assertEqual('ingest_view_export_my_file_tag-None-2019_11_22_11_22_33_444444',
                         args.task_id_tag())

        args = GcsfsIngestViewExportArgs(
            ingest_view_name='my_file_tag',
            upper_bound_datetime_prev=dt_lower,
            upper_bound_datetime_to_export=dt_upper
        )

        self.assertEqual('ingest_view_export_my_file_tag-2019_01_22_11_22_33_444444-2019_11_22_11_22_33_444444',
                         args.task_id_tag())
