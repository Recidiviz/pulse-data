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
"""Tests for direct_ingest_bucket_name_utils.py."""
from unittest import TestCase

from recidiviz.cloud_functions.direct_ingest_bucket_name_utils import (
    get_region_code_from_direct_ingest_bucket,
    is_secondary_ingest_bucket,
    is_primary_ingest_bucket,
)


class DirectIngestBucketNameUtilsTest(TestCase):
    """Tests for direct_ingest_bucket_name_utils.py."""

    def test_get_region_code_from_direct_ingest_bucket_primary(self) -> None:
        self.assertEqual(
            get_region_code_from_direct_ingest_bucket(
                "recidiviz-123-direct-ingest-state-us-nd"
            ),
            "us_nd",
        )
        self.assertEqual(
            get_region_code_from_direct_ingest_bucket(
                "recidiviz-staging-direct-ingest-state-us-pa"
            ),
            "us_pa",
        )

        self.assertEqual(
            get_region_code_from_direct_ingest_bucket(
                "recidiviz-staging-direct-ingest-county-us-ma-middlesex"
            ),
            "us_ma_middlesex",
        )

        # Unknown still produces region code
        self.assertEqual(
            get_region_code_from_direct_ingest_bucket(
                "recidiviz-newproject-direct-ingest-state-us-ca"
            ),
            "us_ca",
        )

    def test_get_region_code_from_direct_ingest_bucket_secondary(self) -> None:
        self.assertEqual(
            get_region_code_from_direct_ingest_bucket(
                "recidiviz-123-direct-ingest-state-us-nd-secondary"
            ),
            "us_nd",
        )
        self.assertEqual(
            get_region_code_from_direct_ingest_bucket(
                "recidiviz-staging-direct-ingest-state-us-pa-secondary"
            ),
            "us_pa",
        )
        self.assertEqual(
            get_region_code_from_direct_ingest_bucket(
                "recidiviz-staging-direct-ingest-county-us-ma-middlesex-secondary"
            ),
            "us_ma_middlesex",
        )

    def test_get_region_code_from_direct_ingest_bucket_upload_testing(self) -> None:
        self.assertEqual(
            get_region_code_from_direct_ingest_bucket(
                "recidiviz-staging-direct-ingest-state-us-pa-upload-testing"
            ),
            "us_pa",
        )

        self.assertEqual(
            get_region_code_from_direct_ingest_bucket(
                "recidiviz-staging-direct-ingest-county-us-ma-middlesex-upload-testing"
            ),
            "us_ma_middlesex",
        )

    def test_get_region_code_from_direct_ingest_bucket_malformed(self) -> None:
        # States should not have three part region names
        self.assertEqual(
            get_region_code_from_direct_ingest_bucket(
                "recidiviz-staging-direct-ingest-state-us-ma-middlesex"
            ),
            None,
        )

        # Missing region type!
        self.assertEqual(
            get_region_code_from_direct_ingest_bucket(
                "recidiviz-staging-direct-ingest-us-nd"
            ),
            None,
        )

    def test_is_primary_ingest_bucket(self) -> None:
        self.assertTrue(
            is_primary_ingest_bucket("recidiviz-staging-direct-ingest-state-us-pa"),
        )
        self.assertTrue(
            is_primary_ingest_bucket(
                "recidiviz-staging-direct-ingest-county-us-ma-middlesex"
            ),
        )

        self.assertFalse(
            is_primary_ingest_bucket(
                "recidiviz-staging-direct-ingest-state-us-pa-upload-testing"
            ),
        )

        self.assertFalse(
            is_primary_ingest_bucket(
                "recidiviz-staging-direct-ingest-state-us-pa-secondary"
            ),
        )
        self.assertFalse(
            is_primary_ingest_bucket(
                "recidiviz-staging-direct-ingest-county-us-ma-middlesex-secondary"
            ),
        )

    def test_is_secondary_ingest_bucket(self) -> None:
        self.assertFalse(
            is_secondary_ingest_bucket("recidiviz-staging-direct-ingest-state-us-pa"),
        )
        self.assertFalse(
            is_secondary_ingest_bucket(
                "recidiviz-staging-direct-ingest-state-us-pa-upload-testing"
            ),
        )
        self.assertFalse(
            is_secondary_ingest_bucket(
                "recidiviz-staging-direct-ingest-county-us-ma-middlesex"
            ),
        )

        self.assertTrue(
            is_secondary_ingest_bucket(
                "recidiviz-staging-direct-ingest-state-us-pa-secondary"
            ),
        )
        self.assertTrue(
            is_secondary_ingest_bucket(
                "recidiviz-staging-direct-ingest-county-us-ma-middlesex-secondary"
            ),
        )
