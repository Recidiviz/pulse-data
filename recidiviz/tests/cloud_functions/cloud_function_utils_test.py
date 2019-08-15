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
"""Tests for cloud_function_utils.py."""
from unittest import TestCase

from recidiviz.cloud_functions.cloud_function_utils import \
    get_state_region_code_from_direct_ingest_bucket


class CloudFunctionUtilsTest(TestCase):
    """Tests for cloud_function_utils.py."""
    def test_get_state_region_code_from_bucket(self) -> None:
        self.assertEqual(
            get_state_region_code_from_direct_ingest_bucket(
                'recidiviz-123-direct-ingest-state-us-nd'),
            'us_nd')
        self.assertEqual(
            get_state_region_code_from_direct_ingest_bucket(
                'recidiviz-staging-direct-ingest-state-us-pa'),
            'us_pa')

        # Not a state!
        self.assertEqual(
            get_state_region_code_from_direct_ingest_bucket(
                'recidiviz-staging-direct-ingest-county-us-ma-middlesex'),
            None)
        self.assertEqual(
            get_state_region_code_from_direct_ingest_bucket(
                'recidiviz-staging-direct-ingest-state-us-ma-middlesex'),
            None)

        # Unknown project!
        self.assertEqual(
            get_state_region_code_from_direct_ingest_bucket(
                'recidiviz-newproject-direct-ingest-state-us-ca'),
            None)

        # Missing region type!
        self.assertEqual(
            get_state_region_code_from_direct_ingest_bucket(
                'recidiviz-staging-direct-ingest-us-nd'),
            None)
