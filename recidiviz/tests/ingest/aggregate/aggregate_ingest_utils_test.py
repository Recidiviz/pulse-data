# Recidiviz - a platform for tracking granular recidivism metrics in real time
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
"""Tests for aggregate_ingest_utils."""
from unittest import TestCase

import pandas as pd
from pandas.util.testing import assert_frame_equal

from recidiviz.ingest.aggregate import aggregate_ingest_utils


class TestAggregateIngestUtils(TestCase):
    """Tests for aggregate_ingest_utils."""

    def testRenameWithoutRegex(self):
        # Arrange
        subject = pd.DataFrame({
            'County': ['Anderson', 'Andrews', 'Angelina'],
            'PRETRIAL': [90, 20, 105],
            'CON. Felons': [2, 11, 26],
        })

        rename_dict = {
            'County': 'facility_name',
            'PRETRIAL': 'pretrial_adp',
            'CON. Felons': 'convicted_adp'
        }

        # Act
        result = aggregate_ingest_utils.rename_columns_and_select(
            subject, rename_dict)

        # Assert
        expected_result = pd.DataFrame({
            'facility_name': ['Anderson', 'Andrews', 'Angelina'],
            'pretrial_adp': [90, 20, 105],
            'convicted_adp': [2, 11, 26],
        })

        assert_frame_equal(result, expected_result)

    def testRenameWithRegex(self):
        # Arrange
        subject = pd.DataFrame({
            'County': ['Anderson', 'Andrews', 'Angelina'],
            'PRETRIAL': [90, 20, 105],
            'CON. Felons': [2, 11, 26],
        })

        rename_dict = {
            r'.*Cou.*': 'facility_name',
            r'PRETRIAL': 'pretrial_adp',
            r'CON\. Felons': 'convicted_adp'
        }

        # Act
        result = aggregate_ingest_utils.rename_columns_and_select(
            subject, rename_dict, use_regex=True)

        # Assert
        expected_result = pd.DataFrame({
            'facility_name': ['Anderson', 'Andrews', 'Angelina'],
            'pretrial_adp': [90, 20, 105],
            'convicted_adp': [2, 11, 26],
        })

        assert_frame_equal(result, expected_result)
