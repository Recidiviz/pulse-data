# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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
"""Tests for fl_aggregate_ingest.py."""
from datetime import datetime
from unittest import TestCase

import pandas as pd
from pandas.util.testing import assert_frame_equal

from recidiviz.ingest.aggregate.regions.fl import fl_aggregate_ingest
from recidiviz.tests.ingest import fixtures

# Cache the parsed pdf between tests since it's expensive to compute
PARSED_PDF = fl_aggregate_ingest.parse(
    fixtures.as_filepath('jails-2018-01.pdf'))


class TestAggregateIngest(TestCase):
    """Test that fl_aggregate_ingest correctly parses the FL PDF."""

    def testParseCountyAdp_parsesHeadAndTail(self):
        result = PARSED_PDF['fl_county_adp']

        # Assert Head
        expected_head = pd.DataFrame({
            'County': ['Alachua', 'Baker', 'Bay', 'Bradford', 'Brevard'],
            'County Population': [257062, 26965, 176016, 27440, 568919],
            'Average Daily Population': [799, 478, 1015, 141, 1547],
            'Date Reported': [pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT]
        })
        assert_frame_equal(result.head(), expected_head)

        # Assert Tail
        expected_tail = pd.DataFrame({
            'County': ['Union', 'Volusia', 'Wakulla', 'Walton', 'Washington'],
            'County Population': [15887, 517411, 31599, 62943, 24888],
            'Average Daily Population': [38, 1413, 174, 293, 110],
            'Date Reported': [pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT]
        }, index=range(62, 67))
        assert_frame_equal(result.tail(), expected_tail)

    def testParseCountyAdp_parsesDateReported(self):
        result = PARSED_PDF['fl_county_adp']

        # Specifically verify Row 43 since it has 'Date Reported' set
        expected_row_43 = pd.DataFrame({
            'County': ['Monroe'],
            'County Population': [76047],
            'Average Daily Population': [545],
            'Date Reported': [datetime(day=1, month=9, year=2017)]
        }, index=[43])

        result_row_43 = result.iloc[43:44]
        assert_frame_equal(result_row_43, expected_row_43)
