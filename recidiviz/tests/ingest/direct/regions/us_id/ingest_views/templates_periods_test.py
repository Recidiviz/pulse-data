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
"""Tests the template period functionality"""

from datetime import date

import pandas as pd
from mock import Mock, patch
from pandas.testing import assert_frame_equal

from recidiviz.ingest.direct.query_utils import get_region_raw_file_config
from recidiviz.ingest.direct.regions.us_id.ingest_views.templates_periods import (
    PeriodType,
    get_all_periods_query_fragment,
)
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.tests.big_query.view_test_util import BaseViewTest


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="t"))
class TemplatesPeriodsTest(BaseViewTest):
    """Tests the template period functionality"""

    # TODO(#7250): Expand these tests, and load the input tables and expected results
    # from fixture csvs instead of hardcoding inline.
    def test_template_periods_incarceration_only_movement(self) -> None:
        # Arrange
        raw_file_configs = get_region_raw_file_config("us_id").raw_file_configs

        self.create_mock_raw_file("us_id", raw_file_configs["casemgr"], [])
        self.create_mock_raw_file("us_id", raw_file_configs["employee"], [])
        self.create_mock_raw_file("us_id", raw_file_configs["facility"], [])
        self.create_mock_raw_file("us_id", raw_file_configs["location"], [])
        self.create_mock_raw_file("us_id", raw_file_configs["lvgunit"], [])
        self.create_mock_raw_file("us_id", raw_file_configs["mittimus"], [])
        self.create_mock_raw_file(
            "us_id",
            raw_file_configs["movement"],
            [
                (
                    "10000001",
                    "11111",
                    "1",
                    "2020-01-01 8:00:00",
                    "I",
                    "1",
                    "",
                    "001",
                    "00",
                    "1",
                    "1",
                    "A",
                    None,
                    None,
                )
            ],
        )
        self.create_mock_raw_file("us_id", raw_file_configs["offstat"], [])
        self.create_mock_raw_file("us_id", raw_file_configs["ofndr_loc_hist"], [])
        self.create_mock_raw_file("us_id", raw_file_configs["ofndr_wrkld"], [])
        self.create_mock_raw_file("us_id", raw_file_configs["wrkld_cat"], [])

        # Act
        dimensions = ["docno", "incrno", "start_date", "end_date"]
        results = self.query_raw_data_view_for_builder(
            DirectIngestPreProcessedIngestViewBuilder(
                region="us_id",
                ingest_view_name="incarceration_periods",
                view_query_template=f"""
            WITH {get_all_periods_query_fragment(period_type=PeriodType.INCARCERATION)}
            SELECT * FROM periods_with_previous_and_next_info
            """,
                order_by_cols="docno, incrno, start_date, end_date",
            ),
            dimensions=dimensions,
        )

        # Assert
        expected = pd.DataFrame(
            [
                [
                    "11111",
                    "1",
                    date(2020, 1, 1),
                    date(9999, 12, 31),
                    None,
                    None,
                    None,
                    "1",
                ]
                + [None] * 6
                + ["10000001"]
                + [None] * 9
            ],
            columns=dimensions
            + [
                "prev_fac_typ",
                "prev_fac_cd",
                "prev_loc_ldesc",
                "fac_cd",
                "fac_typ",
                "fac_ldesc",
                "loc_cd",
                "loc_ldesc",
                "lu_cd",
                "lu_ldesc",
                "move_srl",
                "statuses",
                "wrkld_cat_title",
                "empl_cd",
                "empl_sdesc",
                "empl_ldesc",
                "empl_title",
                "next_fac_typ",
                "next_fac_cd",
                "next_loc_ldesc",
            ],
        )
        expected = expected.set_index(dimensions)
        assert_frame_equal(expected, results)
