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
from mock import Mock, patch

from recidiviz.common.constants.states import StateCode
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

    def setUp(self) -> None:
        super().setUp()
        self.region_code = StateCode.US_ID.value
        self.data_types = str

    def test_template_periods_incarceration_only_movement(self) -> None:
        self.view_builder = DirectIngestPreProcessedIngestViewBuilder(
            region=self.region_code,
            ingest_view_name="templates_periods_incarceration",
            view_query_template=f"""
                    WITH {get_all_periods_query_fragment(period_type=PeriodType.INCARCERATION)}
                    SELECT * FROM periods_with_previous_and_next_info
                    """,
            order_by_cols="docno, incrno, start_date, end_date",
        )
        self.run_ingest_view_test(
            fixtures_files_name="templates_periods_incarceration_only_movement.csv"
        )

    def test_supervision_same_day_release(self) -> None:
        # This tests a movement to parole and then history on the same day, where the
        # move_srl of the history movement is earlier than the parole.
        self.view_builder = DirectIngestPreProcessedIngestViewBuilder(
            region="us_id",
            ingest_view_name="templates_periods_supervision",
            view_query_template=f"""
            WITH {get_all_periods_query_fragment(period_type=PeriodType.SUPERVISION)}
            SELECT * FROM periods_with_previous_and_next_info
            """,
            order_by_cols="docno, incrno, start_date, end_date",
        )
        self.run_ingest_view_test(
            fixtures_files_name="templates_periods_supervision_same_day_release.csv"
        )
