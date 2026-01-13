# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests for functionality in task_eligibility_spans.py"""
import unittest

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.task_eligibility.task_eligibility_spans import get_unioned_view_builders


class TestGetUnionedViewBuilders(unittest.TestCase):
    """Tests for the get_unioned_view_builders() function in task_eligibility_spans.py."""

    def test_get_unioned_view_builders(self) -> None:
        builders = get_unioned_view_builders()

        expected_all_tasks_view_addresses = [
            BigQueryAddress(dataset_id="task_eligibility", table_id="all_criteria"),
            BigQueryAddress(
                dataset_id="task_eligibility_criteria_general",
                table_id="all_general_criteria",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility", table_id="all_candidate_populations"
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_candidates_general",
                table_id="all_general_candidate_populations",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_candidates_us_mo",
                table_id="all_state_specific_candidate_populations",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_candidates_us_tx",
                table_id="all_state_specific_candidate_populations",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_candidates_us_az",
                table_id="all_state_specific_candidate_populations",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_candidates_us_ix",
                table_id="all_state_specific_candidate_populations",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility",
                table_id="all_completion_events",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_completion_events_general",
                table_id="all_general_completion_events",
            ),
            BigQueryAddress(dataset_id="task_eligibility", table_id="all_tasks"),
            BigQueryAddress(
                dataset_id="task_eligibility", table_id="all_tasks__collapsed"
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_criteria_us_ca",
                table_id="all_state_specific_criteria",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_criteria_us_ia",
                table_id="all_state_specific_criteria",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_criteria_us_ix",
                table_id="all_state_specific_criteria",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_criteria_us_nd",
                table_id="all_state_specific_criteria",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_criteria_us_tn",
                table_id="all_state_specific_criteria",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_criteria_us_me",
                table_id="all_state_specific_criteria",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_criteria_us_mi",
                table_id="all_state_specific_criteria",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_criteria_us_mo",
                table_id="all_state_specific_criteria",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_criteria_us_pa",
                table_id="all_state_specific_criteria",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_criteria_us_ar",
                table_id="all_state_specific_criteria",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_criteria_us_tx",
                table_id="all_state_specific_criteria",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_criteria_us_az",
                table_id="all_state_specific_criteria",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_criteria_us_ut",
                table_id="all_state_specific_criteria",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_criteria_us_ne",
                table_id="all_state_specific_criteria",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_criteria_us_nc",
                table_id="all_state_specific_criteria",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_completion_events_us_ar",
                table_id="all_state_specific_completion_events",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_completion_events_us_az",
                table_id="all_state_specific_completion_events",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_completion_events_us_nd",
                table_id="all_state_specific_completion_events",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_completion_events_us_ne",
                table_id="all_state_specific_completion_events",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_completion_events_us_mi",
                table_id="all_state_specific_completion_events",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_completion_events_us_mo",
                table_id="all_state_specific_completion_events",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_completion_events_us_tn",
                table_id="all_state_specific_completion_events",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_completion_events_us_ix",
                table_id="all_state_specific_completion_events",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_completion_events_us_me",
                table_id="all_state_specific_completion_events",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_completion_events_us_pa",
                table_id="all_state_specific_completion_events",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_completion_events_us_nc",
                table_id="all_state_specific_completion_events",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_ca", table_id="all_tasks"
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_ca",
                table_id="all_tasks__collapsed",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_co", table_id="all_tasks"
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_co",
                table_id="all_tasks__collapsed",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_ia", table_id="all_tasks"
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_ia",
                table_id="all_tasks__collapsed",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_completion_events_us_ia",
                table_id="all_state_specific_completion_events",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_ix", table_id="all_tasks"
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_ix",
                table_id="all_tasks__collapsed",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_nd", table_id="all_tasks"
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_nd",
                table_id="all_tasks__collapsed",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_tn", table_id="all_tasks"
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_tn",
                table_id="all_tasks__collapsed",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_me", table_id="all_tasks"
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_me",
                table_id="all_tasks__collapsed",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_mi", table_id="all_tasks"
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_mi",
                table_id="all_tasks__collapsed",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_mo", table_id="all_tasks"
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_mo",
                table_id="all_tasks__collapsed",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_pa", table_id="all_tasks"
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_pa",
                table_id="all_tasks__collapsed",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_ar", table_id="all_tasks"
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_ar",
                table_id="all_tasks__collapsed",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_az", table_id="all_tasks"
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_az",
                table_id="all_tasks__collapsed",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_ut", table_id="all_tasks"
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_ut",
                table_id="all_tasks__collapsed",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_tx", table_id="all_tasks"
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_tx",
                table_id="all_tasks__collapsed",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_ne", table_id="all_tasks"
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_ne",
                table_id="all_tasks__collapsed",
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_nc", table_id="all_tasks"
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_nc",
                table_id="all_tasks__collapsed",
            ),
            # ... add addresses here as tasks for new states are added ...
        ]
        self.maxDiff = None
        self.assertCountEqual(
            expected_all_tasks_view_addresses, [b.address for b in builders]
        )
