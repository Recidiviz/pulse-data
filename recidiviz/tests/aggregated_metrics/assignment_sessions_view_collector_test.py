# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for assignment_sessions_view_collector.py"""
import unittest

from recidiviz.aggregated_metrics.assignment_sessions_view_builder import (
    metric_assignment_sessions_schema,
)
from recidiviz.aggregated_metrics.assignment_sessions_view_collector import (
    collect_assignment_sessions_view_builders,
    get_standard_population_selector_for_unit_of_observation,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


class AssignmentSessionsViewCollectorTest(unittest.TestCase):
    """Tests for assignment_sessions_view_collector.py"""

    def test_get_standard_population_selector_for_unit_of_observation(self) -> None:
        for population_type in MetricPopulationType:
            if population_type is MetricPopulationType.CUSTOM:
                continue
            for unit_of_observation_type in MetricUnitOfObservationType:
                span_selector = (
                    get_standard_population_selector_for_unit_of_observation(
                        population_type=population_type,
                        unit_of_observation_type=unit_of_observation_type,
                    )
                )
                if not span_selector:
                    continue

                self.assertEqual(
                    unit_of_observation_type,
                    span_selector.unit_of_observation_type,
                    f"Population selector for ({population_type}, "
                    f"{unit_of_observation_type}) has spans with conflicting unit of "
                    f"observation {span_selector.unit_of_observation_type}",
                )

    def test_collect_view_builders(self) -> None:
        view_builders = collect_assignment_sessions_view_builders()
        seen_addresses = set()
        for view_builder in view_builders:
            if view_builder.address in seen_addresses:
                raise ValueError(
                    f"More than one metric assignment sessions view builder generated "
                    f"with address [{view_builder.address}]"
                )
            seen_addresses.add(view_builder.address)

    def test_all_view_builders_have_schema(self) -> None:
        with local_project_id_override(GCP_PROJECT_STAGING):
            for view_builder in collect_assignment_sessions_view_builders():
                view = view_builder.build()
                self.assertIsNotNone(
                    view.schema,
                    f"Expected schema to be set for assignment sessions view "
                    f"[{view_builder.address}]",
                )

    def test_metric_assignment_sessions_schema_anchor(self) -> None:
        schema = metric_assignment_sessions_schema(
            unit_of_observation_type=MetricUnitOfObservationType.PERSON_ID,
            unit_of_analysis_type=MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
        )
        col_info = [(c.name, c.field_type.value, c.mode) for c in schema]
        self.assertEqual(
            col_info,
            [
                ("person_id", "INTEGER", "NULLABLE"),
                ("state_code", "STRING", "NULLABLE"),
                ("district", "STRING", "REQUIRED"),
                ("office", "STRING", "REQUIRED"),
                ("assignment_date", "DATE", "REQUIRED"),
                ("end_date", "DATE", "NULLABLE"),
                ("end_date_exclusive", "DATE", "NULLABLE"),
                ("assignment_is_first_day_in_population", "BOOLEAN", "REQUIRED"),
            ],
        )
