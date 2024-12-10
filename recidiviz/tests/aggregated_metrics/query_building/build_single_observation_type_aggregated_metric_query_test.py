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
"""Tests for build_single_observation_type_aggregated_metric_query.py"""
import unittest

from recidiviz.aggregated_metrics.models.aggregated_metric import (
    EventCountMetric,
    PeriodEventAggregatedMetric,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.aggregated_metrics.query_building.build_single_observation_type_aggregated_metric_query import (
    build_single_observation_type_aggregated_metric_query_template,
)
from recidiviz.observations.event_selector import EventSelector
from recidiviz.observations.event_type import EventType

MY_DRUG_SCREENS_METRIC = EventCountMetric(
    name="my_drug_screens",
    display_name="My Drug Screens",
    description="Number of my drug screens",
    event_selector=EventSelector(
        event_type=EventType.DRUG_SCREEN,
        event_conditions_dict={},
    ),
)

MY_CONTACTS_ATTEMPTED_METRIC = EventCountMetric(
    name="my_contacts_attempted",
    display_name="Contacts: Attempted",
    description="Number of attempted contacts",
    event_selector=EventSelector(
        event_type=EventType.SUPERVISION_CONTACT,
        event_conditions_dict={"status": ["ATTEMPTED"]},
    ),
)

MY_CONTACTS_COMPLETED_METRIC = EventCountMetric(
    name="my_contacts_completed",
    display_name="Contacts: Completed",
    description="Number of completed contacts",
    event_selector=EventSelector(
        event_type=EventType.SUPERVISION_CONTACT,
        event_conditions_dict={"status": ["COMPLETED"]},
    ),
)


class TestBuildSingleObservationTypeAggregatedMetricQueryTemplate(unittest.TestCase):
    """Tests for build_single_observation_type_aggregated_metric_query_template()"""

    def test_build_period_event_metric__single(self) -> None:
        result = build_single_observation_type_aggregated_metric_query_template(
            observation_type=EventType.DRUG_SCREEN,
            unit_of_analysis_type=MetricUnitOfAnalysisType.FACILITY,
            metric_class=PeriodEventAggregatedMetric,
            single_observation_type_metrics=[
                MY_DRUG_SCREENS_METRIC,
            ],
            assignments_by_time_period_cte_name="person_assignments_by_time_period",
        )

        expected_result = """
WITH
observations AS (
    SELECT
        person_id,
        state_code,
        event_date
    FROM 
        `{project_id}.observations__person_event.drug_screen_materialized`
    WHERE
        TRUE
)
SELECT
    person_assignments_by_time_period.state_code,
    person_assignments_by_time_period.facility,
    person_assignments_by_time_period.metric_period_start_date,
    person_assignments_by_time_period.metric_period_end_date_exclusive,
    person_assignments_by_time_period.period,
    COUNT(DISTINCT IF(
        (TRUE),
        CONCAT(
            observations.person_id, observations.state_code, 
            observations.event_date
        ), NULL
    )) AS my_drug_screens
FROM 
    person_assignments_by_time_period
JOIN 
    observations
ON
    observations.person_id = person_assignments_by_time_period.person_id
    AND observations.state_code = person_assignments_by_time_period.state_code
    -- Include events occurring on the last date of an end-date exclusive span,
    -- but exclude events occurring on the last date of an end-date exclusive 
    -- analysis period.
    AND observations.event_date >= person_assignments_by_time_period.event_applies_to_period_start_date
    AND observations.event_date <  person_assignments_by_time_period.event_applies_to_period_end_date_exclusive_nonnull
GROUP BY state_code, facility, metric_period_start_date, metric_period_end_date_exclusive, period
"""

        self.assertEqual(expected_result, result)

    def test_build_period_event_metric__multiple(self) -> None:
        result = build_single_observation_type_aggregated_metric_query_template(
            observation_type=EventType.SUPERVISION_CONTACT,
            unit_of_analysis_type=MetricUnitOfAnalysisType.SUPERVISION_UNIT,
            metric_class=PeriodEventAggregatedMetric,
            single_observation_type_metrics=[
                MY_CONTACTS_ATTEMPTED_METRIC,
                MY_CONTACTS_COMPLETED_METRIC,
            ],
            assignments_by_time_period_cte_name="person_assignments_by_time_period",
        )

        expected_result = """
WITH
observations AS (
    SELECT
        person_id,
        state_code,
        event_date,
        status
    FROM 
        `{project_id}.observations__person_event.supervision_contact_materialized`
    WHERE
        ( status IN ("ATTEMPTED") )
        OR ( status IN ("COMPLETED") )
)
SELECT
    person_assignments_by_time_period.state_code,
    person_assignments_by_time_period.unit_supervisor,
    person_assignments_by_time_period.metric_period_start_date,
    person_assignments_by_time_period.metric_period_end_date_exclusive,
    person_assignments_by_time_period.period,
    COUNT(DISTINCT IF(
        (status IN ("ATTEMPTED")),
        CONCAT(
            observations.person_id, observations.state_code, 
            observations.event_date
        ), NULL
    )) AS my_contacts_attempted,
    COUNT(DISTINCT IF(
        (status IN ("COMPLETED")),
        CONCAT(
            observations.person_id, observations.state_code, 
            observations.event_date
        ), NULL
    )) AS my_contacts_completed
FROM 
    person_assignments_by_time_period
JOIN 
    observations
ON
    observations.person_id = person_assignments_by_time_period.person_id
    AND observations.state_code = person_assignments_by_time_period.state_code
    -- Include events occurring on the last date of an end-date exclusive span,
    -- but exclude events occurring on the last date of an end-date exclusive 
    -- analysis period.
    AND observations.event_date >= person_assignments_by_time_period.event_applies_to_period_start_date
    AND observations.event_date <  person_assignments_by_time_period.event_applies_to_period_end_date_exclusive_nonnull
GROUP BY state_code, unit_supervisor, metric_period_start_date, metric_period_end_date_exclusive, period
"""

        self.assertEqual(expected_result, result)

    # TODO(#29291): Add tests for PeriodSpanAggregatedMetric
    # TODO(#29291): Add tests for AssignmentEventAggregatedMetric
    # TODO(#29291): Add tests for AssignmentSpanAggregatedMetric
