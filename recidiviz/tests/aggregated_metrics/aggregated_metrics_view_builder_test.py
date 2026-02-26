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
"""Tests for aggregated_metrics_view_builder.py"""
import unittest

from recidiviz.aggregated_metrics.aggregated_metrics_view_builder import (
    AggregatedMetricsBigQueryViewBuilder,
    aggregated_metric_view_description,
)
from recidiviz.aggregated_metrics.metric_time_period_config import (
    MetricTimePeriodConfig,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AssignmentEventAggregatedMetric,
    AssignmentSpanAggregatedMetric,
    PeriodEventAggregatedMetric,
    PeriodSpanAggregatedMetric,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.tests.aggregated_metrics.fixture_aggregated_metrics import (
    MY_CONTACTS_COMPLETED_METRIC,
    MY_DRUG_SCREENS_METRIC,
    MY_LOGINS_BY_PRIMARY_WORKFLOWS,
    MY_TASK_COMPLETIONS,
)
from recidiviz.utils.types import assert_type


class TestAggregatedMetricViewDescription(unittest.TestCase):
    """Tests for aggregated_metric_view_description()"""

    def test_aggregated_metric_view_description__period_event(self) -> None:
        docstring = aggregated_metric_view_description(
            population_type=MetricPopulationType.SUPERVISION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL,
            metric_class=PeriodEventAggregatedMetric,
            time_period=MetricTimePeriodConfig.month_periods(lookback_months=12),
        )

        expected_docstring = """
Metrics for the supervision population calculated using
event observations across an entire analysis period, disaggregated by officer_or_previous_if_transitional.

Contains metrics only for: Monthly metric periods for the last 12 months.

All end_dates are exclusive, i.e. the metric is for the range [start_date, end_date).
"""
        self.assertEqual(expected_docstring, docstring)

    def test_aggregated_metric_view_description__period_span(self) -> None:
        docstring = aggregated_metric_view_description(
            population_type=MetricPopulationType.INCARCERATION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.FACILITY,
            metric_class=PeriodSpanAggregatedMetric,
            time_period=MetricTimePeriodConfig.month_periods(lookback_months=12),
        )

        expected_docstring = """
Metrics for the incarceration population calculated using
span observations across an entire analysis period, disaggregated by facility.

Contains metrics only for: Monthly metric periods for the last 12 months.

All end_dates are exclusive, i.e. the metric is for the range [start_date, end_date).
"""
        self.assertEqual(expected_docstring, docstring)

    def test_aggregated_metric_view_description__assignment_event(self) -> None:
        docstring = aggregated_metric_view_description(
            population_type=MetricPopulationType.SUPERVISION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL,
            metric_class=AssignmentEventAggregatedMetric,
            time_period=MetricTimePeriodConfig.year_periods_rolling_monthly(
                lookback_months=12
            ),
        )

        expected_docstring = """
Metrics for the supervision population calculated using
events over some window following assignment, for all assignments
during an analysis period, disaggregated by officer_or_previous_if_transitional.

Contains metrics only for: Year-long metric periods, ending (exclusive) on the first of every month, for the last 12 months.

All end_dates are exclusive, i.e. the metric is for the range [start_date, end_date).
"""
        self.assertEqual(expected_docstring, docstring)

    def test_aggregated_metric_view_description__assignment_span(self) -> None:
        docstring = aggregated_metric_view_description(
            population_type=MetricPopulationType.SUPERVISION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL,
            metric_class=AssignmentSpanAggregatedMetric,
            time_period=MetricTimePeriodConfig.year_periods_rolling_monthly(
                lookback_months=12
            ),
        )

        expected_docstring = """
Metrics for the supervision population calculated using
spans over some window following assignment, for all assignments
during an analysis period, disaggregated by officer_or_previous_if_transitional.

Contains metrics only for: Year-long metric periods, ending (exclusive) on the first of every month, for the last 12 months.

All end_dates are exclusive, i.e. the metric is for the range [start_date, end_date).
"""
        self.assertEqual(expected_docstring, docstring)


class TestAggregatedMetricsBigQueryViewBuilder(unittest.TestCase):
    """Tests for AggregatedMetricsBigQueryViewBuilder"""

    def test_builder__period_event(self) -> None:
        builder = AggregatedMetricsBigQueryViewBuilder(
            dataset_id="my_aggregated_metrics",
            population_type=MetricPopulationType.SUPERVISION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL,
            metric_class=PeriodEventAggregatedMetric,
            metrics=[
                MY_DRUG_SCREENS_METRIC,
                MY_CONTACTS_COMPLETED_METRIC,
                MY_LOGINS_BY_PRIMARY_WORKFLOWS,
            ],
            time_period=MetricTimePeriodConfig.month_periods(lookback_months=12),
            collection_tag=None,
            disaggregate_by_observation_attributes=None,
        )

        self.assertEqual(
            "my_aggregated_metrics.supervision_officer_or_previous_if_transitional_period_event_aggregated_metrics__last_12_months",
            builder.address.to_str(),
        )
        self.assertEqual(
            "my_aggregated_metrics.supervision_officer_or_previous_if_transitional_period_event_aggregated_metrics__last_12_months_materialized",
            assert_type(builder.materialized_address, BigQueryAddress).to_str(),
        )

        self.assertFalse(MY_DRUG_SCREENS_METRIC.name in builder.description)
        self.assertFalse(MY_DRUG_SCREENS_METRIC.name in builder.bq_description)

        self.assertEqual(
            [
                "state_code",
                "officer_id",
                "start_date",
                "end_date",
                "period",
                "my_contacts_completed",
                "my_drug_screens",
                "my_logins_primary_workflows_user",
            ],
            builder.output_columns,
        )

        col_info = [
            (c.name, c.field_type, c.mode, c.description) for c in builder.schema
        ]
        self.assertEqual(
            col_info,
            [
                ("state_code", "STRING", "REQUIRED", "Primary key: state_code"),
                ("officer_id", "STRING", "REQUIRED", "Primary key: officer_id"),
                (
                    "start_date",
                    "DATE",
                    "REQUIRED",
                    "Analysis period start date (inclusive)",
                ),
                (
                    "end_date",
                    "DATE",
                    "REQUIRED",
                    "Analysis period end date (exclusive)",
                ),
                (
                    "period",
                    "STRING",
                    "REQUIRED",
                    "A string descriptor for the analysis period length. One of: 'DAY', 'WEEK', 'MONTH', 'QUARTER', 'YEAR' or 'CUSTOM'.",
                ),
                (
                    "my_contacts_completed",
                    "INTEGER",
                    "NULLABLE",
                    "Contacts: Completed: Number of completed contacts (Observation Type: SUPERVISION_CONTACT)",
                ),
                (
                    "my_drug_screens",
                    "INTEGER",
                    "NULLABLE",
                    "My Drug Screens: Number of my drug screens (Observation Type: DRUG_SCREEN)",
                ),
                (
                    "my_logins_primary_workflows_user",
                    "INTEGER",
                    "NULLABLE",
                    "My Logins, Primary Workflows Users: Number of logins performed by primary Workflows users (Observation Type: WORKFLOWS_USER_LOGIN)",
                ),
            ],
        )

    def test_builder__period_event_with_collection_tag(self) -> None:
        builder = AggregatedMetricsBigQueryViewBuilder(
            dataset_id="my_aggregated_metrics",
            population_type=MetricPopulationType.SUPERVISION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL,
            metric_class=PeriodEventAggregatedMetric,
            metrics=[
                MY_DRUG_SCREENS_METRIC,
                MY_CONTACTS_COMPLETED_METRIC,
                MY_LOGINS_BY_PRIMARY_WORKFLOWS,
            ],
            time_period=MetricTimePeriodConfig.month_periods(lookback_months=12),
            collection_tag="my_tag",
            disaggregate_by_observation_attributes=None,
        )

        self.assertEqual(
            "my_aggregated_metrics.my_tag__supervision_officer_or_previous_if_transitional_period_event_aggregated_metrics__last_12_months",
            builder.address.to_str(),
        )
        self.assertEqual(
            "my_aggregated_metrics.my_tag__supervision_officer_or_previous_if_transitional_period_event_aggregated_metrics__last_12_months_materialized",
            assert_type(builder.materialized_address, BigQueryAddress).to_str(),
        )

        self.assertFalse(MY_DRUG_SCREENS_METRIC.name in builder.description)
        self.assertFalse(MY_DRUG_SCREENS_METRIC.name in builder.bq_description)

        self.assertEqual(
            [
                "state_code",
                "officer_id",
                "start_date",
                "end_date",
                "period",
                "my_contacts_completed",
                "my_drug_screens",
                "my_logins_primary_workflows_user",
            ],
            builder.output_columns,
        )

        col_info = [
            (c.name, c.field_type, c.mode, c.description) for c in builder.schema
        ]
        self.assertEqual(
            col_info,
            [
                ("state_code", "STRING", "REQUIRED", "Primary key: state_code"),
                ("officer_id", "STRING", "REQUIRED", "Primary key: officer_id"),
                (
                    "start_date",
                    "DATE",
                    "REQUIRED",
                    "Analysis period start date (inclusive)",
                ),
                (
                    "end_date",
                    "DATE",
                    "REQUIRED",
                    "Analysis period end date (exclusive)",
                ),
                (
                    "period",
                    "STRING",
                    "REQUIRED",
                    "A string descriptor for the analysis period length. One of: 'DAY', 'WEEK', 'MONTH', 'QUARTER', 'YEAR' or 'CUSTOM'.",
                ),
                (
                    "my_contacts_completed",
                    "INTEGER",
                    "NULLABLE",
                    "Contacts: Completed: Number of completed contacts (Observation Type: SUPERVISION_CONTACT)",
                ),
                (
                    "my_drug_screens",
                    "INTEGER",
                    "NULLABLE",
                    "My Drug Screens: Number of my drug screens (Observation Type: DRUG_SCREEN)",
                ),
                (
                    "my_logins_primary_workflows_user",
                    "INTEGER",
                    "NULLABLE",
                    "My Logins, Primary Workflows Users: Number of logins performed by primary Workflows users (Observation Type: WORKFLOWS_USER_LOGIN)",
                ),
            ],
        )

    def test_builder__period_event_with_disaggregation_attribute(self) -> None:
        builder = AggregatedMetricsBigQueryViewBuilder(
            dataset_id="my_aggregated_metrics",
            population_type=MetricPopulationType.JUSTICE_INVOLVED,
            unit_of_analysis_type=MetricUnitOfAnalysisType.WORKFLOWS_PROVISIONED_USER,
            metric_class=PeriodEventAggregatedMetric,
            metrics=[MY_LOGINS_BY_PRIMARY_WORKFLOWS],
            time_period=MetricTimePeriodConfig.month_periods(lookback_months=12),
            collection_tag=None,
            disaggregate_by_observation_attributes=["task_type"],
        )

        self.assertEqual(
            [
                "state_code",
                "email_address",
                "start_date",
                "end_date",
                "period",
                "task_type",
                "my_logins_primary_workflows_user",
            ],
            builder.output_columns,
        )

        col_info = [
            (c.name, c.field_type, c.mode, c.description) for c in builder.schema
        ]
        self.assertEqual(
            col_info,
            [
                ("state_code", "STRING", "REQUIRED", "Primary key: state_code"),
                (
                    "email_address",
                    "STRING",
                    "REQUIRED",
                    "Primary key: email_address",
                ),
                (
                    "start_date",
                    "DATE",
                    "REQUIRED",
                    "Analysis period start date (inclusive)",
                ),
                (
                    "end_date",
                    "DATE",
                    "REQUIRED",
                    "Analysis period end date (exclusive)",
                ),
                (
                    "period",
                    "STRING",
                    "REQUIRED",
                    "A string descriptor for the analysis period length. One of: 'DAY', 'WEEK', 'MONTH', 'QUARTER', 'YEAR' or 'CUSTOM'.",
                ),
                (
                    "task_type",
                    "STRING",
                    "NULLABLE",
                    "Disaggregation attribute: task_type",
                ),
                (
                    "my_logins_primary_workflows_user",
                    "INTEGER",
                    "NULLABLE",
                    "My Logins, Primary Workflows Users: Number of logins performed by primary Workflows users (Observation Type: WORKFLOWS_USER_LOGIN)",
                ),
            ],
        )

    def test_builder__primary_key_type_override(self) -> None:
        """Ensure that primary key columns with type overrides are correctly represented in the schema."""
        builder = AggregatedMetricsBigQueryViewBuilder(
            dataset_id="my_aggregated_metrics",
            population_type=MetricPopulationType.INCARCERATION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.FACILITY_COUNSELOR,
            metric_class=PeriodEventAggregatedMetric,
            metrics=[MY_TASK_COMPLETIONS],
            time_period=MetricTimePeriodConfig.month_periods(lookback_months=12),
            collection_tag=None,
            disaggregate_by_observation_attributes=None,
        )

        col_info = [
            (c.name, c.field_type, c.mode, c.description) for c in builder.schema
        ]
        self.assertEqual(
            col_info,
            [
                ("state_code", "STRING", "REQUIRED", "Primary key: state_code"),
                (
                    "facility_counselor_id",
                    "INTEGER",
                    "REQUIRED",
                    "Primary key: facility_counselor_id",
                ),
                (
                    "start_date",
                    "DATE",
                    "REQUIRED",
                    "Analysis period start date (inclusive)",
                ),
                (
                    "end_date",
                    "DATE",
                    "REQUIRED",
                    "Analysis period end date (exclusive)",
                ),
                (
                    "period",
                    "STRING",
                    "REQUIRED",
                    "A string descriptor for the analysis period length. One of: 'DAY', 'WEEK', 'MONTH', 'QUARTER', 'YEAR' or 'CUSTOM'.",
                ),
                (
                    "my_task_completions",
                    "INTEGER",
                    "NULLABLE",
                    "My Task Completions: My count of task completions (Observation Type: TASK_COMPLETED)",
                ),
            ],
        )
