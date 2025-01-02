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

from freezegun import freeze_time

from recidiviz.aggregated_metrics.aggregated_metrics_view_builder import (
    AggregatedMetricsBigQueryViewBuilder,
    aggregated_metric_view_description,
)
from recidiviz.aggregated_metrics.metric_time_period_config import (
    MetricTimePeriodConfig,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AssignmentEventAggregatedMetric,
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
    MY_ANY_INCARCERATION_365,
    MY_AVG_DAILY_POPULATION,
    MY_AVG_DAILY_POPULATION_GENERAL_INCARCERATION,
    MY_AVG_LSIR_SCORE,
    MY_CONTACTS_COMPLETED_METRIC,
    MY_DAYS_TO_FIRST_INCARCERATION_100,
    MY_DRUG_SCREENS_METRIC,
    MY_EMPLOYER_CHANGES_365,
    MY_LOGINS_BY_PRIMARY_WORKFLOWS,
)
from recidiviz.utils.types import assert_type


@freeze_time("2024-12-15")
class TestAggregatedMetricViewDescription(unittest.TestCase):
    """Tests for aggregated_metric_view_description()"""

    def test_aggregated_metric_view_description__period_event(self) -> None:
        docstring = aggregated_metric_view_description(
            population_type=MetricPopulationType.SUPERVISION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
            metric_class=PeriodEventAggregatedMetric,
            metrics=[
                MY_DRUG_SCREENS_METRIC,
                MY_CONTACTS_COMPLETED_METRIC,
                MY_LOGINS_BY_PRIMARY_WORKFLOWS,
            ],
            time_period=MetricTimePeriodConfig.month_periods(lookback_months=12),
        )

        expected_docstring = """
Metrics for the supervision population calculated using
event observations across an entire analysis period, disaggregated by officer.

Contains metrics only for MONTH-length time periods with
the most recent period ending on 2024-12-01 and the
least recent period ending on 2024-01-01.

All end_dates are exclusive, i.e. the metric is for the range [start_date, end_date).

# Metrics
| Name                               | Column                           | Description                                           | Event observation type   |
|------------------------------------|----------------------------------|-------------------------------------------------------|--------------------------|
| My Drug Screens                    | my_drug_screens                  | Number of my drug screens                             | DRUG_SCREEN              |
| Contacts: Completed                | my_contacts_completed            | Number of completed contacts                          | SUPERVISION_CONTACT      |
| My Logins, Primary Workflows Users | my_logins_primary_workflows_user | Number of logins performed by primary Workflows users | WORKFLOWS_USER_LOGIN     |
"""
        self.assertEqual(expected_docstring, docstring)

    def test_aggregated_metric_view_description__period_span(self) -> None:
        docstring = aggregated_metric_view_description(
            population_type=MetricPopulationType.INCARCERATION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.FACILITY,
            metric_class=PeriodSpanAggregatedMetric,
            metrics=[
                MY_AVG_DAILY_POPULATION,
                MY_AVG_DAILY_POPULATION_GENERAL_INCARCERATION,
                MY_AVG_LSIR_SCORE,
            ],
            time_period=MetricTimePeriodConfig.month_periods(lookback_months=12),
        )

        expected_docstring = """
Metrics for the incarceration population calculated using
span observations across an entire analysis period, disaggregated by facility.

Contains metrics only for MONTH-length time periods with
the most recent period ending on 2024-12-01 and the
least recent period ending on 2024-01-01.

All end_dates are exclusive, i.e. the metric is for the range [start_date, end_date).

# Metrics
| Name                                         | Column                                  | Description                                                | Span observation type    |
|----------------------------------------------|-----------------------------------------|------------------------------------------------------------|--------------------------|
| My Average Population                        | my_avg_daily_population                 | My Average daily count of clients in the population        | COMPARTMENT_SESSION      |
| My Average Population: General Incarceration | my_avg_population_general_incarceration | My Average daily count of clients in general incarceration | COMPARTMENT_SESSION      |
| My Average LSI-R Score                       | my_avg_lsir_score                       | My Average daily LSI-R score of the population             | ASSESSMENT_SCORE_SESSION |
"""
        self.assertEqual(expected_docstring, docstring)

    def test_aggregated_metric_view_description__assignment_event(self) -> None:
        docstring = aggregated_metric_view_description(
            population_type=MetricPopulationType.SUPERVISION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
            metric_class=AssignmentEventAggregatedMetric,
            metrics=[
                MY_ANY_INCARCERATION_365,
                MY_DAYS_TO_FIRST_INCARCERATION_100,
                MY_EMPLOYER_CHANGES_365,
            ],
            time_period=MetricTimePeriodConfig.monthly_year_periods(lookback_months=12),
        )

        expected_docstring = """
Metrics for the supervision population calculated using
events over some window following assignment, for all assignments
during an analysis period, disaggregated by officer.

Contains metrics only for YEAR-length time periods with
the most recent period ending on 2024-12-01 and the
least recent period ending on 2024-12-01.

All end_dates are exclusive, i.e. the metric is for the range [start_date, end_date).

# Metrics
| Name                                                          | Column                             | Description                                                                                                                                                                                     | Event observation type   |
|---------------------------------------------------------------|------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------|
| My Any Incarceration Start Within 1 Year of Assignment        | my_any_incarceration_365           | My number of client assignments followed by an incarceration start within 1 year                                                                                                                | INCARCERATION_START      |
| My Days To First Incarceration Within 1 Year After Assignment | my_days_to_first_incarceration_100 | My sum of the number of days prior to first incarceration within 1 year following assignment, for all assignments during the analysis period. Only counts incarcerations following supervision. | INCARCERATION_START      |
| My Employer Changes Within 1 Year Of Assignment               | my_employer_changes_365            | My number of times client starts employment with a new employer within 1 year of assignment                                                                                                     | EMPLOYMENT_PERIOD_START  |
"""
        self.assertEqual(expected_docstring, docstring)

    # TODO(#35898): Add tests for AssignmentSpanAggregatedMetric


class TestAggregatedMetricsBigQueryViewBuilder(unittest.TestCase):
    """Tests for AggregatedMetricsBigQueryViewBuilder"""

    def test_builder__period_event(self) -> None:
        builder = AggregatedMetricsBigQueryViewBuilder(
            dataset_id="my_aggregated_metrics",
            population_type=MetricPopulationType.SUPERVISION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
            metric_class=PeriodEventAggregatedMetric,
            metrics=[
                MY_DRUG_SCREENS_METRIC,
                MY_CONTACTS_COMPLETED_METRIC,
                MY_LOGINS_BY_PRIMARY_WORKFLOWS,
            ],
            time_period=MetricTimePeriodConfig.month_periods(lookback_months=12),
        )

        self.assertEqual(
            "my_aggregated_metrics.supervision_officer_period_event_aggregated_metrics__last_12_months",
            builder.address.to_str(),
        )
        self.assertEqual(
            "my_aggregated_metrics.supervision_officer_period_event_aggregated_metrics__last_12_months_materialized",
            assert_type(builder.materialized_address, BigQueryAddress).to_str(),
        )

        self.assertTrue(MY_DRUG_SCREENS_METRIC.name in builder.description)
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
