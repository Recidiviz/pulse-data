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
"""Revocations Matrix Events by Month."""
# pylint: disable=trailing-whitespace
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REVOCATIONS_MATRIX_EVENTS_BY_MONTH_VIEW_NAME = "revocations_matrix_events_by_month"

REVOCATIONS_MATRIX_EVENTS_BY_MONTH_DESCRIPTION = """
Monthly event-based counts of revocations of supervision, broken down by number of violations leading up to the
revocation, the most severe violation type.
"""

REVOCATIONS_MATRIX_EVENTS_BY_MONTH_QUERY_TEMPLATE = """
    /*{description}*/
    WITH revocations AS (
        SELECT
            state_code,
            year,
            month,
            admission_type,
            {most_severe_violation_type_subtype_grouping},
            -- The revocations by violation type dashboard chart maxes out at 8+
            IF(response_count > 8, 8, response_count) as reported_violations,
            supervision_type,
            {state_specific_supervision_level},
            case_type,
            level_1_supervision_location,
            level_2_supervision_location,
        FROM `{project_id}.{reference_views_dataset}.event_based_commitments_from_supervision_for_matrix_materialized`
        -- We want MoM commitments from supervision for the last 36 months
        WHERE admission_date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE('US/Pacific'), MONTH), INTERVAL 35 MONTH)
            -- Filter out any rows that don't have a specified violation_type
            AND {state_specific_supervision_type_inclusion_filter}
            
    )
    SELECT
        state_code,
        year,
        month,
        admission_type,
        violation_type,
        reported_violations,
        supervision_type,
        supervision_level,
        charge_category,
        level_1_supervision_location,
        level_2_supervision_location,
        COUNT(*) AS total_revocations
    FROM revocations,
    {admission_type_dimension},
    {charge_category_dimension},
    {level_1_supervision_location_dimension},
    {level_2_supervision_location_dimension},
    {supervision_level_dimension},
    {supervision_type_dimension},
    {reported_violations_dimension},
    {violation_type_dimension}  
    WHERE supervision_type IN ('ALL', 'DUAL', 'PAROLE', 'PROBATION')
        AND {state_specific_supervision_location_optimization_filter}
        AND {state_specific_dimension_filter}
        AND (reported_violations = 'ALL' OR violation_type != 'ALL')      
    GROUP BY state_code, year, month, violation_type, reported_violations, supervision_type, supervision_level,
        charge_category, level_1_supervision_location, level_2_supervision_location, admission_type
    """

REVOCATIONS_MATRIX_EVENTS_BY_MONTH_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=REVOCATIONS_MATRIX_EVENTS_BY_MONTH_VIEW_NAME,
    view_query_template=REVOCATIONS_MATRIX_EVENTS_BY_MONTH_QUERY_TEMPLATE,
    admission_type_dimension=bq_utils.unnest_column("admission_type", "admission_type"),
    charge_category_dimension=bq_utils.unnest_charge_category(),
    level_1_supervision_location_dimension=bq_utils.unnest_column(
        "level_1_supervision_location", "level_1_supervision_location"
    ),
    level_2_supervision_location_dimension=bq_utils.unnest_column(
        "level_2_supervision_location", "level_2_supervision_location"
    ),
    supervision_type_dimension=bq_utils.unnest_supervision_type(),
    supervision_level_dimension=bq_utils.unnest_column(
        "supervision_level", "supervision_level"
    ),
    reported_violations_dimension=bq_utils.unnest_reported_violations(),
    violation_type_dimension=bq_utils.unnest_column("violation_type", "violation_type"),
    most_severe_violation_type_subtype_grouping=state_specific_query_strings.state_specific_most_severe_violation_type_subtype_grouping(),
    state_specific_dimension_filter=state_specific_query_strings.state_specific_dimension_filter(
        filter_admission_type=True
    ),
    state_specific_supervision_level=state_specific_query_strings.state_specific_supervision_level(),
    state_specific_supervision_location_optimization_filter=state_specific_query_strings.state_specific_supervision_location_optimization_filter(),
    state_specific_supervision_type_inclusion_filter=state_specific_query_strings.state_specific_supervision_type_inclusion_filter(),
    dimensions=(
        "state_code",
        "year",
        "month",
        "level_1_supervision_location",
        "level_2_supervision_location",
        "admission_type",
        "supervision_type",
        "supervision_level",
        "violation_type",
        "reported_violations",
        "charge_category",
    ),
    description=REVOCATIONS_MATRIX_EVENTS_BY_MONTH_DESCRIPTION,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATIONS_MATRIX_EVENTS_BY_MONTH_VIEW_BUILDER.build_and_print()
