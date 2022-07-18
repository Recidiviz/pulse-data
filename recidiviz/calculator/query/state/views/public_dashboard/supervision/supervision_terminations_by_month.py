# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Rates of successful supervision terminations by month."""
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_TERMINATIONS_BY_MONTH_VIEW_NAME = "supervision_terminations_by_month"

SUPERVISION_TERMINATIONS_BY_MONTH_VIEW_DESCRIPTION = """Rates of successful supervision terminations by month.

This is typically used as a backup for the corresponding supervision_success* view when that view produces unreliable data (e.g. when the sentencing data in a state is inaccurate)."""

SUPERVISION_TERMINATIONS_BY_MONTH_VIEW_QUERY_TEMPLATE = """
    /*{description}*/
    WITH supervision_terminations AS (
      SELECT 
        state_code,
        year,
        month,
        supervision_type,
        IFNULL(district, 'EXTERNAL_UNKNOWN') as district,
        LOGICAL_AND(termination_reason in ('COMMUTED', 'DISCHARGE','EXPIRATION','PARDONED', 'VACATED')) as successful_termination,
        person_id,
      FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_termination_metrics_materialized`,
      {district_dimension}
      WHERE {thirty_six_month_filter} AND termination_reason NOT IN (
        'DEATH',
        'EXTERNAL_UNKNOWN',
        'INTERNAL_UNKNOWN',
        'RETURN_FROM_ABSCONSION',
        'SUSPENSION',
        'TRANSFER_WITHIN_STATE',
        'TRANSFER_TO_OTHER_JURISDICTION')
      GROUP BY state_code, year, month, district, supervision_type, person_id
    ), supervision_termination_counts AS (
      SELECT
        state_code,
        year,
        month,
        district,
        supervision_type,
        COUNT(DISTINCT IF(successful_termination, person_id, NULL)) AS successful_termination_count,
        COUNT(DISTINCT(person_id)) AS termination_count
      FROM supervision_terminations
      GROUP BY state_code, year, month, district, supervision_type
    )
    
    
    SELECT
        *,
        ROUND(IEEE_DIVIDE(successful_termination_count, termination_count), 2) as success_rate
    FROM supervision_termination_counts
    WHERE {state_specific_supervision_type_inclusion_filter}
    ORDER BY state_code, year, month, supervision_type
    """

SUPERVISION_TERMINATIONS_BY_MONTH_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET,
    view_id=SUPERVISION_TERMINATIONS_BY_MONTH_VIEW_NAME,
    view_query_template=SUPERVISION_TERMINATIONS_BY_MONTH_VIEW_QUERY_TEMPLATE,
    dimensions=(
        "state_code",
        "supervision_type",
        "year",
        "month",
        "district",
    ),
    description=SUPERVISION_TERMINATIONS_BY_MONTH_VIEW_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    district_dimension=bq_utils.unnest_district(
        state_specific_query_strings.state_supervision_specific_district_groupings(
            "supervising_district_external_id", "judicial_district_code"
        )
    ),
    thirty_six_month_filter=bq_utils.thirty_six_month_filter(),
    state_specific_supervision_type_inclusion_filter=state_specific_query_strings.state_specific_supervision_type_inclusion_filter(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_TERMINATIONS_BY_MONTH_VIEW_BUILDER.build_and_print()
