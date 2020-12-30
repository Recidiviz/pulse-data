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
"""Active FTR participation counts by the region of the program location."""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config, state_specific_query_strings
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ACTIVE_PROGRAM_PARTICIPATION_BY_REGION_VIEW_NAME = 'active_program_participation_by_region'

ACTIVE_PROGRAM_PARTICIPATION_BY_REGION_VIEW_DESCRIPTION = \
    """Active program participation counts by the region of the program location."""


# TODO(#4294): Use the prioritized_race_or_ethnicity column
ACTIVE_PROGRAM_PARTICIPATION_BY_REGION_VIEW_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH most_recent_job_id AS (
      SELECT
        state_code,
        metric_date as date_of_participation,
        job_id,
        metric_type
      FROM
        `{project_id}.{materialized_metrics_dataset}.most_recent_daily_job_id_by_metric_and_state_code_materialized`
    ), participants_with_race_or_ethnicity AS (
      SELECT
        state_code,
        supervision_type,
        region_id,
        person_id,
        race_or_ethnicity,
        ROW_NUMBER() OVER
        -- People can be counted on multiple types of supervision and enrolled in multiple regions simultaneously --
        (PARTITION BY state_code, person_id, supervision_type, region_id ORDER BY representation_priority) as inclusion_priority
      FROM
        `{project_id}.{metrics_dataset}.program_participation_metrics`
      INNER JOIN
        most_recent_job_id
      USING (state_code, job_id, date_of_participation, metric_type)
      LEFT JOIN
        `{project_id}.{static_reference_dataset}.program_locations`
      USING (state_code, program_location_id),
        {race_or_ethnicity_dimension}
      LEFT JOIN
         `{project_id}.{static_reference_dataset}.state_race_ethnicity_population_counts`
      USING (state_code, race_or_ethnicity)
      WHERE state_code = 'US_ND'
        AND methodology = 'EVENT'
        AND metric_period_months = 0
        AND person_id IS NOT NULL
        AND supervision_type IN ('PAROLE', 'PROBATION')
    )

    SELECT
      state_code,
      supervision_type,
      region_id,
      {state_specific_race_or_ethnicity_groupings},
      COUNT(DISTINCT(person_id)) as participation_count
    FROM
      participants_with_race_or_ethnicity,
      {unnested_race_or_ethnicity_dimension},
      {region_dimension},
      {supervision_type_dimension}
    WHERE inclusion_priority = 1
    GROUP BY state_code, supervision_type, race_or_ethnicity, region_id
    ORDER BY state_code, supervision_type, race_or_ethnicity, region_id
    """

ACTIVE_PROGRAM_PARTICIPATION_BY_REGION_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET,
    view_id=ACTIVE_PROGRAM_PARTICIPATION_BY_REGION_VIEW_NAME,
    view_query_template=ACTIVE_PROGRAM_PARTICIPATION_BY_REGION_VIEW_QUERY_TEMPLATE,
    dimensions=['state_code', 'supervision_type', 'race_or_ethnicity', 'region_id'],
    description=ACTIVE_PROGRAM_PARTICIPATION_BY_REGION_VIEW_DESCRIPTION,
    base_dataset=dataset_config.STATE_BASE_DATASET,
    static_reference_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    current_month_condition=bq_utils.current_month_condition(),
    state_specific_race_or_ethnicity_groupings=
    state_specific_query_strings.state_specific_race_or_ethnicity_groupings(),
    race_or_ethnicity_dimension=bq_utils.unnest_race_and_ethnicity(),
    unnested_race_or_ethnicity_dimension=bq_utils.unnest_column('race_or_ethnicity', 'race_or_ethnicity'),
    region_dimension=bq_utils.unnest_column('region_id', 'region_id'),
    supervision_type_dimension=bq_utils.unnest_supervision_type(),
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        ACTIVE_PROGRAM_PARTICIPATION_BY_REGION_VIEW_BUILDER.build_and_print()
