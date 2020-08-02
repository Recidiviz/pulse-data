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
"""Supervision revocations by period by type by demographics. Person-based counts with respect to metric_period_months
and supervision_type. If a person has more than one revocation of the same supervision type in a given metric period,
the most recent one is chosen."""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GAE_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_REVOCATIONS_BY_PERIOD_BY_TYPE_BY_DEMOGRAPHICS_VIEW_VIEW_NAME = \
    'supervision_revocations_by_period_by_type_by_demographics'

SUPERVISION_REVOCATIONS_BY_PERIOD_BY_TYPE_BY_DEMOGRAPHICS_VIEW_VIEW_DESCRIPTION = \
    """Supervision revocations by period, by source violation type, and by demographic breakdowns. Person-based counts
    with respect to metric_period_months and supervision_type. If a person has more than one revocation of the same
    supervision type in a given metric period, the most recent one is chosen."""

SUPERVISION_REVOCATIONS_BY_PERIOD_BY_TYPE_BY_DEMOGRAPHICS_VIEW_VIEW_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH revocations_by_period AS (
      SELECT
        state_code,
        metric_period_months,
        race,
        ethnicity,
        person_id,
        supervision_type,
        IFNULL(source_violation_type, 'EXTERNAL_UNKNOWN') as source_violation_type,
        IFNULL(gender, 'EXTERNAL_UNKNOWN') as gender,
        IFNULL(age_bucket, 'EXTERNAL_UNKNOWN') as age_bucket,
        ROW_NUMBER() OVER (PARTITION BY state_code, metric_period_months, supervision_type, person_id ORDER BY revocation_admission_date DESC) as ranking
      FROM `{project_id}.{reference_dataset}.event_based_revocations`,
        UNNEST ([36]) AS metric_period_months
      WHERE {metric_period_condition}
      AND supervision_type IN ('PAROLE', 'PROBATION')
    ), revocations_by_period_by_person AS (
      SELECT
        *
      FROM
        revocations_by_period,
        {race_or_ethnicity_dimension}
      WHERE ranking = 1
    )
    
    SELECT
      state_code,
      metric_period_months,
      supervision_type,
      COUNT(DISTINCT IF(source_violation_type IN ('FELONY', 'MISDEMEANOR'), person_id, NULL)) AS new_crime_count,
      COUNT(DISTINCT IF(source_violation_type = 'TECHNICAL', person_id, NULL)) AS technical_count,
      COUNT(DISTINCT IF(source_violation_type = 'ABSCONDED', person_id, NULL)) AS absconsion_count,
      COUNT(DISTINCT IF(source_violation_type = 'EXTERNAL_UNKNOWN', person_id, NULL)) as unknown_count,
      {state_specific_race_or_ethnicity_groupings},
      gender,
      age_bucket,
      COUNT(DISTINCT person_id) AS revocation_count
    FROM revocations_by_period_by_person,
      {unnested_race_or_ethnicity_dimension},
      {gender_dimension},
      {age_dimension}
    WHERE (race_or_ethnicity != 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL') -- Race breakdown
      OR (race_or_ethnicity = 'ALL' AND gender != 'ALL' AND age_bucket = 'ALL') -- Gender breakdown
      OR (race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket != 'ALL') -- Age breakdown
      OR (race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL') -- Overall breakdown
    GROUP BY state_code, metric_period_months, supervision_type, race_or_ethnicity, gender, age_bucket
    ORDER BY state_code, metric_period_months, supervision_type, race_or_ethnicity, gender, age_bucket
    """

SUPERVISION_REVOCATIONS_BY_PERIOD_BY_TYPE_BY_DEMOGRAPHICS_VIEW_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET,
    view_id=SUPERVISION_REVOCATIONS_BY_PERIOD_BY_TYPE_BY_DEMOGRAPHICS_VIEW_VIEW_NAME,
    view_query_template=SUPERVISION_REVOCATIONS_BY_PERIOD_BY_TYPE_BY_DEMOGRAPHICS_VIEW_VIEW_QUERY_TEMPLATE,
    description=SUPERVISION_REVOCATIONS_BY_PERIOD_BY_TYPE_BY_DEMOGRAPHICS_VIEW_VIEW_DESCRIPTION,
    reference_dataset=dataset_config.REFERENCE_TABLES_DATASET,
    race_or_ethnicity_dimension=bq_utils.unnest_race_and_ethnicity(),
    metric_period_condition=bq_utils.metric_period_condition(),
    unnested_race_or_ethnicity_dimension=bq_utils.unnest_column('race_or_ethnicity', 'race_or_ethnicity'),
    gender_dimension=bq_utils.unnest_column('gender', 'gender'),
    age_dimension=bq_utils.unnest_column('age_bucket', 'age_bucket'),
    state_specific_race_or_ethnicity_groupings=bq_utils.state_specific_race_or_ethnicity_groupings()
)

if __name__ == '__main__':
    with local_project_id_override(GAE_PROJECT_STAGING):
        SUPERVISION_REVOCATIONS_BY_PERIOD_BY_TYPE_BY_DEMOGRAPHICS_VIEW_VIEW_BUILDER.build_and_print()
