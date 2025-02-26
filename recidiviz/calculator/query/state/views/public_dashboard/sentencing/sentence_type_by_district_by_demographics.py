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
"""Current incarcerated and supervised population, broken down by sentence type (probation/incarceration),
judicial district, and demographics."""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GAE_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SENTENCE_TYPE_BY_DISTRICT_BY_DEMOGRAPHICS_VIEW_NAME = 'sentence_type_by_district_by_demographics'

SENTENCE_TYPE_BY_DISTRICT_BY_DEMOGRAPHICS_VIEW_DESCRIPTION = \
    """Current incarcerated and supervised population, broken down by sentence type (probation/incarceration),
        judicial district, and demographics."""

# TODO(3720): Improve the sentence type classification and make it less ND specific
SENTENCE_TYPE_BY_DISTRICT_BY_DEMOGRAPHICS_VIEW_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH incarceration_population AS (
      SELECT
        state_code,
        person_id,
        -- TODO(3720): Improve the sentence type classification and make it less ND specific --
        IF(admission_reason = 'PROBATION_REVOCATION', 'PROBATION', 'INCARCERATION') as sentence_type,
        race_or_ethnicity,
        gender,
        age_bucket,
        date_of_stay as population_date,
        judicial_district_code
      FROM
         `{project_id}.{reference_dataset}.most_recent_daily_incarceration_population`
    ), supervision_population AS (
      SELECT
        state_code,
        person_id,
        -- TODO(3720): Improve the sentence type classification and make it less ND specific --
        IF(supervision_type = 'PROBATION', 'PROBATION', 'INCARCERATION') as sentence_type,
        race_or_ethnicity,
        gender,
        age_bucket,
        date_of_supervision as population_date,
        judicial_district_code
      FROM
        `{project_id}.{reference_dataset}.most_recent_daily_supervision_population`
      WHERE supervision_type IN ('PROBATION', 'PAROLE')
    ), all_incarceration_supervision AS (
      (SELECT * FROM incarceration_population)
      UNION ALL
      (SELECT * FROM supervision_population)
    )
        
    SELECT
      state_code,
      district,
      {state_specific_race_or_ethnicity_groupings},
      gender,
      age_bucket,
      COUNT(DISTINCT IF(sentence_type = 'PROBATION', person_id, NULL)) as probation_count,
      COUNT(DISTINCT IF(sentence_type = 'INCARCERATION', person_id, NULL)) as incarceration_count,
      COUNT(DISTINCT(person_id)) as total_population_count
    FROM all_incarceration_supervision,
      {unnested_race_or_ethnicity_dimension},
      {gender_dimension},
      {age_dimension},
      {district_dimension}
    WHERE ((race_or_ethnicity != 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL') -- Race breakdown
      OR (race_or_ethnicity = 'ALL' AND gender != 'ALL' AND age_bucket = 'ALL') -- Gender breakdown
      OR (race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket != 'ALL') -- Age breakdown
      OR (race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL')) -- State-wide count
    GROUP BY state_code, district, race_or_ethnicity, gender, age_bucket
    ORDER BY state_code, district, race_or_ethnicity, gender, age_bucket
    """

SENTENCE_TYPE_BY_DISTRICT_BY_DEMOGRAPHICS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET,
    view_id=SENTENCE_TYPE_BY_DISTRICT_BY_DEMOGRAPHICS_VIEW_NAME,
    view_query_template=SENTENCE_TYPE_BY_DISTRICT_BY_DEMOGRAPHICS_VIEW_QUERY_TEMPLATE,
    description=SENTENCE_TYPE_BY_DISTRICT_BY_DEMOGRAPHICS_VIEW_DESCRIPTION,
    reference_dataset=dataset_config.REFERENCE_TABLES_DATASET,
    state_specific_race_or_ethnicity_groupings=bq_utils.state_specific_race_or_ethnicity_groupings(),
    unnested_race_or_ethnicity_dimension=bq_utils.unnest_column('race_or_ethnicity', 'race_or_ethnicity'),
    gender_dimension=bq_utils.unnest_column('gender', 'gender'),
    age_dimension=bq_utils.unnest_column('age_bucket', 'age_bucket'),
    district_dimension=
    bq_utils.unnest_district(bq_utils.state_specific_judicial_district_groupings('judicial_district_code')),
)

if __name__ == '__main__':
    with local_project_id_override(GAE_PROJECT_STAGING):
        SENTENCE_TYPE_BY_DISTRICT_BY_DEMOGRAPHICS_VIEW_BUILDER.build_and_print()
