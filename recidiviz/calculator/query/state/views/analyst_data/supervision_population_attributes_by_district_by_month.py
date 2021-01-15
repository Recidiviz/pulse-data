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
"""View tracking characteristics/composition of the supervision population in each district by month"""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_POPULATION_ATTRIBUTES_BY_DISTRICT_BY_MONTH_VIEW_NAME = \
    "supervision_population_attributes_by_district_by_month"

SUPERVISION_POPULATION_ATTRIBUTES_BY_DISTRICT_BY_MONTH_VIEW_DESCRIPTION = \
    "Captures demographic composition of supervision population for a given month by district"

SUPERVISION_POPULATION_ATTRIBUTES_BY_DISTRICT_BY_MONTH_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT
        state_code,
        year,
        month,
        supervising_district_external_id AS district,
        COUNT (DISTINCT person_id) AS supervision_population,
        /* All metrics indicate the proportion of all people on supervision in a given month who belonged to a given demographic group at any point in that month */
        COUNT (DISTINCT(IF(supervision_type = 'PROBATION', person_id, NULL)))/COUNT(DISTINCT person_id) AS probation_population,
        COUNT (DISTINCT(IF(supervision_type = 'PAROLE', person_id, NULL)))/COUNT(DISTINCT person_id) AS parole_population,
        COUNT (DISTINCT(IF(gender = 'MALE', person_id, NULL)))/COUNT(DISTINCT person_id) AS gender_male_population,
        COUNT (DISTINCT(IF(prioritized_race_or_ethnicity = 'WHITE', person_id, NULL)))/COUNT(DISTINCT person_id) AS race_white_population,
        COUNT (DISTINCT(IF (age_bucket IN ('<25', '25-29'), person_id, NULL)))/COUNT(DISTINCT person_id) AS age_under_30_population,
        COUNT (DISTINCT(IF(supervision_level = 'MINIMUM', person_id, NULL)))/COUNT(DISTINCT person_id) AS supervision_level_minimum_population,
        COUNT (DISTINCT(IF(supervision_level = 'MEDIUM', person_id, NULL)))/COUNT(DISTINCT person_id) AS supervision_level_medium_population,
        COUNT (DISTINCT(IF(supervision_level = 'HIGH', person_id, NULL)))/COUNT(DISTINCT person_id) AS supervision_level_high_population,
        COUNT (DISTINCT(IF(assessment_score_bucket IN ('39+', '30-38'), person_id, NULL)))/COUNT(DISTINCT person_id) AS risk_score_over_30_population,
        COUNT (DISTINCT(IF(assessment_score_bucket = '39+', person_id, NULL)))/COUNT(DISTINCT person_id) AS risk_score_over_39_population,
        COUNT (DISTINCT(IF(assessment_score_bucket = '0-23', person_id, NULL)))/COUNT(DISTINCT person_id) AS risk_score_under_24_population
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_population_metrics_materialized`
    WHERE supervision_type IN ('DUAL', 'PROBATION', 'PAROLE', 'INTERNAL_UNKNOWN')
    GROUP BY state_code, year, month, supervising_district_external_id
    """

SUPERVISION_POPULATION_ATTRIBUTES_BY_DISTRICT_BY_MONTH_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.ANALYST_VIEWS_DATASET,
    view_id=SUPERVISION_POPULATION_ATTRIBUTES_BY_DISTRICT_BY_MONTH_VIEW_NAME,
    view_query_template=SUPERVISION_POPULATION_ATTRIBUTES_BY_DISTRICT_BY_MONTH_QUERY_TEMPLATE,
    description=SUPERVISION_POPULATION_ATTRIBUTES_BY_DISTRICT_BY_MONTH_VIEW_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    should_materialize=True
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_POPULATION_ATTRIBUTES_BY_DISTRICT_BY_MONTH_VIEW_BUILDER.build_and_print()
