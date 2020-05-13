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

"""A view comparing district values on internal end of month person-level supervision population lists to the district
values in external lists provided by the state."""

# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.validation.views import dataset_config

SUPERVISION_EOM_POPULATION_PERSON_LEVEL_DISTRICT_EXTERNAL_COMPARISON_VIEW_NAME = \
    'supervision_eom_population_person_level_district_external_comparison'

SUPERVISION_EOM_POPULATION_PERSON_LEVEL_DISTRICT_EXTERNAL_COMPARISON_DESCRIPTION = \
    """ Comparison of district values between internal and external lists of end of month person-level supervision
    populations."""

SUPERVISION_EOM_POPULATION_PERSON_LEVEL_DISTRICT_EXTERNAL_COMPARISON_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT
      state_code as region_code,
      year,
      month,
      person_external_id,
      district as external_district,
      internal_district
    FROM
      `{project_id}.{external_accuracy_dataset}.supervision_eom_population_person_level_with_district`
    FULL OUTER JOIN
      (SELECT * FROM
        -- Only compare states and months for which we have external validation data
        (SELECT DISTINCT state_code, year, month FROM
            `{project_id}.{external_accuracy_dataset}.supervision_eom_population_person_level_with_district`)
      LEFT JOIN
        (SELECT
            state_code, year, month, person_external_id, supervising_district_external_id as internal_district
         FROM `{project_id}.{metrics_dataset}.supervision_population_metrics`
         JOIN `{project_id}.{reference_dataset}.most_recent_job_id_by_metric_and_state_code` job
            USING (state_code, job_id, year, month, metric_period_months)
         WHERE metric_period_months = 1
         AND methodology = 'PERSON'
         AND is_on_supervision_last_day_of_month = TRUE
         AND job.metric_type = 'SUPERVISION_POPULATION')
      USING (state_code, year, month))
    USING(state_code, year, month, person_external_id)
    ORDER BY region_code, year, month, person_external_id
"""

SUPERVISION_EOM_POPULATION_PERSON_LEVEL_DISTRICT_EXTERNAL_COMPARISON_VIEW = BigQueryView(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=SUPERVISION_EOM_POPULATION_PERSON_LEVEL_DISTRICT_EXTERNAL_COMPARISON_VIEW_NAME,
    view_query_template=SUPERVISION_EOM_POPULATION_PERSON_LEVEL_DISTRICT_EXTERNAL_COMPARISON_QUERY_TEMPLATE,
    description=SUPERVISION_EOM_POPULATION_PERSON_LEVEL_DISTRICT_EXTERNAL_COMPARISON_DESCRIPTION,
    external_accuracy_dataset=dataset_config.EXTERNAL_ACCURACY_DATASET,
    metrics_dataset=state_dataset_config.DATAFLOW_METRICS_DATASET,
    reference_dataset=state_dataset_config.REFERENCE_TABLES_DATASET
)

if __name__ == '__main__':
    print(SUPERVISION_EOM_POPULATION_PERSON_LEVEL_DISTRICT_EXTERNAL_COMPARISON_VIEW.view_id)
    print(SUPERVISION_EOM_POPULATION_PERSON_LEVEL_DISTRICT_EXTERNAL_COMPARISON_VIEW.view_query)
