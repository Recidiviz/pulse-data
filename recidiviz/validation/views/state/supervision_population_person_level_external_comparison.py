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

"""A view comparing various values from internal person-level supervision population metrics to the person-level values
from external metrics provided by the state.
"""

# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

SUPERVISION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_NAME = \
    'supervision_population_person_level_external_comparison'

SUPERVISION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_DESCRIPTION = """
Comparison of district values between internal and external lists of end of month person-level supervision
populations.
"""

SUPERVISION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_QUERY_TEMPLATE = \
    """
/*{description}*/
WITH sanitized_internal_metrics AS (
  SELECT
      state_code AS region_code, 
      date_of_supervision, 
      person_external_id, 
      CASE  
        # TODO(3830): Check back in with ID to see if they have rectified their historical data. If so, we can remove
        #  this case.
        # US_ID - All low supervision unit POs had inconsistent data before July 2020.
        WHEN state_code = 'US_ID' 
            AND supervising_officer_external_id IN ('REGARCIA', 'CAMCDONA', 'SLADUKE', 'COLIMSUP') 
            AND date_of_supervision < '2020-07-01' THEN 'LSU'
        ELSE supervising_officer_external_id
      END AS supervising_officer_external_id,
      supervision_level_raw_text,
      supervising_district_external_id AS internal_district,
   FROM `{project_id}.{metrics_dataset}.supervision_population_metrics`
   JOIN `{project_id}.{reference_dataset}.most_recent_job_id_by_metric_and_state_code` job
      USING (state_code, job_id, year, month, metric_period_months)
   WHERE metric_period_months = 0
   AND methodology = 'PERSON'
   AND job.metric_type = 'SUPERVISION_POPULATION'
   AND (state_code != 'US_ID' OR 
       # Idaho only gives us population numbers for folks explicitly on active probation, parole, or dual supervision.
       # The following groups are folks we consider a part of the SupervisionPopulation even though ID does not:
       #    - `INFORMAL_PROBATION` - although IDOC does not actively supervise these folks, they can be revoked 
       #       and otherwise punished as if they were actively on supervision. 
       #    - `INTERNAL_UNKNOWN` - vast majority of these people are folks with active bench warrants
       (supervision_type IN ('PROBATION', 'PAROLE', 'DUAL') 
       # TODO(3831): Add bit to SupervisionPopulation metric to describe absconsion instead of this filter. 
       AND supervising_district_external_id IS NOT NULL)))
SELECT
      region_code,
      date_of_supervision,
      person_external_id,
      district AS external_district,
      internal_district,
      supervision_level AS external_supervision_level,
      supervision_level_raw_text AS internal_supervision_level,
      supervising_officer AS external_supervising_officer,
      supervising_officer_external_id AS internal_supervising_officer,
FROM
  `{project_id}.{external_accuracy_dataset}.supervision_population_person_level`
FULL OUTER JOIN
  (SELECT * FROM
    -- Only compare regions and months for which we have external validation data
    (SELECT DISTINCT region_code, date_of_supervision FROM
        `{project_id}.{external_accuracy_dataset}.supervision_population_person_level`)
  LEFT JOIN
    sanitized_internal_metrics
  USING (region_code, date_of_supervision))
USING(region_code, date_of_supervision, person_external_id)
ORDER BY region_code, date_of_supervision, person_external_id
"""

SUPERVISION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=SUPERVISION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_NAME,
    view_query_template=SUPERVISION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_QUERY_TEMPLATE,
    description=SUPERVISION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_DESCRIPTION,
    external_accuracy_dataset=dataset_config.EXTERNAL_ACCURACY_DATASET,
    metrics_dataset=state_dataset_config.DATAFLOW_METRICS_DATASET,
    reference_dataset=state_dataset_config.REFERENCE_TABLES_DATASET
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER.build_and_print()
