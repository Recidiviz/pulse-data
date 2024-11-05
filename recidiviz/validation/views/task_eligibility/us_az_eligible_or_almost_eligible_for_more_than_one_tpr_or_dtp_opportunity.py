# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""A view revealing when there are any clients eligible or almost eligible for more than
one of the following opportunities:
  - OVERDUE_FOR_ACIS_DTP_REQUEST,
  - OVERDUE_FOR_RECIDIVIZ_DTP_REQUEST,
  - OVERDUE_FOR_ACIS_TPR_REQUEST and
  - OVERDUE_FOR_RECIDIVIZ_TPR_REQUEST
This shouldn't be happening. They should only be eligible for at most one.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.task_eligibility.dataset_config import TASK_ELIGIBILITY_DATASET_ID
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

US_AZ_ELIGIBLE_OR_ALMOST_ELIGIBLE_FOR_MORE_THAN_ONE_TPR_OR_DTP_OPPORTUNITY_VIEW_NAME = (
    "us_az_eligible_or_almost_eligible_for_more_than_one_tpr_or_dtp_opportunity"
)

US_AZ_ELIGIBLE_OR_ALMOST_ELIGIBLE_FOR_MORE_THAN_ONE_TPR_OR_DTP_OPPORTUNITY_DESCRIPTION = (
    __doc__
)

US_AZ_ELIGIBLE_OR_ALMOST_ELIGIBLE_FOR_MORE_THAN_ONE_TPR_OR_DTP_OPPORTUNITY_QUERY_TEMPLATE = f"""
WITH eligible_and_almost_eligible AS (
  SELECT
    tes.person_id,
    tes.state_code,
    tes.state_code AS region_code,
    tes.reasons,
    tes.ineligible_criteria,
    tes.is_eligible,
    tes.is_almost_eligible,
    tes.task_name,
  FROM `{{project_id}}.{{task_eligibility_dataset}}.all_tasks_materialized` tes
  WHERE 
    CURRENT_DATE('US/Pacific') BETWEEN tes.start_date AND {nonnull_end_date_exclusive_clause('tes.end_date')}
    AND tes.state_code = 'US_AZ'
    AND (tes.is_eligible OR tes.is_almost_eligible)
    AND task_name IN (
      'OVERDUE_FOR_ACIS_DTP_REQUEST',
      'OVERDUE_FOR_RECIDIVIZ_DTP_REQUEST',
      'OVERDUE_FOR_ACIS_TPR_REQUEST',
      'OVERDUE_FOR_RECIDIVIZ_TPR_REQUEST'
    )
),

more_than_one_row AS (
  SELECT
    region_code,
    person_id,
  FROM eligible_and_almost_eligible 
  GROUP BY 1, 2
  HAVING count(*) > 1
)

SELECT eae.*
FROM more_than_one_row
INNER JOIN eligible_and_almost_eligible eae
  USING(region_code, person_id)
"""

US_AZ_ELIGIBLE_OR_ALMOST_ELIGIBLE_FOR_MORE_THAN_ONE_TPR_OR_DTP_OPPORTUNITY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=US_AZ_ELIGIBLE_OR_ALMOST_ELIGIBLE_FOR_MORE_THAN_ONE_TPR_OR_DTP_OPPORTUNITY_VIEW_NAME,
    view_query_template=US_AZ_ELIGIBLE_OR_ALMOST_ELIGIBLE_FOR_MORE_THAN_ONE_TPR_OR_DTP_OPPORTUNITY_QUERY_TEMPLATE,
    description=US_AZ_ELIGIBLE_OR_ALMOST_ELIGIBLE_FOR_MORE_THAN_ONE_TPR_OR_DTP_OPPORTUNITY_DESCRIPTION,
    task_eligibility_dataset=TASK_ELIGIBILITY_DATASET_ID,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_AZ_ELIGIBLE_OR_ALMOST_ELIGIBLE_FOR_MORE_THAN_ONE_TPR_OR_DTP_OPPORTUNITY_VIEW_BUILDER.build_and_print()
