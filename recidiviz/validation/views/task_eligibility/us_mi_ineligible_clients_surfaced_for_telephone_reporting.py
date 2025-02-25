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
"""A view revealing when there are any clients that are on a supervision level that is ineligible
for telephone reporting (i.e. any supervision level that is not minimum low or minimum in person) or already on
telephone reporting.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility import dataset_config as tes_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

US_MI_INELIGIBLE_CLIENTS_SURFACED_FOR_TELEPHONE_REPORTING_VIEW_NAME = (
    "us_mi_ineligible_clients_surfaced_for_telephone_reporting"
)

US_MI_INELIGIBLE_CLIENTS_SURFACED_FOR_TELEPHONE_REPORTING_DESCRIPTION = """
Identifies when there are any clients that are on a supervision level that is ineligible 
for telephone reporting (i.e. any supervision level that is not minimum low or minimum in person) or already on 
telephone reporting.
"""

US_MI_INELIGIBLE_CLIENTS_SURFACED_FOR_TELEPHONE_REPORTING_QUERY_TEMPLATE = f"""
WITH
  ineligible_clients AS (
  SELECT
    person_id
  FROM
    `{{project_id}}.sessions.compartment_sub_sessions_materialized`c
  LEFT JOIN
    `{{project_id}}.analyst_data.us_mi_supervision_level_raw_text_mappings` omni_sls
  ON
    REGEXP_EXTRACT(c.correctional_level_raw_text, r'(\\d+)') = \
    omni_sls.supervision_level_raw_text \
    AND omni_sls.source = "OMNI"
  LEFT JOIN 
    `{{project_id}}.analyst_data.us_mi_supervision_level_raw_text_mappings` coms_sls
  ON REPLACE(c.correctional_level_raw_text, "##IMPUTED", "") \
    = coms_sls.supervision_level_raw_text \
    AND coms_sls.source = 'COMS'
  WHERE
    state_code = 'US_MI'
    AND compartment_level_1 = 'SUPERVISION'
    AND NOT COALESCE(omni_sls.is_minimum_low, coms_sls.is_minimum_low, FALSE)
    AND NOT COALESCE(omni_sls.is_minimum_in_person, coms_sls.is_minimum_in_person, FALSE)
    AND NOT COALESCE(omni_sls.is_telephone, coms_sls.is_telephone, FALSE)
    AND CURRENT_DATE('US/Pacific') BETWEEN start_date
    AND {nonnull_end_date_exclusive_clause('end_date_exclusive')}
    )
SELECT
  tr.state_code as region_code,
  tr.person_id,
FROM
  `{{project_id}}.{{task_eligibility_dataset}}.complete_transfer_to_telephone_reporting_request_materialized` tr
INNER JOIN
  ineligible_clients
ON
  ineligible_clients.person_id = tr.person_id
WHERE
  is_eligible
  AND CURRENT_DATE('US/Pacific') BETWEEN tr.start_date
  AND {nonnull_end_date_exclusive_clause('end_date')}
"""

US_MI_INELIGIBLE_CLIENTS_SURFACED_FOR_TELEPHONE_REPORTING_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=US_MI_INELIGIBLE_CLIENTS_SURFACED_FOR_TELEPHONE_REPORTING_VIEW_NAME,
    view_query_template=US_MI_INELIGIBLE_CLIENTS_SURFACED_FOR_TELEPHONE_REPORTING_QUERY_TEMPLATE,
    description=US_MI_INELIGIBLE_CLIENTS_SURFACED_FOR_TELEPHONE_REPORTING_DESCRIPTION,
    task_eligibility_dataset=tes_dataset_config.task_eligibility_spans_state_specific_dataset(
        StateCode.US_MI
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MI_INELIGIBLE_CLIENTS_SURFACED_FOR_TELEPHONE_REPORTING_VIEW_BUILDER.build_and_print()
