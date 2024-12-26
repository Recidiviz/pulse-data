# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Defines a criteria span view that shows spans of time during which someone has
    been marked ineligible for admin supervision in workflows. If they have been marked
    ineligible for reasons that don't preclude them from participating in special circumstances
    supervision, they can then be marked eligible for special circumstances consideration
"""

from google.cloud import bigquery

from recidiviz.calculator.query.state.dataset_config import (
    EXPORT_ARCHIVES_DATASET,
    WORKFLOWS_VIEWS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_PA_MARKED_INELIGIBLE_FOR_ADMIN_SUPERVISION"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which someone has
    been marked ineligible for admin supervision in workflows. If they have been marked
    ineligible for reasons that don't preclude them from participating in special circumstances
    supervision, they can then be marked eligible for special circumstances consideration
"""

_QUERY_TEMPLATE = """
SELECT 
    a.state_code,
    person_id,
    snooze_start_date AS start_date, 
    IF( # if the snooze was not still active as of the last day we have data for (presumably today) 
        MAX(as_of)<(SELECT MAX(as_of) FROM `{project_id}.{export_archives_dataset}.workflows_snooze_status_archive`),
        # then take the last day it was active 
        MAX(as_of),
        # else assume it is still active and assign an end date of null 
        NULL) AS end_date,
    TRUE AS meets_criteria,
    TO_JSON(STRUCT(snooze_start_date AS admin_marked_ineligible_date)) AS reason,
    snooze_start_date AS admin_marked_ineligible_date,
FROM `{project_id}.{export_archives_dataset}.workflows_snooze_status_archive` a
LEFT JOIN `{project_id}.{workflows_views_dataset}.person_id_to_external_id_materialized` pei
    ON pei.state_code = a.state_code 
    AND UPPER(pei.person_external_id) = UPPER(a.person_external_id)
WHERE a.state_code = 'US_PA'
    AND opportunity_type = 'usPaAdminSupervision'
    AND 'FINES & FEES' NOT IN UNNEST(denial_reasons) 
    AND 'SPECIAL CONDITIONS' NOT IN UNNEST(denial_reasons)
GROUP BY 1, 2, 3
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    state_code=StateCode.US_PA,
    workflows_views_dataset=WORKFLOWS_VIEWS_DATASET,
    export_archives_dataset=EXPORT_ARCHIVES_DATASET,
    reasons_fields=[
        ReasonsField(
            name="admin_marked_ineligible_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date that someone was marked ineligible for administrative supervision in Workflows",
        )
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
