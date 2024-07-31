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
"""
Defines a criteria span view that shows spans of time during which
someone in ID  has been in a Community Reentry Center facility or
PWCC Unit 1 for 60 days.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_criteria_state_specific_dataset,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_IX_IN_CRC_FACILITY_OR_PWCC_UNIT_1_FOR_60_DAYS"

_DESCRIPTION = """
Defines a criteria span view that shows spans of time during which
someone in ID  has been in a Community Reentry Center facility or
PWCC Unit 1 for 60 days.
"""

_QUERY_TEMPLATE = f"""
WITH in_crc AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
    FROM `{{project_id}}.{{task_eligibility_criteria_us_ix_dataset}}.in_crc_facility_or_pwcc_unit_1_materialized`
),

critical_date_spans AS (
    SELECT 
        *,
        start_date AS start_datetime,
        end_date AS end_datetime,
        DATE_ADD(start_date, INTERVAL 60 DAY) AS critical_date
    FROM ({aggregate_adjacent_spans('in_crc')})
),

{critical_date_has_passed_spans_cte()}

SELECT
    cd.state_code,
    cd.person_id,
    cd.start_date,
    cd.end_date,
    cd.critical_date_has_passed AS meets_criteria,
    TO_JSON(STRUCT(
        cd.critical_date AS sixty_days_in_crc_facility_date)) AS reason,
    cd.critical_date AS sixty_days_in_crc_facility_date,
FROM critical_date_has_passed_spans cd
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    state_code=StateCode.US_IX,
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    description=_DESCRIPTION,
    task_eligibility_criteria_us_ix_dataset=task_eligibility_criteria_state_specific_dataset(
        StateCode.US_IX
    ),
    reasons_fields=[
        ReasonsField(
            name="sixty_days_in_crc_facility_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="The date on which the person has been in a Community Reentry Center facility or PWCC Unit 1 for 60 days.",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
