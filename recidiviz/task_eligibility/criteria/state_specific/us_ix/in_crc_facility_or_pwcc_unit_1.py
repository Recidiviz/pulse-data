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
someone in ID is in a Community Reentry Center facility or in Pocatello
Women's Correctional Center Unit 1
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_criteria_state_specific_dataset,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    extract_object_from_json,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_IX_IN_CRC_FACILITY_OR_PWCC_UNIT_1"

_DESCRIPTION = """
Defines a criteria span view that shows spans of time during which
someone in ID is in a Community Reentry Center facility or in Pocatello 
Women's Correctional Center Unit 1
"""

_QUERY_TEMPLATE = f"""WITH pwcc_unit_1 AS (
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date_exclusive AS end_date,
        facility,
    FROM `{{project_id}}.{{sessions_dataset}}.housing_unit_sessions_materialized`
    WHERE state_code = 'US_IX'
        AND facility = "POCATELLO WOMEN'S CORRECTIONAL CENTER"
        # Only those in Unit 1
        AND REGEXP_CONTAINS(housing_unit, r'UNIT 1')
),

pwcc_unit_1_and_crc AS (
    # PWCC Unit 1
    SELECT  
        state_code,
        person_id,
        start_date,
        end_date,
        facility AS facility_name,
    FROM ({aggregate_adjacent_spans(table_name='pwcc_unit_1',
                                    attribute='facility',)})

    UNION ALL

    # CRC
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        {extract_object_from_json(object_column = 'facility_name', 
                                  object_type = 'STRING')} AS facility_name,
    FROM `{{project_id}}.{{task_eligibility_criteria_us_ix_dataset}}.in_crc_facility_materialized`
),

{create_sub_sessions_with_attributes('pwcc_unit_1_and_crc')}

SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    TRUE AS meets_criteria,
    TO_JSON(STRUCT(start_date AS crc_start_date, ANY_VALUE(facility_name) AS facility_name)) AS reason,
    start_date AS crc_start_date,
    ANY_VALUE(facility_name) AS facility_name,
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    state_code=StateCode.US_IX,
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    description=_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    task_eligibility_criteria_us_ix_dataset=task_eligibility_criteria_state_specific_dataset(
        StateCode.US_IX
    ),
    reasons_fields=[
        ReasonsField(
            name="crc_start_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="The date on which the person is in a Community Reentry Center (CRC) facility",
        ),
        ReasonsField(
            name="facility_name",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="The name of the facility where the person is in a CRC facility or PWCC Unit 1",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
