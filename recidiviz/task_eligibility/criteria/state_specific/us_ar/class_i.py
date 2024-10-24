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
# ============================================================================
"""Spans of time when someone is classified as Class I in Arkansas custody.
"""
from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_AR_CLASS_I"

_DESCRIPTION = (
    """Spans of time when someone is classified as Class I in Arkansas custody."""
)

_QUERY_TEMPLATE = """
    WITH class_i_status AS (
        SELECT
            pei.state_code,
            pei.person_id,
            ip.CURRENTGTEARNINGCLASS AS current_gt_earning_class,
            COALESCE(ip.CURRENTGTEARNINGCLASS LIKE "%I-%", FALSE) AS is_class_i,
            DATE(ip.DATELASTUPDATE) AS start_date,
            CAST(NULL AS DATE) AS end_date,
        -- TODO(#34317): Use spans based on update_datetime in `INMATEPROFILE`, or find
        -- another way to identify historical classification changes.
        FROM `{project_id}.us_ar_raw_data_up_to_date_views.INMATEPROFILE_latest` ip
        LEFT JOIN `{project_id}.normalized_state.state_person_external_id` pei
        ON
            pei.state_code = 'US_AR'
            AND pei.id_type = 'US_AR_OFFENDERID'
            AND ip.OFFENDERID = pei.external_id
    )
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        is_class_i AS meets_criteria,
        current_gt_earning_class,
        TO_JSON(STRUCT(current_gt_earning_class)) AS reason
    FROM class_i_status
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        reasons_fields=[
            ReasonsField(
                name="current_gt_earning_class",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Current Class status in Arkansas.",
            )
        ],
        state_code=StateCode.US_AR,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        meets_criteria_default=False,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
