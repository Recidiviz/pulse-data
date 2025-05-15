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
"""Spans of time when someone is classified as Class I in Arkansas custody."""
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
    WITH good_time_earning_class_entries AS (
        SELECT
            pei.person_id,
            pei.state_code,
            pei.external_id,
            DATE(sentence_credit.SENTENCECREDITENTRYDATE) AS sentence_credit_entry_date,
            sentence_credit.GOODTIMEEARNINGCLASS AS good_time_earning_class,
        FROM `{project_id}.us_ar_raw_data_up_to_date_views.SENTENCECREDITDBT_latest` sentence_credit
        LEFT JOIN `{project_id}.normalized_state.state_person_external_id` pei
            ON
                pei.state_code = 'US_AR'
                AND pei.id_type = 'US_AR_OFFENDERID'
                AND sentence_credit.OFFENDERID = pei.external_id
        WHERE 
            -- '2' indicates that the earning class change is "applied". Anything else is pending or void.
            sentence_credit.EARNEDTIMESTATUS = '2'
            -- Entry dates in the future are erroneous
            AND DATE(sentence_credit.SENTENCECREDITENTRYDATE) <= CURRENT_DATE("US/Eastern")
            AND sentence_credit.GOODTIMEEARNINGCLASS IS NOT NULL
    )
    ,
    good_time_earning_class_changes AS (
        SELECT
            state_code,
            person_id,
            external_id,
            sentence_credit_entry_date,
            good_time_earning_class,
        FROM good_time_earning_class_entries
        -- Omit entries with the same class as the previous entry (i.e., there's no change)
        QUALIFY LAG(good_time_earning_class) OVER person_window != good_time_earning_class
        WINDOW person_window AS (
            PARTITION BY state_code, person_id
            ORDER BY sentence_credit_entry_date ASC
        )
    )
    ,
    good_time_earning_class_spans AS (
        SELECT
            state_code,
            person_id,
            sentence_credit_entry_date AS start_date,
            LEAD(sentence_credit_entry_date) OVER person_window AS end_date,
            -- Prioritize current earning class in INMATEPROFILE, replacing the most recent sentence
            -- credit entry span if they differ.
            -- TODO(#42433): Refine this logic once we better understand the source of these
            -- discrepancies.
            IF(
                LEAD(sentence_credit_entry_date) OVER person_window IS NULL,
                IFNULL(ip.CURRENTGTEARNINGCLASS, good_time_earning_class),
                good_time_earning_class
            ) AS good_time_earning_class,
        FROM good_time_earning_class_changes gt
        -- Join to get current earning class in INMATEPROFILE
        LEFT JOIN `{project_id}.us_ar_raw_data_up_to_date_views.INMATEPROFILE_latest` ip
            ON gt.external_id = ip.OFFENDERID
        WINDOW person_window AS (
            PARTITION BY state_code, person_id
            ORDER BY sentence_credit_entry_date ASC
        )
    )
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        good_time_earning_class LIKE "%I-%" AS meets_criteria,
        good_time_earning_class,
        TO_JSON(STRUCT(good_time_earning_class)) AS reason,
    FROM good_time_earning_class_spans
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        reasons_fields=[
            ReasonsField(
                name="good_time_earning_class",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Good Time Earning Class status in Arkansas.",
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
