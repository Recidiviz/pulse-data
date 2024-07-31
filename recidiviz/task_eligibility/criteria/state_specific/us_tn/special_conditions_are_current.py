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
# ============================================================================
"""Describes the spans of time when a TN client's special conditions contact note is not overdue."""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.task_eligibility.utils.us_tn_query_fragments import (
    EXCLUDED_MEDIUM_RAW_TEXT,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_SPECIAL_CONDITIONS_ARE_CURRENT"

_DESCRIPTION = """Describes the spans of time when a TN client's special conditions contact note is not overdue. The
logic of when a special conditions contact is due depends someone's supervision level. This criteria ultimately produces
spans of time when the next special conditions contact was not overdue. This might mean there are no special conditions,
conditions are terminated, or they are current. Note, these spans are only defined for people on Minimum or Medium (not
PSU) supervision levels, since those are the ones relevant for Compliant Reporting. If these contact standards change,
the variables below will need to be updated.
"""

US_TN_MINIMUM_SPE_NOTE_CONTACT_STANDARD = 4
US_TN_MEDIUM_SPE_NOTE_CONTACT_STANDARD = 2

_QUERY_TEMPLATE = f"""
    
    WITH spe_sessions_cte AS
    (
        SELECT
            c.state_code,
            c.person_id,
            contact_date AS start_date,
            contact_date,
            LAG(IF(contact_type='SPEC',contact_date,NULL))
                OVER (PARTITION BY c.person_id 
                      ORDER BY contact_date ASC)
                AS most_recent_spec_date,
            LEAD(contact_date)
                OVER (PARTITION BY c.person_id 
                      ORDER BY contact_date ASC)
                AS end_date,
            contact_type,
        FROM
            (
            SELECT 
                state_code,
                person_id,
                CAST(CAST(ContactNoteDateTime AS datetime) AS DATE) AS contact_date,
                ContactNoteType AS contact_type,
            FROM
                `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.ContactNoteType_latest` contact
            INNER JOIN
                `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
            ON
                contact.OffenderID = pei.external_id
            AND
                pei.state_code = 'US_TN'
            WHERE
                -- SPEC means SPECIAL CONDITIONS MONITORED AS DESCRIBED IN COMMENTS 
                -- SPET means ALL SPECIAL CONDITIONS MONITORING TERMINATED
                -- XSPE means SPECIAL CONDITIONS NOT VERIFIED, SEE COMMENTS
                ContactNoteType IN ('SPEC','SPET','XSPE')
            /* Prioritize SPET > SPEC > XSPE. If someone has a SPET on the same day as a SPEC, their 
                special conditions have been terminated and don't need to be checked again. If they have a SPEC
                and XSPE on the same day, we want to prioritize the contact that was able to verify special conditions */
            QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, contact_date
                                 ORDER BY CASE WHEN contact_type = "SPET" THEN 0 
                                                WHEN contact_type = "SPEC" THEN 1
                                                ELSE 2 END ASC) = 1
            ) c
    )
    ,
    /*
        This CTE brings in supervision level information since that determines how often someone's special conditions
        need to be checked. We do this and create sub sessions with attributes because someone's next SPE due date
        is determined by their current supervision level, not the level they were on when the last contact was 
        conducted
    */    
    unioned AS (
        SELECT
            state_code,
            person_id,
            start_date,
            most_recent_spec_date,
            end_date,
            contact_date,
            contact_type,
            NULL AS supervision_level,
            NULL AS supervision_level_raw_text
        FROM spe_sessions_cte
        
        UNION ALL
        
        SELECT
            state_code,
            person_id,
            start_date,
            NULL AS most_recent_spec_date,
            end_date_exclusive AS end_date,
            NULL AS contact_date,
            NULL AS contact_type,
            supervision_level,
            supervision_level_raw_text
        FROM
            `{{project_id}}.{{sessions_dataset}}.supervision_level_raw_text_sessions_materialized` sl
        WHERE
            state_code = 'US_TN'
    ),
    {create_sub_sessions_with_attributes('unioned')},
    -- This CTE combines information on contacts and supervision levels
    priority_levels AS (
        SELECT  person_id, 
                state_code, 
                start_date, 
                end_date, 
                MAX(most_recent_spec_date) AS most_recent_spec_date,
                MAX(contact_date) AS contact_date,
                MAX(contact_type) AS contact_type,
                MAX(supervision_level) AS supervision_level,
                MAX(supervision_level_raw_text) AS supervision_level_raw_text,
        FROM sub_sessions_with_attributes
        GROUP BY 1,2,3,4
    ),
    -- This CTE uses supervision-level specific mapping to determine when the "critical date", or due date for
    -- next SPE note is. 
    critical_date_spans AS (
        SELECT
            state_code,
            person_id,
            start_date AS start_datetime,
            end_date AS end_datetime,
            contact_type,
            supervision_level,
            supervision_level_raw_text,
            -- If contact_type = SPET then another contact is not needed so critical date is left null.
            CASE WHEN contact_type = 'SPEC' THEN (
                CASE
                    WHEN supervision_level = "MINIMUM" 
                        THEN LAST_DAY(DATE_ADD(contact_date, INTERVAL {US_TN_MINIMUM_SPE_NOTE_CONTACT_STANDARD} MONTH))
                    WHEN supervision_level = "MEDIUM" AND supervision_level_raw_text NOT IN ('{{exclude_medium}}') 
                        THEN LAST_DAY(DATE_ADD(contact_date, INTERVAL {US_TN_MEDIUM_SPE_NOTE_CONTACT_STANDARD} MONTH))
                    END
            )
                WHEN contact_type = 'XSPE' THEN (
                    CASE
                        WHEN supervision_level = "MINIMUM" 
                            THEN LAST_DAY(
                                    DATE_ADD(
                                        COALESCE(most_recent_spec_date,contact_date), 
                                        INTERVAL {US_TN_MINIMUM_SPE_NOTE_CONTACT_STANDARD} MONTH
                                        )
                                    )
                        WHEN supervision_level = "MEDIUM" AND supervision_level_raw_text NOT IN ('{{exclude_medium}}') 
                            THEN LAST_DAY(
                                    DATE_ADD(
                                        COALESCE(most_recent_spec_date,contact_date),
                                        INTERVAL {US_TN_MEDIUM_SPE_NOTE_CONTACT_STANDARD} MONTH
                                        )
                                    )
                        END
            ) 
            END AS critical_date,
        FROM
            priority_levels
    ),
    {critical_date_has_passed_spans_cte(attributes=['contact_type','supervision_level_raw_text'])}
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        -- If critical date has passed, the SPE note is overdue. And if the note was XSPE, the criteria 
        -- is not met - not because the note is overdue but because conditions have not been verified
        CASE WHEN supervision_level_raw_text IN ('{{exclude_medium}}') THEN FALSE
             WHEN contact_type = 'XSPE' THEN FALSE
             ELSE NOT critical_date_has_passed
             END AS meets_criteria,
        TO_JSON(STRUCT(
            critical_date AS spe_note_due
        )) AS reason,
        critical_date AS spe_note_due_date,
    FROM
        critical_date_has_passed_spans

"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        state_code=StateCode.US_TN,
        meets_criteria_default=True,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        sessions_dataset=SESSIONS_DATASET,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
        ),
        exclude_medium="', '".join(EXCLUDED_MEDIUM_RAW_TEXT),
        reasons_fields=[
            ReasonsField(
                name="spe_note_due_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)
if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
