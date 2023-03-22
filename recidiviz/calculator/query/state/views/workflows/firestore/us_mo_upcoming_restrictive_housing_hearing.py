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
"""Query for relevant metadata needed to support upcoming restrictive housing hearing opportunity in Missouri
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.utils.us_mo_query_fragments import (
    current_bed_stay_cte,
    hearings_dedup_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MO_UPCOMING_RESTRICTIVE_HOUSING_HEARING_RECORD_VIEW_NAME = (
    "us_mo_upcoming_restrictive_housing_hearing_record"
)

US_MO_UPCOMING_RESTRICTIVE_HOUSING_HEARING_RECORD_DESCRIPTION = """
    Query for relevant metadata needed to support upcoming restrictive housing hearing opportunity in Missouri 
    """

US_MO_CDV_MINOR_NUM_MONTHS_TO_DISPLAY = "6"
US_MO_CDV_MAJOR_NUM_MONTHS_TO_DISPLAY = "12"

US_MO_UPCOMING_RESTRICTIVE_HOUSING_HEARING_RECORD_QUERY_TEMPLATE = f"""
    WITH base_query AS (
        SELECT
           tes.person_id,
           pei.external_id, 
           tes.state_code,
           tes.reasons AS reasons,
        FROM `{{project_id}}.{{task_eligibility_dataset}}.upcoming_restrictive_housing_hearing_materialized`  tes
        LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
          USING(person_id)
        WHERE CURRENT_DATE('US/Pacific') BETWEEN tes.start_date AND {nonnull_end_date_exclusive_clause('tes.end_date')}
            AND tes.is_eligible
            AND tes.state_code = 'US_MO'
    )
    ,
    {hearings_dedup_cte()}
    ,
    hearing_comments AS (
        SELECT
            ISCSQ_ as hearing_id,
            -- Trim whitespace and quotation marks to concatenate comments across rows.
            -- Comment lines in raw table are sometimes separated in reality by line breaks, and sometimes
            -- by a single space. This inserts line breaks as a general case that doesn't look terrible.
            STRING_AGG(TRIM(ISCMNT, " \\""), "\\n" ORDER BY CAST(ISCLN_ AS INT64) ASC) AS hearing_comments,
        FROM `{{project_id}}.{{us_mo_raw_data_up_to_date_dataset}}.LBAKRDTA_TAK294_latest`
        GROUP BY 1
    )
    ,
    most_recent_hearings AS (
        SELECT DISTINCT
            state_code,
            person_id,
            FIRST_VALUE(hearing_id) OVER person_window AS most_recent_hearing_id,
            FIRST_VALUE(hearing_date) OVER person_window AS most_recent_hearing_date,
            FIRST_VALUE(hearing_type) OVER person_window AS most_recent_hearing_type,
            FIRST_VALUE(hearing_facility) OVER person_window AS most_recent_hearing_facility,
        FROM hearings
        WINDOW person_window AS (
            PARTITION by person_id, state_code
            ORDER BY hearing_date DESC
        )
    )
    ,
    most_recent_hearings_with_comment AS (
        SELECT
            state_code,
            person_id,
            hearing_comments.hearing_comments AS most_recent_hearing_comments,
            most_recent_hearings.most_recent_hearing_date,
            most_recent_hearings.most_recent_hearing_facility,
            most_recent_hearings.most_recent_hearing_type
        FROM most_recent_hearings
        LEFT JOIN hearing_comments
        ON most_recent_hearings.most_recent_hearing_id = hearing_comments.hearing_id
    )
    ,
    current_confinement_stay AS (
        SELECT DISTINCT
            person_id,
            state_code,
            FIRST_VALUE(start_date) OVER w as start_date,
        FROM `{{project_id}}.{{sessions_dataset}}.us_mo_confinement_type_sessions_materialized`
        WINDOW w AS (
            PARTITION BY person_id, state_code
            ORDER BY confinement_type_session_id DESC
        )
    )
    ,
    current_housing_stay AS (
        SELECT DISTINCT
            person_id,
            state_code,
            FIRST_VALUE(facility_code) OVER w as facility_code,
        FROM `{{project_id}}.{{sessions_dataset}}.us_mo_housing_stay_sessions_materialized`
        WINDOW w AS (
            PARTITION BY person_id, state_code
            ORDER BY housing_stay_session_id DESC
        )
    )
    ,
    {current_bed_stay_cte()}
    ,
    cdv_clean AS (
        SELECT 
            pei.person_id,
            SAFE.PARSE_DATE("%Y%m%d", cdv.IZWDTE) AS cdv_date,
            IZVRUL as cdv_rule,
            -- Rules are formatted as [major_number].[minor_number] e.g. 11.5
            CAST(RTRIM(REGEXP_EXTRACT(cdv.IZVRUL, r'.+\\.'), r'\\.') AS INT64) as cdv_rule_part1,
        FROM `{{project_id}}.{{us_mo_raw_data_up_to_date_dataset}}.LBAKRDTA_TAK233_latest` cdv
        LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON
            cdv.IZ_DOC = pei.external_id
            AND pei.state_code = 'US_MO'
    )
    ,
    cdv_all AS (
        SELECT
            person_id,
            cdv.cdv_date as event_date,
            cdv.cdv_rule as note_title,
            NULL as note_body,
            -- Major violations are rules 1-9
            -- Further info on CDVs here: 
            -- https://doc.mo.gov/sites/doc/files/media/pdf/2020/03/Offender_Rulebook_REVISED_2019.pdf
            cdv.cdv_rule_part1 < 10 AS is_major_violation,
            h.most_recent_hearing_date,
            -- Excludes violations that occur the day of a hearing, which could in reality
            -- occur either before or after the hearing, since hearings are likely to be
            -- scheduled further in advance than on the same day as the violation.
            cdv.cdv_date > h.most_recent_hearing_date AS occurred_since_last_hearing,
        FROM cdv_clean cdv
        LEFT JOIN most_recent_hearings h
        USING(person_id)
    )
    ,
    cdv_case_notes AS (
        SELECT
            person_id,
            TO_JSON(
                ARRAY_AGG(
                    STRUCT(
                        note_title,
                        note_body,
                        event_date,
                        -- Separate violations into mutually exclusive groups:
                        -- All major CDVs, minor CDVs since the most recent hearing,
                        -- and minor CDVs before the most recent hearing
                        (
                            CASE 
                            WHEN is_major_violation THEN "Major CDVs, past 12 months"
                            WHEN occurred_since_last_hearing THEN "Other CDVs since last hearing"
                            ELSE "Other Minor CDVs, past 6 months" END
                        ) AS criteria
                    )
                    ORDER BY event_date DESC
                )
            ) AS case_notes
        FROM cdv_all
        WHERE 
            -- Only surface CDVs corresponding to the criteria groups
            (is_major_violation AND event_date >= DATE_SUB(CURRENT_DATE('US/Pacific'), INTERVAL {{us_mo_cdv_major_months_to_display}} MONTH))
            OR
            ((NOT is_major_violation) AND event_date >= DATE_SUB(CURRENT_DATE('US/Pacific'), INTERVAL {{us_mo_cdv_minor_months_to_display}} MONTH))
            OR
            (occurred_since_last_hearing)
        GROUP BY 1
    )
    ,
    final AS (
        SELECT
            base.external_id,
            base.state_code,
            base.reasons,
            hearings.most_recent_hearing_date AS metadata_most_recent_hearing_date,
            hearings.most_recent_hearing_type AS metadata_most_recent_hearing_type,
            hearings.most_recent_hearing_facility AS metadata_most_recent_hearing_facility,
            hearings.most_recent_hearing_comments AS metadata_most_recent_hearing_comments,
            housing.facility_code AS metadata_current_facility,
            confinement.start_date AS metadata_restrictive_housing_start_date,
            bed.bed_number AS metadata_bed_number,
            bed.room_number AS metadata_room_number,
            bed.complex_number AS metadata_complex_number,
            bed.building_number AS metadata_building_number,
            bed.housing_use_code AS metadata_housing_use_code,
            IFNULL(cdv.case_notes, PARSE_JSON("[]")) AS metadata_case_notes,
        FROM base_query base
        LEFT JOIN most_recent_hearings_with_comment hearings
        USING (person_id)
        LEFT JOIN current_housing_stay housing
        USING (person_id)
        LEFT JOIN current_confinement_stay confinement
        USING (person_id)
        LEFT JOIN current_bed_stay bed
        USING (person_id)
        LEFT JOIN cdv_case_notes cdv
        USING (person_id)
    )
    SELECT * FROM final
"""

US_MO_UPCOMING_RESTRICTIVE_HOUSING_HEARING_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_MO_UPCOMING_RESTRICTIVE_HOUSING_HEARING_RECORD_VIEW_NAME,
    view_query_template=US_MO_UPCOMING_RESTRICTIVE_HOUSING_HEARING_RECORD_QUERY_TEMPLATE,
    description=US_MO_UPCOMING_RESTRICTIVE_HOUSING_HEARING_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_MO
    ),
    should_materialize=True,
    us_mo_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_MO, instance=DirectIngestInstance.PRIMARY
    ),
    us_mo_cdv_minor_months_to_display=US_MO_CDV_MINOR_NUM_MONTHS_TO_DISPLAY,
    us_mo_cdv_major_months_to_display=US_MO_CDV_MAJOR_NUM_MONTHS_TO_DISPLAY,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MO_UPCOMING_RESTRICTIVE_HOUSING_HEARING_RECORD_VIEW_BUILDER.build_and_print()
