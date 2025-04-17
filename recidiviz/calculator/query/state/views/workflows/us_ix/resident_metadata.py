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
"""Idaho resident metadata"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_exclusive_clause,
    today_between_start_date_and_nullable_end_date_exclusive_clause,
)
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SENTENCE_SESSIONS_V2_ALL_DATASET,
    SESSIONS_DATASET,
    WORKFLOWS_VIEWS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.utils.us_ix_query_fragments import ix_general_case_notes
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_IX_RESIDENT_METADATA_VIEW_NAME = "us_ix_resident_metadata"

US_IX_RESIDENT_METADATA_VIEW_DESCRIPTION = """
Idaho resident metadata
"""


US_IX_RESIDENT_METADATA_VIEW_QUERY_TEMPLATE = f"""
WITH xcrc_facility AS (
/* identify residents eligible for xcrc so they appear sorted under their current facility */
    SELECT
        person_id,
        ARRAY_AGG(DISTINCT facility ORDER BY facility) AS crc_facilities
    FROM (
        SELECT
            person_id,
            ARRAY_AGG(
                -- Prefixing facility ids with CRC since the matching location records (id_type = 'crcFacilityId')
                -- for CRC Facility search also have the prefix
                'CRC ' || UPPER(JSON_EXTRACT_SCALAR(JSON_EXTRACT(reason_struct, '$.reason'), '$.facility_name'))
                ORDER BY 
                'CRC ' || UPPER(JSON_EXTRACT_SCALAR(JSON_EXTRACT(reason_struct, '$.reason'), '$.facility_name'))
            ) AS crc_facilities
        FROM `{{project_id}}.{{task_eligibility_dataset}}.transfer_to_xcrc_request_materialized`xc, 
        UNNEST(JSON_EXTRACT_ARRAY(reasons)) AS reason_struct
        WHERE {today_between_start_date_and_nullable_end_date_exclusive_clause(
                start_date_column="start_date",
                end_date_column="end_date"
            )}
            AND JSON_EXTRACT_SCALAR(reason_struct, '$.criteria_name') = 'US_IX_IN_CRC_FACILITY_OR_PWCC_UNIT_1'
            AND (is_eligible OR is_almost_eligible)
        GROUP BY 1
    ), UNNEST(crc_facilities) AS facility
    GROUP BY person_id

),
case_notes_cte AS (
{ix_general_case_notes(where_clause_addition="AND ContactModeDesc lIKE '%CRC Request%'", 
                           criteria_str="CRC Release District case note")}
),
crc_release_district_notes AS (
/* for all residents NOT eligible for xcrc, pull their CRC Release District note if it occurs during the current
 incarceration compartment_level_1 super session. */
    SELECT
        sess.person_id,
        CONCAT('DISTRICT ', REGEXP_EXTRACT(note_title, r'\\d+')) AS release_district
    FROM case_notes_cte c
    INNER JOIN `{{project_id}}.normalized_state.state_person_external_id` pei
        ON c.external_id = pei.external_id
        AND pei.id_type = "US_IX_DOC"
    INNER JOIN `{{project_id}}.{{sessions_dataset}}.compartment_level_1_super_sessions_materialized` sess
        ON sess.person_id = pei.person_id 
        AND c.event_date BETWEEN sess.start_date AND {nonnull_end_date_exclusive_clause("sess.end_date_exclusive")}
        AND sess.compartment_level_1 = 'INCARCERATION'
    LEFT JOIN xcrc_facility xc
        ON xc.person_id = sess.person_id
    WHERE --exclude residents eligible for xcrc
        xc.person_id IS NULL
        AND {today_between_start_date_and_nullable_end_date_exclusive_clause(
            start_date_column="sess.start_date",
            end_date_column="sess.end_date_exclusive"
        )}
    --choose the most recent note during an incarceration compartment_level_1 super session
    QUALIFY ROW_NUMBER() OVER ( PARTITION BY sess.person_id ORDER BY event_date DESC ) = 1 
),
release_district AS (
/* for all residents NOT eligible for xcrc, and pull in their gender and release district if they don't have a 
 CRC Release District note */
    SELECT
        cs.person_id,
        facility,
        --proofing for if we get a value for gender that is not `MALE` or `FEMALE`. If we see `EXTERNAL_UNKNOWN` assume `MALE`
        IF(sp.gender LIKE '%FEMALE', 'FEMALE', 'MALE') AS gender,
        COALESCE(c.release_district, JSON_VALUE(ref.location_metadata, '$.supervision_district_name'), 'UNKNOWN') AS release_district
    FROM `{{project_id}}.sessions.compartment_sessions_materialized` cs
    LEFT JOIN crc_release_district_notes c
        ON cs.person_id = c.person_id
    LEFT JOIN `{{project_id}}.normalized_state.state_person` sp
        ON cs.person_id = sp.person_id 
    LEFT JOIN `{{project_id}}.normalized_state.state_incarceration_period` ip
        ON cs.person_id = ip.person_id
        AND ip.release_date IS NULL
    LEFT JOIN `{{project_id}}.reference_views.us_ix_location_metadata_materialized` ref
        ON ip.county_code = REPLACE(ref.location_external_id, 'ATLAS-', '')
        AND ref.state_code = 'US_IX'
        AND ref.location_type = 'CITY_COUNTY'
    LEFT JOIN xcrc_facility xc
        ON xc.person_id = cs.person_id
    WHERE 
        {today_between_start_date_and_nullable_end_date_exclusive_clause(
            start_date_column="cs.start_date",
            end_date_column="cs.end_date_exclusive"
        )}
        AND cs.compartment_level_1 = 'INCARCERATION'
        AND cs.state_code = "US_IX"
        --exclude residents eligible for xcrc
        AND xc.person_id IS NULL
    --get the latest incarceration period per resident if there are multiple open periods 
    QUALIFY ROW_NUMBER() OVER(PARTITION BY cs.person_id ORDER BY ip.admission_date DESC)=1
),
crc_facility AS (
    SELECT 
        person_id,
        ARRAY_AGG(DISTINCT crc_facility ORDER BY crc_facility) AS crc_facilities
    FROM (
        -- join all facilities relevant per release district
        SELECT 
            rd.person_id,
            -- Prefixing facility ids with CRC since the matching location records (id_type = 'crcFacilityId')
            -- for CRC Facility search also have the prefix
            'CRC ' || r.CRC_FACILITY AS crc_facility
        FROM release_district rd
        LEFT JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.RECIDIVIZ_REFERENCE_release_to_crc_facility_mappings_latest` r
            ON rd.RELEASE_DISTRICT = r.RELEASE_DISTRICT
            AND rd.gender = r.gender
        WHERE r.CRC_FACILITY IS NOT NULL
    
        UNION ALL
        
        -- for residents at PWCC, add PWCC as a potential CRC option since there is one crc unit at this facility
        SELECT 
            rd.person_id,
            "CRC POCATELLO WOMEN'S CORRECTIONAL CENTER" AS crc_facility
        FROM release_district rd
        WHERE rd.facility = "POCATELLO WOMEN'S CORRECTIONAL CENTER"
    ) combined
    GROUP BY person_id
    
    UNION ALL 
        
    SELECT *
    FROM xcrc_facility
)
SELECT 
    c.person_id,
    c.crc_facilities,
    p.group_projected_parole_release_date AS tentative_parole_date,
    i.initial_parole_hearing_date,
    i.next_parole_hearing_date
FROM crc_facility c
LEFT JOIN `{{project_id}}.{{analyst_views_dataset}}.us_ix_parole_dates_spans_preprocessing_materialized` i
    ON c.person_id = i.person_id
    AND {today_between_start_date_and_nullable_end_date_exclusive_clause(
            start_date_column="i.start_date",
            end_date_column="i.end_date_exclusive"
        )}
LEFT JOIN `{{project_id}}.{{sentence_sessions_v2_dataset}}.person_projected_date_sessions_materialized` p
    ON p.state_code = "US_IX"
    AND p.person_id = c.person_id
    AND {today_between_start_date_and_nullable_end_date_exclusive_clause(
            start_date_column="p.start_date",
            end_date_column="p.end_date_exclusive"
        )}

"""

US_IX_RESIDENT_METADATA_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=WORKFLOWS_VIEWS_DATASET,
    us_ix_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_IX,
        instance=DirectIngestInstance.PRIMARY,
    ),
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_IX
    ),
    sessions_dataset=SESSIONS_DATASET,
    sentence_sessions_v2_dataset=SENTENCE_SESSIONS_V2_ALL_DATASET,
    view_id=US_IX_RESIDENT_METADATA_VIEW_NAME,
    view_query_template=US_IX_RESIDENT_METADATA_VIEW_QUERY_TEMPLATE,
    description=US_IX_RESIDENT_METADATA_VIEW_DESCRIPTION,
    analyst_views_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IX_RESIDENT_METADATA_VIEW_BUILDER.build_and_print()
