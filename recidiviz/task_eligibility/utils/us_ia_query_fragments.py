# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Helper fragments for IA opportunities"""


def is_veh_hom() -> str:
    # for IA ED opportunity, return whether an offense is vehicular homicide as specified in appendix A
    return """statute IN ('707.6A(1)', '707.6A(2)')"""


def is_fleeing() -> str:
    # for IA ED opportunity, return whether an offense is fleeing a scene of a crime as specified in appendix A
    # sometimes this is its own statute code, sometimes it is in the description of the vehicular offense
    return """(statute LIKE '%321.261(4)%' OR description LIKE '%321.261(4)%')"""


def is_other_ed_ineligible_offense() -> str:
    # for IA ED opportunity, return whether an offense is ineligible (excluding the vehicular homicide/fleeing cases specified above)
    return """(clean_statute IN ('707.3', '690.3') -- second degree murder (including pre-1978 statute code)
      OR clean_statute IN ('707.11', '690.6', '9999') -- attempted murder (including pre-1978 statute code and NCIC code)
      OR clean_statute = '709.3' -- second degree sexual abuse
      OR (clean_statute = '709.4' AND classification_subtype = 'A FELONY') -- second or subsequent offense of third degree sexual abuse (class A felony)
      OR ((statute LIKE '%709.8(1)(A)%' OR statute LIKE '%709.8(1)(B)%') AND classification_subtype = 'A FELONY') -- second or subsequent offense of lascivious acts with a child in violation of s. 709.8(1)(a) or (b) (class A felony)
      OR clean_statute = '711.3' -- second degree robbery
      OR clean_statute = '709.23' -- continuous sexual abuse of a child
      OR clean_statute = '710.3' -- second degree kidnapping
      OR clean_statute = '711.2' -- first degree robbery
      OR clean_statute = '712.2' -- first degree arson
      OR clean_statute = '728.12' -- sexual exploitation of a minor
      OR clean_statute = '710A.2' -- human trafficking
      OR statute LIKE '%726.6(5)%' -- child endangerment resulting in death
      OR (clean_statute = '726.6' AND description LIKE '%DEATH%') -- i think some of these 726.6(5) cases are mislabeled as 726.6(4). to cover all bases, including all child endangerment cases that refer to death 
      OR clean_statute = '902.12' OR clean_statute = '902.14' -- felony guidelines for the above crimes, sometimes used as a statute code
      OR classification_subtype = 'A FELONY' -- per section 902.1, all class A felonies should result in lifetime supervision and thus be excluded from this early discharge policy. this clause handles any edge cases where a client has a class A felony and is not caught by the not_serving_life_sentence criterion
    )"""


def case_notes_helper() -> str:
    return """
    -- All active warrants/detainers 
    SELECT DISTINCT pei.external_id,
        pei.person_id,
        'Active Warrants & Detainers' AS criteria,
        UPPER(ReleaseNotificationType) AS note_title,
        COALESCE(ContactAgency, JurisdictionType) AS note_body,
        CAST(CAST(IssuingDt AS DATETIME) AS DATE) AS event_date,
    FROM `{project_id}.{us_ia_raw_data_up_to_date_dataset}.IA_DOC_ReleaseNotifications_latest` d
    INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
      ON d.OffenderCd = pei.external_id
      AND pei.id_type = 'US_IA_OFFENDERCD'
    WHERE (ReleaseNotificationType = 'Warrant/No Hold' OR ReleaseNotificationType LIKE 'Detainer%')
        AND CloseDt IS NULL
    
    UNION ALL 
    
    -- All open interventions
    SELECT DISTINCT pei.external_id,
        pa.person_id,
        'Open Interventions' AS criteria,
        CASE WHEN pa.external_id LIKE '%PROGRAM%' AND pa.external_id LIKE '%INTERVENTION%'
                AND program_id LIKE "%##%"
            THEN SPLIT(program_id, '##')[SAFE_OFFSET(1)]
            ELSE program_id
            END AS note_title,
        CASE WHEN participation_status = 'IN_PROGRESS' 
            THEN 'In progress. Started on'
            ELSE 'Pending. Referred on'
            END AS note_body,
        COALESCE(start_date, referral_date) AS event_date,
    FROM `{project_id}.{normalized_state_dataset}.state_program_assignment` pa
    INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
        ON pa.person_id = pei.person_id
        AND pei.id_type = 'US_IA_OFFENDERCD'
    WHERE pa.state_code = 'US_IA' 
        AND pa.participation_status IN ('IN_PROGRESS', 'PENDING') -- only include open interventions 

    UNION ALL 
    
    -- List of violation incidents that occurred in the last six months that don't have an associated report
    (WITH violations AS (
        SELECT pei.external_id,
            svr.person_id,
            NULLIF(SPLIT(c.condition_raw_text, "@@")[SAFE_OFFSET(1)], "NONE") AS condition_description, -- description is sometimes unavailable and in those cases I had hydrated it as a placeholder "NONE" in ingest
            CASE WHEN c.condition = 'SUBSTANCE' THEN 'DRUG VIOLATION'
                WHEN c.condition = 'EMPLOYMENT' THEN 'EMPLOYMENT VIOLATION'
                ELSE 'NO CONDITION PROVIDED' END AS violation_type, -- violation type (employment, substance, or internal_unknown for other sources)
            JSON_VALUE(violation_metadata, '$.ViolationComments') AS ViolationComments,
            violation_date,
        FROM `{project_id}.{normalized_state_dataset}.state_supervision_violation_response` svr
        LEFT JOIN `{project_id}.{normalized_state_dataset}.state_supervision_violation` sv 
            USING(supervision_violation_id, person_id, state_code)
        LEFT JOIN `{project_id}.{normalized_state_dataset}.state_supervision_violated_condition_entry` c 
            USING(supervision_violation_id, person_id, state_code)
        INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
            ON svr.person_id = pei.person_id
            AND pei.id_type = 'US_IA_OFFENDERCD'
        WHERE svr.state_code = 'US_IA'
            -- only need to include CITATION responses since we ingest a CITATION response for every incident in the data we see in the data, 
            -- regardless of whether there is an associated report
            AND response_type = 'CITATION' 
            AND violation_date BETWEEN DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 6 MONTH) AND CURRENT_DATE('US/Eastern')
        )
    SELECT DISTINCT external_id,
        person_id,
        'Violation Incidents Dated Within the Past 6 Months',
        COALESCE(condition_description, violation_type, 'NO CONDITION PROVIDED') AS note_title,
        COALESCE(ViolationComments, 'NO DESCRIPTION PROVIDED') AS note_body,
        violation_date AS event_date,
    FROM violations)
  """
