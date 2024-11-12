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
"""
Defines a criteria view that shows spans of time when clients are not serving any Admin Supervision-ineligible sentences.
All offense info for below can be found at https://www.legis.state.pa.us/cfdocs/legis/LI/Public/cons_index.cfm by selecting
the correct title (typically 18)
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_pa_query_fragments import (
    description_refers_to_assault,
    statute_is_conspiracy_or_attempt,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_PA_NOT_SERVING_INELIGIBLE_OFFENSE_FOR_ADMIN_SUPERVISION"

_DESCRIPTION = """Defines a criteria view that shows spans of time when clients are not serving any Admin Supervision-ineligible sentences.
All offense info for below can be found at https://www.legis.state.pa.us/cfdocs/legis/LI/Public/cons_index.cfm by selecting
the correct title (typically 18)
"""

_REASON_QUERY = f"""
    WITH
      ineligible_spans AS (
          SELECT
            state_code,
            person_id,
            CASE WHEN date_imposed IS NULL OR date_imposed > CURRENT_DATE('US/Eastern') 
                THEN '1900-01-01' ELSE date_imposed END AS start_date, -- placeholder for missing or incorrect imposed dates
            CAST(NULL AS DATE) AS end_date,
            description,
            FALSE AS meets_criteria,
          FROM
            `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized`
          WHERE
            state_code = 'US_PA'
            AND description NOT LIKE '%SOL%' --criminal solicitation of crimes is not included
            AND (description IS NOT NULL OR statute IS NOT NULL) --one field can be missing but not both
            AND (
            --18 Pa. C.S. Ch. 25 relating to Crim. Homicide
            --Per Ch 25, criminal homicide includes murder, voluntary manslaughter, & involuntary manslaughter
            ((statute LIKE '%2501%' --criminal homicide
                OR statute LIKE '%2502%' --murder
                OR statute LIKE '%2503%' --voluntary manslaughter
                OR statute LIKE '%2504%' --involuntary manslaughter
                OR statute LIKE '%2505A%' --causing suicide as criminal homicide
                OR statute LIKE '%2505.A%'
                OR statute LIKE '%2507%' --criminal homicide of law enforcement officer
                OR {statute_is_conspiracy_or_attempt()}
                OR statute IS NULL)
              AND ((description LIKE '%CR%' AND description LIKE '%HOM%')
                OR description LIKE '%HOMI%'
                OR description LIKE '%MUR%'
                OR description LIKE '%MANSLAUGHTER%'
                OR description IS NULL))
                
            -- 18 Pa. C.S. Ch. 27 rel. to Assault
            OR ((statute LIKE '%2701%' --simple assault
                OR statute LIKE '%2702%' --aggravated assault
                OR statute LIKE '%2703%' --assault by prisoner
                OR statute LIKE '%2704%' --assault by life prisoner
                OR statute LIKE '%2712%' --assault on a sports official 
                OR statute LIKE '%2718%' --strangulation
                -- assuming strangulation is a form of assault but will confirm 
                -- TODO(#33544) Clarify policy nuances in admin supervision form 402
                OR {statute_is_conspiracy_or_attempt()}
                OR statute IS NULL)
              AND (statute NOT LIKE '%27031%' -- 2703.1 refers to harassment not assault
                AND statute NOT LIKE '%2703.1%')
              AND ({description_refers_to_assault()}
                OR description LIKE 'AA%' -- agg assault
                OR description LIKE '%AGG AS%'
                OR description LIKE '%AGG. AS%'
                OR (description LIKE '%AGG%' AND (description LIKE '%BOD%' OR description LIKE '%OFF%')) --agg assault w serious bodily injury to an officer
                OR (description LIKE '%AGG%' AND description LIKE '%WEA%') --agg assault w a deadly weapon
                OR description LIKE '%SIMP AS%' --simple assault
                OR description LIKE '%SIMPLE AS%'
                OR description LIKE '%STRANG%' 
                OR description IS NULL)
              AND description NOT LIKE '%VEH%' --does not include assault by motor vehicle
              AND description NOT LIKE '%UNB%') --does not include assault on unborn child (this is specified in a different chapter)
              
            -- 18 Pa. C.S. Ch. 29 rel. to Kidnapping
            OR((statute LIKE '%2901%' --kidnapping
                OR {statute_is_conspiracy_or_attempt()}
                OR statute IS NULL)
              AND (description LIKE '%KID%'
                OR description IS NULL))
     
            -- 18 Pa. C.S. Ch. 31 rel. to Sexual Assault
            OR((statute LIKE '%3121%' --rape
                OR statute LIKE '%3122%' --statutory rape
                OR statute LIKE '%3123%' --involuntary deviate sexual intercourse
                OR statute LIKE '%3124%' --sexual assault
                OR statute LIKE '%3125%' --aggravated indecent assault
                OR statute LIKE '%3126%' --indecent assault
                OR statute LIKE '%3128%' --spousal sexual assault
                OR {statute_is_conspiracy_or_attempt()}
                OR statute IS NULL)
              AND ({description_refers_to_assault()}
                OR (description LIKE '%RAPE%' AND description NOT LIKE '%PARAPERNALIA%')
                OR (description LIKE '%STAT%' AND description LIKE '%SEX%')
                OR (description LIKE '%INV%' AND description LIKE '%DEV%' AND description LIKE '%SEX%')
                OR (description LIKE '%IDSI%')
                OR (description LIKE '%AGG%' AND description LIKE '%IND%') -- agg indecent assault
                OR description IS NULL)
              AND description NOT LIKE '%VEH%' --does not include assault by motor vehicle
              AND description NOT LIKE '%UNB%') --does not include assault on unborn child (this is specified in a different chapter)

            -- 18 Pa. C.S. Ch. 33 rel. to Arson
            OR((statute LIKE '%3301%' --arson
                OR {statute_is_conspiracy_or_attempt()}
                OR statute IS NULL)
              AND (description LIKE '%ARSON%'
                OR description LIKE '%ARSN%'
                OR description IS NULL))
                    
            -- 18 Pa. C.S. Ch. 37 rel. to Robbery
            OR((statute LIKE '%3701%' --robbery
                OR statute LIKE '%3702%' --robbery of motor vehicle
                OR {statute_is_conspiracy_or_attempt()}
                OR statute IS NULL)
              AND ((description LIKE '%ROB%' AND description NOT LIKE '%PROB%')
                OR description IS NULL))
              
            -- 18 Pa. C.S. Ch. 49 rel. to Victim/Witness Intimidation
            OR((statute LIKE '%4952%' --intimidation of witnesses or victims
                OR {statute_is_conspiracy_or_attempt()}
                OR statute IS NULL)
              AND ((description LIKE '%INT%' AND ((description LIKE '%WIT%' AND description NOT LIKE '%WITH%')
                                                    OR description LIKE '%VICT%')
                OR description IS NULL))
                              
            -- 30 Pa. C.S. 5502.1 Relating to Homicide by watercraft under influence of alcohol or controlled substance
            OR ((statute LIKE '%5502.1%'
                OR statute LIKE '%55021%') --homicide descriptions with null statutes already covered by crim homicide section
              AND (description LIKE '%HOM%' AND description LIKE '%WATER%'))
              
            -- Former 75 Pa. C.s. 3731 relating to DUI/Controlled Substance in cases involving bodily injury
            -- we actually can't check for this - statute 3731 includes all DUIs, not just cases involving bodily injuries
            -- TODO(#33420) - flag cases that could make someone ineligible

            -- 75 Pa.C.S. 3732 Relating to Homicide by Vehicle
            OR(((statute LIKE '%3732%' AND statute NOT LIKE '%3732.1%' AND statute NOT LIKE '%37321%') -- 3732.1 is technically assault, not homicide
                OR {statute_is_conspiracy_or_attempt()}
                OR statute IS NULL)
              AND ((description LIKE '%HOMI%' AND description LIKE '%VEH%')
                OR description IS NULL))

            -- 75 Pa.C.S. 3735 Relating to Homicide by Vehicle while DUI
            -- 75 Pa.C.s. 3735.1 Relating to Agg Assault by Vehicle while DUI
            OR((statute LIKE '%3735%'
                OR {statute_is_conspiracy_or_attempt()}
                OR statute IS NULL)
              AND ((({description_refers_to_assault()} OR description LIKE '%HOM%') AND (description LIKE '%DUI%' OR description LIKE '%INFLUENCE%'))
                OR description IS NULL
                OR description = 'UNKNOWN'))

            -- 75 Pa.C.S. 3742 Relating to accidents involving death or personal injury
            OR((statute LIKE '%3742%'
                OR {statute_is_conspiracy_or_attempt()}
                OR statute IS NULL)
              AND ((description LIKE '%ACC%' AND (description LIKE '%DEATH%' OR description LIKE '%INJ%' OR description LIKE '%DTH%'))
                OR description IS NULL))

            -- 18 Pa. C.S. 4302 Incest
            OR((statute LIKE '%4302%'
                OR statute IS NULL)
              AND (description LIKE '%INCES%'
                OR description IS NULL))

            -- 18 Pa. C.S. 5901 Open Lewdness
            OR((statute LIKE '%5901%'
                OR statute IS NULL)
              AND ((description LIKE '%OPEN%' AND description LIKE '%LEWD%')
                OR description IS NULL))
    
            -- 18 Pa. C.S. 5902(b) Prostitution
            OR((statute LIKE '%5902B%' -- promoting prostitution
                OR statute LIKE '%5902.B%')
              OR (description LIKE '%PROM%' AND description LIKE '%PROST%'))

            -- 18 Pa. C.S. 5903(4)(5)(6) obscene/sexual material/performance where the victim is minor
            -- 5903A4 & 5903A5 can be perpetrated against a non-minor (i) or a minor (ii)
            -- but this info is not specified in the statute field or description, so we can't know for sure 
            -- TODO(#33420) - flag cases that could make someone ineligible
            OR (statute LIKE '%5903.A6%'
                OR statute LIKE '%5903A6%')
                
            -- 18 Pa. C.S. Ch. 76 Internet Child Pornography
            -- note: this chapter covers offenses 7621-7630, but there are no instances of these statute codes being
            -- used in the data or any descriptions that reference internet child pornography specifically
            -- however, child pornography (including digital) also falls under 6312, which is excluded later
            OR ((statute LIKE '%7621%'
                OR statute LIKE '%7622%'
                OR statute LIKE '%7623%'
                OR statute LIKE '%7624%'
                OR statute LIKE '%7625%'
                OR statute LIKE '%7626%'
                OR statute LIKE '%7627%'
                OR statute LIKE '%7628%'
                OR statute LIKE '%7629%'
                OR statute LIKE '%7630%')
              AND description LIKE '%PORN%')

            -- 42 Pa. C.S. 9795.1 Megan’s Law Registration
            -- this statute has been repealed, so commenting out for now 
            -- will check with TTs during next round to figure out if they're using an updated version
            -- TODO(#33544) Clarify policy nuances in admin supervision form 402
            -- OR (statute LIKE '%9795.1%'
            --      OR statute LIKE '%97951%')
              
            -- 18 Pa. C.S. 6312 Sexual Abuse of Children
            OR ((statute LIKE '%6312%'
                OR statute IS NULL))
              AND (((description LIKE '%CHILD%' AND description LIKE '%SEX%' AND description LIKE '%AB%')
                OR (description LIKE '%CHILD%' AND description LIKE '%SEX%' AND description LIKE '%PHOT%')
                OR (description LIKE '%CHILD%' AND description LIKE '%PORN%')
                OR description IS NULL)))
              
            -- 18 Pa. C.S. 6318 Unlawful Contact with Minor
            OR((statute LIKE '%6318%'
                OR statute IS NULL)
              AND ((description LIKE '%CONT%' AND description LIKE '%MINOR%')
                OR description IS NULL))

            -- 18 Pa. C.S. 6320 Sexual Abuse of Children
            OR((statute LIKE '%6320%'
                OR statute IS NULL)
              AND ((description LIKE '%EXPLOIT%' AND description LIKE '%CHILD%')
                OR description IS NULL))
                
            -- 42 Pa. C.S. 9712 Firearm Enhancement
            -- 204 PA Code 303.10(a) Deadly Weapon Enhancement
            -- can't actually find any usage of statute 9712 or any other statute that references this
            -- just going to use the description for now and can validate with TTs 
            OR((description LIKE '%ENH%' AND (description LIKE '%WPN%' OR description LIKE '%WEA%')))

            -- 18 Pa. C.S. Firearms or Dangerous Articles
            -- according to one TT, this includes anything related to firearms in title 18 
            -- for now I'm going to include everything that mentions firearms + any offenses specified in Title 18 Ch 61 - Firearms or Dangerous Articles
            -- but will validate more during next round of TT calls
            -- TODO(#33544) Clarify policy nuances in admin supervision form 402
            OR ((statute LIKE '%CC61%'
                OR statute LIKE '%18.61%')
              OR (description LIKE '%FIREARM%'
                OR description LIKE '%F/A%'
                OR description LIKE '%VUFA%' --violation of uniform firearms act
                OR description LIKE '%VOFA%'
                OR (description LIKE '%FA%' and description like '%CAR%') --carrying firearm
                OR description LIKE '%CFA%' --carrying firearm
                OR description LIKE '%NTP%' -- 6105 not to possess firearms
                OR (description LIKE '%EMERGENCY%' AND description LIKE '%WEAPON%') -- 6107 prohibited conduct during emergency related to weapon
                OR (description LIKE '%DELIV%' AND description LIKE '%WEAPON%') -- 6110 persons to whom delivery of weapon shall not be made
                OR (description LIKE '%DEAL%' AND description LIKE '%WEAPON%') -- 6112 retail dealer required to be licensed
                OR (description LIKE '%FALSE EVIDENCE OF IDENTITY%') -- 6116 false evidence of identity
                OR (description LIKE '%ALTER%' AND (description LIKE '%WEAPON%' OR description LIKE '%MARK%')) -- 6117 altering id weapon
                OR (description LIKE '%CONVEY%' AND description LIKE '%EXPL%') -- 6161 carrying explosives on conveyances
                OR (description LIKE '%SHIP%' AND description LIKE '%EXPL%')) -- 6162 shipping explosives
              AND description NOT LIKE '%FAILURE TO REPORT INJUR%') -- failure to report injury by firearm isn't really a firearm-related offense

            -- Designated as sexually violent predator
            -- We're unable to pull an indicator that confirms if someone is an SVP, but there are some offense descriptions that reference failure to register as one
            OR ((description LIKE '%SEX%'
                AND description LIKE '%VIO%'
                AND description LIKE '%PRED%')
              OR description  LIKE '%SVP%')
            )
              
            -- Removes a few cases where someone has multiple spans for the same offense so they aren't later
            -- aggregated unnecessarily
            QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id, description ORDER BY date_imposed ASC, projected_completion_date_max DESC) = 1 ),
    {create_sub_sessions_with_attributes('ineligible_spans')},
    dedup_cte AS (
        SELECT * except (description),
            TO_JSON(STRUCT( ARRAY_AGG(DISTINCT description ORDER BY description) AS ineligible_offenses)) AS reason,
            ARRAY_AGG(DISTINCT description ORDER BY description) AS ineligible_offenses,
        FROM sub_sessions_with_attributes
        GROUP BY 1,2,3,4,5
     )
    SELECT
      *
    FROM
      dedup_cte
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    criteria_spans_query_template=_REASON_QUERY,
    state_code=StateCode.US_PA,
    sessions_dataset=SESSIONS_DATASET,
    meets_criteria_default=True,
    reasons_fields=[
        ReasonsField(
            name="ineligible_offenses",
            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
            description="List of offenses that a client has committed which make them ineligible for admin supervision",
        )
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
