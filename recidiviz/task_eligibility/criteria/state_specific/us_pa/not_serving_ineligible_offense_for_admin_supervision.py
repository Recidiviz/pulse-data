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
    offense_is_violent,
    statute_code_is_like,
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
            CONCAT(COALESCE(statute, ''), ' ', COALESCE(description, '')) AS offense,
            FALSE AS meets_criteria,
          FROM
            `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized`
          WHERE
            state_code = 'US_PA'
            AND (
            --18 Pa. C.S. Ch. 25 relating to Crim. Homicide
            --Per Ch 25, criminal homicide includes murder, voluntary manslaughter, & involuntary manslaughter
            ({statute_code_is_like('18', '2501')} --criminal homicide
                OR {statute_code_is_like('18', '2502')} --murder
                OR {statute_code_is_like('18', '2503')}--voluntary manslaughter
                OR {statute_code_is_like('18', '2504')} --involuntary manslaughter
                OR {statute_code_is_like('18', '2505A')} --causing suicide as criminal homicide
                OR {statute_code_is_like('18', '2505.A')}
                OR {statute_code_is_like('18', '2506')} -- drug delivery resulting in death
                OR {statute_code_is_like('18', '2507')} --criminal homicide of law enforcement officer
            OR (((description LIKE '%CR%' AND description LIKE '%HOM%')
                OR (description LIKE '%HOM%' AND description NOT LIKE '%HOME%' AND description NOT LIKE '%WHOM%')
                OR description LIKE '%MUR%'
                OR (description LIKE '%MANS%' AND NOT REGEXP_CONTAINS(description, '[A-Z]MANS')) -- exclude things like firemans, workmans
                OR description LIKE '%MNSL%'
                OR (description LIKE '%SUICIDE%' AND description LIKE '%CAUS%')
                OR description LIKE '%DEATH%' 
                OR description LIKE '%DTH%')
                AND description NOT LIKE '%ANIMAL%' AND description NOT LIKE '%CONCEAL%')) -- does not include agg cruelty to animals resulting in death or concealment of death 
                
            -- 18 Pa. C.S. Ch. 27 rel. to Assault
            OR ((({statute_code_is_like('18','2701')} --simple assault
                OR {statute_code_is_like('18','2702')} --aggravated assault
                OR {statute_code_is_like('18','2703')} --assault by prisoner
                OR {statute_code_is_like('18','2704')} --assault by life prisoner
                OR {statute_code_is_like('18','2712')} --assault on a sports official 
                OR {statute_code_is_like('18','2718')}) --strangulation
                AND (statute NOT LIKE '%27031%' AND statute NOT LIKE '%2703.1%')) -- 2703.1 refers to harassment not assault
                OR (({description_refers_to_assault()}
                OR description LIKE 'AA%' -- agg assault
                OR description LIKE '%AGG AS%'
                OR description LIKE '%AGG. AS%'
                OR (description LIKE '%AGG%' AND (description LIKE '%BOD%' OR description LIKE '%OFF%')) --agg assault w serious bodily injury to an officer
                OR (description LIKE '%AGG%' AND description LIKE '%WEA%') --agg assault w a deadly weapon
                OR description LIKE '%AGGRAVATED TO TEACHER%' -- agg assault to teacher
                OR description LIKE '%SIMP AS%' --simple assault
                OR description LIKE '%SIMPLE AS%'
                OR description LIKE '%STRANG%')
                AND (description NOT LIKE '%UNB%' AND description NOT LIKE '%VEH%'))) --does not include assault on unborn child (this is specified in a different chapter) or assault by motor vehicle
              
            -- 18 Pa. C.S. Ch. 29 rel. to Kidnapping
            OR({statute_code_is_like('18','2901')} --kidnapping
              OR {statute_code_is_like('18','2902')} --unlawful restraint 
              OR {statute_code_is_like('18','2903')} --false imprisonment 
              OR {statute_code_is_like('18','2904')} --interference with custody of children 
              OR {statute_code_is_like('18','2907')} --disposition of ransom
              OR {statute_code_is_like('18','2909')} --concealment of whereabouts of a child 
              OR {statute_code_is_like('18','2910')} --luring a child into a motor vehicle or structure 
              OR description LIKE '%KID%'
              OR description LIKE '%RESTRAIN%'
              OR (description LIKE '%UNL%' AND (description LIKE '%REST%' OR description LIKE '%RST%'))
              OR description LIKE '%IMPRIS%'
              OR (description LIKE '%INTER%' AND description LIKE '%CUS%' AND (description LIKE '%CHIL%' OR description LIKE '%CHL%'))
              OR description LIKE '%RANSOM%'
              OR (description LIKE '%CONCEAL%' AND description LIKE '%WHERE%')
              OR (description LIKE '%LUR%' AND description NOT LIKE '%FAILUR%'))
                
            -- 18 Pa. C.S. Ch. 30 rel. to Human Trafficking
            OR ({statute_code_is_like('18','3011')} -- Trafficking in individuals
              OR {statute_code_is_like('18','3012')} -- Involuntary servitude
              OR {statute_code_is_like('18','3013')} -- Patronizing a victim of sexual servitude
              OR {statute_code_is_like('18','3014')} -- Unlawful conduct regarding documents
              OR {statute_code_is_like('18','3015')} -- Nonpayment of wages
              OR {statute_code_is_like('18','3016')} -- Obstruction of justice
              OR {statute_code_is_like('18','3017')} -- Violation by business entities
              OR description LIKE '%TRAFFICK%'
              OR description LIKE '%SERVITUDE%')
     
            -- 18 Pa. C.S. Ch. 31 rel. to Sexual Offenses
            OR({statute_code_is_like('18','3121')} -- rape
                OR {statute_code_is_like('18','3122')} -- statutory rape
                OR {statute_code_is_like('18','3123')} -- involuntary deviate sexual intercourse
                OR {statute_code_is_like('18','3124')} -- sexual assault
                OR {statute_code_is_like('18','3125')} -- aggravated indecent assault
                OR {statute_code_is_like('18','3126')} -- indecent assault
                OR {statute_code_is_like('18','3127')} -- indecent exposure
                OR {statute_code_is_like('18','3128')} -- spousal sexual assault
                OR {statute_code_is_like('18','3129')} -- sexual intercourse with animal
                OR {statute_code_is_like('18','3130')} -- conduct relating to sex offenders
                OR {statute_code_is_like('18','3131')} -- unlawful dissemination of intimate image
                OR {statute_code_is_like('18','3132')} -- female mutiliation
                OR {statute_code_is_like('18','3133')} -- sexual extortion
              OR ( -- note - all descriptions relating to assault are already covered above 
                (description LIKE '%RAPE%' AND description NOT LIKE '%PARAPERNALIA%')
                OR (description LIKE '%SEX%' AND description NOT LIKE '%MAT%') -- sex offenses related to obscene materials are dealt with later 
                OR description LIKE '%IDSI%' -- involuntary deviate sexual intercourse
                OR (description LIKE '%AGG%' AND description LIKE '%IND%') -- agg indecent assault
                OR (description LIKE '%EXP%' AND description LIKE '%IND%' AND description NOT LIKE '%HINDER%') -- indecent exposure
                OR ((description LIKE '%IMG%' OR description LIKE '%IMAGE%') AND description LIKE '%INT%'))) -- unlawful dissemination of intimate image

            -- 18 Pa. C.S. Ch. 33 rel. to Arson
            OR({statute_code_is_like('18','3301')} --arson
              OR description LIKE '%ARSON%'
              OR description LIKE '%ARSN%'
              OR description LIKE '%BURN%'
              OR (description LIKE '%CONTROL%' AND description LIKE '%FAIL%')) -- failure to control or report dangerous fires
                    
            -- 18 Pa. C.S. Ch. 37 rel. to Robbery
            OR({statute_code_is_like('18','3701')} --robbery
              OR {statute_code_is_like('18','3702')} --robbery of motor vehicle
              OR (description LIKE '%ROB%' AND description NOT LIKE '%PROB%')
              OR description LIKE '%BURGLARY-MOTOR VEHICLE%' --sometimes incorrectly described as burglary instead of robbery
              OR (description LIKE '%CAR%' AND description LIKE '%JACK%'))
              
            -- 18 Pa. C.S. Ch. 49 rel. to Victim/Witness Intimidation
            OR({statute_code_is_like('18','4952')} --intimidation of witnesses or victims
              OR ((description LIKE '%INT%' AND ((description LIKE '%WIT%' AND description NOT LIKE '%WITH%') OR description LIKE '%VICT%'))))
            
            -- Any crime of violence defined in 42 Pa.C.S. §9714(g), or any attempt, conspiracy or solicitation to 
            -- commit a crime of violence defined in 42 Pa.C.S. § 9714(g), including any equivalent crime committed in another jurisdiction.
            OR {offense_is_violent()}

            -- Former 75 Pa. C.s. 3731 relating to DUI/Controlled Substance in cases involving bodily injury
            -- we actually can't check for this - statute 3731 includes all DUIs, not just cases involving bodily injuries
            -- this is now flagged in case notes, see adm_case_notes_helper fxn in us_pa_query_fragments

            -- 75 Pa.C.S. 3732 Relating to Homicide by Vehicle
            OR (({statute_code_is_like('75','3732')}
                AND NOT {statute_code_is_like('75','3732.1')}
                AND NOT {statute_code_is_like('75','37321')}) -- 3732.1 is technically assault, not homicide
              OR (description LIKE '%HOMI%' AND description LIKE '%VEH%'))

            -- 75 Pa.C.S. 3735 Relating to Homicide by Vehicle while DUI
            -- 75 Pa.C.s. 3735.1 Relating to Agg Assault by Vehicle while DUI
            OR ({statute_code_is_like('75','3735')}
              OR (({description_refers_to_assault()} OR description LIKE '%HOM%') AND (description LIKE '%DUI%' OR description LIKE '%DWI%' OR description LIKE '%INF%')))

            -- 75 Pa.C.S. 3742 Relating to accidents involving death or personal injury
            OR({statute_code_is_like('75','3742')}
              OR ((description LIKE '%ACC%' AND (description LIKE '%DEATH%' OR description LIKE '%INJ%' OR description LIKE '%DTH%'))))

            -- 75 Pa.C.S Ch. 38 Relating to driving after imbibing alcohol or utilizing drugs in cases of bodily injury.
            -- we actually can't check for this because there is not a specific statute in Ch 38 for DUI with bodily injury 
            -- instead, this is flagged in case notes, see adm_case_notes_helper fxn in us_pa_query_fragments
            
            -- 30 Pa. C.S. 5502.1 Relating to homicide by watercraft under influence of alcohol or controlled substance
            -- 30 Pa.C.S. § 5502.2 Relating to homicide by watercraft
            -- 30 Pa.C.S. § 5502.3 Relating to aggravated assault by watercraft while operating under the influence.
            -- 30 Pa.C.S. § 5502.4 Relating to aggravated assault by watercraft.
            OR ({statute_code_is_like('30','5502.1')}
              OR {statute_code_is_like('30','5502.2')}
              OR {statute_code_is_like('30','5502.3')}
              OR {statute_code_is_like('30','5502.4')}
              OR ((description LIKE '%HOM%' OR {description_refers_to_assault()}) AND description LIKE '%WATER%'))
              
            -- 18 Pa. C.S. 4302 Incest
            OR({statute_code_is_like('18','4302')}
              OR description LIKE '%INCES%')

            -- 18 Pa. C.S. 5901 Open Lewdness
            OR({statute_code_is_like('18','5901')}
              OR (description LIKE '%OPEN%' AND description LIKE '%LEWD%'))
    
            -- 18 Pa. C.S. 5902(b) Prostitution
            OR({statute_code_is_like('18','5902B')} 
                OR {statute_code_is_like('18','5902.B')}
              OR (description LIKE '%PROM%' AND description LIKE '%PROST%'))

            -- 18 Pa. C.S. 5903(4)(5)(6) obscene/sexual material/performance where the victim is minor
            -- 5903A4 & 5903A5 can be perpetrated against a non-minor (i) or a minor (ii), 5903A6 always relates to minors
            -- if (i) or (ii) is not specified, we flag in case notes - see adm_case_notes_helper fxn in us_pa_query_fragments
            OR (({statute_code_is_like('18','5903.A4II')}  -- write/print/publish obscene materials/performance
                OR {statute_code_is_like('18','5903A4II')}
                OR {statute_code_is_like('18','59023.A5II')}  -- produce/present/direct obscene materials/performance
                OR {statute_code_is_like('18','5903A5II')}
                OR {statute_code_is_like('18','5903.A6')}  -- hire/employ/use minor child
                OR {statute_code_is_like('18','5903A6')})
              OR (description LIKE '%MINOR%' AND description like '%OBSCENE%' AND (description LIKE '%WRITE%' OR description LIKE '%PRODUCE%' OR description LIKE '%HIRE%')))
                
            -- 18 Pa. C.S. Ch. 76 Internet Child Pornography
            -- note: this chapter covers offenses 7621-7630, but there is only one instance of these statute codes being used in the data (CS7622)
            -- however, child pornography (including digital) also falls under 6312, which is excluded later
            OR (({statute_code_is_like('18','7621')}
                OR {statute_code_is_like('18','7622')}
                OR {statute_code_is_like('18','7623')}
                OR {statute_code_is_like('18','7624')}
                OR {statute_code_is_like('18','7625')}
                OR {statute_code_is_like('18','7626')}
                OR {statute_code_is_like('18','7627')}
                OR {statute_code_is_like('18','7628')}
                OR {statute_code_is_like('18','7629')}
                OR {statute_code_is_like('18','7630')})
              OR (description LIKE '%PORN%' AND (description LIKE '%CHLD%' OR description LIKE '%CHILD%')))

            -- 42 Pa. C.S. §§ 9799.14, 9799.55 Megan’s Law Registration
            -- 9799.14 lists tiers of sexual offenses, and 9799.55 lists sexual offenses which require Megan's Law registration. 
            -- Most of the listed offenses are already excluded because they fall under previously listed
            -- chapters, but I'm including the few remaining ones here as well as any reference to Megan's Law 
            OR ({statute_code_is_like('18','5903.A3II')}  -- 18 Pa.C.S. § 5903(a)(3)(ii) (relating to obscene and other sexual materials and performances).
                OR {statute_code_is_like('18','5903A3II')}
                OR (description LIKE 'MINOR' AND description like '%OBSCENE%' AND description LIKE '%DESIGN%')
                OR {statute_code_is_like('18','6301.A1II')} -- 18 Pa.C.S. § 6301(a)(1)(ii) (relating to corruption of minors).
                OR {statute_code_is_like('18','6301A1II')}
                OR {statute_code_is_like('18','7507.1')}  -- 18 Pa.C.S. § 7507.1. (relating to invasion of privacy)
                OR {statute_code_is_like('18','75071')}
                OR (description LIKE '%INVASION%' AND description LIKE '%PRIVACY%')
                OR description LIKE '%MEGAN%'
                OR {statute_code_is_like('18','4915')})-- not listed specifically but relates to failing to register as a sex offender
              
            -- 18 Pa. C.S. 6312 Sexual Abuse of Children
            OR ({statute_code_is_like('18','6312')}
              OR (((description LIKE '%SEX%' AND description LIKE '%AB%')
                OR (description LIKE '%SEX%' AND description LIKE '%PHOT%')
                OR (description LIKE '%CHILD%' AND description LIKE '%SEX%'))))
              
            -- 18 Pa. C.S. 6318 Unlawful Contact with Minor
            OR({statute_code_is_like('18','6318')}
              OR (description LIKE '%CONT%' AND description LIKE '%MINOR%'))

            -- 18 Pa. C.S. 6320 Sexual Exploitation of Children
            OR({statute_code_is_like('18','6320')}
              OR (description LIKE '%EXPLOIT%' AND (description LIKE '%CHILD%' OR description LIKE '%MINOR%')))
                
            -- 42 Pa. C.S. 9712 Firearm Enhancement
            -- 204 PA Code 303.10(a) Deadly Weapon Enhancement
            -- so these are both sentencing enhancements rather than offense codes, which we don't have access to
            -- they dictate additional sentencing requirements for any offense where the person used a firearm or other deadly weapon
            -- this has been added to list of things agents still need to check 
            OR((description LIKE '%ENH%' AND (description LIKE '%WPN%' OR description LIKE '%WEA%')))

            -- 18 Pa. C.S. Firearms or Dangerous Articles
            OR (({statute_code_is_like('18','61')})
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
            -- This is not a specific offense code, but is checked in not_on_sex_offense_protocol criterion 
            -- but including here some offense descriptions that reference failure to register as an SVP 
            OR (description LIKE '%SVP%')
              
            -- Named in a PFA Order (or history of PFAs)
            -- Similarly this is not a specific offense (listed under "criteria officers need to check" in the tool) 
            -- but we can look at if they have a history of violating PFA (protection from abuse) orders
            -- (I think PFA can also stand for possessing firearm but that should also be excluded so it's ok) 
            OR description LIKE '%PFA%'
            
            -- 18 Pa.C.S. 2803 Aggravated Hazing
            OR({statute_code_is_like('18','2803')})
            -- I don't see any descriptions that relate to hazing
            )
              
            -- Removes a few cases where someone has multiple spans for the same offense so they aren't later
            -- aggregated unnecessarily
            QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id, description ORDER BY date_imposed ASC, projected_completion_date_max DESC) = 1 ),
    {create_sub_sessions_with_attributes('ineligible_spans')},
    dedup_cte AS (
        SELECT * except (offense),
            TO_JSON(STRUCT(ARRAY_AGG(DISTINCT offense ORDER BY offense) AS ineligible_offenses)) AS reason,
            ARRAY_AGG(DISTINCT offense ORDER BY offense) AS ineligible_offenses,
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
