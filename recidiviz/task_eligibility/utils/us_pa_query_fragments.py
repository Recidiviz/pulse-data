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
"""Helper fragments to import data for case notes in PA"""

from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    nonnull_end_date_exclusive_clause,
)


def case_when_special_case() -> str:
    return """CASE WHEN supervision_type_raw_text LIKE '%05%'
        OR supervision_type_raw_text LIKE '%04%'"""


def violations_helper() -> str:
    """pulls all violations within the last 12 months"""
    # note - we ended up removing this from sidebar due to concerns about carceral impact, but leaving helper here
    # in case we want to add back in
    return """
        SELECT pei.external_id,
            'Violations in the last 12 months' AS criteria,
            COALESCE(violation_type_raw_text, 'None') AS note_title,
            CASE WHEN violation_type_raw_text IS NULL THEN ''
              WHEN violation_type_raw_text = 'H06' THEN 'Failure to report upon release'
              WHEN violation_type_raw_text = 'H09' THEN 'Absconding'
              WHEN violation_type_raw_text = 'H04' THEN 'Pending criminal charges (UCV) Detained/Not detained'
              WHEN violation_type_raw_text = 'M20' THEN 'Conviction of Misdemeanor Offense'
              WHEN violation_type_raw_text = 'M13' THEN 'Conviction of a summary offense (a minor criminal, not civil offense)'
              WHEN violation_type_raw_text = 'M04' THEN 'Travel violations'
              WHEN violation_type_raw_text = 'H01' THEN 'Changing residence without permission'
              WHEN violation_type_raw_text = 'M02' THEN 'Failure to report as instructed'
              WHEN violation_type_raw_text = 'M19' THEN 'Failure to notify agent of arrest or citation within 72 hrs'
              WHEN violation_type_raw_text = 'L07' THEN 'Failure to notify agent of change in status/employment'
              WHEN violation_type_raw_text = 'M01' THEN 'Failure to notify agent of change in status/employment'
              WHEN violation_type_raw_text = 'L08' THEN 'Positive urine, drugs'
              WHEN violation_type_raw_text = 'M03' THEN 'Positive urine, drugs'
              WHEN violation_type_raw_text = 'H12' THEN 'Positive urine, drugs'
              WHEN violation_type_raw_text = 'H10' THEN 'Possession of offense weapon'
              WHEN violation_type_raw_text = 'H11' THEN 'Possession of firearm'
              WHEN violation_type_raw_text = 'H08' THEN 'Assaultive behavior'
              WHEN violation_type_raw_text = 'L06' THEN 'Failure to pay court ordered fees, restitution'
              WHEN violation_type_raw_text = 'L01' THEN 'Failure to participate in community service'
              WHEN violation_type_raw_text = 'L03' THEN 'Failure to pay supervision fees'
              WHEN violation_type_raw_text = 'L04' THEN 'Failure to pay urinalysis fees'
              WHEN violation_type_raw_text = 'L05' THEN 'Failure to support dependents'
              WHEN violation_type_raw_text = 'M05' THEN 'Possession of contraband, cell phones, etc.'
              WHEN violation_type_raw_text = 'M06' THEN 'Failure to take medications as prescribed'
              WHEN violation_type_raw_text = 'M07' THEN 'Failure to maintain employment'
              WHEN violation_type_raw_text = 'M08' THEN 'Failure to participate or maintain treatment'
              WHEN violation_type_raw_text = 'M09' THEN 'Entering prohibited establishments'
              WHEN violation_type_raw_text = 'M10' THEN 'Associating with gang members, co-defendants, etc'
              WHEN violation_type_raw_text = 'M11' THEN 'Failure to abide by written instructions'
              WHEN violation_type_raw_text = 'M12' THEN 'Failure to abide by field imposed special conditions'
              WHEN violation_type_raw_text = 'L02' THEN 'Positive urine, alcohol (Previous History)'
              WHEN violation_type_raw_text = 'M14' THEN 'Positive urine, alcohol (Previous History)'
              WHEN violation_type_raw_text = 'H03' THEN 'Positive urine, alcohol (Previous History)'
              WHEN violation_type_raw_text = 'M15' THEN 'Violating curfew'
              WHEN violation_type_raw_text = 'M16' THEN 'Violating electronic monitoring'
              WHEN violation_type_raw_text = 'M17' THEN 'Failure to provide urine'
              WHEN violation_type_raw_text = 'M18' THEN 'Failure to complete treatment'
              WHEN violation_type_raw_text = 'H02' THEN 'Associating with crime victims'
              WHEN violation_type_raw_text = 'H05' THEN 'Failure to abide by Board Imposed Special Conditions'
              WHEN violation_type_raw_text = 'H07' THEN 'Removal from Treatment/CCC Failure'
              ELSE 'Other' END AS note_body,
            violation_date AS event_date 
        FROM `{project_id}.{normalized_state_dataset}.state_supervision_violation` v
        LEFT JOIN `{project_id}.{normalized_state_dataset}.state_supervision_violation_type_entry` vt
            ON v.person_id = vt.person_id
            AND v.supervision_violation_id = vt.supervision_violation_id
            AND vt.state_code = 'US_PA'
        LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
            ON v.person_id = pei.person_id
            AND pei.id_type = 'US_PA_PBPP'
        WHERE v.state_code = 'US_PA'
            AND v.violation_date IS NOT NULL
            AND v.violation_date >= DATE_ADD(CURRENT_DATE("US/Pacific"), INTERVAL -1 YEAR)
        """


def statute_is_conspiracy_or_attempt() -> str:
    return """(statute LIKE '%C0901%' -- criminal attempt
              OR statute LIKE '%C0903%' -- criminal conspiracy
              OR statute LIKE '%18.901%'
              OR statute LIKE '%18.903%'
              OR statute LIKE '0901%' 
              OR statute LIKE '0903%' 
              OR statute LIKE '18901%' 
              OR statute LIKE '18903%'               
              OR statute LIKE '%CC901%' 
              OR statute LIKE '%CC903%'    
              OR statute LIKE '%CS0901%' 
              OR statute LIKE '%CS0903%'
              OR statute LIKE '%1001%')
              """


def statute_is_solicitation() -> str:
    return """(statute LIKE '%C0902%'
              OR statute LIKE '%18.902%'
              OR statute LIKE '0902%' 
              OR statute LIKE '18902%'               
              OR statute LIKE '%CC902%' 
              OR statute LIKE '%CS0902%') 
              """


def description_refers_to_assault() -> str:
    return """(description LIKE '%ASLT%'
                OR description LIKE '%AS\\'LT%'
                OR description LIKE '%ALST%'
                OR description LIKE '%ASSAU%'
                OR description LIKE '%ASSLT%'
                OR description LIKE '%ASS\\'LT%'
                OR description LIKE 'ASS%')"""


def description_refers_to_serious_bodily_injury() -> str:
    return """
        ((description LIKE '%SER%' AND (description LIKE '%INJ%' OR REGEXP_REPLACE(description, r'[^a-zA-Z]', '') LIKE '%BI%'))
        OR REGEXP_REPLACE(description, r'[^a-zA-Z ]', '') LIKE '%SBI%')
    """


def us_pa_supervision_super_sessions() -> str:
    """Custom supervision logic for time served on supervision in PA
    The supervision period stays open as long as the state says they're on supervision, unless they're simultaneously
        in general incarceration.
    In addition, we manually add shock incarceration and parole board holds since the state does not handle these consistently"""
    # TODO(#31253) - Move this upstream of prioritized super sessions
    return f"""
        WITH supervision_periods AS (
            SELECT 
                state_code,
                person_id,
                start_date,
                end_date_exclusive,
            FROM `{{project_id}}.{{sessions_dataset}}.compartment_sub_sessions_materialized`
            WHERE (open_supervision_cl1 IS NOT NULL -- client has an open supervision period
                    OR compartment_level_2 IN ('PAROLE_BOARD_HOLD', 'SHOCK_INCARCERATION')) -- or a PBH/shock inc. 
                AND compartment_level_2 <> 'GENERAL' -- but not an actual prison term
                AND state_code = 'US_PA'
        )
        /* this aggregates all of the above periods */
            SELECT
                person_id,
                state_code,
                super_session_id,
                start_date,
                end_date_exclusive,
                FROM ({aggregate_adjacent_spans(table_name='supervision_periods',
                                                session_id_output_name='super_session_id',
                                                end_date_field_name='end_date_exclusive')})  
    """


def offense_is_violent() -> str:
    # TODO(#33754) -  Move ineligible offense flags upstream
    return f"""
/* Policy note: This list of violent crimes is taken from 42 PA. C.S. §9714(g), which is a sentencing guideline for cases 
where someone has a previous violent crime on their record. You'll notice that some more severe violent crimes were left
off of this list, presumably because they automatically come with a life sentence (e.g. first degree murder is not on 
the list because it mandates a life sentence, but third degree murder is on the list). In writing this criteria, I 
included things like first degree murder because they should still be considered violent crimes for the purposes of 
special circumstances eligibility. Per policy, this also includes attempt, solicitation, or conspiracy to commit any of 
these offenses. */

/* Methodology note: This follows the same methodology as the not_serving_ineligible_offense_for_admin_supervision
criteria. In a nutshell, statute/descriptions are a bit messy and often missing. So for each violent offense, we pull
records where: 
1. the statute matches what we expect and the description is null 
2. the description matches what we expect and the statute is null
3. both match what we expect */ 

(description IS NOT NULL OR statute IS NOT NULL) --one field can be missing but not both
AND NOT (({statute_is_conspiracy_or_attempt()} OR {statute_is_solicitation()}) AND description IS NULL)
-- exclude rare cases where statute is conspiracy/attempt/solicitation but description is missing 
AND ( 
-- What the policy lists: 18 Pa.C.S. § 2502(c) Murder of the Third Degree
-- What we're going to check for: 18 Pa.C.S. § 2502 - Murder + 18 Pa.C.S. § 1102 - sentencing related to murder
    ((statute LIKE '%2502%'
        OR statute LIKE '%1102%'
        OR {statute_is_conspiracy_or_attempt()}
        OR {statute_is_solicitation()}
        OR statute IS NULL)
      AND (description LIKE '%MUR%'
        OR description IS NULL))

--18 Pa.C.S. § 2503 Voluntary Manslaughter
    OR ((statute LIKE '%2503%'
        OR {statute_is_conspiracy_or_attempt()}
        OR {statute_is_solicitation()}
        OR statute IS NULL)
      AND (((description LIKE '%MANS%' OR description LIKE '%MNSL%') AND description LIKE '%VOL%' AND description NOT LIKE '%INV%')
        OR description IS NULL))
        
-- What policy lists:
-- 18 Pa.C.S. § 2507(c) Manslaughter of a Law Enforcement Officer in The First Degree 
-- 18 Pa.C.S. § 2507(d) Manslaughter of a Law Enforcement Officer in The Second Degree
-- What we're going to check for: 
-- 18 Pa.C.S. § 2507 - including both murder and manslaughter of law enforcement officers
    OR ((statute LIKE '%2507%'
        OR {statute_is_conspiracy_or_attempt()}
        OR {statute_is_solicitation()}
        OR statute IS NULL)
      AND (((description LIKE '%MANS%' OR description LIKE '%MNSL%' OR description LIKE '%MUR%') AND description LIKE '%OFFICER%')
        OR description IS NULL))

-- What policy lists: 18 Pa.C.S. § 2604(c) Murder of The Third Degree Involving an Unborn Child
-- What we're going to check for: 18 Pa.C.S. § 2604 - including murder of all degrees involving an unborn child 
    OR ((statute LIKE '%2604%'
        OR {statute_is_conspiracy_or_attempt()}
        OR {statute_is_solicitation()}
        OR statute IS NULL)
      AND ((description LIKE '%MUR%' AND description LIKE '%UNB%')
        OR description IS NULL))

-- 18 Pa.C.S. § 2606 Aggravated Assault of An Unborn Child
    OR ((statute LIKE '%2606%'
        OR {statute_is_conspiracy_or_attempt()}
        OR {statute_is_solicitation()}
        OR statute IS NULL)
      AND (({description_refers_to_assault()} AND description LIKE '%AG%' AND description LIKE '%UNB%')
        OR description IS NULL))

-- 18 Pa.C.S. § 2702(a)(1) or (2) Aggravated Assault- Serious Bodily Injury
    OR ((statute LIKE '%2702A1%'
          OR statute LIKE '%2702.A1%'
          OR statute LIKE '%2702AI%'
          OR statute LIKE '%2702A2%'
          OR statute LIKE '%2702.A2%'
          OR statute LIKE '%2702AII%'
          OR {statute_is_conspiracy_or_attempt()}
          OR {statute_is_solicitation()}
          OR statute IS NULL)
        AND (((description LIKE '%AG%' OR description LIKE '%AA%')
          AND {description_refers_to_serious_bodily_injury()}
          AND description NOT LIKE '%FEAR%' -- fear of serious bodily injury is not included)
          AND description NOT LIKE '%ANIMAL%') -- agg cruelty to animals is not included
          OR description IS NULL))

-- 18 Pa.C.S. § 2702.1(a)(1) Assault of Law Enforcement Officer
    OR ((statute LIKE '%27021A1%'
        OR statute LIKE '%2702.1.A1%'
        OR statute LIKE '%CC2702.1%' 
        -- seems like people are just using CC2702.1 statute rather than specifying A/B most of the time
        -- just going to air on the side of including all of these since there are no instances of 2702.1(b) being used at all
        OR {statute_is_conspiracy_or_attempt()}
        OR {statute_is_solicitation()}
        OR statute IS NULL)
      AND (({description_refers_to_assault()} AND description LIKE '%OFFICER%')
        OR description IS NULL))
        
-- 18 Pa.C.S. § 2716(b) Use of Weapons of Mass Destruction
    OR (statute LIKE '%2716B%' OR statute LIKE '%2716.B%') -- there don't seem to be any examples of this actually being used 

-- 18 Pa.C.S. § 2717(b)(2) Terrorism When Graded as a Felony in the First Degree  -- there don't seem to be any examples of this actually being used 
    OR (statute LIKE '%2717B2%'
          OR statute LIKE '%2717.B2%'
          OR statute LIKE '%2717.BII%')

-- 18 Pa.C.S. § 2718 Strangulation When Graded as a Felony
-- in this case, we can pull strangulation charges but we don't know whether they were graded as felonies
-- this is now flagged in case notes, see spc_case_notes_helper fxn in us_pa_query_fragments

-- 18 Pa.C.S. § 3011 Trafficking of Persons When Graded as a Felony of the First Degree
-- only 3011.1.A1 & A2 are specified as felonies of the first degree
-- however there don't seem to be any examples of statute 3011 actually being used 
    OR (statute LIKE '%3011.A1%'
          OR statute LIKE '%3011A1%'
          OR statute LIKE '%3011.A2%'
          OR statute LIKE '%3011A2%')

-- 18 Pa.C.S. § 3121 Rape 
    OR ((statute LIKE '%3121%'
        OR {statute_is_conspiracy_or_attempt()}
        OR {statute_is_solicitation()}
        OR statute IS NULL)
      AND ((description LIKE '%RAPE%' AND description NOT LIKE '%PARAPERNALIA%' AND description NOT LIKE '%STAT%') -- statutory rape is covered under 3122
        OR description IS NULL))

-- 18 Pa.C.S. § 3123 Involuntary Deviate Sexual Intercourse
    OR ((statute LIKE '%3123%'
        OR {statute_is_conspiracy_or_attempt()}
        OR {statute_is_solicitation()}
        OR statute IS NULL)
      AND ((description LIKE '%INV%' AND description LIKE '%DEV%' AND description LIKE '%SEX%')
        OR REGEXP_REPLACE(description, r'[^a-zA-Z]', '') like '%IDSI%'
        OR description IS NULL))

-- 18 Pa.C.S. § 3125 Aggravated Indecent Assault
    OR ((statute LIKE '%3125%'
        OR {statute_is_conspiracy_or_attempt()}
        OR {statute_is_solicitation()}
        OR statute IS NULL)
      AND (((description LIKE '%AGG%' OR description LIKE '%AGRVTD%')
              AND description LIKE '%IND%'
              AND description <> '%CORRUPTION OF MINORS%') -- 3125 used to refer to corruption of minors
        OR description IS NULL))

-- 18 Pa.C.S. § 4302 Incest
    OR ((statute LIKE '%4302%'
        OR {statute_is_conspiracy_or_attempt()}
        OR {statute_is_solicitation()}
        OR statute IS NULL)
      AND (description LIKE '%INCES%'
        OR description IS NULL))

-- 18 Pa.C.S. § 3124.1 Sexual Assault
    OR ((statute LIKE '%3124.1%'
        OR statute like '%31241%'
        OR {statute_is_conspiracy_or_attempt()}
        OR {statute_is_solicitation()}
        OR statute IS NULL)
      AND (({description_refers_to_assault()}
          AND description LIKE '%SEX%'
          AND description NOT LIKE '%STAT%' -- stat sexual assault is covered in 3122
          AND description NOT LIKE '%INST%' -- institutional sexual assault is covered in 3124.2
          AND description NOT LIKE '%SPOUS%' -- spousal sexual assault is covered in 3128
          AND description NOT LIKE '%VOLUNTEER%') -- Sexual assault by sports official, volunteer or employee of nonprofit association is covered in 3124.3
        OR description IS NULL))

-- 18 Pa.C.S. § 3301(a) OR 18 Pa.C.S. §3301(a.1) Arson Endangering Persons OR Aggravated Arson
    OR ((((statute LIKE '%3301A%' OR statute LIKE '%3301.A%') AND (statute NOT LIKE '%A.2%' AND statute NOT LIKE '%A.11%'))
        OR {statute_is_conspiracy_or_attempt()}
        OR {statute_is_solicitation()}
        OR statute IS NULL)
      AND (
        ((description LIKE '%ARSON%' OR description LIKE '%ARSN%') 
          AND ((description LIKE '%END%' AND (description like '%PER%' or description like '%PRSN%') AND description NOT LIKE '%PROP%') -- endangering person 3301(a) 
          OR (description like '%INJ%' OR description LIKE '%DEA%' OR description LIKE '%DTH%') -- places another person in danger of death or bodily injury 3301(a)(1)(i)
          OR description like '%INHAB%')) -- with the purpose of destroying an inhabited building 3301(a)(1)(ii)
        OR description IS NULL))

-- 18 Pa.C.S. § 3311(b)(3) Ecoterrorism
    OR (statute LIKE '%3311B3%' OR statute LIKE '%3311.B3%') -- no examples of this actually occurring

-- 18 Pa.C.S § 2901 Kidnapping
    OR ((statute LIKE '%2901%'
        OR statute LIKE '%XX0975%' -- other random statute used for kidnapping
        OR {statute_is_conspiracy_or_attempt()}
        OR {statute_is_solicitation()}
        OR statute IS NULL)
      AND (description LIKE '%KID%'
        OR description IS NULL))

-- 18 Pa.C.S. § 3502(a)(1) Burglary- Adapted for Overnight Accommodation and Person Present (Felony of the First Degree)
    OR ((statute LIKE '%3502A1%'
        OR statute LIKE '%3502.A1%'
        OR statute = 'CC3502A2' -- based on the description i think this means 3502(a)(1)(ii), since it specifies that a person is present 
        OR {statute_is_conspiracy_or_attempt()}
        OR {statute_is_solicitation()}
        OR statute IS NULL)
      AND ((description LIKE '%BURG%'
          AND (REGEXP_REPLACE(description, r'[^a-zA-Z]', '') LIKE '%OVERNIGHTACCOMMODATIONPERSONPRESENT%'
              OR REGEXP_REPLACE(description, r'[^a-zA-Z]', '') LIKE '%OVERNIGHTACCOMMODATIONSPERSONPRESENT%'   
              OR REGEXP_REPLACE(description, r'[^a-zA-Z]', '') LIKE '%OAPP%'
              OR description LIKE '%ANY PERSON%'))
        OR description IS NULL))

-- 18 Pa.C.S. § 3701(a)(1)(i), (ii) or (iii) Robbery- Cause/Threaten to Cause Serious Bodily Injury or Threaten to Commit Felony of the First or Second Degree (Felony of the First Degree)
   OR ((statute LIKE '%CC3701A1%'
        OR statute LIKE '%CC3701A2%'
        OR statute LIKE '%CC3701A3%'
        OR statute LIKE '%3701.A1I%'
        OR statute LIKE '%3701.A1II%'
        OR statute LIKE '%3701.A1III%'
        OR statute LIKE '%3701A1I%'
        OR statute LIKE '%3701A1II%'
        OR statute LIKE '%3701A1III%'
        OR {statute_is_conspiracy_or_attempt()}
        OR {statute_is_solicitation()}
        OR statute IS NULL)
      AND (((description LIKE '%ROB%' AND description NOT LIKE '%PROB%') -- refers to robbery 
          AND ({description_refers_to_serious_bodily_injury()} -- 3701(a)(1)(i) & (ii) refer to serious bodily injury 
              OR description LIKE '%FEL%')) -- 3701(a)(1)(iii) refers to committing or threatening to commit a felony
        OR description IS NULL))

-- 18 Pa.C.S. § 3702 Robbery of a Motor Vehicle
    OR ((statute LIKE '%3702%'
        OR {statute_is_conspiracy_or_attempt()}
        OR {statute_is_solicitation()}
        OR statute IS NULL)
      AND (((description LIKE '%ROB%' AND description NOT LIKE '%PROB%')
              AND (description LIKE '%MOT%' OR REGEXP_REPLACE(description, r'[^a-zA-Z]', '') LIKE '%MV%' OR description LIKE '%VEH%'))
        OR description LIKE '%CARJACK%'
        OR description IS NULL))

-- 18 Pa.C.S. § 2506(a) Drug Delivery Resulting in Death 
-- not really sure what they mean by specifically 2506(a) here, since (a) includes the entire offense 
    OR ((statute LIKE '%2506%'
        OR {statute_is_conspiracy_or_attempt()}
        OR {statute_is_solicitation()}
        OR statute IS NULL)
      AND ((description like '%DEL%' and (description like '%DEA%' or description like '%DTH%'))
        OR description IS NULL))
)
    """


def adm_form_information_helper() -> str:
    # this pulls information used to complete form 402a, the drug addendum for admin supervision
    return """
        SELECT person_id,
        LOGICAL_OR(offense_type = 'DRUGS' 
            OR(offense_type IS NULL
                AND (description LIKE '%DRUG%'
                    OR description LIKE '%DRG%'
                    OR description LIKE '%MARIJUANA%'
                    OR description LIKE '%MARA%'
                    OR description LIKE '%METH%'
                    OR description LIKE '%COCAINE%'
                    OR description LIKE '%HALLUCINOGEN%'
                    OR description LIKE '%NARC%'
                    OR description LIKE '%VCS%' -- violation of controlled substances act 
                    OR description LIKE '%CSA%'
                    OR (description LIKE '%CONT%' AND description LIKE '%SUB%' AND description NOT LIKE '%ALC%') 
                    OR ((description LIKE '%CS%' OR description LIKE '%C/S%') 
                        AND (description LIKE '%DEL%' OR description LIKE '%POS%' OR description LIKE '%MNF%' 
                            OR description LIKE '%MANU%' OR description like '%PWI%') -- deliver, possess, manufacture, possess with intent
                        AND description NOT LIKE '%ALC%')))
                        -- cs can mean criminal solicitation or controlled substances so trying to narrow it down a bit 
            ) AS form_information_drug_charge_initial,

        -- 35 P.S. 780-113 (14) - administration/dispensary/delivery of drugs by practitioner
        LOGICAL_OR((statute LIKE '%13A14%'
                OR statute LIKE '%13.A14%')
            OR (description LIKE '%DRUG%' 
                AND description LIKE '%ADMIN%' 
                AND description LIKE '%DISP%' 
                AND description LIKE '%DEL%' 
                AND description LIKE '%PRAC%')) AS form_information_statue_14,

        -- 35 P.S. 780-113 (30) - manufacture, sale, delivery, or possession with intent to deliver 
        LOGICAL_OR((statute LIKE '%13A30%' 
                OR statute LIKE '%13.A30%')
            OR (((description LIKE '%POSS%' AND description LIKE '%INT%' AND description LIKE '%DEL%')
                    OR description LIKE '%PWI%'
                    OR description LIKE '%P/W/I%'
                    OR REGEXP_REPLACE(description, r'[^a-zA-Z0-9]', '') like '%POSSWITHINT%'
                    OR REGEXP_REPLACE(description, r'[^a-zA-Z0-9]', '') like '%POSSWINT%' 
                    OR (description LIKE '%MAN%' AND description LIKE '%SAL%' AND description LIKE '%DEL%')
                    OR description LIKE '%MSD%'
                    OR description LIKE '%M/S/D%')
                AND (description NOT LIKE '%PAR%' -- doesn't include paraphernalia 
                    AND description NOT LIKE '%NON%' -- doesn't include non-controlled substances
                    AND description NOT LIKE 'ID THEFT%'))) AS form_information_statue_30,  -- doesn't include possession of id with intent to use

        -- 35 P.S. 780-113 (37) - possessing excessive amounts of steroids
        LOGICAL_OR((statute LIKE '%13A37%'
                OR statute LIKE '%13.A37%')
            OR description LIKE '%POSSESS EXCESSIVE AMOUNTS OF STERIODS%') AS form_information_statue_37, 
            -- steroids is misspelled in the data
    FROM `{project_id}.{normalized_state_dataset}.state_charge`
    WHERE state_code = 'US_PA'
    GROUP BY 1
    """


def case_notes_helper() -> str:
    return f"""
    /* pull special conditions for the current supervision period related to treatment, evaluations, and enhanced supervision.
        also display previous conditions related to treatment, evaluations, and enhanced supervision IF the client has a 
        condition specifying that all previously imposed conditions apply */
        
    WITH prev_conditions_apply AS (
      -- pulls all clients who currently have a special condition that specifies that previous special conditions should apply 
      SELECT DISTINCT sup.person_id
      FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period` sup
      WHERE sup.state_code = 'US_PA'
        AND conditions LIKE '%PREVIOUS%'
        AND termination_date IS NULL
    )    
    SELECT DISTINCT pei.external_id,
      'Special Conditions' AS criteria,
      CASE WHEN condition LIKE '%PREVIOUS%' THEN 'PREVIOUS CONDITIONS APPLY'
        WHEN termination_date IS NOT NULL THEN 'PREVIOUS CONDITION'
        WHEN condition LIKE '%EVALUATION%' THEN 'EVALUATION' 
        WHEN condition LIKE '%ENHANCE%' THEN 'ENHANCED SUPERVISION'
        ELSE 'TREATMENT' END AS note_title,
      condition AS note_body,
      CAST(NULL AS DATE) AS event_date,
      CASE WHEN termination_date IS NULL AND condition NOT LIKE '%PREVIOUS%' THEN 0 -- first display current conditions (that aren't "previous conditions apply")
        WHEN condition LIKE '%PREVIOUS%' THEN 1 -- then display "previous conditions apply"
        ELSE 2 END AS note_order, -- then display all the previous conditions 
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period` sup
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
      ON sup.person_id = pei.person_id
      AND sup.state_code = pei.state_code
      AND id_type = 'US_PA_PBPP',
    UNNEST(SPLIT(conditions, '##')) condition
    WHERE sup.state_code = 'US_PA' 
      AND (termination_date IS NULL -- either a current special condition 
        OR sup.person_id IN (SELECT person_id FROM prev_conditions_apply)) -- or a past condition IF the client's current condition specifies that previous conditions apply) 
      AND ((condition LIKE '%TREATMENT%' AND condition LIKE '%SPECIAL CONDITION%')
        OR condition LIKE '%EVALUATION%'
        OR condition LIKE '%ENHANCE%'
        OR (condition LIKE '%PREVIOUS%' AND termination_date IS NULL)) -- display the condition "previous conditions apply" only if it is a current condition 
    
    UNION ALL 
    
    /* pull all treatments that were referred, started, or discharged during the current supervision period */ 
    (
    WITH supervision_starts AS (
      SELECT person_id, 
        start_date, 
      FROM ({us_pa_supervision_super_sessions()})
      WHERE state_code = 'US_PA'
        AND end_date_exclusive IS NULL
    )
    
    SELECT DISTINCT
      pei.external_id,
      'Treatments' AS criteria,
      JSON_EXTRACT_SCALAR(referral_metadata, "$.PROGRAM_NAME") AS note_title,
      CASE WHEN participation_status_raw_text IN ('ASSIGNED')
            THEN CONCAT('Assigned - Referred on ', COALESCE(CAST(referral_date AS STRING), 'Unknown Date')) 
          WHEN participation_status IN ('DISCHARGED_SUCCESSFUL') AND discharge_date IS NOT NULL
            THEN CONCAT('Completed - Discharged on ', COALESCE(CAST(discharge_date AS STRING), 'Unknown Date')) 
          WHEN participation_status IN ('DISCHARGED_SUCCESSFUL')
            THEN CONCAT('Completed - Referred on ', COALESCE(CAST(referral_date AS STRING), 'Unknown Date')) 
            -- using referral date here because sometimes the discharge date is missing in old OMS       
          WHEN participation_status_raw_text IN ('IN PROGRESS') 
          -- note - using raw text here means we only display in progress records from vantage. in progress records from the old OMS are probably not 
          -- still actually in progress (since it stopped being used in 2022) so displaying those records in "unknown status" bucket instead
            THEN CONCAT('In progress - Started on ', COALESCE(CAST(tre.start_date AS STRING), 'Unknown Date')) 
          WHEN participation_status IN ('DISCHARGED_UNSUCCESSFUL', 'REFUSED') AND discharge_date IS NOT NULL
            THEN CONCAT('Failed to complete - Discharged on ', COALESCE(CAST(discharge_date AS STRING), 'Unknown Date')) 
          WHEN participation_status IN ('DISCHARGED_UNSUCCESSFUL') 
            THEN CONCAT('Failed to complete - Referred on ', COALESCE(CAST(referral_date AS STRING), 'Unknown Date'))   
            -- using referral date here because sometimes the discharge date is missing in old OMS       
          WHEN participation_status_raw_text IN ('DISCHARGED PRIOR TO COMPLETION')
            THEN CONCAT('Discharged prior to completion - Discharged on ', COALESCE(CAST(discharge_date AS STRING), 'Unknown Date')) 
          ELSE CONCAT('Unknown Status - Referred on ', COALESCE(CAST(referral_date AS STRING), 'Unknown Date')) 
        END AS note_body,
      CASE WHEN participation_status_raw_text IN ('IN PROGRESS') THEN tre.start_date
          WHEN (participation_status IN ('DISCHARGED_SUCCESSFUL', 'DISCHARGED_UNSUCCESSFUL', 'REFUSED') AND discharge_date IS NOT NULL)
            OR participation_status_raw_text IN ('DISCHARGED PRIOR TO COMPLETION') THEN discharge_date
          ELSE referral_date
        END AS event_date,
        NULL as note_order,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_program_assignment` tre
    LEFT JOIN supervision_starts sup 
      ON sup.person_id = tre.person_id
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
      ON tre.person_id = pei.person_id
      AND tre.state_code = pei.state_code
      AND id_type = 'US_PA_PBPP'
    WHERE (COALESCE(tre.discharge_date, tre.start_date, tre.referral_date) >= sup.start_date -- only display treatments during current supervision period 
            OR tre.person_id IN (SELECT person_id FROM prev_conditions_apply)) -- UNLESS special conditions from a previous term apply, then we display all past treatments 
    ) 
    
    UNION ALL 
    
    /* pull all currently open employment periods */ 
    SELECT DISTINCT
      pei.external_id,
      'Employment' AS criteria,
      CASE WHEN employment_status = 'EMPLOYED_FULL_TIME' THEN 'EMPLOYED - FULL-TIME'
        WHEN employment_status = 'EMPLOYED_PART_TIME' THEN 'EMPLOYED - PART-TIME'
        WHEN employment_status_raw_text = 'UABLE' THEN 'UNEMPLOYED'
        WHEN employment_status_raw_text = 'UNRUI' THEN 'UNEMPLOYED - RECEIVES UNEARNED INCOME'
        WHEN employment_status_raw_text = 'UNSTU' THEN 'UNEMPLOYED - STUDENT'
        WHEN employment_status_raw_text = 'UTWRK' THEN 'UNEMPLOYED - UNABLE TO WORK'
        ELSE 'EMPLOYED' END AS note_title,
      CASE WHEN employment_status IN ('EMPLOYED_FULL_TIME', 'EMPLOYED_PART_TIME') THEN COALESCE(employer_name, 'Unknown Employer') ELSE '' END AS note_body,
      start_date AS event_date,
      NULL as note_order,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_employment_period` emp
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
      ON emp.person_id = pei.person_id
      AND emp.state_code = pei.state_code
      AND id_type = 'US_PA_PBPP'
    WHERE emp.state_code = 'US_PA'
      AND end_date IS NULL
    """


def adm_case_notes_helper() -> str:
    # this pulls all pa case notes and adds a few that should only be displayed for the admin supervision opportunity
    return f"""
    SELECT * FROM ({case_notes_helper()})
    
    UNION ALL
    
    /* pulls all potential barriers to eligibility - these are gray area cases that we want to flag to the agent because
    we don't have all the information we need to determine whether it should change eligibility */
    SELECT DISTINCT pei.external_id,
      'Potential Barriers to Eligibility' AS criteria,
      CASE WHEN statute LIKE '%3731%' THEN 'DUI' 
        WHEN statute LIKE '%5903%' THEN 'OBSCENE MATERIALS'
        ELSE 'DRUG' 
        END AS note_title,
      CASE WHEN statute like '%3731%' THEN 'This reentrant has 75 C.S. 3731 relating to DUI on their criminal record. They would be ineligible for admin supervision if this charge resulted in bodily injury. Check criminal history for bodily injury and update eligibility accordingly.' 
        WHEN (statute LIKE '%5903.A4%' OR statute LIKE '%5903A4%') THEN 'This reentrant has 18 C.S. 5903(4) relating to obscene materials on their criminal record. They would be ineligible for admin supervision if the victim of this charge was a minor. Check criminal history for minor victim and update eligibility accordingly.'
        WHEN (statute LIKE '%5903.A5%' OR statute LIKE '%5903A5%') THEN 'This reentrant has 18 C.S. 5903(5) relating to obscene performance on their criminal record. They would be ineligible for admin supervision if the victim of this charge was a minor. Check criminal history for minor victim and update eligibility accordingly.'
        WHEN form_information_statue_14 THEN 'This reentrant has 35 P.S. 780-113(14) relating to controlled substances on their criminal record. They could be ineligible for admin supervision if certain sentencing enhancements apply. Click “complete checklist” and scroll down to the drug addendum to determine eligibility.' 
        WHEN form_information_statue_30 THEN 'This reentrant has 35 P.S. 780-113(30) relating to controlled substances on their criminal record. They could be ineligible for admin supervision if certain sentencing enhancements apply. Click “complete checklist” and scroll down to the drug addendum to determine eligibility.'
        WHEN form_information_statue_37 THEN 'This reentrant has 35 P.S. 780-113(37) relating to steroids on their criminal record. They could be ineligible for admin supervision if certain sentencing enhancements apply. Click “complete checklist” and scroll down to the drug addendum to determine eligibility.'
        END AS note_body,
      CAST(NULL AS DATE) AS event_date,
      NULL as note_order,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_charge` sc
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
      ON sc.person_id = pei.person_id
      AND sc.state_code = pei.state_code
      AND id_type = 'US_PA_PBPP'
    LEFT JOIN ({adm_form_information_helper()}) form
      ON sc.person_id = form.person_id
    WHERE sc.state_code = 'US_PA' 
      AND (sc.statute LIKE '%3731%' 
        OR (statute LIKE '%5903.A4%' OR statute LIKE '%5903A4%' OR statute LIKE '%5903.A5%' OR statute LIKE '%5903A5%')
        OR form.form_information_statue_14
        OR form.form_information_statue_30
        OR form.form_information_statue_37)
    """


def spc_case_notes_helper() -> str:
    # this pulls all pa case notes and adds one that should only be displayed for the special circumstances opportunity
    return f"""
    SELECT * FROM ({case_notes_helper()})

    UNION ALL

    /* pulls all potential barriers to eligibility - these are gray area cases that we want to flag to the agent because
    we don't have all the information we need to determine whether it should change eligibility */
    SELECT DISTINCT pei.external_id,
      'Potential Barriers to Eligibility' AS criteria,
      'STRANGULATION' AS note_title,
      'This reentrant has 18 C.S. 2718 relating to strangulation on their criminal record. Check criminal history: if this charge was graded as a felony, the reentrant is considered a violent case and must serve 5 years rather than 3 years on supervision before becoming eligible' AS note_body,
      CAST(NULL AS DATE) AS event_date,
      NULL as note_order,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_charge` sc
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
      ON sc.person_id = pei.person_id
      AND sc.state_code = pei.state_code
      AND id_type = 'US_PA_PBPP'
    INNER JOIN `{{project_id}}.{{criteria_dataset}}.meets_special_circumstances_criteria_for_time_served_materialized` crit
      ON sc.person_id = crit.person_id
      AND sc.state_code = crit.state_code 
      AND CURRENT_DATE('US/Eastern') BETWEEN crit.start_date AND {nonnull_end_date_exclusive_clause('crit.end_date')} 
    WHERE sc.state_code = 'US_PA' 
      AND (sc.statute LIKE '%2718%' OR sc.description LIKE '%STRANG%')
      AND JSON_EXTRACT_SCALAR(crit.reason, "$.case_type") = 'non-life sentence (non-violent case)' -- only pull cases that would otherwise be considered non-violent
    """
