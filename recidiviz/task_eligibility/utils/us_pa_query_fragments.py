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
"""Helper fragments to import data for case notes in PA"""

from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    nonnull_end_date_exclusive_clause,
    sessionize_ledger_data,
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
                OR description LIKE '%A&B%' -- assault and battery 
                OR description LIKE '%ASSL%'
                )"""


def description_refers_to_serious_bodily_injury() -> str:
    return """
        ((description LIKE '%SER%' AND (description LIKE '%INJ%' OR REGEXP_REPLACE(description, r'[^a-zA-Z]', '') LIKE '%BI%'))
        OR REGEXP_REPLACE(description, r'[^a-zA-Z ]', '') LIKE '%SBI%')
    """


def us_pa_supervision_super_sessions() -> str:
    """Custom logic for time served on supervision in PA. This fxn creates spans of time that a reentrant
    has a given raw data release date, which is what agents use to determine time spent on supervision."""
    # TODO(#37715) - Pull this from sentencing once sentencing v2 is implemented in PA
    return f"""
        WITH release_dates AS (
            SELECT state_code,
                person_id,
                DATE(update_datetime) AS update_date,
                DATE(CAST(RelReleaseDateYear AS INT64), CAST(RelReleaseDateMonth AS INT64), CAST(RelReleaseDateDay AS INT64)) AS release_date,
            FROM `{{project_id}}.{{us_pa_raw_data_dataset}}.dbo_Release` rel
            LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei 
              ON rel.ParoleNumber = pei.external_id
              AND pei.id_type = 'US_PA_PBPP'
        )
        SELECT * 
        FROM ({sessionize_ledger_data(table_name = 'release_dates', index_columns = ['state_code', 'person_id'], update_column_name = 'update_date', attribute_columns = ['release_date'])})
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

(
-- What the policy lists: 18 Pa.C.S. § 2502(c) Murder of the Third Degree
-- What we're going to check for: 18 Pa.C.S. § 2502 - Murder + 2501 - Criminal homicide
-- Note: 2501 is not specifically listed, but I've gotten denials from TTs from people who have homicide charges, and it seems logical to me that homicide should be considered violent unless an explicitly accidental case (like a DUI) 
    ({statute_code_is_like('18', '2502')}
      OR {statute_code_is_like('18', '2501')}
      OR description LIKE '%MUR%'
      OR ((description LIKE '%HOM%' AND description NOT LIKE '%HOME%' AND description NOT LIKE '%WHOM%')
        AND NOT ({dui_indicator()} OR description LIKE '%VEH%' OR description LIKE '%WATER%')))

--18 Pa.C.S. § 2503 Voluntary Manslaughter
    OR ({statute_code_is_like('18', '2503')}
      OR ((description LIKE '%MANS%' OR description LIKE '%MNSL%') AND description LIKE '%VOL%' AND description NOT LIKE '%INV%'))
        
-- What policy lists:
-- 18 Pa.C.S. § 2507(c) Manslaughter of a Law Enforcement Officer in The First Degree
-- 18 Pa.C.S. § 2507(d) Manslaughter of a Law Enforcement Officer in The Second Degree
-- What we're going to check for:
-- 18 Pa.C.S. § 2507 - including both murder and manslaughter of law enforcement officers
    OR ({statute_code_is_like('18', '2507')}
      OR ((description LIKE '%MANS%' OR description LIKE '%MNSL%' OR description LIKE '%MUR%') AND description LIKE '%OFFICER%'))

-- What policy lists: 18 Pa.C.S. § 2604(c) Murder of The Third Degree Involving an Unborn Child
-- What we're going to check for: 18 Pa.C.S. § 2604 - including murder of all degrees involving an unborn child
    OR ({statute_code_is_like('18', '2604')}
      OR (description LIKE '%MUR%' AND description LIKE '%UNB%'))

-- 18 Pa.C.S. § 2606 Aggravated Assault of An Unborn Child
    OR ({statute_code_is_like('18', '2606')}
      OR ({description_refers_to_assault()} AND description LIKE '%AG%' AND description LIKE '%UNB%'))

-- 18 Pa.C.S. § 2702(a)(1) or (2) Aggravated Assault- Serious Bodily Injury
    OR ({statute_code_is_like('18', '2702A1')}
      OR {statute_code_is_like('18', '2702.A1')}
      OR {statute_code_is_like('18', '2702AI')}
      OR {statute_code_is_like('18', '2702A2')}
      OR {statute_code_is_like('18', '2702.A2')}
      OR {statute_code_is_like('18', '2702AII')}
      OR ((description LIKE '%AG%' OR description LIKE '%AA%')
          AND {description_refers_to_serious_bodily_injury()}
          AND description NOT LIKE '%FEAR%' -- fear of serious bodily injury is not included)
          AND description NOT LIKE '%ANIMAL%')) -- agg cruelty to animals is not included

-- 18 Pa.C.S. § 2702.1(a)(1) Assault of Law Enforcement Officer
    OR ({statute_code_is_like('18', '27021A1')}
      OR {statute_code_is_like('18', '2702.1.A1')}
      OR statute LIKE '%CC2702.1%'
        -- seems like people are just using CC2702.1 statute rather than specifying A/B most of the time
        -- just going to air on the side of including all of these since there are no instances of 2702.1(b) being used at all
      OR ({description_refers_to_assault()} AND description LIKE '%OFFICER%'))
        
-- 18 Pa.C.S. § 2716(b) Use of Weapons of Mass Destruction
    OR ({statute_code_is_like('18', '2716B')}
      OR {statute_code_is_like('18', '2716.B')}
      OR (description LIKE '%WEAP%' AND description LIKE '%MASS%' AND description LIKE '%CAUS%'))

-- 18 Pa.C.S. § 2717(b)(2) Terrorism When Graded as a Felony in the First Degree  -- there don't seem to be any examples of this actually being used
    OR ({statute_code_is_like('18', '2717B2')}
      OR {statute_code_is_like('18', '2717.B2')})

-- 18 Pa.C.S. § 2718 Strangulation When Graded as a Felony
-- in this case, we can pull strangulation charges but we don't know whether they were graded as felonies
-- this is now flagged in case notes, see spc_case_notes_helper fxn in us_pa_query_fragments

-- 18 Pa.C.S. § 3011 Trafficking of Persons When Graded as a Felony of the First Degree
-- 3011.A1 & A2 are specified as felonies of the first degree, 3011.B relates to sentencing of 3011.A1 & A2 when the victim is a minor
    OR ({statute_code_is_like('18', '3011.A1')} -- sexual servitude
      OR {statute_code_is_like('18', '3011A1')}
      OR {statute_code_is_like('18', '3011.A2')} -- financial benefit
      OR {statute_code_is_like('18', '3011A2')}
      OR {statute_code_is_like('18', '3011.B')} -- trafficking in minors
      OR {statute_code_is_like('18', '3011B')}
      OR (description LIKE '%TRAFFICK%' AND (description LIKE '%SEX%' OR description LIKE '%FINANCIAL%' OR description LIKE '%MINOR%')))

-- 18 Pa.C.S. § 3121 Rape
    OR ({statute_code_is_like('18', '3121')}
      OR (description LIKE '%RAPE%' AND description NOT LIKE '%PARAPERNALIA%' AND description NOT LIKE '%STAT%')) -- statutory rape is covered under 3122

-- 18 Pa.C.S. § 3123 Involuntary Deviate Sexual Intercourse
    OR ({statute_code_is_like('18', '3123')}
      OR (description LIKE '%INV%' AND description LIKE '%DEV%' AND description LIKE '%SEX%')
      OR REGEXP_REPLACE(description, r'[^a-zA-Z]', '') LIKE '%IDSI%')

-- 18 Pa.C.S. § 3125 Aggravated Indecent Assault
    OR (({statute_code_is_like('18', '3125')} AND NOT description LIKE '%CORRUPTION%') -- 3125 used to refer to corruption of minors
      OR ((description LIKE '%AGG%' OR description LIKE '%AGRVTD%') AND description LIKE '%IND%'))

-- 18 Pa.C.S. § 4302 Incest
    OR({statute_code_is_like('18','4302')}
      OR description LIKE '%INCES%')

-- What the policy says: 18 Pa.C.S. § 3124.1 Sexual Assault
-- What we're going to check for: 18 Pa.C.S. § 3124.1 Sexual Assault, 3124.2 Institutional Sexual Assault, 3124.3 Sexual assault by sports official, volunteer or employee of nonprofit association.
    OR({statute_code_is_like('18','3124.1')}
      OR {statute_code_is_like('18','31241')}
      OR ({description_refers_to_assault()}
          AND description LIKE '%SEX%'
          AND description NOT LIKE '%STAT%')) -- stat sexual assault is covered in 3122

-- 18 Pa.C.S. § 3301(a) OR 18 Pa.C.S. §3301(a.1) Arson Endangering Persons OR Aggravated Arson
    OR ((({statute_code_is_like('18','3301A')} OR {statute_code_is_like('18','3301.A')}) 
            AND (statute NOT LIKE '%A.2%' AND statute NOT LIKE '%A.11%'))
      OR ((description LIKE '%ARSON%' OR description LIKE '%ARSN%')
          AND ((description LIKE '%END%' AND (description like '%PER%' or description like '%PRSN%') AND description NOT LIKE '%PROP%') -- endangering person 3301(a)
            OR (description like '%INJ%' OR description LIKE '%DEA%' OR description LIKE '%DTH%')))) -- places another person in danger of death or bodily injury 3301(a)(1)(i)

-- 18 Pa.C.S. § 3311(b)(3) Ecoterrorism
    OR ({statute_code_is_like('18','3311B3')}
      OR {statute_code_is_like('18','3311.B3')}) -- no examples of this actually occurring

-- 18 Pa.C.S § 2901 Kidnapping
    OR({statute_code_is_like('18','2901')} --kidnapping
      OR description LIKE '%KID%')

-- 18 Pa.C.S. § 3502(a)(1) Burglary- Adapted for Overnight Accommodation and Person Present (Felony of the First Degree)
    OR ({statute_code_is_like('18','3502A1')}
      OR {statute_code_is_like('18','3502.A1')}
      OR ((description LIKE '%BURG%'
          AND (REGEXP_REPLACE(description, r'[^a-zA-Z]', '') LIKE '%OVERNIGHTACCOMMODATIONPERSONPRESENT%'
              OR REGEXP_REPLACE(description, r'[^a-zA-Z]', '') LIKE '%OVERNIGHTACCOMMODATIONSPERSONPRESENT%' 
              OR REGEXP_REPLACE(description, r'[^a-zA-Z]', '') LIKE '%OAPP%'
              OR description LIKE '%ANY PERSON%')
          AND description NOT LIKE '%NO%')))

-- 18 Pa.C.S. § 3701(a)(1)(i), (ii) or (iii) Robbery- Cause/Threaten to Cause Serious Bodily Injury or Threaten to Commit Felony of the First or Second Degree (Felony of the First Degree)
   OR ((({statute_code_is_like('18','3701A1I')} AND NOT {statute_code_is_like('18','3701A1IV')})
      OR {statute_code_is_like('18','3701A1II')}
      OR {statute_code_is_like('18','3701A1III')}
      OR ({statute_code_is_like('18','3701.A1I')} AND NOT {statute_code_is_like('18','3701.A1IV')})
      OR {statute_code_is_like('18','3701.A1II')}
      OR {statute_code_is_like('18','3701.A1II')}
      OR ((description LIKE '%ROB%' AND description NOT LIKE '%PROB%') -- refers to robbery
          AND ({description_refers_to_serious_bodily_injury()} -- 3701(a)(1)(i) & (ii) refer to serious bodily injury
              OR description LIKE '%FEL%')))) -- 3701(a)(1)(iii) refers to committing or threatening to commit a felony

-- 18 Pa.C.S. § 3702 Robbery of a Motor Vehicle
    OR ({statute_code_is_like('18','3702')}
      OR ((description LIKE '%ROB%' AND description NOT LIKE '%PROB%')
              AND (description LIKE '%MOT%' OR REGEXP_REPLACE(description, r'[^a-zA-Z]', '') LIKE '%MV%' OR description LIKE '%VEH%'))
        OR description LIKE '%CARJACK%')

-- 18 Pa.C.S. § 2506(a) Drug Delivery Resulting in Death
-- not really sure what they mean by specifically 2506(a) here, since (a) includes the entire offense
    OR ({statute_code_is_like('18','2506')}
      OR (description like '%DEL%' and (description like '%DEA%' or description like '%DTH%')))
)
    """


def offense_is_admin_ineligible() -> str:
    # TODO(#33754) -  Move ineligible offense flags upstream
    return f"""(
    /* defines offenses that are ineligible for admin supervision per form 402 */
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
            OR (((description LIKE '%HOM%' AND description NOT LIKE '%HOME%' AND description NOT LIKE '%WHOM%')
                OR description LIKE '%MUR%'
                OR (description LIKE '%MANS%' AND NOT REGEXP_CONTAINS(description, '[A-Z]MANS')) -- exclude things like firemans, workmans
                OR description LIKE '%MNSL%'
                OR (description LIKE '%SUICIDE%' AND description LIKE '%CAUS%')
                OR description LIKE '%DEATH%' 
                OR (description LIKE '%DTH%' AND description NOT LIKE '%WIDTH%')
                OR description LIKE '%KILL%')
                AND description NOT LIKE '%ANIMAL%' AND description NOT LIKE '%DOG%' AND description NOT LIKE '%GAME%' AND description NOT LIKE '%BIRD%' -- does not include killing animals/big game
                AND description NOT LIKE '%CONCEAL%' AND description NOT LIKE '%CONCL%' -- does not include concealment of death 
                AND description NOT LIKE '%FAIL%')) -- does not include failure to register vehicle following death of owner or failure to report death
                
            -- 18 Pa. C.S. Ch. 27 rel. to Assault
            OR ((({statute_code_is_like('18','2701')} --simple assault
                OR {statute_code_is_like('18','2702')} --aggravated assault
                OR {statute_code_is_like('18','2703')} --assault by prisoner
                OR {statute_code_is_like('18','2704')} --assault by life prisoner
                OR {statute_code_is_like('18','2711')} --domestic violence 
                OR {statute_code_is_like('18','2712')} --assault on a sports official 
                OR {statute_code_is_like('18','2718')}) --strangulation
                AND (statute NOT LIKE '%27031%' AND statute NOT LIKE '%2703.1%')) -- 2703.1 refers to harassment not assault
                OR (({description_refers_to_assault()}
                OR description LIKE 'AA%' -- agg assault
                OR description LIKE '%AGG AS%'
                OR description LIKE '%AGG. AS%'
                OR (description LIKE '%AGG%' AND (description LIKE '%BOD%' OR description LIKE '%OFFICER%' OR description LIKE '%BI%') AND description NOT LIKE '%ANIMAL%') --agg assault w serious bodily injury to an officer
                OR (description LIKE '%AGG%' AND description LIKE '%WEA%') --agg assault w a deadly weapon
                OR description LIKE '%AGGRAVATED TO TEACHER%' -- agg assault to teacher
                OR description LIKE '%SIMP AS%' --simple assault
                OR description LIKE '%SIMPLE AS%'
                OR (description LIKE '%PRISONER%' AND description LIKE '%ASST%') -- assault by prisoner
                OR description LIKE '%STRANG%'
                OR ((description LIKE '%VIOL%' OR description LIKE '%ABUSE%') AND (description LIKE '%DOMESTIC%' OR description LIKE '%DOMSTC%'))
                OR description LIKE '%BATTERY%')
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
              OR description = 'CRIMINAL RESTRAINT' -- NJ code for unlawful restraint
              OR ((description LIKE '%UNL%' OR description LIKE '%U/L%' OR description LIKE '%UN/%' OR description LIKE '%FEL%')
                AND (description LIKE '%REST%' OR description LIKE '%RST%') 
                AND description NOT LIKE '%RESIST%' AND description NOT LIKE '%RESTITUTION%') -- unlawful restraint, excluding unlawfully resisting arrest charge
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
              OR (description LIKE '%TRAFFICK%' AND description NOT LIKE '%DRUG%')
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
                OR (description LIKE '%SEX%'
                    AND description NOT LIKE '%MAT%' -- sex offenses related to obscene materials are dealt with later 
                    AND description NOT LIKE '%TRANSMISSION%' -- transmission of sexually explicit images by minor is not included 
                    AND description NOT LIKE '%COMMERCIAL%') -- commercial sex not included
                OR description LIKE '%IDSI%' -- involuntary deviate sexual intercourse
                OR (description LIKE '%AGG%' AND description LIKE '%IND%') -- agg indecent assault
                OR (description LIKE '%EXP%' AND description LIKE '%IND%' AND description NOT LIKE '%HINDER%') -- indecent exposure
                OR description LIKE '%INDECENT%'
                OR ((description LIKE '%IMG%' OR description LIKE '%IMAGE%') AND description LIKE '%INT%')) -- unlawful dissemination of intimate image
                OR description LIKE '%MOLEST%' -- molestation (used by other states)
                OR description LIKE '%TOUCHING%') -- forcible touching (used by other states)

            -- 18 Pa. C.S. Ch. 33 rel. to Arson
            OR({statute_code_is_like('18','3301')} --arson
              OR description LIKE '%ARSON%'
              OR description LIKE '%ARSN%'
              OR description LIKE '%BURN%'
              OR (description LIKE '%CONTROL%' AND description LIKE '%FAIL%' AND description LIKE '%FIRE%')) -- failure to control or report dangerous fires
                    
            -- 18 Pa. C.S. Ch. 37 rel. to Robbery
            OR({statute_code_is_like('18','3701')} --robbery
              OR {statute_code_is_like('18','3702')} --robbery of motor vehicle
              OR (description LIKE '%ROB%' AND description NOT LIKE '%PROB%')
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
              OR (description LIKE '%PROM%' AND description LIKE '%PROST%' AND (statute IS NULL OR NOT {statute_code_is_like('18','5902.A')})))
              -- for some reason there are records where the statute is 5902.A (engaging in prostitution) but the description is promoting prostitution (which should be 5902.B)
              
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
                OR {statute_code_is_like('18','4915')} OR {statute_code_is_like('42','979')})  -- not listed specifically but relates to sex offender registration requirements

            -- 18 Pa. C.S. 6312 Sexual Abuse of Children
            OR ({statute_code_is_like('18','6312')}
              OR (((description LIKE '%SEX%' AND description LIKE '%AB%')
                OR (description LIKE '%SEX%' AND description LIKE '%PHOT%')
                OR (description LIKE '%CHILD%' AND description LIKE '%SEX%'))))

            -- 18 Pa. C.S. 6318 Unlawful Contact with Minor
            OR({statute_code_is_like('18','6318')}
              OR (description LIKE '%CONT%' AND description NOT LIKE '%CONTR%' AND description LIKE '%MINOR%'))

            -- 18 Pa. C.S. 6320 Sexual Exploitation of Children
            OR({statute_code_is_like('18','6320')}
              OR (description LIKE '%EXPLOIT%' AND (description LIKE '%CHILD%' OR description LIKE '%MINOR%')))
                
            -- 42 Pa. C.S. 9712 Firearm Enhancement
            -- 204 PA Code 303.10(a) Deadly Weapon Enhancement
            -- so these are both sentencing enhancements rather than offense codes, which we don't have access to
            -- they dictate additional sentencing requirements for any offense where the person used a firearm or other deadly weapon
            -- this has been added to list of things agents still need to check 
            OR(description LIKE '%ENH%' AND (description LIKE '%WPN%' OR description LIKE '%WEA%'))

            -- 18 Pa. C.S. Firearms or Dangerous Articles
            OR (({statute_code_is_like('18','61')})
              OR ((description LIKE '%FIREARM%'
                OR (description LIKE '%GUN%' AND description NOT LIKE '%PAINT%')
                OR description LIKE '%F/A%'
                OR description LIKE '%FRARM%'
                OR description LIKE '%FARM%'
                OR REGEXP_CONTAINS(description, '([^A-Z]FA[^A-Z])|(^FA[^A-Z])|([^A-Z]FA$)') -- contains FA (firearm) with no following/preceding letters. for example, we want to include "carrying FA w/o license", but not "welFAre" or "FAcility" 
                OR REGEXP_CONTAINS(description, '([^A-Z]UFA[^A-Z])|(^UFA[^A-Z])|([^A-Z]UFA$)') -- same logic as above for UFA (uniform firearm act)
                OR description LIKE '%VUFA%' --violation of uniform firearms act
                OR description LIKE '%VOFA%'
                OR description LIKE '%CFA%' --carrying firearm
                OR description LIKE '%NTP%' -- 6105 not to possess firearms
                OR (description LIKE '%EMERGENCY%' AND description LIKE '%WEAPON%') -- 6107 prohibited conduct during emergency related to weapon
                OR (description LIKE '%DELIV%' AND description LIKE '%WEAPON%') -- 6110 persons to whom delivery of weapon shall not be made
                OR (description LIKE '%DEAL%' AND description LIKE '%WEAPON%') -- 6112 retail dealer required to be licensed
                OR (description LIKE '%FALSE EVIDENCE OF IDENTITY%') -- 6116 false evidence of identity
                OR (description LIKE '%ALTER%' AND (description LIKE '%WEAPON%' OR description LIKE '%MARK%') AND description NOT LIKE '%RETAIL%') -- 6117 altering id weapon (don't include retail theft charge about altering labels)
                OR (description LIKE '%CONVEY%' AND description LIKE '%EXPL%') -- 6161 carrying explosives on conveyances
                OR (description LIKE '%SHIP%' AND description LIKE '%EXPL%')) -- 6162 shipping explosives
              AND description NOT LIKE '%FAILURE TO REPORT INJUR%')) -- failure to report injury by firearm isn't really a firearm-related offense

            -- Designated as sexually violent predator
            -- This is not a specific offense code, but is checked in not_on_sex_offense_protocol criterion 
            -- but including here some offense descriptions that reference failure to register as an SVP 
            OR (description LIKE '%SVP%')
              
            -- Named in a PFA Order (or history of PFAs)
            -- Similarly this is not a specific offense (listed under "criteria officers need to check" in the tool) 
            -- but we can look at if they have a history of violating PFA (protection from abuse) orders
            -- (I think PFA can also stand for possessing firearm but that should also be excluded so it's ok) 
            OR description LIKE '%PFA%'
            
            )
"""


def sentences_and_charges_cte() -> str:
    # combine sentences preprocessed (to get correct date info) and state charge (to get status of individual charges)
    return """SELECT
            sen.state_code,
            sen.person_id,
            sen.date_imposed,
            sen.projected_completion_date_max,
            sen.statute,
            sen.description,
            sen.offense_type,
            sc.status,
        FROM `{project_id}.{sessions_dataset}.sentences_preprocessed_materialized` sen
        LEFT JOIN `{project_id}.{normalized_state_dataset}.state_charge` sc
            USING(person_id, charge_id)
        WHERE sen.state_code = 'US_PA'"""


def adm_form_information_helper() -> str:
    # this pulls information used to complete form 402a, the drug addendum for admin supervision
    return f"""
        WITH sentences_and_charges AS ({sentences_and_charges_cte()})
        SELECT person_id,
        (offense_type = 'DRUGS' 
            OR(offense_type IS NULL AND (description LIKE '%DRUG%'
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
        ((statute LIKE '%13A14%'
                OR statute LIKE '%13.A14%')
            OR (description LIKE '%DRUG%' 
                AND description LIKE '%ADMIN%' 
                AND description LIKE '%DISP%' 
                AND description LIKE '%DEL%' 
                AND description LIKE '%PRAC%')) AS form_information_statute_14,

        -- 35 P.S. 780-113 (30) - manufacture, sale, delivery, or possession with intent to deliver 
        ((statute LIKE '%13A30%' 
                OR statute LIKE '%13.A30%')
            OR (((description LIKE '%POSS%' AND (description LIKE '%INT%' OR description LIKE '%W/I%') AND description LIKE '%DEL%')
                    OR description LIKE '%PWI%'
                    OR description LIKE '%P/W/I%'
                    OR REGEXP_REPLACE(description, r'[^a-zA-Z0-9]', '') like '%POSSWITHINT%'
                    OR REGEXP_REPLACE(description, r'[^a-zA-Z0-9]', '') like '%POSSWINT%'
                    OR ((description LIKE '%MAN%' OR description LIKE '%MFG%')
                        AND (description LIKE '%SAL%' OR description LIKE '%SELL%')
                        AND description LIKE '%DEL%')
                    OR description LIKE '%MSD%'
                    OR description LIKE '%M/S/D%')
                AND (description NOT LIKE '%PAR%' -- doesn't include paraphernalia 
                    AND description NOT LIKE '%NON%' -- doesn't include non-controlled substances
                    AND description NOT LIKE 'ID THEFT%'))) AS form_information_statute_30,  -- doesn't include possession of id with intent to use

        -- 35 P.S. 780-113 (37) - possessing excessive amounts of steroids
        ((statute LIKE '%13A37%'
                OR statute LIKE '%13.A37%')
            OR description LIKE '%POSSESS EXCESSIVE AMOUNTS OF STERIODS%') AS form_information_statute_37, 
            -- steroids is misspelled in the data,
        
        (status = 'CONVICTED') AS guilty_charge_indicator,
        (status = 'EXTERNAL_UNKNOWN') as unreported_disposition_indicator,
        date_imposed,
    FROM sentences_and_charges
    WHERE state_code = 'US_PA'
    """


def case_notes_helper() -> str:
    return f"""
    /* pull special conditions for the current supervision period related to treatment, evaluations, and enhanced supervision.
        also display previous conditions related to treatment, evaluations, and enhanced supervision IF the client has a 
        condition specifying that all previously imposed conditions apply */
        
    WITH conditions AS (
      SELECT DISTINCT sup.state_code, 
        sup.person_id,
        pei.external_id,
        sup.start_date,
        sup.termination_date,
        condition
      FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period` sup
      INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON sup.person_id = pei.person_id
        AND sup.state_code = pei.state_code
        AND id_type = 'US_PA_PBPP',
      UNNEST(SPLIT(conditions, '##')) condition
    ), 
    agg_conditions AS (
      -- this aggregates spans so that you can see the entire time that a certain condition applies 
      {aggregate_adjacent_spans(table_name='conditions', index_columns=['state_code', 'person_id', 'external_id'], attribute='condition', end_date_field_name='termination_date')}
    ),
    prev_conditions_apply AS (
      -- pulls all clients who currently have a special condition that specifies that previous special conditions should apply 
      SELECT DISTINCT person_id
      FROM agg_conditions
      WHERE state_code = 'US_PA'
        AND condition LIKE '%PREVIOUS%'
        AND termination_date IS NULL
    )    
    SELECT DISTINCT external_id,
      'Special Conditions rel. to Treatment/Evaluation' AS criteria,
      CASE WHEN condition LIKE '%PREVIOUS%' THEN 'PREVIOUS CONDITIONS APPLY'
        WHEN termination_date IS NOT NULL THEN 'PREVIOUS CONDITION'
        WHEN condition LIKE '%EVALUATION%' THEN 'EVALUATION' 
        WHEN condition LIKE '%ENHANCE%' THEN 'ENHANCED SUPERVISION'
        ELSE 'TREATMENT' END AS note_title,
      condition AS note_body,
      start_date AS event_date,
    FROM agg_conditions
    WHERE state_code = 'US_PA' 
      AND (termination_date IS NULL -- either a current special condition 
        OR person_id IN (SELECT person_id FROM prev_conditions_apply)) -- or a past condition IF the client's current condition specifies that previous conditions apply) 
      AND ((condition LIKE '%TREATMENT%' AND condition LIKE '%SPECIAL CONDITION%')
        OR condition LIKE '%EVALUATION%'
        OR condition LIKE '%ENHANCE%'
        OR (condition LIKE '%PREVIOUS%' AND termination_date IS NULL)) -- display the condition "previous conditions apply" only if it is a current condition 
    
    UNION ALL 
    
    /* pull all treatments that were referred, started, or discharged during the current supervision period */ 
    (
    WITH supervision_starts AS (
      SELECT person_id, 
        release_date AS start_date, 
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
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_employment_period` emp
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
      ON emp.person_id = pei.person_id
      AND emp.state_code = pei.state_code
      AND id_type = 'US_PA_PBPP'
    WHERE emp.state_code = 'US_PA'
      AND end_date IS NULL
    """


def dui_indicator() -> str:
    return f"""({statute_code_is_like('75','3731')}
            OR {statute_code_is_like('75','38')}
            OR description LIKE '%DUI%'
            OR description LIKE '%DWI%'
            OR (description LIKE '%DRI%' AND description LIKE '%INF%'))"""


def obsc_materials_indicator() -> str:
    return f"""(({statute_code_is_like('18','5903.A3')}
            OR {statute_code_is_like('18','5903A3')}
            OR {statute_code_is_like('18','5903.A4')}
            OR {statute_code_is_like('18','5903A4')}
            OR {statute_code_is_like('18','5903.A5')}
            OR {statute_code_is_like('18','5903A5')})
        AND NOT ({statute_code_is_like('18','5903.A3I')} -- only include if convicted of 5903(3), but we don't know whether it's 3(i) non-minor or 3(ii) minor victim
            OR {statute_code_is_like('18','5903A3I')}
            OR {statute_code_is_like('18','5903.A4I')}
            OR {statute_code_is_like('18','5903A4I')}
            OR {statute_code_is_like('18','5903.A5I')}
            OR {statute_code_is_like('18','5903A5I')}))"""


def adm_case_notes_helper() -> str:
    # this pulls all pa case notes and adds a few that should only be displayed for the admin supervision opportunity
    return f"""
    SELECT * FROM ({case_notes_helper()})
    
    UNION ALL
    
    /* pulls all potential barriers to eligibility - these are gray area cases that we want to flag to the agent because
    we don't have all the information we need to determine whether it should change eligibility */
    
    SELECT DISTINCT pei.external_id,
      'Potential Barriers to Eligibility' AS criteria,
      'DUI' AS note_title,
      'This reentrant has a DUI charge on their criminal record. They would be ineligible for admin supervision if this charge resulted in bodily injury. Check criminal history for bodily injury and update eligibility accordingly.' AS note_body,
      sc.date_imposed AS event_date,
    FROM `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sc
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
      ON sc.person_id = pei.person_id
      AND sc.state_code = pei.state_code
      AND id_type = 'US_PA_PBPP'
    WHERE sc.state_code = 'US_PA' 
      AND {dui_indicator()}
    
    UNION ALL 
    
    (WITH sentences_and_charges AS ({sentences_and_charges_cte()})
    SELECT DISTINCT pei.external_id,
      'Potential Barriers to Eligibility' AS criteria,
      'OBSCENE MATERIALS' AS note_title,
      'This reentrant has 18 C.S. 5903(3)(4)(5) relating to obscene materials on their criminal record. They would be ineligible for admin supervision if the victim of this charge was a minor. Check criminal history for minor victim and update eligibility accordingly.' AS note_body,
      sc.date_imposed AS event_date,
    FROM sentences_and_charges sc
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
      ON sc.person_id = pei.person_id
      AND sc.state_code = pei.state_code
      AND id_type = 'US_PA_PBPP'
    WHERE sc.state_code = 'US_PA' 
      AND {obsc_materials_indicator()})
      
    UNION ALL 

    SELECT DISTINCT pei.external_id,
      'Potential Barriers to Eligibility' AS criteria,
      'DRUG' AS note_title,
      CASE WHEN form_information_statute_14 THEN 'This reentrant has 35 P.S. 780-113(14) relating to controlled substances on their criminal record. They could be ineligible for admin supervision if certain sentencing enhancements apply. Click “complete checklist” and scroll down to the drug addendum to determine eligibility.' 
        WHEN form_information_statute_30 THEN 'This reentrant has 35 P.S. 780-113(30) relating to controlled substances on their criminal record. They could be ineligible for admin supervision if certain sentencing enhancements apply. Click “complete checklist” and scroll down to the drug addendum to determine eligibility.'
        WHEN form_information_statute_37 THEN 'This reentrant has 35 P.S. 780-113(37) relating to steroids on their criminal record. They could be ineligible for admin supervision if certain sentencing enhancements apply. Click “complete checklist” and scroll down to the drug addendum to determine eligibility.'
        END AS note_body,
      date_imposed AS event_date,
    FROM ({adm_form_information_helper()}) form
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
      ON form.person_id = pei.person_id
      AND pei.state_code = 'US_PA'
      AND id_type = 'US_PA_PBPP'
    WHERE (form.form_information_statute_14
      OR form.form_information_statute_30
      OR form.form_information_statute_37)
    
    UNION ALL 
    
    /* pulls all pending charges from criminal history 
       note - flagging these in sidebar instead of building into eligibility criteria due to TT's recommendation & concerns with data quality */
    (
    WITH pending_charges AS (
        SELECT Parole_No AS external_id,
          UPPER(code) AS statute,
          UPPER(description) AS description,
          'Pending' AS status,
          DATE(Disposition_Date) AS event_date,
        FROM `{{project_id}}.{{us_pa_raw_data_up_to_date_views_dataset}}.Criminal_History_latest` cr    
        WHERE Disposition = 'Active Case'
    )
    SELECT DISTINCT
      pc.external_id,
      'Potential Barriers to Eligibility' AS criteria,
      'PENDING CHARGE' AS note_title,
      CASE WHEN {offense_is_admin_ineligible()} THEN CONCAT('This reentrant has an active case of ', statute, ' - ', description, ' on their criminal history. If this charge is still pending or if the reentrant has been found guilty, they would not be eligible for administrative supervision.') -- for admin ineligible charges, they would be ineligible if pending or found guilty 
        ELSE CONCAT('This reentrant has an active case of ', statute, ' - ', description, ' on their criminal history. If this charge is still pending, they would not be eligible for administrative supervision.') -- otherwise, they just can't have new pending charges
        END AS note_body,
      event_date,
    FROM pending_charges pc
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
      ON pc.external_id = pei.external_id
      AND pei.state_code = 'US_PA'
      AND id_type = 'US_PA_PBPP'
    LEFT JOIN ({us_pa_supervision_super_sessions()}) ss
      ON ss.person_id = pei.person_id
    WHERE {offense_is_admin_ineligible()}
      OR event_date >= release_date -- only include admin ineligible charges OR newly incurred charges while on supervision
    )
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
      date_imposed AS event_date,
    FROM `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sc
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
      ON sc.person_id = pei.person_id
      AND sc.state_code = pei.state_code
      AND id_type = 'US_PA_PBPP'
    INNER JOIN `{{project_id}}.{{criteria_dataset}}.meets_special_circumstances_criteria_for_time_served_materialized` crit
      ON sc.person_id = crit.person_id
      AND sc.state_code = crit.state_code 
      AND CURRENT_DATE('US/Eastern') BETWEEN crit.start_date AND {nonnull_end_date_exclusive_clause('crit.end_date')} 
    WHERE sc.state_code = 'US_PA' 
      AND ({statute_code_is_like('18','2718')} OR sc.description LIKE '%STRANG%')
      AND JSON_EXTRACT_SCALAR(crit.reason, "$.case_type") = 'non-life sentence (non-violent case)' -- only pull cases that would otherwise be considered non-violent
      
     
    UNION ALL 
    
    /* pulls all cases where someone we consider non-violent has a violent pending charge
       note - flagging these in sidebar instead of building into eligibility criteria due to TT's recommendation & concerns with data quality */
    (
    WITH pending_charges AS (
        SELECT Parole_No AS external_id,
          UPPER(code) AS statute,
          UPPER(description) AS description,
          'Pending' AS status,
          DATE(Disposition_Date) AS event_date,
          JSON_EXTRACT_SCALAR(crit.reason, "$.case_type") AS case_type, -- pull whether they are currently considered a violent or non-violent case according to SPC criteria 
        FROM `{{project_id}}.{{us_pa_raw_data_up_to_date_views_dataset}}.Criminal_History_latest` cr    
        INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
          ON cr.Parole_No = pei.external_id
          AND id_type = 'US_PA_PBPP'
        INNER JOIN `{{project_id}}.{{criteria_dataset}}.meets_special_circumstances_criteria_for_time_served_materialized` crit
          ON pei.person_id = crit.person_id
          AND pei.state_code = crit.state_code 
          AND CURRENT_DATE('US/Eastern') BETWEEN crit.start_date AND {nonnull_end_date_exclusive_clause('crit.end_date')} 
        WHERE Disposition = 'Active Case'
    )
    SELECT DISTINCT
      external_id,
      'Potential Barriers to Eligibility' AS criteria,
      'PENDING CHARGE' AS note_title,
      CONCAT('This reentrant has an active case of ', statute, ' - ', description, ' on their criminal history. If the reentrant is found guilty of this offense, they could be considered a violent case and must serve 5 years rather than 3 years on supervision before being eligible for special circumstances.') AS note_body,
      event_date,
    FROM pending_charges
    WHERE {offense_is_violent()}
        AND case_type = 'non-life sentence (non-violent case)' -- only pull cases with a violent pending charge that would otherwise be considered non-violent
    )
"""


def statute_code_is_like(title: str, statute_code: str) -> str:
    return f"""
    (statute LIKE '{title}.{statute_code}%'
    OR statute LIKE '{title}{statute_code}%'
    OR ({title} = 18 AND (statute LIKE '%CC{statute_code}%' OR statute LIKE '%CS{statute_code}%')) -- different prefixes are used for offense codes depending on the title. title 18 = criminal offenses = CC or CS 
    OR ({title} = 75 AND statute LIKE '%VC{statute_code}%') -- vehicle
    OR ({title} = 30 AND statute LIKE '%FB{statute_code}%') -- fishing & boating
    OR ({title} = 42 AND statute LIKE '%JC{statute_code}%')) -- judicial code
    """


def contains_nae(column: str) -> str:
    """helper function to determine if a string value contains NAE (not admin eligible) not surrounded by other letters.
    This helps ensure we include NAE but not names like SHANAE, which sometimes end up in notes fields"""
    return f"""
    ((REGEXP_CONTAINS(UPPER({column}),  r'NAE')
        AND NOT REGEXP_CONTAINS(UPPER({column}),  r'NAE[A-Z]')
        AND NOT REGEXP_CONTAINS(UPPER({column}),  r'[A-Z]NAE'))
    OR UPPER({column}) LIKE '%NOT ADMIN ELIG%')
    """
