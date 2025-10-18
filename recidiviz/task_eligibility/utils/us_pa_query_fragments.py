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
    """custom PA function to determine whether an offense is violent per 42 PA. C.S. §9714(g)"""
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
    OR ({statute_code_is_like('18', '2702', 'A1')}
      OR {statute_code_is_like('18', '2702', 'A2')}
      OR ((description LIKE '%AG%' OR description LIKE '%AA%')
          AND {description_refers_to_serious_bodily_injury()}
          AND description NOT LIKE '%FEAR%' -- fear of serious bodily injury is not included)
          AND description NOT LIKE '%ANIMAL%')) -- agg cruelty to animals is not included

-- 18 Pa.C.S. § 2702.1(a)(1) Assault of Law Enforcement Officer
    OR ({statute_code_is_like('18', '2702', '1A1')}
      OR ({description_refers_to_assault()} AND description LIKE '%OFFICER%'))
        
-- 18 Pa.C.S. § 2716(b) Use of Weapons of Mass Destruction
    OR ({statute_code_is_like('18', '2716', 'B')}
      OR (description LIKE '%WEAP%' AND description LIKE '%MASS%' AND description LIKE '%CAUS%'))

-- 18 Pa.C.S. § 2717(b)(2) Terrorism When Graded as a Felony in the First Degree  -- there don't seem to be any examples of this actually being used
    OR ({statute_code_is_like('18', '2717', 'B2')})
    -- not pulling in additional grade information here because B2 is explicitly specified as a first degree felony

-- 18 Pa.C.S. § 2718 Strangulation When Graded as a Felony
-- What we check: all strangulation cases unless explicitly specified as a misdemeanor 
    OR (({statute_code_is_like('18','2718')} 
        OR description LIKE '%STRANG%')
        AND (classification_type <> 'MISDEMEANOR' AND classification_subtype NOT LIKE 'M%'))
    #TODO(#45723) to fix cases where the classification type is populated but the type is not 

-- 18 Pa.C.S. § 3011 Trafficking of Persons When Graded as a Felony of the First Degree
-- 3011.A1 & A2 are specified as felonies of the first degree, 3011.B relates to sentencing of 3011.A1 & A2 when the victim is a minor
    OR ({statute_code_is_like('18', '3011', 'A1')} -- sexual servitude
      OR {statute_code_is_like('18', '3011', 'A2')} -- financial benefit
      OR {statute_code_is_like('18', '3011', 'B')} -- trafficking in minors
      OR (description LIKE '%TRAFFICK%' AND (description LIKE '%SEX%' OR description LIKE '%FINANCIAL%' OR description LIKE '%MINOR%')))
    -- not pulling in additional grade information here because A1, A2, & B are explicitly specified as a first degree felonies

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
    OR({statute_code_is_like('18','3124', '1')}
      OR ({description_refers_to_assault()}
          AND description LIKE '%SEX%'
          AND description NOT LIKE '%STAT%')) -- stat sexual assault is covered in 3122

-- 18 Pa.C.S. § 3301(a) OR 18 Pa.C.S. §3301(a.1) Arson Endangering Persons OR Aggravated Arson
    OR ({statute_code_is_like('18','3301', 'A')}
      OR ((description LIKE '%ARSON%' OR description LIKE '%ARSN%')
          AND ((description LIKE '%END%' AND (description like '%PER%' or description like '%PRSN%') AND description NOT LIKE '%PROP%') -- endangering person 3301(a)
            OR (description like '%INJ%' OR description LIKE '%DEA%' OR description LIKE '%DTH%')))) -- places another person in danger of death or bodily injury 3301(a)(1)(i)

-- 18 Pa.C.S. § 3311(b)(3) Ecoterrorism
    OR ({statute_code_is_like('18','3311', 'B3')}) -- no examples of this actually occurring

-- 18 Pa.C.S § 2901 Kidnapping
    OR({statute_code_is_like('18','2901')} --kidnapping
      OR description LIKE '%KID%')

-- 18 Pa.C.S. § 3502(a)(1) Burglary- Adapted for Overnight Accommodation and Person Present (Felony of the First Degree)
    OR ({statute_code_is_like('18','3502', 'A1')}
      OR ((description LIKE '%BURG%'
          AND (REGEXP_REPLACE(description, r'[^a-zA-Z]', '') LIKE '%OVERNIGHTACCOMMODATIONPERSONPRESENT%'
              OR REGEXP_REPLACE(description, r'[^a-zA-Z]', '') LIKE '%OVERNIGHTACCOMMODATIONSPERSONPRESENT%' 
              OR REGEXP_REPLACE(description, r'[^a-zA-Z]', '') LIKE '%OAPP%'
              OR description LIKE '%ANY PERSON%')
          AND description NOT LIKE '%NO%')))

-- 18 Pa.C.S. § 3701(a)(1)(i), (ii) or (iii) Robbery- Cause/Threaten to Cause Serious Bodily Injury or Threaten to Commit Felony of the First or Second Degree (Felony of the First Degree)
   OR ({statute_code_is_like('18','3701', 'A11')}
      OR {statute_code_is_like('18','3701', 'A12')}
      OR {statute_code_is_like('18','3701', 'A13')}
      OR ((description LIKE '%ROB%' AND description NOT LIKE '%PROB%') -- refers to robbery
          AND ({description_refers_to_serious_bodily_injury()} -- 3701(a)(1)(i) & (ii) refer to serious bodily injury
              OR description LIKE '%FEL%'))) -- 3701(a)(1)(iii) refers to committing or threatening to commit a felony

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
    """custom PA function to determine whether an offense is ineligible for admin supervision per form 402"""
    # TODO(#33754) -  Move ineligible offense flags upstream
    return f"""(
            --18 Pa. C.S. Ch. 25 relating to Crim. Homicide
            --Per Ch 25, criminal homicide includes murder, voluntary manslaughter, & involuntary manslaughter
            ({statute_code_is_like('18', '2501')} --criminal homicide
                OR {statute_code_is_like('18', '2502')} --murder
                OR {statute_code_is_like('18', '2503')}--voluntary manslaughter
                OR {statute_code_is_like('18', '2504')} --involuntary manslaughter
                OR {statute_code_is_like('18', '2505', 'A')} --causing suicide as criminal homicide
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
            OR (({statute_code_is_like('18','2701')} --simple assault
                OR {statute_code_is_like('18','2702')} --aggravated assault
                OR ({statute_code_is_like('18','2703')} AND (subsection IS NULL OR subsection NOT LIKE '1%')) --assault by prisoner (2703.1 refers to harassment not assault)
                OR {statute_code_is_like('18','2704')} --assault by life prisoner
                OR {statute_code_is_like('18','2711')} --domestic violence 
                OR {statute_code_is_like('18','2712')} --assault on a sports official 
                OR {statute_code_is_like('18','2718')}) --strangulation
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
            OR (({statute_code_is_like('75','3732')} AND (subsection IS NULL OR subsection NOT LIKE '1%')) -- 3732.1 is technically assault, not homicide
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
            OR ({statute_code_is_like('30','5502', '1')}
              OR {statute_code_is_like('30','5502', '2')}
              OR {statute_code_is_like('30','5502', '3')}
              OR {statute_code_is_like('30','5502', '4')}
              OR ((description LIKE '%HOM%' OR {description_refers_to_assault()}) AND description LIKE '%WATER%'))
              
            -- 18 Pa. C.S. 4302 Incest
            OR({statute_code_is_like('18','4302')}
              OR description LIKE '%INCES%')

            -- 18 Pa. C.S. 5901 Open Lewdness
            OR({statute_code_is_like('18','5901')}
              OR (description LIKE '%OPEN%' AND description LIKE '%LEWD%'))
    
            -- 18 Pa. C.S. 5902(b) Prostitution
            OR({statute_code_is_like('18','5902', 'B')} 
              OR (description LIKE '%PROM%' AND description LIKE '%PROST%' AND subsection NOT LIKE 'A%'))
              -- for some reason there are records where the statute is 5902.A (engaging in prostitution) but the description is promoting prostitution (which should be 5902.B)
              
            -- 18 Pa. C.S. 5903(4)(5)(6) obscene/sexual material/performance where the victim is minor
            -- 5903A4 & 5903A5 can be perpetrated against a non-minor (i) or a minor (ii), 5903A6 always relates to minors
            OR (({statute_code_is_like('18','5903', 'A42')}-- write/print/publish obscene materials/performance
                OR {statute_code_is_like('18','5903', 'A52')} -- produce/present/direct obscene materials/performance
                OR {statute_code_is_like('18','5903', 'A6')})  -- hire/employ/use minor child
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
            OR ({statute_code_is_like('18','5903', 'A32')}  -- 18 Pa.C.S. § 5903(a)(3)(ii) (relating to obscene and other sexual materials and performances)
                OR (description LIKE 'MINOR' AND description like '%OBSCENE%' AND description LIKE '%DESIGN%')
                OR {statute_code_is_like('18','6301', 'A12')} -- 18 Pa.C.S. § 6301(a)(1)(ii) (relating to corruption of minors).
                OR {statute_code_is_like('18','7507', '1')}  -- 18 Pa.C.S. § 7507.1. (relating to invasion of privacy)
                OR (description LIKE '%INVASION%' AND description LIKE '%PRIVACY%')
                OR description LIKE '%MEGAN%'
                OR {statute_code_is_like('18','4915')} OR {statute_code_is_like('42','979%')})-- not listed specifically but relates to sex offender registration requirements

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
            OR (({statute_code_is_like('18','61%')})
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


def clean_and_split_statute(table: str) -> str:
    # TODO(#45629) Move cleaning & splitting of statute codes upstream
    """custom PA logic to clean statute codes and split them into title, section, and subsection(s)
    for example, a statute code of 18.2702(b) will get split into title = 18, section = 2702, and subsection = b"""
    return f"""
        WITH clean_statute AS (
            SELECT *, 
                REGEXP_REPLACE(statute_split, r"\\.|-|\\*", '') AS statute_clean, -- remove periods, hyphens, and asterisks
            FROM {table}
            LEFT JOIN UNNEST(SPLIT(REPLACE(statute, ',', ';'), ';')) AS statute_split -- split on commas and semicolons
        )
        SELECT *,
            SUBSTR(statute_clean, 0, 2) AS title,
            SUBSTR(statute_clean, 3, 4) AS section,
            {convert_roman_numerals('SUBSTR(statute_clean, 7)')} AS subsection,
        FROM clean_statute
    """


def adm_form_information_helper() -> str:
    """this pulls information used to complete form 402a, the drug addendum for admin supervision"""
    return """
        WITH sentences_and_charges AS (
        /* combine sentences preprocessed (to get correct date info) and state charge (to get status of individual charges) */
            SELECT sen.state_code,
                sen.person_id,
                sen.date_imposed,
                sen.statute,
                sen.description,
                sen.offense_type,
                sc.status,
            FROM `{project_id}.{sessions_dataset}.sentences_preprocessed_materialized` sen
            LEFT JOIN `{project_id}.{normalized_state_dataset}.state_charge` sc
                USING(person_id, charge_id)
            WHERE sen.state_code = 'US_PA'
        )
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
    """custom PA function that returns case notes for admin and special circ opportunities related to special conditions,
    treatment, and employment"""
    return f"""
    /* pull special conditions for the current supervision period related to treatment, evaluations, and enhanced supervision.
        also display previous conditions related to treatment, evaluations, and enhanced supervision IF the client has a 
        condition specifying that all previously imposed conditions apply */
        
    WITH conditions AS (
      SELECT DISTINCT sup.state_code, 
        sup.person_id,
        sup.start_date,
        sup.termination_date,
        condition
      FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period` sup,
      UNNEST(SPLIT(conditions, '##')) condition
    ), 
    agg_conditions AS (
      -- this aggregates spans so that you can see the entire time that a certain condition applies 
      {aggregate_adjacent_spans(table_name='conditions', index_columns=['state_code', 'person_id'], attribute='condition', end_date_field_name='termination_date')}
    ),
    prev_conditions_apply AS (
      -- pulls all clients who currently have a special condition that specifies that previous special conditions should apply 
      SELECT DISTINCT person_id
      FROM agg_conditions
      WHERE state_code = 'US_PA'
        AND condition LIKE '%PREVIOUS%'
        AND termination_date IS NULL
    )    
    SELECT DISTINCT person_id,
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
      person_id,
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
    USING(person_id)
    WHERE (COALESCE(tre.discharge_date, tre.start_date, tre.referral_date) >= sup.start_date -- only display treatments during current supervision period 
            OR tre.person_id IN (SELECT person_id FROM prev_conditions_apply)) -- UNLESS special conditions from a previous term apply, then we display all past treatments 
    ) 
    
    UNION ALL 
    
    /* pull all currently open employment periods */ 
    SELECT DISTINCT
      person_id,
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
    FROM `{{project_id}}.us_pa_normalized_state.state_employment_period` emp
    WHERE end_date IS NULL
    """


def dui_indicator() -> str:
    """custom PA function that returns whether an offense relates to DUI"""
    return f"""({statute_code_is_like('75','3731')}
            OR {statute_code_is_like('75','38%')}
            OR description LIKE '%DUI%'
            OR description LIKE '%DWI%'
            OR (description LIKE '%DRI%' AND description LIKE '%INF%'))"""


def adm_case_notes_helper() -> str:
    """this pulls all pa case notes and adds a few that should only be displayed for the admin supervision opportunity"""
    return f"""
    SELECT * FROM ({case_notes_helper()})
    
    UNION ALL 
    
    -- pulls in all offenses we have from our 5 sources that have either a description
    -- or statute

    {build_and_deduplicate_offense_history(filter_to_current=False)}
    
    UNION ALL
    
    -- pulls all potential barriers to eligibility - these are gray area cases that we want to flag to the agent because
    -- we don't have all the information we need to determine whether it should change eligibility

    -- first, as it relates to DUIs
    
    (WITH sentences AS (
        SELECT * 
        FROM `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sent
        WHERE state_code = 'US_PA'
    ), sentences_clean AS (
        {clean_and_split_statute('sentences')}
    )
    SELECT DISTINCT person_id,
      'Potential Barriers to Eligibility' AS criteria,
      'DUI' AS note_title,
      'This reentrant has a DUI charge on their criminal record. They would be ineligible for admin supervision if this charge resulted in bodily injury. Check criminal history for bodily injury and update eligibility accordingly.' AS note_body,
      sc.date_imposed AS event_date,
    FROM sentences_clean sc
    WHERE {dui_indicator()}
    )

    UNION ALL 

    -- second, as it related to specific drug charges

    SELECT DISTINCT person_id,
      'Potential Barriers to Eligibility' AS criteria,
      'DRUG' AS note_title,
      CASE WHEN form_information_statute_14 THEN 'This reentrant has 35 P.S. 780-113(14) relating to controlled substances on their criminal record. They could be ineligible for admin supervision if certain sentencing enhancements apply. Click "Complete 402 Forms" and scroll down to the drug addendum to determine eligibility.' 
        WHEN form_information_statute_30 THEN 'This reentrant has 35 P.S. 780-113(30) relating to controlled substances on their criminal record. They could be ineligible for admin supervision if certain sentencing enhancements apply. Click "Complete 402 Forms" and scroll down to the drug addendum to determine eligibility.'
        WHEN form_information_statute_37 THEN 'This reentrant has 35 P.S. 780-113(37) relating to steroids on their criminal record. They could be ineligible for admin supervision if certain sentencing enhancements apply. Click "Complete 402 Forms" and scroll down to the drug addendum to determine eligibility.'
        END AS note_body,
      date_imposed AS event_date,
    FROM ({adm_form_information_helper()}) form
    WHERE (form.form_information_statute_14
      OR form.form_information_statute_30
      OR form.form_information_statute_37)
    
    UNION ALL 
    
    -- third, as it relates to all pending charges from criminal history
    -- note - flagging these in sidebar instead of building into eligibility criteria due to TT's recommendation & concerns with data quality
    (
    WITH pending_charges AS (
    /* pull all pending charges */
        SELECT person_id,
          UPPER(code) AS statute,
          UPPER(description) AS description,
          'Pending' AS status,
          DATE(Disposition_Date) AS event_date,
          CAST(NULL AS STRING) AS classification_type,
          Grade AS classification_subtype,
        FROM `{{project_id}}.{{us_pa_raw_data_up_to_date_views_dataset}}.Criminal_History_latest` cr
        INNER JOIN `{{project_id}}.us_pa_normalized_state.state_person_external_id` pei
            ON cr.Parole_No = pei.external_id
            AND id_type = 'US_PA_PBPP'
        WHERE Disposition = 'Active Case'
    ), pending_charges_clean AS (
    /* takes PA pending charges and splits the statute codes into titles, sections, and subsections, which we use to determine
        which offenses are admin-ineligible later */ 
        SELECT person_id,
            statute,
            description,
            status,
            event_date,
            classification_type,
            classification_subtype,
            title,
            section,
            subsection,
        FROM ({clean_and_split_statute('pending_charges')})
    ), pending_charges_with_admin_indicator AS (
    /* pulls whether the pending charge is admin-ineligible */
        SELECT *,
            {offense_is_admin_ineligible()} AS is_admin_ineligible
        FROM pending_charges_clean
    )
    SELECT DISTINCT
      person_id,
      'Potential Barriers to Eligibility' AS criteria,
      'PENDING CHARGE' AS note_title,
      CASE WHEN is_admin_ineligible THEN CONCAT('This reentrant has an active case of ', statute, ' - ', description, ' on their criminal history. If this charge is still pending or if the reentrant has been found guilty, they would not be eligible for administrative supervision.') -- for admin ineligible charges, they would be ineligible if pending or found guilty 
        ELSE CONCAT('This reentrant has an active case of ', statute, ' - ', description, ' on their criminal history. If this charge is still pending, they would not be eligible for administrative supervision.') -- otherwise, they just can't have new pending charges
        END AS note_body,
      event_date,
    FROM pending_charges_with_admin_indicator pc
    LEFT JOIN ({us_pa_supervision_super_sessions()}) ss
        USING(person_id)
    WHERE is_admin_ineligible
      OR event_date >= release_date -- only include admin ineligible charges OR newly incurred charges while on supervision
    )
    """


def spc_case_notes_helper() -> str:
    """this pulls all pa case notes and adds one that should only be displayed for the special circumstances opportunity"""
    return f"""
    SELECT * FROM ({case_notes_helper()})
        
    UNION ALL 
    
    {build_and_deduplicate_offense_history(filter_to_current=True)}
    
    UNION ALL 
    
    /* pulls all cases where someone we consider non-violent has a violent pending charge
       note - flagging these in sidebar instead of building into eligibility criteria due to TT's recommendation & concerns with data quality */
    (
    WITH pending_charges AS (
        /* pull all pending charges */
        SELECT person_id,
          UPPER(active_cases.Code) AS statute,
          UPPER(active_cases.Description) AS description,
          'Pending' AS status,
          DATE(active_cases.Disposition_Date) AS event_date,
          CAST(NULL AS STRING) AS classification_type,
          active_cases.Grade AS classification_subtype,
          JSON_EXTRACT_SCALAR(crit.reason, "$.case_type") AS case_type, -- pull whether they are currently considered a violent or non-violent case according to SPC criteria 
        FROM (
            SELECT person_id, cr.* EXCEPT(Parole_No)
            FROM `{{project_id}}.{{us_pa_raw_data_up_to_date_views_dataset}}.Criminal_History_latest` cr
            INNER JOIN `{{project_id}}.us_pa_normalized_state.state_person_external_id` pei
                ON cr.Parole_No = pei.external_id
                AND id_type = 'US_PA_PBPP'
            WHERE Disposition = 'Active Case'
        ) active_cases
        INNER JOIN (
            -- Select current criteria periods
            SELECT *
            FROM `{{project_id}}.{{criteria_dataset}}.meets_special_circumstances_criteria_for_time_served_materialized` crit
            WHERE CURRENT_DATE('US/Eastern') BETWEEN crit.start_date AND {nonnull_end_date_exclusive_clause('crit.end_date')}
        ) crit
        USING (person_id)
    ), pending_charges_clean AS (
        /* takes PA pending charges and splits the statute codes into titles, sections, and subsections, which we use to determine
        which offenses are violent later */ 
        SELECT person_id,
            statute,
            description,
            status,
            event_date,
            classification_type,
            classification_subtype,
            case_type,
            title,
            section,
            subsection,
        FROM ({clean_and_split_statute('pending_charges')})
    ) 
    SELECT DISTINCT
      person_id,
      'Potential Barriers to Eligibility' AS criteria,
      'PENDING CHARGE' AS note_title,
      CONCAT('This reentrant has an active case of ', statute, ' - ', description, ' on their criminal history. If the reentrant is found guilty of this offense, they could be considered a violent case and must serve 5 years rather than 3 years on supervision before being eligible for special circumstances.') AS note_body,
      event_date,
    FROM pending_charges_clean
    WHERE {offense_is_violent()}
        AND case_type = 'non-life sentence (non-violent case)' -- only pull cases with a violent pending charge that would otherwise be considered non-violent
    )
"""


def build_and_deduplicate_offense_history(filter_to_current: bool) -> str:
    """builds offense history, filtering to just offenses for currently serving sentences
    if |filter_to_current| is True, or including all offenses if it is False. as we
    receive offense/charge history from 5 different sources, we also deduplicate
    offenses that have the same statue and imposed date that appear in multiple sources
    """

    if filter_to_current:
        # in order to filter to _just_ current charges, we pull in sentences to
        # filter out any sentences that are not currently being served
        from_clause = f"""FROM `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized` span,
    UNNEST (sentences_preprocessed_id_array_actual_completion) AS sentences_preprocessed_id
    INNER JOIN `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sen
        USING (state_code, person_id, sentences_preprocessed_id)
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_charge` sc 
        USING(person_id, charge_id)
    WHERE sen.state_code = 'US_PA' 
      AND (sen.statute IS NOT NULL OR sen.description IS NOT NULL)
      AND CURRENT_DATE('US/Eastern') BETWEEN span.start_date AND {nonnull_end_date_exclusive_clause('span.end_date')} 
    """
    else:
        # for all charges, we just join sentences & charges
        from_clause = """FROM `{project_id}.{sessions_dataset}.sentences_preprocessed_materialized` sen
    INNER JOIN `{project_id}.{normalized_state_dataset}.state_charge` sc 
        USING(person_id, charge_id) 
    WHERE sen.state_code = 'US_PA' 
      AND (sen.statute IS NOT NULL OR sen.description IS NOT NULL)
    """

    return f"""
    SELECT DISTINCT person_id,
      'Offense History' AS criteria,
      COALESCE(statute, 'NO STATUTE PROVIDED') AS note_title,
      CONCAT(COALESCE(INITCAP(description), 'No Description Provided'), source) AS note_body,
      date_imposed as event_date,
      FROM (
        WITH 
        -- each row is a one charge with an attached statute, annotated with it's source
        all_statue_and_source_info AS (
          SELECT 
            person_id, 
            sen.statute,
            sen.description,
            CASE 
              WHEN SPLIT(sc.external_id, "-")[OFFSET(0)] = 'CRIM_HIST' THEN ' (Source: CAPTOR - Criminal History)'
              WHEN SPLIT(sc.external_id, "-")[OFFSET(0)] = 'DBO_SENTENCE' THEN ' (Source: Supervisor Controls)'
              WHEN SPLIT(sc.external_id, "-")[OFFSET(0)] = 'SENREC' THEN ' (Source: CAPTOR - Sentence Summary)'
              WHEN SPLIT(sc.external_id, "-")[OFFSET(0)] = 'INCARCERATION_SENTENCE' THEN ' (Source: CalcRite)'
              ELSE ' (Source: CAPTOR - Other)'
            END as source,
            CASE 
              WHEN SPLIT(sc.external_id, "-")[OFFSET(0)] = 'CRIM_HIST' THEN 1
              WHEN SPLIT(sc.external_id, "-")[OFFSET(0)] = 'SENREC' THEN 2
              WHEN SPLIT(sc.external_id, "-")[OFFSET(0)] = 'DBO_SENTENCE' THEN 3
              WHEN SPLIT(sc.external_id, "-")[OFFSET(0)] = 'INCARCERATION_SENTENCE' THEN 4
              ELSE 5
            END as source_sorting_rank,
            sen.date_imposed,
          {from_clause}
        ),
        -- here, we normalize the statute's title to make it easier to deduplicate, leaving
        -- everything after the title in place (such as *'s, which can represent the offense
        -- count) to make sure that conflicting or differing info is not collapsed
        normalized_title_and_source_info AS (
          SELECT 
          *,
          CASE
            WHEN title in ('18', 'CC', 'CS') THEN '18' -- title 18 = criminal offenses = CC or CS 
            WHEN title IN ('75', 'VC') THEN '75' -- vehicle
            WHEN title IN ('30', 'FB') THEN '30' -- fishing & boating
            WHEN title IN ('42', 'JC') THEN '42' -- judicial code
            ELSE title
          END as normalized_title
          FROM (
            SELECT *,
              SUBSTR(statute_clean, 0, 2) AS title,
              SUBSTR(statute_clean, 3) AS statute_after_title,
            FROM (
              SELECT *, 
                  REGEXP_REPLACE(statute_split, r"\\.|-", '') AS statute_clean, -- remove periods, hyphens
              FROM all_statue_and_source_info
              LEFT JOIN UNNEST(SPLIT(REPLACE(statute, ',', ';'), ';')) AS statute_split -- split on commas and semicolons
            )
          )
        )
        SELECT 
          person_id, 
          statute, 
          description, 
          source,
          date_imposed
        FROM normalized_title_and_source_info
        -- duplicates of the same statute/date imposed exist because we pull offenses from different systems, 
        -- for each statue that has multiple entries on the same date, preferentially select
        -- the source we think the agent will be most familiar w/
        QUALIFY RANK() OVER(PARTITION BY person_id, normalized_title, statute_after_title, date_imposed, description ORDER BY source_sorting_rank) = 1
      )
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


def statute_code_is_like(
    title: str,
    section: str,
    subsection: str = "",
) -> str:
    """Custom PA function that checks for a match in the title, section, and subsection(s) of statute codes
    Note about checking subsections: this gets tricky because there can be multiple layers of subsections and we
    want to match as much we can, but also default to including offenses when we are missing information (per user feedback).
    We achieve this by truncating the input subsection and the actual subsection to the same length.
    Some examples of how this works:
        1. the policy states 4502(a)(1)(iv), but the data only says 4502(a). We truncate to 4502(a) = 4502(a) and determine this is a match
        2. the policy states 4502(a)(1)(iv), but the data says 4502(a)(2). We truncate to 4502(a)(1) != 4502(a)(2) and determine this is not a match
        3.  the policy states 4502(a)(1), but the data says 4502(a)(1)(i). We truncate to 4502(a)(1) = 4502(a)(1) and determine this is a match"""
    return f"""(
    /* check that title is correct */ 
    CASE WHEN '{title}' = '18' THEN title IN ('18', 'CC', 'CS') -- different prefixes are used for offense codes depending on the title. title 18 = criminal offenses = CC or CS 
        WHEN '{title}' = '75' THEN title IN ('75', 'VC') -- vehicle
        WHEN '{title}' = '30' THEN title IN ('30', 'FB') -- fishing & boating
        WHEN '{title}' = '42' THEN title IN ('42', 'JC') -- judicial code
    END
    /* check that section is correct */
    AND section LIKE '{section}'
    /* optional: check that subsection is correct */
    AND CASE WHEN COALESCE('{subsection}', '') = '' THEN TRUE -- if no subsection is input, return true
        WHEN COALESCE(subsection, '') = '' THEN TRUE -- if no subsection is present in the data, return true
        ELSE SUBSTR('{subsection}', 0, LEAST(LENGTH('{subsection}'), LENGTH(subsection)))
             = SUBSTR(subsection, 0, LEAST(LENGTH('{subsection}'), LENGTH(subsection))) -- else check that subsection matches per above logic
    END
    )"""


# TODO(#45075) replace this with an actual helper function
def convert_roman_numerals(string: str) -> str:
    """extremely hacky function to replace roman numerals with actual numbers for values of I, II, III & IV"""
    return f"""REGEXP_REPLACE(UPPER({string}), r'IV|III|II|I',
                    CASE WHEN REGEXP_EXTRACT(UPPER({string}), r'(IV|III|II|I)') = 'IV' THEN '4'
                        WHEN REGEXP_EXTRACT(UPPER({string}), r'(IV|III|II|I)') = 'III' THEN '3'
                        WHEN REGEXP_EXTRACT(UPPER({string}), r'(IV|III|II|I)') = 'II' THEN '2'
                        WHEN REGEXP_EXTRACT(UPPER({string}), r'(IV|III|II|I)') = 'I' THEN '1'
                        ELSE '' END)"""
