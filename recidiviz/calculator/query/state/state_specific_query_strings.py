# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Functions that return state-specific logic used in BigQuery queries."""
# pylint: disable=anomalous-backslash-in-string

from enum import Enum
from typing import Dict, List, Optional

from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import (
    get_required_state_specific_metrics_producer_delegates,
    get_supported_states,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_supervision_metrics_producer_delegate import (
    StateSpecificSupervisionMetricsProducerDelegate,
)
from recidiviz.common.constants.states import StateCode

# The states in the vitals reports that will be grouping by level 1 supervision locations.
VITALS_LEVEL_1_SUPERVISION_LOCATION_STATES: List[str] = ['"US_ND"', '"US_PA"']
VITALS_LEVEL_1_SUPERVISION_LOCATION_OPTIONS: str = (
    f"({', '.join(VITALS_LEVEL_1_SUPERVISION_LOCATION_STATES)})"
)

# The states in the vitals reports that will be grouping by level 2 supervision locations.
VITALS_LEVEL_2_SUPERVISION_LOCATION_STATES: List[str] = ['"US_MO"', '"US_ID"']
VITALS_LEVEL_2_SUPERVISION_LOCATION_OPTIONS: str = (
    f"({', '.join(VITALS_LEVEL_2_SUPERVISION_LOCATION_STATES)})"
)

# The states in the Pathways views that will be grouping by level 2 incarceration locations.
PATHWAYS_LEVEL_2_INCARCERATION_LOCATION_STATES: List[str] = [
    '"US_ID"',
    '"US_TN"',
    '"US_CO"',
]
PATHWAYS_LEVEL_2_INCARCERATION_LOCATION_OPTIONS: str = (
    f"({', '.join(PATHWAYS_LEVEL_2_INCARCERATION_LOCATION_STATES)})"
)

STATE_RACE_ETHNICITY_POPULATION_TABLE_NAME = "state_race_ethnicity_population_counts"

# Select the raw table that ultimately powers a state's incarceration pathways calculations.
# Note: there are a few limitations with this strategy. Most notably, this only picks one raw table per state. It does
# not accommodate checking multiple tables that may potentially power incarceration periods in ingest.
# TODO(#11642) Allow for more than one table per state
STATE_CODE_TO_PATHWAYS_INCARCERATION_LAST_UPDATED_DATE_SOURCE_TABLE: Dict[
    StateCode, str
] = {
    StateCode.US_ID: "movement",
    StateCode.US_ME: "CIS_309_MOVEMENT",
    StateCode.US_ND: "elite_externalmovements",
    StateCode.US_TN: "OffenderMovement",
    StateCode.US_MI: "ADH_OFFENDER_EXTERNAL_MOVEMENT",
    StateCode.US_CO: "eomis_externalmovement",
}

# Select the raw table that ultimately powers a state's supervision pathways calculations.
STATE_CODE_TO_PATHWAYS_SUPERVISION_LAST_UPDATED_DATE_SOURCE_TABLE: Dict[
    StateCode, str
] = {
    StateCode.US_ID: "movement",
    StateCode.US_ND: "docstars_offendercasestable",
    StateCode.US_TN: "SupervisionPlan",
    StateCode.US_ME: "CIS_124_SUPERVISION_HISTORY",
}


def state_supervision_specific_district_groupings(
    district_column: str, judicial_district_column: str
) -> str:
    return f"""IFNULL(CASE WHEN supervision_type = 'PROBATION' THEN
        {state_specific_judicial_district_groupings(judicial_district_column)}
        ELSE {district_column} END, 'EXTERNAL_UNKNOWN')"""


def state_specific_judicial_district_groupings(judicial_district_column: str) -> str:
    return f"""(CASE WHEN state_code = 'US_ND'
               AND ({judicial_district_column} IS NULL
                    OR {judicial_district_column} IN ('OUT_OF_STATE', 'EXTERNAL_UNKNOWN', 'FEDERAL'))
                THEN 'OTHER'
               WHEN {judicial_district_column} IS NULL THEN 'EXTERNAL_UNKNOWN'
               ELSE {judicial_district_column} END)"""


def state_specific_race_or_ethnicity_groupings(
    race_or_ethnicity_column: str = "race_or_ethnicity",
    supported_race_overrides: Optional[Dict[StateCode, str]] = None,
) -> str:
    overrides_clauses = []
    if supported_race_overrides:
        for state_code, override_values in supported_race_overrides.items():
            when_clause = (
                f"WHEN state_code = '{state_code.value}' AND ({race_or_ethnicity_column} IS NULL "
                f"OR {race_or_ethnicity_column} NOT IN ({override_values})) THEN 'OTHER'"
            )
            overrides_clauses.append(when_clause)
    override_when_block = "\n              ".join(overrides_clauses)

    return f"""CASE {override_when_block}
              WHEN state_code = 'US_ND' AND ({race_or_ethnicity_column} IS NULL OR {race_or_ethnicity_column} IN
              ('EXTERNAL_UNKNOWN', 'ASIAN', 'NATIVE_HAWAIIAN_PACIFIC_ISLANDER')) THEN 'OTHER'
              WHEN state_code = 'US_ID' AND {race_or_ethnicity_column} = 'NATIVE_HAWAIIAN_PACIFIC_ISLANDER'
                THEN 'ASIAN'
              WHEN {race_or_ethnicity_column} IS NULL THEN 'EXTERNAL_UNKNOWN'
              ELSE {race_or_ethnicity_column} END AS race_or_ethnicity"""


def state_specific_assessment_bucket(
    output_column_name: str = "assessment_score_bucket",
) -> str:
    return f"""CASE WHEN state_code = 'US_MO' AND assessment_score_bucket = 'LOW_MEDIUM' THEN 'MEDIUM'
               ELSE assessment_score_bucket END AS {output_column_name}"""


def state_specific_most_severe_violation_type_subtype_grouping() -> str:
    return """CASE
                WHEN most_severe_violation_type IS NULL THEN 'NO_VIOLATION_TYPE'
                WHEN state_code = 'US_MO' AND most_severe_violation_type = 'TECHNICAL' THEN
                CASE WHEN most_severe_violation_type_subtype = 'SUBSTANCE_ABUSE' THEN most_severe_violation_type_subtype
                     WHEN most_severe_violation_type_subtype = 'LAW_CITATION' THEN 'MISDEMEANOR'
                     ELSE most_severe_violation_type END
                WHEN state_code = 'US_PA' THEN
                    CASE WHEN most_severe_violation_type = 'TECHNICAL' THEN most_severe_violation_type_subtype
                         ELSE most_severe_violation_type END
                ELSE most_severe_violation_type
            END AS violation_type"""


def state_specific_recommended_for_revocation() -> str:
    return f"""(({state_specific_officer_recommendation('most_severe_response_decision', False)}) = 'REVOCATION')
            AS recommended_for_revocation"""


def state_specific_officer_recommendation(
    input_col: str, include_col_declaration: bool = True
) -> str:
    return f"""CASE WHEN state_code = 'US_MO' THEN
                CASE WHEN {input_col} = 'SHOCK_INCARCERATION' THEN 'CODS'
                WHEN {input_col} = 'WARRANT_ISSUED' THEN 'CAPIAS'
                ELSE {input_col} END
           WHEN state_code = 'US_PA' THEN
                -- TODO(#3596): Remove this once we differentiate returns from true revocations 
                CASE WHEN {input_col} = 'REVOCATION' THEN 'PLACEMENT_IN_DOC_FACILITY'
                ELSE {input_col} END
           ELSE {input_col}
      END {"AS officer_recommendation" if include_col_declaration else ""}"""


def state_specific_violation_type_entry() -> str:
    return """CASE WHEN state_code = 'US_MO' AND violation_type_entry = 'LAW_CITATION' THEN 'MISDEMEANOR'
              ELSE violation_type_entry
        END as violation_type_entry"""


def state_specific_violation_type_entry_categories() -> str:
    return """-- US_MO categories --
      COUNTIF(state_code = 'US_MO' AND violation_type_entry = 'ASC') AS association_count,
      COUNTIF(state_code = 'US_MO' AND violation_type_entry = 'DIR') AS directive_count,
      COUNTIF(state_code = 'US_MO' AND violation_type_entry = 'EMP') AS employment_count,
      COUNTIF(state_code = 'US_MO' AND violation_type_entry = 'INT') AS intervention_fee_count,
      COUNTIF(state_code = 'US_MO' AND violation_type_entry = 'RES') AS residency_count,
      COUNTIF(state_code = 'US_MO' AND violation_type_entry = 'SPC') AS special_count,
      COUNTIF(state_code = 'US_MO' AND violation_type_entry = 'SUP') AS supervision_strategy_count,
      COUNTIF(state_code = 'US_MO' AND violation_type_entry = 'TRA') AS travel_count,
      COUNTIF(state_code = 'US_MO' AND violation_type_entry = 'WEA') AS weapon_count,
      -- US_PA categories --
      COUNTIF(state_code = 'US_PA' AND violation_type_entry = 'ELEC_MONITORING') as elec_monitoring_count,
      COUNTIF(state_code = 'US_PA' AND violation_type_entry = 'LOW_TECH') as low_tech_count,
      COUNTIF(state_code = 'US_PA' AND violation_type_entry = 'MED_TECH') as med_tech_count,
      COUNTIF(state_code = 'US_PA' AND violation_type_entry = 'HIGH_TECH') as high_tech_count,
    """


def state_specific_supervision_level() -> str:
    return """IFNULL(
                (CASE WHEN state_code = 'US_PA' THEN
                  CASE WHEN supervision_level = 'LIMITED' THEN 'SPECIAL'
                       WHEN (supervision_level = 'INTERNAL_UNKNOWN' AND supervision_level_raw_text = 'SPC') THEN 'SPECIAL'
                       WHEN supervision_level = 'HIGH' THEN 'ENHANCED'
                       ELSE supervision_level END
                ELSE supervision_level END),
           'EXTERNAL_UNKNOWN') AS supervision_level"""


def state_specific_facility_exclusion() -> str:
    return """-- Revisit these exclusions when #3657 and #3723 are complete --
      (state_code != 'US_ND' OR facility not in ('OOS', 'CPP'))"""


def state_specific_external_id_type(state_code_table_prefix: str) -> str:
    return f"""
        CASE 
          WHEN {state_code_table_prefix}.state_code = 'US_ID'
          THEN 'US_ID_DOC'
          WHEN {state_code_table_prefix}.state_code = 'US_PA'
          THEN 'US_PA_PBPP'
          WHEN {state_code_table_prefix}.state_code = 'US_MO'
          THEN 'US_MO_DOC'
          WHEN {state_code_table_prefix}.state_code = 'US_ND'
          THEN 'US_ND_SID'
          WHEN {state_code_table_prefix}.state_code = 'US_TN'
          THEN 'US_TN_DOC'
          WHEN {state_code_table_prefix}.state_code = 'US_ME'
          THEN 'US_ME_DOC'
        END
    """


def state_specific_supervision_location_optimization_filter() -> str:
    """State-specific logic for filtering rows based on supervision location values that are unused by the front end."""
    return """level_1_supervision_location != 'EXTERNAL_UNKNOWN'
      AND level_2_supervision_location != 'EXTERNAL_UNKNOWN'
      AND CASE
            -- TODO(#3829): MO does not have level 2 values ingested, so level_2_supervision_location values are only
            -- 'EXTERNAL_UNKNOWN'. For scale reasons, we filter for only rows that are aggregated on
            -- level_2_supervision_location to filter out the "duplicate" rows with 'EXTERNAL_UNKNOWN'.
            WHEN state_code = 'US_MO' THEN level_2_supervision_location = 'ALL'
            ELSE level_1_supervision_location = 'ALL' OR level_2_supervision_location != 'ALL'
        END"""


def state_specific_dimension_filter(filter_admission_type: bool = False) -> str:
    """State-specific logic for filtering rows based on dimensions that are supported on the FE. This helps us avoid
    sending data to the FE that is never used."""
    us_mo_comment = (
        """-- US_MO doesn't support the"""
        f""" {"admission_type or " if filter_admission_type else ''}supervision_level filters"""
    )
    us_mo_filter = f"""({"admission_type = 'ALL' AND " if filter_admission_type else ''}supervision_level = 'ALL'))"""

    return f"""{us_mo_comment}
        (state_code != 'US_MO' OR {us_mo_filter}
        -- US_PA doesn't support the supervision_type or charge category filters
        AND (state_code != 'US_PA' OR (supervision_type = 'ALL' AND charge_category = 'ALL'))"""


def state_specific_supervision_type_inclusion_filter() -> str:
    """State-specific logic for filtering rows based on dimension values that are not supported for a given state."""
    return """-- US_PA only includes PAROLE
        (state_code != 'US_PA' OR supervision_type = 'PAROLE')"""


def state_specific_supervision_type_groupings() -> str:
    """State-specific logic for grouping multiple supervision types together."""
    return """-- US_ID counts DUAL under PAROLE
        CASE
            WHEN state_code = 'US_ID' AND supervision_type = 'DUAL' THEN 'PAROLE'
            ELSE supervision_type
        END AS supervision_type"""


def state_specific_admission_type_inclusion_filter() -> str:
    """State-specific admission_type inclusions"""
    return """
    -- US_MO only includes Legal Revocation admissions
    (state_code != 'US_MO' OR specialized_purpose_for_incarceration = 'GENERAL')
    -- US_PA includes Legal Revocation and Shock Incarceration admissions
    AND (state_code != 'US_PA' OR specialized_purpose_for_incarceration IN ('GENERAL', 'SHOCK_INCARCERATION'))"""


def state_specific_admission_type() -> str:
    return """CASE WHEN specialized_purpose_for_incarceration = 'GENERAL' THEN 'LEGAL_REVOCATION'
             WHEN state_code = 'US_PA' THEN
                 CASE WHEN specialized_purpose_for_incarceration = 'SHOCK_INCARCERATION' THEN
                         CASE WHEN purpose_for_incarceration_subtype = 'PVC' THEN 'SHOCK_INCARCERATION_PVC'
                              WHEN purpose_for_incarceration_subtype = 'RESCR' THEN 'SHOCK_INCARCERATION_0_TO_6_MONTHS'
                              WHEN purpose_for_incarceration_subtype = 'RESCR6' THEN 'SHOCK_INCARCERATION_6_MONTHS'
                              WHEN purpose_for_incarceration_subtype = 'RESCR9' THEN 'SHOCK_INCARCERATION_9_MONTHS'
                              WHEN purpose_for_incarceration_subtype = 'RESCR12' THEN 'SHOCK_INCARCERATION_12_MONTHS'
                         END
                  END
             ELSE specialized_purpose_for_incarceration
        END AS admission_type"""


def state_specific_admission_history_description() -> str:
    """State-specific logic for aggregating admissions type history."""
    return """CASE
            -- US_MO only includes Legal Revocation admissions, so we display the number of admissions instead
            WHEN state_code = 'US_MO' THEN CAST(COUNT(admission_type) AS STRING)
            ELSE STRING_AGG(admission_type, ";" ORDER BY admission_date)
            END AS admission_history_description"""


def vitals_state_specific_district_id(table: str) -> str:
    """State-specific logic for pulling in the right district ID for vitals."""
    return f"""CASE
            WHEN {table}.state_code IN {VITALS_LEVEL_1_SUPERVISION_LOCATION_OPTIONS}
                THEN IFNULL({table}.level_1_supervision_location_external_id, "UNKNOWN")
            WHEN {table}.state_code IN {VITALS_LEVEL_2_SUPERVISION_LOCATION_OPTIONS}
                THEN IFNULL({table}.level_2_supervision_location_external_id, "UNKNOWN")
        END as district_id"""


def vitals_state_specific_district_name(table: str) -> str:
    """State-specific logic for pulling in the right district name for vitals."""
    return f"""CASE
            WHEN {table}.state_code IN {VITALS_LEVEL_1_SUPERVISION_LOCATION_OPTIONS}
                THEN locations.level_1_supervision_location_name
            WHEN {table}.state_code = "US_ID"
                # We want the district name for US_ID to be the district id.
                THEN {table}.level_2_supervision_location_external_id
            WHEN {table}.state_code = "US_MO"
                THEN locations.level_2_supervision_location_name
        END as district_name"""


def vitals_state_specific_join_with_supervision_location_ids(left_table: str) -> str:
    """State-specific logic joining with supervision location table for vitals."""
    return f"""CASE
            WHEN {left_table}.state_code IN {VITALS_LEVEL_1_SUPERVISION_LOCATION_OPTIONS}
                THEN {left_table}.level_1_supervision_location_external_id = 
                    locations.level_1_supervision_location_external_id
            WHEN {left_table}.state_code IN {VITALS_LEVEL_2_SUPERVISION_LOCATION_OPTIONS}
                THEN {left_table}.level_2_supervision_location_external_id =
                        locations.level_2_supervision_location_external_id
            END"""


def vitals_state_specific_join_with_supervision_population(right_table: str) -> str:
    """State-specific logic joining with supervision population table for vitals."""
    return f"""CASE
            WHEN {right_table}.state_code IN {VITALS_LEVEL_1_SUPERVISION_LOCATION_OPTIONS}
                THEN sup_pop.supervising_district_external_id = {right_table}.level_1_supervision_location_external_id
            WHEN {right_table}.state_code IN {VITALS_LEVEL_2_SUPERVISION_LOCATION_OPTIONS}
                THEN sup_pop.supervising_district_external_id = {right_table}.level_2_supervision_location_external_id
        END"""


def vitals_state_specific_district_display_name(
    state_code: str, district_name: str
) -> str:
    """State-specific logic to normalize district names into displayable versions."""
    return f"""
        CASE {state_code}
          WHEN 'US_ND'
            THEN IF(
              INSTR(UPPER({district_name}), 'OFFICE') != 0,
              {district_name},
              CONCAT({district_name}, ' OFFICE'))
          WHEN 'US_ID'
            THEN REPLACE({district_name}, ' - ', ', ')
          ELSE {district_name}
        END
    """


def vitals_state_specific_supervision_location_exclusions(cte: str) -> str:
    """State-specific logic to exclude duplicate locations."""
    return f""",
    {cte}_excluded_locations AS (
        SELECT *
        FROM {cte}
        WHERE
            CASE
                WHEN state_code NOT IN {VITALS_LEVEL_2_SUPERVISION_LOCATION_OPTIONS} THEN
                    CASE
                        -- do not include ALL aggregations for level_2 supervision locations
                        WHEN supervising_officer_external_id = 'ALL' AND level_1_supervision_location_external_id = 'ALL'
                            THEN level_2_supervision_location_external_id = 'ALL'
                        ELSE TRUE
                    END
                -- take no action for other states
                ELSE TRUE
            END
    )
    """


def agent_state_specific_full_name(state_code: str) -> str:
    """State-specific logic to normalize agent names into displayable versions."""

    def build_full_name_from_given_and_surname() -> str:
        return "TRIM(CONCAT(COALESCE(given_names, ''), ' ', COALESCE(surname, '')))"

    def format_first_and_last_name() -> str:
        return "TRIM(CONCAT(SPLIT(full_name, ',')[SAFE_OFFSET(1)], ' ', SPLIT(full_name, ',')[SAFE_OFFSET(0)]))"

    return f"""
            CASE {state_code}
              WHEN 'US_ID'
                THEN IFNULL({format_first_and_last_name()}, {build_full_name_from_given_and_surname()})
              ELSE IFNULL(full_name, {build_full_name_from_given_and_surname()})
            END AS full_name
        """


class SpotlightFacilityType(Enum):
    PRISON = "prison"
    COMMUNITY = "community"


# 3-digit codes 1xx, 2xx, 3xx denote Community Correction Centers
# TODO(#10054): Use stricter regex once facilities are normalized.
PA_COMMUNITY_CORRECTIONS_MATCH = """REGEXP_CONTAINS(facility, r"^[123]\d\d\D*")"""


def spotlight_state_specific_facility_filter(
    facility_type: SpotlightFacilityType,
) -> str:
    """State-specific logic to identify community correctional facilities."""
    if facility_type is SpotlightFacilityType.COMMUNITY:
        match = "true"
    else:
        match = "false"

    return f"""
        CASE
            WHEN state_code = "US_PA" THEN
                {PA_COMMUNITY_CORRECTIONS_MATCH}
            ELSE false
        END = {match}
    """


def spotlight_state_specific_facility() -> str:
    """State-specific logic to normalize facility identifiers for Spotlight."""
    return f"""
        CASE
            WHEN state_code = 'US_PA' THEN
                (CASE
                    -- these are the PA correctional institutions
                    WHEN facility IN (
                        "ALB",
                        "CAM",
                        "BEN",
                        "CBS",
                        "CHS",
                        "COA",
                        "DAL",
                        "FRA",
                        "FRS",
                        "FYT",
                        "GRN",
                        "HOU",
                        "HUN",
                        "LAU",
                        "MAH",
                        "MER",
                        "MUN",
                        "PHX",
                        "PNG",
                        "QUE",
                        "ROC",
                        "SMI",
                        "SMR",
                        "WAM"
                    ) THEN facility
                    WHEN {PA_COMMUNITY_CORRECTIONS_MATCH} THEN facility
                    -- includes out-of-state placements and misc others
                    ELSE 'OTHER'
                END)
            WHEN state_code = 'US_ID' THEN
                CASE
                    WHEN facility = "KOOTENAI COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "JEFFERSON COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "ADA COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "JEROME COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "BONNEVILLE COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "CASSIA COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "CANYON COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "TWIN FALLS COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "BINGHAM COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "NEZ PERCE COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "BANNOCK COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "ELMORE COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "WASHINGTON COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "OWYHEE COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "CARIBOU COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "PAYETTE COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "ADAMS COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "MADISON COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "BOUNDARY COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "BONNER COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "GOODING COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "BLAINE COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "POWER COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "CLEARWATER COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "VALLEY COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "SHOSHONE COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "LEMHI COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "COUNTY JAIL" THEN "County Jail"
                    WHEN facility = "BENEWAH COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "LATAH COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "MINIDOKA COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "GEM COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "FREMONT COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "BUTTE COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "FRANKLIN COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "CUSTER COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "IDAHO COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "CLARK COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "JAIL BACKLOG" THEN "County Jail"
                    WHEN facility = "LEWIS COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "ONEIDA COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "LINCOLN COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "BEAR LAKE COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "BOISE COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "TETON COUNTY SHERIFF DEPARTMENT" THEN "County Jail"
                    WHEN facility = "OTHER JAILS" THEN "County Jail"
                    WHEN facility = "NAMPA COMMUNITY WORK CENTER, NAMPA" THEN "Community Reentry Centers"
                    WHEN facility = "TWIN FALLS COMMUNITY WORK CENTER, TWIN FALLS" THEN "Community Reentry Centers"
                    WHEN facility = "SICI COMMUNITY WORK CENTER" THEN "Community Reentry Centers"
                    WHEN facility = "EAST BOISE COMMUNITY WORK CENTER, BOISE" THEN "Community Reentry Centers"
                    WHEN facility = "IDAHO FALLS COMMUNITY WORK CENTER, IDAHO FALLS" THEN "Community Reentry Centers"
                    WHEN facility = "SAGUARO CORRECTIONAL CENTER, ARIZONA" THEN "Saguaro Correctional Center, Arizona"
                    WHEN facility = "SAGUARO CORR CENTER" THEN "Saguaro Correctional Center, Arizona"
                    WHEN facility = "CORRECTIONAL ALTERNATIVE PLACEMENT PROGRAM - BOISE" THEN "Correctional Alternative Placement Program"
                    WHEN facility = "IDAHO CORRECTIONAL INSTITUTION, OROFINO" THEN "Idaho Correctional Institution-Orofino"
                    WHEN facility = "IDAHO MAXIMUM SECURITY INSTITUTION, BOISE" THEN "Idaho Maximum Security Institution"
                    WHEN facility = "IDAHO CORRECTIONAL CENTER - BOISE" THEN "Idaho State Correctional Center"
                    WHEN facility = "IDAHO STATE CORRECTIONAL INSTITUTION, BOISE" THEN "Idaho State Correctional Institution"
                    WHEN facility = "NORTH IDAHO CORRECTIONAL INSTITUTION, COTTONWOOD" THEN "North Idaho Correctional Institution"
                    WHEN facility = "POCATELLO WOMAN'S CORRECTIONAL CENTER, POCATELLO" THEN "Pocatello Women's Correctional Center"
                    WHEN facility = "ST. ANTHONY WORK CENTER, ST. ANTHONY" THEN "St. Anthony Work Camp"
                    WHEN facility = "SOUTH IDAHO CORRECTIONAL INSTITUTION, BOISE" THEN "South Idaho Correctional Institution"
                    WHEN facility = "PRE-RELEASE CENTER, SICI" THEN "South Idaho Correctional Institution"
                    WHEN facility = "WASHINGTON" THEN NULL
                    WHEN facility = "OREGON" THEN NULL
                    WHEN facility = "MONTANA" THEN NULL
                    WHEN facility = "UTAH" THEN NULL
                    WHEN facility = "TENNESSEE" THEN NULL
                    WHEN facility = "HAWAII" THEN NULL
                    WHEN facility = "WYOMING" THEN NULL
                    WHEN facility = "NEVADA" THEN NULL
                    WHEN facility = "KENTUCKY" THEN NULL
                    WHEN facility = "COLORADO" THEN NULL
                    WHEN facility = "VIRGINIA" THEN NULL
                    WHEN facility = "ILLINOIS" THEN NULL
                    WHEN facility = "OHIO" THEN NULL
                    WHEN facility = "MINNESOTA" THEN NULL
                    WHEN facility = "NEW HAMPSHIRE" THEN NULL
                    WHEN facility = "FLORIDA" THEN NULL
                    WHEN facility = "CALIFORNIA" THEN NULL
                    WHEN facility = "ARIZONA" THEN NULL
                    WHEN facility = "TEXAS" THEN NULL
                    WHEN facility = "PENNSYLVANIA" THEN NULL
                    WHEN facility = "GEORGIA" THEN NULL
                    WHEN facility = "KANSAS" THEN NULL
                    WHEN facility = "NEW YORK" THEN NULL
                    WHEN facility = "NEBRASKA" THEN NULL
                    WHEN facility = "MASSACHUSETTS" THEN NULL
                    WHEN facility = "ALASKA" THEN NULL
                    WHEN facility = "SOUTH CAROLINA" THEN NULL
                    WHEN facility = "ARKANSAS" THEN NULL
                    WHEN facility = "LOUISIANA" THEN NULL
                    WHEN facility = "INDIANA" THEN NULL
                    WHEN facility = "MISSOURI" THEN NULL
                    WHEN facility = "MISSISSIPPI" THEN NULL
                    WHEN facility = "OKLAHOMA" THEN NULL
                    WHEN facility = "MICHIGAN" THEN NULL
                    WHEN facility = "ALABAMA" THEN NULL
                    WHEN facility = "NORTH DAKOTA" THEN NULL
                    WHEN facility = "WISCONSIN" THEN NULL
                    WHEN facility = "NEW MEXICO" THEN NULL
                    WHEN facility = "IDAHO" THEN NULL
                    WHEN facility = "NORTH CAROLINA" THEN NULL
                    WHEN facility = "KARNES COUNTY CORRECTIONAL CENTER, TEXAS" THEN NULL
                    WHEN facility = "EAGLE PASS CORRECTIONAL FACILITY, TEXAS" THEN NULL
                    WHEN facility = "U.S. MARSHALL CUSTODY" THEN NULL
                    WHEN facility = "ABSENT WITHOUT LEAVE" THEN NULL
                    WHEN facility = "RECORDS BUREAU TRACKING" THEN NULL
                    WHEN facility = "U.S. MARSHAL DETAINER" THEN NULL
                    WHEN facility = "FEDERAL DETAINER" THEN NULL
                    WHEN facility = "FEDERAL BUREAU OF PRISONS" THEN NULL
                    WHEN facility = "FEDERAL CUSTODY" THEN NULL
                    WHEN facility = "U.S. IMMIGRATION NATURALIZATION DETAINER" THEN NULL
                    WHEN facility = "JUDICIAL DISTRICT" THEN NULL
                    WHEN facility = "COURT ORDERED RELEASE / BOND APPEAL" THEN NULL
                    WHEN facility = "SECURITY PLACEMENT" THEN NULL
                    WHEN facility = "ORCHARD EXTENSION" THEN NULL
                    WHEN facility = "STATE HOSPITAL" THEN NULL
                    WHEN facility = "DEPARTMENT OF JUVENILE CORRECTIONS" THEN NULL
                    WHEN facility = "OUT ON OWN RECOGNANCE" THEN NULL
                    WHEN facility = "UNKNOW LOCATION" THEN NULL
                    WHEN facility = "ST. JOSEPH'S HOSPITAL, LEWISTON" THEN NULL
                    WHEN facility = "EASTERN IDAHO REGIONAL MEDICAL CENTER, IDAHO FALLS" THEN NULL
                    WHEN facility = "PENDING ARREST" THEN NULL
                    WHEN facility = "ST. LUKE'S HOSPITAL, BOISE" THEN NULL
                    WHEN facility = "BINGHAN MEMORIAL, BLACKFOOT" THEN NULL
                    WHEN facility = "ABSCONDERS" THEN NULL
                    WHEN facility = "ON FURLOUGH" THEN NULL
                    WHEN facility = "ST. ALPHONSUS HOSPITAL, BOISE" THEN NULL
                    WHEN facility = "SOUTH BOISE WOMEN'S CORRECTIONAL CENTER" THEN "South Boise Women's Correctional Center"
                END
            ELSE facility
        END
        AS facility
    """


def _get_pathways_last_updated_date(tables: Dict[StateCode, str]) -> str:
    """Builds query for state-specific last updated dates, based on the `update_datetime` of each table
    in the provided mapping"""
    last_item_index = len(tables) - 1
    query_string = ""
    for index, (state_code, table_name) in enumerate(tables.items()):
        query_string += f"""
        SELECT
            \'{state_code.value}\' as state_code,
            date(max(update_datetime)) as last_updated
        FROM `{{project_id}}.{state_code.value.lower()}_raw_data.{table_name}`
         """
        if index != last_item_index:
            query_string += """
        UNION ALL
            """
    return query_string


def get_pathways_incarceration_last_updated_date() -> str:
    """Add state-specific last updated dates, based on the `update_datetime` of each state's pathways incarceration
    specific table."""
    return _get_pathways_last_updated_date(
        STATE_CODE_TO_PATHWAYS_INCARCERATION_LAST_UPDATED_DATE_SOURCE_TABLE
    )


def get_pathways_supervision_last_updated_date() -> str:
    """Add state-specific last updated dates, based on the `update_datetime` of each state's pathways supervision
    specific table."""
    return _get_pathways_last_updated_date(
        STATE_CODE_TO_PATHWAYS_SUPERVISION_LAST_UPDATED_DATE_SOURCE_TABLE
    )


def pathways_state_specific_facility_filter() -> str:
    """State-specific logic to filter out facilities that should not be included in
    Pathways metrics."""
    return """
        CASE
            # TODO(#10432): Remove this clause when we better understand what TABLET is.
            WHEN state_code = "US_ND" THEN
                facility != "TABLET"
            WHEN state_code = "US_TN" THEN
                facility not in ("CJ","WH", "GENERAL")
            WHEN state_code = "US_ME" THEN
                facility not in ("BANGOR (MAIN OFFICE), ADULT")
            WHEN state_code = "US_MI" THEN
                facility not in ("COUNTY JAILS", "JLS", "SPECIAL ALTERNATIVE INCARCERATION/MEN'S", "SPECIAL ALTERNATIVE INCARCERATION/WOMEN'S", "ZLI", "ZLW")
            ELSE true
        END
    """


def pathways_state_specific_supervision_district_filter(
    *,
    district_column_name: str = "district",
) -> str:
    """State-specific logic to filter out supervision locations that should not be included in
    Pathways metrics."""
    return f"""
        CASE
            WHEN state_code = "US_ME" THEN
                UPPER({district_column_name}) NOT IN (
                    -- Filter out Central Office Facilities
                    "NON-COMMITTED ADULT",
                    "TEMP SOCIETY OUT ADULT",
                    "TEMP SOCIETY OUT",
                    "CENTRAL OFFICE",
                     -- Filter out DOC Facilities
                     -- TODO(#12239): Remove this filtering once we figure out why there are DOC facilities in supervision periods
                    "MAINE CORRECTIONAL CENTER",
                    "MOUNTAIN VIEW CORRECTIONAL FACILITY",
                    "SOUTHERN MAINE WOMEN'S REENTRY CENTER",
                    "BOLDUC CORRECTIONAL FACILITY",
                    "MAINE STATE PRISON",
                    "CENTRAL MAINE PRE-RELEASE CENTER",
                    "DOWNEAST CORRECTIONAL FACILITY",
                    "BANGOR PRE-RELEASE CENTER",
                    "BANGOR WOMEN'S CENTER",
                    "CENTRAL OFFICE, IT",
                    "MOUNTAIN VIEW ADULT CENTER",
                    "CHARLESTON CORRECTIONAL FACILITY",
                    "SOUTHERN MAINE PRE-RELEASE"
                )
            ELSE TRUE
        END
    """


def pathways_state_specific_supervision_level(
    state_code_query: str = "state_code",
    supervision_level_query: str = "supervision_level",
) -> str:
    """State-specific logic to normalize supervision level for Pathways."""
    return f"""
        CASE 
            WHEN {state_code_query}='US_ID' THEN
                (CASE
                    WHEN COALESCE({supervision_level_query}, "INTERNAL_UNKNOWN") = "INTERNAL_UNKNOWN"
                        THEN "OTHER"
                    WHEN {supervision_level_query} = "MAXIMUM"
                        THEN "HIGH"
                    ELSE IFNULL({supervision_level_query}, "EXTERNAL_UNKNOWN")
                END)
            ELSE IFNULL({supervision_level_query}, "EXTERNAL_UNKNOWN")
        END
    """


def get_all_primary_supervision_external_id_types() -> tuple:
    """Returns a tuple of strings that indicate all of the state external id types for queries."""
    supervision_id_types = []
    for state in get_supported_states():
        delegate = get_required_state_specific_metrics_producer_delegates(
            state_code=state.value,
            required_delegates={StateSpecificSupervisionMetricsProducerDelegate},
        ).get(StateSpecificSupervisionMetricsProducerDelegate.__name__)
        if delegate and delegate.primary_person_external_id_to_include():
            supervision_id_types.append(
                delegate.primary_person_external_id_to_include()
            )
    return tuple(supervision_id_types)
