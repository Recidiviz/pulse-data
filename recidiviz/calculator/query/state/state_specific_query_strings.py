# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

from typing import List, Optional, Dict
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
              WHEN {race_or_ethnicity_column} IS NULL THEN 'EXTERNAL_UNKNOWN'
              ELSE {race_or_ethnicity_column} END AS race_or_ethnicity"""


def state_specific_assessment_bucket(
    output_column_name: str = "assessment_score_bucket",
) -> str:
    return f"""-- TODO(#3135): remove this aggregation once the dashboard supports LOW_MEDIUM
        CASE WHEN state_code = 'US_MO' AND assessment_score_bucket = 'LOW_MEDIUM' THEN 'MEDIUM'
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
                  CASE WHEN supervision_level IN ('LIMITED', 'INTERNAL_UNKNOWN') THEN 'SPECIAL'
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


def state_specific_admission_type_inclusion_filter() -> str:
    """State-specific admission_type inclusions """
    return """
    -- US_MO only includes Legal Revocation admissions
    (state_code != 'US_MO' OR revocation_type = 'REINCARCERATION')
    -- US_PA includes Legal Revocation and Shock Incarceration admissions
    AND (state_code != 'US_PA' OR revocation_type IN ('REINCARCERATION', 'SHOCK_INCARCERATION'))"""


def state_specific_admission_type() -> str:
    return """CASE WHEN revocation_type = 'REINCARCERATION' THEN 'LEGAL_REVOCATION'
             WHEN state_code = 'US_PA' THEN
                 CASE WHEN revocation_type = 'SHOCK_INCARCERATION' THEN
                         CASE WHEN revocation_type_subtype = 'PVC' THEN 'SHOCK_INCARCERATION_PVC'
                              WHEN revocation_type_subtype = 'RESCR' THEN 'SHOCK_INCARCERATION_0_TO_6_MONTHS'
                              WHEN revocation_type_subtype = 'RESCR6' THEN 'SHOCK_INCARCERATION_6_MONTHS'
                              WHEN revocation_type_subtype = 'RESCR9' THEN 'SHOCK_INCARCERATION_9_MONTHS'
                              WHEN revocation_type_subtype = 'RESCR12' THEN 'SHOCK_INCARCERATION_12_MONTHS'
                         END
                  END
             ELSE revocation_type
        END AS admission_type"""


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
