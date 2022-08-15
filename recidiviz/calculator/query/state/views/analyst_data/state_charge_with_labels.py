# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Temporary copy of state_charge with offense labels added"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    STATE_BASE_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

STATE_CHARGE_WITH_LABELS_VIEW_NAME = "state_charge_with_labels"

STATE_CHARGE_WITH_LABELS_VIEW_DESCRIPTION = """
This view copies state.state_charge and joins static_reference_tables.offense_description_to_labels.
"""

MATCH_PROBABILITY_CUTOFF = "0.5"


def hydrate_by_probability(
    result_column: str, probability_column: str = "probability"
) -> str:
    return f"""CASE WHEN {probability_column} IS NULL THEN "EXTERNAL_UNKNOWN"
    WHEN {probability_column} < {MATCH_PROBABILITY_CUTOFF} THEN "INTERNAL_UNKNOWN"
    ELSE {result_column} END"""


def null_if_low_probability(
    result_column: str, probability_column: str = "probability"
) -> str:
    return (
        f"IF({probability_column} < {MATCH_PROBABILITY_CUTOFF}, NULL, {result_column})"
    )


STATE_CHARGE_WITH_LABELS_QUERY_TEMPLATE = f"""
    /*{{description}}*/
    --TODO(#14102): Replace all usages of this view with normalize_state.state_charge once the schema change request to hydrate these fields in normalization is implemented
    WITH offense_desc_labels_dedup AS
    /*
    This is done to ensure that if duplicate descriptions get added to the static reference table that we don't create 
    duplicate charges
    */
    (
    SELECT
        * EXCEPT(ncic_code),
        LPAD(CAST(ncic_code AS STRING), 4,'0') AS ncic_code
    FROM `{{project_id}}.{{static_reference_dataset}}.offense_description_to_labels_materialized`
    QUALIFY ROW_NUMBER() OVER(PARTITION BY offense_description ORDER BY probability DESC) = 1
    )
    SELECT
        state_charge.*,
        probability AS probability_match_uniform,
        {null_if_low_probability('l.uccs_code')} AS uccs_code_uniform,
        {hydrate_by_probability('l.uccs_description')} AS uccs_description_uniform,
        {hydrate_by_probability('l.uccs_category')} AS uccs_category_uniform,
        {hydrate_by_probability('l.ncic_code')} AS ncic_code_uniform,
        {hydrate_by_probability('l.ncic_description')} AS ncic_description_uniform,
        {hydrate_by_probability('l.ncic_category')} AS ncic_category_uniform,
        {hydrate_by_probability('l.nbirs_code')} AS nbirs_code_uniform,
        {hydrate_by_probability('l.nbirs_description')} AS nbirs_description_uniform,
        {hydrate_by_probability('l.nbirs_category')} AS nbirs_category_uniform,
        {hydrate_by_probability('l.crime_against')} AS crime_against_uniform,
        {null_if_low_probability('l.is_drug')} AS is_drug_uniform,
        {null_if_low_probability('l.is_violent')} AS is_violent_uniform,
        {null_if_low_probability('l.offense_completed')} AS offense_completed_uniform,
        {null_if_low_probability('l.offense_attempted')} AS offense_attempted_uniform,
        {null_if_low_probability('l.offense_conspired')} AS offense_conspired_uniform,
    FROM `{{project_id}}.{{base_dataset}}.state_charge` state_charge
    LEFT JOIN offense_desc_labels_dedup l
        ON state_charge.description = l.offense_description
"""

STATE_CHARGE_WITH_LABELS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=STATE_CHARGE_WITH_LABELS_VIEW_NAME,
    view_query_template=STATE_CHARGE_WITH_LABELS_QUERY_TEMPLATE,
    description=STATE_CHARGE_WITH_LABELS_VIEW_DESCRIPTION,
    base_dataset=STATE_BASE_DATASET,
    static_reference_dataset=STATIC_REFERENCE_TABLES_DATASET,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        STATE_CHARGE_WITH_LABELS_VIEW_BUILDER.build_and_print()
