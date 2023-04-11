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
"""Reference code for more detailed offense descriptions for charges."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

MATCH_PROBABILITY_CUTOFF = "0.5"


def hydrate_by_probability(column: str) -> str:
    cleaned_column = column if "." not in column else column.partition(".")[2]
    return f"""CASE WHEN probability IS NULL THEN "EXTERNAL_UNKNOWN"
    WHEN probability < {MATCH_PROBABILITY_CUTOFF} THEN "INTERNAL_UNKNOWN"
    ELSE {column} END AS {cleaned_column}"""


def null_if_low_probability(column: str) -> str:
    cleaned_column = column if "." not in column else column.partition(".")[2]
    return f"IF(probability < {MATCH_PROBABILITY_CUTOFF}, NULL, {column}) AS {cleaned_column}"


STATE_CHARGE_OFFENSE_DESCRIPTION_TO_LABELS_VIEW_NAME = (
    "state_charge_offense_description_to_labels"
)

STATE_CHARGE_OFFENSE_DESCRIPTION_LABELS_VIEW_DESCRIPTION = """Connects ingested state_charge
objects to the UCCS offense descriptions and other metadata about the charge."""

STATE_CHARGE_OFFENSE_DESCRIPTION_LABELS_VIEW_QUERY_TEMPLATE = f"""
WITH offense_desc_labels_dedup AS (
    SELECT * EXCEPT(ncic_code), 
        LPAD(CAST(ncic_code AS STRING), 4,'0') AS ncic_code
    FROM `{{project_id}}.{{static_reference_dataset}}.offense_description_to_labels_materialized`
    QUALIFY ROW_NUMBER() OVER(PARTITION BY offense_description ORDER BY probability DESC) = 1
)
SELECT
    state_charge.person_id,
    state_charge.state_code,
    state_charge.charge_id,
    l.offense_description,
    l.probability,
    {null_if_low_probability('l.uccs_code')},
    {hydrate_by_probability('l.uccs_description')},
    {hydrate_by_probability('l.uccs_category')},
    {hydrate_by_probability('l.ncic_code')},
    {hydrate_by_probability('l.ncic_description')},
    {hydrate_by_probability('l.ncic_category')},
    {hydrate_by_probability('l.nibrs_code')},
    {hydrate_by_probability('l.nibrs_description')},
    {hydrate_by_probability('l.nibrs_category')},
    {hydrate_by_probability('l.crime_against')},
    {null_if_low_probability('l.is_drug')},
    {null_if_low_probability('l.is_violent')},
    {null_if_low_probability('l.offense_completed')},
    {null_if_low_probability('l.offense_attempted')},
    {null_if_low_probability('l.offense_conspired')}
FROM `{{project_id}}.{{base_dataset}}.state_charge` state_charge
LEFT JOIN offense_desc_labels_dedup l
ON state_charge.description = l.offense_description
"""

STATE_CHARGE_OFFENSE_DESCRIPTION_LABELS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=STATE_CHARGE_OFFENSE_DESCRIPTION_TO_LABELS_VIEW_NAME,
    view_query_template=STATE_CHARGE_OFFENSE_DESCRIPTION_LABELS_VIEW_QUERY_TEMPLATE,
    description=STATE_CHARGE_OFFENSE_DESCRIPTION_LABELS_VIEW_DESCRIPTION,
    static_reference_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
    base_dataset=dataset_config.STATE_BASE_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        STATE_CHARGE_OFFENSE_DESCRIPTION_LABELS_VIEW_BUILDER.build_and_print()
