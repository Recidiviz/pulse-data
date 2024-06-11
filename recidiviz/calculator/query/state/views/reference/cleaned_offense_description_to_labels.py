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
"""A cleaned up version of the offense_description_to_labels which provides
standardized UCCS offense descriptions and other metadata about a given charge
description.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.datasets.static_data.config import EXTERNAL_REFERENCE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

MATCH_PROBABILITY_CUTOFF = "0.5"


def hydrate_by_probability(column: str) -> str:
    return f"""CASE WHEN probability IS NULL THEN "EXTERNAL_UNKNOWN"
    WHEN probability < {MATCH_PROBABILITY_CUTOFF} THEN "INTERNAL_UNKNOWN"
    ELSE {column} END"""


def null_if_low_probability(column: str) -> str:
    return f"IF(probability < {MATCH_PROBABILITY_CUTOFF}, NULL, {column})"


CLEANED_OFFENSE_DESCRIPTION_TO_LABELS_VIEW_NAME = (
    "cleaned_offense_description_to_labels"
)

CLEANED_OFFENSE_DESCRIPTION_TO_LABELS_VIEW_DESCRIPTION = """A cleaned up version of the
offense_description_to_labels which provides standardized UCCS offense descriptions and
other metadata about a given charge description.
"""

CLEANED_OFFENSE_DESCRIPTION_TO_LABELS_VIEW_QUERY_TEMPLATE = f"""
WITH offense_desc_labels_dedup AS (
    SELECT * EXCEPT(ncic_code), 
        LPAD(CAST(ncic_code AS STRING), 4,'0') AS ncic_code
    FROM `{{project_id}}.{{external_reference_dataset}}.offense_description_to_labels`
    QUALIFY ROW_NUMBER() OVER(PARTITION BY offense_description ORDER BY probability DESC) = 1
)
SELECT
    offense_description,
    probability,
    # Uniform Crime Classification Standard code. First digit is the broad code (violent,
    # property, etc.), first three digits are a unique uccs_category, and all four digits
    # are a unique uccs_description
    {null_if_low_probability('uccs_code')} AS uccs_code_uniform,
    # Uniform Crime Classification Standard description associated with a uccs_code
    {hydrate_by_probability('uccs_description')} AS uccs_description_uniform,
    # Uniform Crime Classification broad category
    {hydrate_by_probability('uccs_category')} AS uccs_category_uniform,
    # The CJARS-provided NCIC (National Crime Information Center) code for this offense.
    {hydrate_by_probability('ncic_code')} AS ncic_code_uniform,
    # The human-readable description associated with the CJARS-provided NCIC code.
    {hydrate_by_probability('ncic_description')} AS ncic_description_uniform,
    # A high-level category associated with the CJARS-provided NCIC code (e.g.
    # Kidnapping, Bribery, etc).
    {hydrate_by_probability('ncic_category')} AS ncic_category_uniform,
    # National Incident-Based Reporting System code, used by FBI for nationwide crime
    # statistics
    {hydrate_by_probability('nibrs_code')} AS nibrs_code_uniform,
    # National Incident-Based Reporting System code description associated with a
    # nibrs_code
    {hydrate_by_probability('nibrs_description')} AS nibrs_description_uniform,
    # National Incident-Based Reporting System broad category
    {hydrate_by_probability('nibrs_category')} AS nibrs_category_uniform,
    # Describes the type of victim of this crime. One of “Person”, “Property”, “Society”.
    {hydrate_by_probability('crime_against')} AS crime_against_uniform,
    # Whether the charge is for a drug-related offense, as inferred from the
    # CJARS-provided NCIC code.
    {null_if_low_probability('is_drug')} AS is_drug_uniform,
    # Whether the charge is for a violent offense, as inferred from the CJARS-provided
    # NCIC code.
    {null_if_low_probability('is_violent')} AS is_violent_uniform,
    # True if the description suggests the offense was not only attempted or conspired.
    {null_if_low_probability('offense_completed')} AS offense_completed_uniform,
    # True if the description suggests the offense was attempted but not completed.
    {null_if_low_probability('offense_attempted')} AS offense_attempted_uniform,
    # True if the description suggests the offense was conspired but not attempted.
    {null_if_low_probability('offense_conspired')} AS offense_conspired_uniform
FROM offense_desc_labels_dedup
"""

CLEANED_OFFENSE_DESCRIPTION_TO_LABELS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=CLEANED_OFFENSE_DESCRIPTION_TO_LABELS_VIEW_NAME,
    view_query_template=CLEANED_OFFENSE_DESCRIPTION_TO_LABELS_VIEW_QUERY_TEMPLATE,
    description=CLEANED_OFFENSE_DESCRIPTION_TO_LABELS_VIEW_DESCRIPTION,
    external_reference_dataset=EXTERNAL_REFERENCE_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CLEANED_OFFENSE_DESCRIPTION_TO_LABELS_VIEW_BUILDER.build_and_print()
