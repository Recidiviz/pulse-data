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
Creates a table where each row is an imposed sentence group and includes:

Data based on sentence length at imposition.

    imposed_group_projected_completion_date_min: 
        The minimum projected completion date known nearest imposition of the sentences in this group.
    imposed_group_projected_completion_date_max: 
        The maximum projected completion date known nearest imposition of the sentences in this group.
    imposed_group_length_days_min: 
        The estimated minimum length required to serve by this group of sentences in days.
    imposed_group_length_days_max: 
        The estimated maximum length required to serve by this group of sentences in days.

Data detailing information of the most severe charge, defined in normalization.
These fields are prefixed with 'most_severe_charge_'.

Data arising from all charges/sentences in the group.

    any_is_violent: 
        True if any offenses related to this group were categorized as violent.
    any_is_drug:
        True if any offenses related to this group were categorized as drug offenses.
    any_is_sex_offense: 
        True if any offenses related to this group were categorized as sex offenses.
    any_life_sentence: 
        True if any sentence in this group is a life sentence.

TODO(#36539) Update this view when 'uniform' offense data is available from normalized_state.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SENTENCE_SESSIONS_DATASET
from recidiviz.calculator.query.state.views.reference.cleaned_offense_description_to_labels import (
    CLEANED_OFFENSE_DESCRIPTION_TO_LABELS_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SENTENCE_IMPOSED_GROUP_SUMMARY_ID = "sentence_imposed_group_summary"

QUERY_TEMPLATE = f"""

WITH

-- Unifies charge and offense information needed for this view's output
-- TODO(#36539) Update this view when normalized_state.state_charge_v2 has uniform fields
uniform_charges AS (
    SELECT
        charge.state_code,
        charge.charge_v2_id,
        charge.external_id,    
        charge.classification_type,
        charge.classification_subtype,
        charge.description,
        charge.is_sex_offense,
        charge.is_violent,
        charge.is_drug, 
        charge.ncic_category_external,
        clean_offense.ncic_category_uniform,
        clean_offense.is_violent_uniform,
        clean_offense.is_drug_uniform,
        clean_offense.ncic_code_uniform
    FROM 
        `{{project_id}}.normalized_state.state_charge_v2` AS charge
    JOIN
        `{{project_id}}.{CLEANED_OFFENSE_DESCRIPTION_TO_LABELS_VIEW_BUILDER.table_for_query.to_str()}` AS clean_offense
    ON
        charge.description = clean_offense.offense_description
),
-- Gathers completion dates to be used in imposed group aggregation
initial_sentence_lengths AS (
    SELECT
        sentence_id,
        projected_completion_date_min_external, 
        projected_completion_date_max_external
    FROM
        `{{project_id}}.normalized_state.state_sentence_length`
    WHERE
        sequence_num = 1
),

-- Aggregates offense and sentence characteristics by imposed group
aggregated_sentence_charge_data AS (
    SELECT
        imposed_groups.sentence_imposed_group_id,
        imposed_groups.most_severe_charge_v2_id,
        imposed_groups.sentencing_authority,
        imposed_groups.imposed_date,
        imposed_groups.serving_start_date,
        LOGICAL_OR(all_charges.is_sex_offense) AS any_is_sex_offense,
        LOGICAL_OR(all_charges.is_violent_uniform) AS any_is_violent,
        LOGICAL_OR(all_charges.is_drug_uniform) AS any_is_drug,
        LOGICAL_OR(sentences.is_life) AS any_is_life,
        MAX(initial_sentence_lengths.projected_completion_date_min_external) AS projected_completion_date_min, 
        MAX(initial_sentence_lengths.projected_completion_date_max_external) AS projected_completion_date_max
    FROM 
        `{{project_id}}.normalized_state.state_sentence_imposed_group` AS imposed_groups
    JOIN 
        `{{project_id}}.normalized_state.state_sentence` AS sentences
    USING 
        (sentence_imposed_group_id)
    JOIN 
        `{{project_id}}.normalized_state.state_charge_v2_state_sentence_association` AS charge_to_sentence
    USING 
        (sentence_id)
    JOIN 
        uniform_charges AS all_charges
    USING 
        (charge_v2_id)
    LEFT JOIN 
        initial_sentence_lengths
    ON
        sentences.sentence_id = initial_sentence_lengths.sentence_id
    GROUP BY
        sentence_imposed_group_id, most_severe_charge_v2_id, sentencing_authority, serving_start_date, imposed_date
)
SELECT
    most_severe_charge.state_code,
    imposed_group.sentence_imposed_group_id,
    imposed_group.most_severe_charge_v2_id,
    imposed_group.sentencing_authority,
    imposed_group.imposed_date,
    imposed_group.serving_start_date,
    imposed_group.any_is_sex_offense,
    imposed_group.any_is_violent,
    imposed_group.any_is_drug,
    imposed_group.any_is_life,
    imposed_group.projected_completion_date_min, 
    imposed_group.projected_completion_date_max,
    DATE_DIFF(
        imposed_group.projected_completion_date_min,
        imposed_group.serving_start_date,
        DAY
    ) AS sentence_length_days_min,
    DATE_DIFF(
        imposed_group.projected_completion_date_max, 
        imposed_group.serving_start_date,
        DAY
    ) AS sentence_length_days_max,
    most_severe_charge.external_id AS most_severe_charge_external_id,    
    most_severe_charge.classification_type AS most_severe_charge_classification_type,
    most_severe_charge.classification_subtype AS most_severe_charge_classification_subtype,
    most_severe_charge.description AS most_severe_charge_description,
    most_severe_charge.is_sex_offense AS most_severe_charge_is_sex_offense,
    most_severe_charge.is_violent AS most_severe_charge_is_violent,
    most_severe_charge.is_drug AS most_severe_charge_is_drug, 
    most_severe_charge.ncic_category_external AS most_severe_charge_ncic_category_external,
    most_severe_charge.ncic_category_uniform AS most_severe_charge_ncic_category_uniform,
    most_severe_charge.is_violent_uniform AS most_severe_charge_is_violent_uniform,
    most_severe_charge.is_drug_uniform AS most_severe_charge_is_drug_uniform,
    most_severe_charge.ncic_code_uniform AS most_severe_charge_ncic_code_uniform

FROM
    aggregated_sentence_charge_data AS imposed_group
JOIN
    uniform_charges AS most_severe_charge
ON
    imposed_group.most_severe_charge_v2_id = most_severe_charge.charge_v2_id
"""

SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SENTENCE_SESSIONS_DATASET,
    view_id=SENTENCE_IMPOSED_GROUP_SUMMARY_ID,
    view_query_template=QUERY_TEMPLATE,
    description=__doc__,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER.build_and_print()
