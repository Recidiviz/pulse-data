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
Flattened sentence and charge data
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.state.dataset_config import SENTENCE_SESSIONS_DATASET
from recidiviz.calculator.query.state.views.sessions.state_sentence_configurations import (
    STATES_NOT_MIGRATED_TO_SENTENCE_V2_SCHEMA,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SENTENCES_AND_CHARGES_VIEW_ID = "sentences_and_charges"

QUERY_TEMPLATE = """
WITH charges AS (
    SELECT
        charge_v2_id,
        NULL AS charge_id,
        assoc.sentence_id,
        charge.external_id AS charge_external_id,
        charge.county_code AS charge_county_code,
        charge.* EXCEPT (charge_v2_id, external_id, county_code),
        charge_labels.* EXCEPT(offense_description, probability),
        COALESCE(
            charge.judicial_district_code,
            TRIM(scc.judicial_district_code),
            'EXTERNAL_UNKNOWN'
        ) AS judicial_district,
    FROM `{project_id}.normalized_state.state_charge_v2` charge
    LEFT JOIN `{project_id}.normalized_state.state_charge_v2_state_sentence_association` assoc
        USING (state_code, charge_v2_id)
    LEFT JOIN `{project_id}.static_reference_tables.state_county_codes` scc
        USING (state_code, county_code)
    LEFT JOIN `{project_id}.reference_views.cleaned_offense_description_to_labels` charge_labels
        ON charge.description = charge_labels.offense_description
    WHERE state_code NOT IN ({v2_non_migrated_states})
),
sentences_and_charges AS (
    SELECT
        sent.person_id,
        sent.state_code,
        sent.sentence_id,
        sent.external_id AS sentence_external_id,
        charges.charge_v2_id,
        NULL AS charge_id,
        charges.charge_external_id,
        sent.sentence_inferred_group_id,
        sent_group.sentence_group_id,
        sent.sentence_group_external_id,
        sent.sentence_imposed_group_id,
        sent.sentence_type,
        sent.sentence_type_raw_text,
        sent.sentencing_authority,
        sent.sentencing_authority_raw_text,
        sent.imposed_date,
        sent.initial_time_served_days,
        COALESCE(sent.is_life, FALSE) AS is_life,
        sent.is_capital_punishment,
        sent.parole_possible,
        sent.county_code AS sentence_county_code,
        sent.sentence_metadata,
        charges.charge_county_code,
        charges.offense_date,
        charges.date_charged,
        charges.ncic_code,
        charges.statute,
        charges.description,
        charges.attempted,
        COALESCE(charges.classification_type, 'EXTERNAL_UNKNOWN') AS classification_type,
        charges.classification_type_raw_text,
        COALESCE(charges.classification_subtype, 'EXTERNAL_UNKNOWN') AS classification_subtype,
        charges.offense_type,
        charges.is_violent,
        charges.is_sex_offense,
        charges.is_drug,
        charges.counts,
        charges.charge_notes,
        charges.is_controlling,
        charges.charging_entity,
        charges.judicial_district,
        charges.judge_full_name,
        charges.judge_external_id,
        charges.uccs_code_uniform,
        charges.uccs_description_uniform,
        charges.uccs_category_uniform,
        charges.ncic_code_uniform,
        charges.ncic_description_uniform,
        charges.ncic_category_uniform,
        charges.nibrs_code_uniform,
        charges.nibrs_description_uniform,
        charges.nibrs_category_uniform,
        charges.crime_against_uniform,
        charges.is_drug_uniform,
        charges.is_violent_uniform,
        charges.offense_completed_uniform,
        charges.offense_attempted_uniform,
        charges.offense_conspired_uniform,
    FROM `{project_id}.normalized_state.state_sentence` AS sent
    LEFT JOIN charges
        USING (state_code, sentence_id)
    LEFT JOIN `{project_id}.normalized_state.state_sentence_group` AS sent_group
        ON sent.state_code = sent_group.state_code
        AND sent.person_id = sent_group.person_id
        AND sent.sentence_group_external_id = sent_group.external_id
    WHERE sent.external_id IS NOT NULL
        AND sent.state_code NOT IN ({v2_non_migrated_states})

    UNION ALL

    # TODO(#33402): deprecate `sentences_preprocessed` once all states are migrated to v2 infra
    SELECT
        person_id,
        state_code,
        sentence_id,
        sent.external_id AS sentence_external_id,
        CAST(NULL AS INT64) AS charge_v2_id,
        charge_id,
        charges.external_id AS charge_external_id,
        NULL AS sentence_inferred_group_id,
        NULL AS sentence_group_id,
        NULL AS sentence_group_external_id,
        NULL AS sentence_imposed_group_id,
        -- Use the V1 sentence_type values to mimic the V2 sentence_type enums
        CASE
            WHEN sent.sentence_type = "INCARCERATION" THEN "STATE_PRISON"
            ELSE sent.sentence_sub_type
        END AS sentence_type,
        NULL AS sentence_type_raw_text,
        NULL AS sentencing_authority,
        NULL AS sentencing_authority_raw_text,
        sent.date_imposed AS imposed_date,
        sent.initial_time_served_days,
        sent.life_sentence AS is_life,
        CAST(NULL AS BOOL) AS is_capital_punishment,
        CAST(NULL AS BOOL) AS parole_possible,
        sent.county_code AS sentence_county_code,
        sent.sentence_metadata,
        charges.county_code AS charge_county_code,
        sent.offense_date,
        CAST(NULL AS DATE) AS date_charged,
        sent.ncic_code,
        sent.statute,
        sent.description,
        charges.attempted,
        COALESCE(sent.classification_type, 'EXTERNAL_UNKNOWN') AS classification_type,
        charges.classification_type_raw_text,
        COALESCE(sent.classification_subtype, 'EXTERNAL_UNKNOWN') AS classification_subtype,
        sent.offense_type,
        sent.is_violent,
        sent.is_sex_offense,
        charges.is_drug,
        charges.counts,
        charges.charge_notes,
        charges.is_controlling,
        charges.charging_entity,
        sent.judicial_district,
        charges.judge_full_name,
        charges.judge_external_id,
        sent.uccs_code_uniform,
        sent.uccs_description_uniform,
        sent.uccs_category_uniform,
        sent.ncic_code_uniform,
        sent.ncic_description_uniform,
        sent.ncic_category_uniform,
        sent.nibrs_code_uniform,
        sent.nibrs_description_uniform,
        sent.nibrs_category_uniform,
        sent.crime_against_uniform,
        sent.is_drug_uniform,
        sent.is_violent_uniform,
        sent.offense_completed_uniform,
        sent.offense_attempted_uniform,
        sent.offense_conspired_uniform,
    FROM `{project_id}.sessions.sentences_preprocessed_materialized` sent
    LEFT JOIN `{project_id}.sessions.charges_preprocessed` charges
        USING (state_code, person_id, charge_id)
    WHERE state_code IN ({v2_non_migrated_states})
)
SELECT
    ROW_NUMBER() OVER (
        PARTITION BY state_code, person_id
        ORDER BY imposed_date, sentence_external_id, charge_external_id, sentence_type
    ) AS sentences_and_charges_id,
    *,
FROM sentences_and_charges
"""

SENTENCES_AND_CHARGES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SENTENCE_SESSIONS_DATASET,
    view_id=SENTENCES_AND_CHARGES_VIEW_ID,
    view_query_template=QUERY_TEMPLATE,
    description=__doc__,
    v2_non_migrated_states=list_to_query_string(
        string_list=STATES_NOT_MIGRATED_TO_SENTENCE_V2_SCHEMA,
        quoted=True,
    ),
    should_materialize=True,
    clustering_fields=["state_code", "person_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCES_AND_CHARGES_VIEW_BUILDER.build_and_print()
