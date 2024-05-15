# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
This validation view compares StateIncarcerationSentence and StateSupervisionSentence
with StateSentence entities--ensuring they have the same charge.

We are looking for StateSentence/StateChargeV2 pairings that do not have v1 counterpart
by joining external IDs.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

# Originally this checked sentence_type as well, but
# many of the v1 sentences were actually INTERNAL_UNKNOWN.
# So we are checking charge external ID which should
# be consistent between both versions of the schema.
V2_SENTENCES = """
    SELECT 
        sentence.external_id AS sentence_external_id, 
        charge_v2.external_id AS charge_v2_external_id,
        state_code
    FROM 
        `{project_id}.{state_dataset}.state_charge_v2_state_sentence_association`
    JOIN
        `{project_id}.{state_dataset}.state_charge_v2` AS charge_v2
    USING 
        (charge_v2_id, state_code)
    JOIN
        `{project_id}.{state_dataset}.state_sentence` AS sentence
    USING
        (sentence_id, state_code)
"""

# TODO(#29211) Update this query to handle US_ID external IDs once entities are hydrated
V1_SENTENCES = """
    SELECT 
        CASE WHEN state_code = 'US_MO'
            THEN CONCAT(sentence.external_id, '-INCARCERATION') 
            ELSE sentence.external_id 
        END AS sentence_external_id, 
        charge.external_id AS charge_v1_external_id,
        state_code
    FROM 
        `{project_id}.{state_dataset}.state_charge_incarceration_sentence_association`
    JOIN
        `{project_id}.{state_dataset}.state_charge` AS charge
    USING
        (charge_id, state_code)
    JOIN
        `{project_id}.{state_dataset}.state_incarceration_sentence` AS sentence
    USING
        (incarceration_sentence_id, state_code)

    UNION ALL

    SELECT 
        CASE WHEN state_code = 'US_MO'
            THEN CONCAT(sentence.external_id, '-SUPERVISION') 
            ELSE sentence.external_id 
        END AS sentence_external_id, 
        charge.external_id AS charge_v1_external_id,
        state_code
    FROM 
        `{project_id}.{state_dataset}.state_charge_supervision_sentence_association`
    JOIN
        `{project_id}.{state_dataset}.state_charge` AS charge
    USING
        (charge_id, state_code)
    JOIN
        `{project_id}.{state_dataset}.state_supervision_sentence` AS sentence
    USING
        (supervision_sentence_id, state_code)
"""


QUERY = f"""
WITH 
    v2 AS ({V2_SENTENCES}),
    v1 AS ({V1_SENTENCES})
SELECT 
    state_code AS region_code,
    sentence_external_id,
    charge_v1_external_id,
    charge_v2_external_id
FROM v2 
LEFT JOIN v1 
USING (state_code, sentence_external_id)
"""


SENTENCE_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id="sentence_comparison",
    view_query_template=QUERY,
    description=__doc__.strip(),
    state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCE_COMPARISON_VIEW_BUILDER.build_and_print()
