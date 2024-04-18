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
with StateSentence entities.

We are looking for StateSentence entities of that do not have v1 counterpart
by joining external ID.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import STATE_BASE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

# Originally this checked sentence_type as well, but
# many of the v1 sentences were actually INTERNAL_UNKNOWN.
# So we are checking imposition date instead which should
# be consistent between both versions of the schema.
V2_SENTENCES = """
    SELECT 
        person_id, 
        external_id, 
        imposed_date AS imposition_v2,
        state_code AS region_code
    FROM 
        `{project_id}.{state_dataset}.state_sentence`
"""

V1_SENTENCES = """
    SELECT 
        person_id, 
        CASE 
            WHEN state_code = 'US_MO'
            THEN concat(external_id, '-INCARCERATION')
        END AS external_id,
        date_imposed AS imposition_v1,
        state_code AS region_code
    FROM 
        `{project_id}.{state_dataset}.state_incarceration_sentence`
    UNION ALL
    SELECT 
        person_id, 
        CASE 
            WHEN state_code = 'US_MO'
            THEN concat(external_id, '-SUPERVISION')
        END AS external_id,
        date_imposed AS imposition_v1,
        state_code AS region_code
    FROM 
        `{project_id}.{state_dataset}.state_supervision_sentence`
"""


QUERY = f"""
WITH 
    v2 AS ({V2_SENTENCES}),
    v1 AS ({V1_SENTENCES})
SELECT * 
FROM v2 
LEFT JOIN v1 
USING(person_id, external_id, region_code)
"""


SENTENCE_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id="sentence_comparison",
    view_query_template=QUERY,
    description=__doc__.strip(),
    state_dataset=STATE_BASE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCE_COMPARISON_VIEW_BUILDER.build_and_print()
