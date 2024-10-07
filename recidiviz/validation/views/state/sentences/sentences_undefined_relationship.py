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
"""A validation view that contains all of the sentences that have an undefined relationship. This can be a result either
of a sentence being more than 6 levels from its parent sentence or from a sentence having a circular dependency (such as
a sentence being listed as consecutive to one of its children or from a sentence being listed as consecutive to itself)"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

SENTENCES_UNDEFINED_RELATIONSHIP_VIEW_NAME = "sentences_undefined_relationship"

SENTENCES_UNDEFINED_RELATIONSHIP_DESCRIPTION = """
A validation view that contains all of the sentences that have an undefined relationship. This can be a result either
of a sentence being more than 6 levels from its parent sentence or from a sentence having a circular dependency (such as
a sentence being listed as consecutive to one of its children or from a sentence being listed as consecutive to itself).
These undefined sentences will impact the accuracy of the field `max_sentence_imposed_group_length_days` in 
`sentence_imposed_group_summary`.
"""

SENTENCES_UNDEFINED_RELATIONSHIP_QUERY_TEMPLATE = """
SELECT
  state_code,
  state_code AS region_code,
  sentence_type,
  person_id,
  sentence_id,
FROM `{project_id}.{sessions_dataset}.sentence_relationship_materialized`
WHERE sentence_level IS NULL
"""


SENTENCES_UNDEFINED_RELATIONSHIP_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=SENTENCES_UNDEFINED_RELATIONSHIP_VIEW_NAME,
    view_query_template=SENTENCES_UNDEFINED_RELATIONSHIP_QUERY_TEMPLATE,
    description=SENTENCES_UNDEFINED_RELATIONSHIP_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCES_UNDEFINED_RELATIONSHIP_VIEW_BUILDER.build_and_print()
