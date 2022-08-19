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
"""State agnostic consecutive sentence lookup"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_NAME = "consecutive_sentences_preprocessed"

CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_DESCRIPTION = (
    """State agnostic consecutive sentence lookup"""
)

CONSECUTIVE_SENTENCES_PREPROCESSED_QUERY_TEMPLATE = """
    /*{description}*/
    --TODO(#14719): Develop consistent way of storing consecutive sentence IDs in state sentencing tables
    SELECT
        *
    FROM `{project_id}.{sessions_dataset}.us_tn_consecutive_sentences_preprocessed_materialized`
    
    UNION ALL
    
    SELECT 
        *
    FROM `{project_id}.{sessions_dataset}.us_mo_consecutive_sentences_preprocessed_materialized`
    
    UNION ALL

    SELECT 
        *
    FROM `{project_id}.{sessions_dataset}.us_me_consecutive_sentences_preprocessed_materialized`

"""

CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_NAME,
    view_query_template=CONSECUTIVE_SENTENCES_PREPROCESSED_QUERY_TEMPLATE,
    description=CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
    clustering_fields=["state_code", "person_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER.build_and_print()
