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
"""A view showing calculated recommended custody levels for all spans of time"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

RECOMMENDED_CUSTODY_LEVEL_SPANS_VIEW_NAME = "recommended_custody_level_spans"

RECOMMENDED_CUSTODY_LEVEL_SPANS_VIEW_DESCRIPTION = """A view showing calculated recommended custody
levels for all spans of time"""

RECOMMENDED_CUSTODY_LEVEL_SPANS_QUERY_TEMPLATE = """
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
        recommended_custody_level,
        score_metadata
    FROM
        `{project_id}.{analyst_dataset}.us_tn_recommended_custody_level_spans`
        
    UNION ALL 
    
    SELECT
        state_code,
        person_id,
        start_date,
        end_date AS end_date_exclusive,
        recommended_custody_level,
        score_metadata
    FROM
        `{project_id}.{analyst_dataset}.us_ix_recommended_custody_level_spans`  
        
"""

RECOMMENDED_CUSTODY_LEVEL_SPANS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=RECOMMENDED_CUSTODY_LEVEL_SPANS_VIEW_NAME,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    description=RECOMMENDED_CUSTODY_LEVEL_SPANS_VIEW_DESCRIPTION,
    view_query_template=RECOMMENDED_CUSTODY_LEVEL_SPANS_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        RECOMMENDED_CUSTODY_LEVEL_SPANS_VIEW_BUILDER.build_and_print()
