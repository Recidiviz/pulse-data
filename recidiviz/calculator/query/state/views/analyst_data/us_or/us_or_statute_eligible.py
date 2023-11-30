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
"""Creates view to identify individuals' sentences that fall under eligible statutes"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_OR_STATUTE_ELIGIBLE_VIEW_NAME = "us_or_statute_eligible"

US_OR_STATUTE_ELIGIBLE_VIEW_DESCRIPTION = """Creates view to identify individuals' sentences that fall under eligible statutes"""

US_OR_STATUTE_ELIGIBLE_QUERY_TEMPLATE = """
    SELECT person_id,
        sentence_id,
        DATE("9999-12-31") AS start_date,
        DATE("9999-12-31") AS end_date,
        NULL AS meets_criteria,
    FROM `{project_id}.sessions.sentences_preprocessed_materialized`
    LIMIT 0
"""

US_OR_STATUTE_ELIGIBLE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_OR_STATUTE_ELIGIBLE_VIEW_NAME,
    description=US_OR_STATUTE_ELIGIBLE_VIEW_DESCRIPTION,
    view_query_template=US_OR_STATUTE_ELIGIBLE_QUERY_TEMPLATE,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_OR_STATUTE_ELIGIBLE_VIEW_BUILDER.build_and_print()
