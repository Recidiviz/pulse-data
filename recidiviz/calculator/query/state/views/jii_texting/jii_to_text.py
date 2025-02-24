# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
A view that contains all JII to text across all relevant states
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

JII_TO_TEXT_VIEW_NAME = "jii_to_text"

JII_TO_TEXT_DESCRIPTION = (
    """A view that contains all JII to text across all relevant states"""
)

JII_TO_TEXT_QUERY_TEMPLATE = """
SELECT
    *,
    'US_ID_LSU' AS topic,
FROM `{project_id}.{jii_texting_views_dataset}.us_ix_lsu_materialized`
"""

JII_TO_TEXT_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.JII_TEXTING_DATASET_ID,
    view_id=JII_TO_TEXT_VIEW_NAME,
    view_query_template=JII_TO_TEXT_QUERY_TEMPLATE,
    description=JII_TO_TEXT_DESCRIPTION,
    jii_texting_views_dataset=dataset_config.JII_TEXTING_DATASET_ID,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        JII_TO_TEXT_VIEW_BUILDER.build_and_print()
