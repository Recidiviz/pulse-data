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
"""Unions together state-specific processing files that have Custody Classification Dates"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CUSTODY_CLASSIFICATION_ASSESSMENT_DATES_VIEW_NAME = (
    "custody_classification_assessment_dates"
)

CUSTODY_CLASSIFICATION_ASSESSMENT_DATES_VIEW_DESCRIPTION = """Unions together state-specific processing files that have Custody Classification Dates"""

CUSTODY_CLASSIFICATION_ASSESSMENT_DATES_QUERY_TEMPLATE = """
    SELECT
        state_code,
        person_id,
        assessment_date,
        classification_date,
        classification_decision_date,
        classification_decision,
        assessment_due_date,
    FROM 
        `{project_id}.analyst_data.us_tn_custody_classification_assessment_dates_preprocessed`
"""

CUSTODY_CLASSIFICATION_ASSESSMENT_DATES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=CUSTODY_CLASSIFICATION_ASSESSMENT_DATES_VIEW_NAME,
    description=CUSTODY_CLASSIFICATION_ASSESSMENT_DATES_VIEW_DESCRIPTION,
    view_query_template=CUSTODY_CLASSIFICATION_ASSESSMENT_DATES_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CUSTODY_CLASSIFICATION_ASSESSMENT_DATES_VIEW_BUILDER.build_and_print()
