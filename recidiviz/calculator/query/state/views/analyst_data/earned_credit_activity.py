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
"""View that logs earned credit activity"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

EARNED_CREDIT_ACTIVITY_VIEW_NAME = "earned_credit_activity"

EARNED_CREDIT_ACTIVITY_VIEW_DESCRIPTION = (
    """State agnostic version of earned credit activity"""
)

EARNED_CREDIT_ACTIVITY_QUERY_TEMPLATE = """
SELECT 
    state_code,
    person_id,
    credit_date,
    credit_type,
    credits_earned,
    TO_JSON(STRUCT(
        activity_code,
        activity,
        activity_type,
        rating,
        activity_seq)
        ) AS activity_attributes,
FROM `{project_id}.{analyst_dataset}.us_ma_earned_credit_activity_preprocessed_materialized`
"""
EARNED_CREDIT_ACTIVITY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=EARNED_CREDIT_ACTIVITY_VIEW_NAME,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    description=EARNED_CREDIT_ACTIVITY_VIEW_DESCRIPTION,
    view_query_template=EARNED_CREDIT_ACTIVITY_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        EARNED_CREDIT_ACTIVITY_VIEW_BUILDER.build_and_print()
