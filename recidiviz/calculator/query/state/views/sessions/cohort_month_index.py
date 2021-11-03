# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Table that defines the month-increments at which outcomes are evaluated against in cohort analyses"""
# pylint: disable=trailing-whitespace
# pylint: disable=line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

COHORT_MONTH_INDEX_VIEW_NAME = "cohort_month_index"

COHORT_MONTH_INDEX_VIEW_DESCRIPTION = """
    Table that defines the month-increments at which outcomes are evaluated against in cohort analyses. These are 1-month
    intervals for the first 18 months and then 6-month intervals from 18 months to 96 months.
    """

COHORT_MONTH_INDEX_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
        cohort_months
    FROM UNNEST(GENERATE_ARRAY(0, 18, 1)) AS cohort_months
    UNION DISTINCT
    SELECT
        cohort_months
    FROM UNNEST(GENERATE_ARRAY(0, 96, 6)) AS cohort_months
    """

COHORT_MONTH_INDEX_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=COHORT_MONTH_INDEX_VIEW_NAME,
    view_query_template=COHORT_MONTH_INDEX_QUERY_TEMPLATE,
    description=COHORT_MONTH_INDEX_VIEW_DESCRIPTION,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COHORT_MONTH_INDEX_VIEW_BUILDER.build_and_print()
