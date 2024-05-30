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
"""Calculates the number of months between a custody level downgrade and when a custody assessment would have been due."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

NUMBER_MONTHS_BETWEEN_CUSTODY_DOWNGRADE_AND_ASSESSMENT_DUE_VIEW_NAME = (
    "number_months_between_custody_downgrade_and_assessment_due"
)

NUMBER_MONTHS_BETWEEN_CUSTODY_DOWNGRADE_AND_ASSESSMENT_DUE_VIEW_DESCRIPTION = """Calculates the number of months between
a custody level downgrade and when a custody assessment would have been due"""

NUMBER_MONTHS_BETWEEN_CUSTODY_DOWNGRADE_AND_ASSESSMENT_DUE_QUERY_TEMPLATE = f"""
    WITH assessment_dates AS (
        SELECT
            c.state_code,
            c.person_id,
            c.assessment_date,
            c.assessment_due_date,
            c.classification_decision_date,
            compartment_level_0_super_session_id,
        FROM `{{project_id}}.analyst_data.custody_classification_assessment_dates_materialized` c
        LEFT JOIN `{{project_id}}.sessions.compartment_level_0_super_sessions_materialized` css
            ON c.person_id = css.person_id
            AND c.assessment_date BETWEEN css.start_date AND {nonnull_end_date_exclusive_clause('css.end_date_exclusive')}
    )
    /* This joins downgrades with past assessment info, for all assessments where the classification decision date was
    before the current downgrade date (and within a compartment_level_0_super_session_id). That allows us to get the 
    expected assessment due date based on the previous most recent classification decision */
    SELECT  
            cl.person_id,
            cl.state_code,
            cl.start_date AS downgrade_date,
            cl.custody_level_session_id,
            cl.compartment_level_0_super_session_id,
            custody_level, 
            previous_custody_level, 
            assessment_due_date,
            DATE_DIFF(cl.start_date, a.assessment_due_date, MONTH) AS months_between_assessment_due_and_downgrade
    FROM `{{project_id}}.sessions.custody_level_sessions_materialized` cl
    LEFT JOIN assessment_dates a
        ON cl.person_id = a.person_id
        AND cl.compartment_level_0_super_session_id = a.compartment_level_0_super_session_id
        AND a.classification_decision_date < cl.start_date
    WHERE custody_downgrade > 0
    QUALIFY ROW_NUMBER() OVER(PARTITION BY cl.person_id, cl.start_date ORDER BY a.classification_decision_date DESC) = 1

"""

NUMBER_MONTHS_BETWEEN_CUSTODY_DOWNGRADE_AND_ASSESSMENT_DUE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=NUMBER_MONTHS_BETWEEN_CUSTODY_DOWNGRADE_AND_ASSESSMENT_DUE_VIEW_NAME,
    description=NUMBER_MONTHS_BETWEEN_CUSTODY_DOWNGRADE_AND_ASSESSMENT_DUE_VIEW_DESCRIPTION,
    view_query_template=NUMBER_MONTHS_BETWEEN_CUSTODY_DOWNGRADE_AND_ASSESSMENT_DUE_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        NUMBER_MONTHS_BETWEEN_CUSTODY_DOWNGRADE_AND_ASSESSMENT_DUE_VIEW_BUILDER.build_and_print()
