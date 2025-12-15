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
# ============================================================================
"""Describes the spans of time someone has received a classification since
they entered state prison custody"""

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "HAS_INITIAL_CLASSIFICATION_IN_STATE_PRISON_CUSTODY"

_QUERY_TEMPLATE = f"""
    SELECT
        custody.state_code,
        custody.person_id,
        -- Set the start date of the span to be when the classification decision was made
        classification_dates.classification_decision_date AS start_date,
        -- Span ends when the custodial authority span ends
        custody.end_date_exclusive AS end_date,
        TRUE AS meets_criteria,
        TO_JSON(STRUCT(
                       classification_dates.assessment_date AS initial_assessment_date,
                       classification_dates.classification_date AS initial_classification_date,
                       classification_dates.classification_decision_date AS initial_classification_decision_date,
                       custody.start_date AS most_recent_prison_custody_start_date
                        )
                ) AS reason,
        classification_dates.assessment_date AS initial_assessment_date,
        classification_dates.classification_date AS initial_classification_date,
        classification_dates.classification_decision_date AS initial_classification_decision_date,
        custody.start_date AS most_recent_prison_custody_start_date
    FROM `{{project_id}}.sessions.custodial_authority_sessions_materialized` custody
    INNER JOIN `{{project_id}}.analyst_data.custody_classification_assessment_dates_materialized` classification_dates
        ON classification_decision_date BETWEEN start_date AND {nonnull_end_date_exclusive_clause('custody.end_date_exclusive')}
        AND classification_dates.person_id = custody.person_id
    WHERE custodial_authority = 'STATE_PRISON'
    -- People will have multiple classifications during a custodial authority span. We want to keep the earliest instance
    -- of this because that's when the criteria for having an initial classification is met
    QUALIFY ROW_NUMBER() OVER(PARTITION BY custody.state_code, custody.person_id, custody.start_date 
                              ORDER BY classification_dates.classification_decision_date ASC) = 1
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=__doc__,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    meets_criteria_default=False,
    reasons_fields=[
        ReasonsField(
            name="initial_assessment_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="The date of the initial assessment in a given span under state prison custody",
        ),
        ReasonsField(
            name="initial_classification_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="The date of the initial classification in a given span under state prison custody. This"
            "can sometimes be the same as assessment date, sometimes vary",
        ),
        ReasonsField(
            name="initial_classification_decision_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="The decision date of the initial classification in a given span under state prison custody",
        ),
        ReasonsField(
            name="most_recent_prison_custody_start_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Start date of the current span under state prison custody",
        ),
    ],
)
if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
