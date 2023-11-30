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
"""Creates view to identify eligibility spans for earned discharge at the person-sentence level"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_OR_EARNED_DISCHARGE_SENTENCE_ELIGIBILITY_SPANS_VIEW_NAME = (
    "us_or_earned_discharge_sentence_eligibility_spans"
)

US_OR_EARNED_DISCHARGE_SENTENCE_ELIGIBILITY_SPANS_VIEW_DESCRIPTION = """Creates view to identify eligibility spans for earned discharge at the person-sentence level"""

US_OR_EARNED_DISCHARGE_SENTENCE_ELIGIBILITY_SPANS_QUERY_TEMPLATE = """
    SELECT person_id,
        sentence_id,
        start_date,
        end_date,
        NULL AS is_eligible,
        meets_criteria AS meets_criteria_sentence_date,
        NULL AS meets_criteria_served_6_months,
        NULL AS meets_criteria_served_half_of_sentence,
        NULL AS meets_criteria_statute,
        NULL AS meets_criteria_no_convictions_since_sentence_start_date,
    FROM `{project_id}.{analyst_dataset}.us_or_sentenced_after_august_2013`
    UNION ALL
    SELECT person_id,
        sentence_id,
        start_date,
        end_date,
        NULL AS is_eligible,
        NULL AS meets_criteria_sentence_date,
        meets_criteria AS meets_criteria_served_6_months,
        NULL AS meets_criteria_served_half_of_sentence,
        NULL AS meets_criteria_statute,
        NULL AS meets_criteria_no_convictions_since_sentence_start_date,
    FROM `{project_id}.{analyst_dataset}.us_or_served_6_months_supervision`
    UNION ALL
    SELECT person_id,
        sentence_id,
        start_date,
        end_date,
        NULL AS is_eligible,
        NULL AS meets_criteria_sentence_date,
        NULL AS meets_criteria_served_6_months,
        meets_criteria AS meets_criteria_served_half_of_sentence,
        NULL AS meets_criteria_statute,
        NULL AS meets_criteria_no_convictions_since_sentence_start_date,
    FROM `{project_id}.{analyst_dataset}.us_or_served_half_sentence`
    UNION ALL
    SELECT person_id,
        sentence_id,
        start_date,
        end_date,
        NULL AS is_eligible,
        NULL AS meets_criteria_sentence_date,
        NULL AS meets_criteria_served_6_months,
        NULL AS meets_criteria_served_half_of_sentence,
        meets_criteria AS meets_criteria_statute,
        NULL AS meets_criteria_no_convictions_since_sentence_start_date,
    FROM `{project_id}.{analyst_dataset}.us_or_statute_eligible`
    UNION ALL
    SELECT person_id,
        sentence_id,
        start_date,
        end_date,
        NULL AS is_eligible,
        NULL AS meets_criteria_sentence_date,
        NULL AS meets_criteria_served_6_months,
        NULL AS meets_criteria_served_half_of_sentence,
        NULL AS meets_criteria_statute,
        meets_criteria AS meets_criteria_no_convictions_since_sentence_start_date,
    FROM `{project_id}.{analyst_dataset}.us_or_no_convictions_since_sentence_start`
"""

US_OR_EARNED_DISCHARGE_SENTENCE_ELIGIBILITY_SPANS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_OR_EARNED_DISCHARGE_SENTENCE_ELIGIBILITY_SPANS_VIEW_NAME,
    description=US_OR_EARNED_DISCHARGE_SENTENCE_ELIGIBILITY_SPANS_VIEW_DESCRIPTION,
    view_query_template=US_OR_EARNED_DISCHARGE_SENTENCE_ELIGIBILITY_SPANS_QUERY_TEMPLATE,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_OR_EARNED_DISCHARGE_SENTENCE_ELIGIBILITY_SPANS_VIEW_BUILDER.build_and_print()
