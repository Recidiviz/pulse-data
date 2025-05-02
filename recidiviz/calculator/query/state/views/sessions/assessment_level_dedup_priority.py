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
"""Establish deduplication priority for assessment levels."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.state.state_assessment import StateAssessmentLevel
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ASSESSMENT_LEVEL_DEDUP_PRIORITY_VIEW_NAME = "assessment_level_dedup_priority"

# TODO(#39399): Confirm whether this ordering is correct, especially re: the distinction
# between 'MODERATE' and 'MEDIUM' (and related levels).
ASSESSMENT_LEVEL_ORDERED_PRIORITY = [
    StateAssessmentLevel.INTENSE,
    StateAssessmentLevel.MAXIMUM,
    StateAssessmentLevel.VERY_HIGH,
    StateAssessmentLevel.HIGH,
    StateAssessmentLevel.MEDIUM_HIGH,
    StateAssessmentLevel.MEDIUM,
    StateAssessmentLevel.MODERATE,
    StateAssessmentLevel.LOW_MEDIUM,
    StateAssessmentLevel.LOW_MODERATE,
    StateAssessmentLevel.LOW,
    StateAssessmentLevel.MINIMUM,
    StateAssessmentLevel.INTERNAL_UNKNOWN,
    StateAssessmentLevel.EXTERNAL_UNKNOWN,
]

ASSESSMENT_LEVEL_DEDUP_PRIORITY_QUERY_TEMPLATE = """
    SELECT 
        assessment_level,
        assessment_level_priority,
    FROM UNNEST([{prioritized_assessment_levels}]) AS assessment_level
    WITH OFFSET AS assessment_level_priority
"""

ASSESSMENT_LEVEL_DEDUP_PRIORITY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=ASSESSMENT_LEVEL_DEDUP_PRIORITY_VIEW_NAME,
    view_query_template=ASSESSMENT_LEVEL_DEDUP_PRIORITY_QUERY_TEMPLATE,
    description=__doc__,
    should_materialize=False,
    prioritized_assessment_levels=(
        "\n,".join([f"'{level.value}'" for level in ASSESSMENT_LEVEL_ORDERED_PRIORITY])
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ASSESSMENT_LEVEL_DEDUP_PRIORITY_VIEW_BUILDER.build_and_print()
