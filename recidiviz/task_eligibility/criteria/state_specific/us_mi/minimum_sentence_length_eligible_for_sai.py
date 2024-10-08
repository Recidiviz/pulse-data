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
# ============================================================================
"""Describes the spans of time when a resident is serving a sentence
    with a minimum length that is eligible for SAI"""
from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.placeholder_criteria_builders import (
    state_specific_placeholder_criteria_view_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_MI_MINIMUM_SENTENCE_LENGTH_ELIGIBLE_FOR_SAI"

_DESCRIPTION = """Describes the spans of time when a resident is serving a sentence
    with a minimum length that is eligible for SAI"""

_REASONS_FIELDS = [
    ReasonsField(
        name="is_indeterminate_sentence",
        type=bigquery.enums.StandardSqlTypeNames.BOOL,
        description="Whether someone is serving an indeterminate sentence",
    ),
    ReasonsField(
        name="min_sentence",
        type=bigquery.enums.StandardSqlTypeNames.INT64,
        description="The minimum length (days) of the client's current sentence",
    ),
    ReasonsField(
        name="is_breaking_and_entering",
        type=bigquery.enums.StandardSqlTypeNames.BOOL,
        description="Whether a client is serving a sentence for Breaking and Entering a Dwelling",
    ),
    ReasonsField(
        name="is_firearm_felony",
        type=bigquery.enums.StandardSqlTypeNames.BOOL,
        description="Whether a client is serving a sentence under the felony firearm law (MCL 750.227b)",
    ),
]

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    state_specific_placeholder_criteria_view_builder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        reasons_fields=_REASONS_FIELDS,
        state_code=StateCode.US_MI,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
