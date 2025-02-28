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
"""
Defines a criteria span view that shows spans of time during which 
someone in AZ has a violent conviction but not an Aggravated Assault, Assault, 
or Robbery conviction.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_az_query_fragments import (
    _ADDL_INELIGIBLE_VIOLENT_STATUTES,
    no_current_or_prior_convictions,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_AZ_NO_VIOLENT_CONVICTION_UNLESS_ASSAULT_OR_AGGRAVATED_ASSAULT_OR_ROBBERY_CONVICTION"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which
someone in AZ has a violent conviction but not an Aggravated Assault, Assault,
or Robbery conviction.
"""

_ELIGIBLE_STATUTES = [
    # Robbery
    "13-1902-1",
    "13-1902-2",
    "13-1902-9",
    "13-1902-A",
    "13-1902-B",
    "13-1902-D",
    "13-1902-E",
    "13-1902-NONE",
    "13-641-NONE",
    "13-643-NONE",
    "13-643A-NONE",
    # Assault
    "13-1203-B",
    "13-1203-NONE",
    "13-241-NONE",
    "13-242-NONE",
    # Aggravated Assault
    "13-1204-1",
    "13-1204-2",
    "13-1204-7",
    "13-1204-9",
    "13-1204-A",
    "13-1204-B",
    "13-1204-C",
    "13-1204-D",
    "13-1204-E",
    "13-1204-F",
    "13-1204-H",
    "13-1204-I",
    "13-1204-N",
    "13-1204-NONE",
    "13-1204-P",
    "13-1204-T",
    "13-1204-U",
    "13-1204-Y",
]

_QUERY_TEMPLATE = no_current_or_prior_convictions(
    statutes_list=_ELIGIBLE_STATUTES,
    exclude_statutes=True,
    additional_where_clauses="AND (sent.is_violent OR "
    + " OR ".join(
        [f"charge.statute LIKE '%{s}%'" for s in _ADDL_INELIGIBLE_VIOLENT_STATUTES]
    )
    + ")"
    # This includes out of state convictions in a resident's history that may make them ineligible
    " OR JSON_VALUE(sentence_metadata, '$.tpr_ineligible_from_previous_conviction') = 'TRUE'",
)

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    state_code=StateCode.US_AZ,
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    sessions_dataset=SESSIONS_DATASET,
    description=__doc__,
    meets_criteria_default=True,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    reasons_fields=[
        ReasonsField(
            name="ineligible_offenses",
            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
            description="List of violent crime statutes that make the person ineligible",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
