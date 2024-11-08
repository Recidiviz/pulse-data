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
"""Describes spans of time when someone is ineligible due to a current or
past conviction for arson"""

from google.cloud import bigquery

from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_az_query_fragments import (
    no_current_or_prior_convictions,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_AZ_NO_ARSON_CONVICTION"

_DESCRIPTION = """Describes spans of time when someone is ineligible due to a current or
past conviction for arson"""

_SEC_STATUTE = [
    "13-231-NONE",  # ARSON FIRST DEGREE
    "13-1705-NONE",  # ARSON OF OCCUPD JAIL/PRSN
    "13-1704-9",  # ARSON OF OCCUPD STRUCTURE
    "13-1704-E",  # ARSON OF OCCUPD STRUCTURE
    "13-1704-1",  # ARSON OF OCCUPD STRUCTURE
    "13-1704-NONE",  # ARSON OF OCCUPD STRUCTURE
    "13-1704-A",  # ARSON OF OCCUPD STRUCTURE
    "13-1703-2",  # ARSON OF STRUCTURE/PROPRTY
    "13-1703-1",  # ARSON OF STRUCTURE/PROPRTY
    "13-1703-NONE",  # ARSON OF STRUCTURE/PROPRTY
    "13-232-NONE",  # ARSON SECOND DEGREE
    "13-233-NONE",  # ARSON THIRD DEGREE
    "13-236-NONE",  # ARSON UNOCCUPIED STRUCTURE
]

_QUERY_TEMPLATE = no_current_or_prior_convictions(
    statute=_SEC_STATUTE, reasons_field_name="ineligible_offenses_arson"
)

_REASONS_FIELDS = [
    ReasonsField(
        name="ineligible_offenses_arson",
        type=bigquery.enums.StandardSqlTypeNames.ARRAY,
        description="A list of ineligible offenses related to arson",
    )
]

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        reasons_fields=_REASONS_FIELDS,
        state_code=StateCode.US_AZ,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
