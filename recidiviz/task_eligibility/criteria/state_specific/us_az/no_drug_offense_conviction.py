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
"""Describes spans of time when someone is ineligible due to a current or past conviction for a drug-related offense"""
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

_CRITERIA_NAME = "US_AZ_NO_DRUG_OFFENSE_CONVICTION"

_DESCRIPTION = """Describes spans of time when someone is ineligible due to a current or
past conviction for a drug-related offense"""


_DRUG_STATUTES = [
    "13-3405",  # MARIJUANA
    "13-3407",  # DANGEROUS DRUG
    "13-3408",  # NARCOTIC
    "13-3415",  # DRUG PARAPHERNALIA
]

_QUERY_TEMPLATE = no_current_or_prior_convictions(statute=_DRUG_STATUTES)

_REASONS_FIELDS = [
    ReasonsField(
        name="ineligible_offenses",
        type=bigquery.enums.StandardSqlTypeNames.ARRAY,
        description="A list of ineligible drug offenses a resident is serving",
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
