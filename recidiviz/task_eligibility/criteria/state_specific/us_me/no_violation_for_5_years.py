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

"""Defines a criteria view that shows spans of time for
which residents are within 5 years of having received a violation.
"""
from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_me_query_fragments import (
    no_violations_for_x_time,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ME_NO_VIOLATION_FOR_5_YEARS"

_DESCRIPTION = """Defines a criteria view that shows spans of time for
which residents are within 5 years of having received a violation.
"""

_QUERY_TEMPLATE = no_violations_for_x_time(x=5, date_part="YEAR")

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    state_code=StateCode.US_ME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    us_me_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ME, instance=DirectIngestInstance.PRIMARY
    ),
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    meets_criteria_default=True,
    reasons_fields=[
        ReasonsField(
            name="highest_class_viol",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Violation class of the highest class violation within the 5 year window.",
        ),
        ReasonsField(
            name="viol_type",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Type of violation within the 90 day window.",
        ),
        ReasonsField(
            name="eligible_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date when the resident is past 5 years of having received the violation.",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
