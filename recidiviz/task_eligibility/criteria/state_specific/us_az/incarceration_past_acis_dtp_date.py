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
"""Defines a criteria span view that shows spans of time during which someone has passed 
their ACIS (Time Comp assigned) Drug Transition Program date.
"""
from google.cloud import bigquery

from recidiviz.common.constants.state.state_task_deadline import StateTaskType
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
    critical_date_spans_cte,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    task_deadline_critical_date_update_datetimes_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_AZ_INCARCERATION_PAST_ACIS_DTP_DATE"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which someone has passed
their ACIS (Time Comp assigned) Drug Transition Program date."""

_QUERY_TEMPLATE = f"""
WITH
{task_deadline_critical_date_update_datetimes_cte(
    task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
    critical_date_column='due_date',
    additional_where_clause="AND task_subtype = 'DRUG TRANSITION RELEASE' AND state_code = 'US_AZ'")
},
{critical_date_spans_cte()},
{critical_date_has_passed_spans_cte()}
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    critical_date_has_passed AS meets_criteria,
    TO_JSON(STRUCT(critical_date AS acis_dtp_date)) AS reason,
    critical_date AS acis_dtp_date,
FROM critical_date_has_passed_spans
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_AZ,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        meets_criteria_default=False,
        reasons_fields=[
            ReasonsField(
                name="acis_dtp_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="ACIS Drug Transition Release date assigned by Time Comp",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
