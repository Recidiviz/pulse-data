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
"""Defines a view of criteria spans, showing periods during which an individual has 
passed their ACIS (Time Comp assigned) Transition Program Release date but 
remains within 100 days of that date.
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

_CRITERIA_NAME = "US_AZ_INCARCERATION_PAST_ACIS_TPR_DATE"

_DESCRIPTION = __doc__

_ADDITIONAL_WHERE_CLAUSE = """
            AND task_subtype = 'STANDARD TRANSITION RELEASE' 
            AND state_code = 'US_AZ' 
            AND eligible_date IS NOT NULL 
            AND eligible_date > '1900-01-01'"""

_QUERY_TEMPLATE = f"""
WITH
{task_deadline_critical_date_update_datetimes_cte(
    task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
    critical_date_column='eligible_date',
    additional_where_clause=_ADDITIONAL_WHERE_CLAUSE)
},
{critical_date_spans_cte()},
crical_date_spans_within_100_days_of_date AS (
    SELECT 
        state_code,
        person_id,
        critical_date,
        start_datetime,
        -- If the end_datetime is within 100 days of the critical_date, use the end_datetime
        -- Otherwise, use the critical_date + 100 days. That way people only stay eligible
        -- for 100 days after their relevant date. After that, they've likely loss their 
        -- eligibility for a transition release.
        IF(
            DATE_ADD(critical_date, INTERVAL 100 DAY) BETWEEN start_datetime AND IFNULL(end_datetime, '9999-12-31'),
            LEAST(IFNULL(end_datetime, '9999-12-31'), 
                  DATE_ADD(critical_date, INTERVAL 100 DAY)),
            end_datetime
        ) AS end_datetime,
    FROM critical_date_spans
    -- Drop row if critical_date or critical_date + 100 is not between start and end_date
    WHERE (critical_date BETWEEN start_datetime AND IFNULL(end_datetime, '9999-12-31') 
        OR DATE_ADD(critical_date, INTERVAL 100 DAY) BETWEEN start_datetime AND IFNULL(end_datetime, '9999-12-31'))
        -- The critical_date + 100 cannot be the start_datetime, this would create zero day spans
        AND DATE_ADD(critical_date, INTERVAL 100 DAY) != start_datetime
),
{critical_date_has_passed_spans_cte(table_name = 'crical_date_spans_within_100_days_of_date')}
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    critical_date_has_passed AS meets_criteria,
    TO_JSON(STRUCT(critical_date AS acis_tpr_date)) AS reason,
    critical_date AS acis_tpr_date,
FROM critical_date_has_passed_spans
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    state_code=StateCode.US_AZ,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    meets_criteria_default=False,
    reasons_fields=[
        ReasonsField(
            name="acis_tpr_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="ACIS Transition Program Release date assigned by Time Comp",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
