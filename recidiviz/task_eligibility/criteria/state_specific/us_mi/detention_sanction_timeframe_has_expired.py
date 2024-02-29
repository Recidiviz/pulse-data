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
"""This criteria view builder defines spans of time where the detention sanction timeframe
for a resident has expired. Policy dictates that detention should not exceed 10 days for
each violation or 20 days for all violations arising from a specific incident.
"""
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_MI_DETENTION_SANCTION_TIMEFRAME_HAS_EXPIRED"

_DESCRIPTION = """This criteria view builder defines spans of time where the detention sanction timeframe
for a resident has expired. Policy dictates that detention should not exceed 10 days for 
each violation or 20 days for all violations arising from a specific incident. 
"""

_QUERY_TEMPLATE = f"""
WITH detention_sanction_spans AS (
/* This cte queries incarceration incidents to identify the detention sanction timeframe (should be
either 10 or 20 according to policy) */
    SELECT
        state_code,
        person_id,
        date_effective AS start_date, 
        --there are only 6 examples of null end dates, but where these exist, project an end date by adding
        --punishment_length_days to the date_effective
        COALESCE(projected_end_date, DATE_ADD(date_effective, INTERVAL punishment_length_days DAY)) AS end_date,
        --according to policy, the detention sanction can be at most 20 days 
        LEAST(punishment_length_days, 20) AS punishment_length_days,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_incident_outcome`
    WHERE outcome_type = 'RESTRICTED_CONFINEMENT'
        AND punishment_length_days IS NOT NULL
        AND date_effective IS NOT NULL
        AND state_code = 'US_MI'
),
detention_designation_spans AS (
/* This cte queries the actual spans of time a resident has a 'DETENTION' designation */
    SELECT 
        state_code,
        person_id,
        offender_id,
        DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', start_date)) AS start_date,
        DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', end_date)) AS end_date,
    FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.ADH_OFFENDER_DESIGNATION_latest` d
    INNER JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.ADH_REFERENCE_CODE_latest` r
        on d.offender_designation_code_id = r.reference_code_id
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON pei.external_id = d.offender_id
        AND pei.id_type = 'US_MI_DOC_ID'
    WHERE r.description = 'Detention'
    AND DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', start_date)) < {nonnull_end_date_clause("DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', end_date))")}
),
critical_date_spans AS (
    SELECT
        dd.state_code,
        dd.person_id,
        dd.start_date AS start_datetime,
        dd.end_date AS end_datetime,
        --if we don't see a detention sanction, set the critical date to 10 days later 
        DATE_ADD(dd.start_date, INTERVAL IFNULL(d.punishment_length_days, 10) DAY) AS critical_date
    FROM ({aggregate_adjacent_spans(table_name='detention_designation_spans')}) dd
     LEFT JOIN detention_sanction_spans d
        ON dd.person_id = d.person_id 
        AND dd.state_code = d.state_code
        --revisit this logic, and dedup? 
        AND dd.start_date < {nonnull_end_date_clause('d.end_date')}
        AND d.start_date < {nonnull_end_date_clause('dd.end_date')}
),
{critical_date_has_passed_spans_cte()},
distinct_critical_date_has_passed_spans AS(
    SELECT DISTINCT
        cd.state_code,
        cd.person_id,
        cd.start_date,
        cd.end_date,
        cd.critical_date_has_passed,
        cd.critical_date,
    FROM critical_date_has_passed_spans cd
)
SELECT
    cd.state_code,
    cd.person_id,
    cd.start_date,
    cd.end_date,
    cd.critical_date_has_passed AS meets_criteria,
    TO_JSON(STRUCT(
        cd.critical_date AS eligible_date,
        cd.critical_date_has_passed AS detention_sanction_has_expired
    )) AS reason,
FROM distinct_critical_date_has_passed_spans cd

"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        state_code=StateCode.US_MI,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_MI,
            instance=DirectIngestInstance.PRIMARY,
        ),
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
