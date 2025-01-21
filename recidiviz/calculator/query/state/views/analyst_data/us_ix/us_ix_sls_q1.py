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
"""Computes Q1 (current sentence severity) of Idaho's Reclassification of Security Level form at any point in time"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SENTENCE_SESSIONS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.utils.general_criteria_builders import (
    join_sentence_serving_periods_to_compartment_sessions,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_IX_SLS_Q1_VIEW_NAME = "us_ix_sls_q1"

US_IX_SLS_Q1_VIEW_DESCRIPTION = """Computes Q1 (current sentence severity) of Idaho's Reclassification of Security Level
form at any point in time. See details of the Reclassification Form here:
https://drive.google.com/file/d/1-Y3-RAqPEUrAKoeSdkNTDmB-V1YAEp1l/view
    """

US_IX_SLS_Q1_QUERY_TEMPLATE = f"""
    WITH charge_spans AS (
       SELECT
        span.state_code,
        span.person_id,
        span.start_date,
        span.end_date_exclusive AS end_date,
        MAX(SeverityIdForInitial) AS max_severity
    {join_sentence_serving_periods_to_compartment_sessions(compartment_level_1_to_overlap='INCARCERATION')}
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_charge` sc
        ON sc.person_id = sent.person_id 
        AND sc.charge_id = sent.charge_id
    INNER JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.scl_OffenseType_latest` scl
        ON sc.offense_type = scl.OffenseTypeId
    WHERE span.state_code = 'US_IX'
    GROUP BY 1,2,3,4
    )
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        IF(max_severity = '5', 9, 1) AS q1_score
    FROM ({aggregate_adjacent_spans(table_name = 'charge_spans', 
                                    attribute='max_severity', 
                                    end_date_field_name= 'end_date')})
"""

US_IX_SLS_Q1_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_IX_SLS_Q1_VIEW_NAME,
    description=US_IX_SLS_Q1_VIEW_DESCRIPTION,
    view_query_template=US_IX_SLS_Q1_QUERY_TEMPLATE,
    sessions_dataset=SESSIONS_DATASET,
    sentence_sessions_dataset=SENTENCE_SESSIONS_DATASET,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    us_ix_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_IX, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IX_SLS_Q1_VIEW_BUILDER.build_and_print()
