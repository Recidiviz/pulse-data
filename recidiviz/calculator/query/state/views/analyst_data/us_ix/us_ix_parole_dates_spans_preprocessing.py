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
""" This view has one record per term and person. It contains parole related information for residents in
IX. This will be used to create criteria queries for the CRC and XCRC workflows."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
    sessionize_ledger_data,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_IX_PAROLE_DATES_SPANS_PREPROCESSING_VIEW_NAME = (
    "us_ix_parole_dates_spans_preprocessing"
)

US_IX_PAROLE_DATES_SPANS_PREPROCESSING_VIEW_DESCRIPTION = """This view has one record per term and person. It contains parole related information for residents in
IX. This will be used to create criteria queries for the CRC and XCRC workflows."""

US_IX_PAROLE_DATES_SPANS_PREPROCESSING_QUERY_TEMPLATE = f"""
WITH parole_hearing_date_deadlines AS (
SELECT 
    state_code,
    person_id,
    update_datetime,
    task_subtype,
    IF(task_subtype = 'INITIAL', eligible_date, NULL) AS initial_parole_hearing_date,
    IF(task_subtype = 'SUBSEQUENT', eligible_date, NULL) AS next_parole_hearing_date
FROM `{{project_id}}.{{normalized_state_dataset}}.state_task_deadline`
WHERE 
    state_code = 'US_IX'
    AND task_type = 'PAROLE_HEARING'
    AND task_subtype IN ('INITIAL', 'SUBSEQUENT')
),
parole_hearing_date_spans AS (
 SELECT * 
 FROM ({sessionize_ledger_data(table_name = 'parole_hearing_date_deadlines', 
                               index_columns = ['state_code', 'person_id', 'task_subtype'], 
                               update_column_name = 'update_datetime', 
                               attribute_columns = ['initial_parole_hearing_date', 'next_parole_hearing_date'])})
),
{create_sub_sessions_with_attributes(table_name='parole_hearing_date_spans',
                                     index_columns=['state_code', 'person_id'], 
                                     end_date_field_name='end_date_exclusive')}
SELECT 
    state_code,
    person_id,
    start_date,
    end_date_exclusive,
    MAX(initial_parole_hearing_date) AS initial_parole_hearing_date,
    MAX(next_parole_hearing_date) AS next_parole_hearing_date
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4
"""

US_IX_PAROLE_DATES_SPANS_PREPROCESSING_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_IX_PAROLE_DATES_SPANS_PREPROCESSING_VIEW_NAME,
    description=US_IX_PAROLE_DATES_SPANS_PREPROCESSING_VIEW_DESCRIPTION,
    view_query_template=US_IX_PAROLE_DATES_SPANS_PREPROCESSING_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IX_PAROLE_DATES_SPANS_PREPROCESSING_VIEW_BUILDER.build_and_print()
