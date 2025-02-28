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
Creates a table where each row is an inferred sentence group at a point in time
with all known projected dates at that time.

Related views are:
- inferred_group_aggregated_sentence_projected_dates
- inferred_group_aggregated_sentence_group_projected_dates
- inferred_group_aggregated_projected_dates_validation

Output fields for this view are:

    - sentence_inferred_group_id:
        The ID for the inferred sentence group. This can be used to link back to the
        constituent sentences and state provided sentence groups.

    - inferred_group_update_datetime:
        This is the datetime where the values in this row begin to be valid.

    - parole_eligibility_date:
        The maximum parole eligibility date across sentences affiliated with this inferred group.

    - projected_parole_release_date:
        The maximum projected parole release date across sentences affiliated with this inferred group.

    - projected_full_term_release_date_min:
        The maximum full term release date (min) across sentences affiliated with this inferred group.

    - projected_full_term_release_date_max:
        The maximum full term release date (max) across the sentences affiliated with this inferred group.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.sessions_query_fragments import sessionize_ledger_data
from recidiviz.calculator.query.state.dataset_config import SENTENCE_SESSIONS_DATASET
from recidiviz.calculator.query.state.views.sentence_sessions.inferred_group_aggregated_sentence_group_projected_dates import (
    INFERRED_GROUP_AGGREGATED_SENTENCE_GROUP_PROJECTED_DATES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.inferred_group_aggregated_sentence_projected_dates import (
    INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.state_sentence_configurations import (
    STATES_NOT_MIGRATED_TO_SENTENCE_V2_SCHEMA,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_INDEX_COLUMNS = [
    "state_code",
    "person_id",
    "sentence_inferred_group_id",
]
_UPDATE_COLUMN_NAME = "inferred_group_update_datetime"
_ATTRIBUTE_COLUMNS = [
    "parole_eligibility_date",
    "projected_parole_release_date",
    "projected_full_term_release_date_min",
    "projected_full_term_release_date_max",
]
COLUMNS = ",".join(_INDEX_COLUMNS + [_UPDATE_COLUMN_NAME] + _ATTRIBUTE_COLUMNS)

SENTENCE_INFERRED_GROUP_PROJECTED_DATE_SESSIONS = (
    "sentence_inferred_group_projected_date_sessions"
)


QUERY_TEMPLATE = f"""
WITH
-- We have constructed views with aggregated sentence group projected dates
-- and aggregated sentence projected dates. They have the same schema and can
-- simply be stacked.
all_aggregated_projected_dates AS (
    SELECT 
        {COLUMNS} 
    FROM 
        `{{project_id}}.{INFERRED_GROUP_AGGREGATED_SENTENCE_GROUP_PROJECTED_DATES_VIEW_BUILDER.table_for_query.to_str()}`
    UNION ALL
    SELECT 
        {COLUMNS} 
    FROM 
        `{{project_id}}.{INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_VIEW_BUILDER.table_for_query.to_str()}`
)
,
ledger_cte AS
(
SELECT
    state_code,
    person_id,
    sentence_inferred_group_id, 
    inferred_group_update_datetime,
    MAX(parole_eligibility_date) AS parole_eligibility_date,
    MAX(projected_parole_release_date) AS projected_parole_release_date,
    MAX(projected_full_term_release_date_min) AS projected_full_term_release_date_min,
    MAX(projected_full_term_release_date_max) AS projected_full_term_release_date_max
FROM
    all_aggregated_projected_dates
GROUP BY state_code, person_id, sentence_inferred_group_id, inferred_group_update_datetime
)
,
sessionized_cte AS
(
{sessionize_ledger_data(
    table_name='ledger_cte',
    index_columns=_INDEX_COLUMNS,
    update_column_name=_UPDATE_COLUMN_NAME,
    attribute_columns=_ATTRIBUTE_COLUMNS,
)})
SELECT
    *
FROM sessionized_cte
WHERE state_code NOT IN ({{v2_non_migrated_states}})
"""


SENTENCE_INFERRED_GROUP_PROJECTED_DATE_SESSIONS_VIEW_BUILDER = (
    SimpleBigQueryViewBuilder(
        dataset_id=SENTENCE_SESSIONS_DATASET,
        view_id=SENTENCE_INFERRED_GROUP_PROJECTED_DATE_SESSIONS,
        view_query_template=QUERY_TEMPLATE,
        description=__doc__,
        should_materialize=True,
        v2_non_migrated_states=list_to_query_string(
            string_list=STATES_NOT_MIGRATED_TO_SENTENCE_V2_SCHEMA,
            quoted=True,
        ),
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCE_INFERRED_GROUP_PROJECTED_DATE_SESSIONS_VIEW_BUILDER.build_and_print()
