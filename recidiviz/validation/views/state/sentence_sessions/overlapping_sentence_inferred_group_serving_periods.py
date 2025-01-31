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
"""Highlight spans of time where a person is serving sentences from more than one sentence inferred group at the same
time"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SENTENCE_SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

OVERLAPPING_SENTENCE_INFERRED_GROUP_SERVING_PERIODS_VIEW_NAME = (
    "overlapping_sentence_inferred_group_serving_periods"
)

OVERLAPPING_SENTENCE_INFERRED_GROUP_SERVING_PERIODS_DESCRIPTION = """
An inferred group is meant to represent the total collection of the sentences that a person is serving (concurrently or 
consecutively) throughout a system session. Therefore, if inferred groups overlap during any period it indicates an
underlying issue with how those groups are created since overlapping sentences should be assigned to the same inferred 
group. If this validation fails it is likely an issue with sentence status snapshot or inferred group normalization.
"""

OVERLAPPING_SENTENCE_INFERRED_GROUP_SERVING_PERIODS_QUERY_TEMPLATE = f"""
WITH serving_periods AS
(
SELECT DISTINCT
    state_code,
    state_code AS region_code,
    person_id,
    sentence_inferred_group_id,
    sentence_id,
    start_date,
    end_date_exclusive,
FROM `{{project_id}}.{{sentence_sessions_dataset}}.sentence_serving_period_materialized`
JOIN `{{project_id}}.{{sentence_sessions_dataset}}.sentences_and_charges_materialized`
    USING(state_code, person_id, sentence_id)
)
,
{create_sub_sessions_with_attributes(
    table_name='serving_periods',
    end_date_field_name='end_date_exclusive')
}
SELECT
    state_code,
    region_code,
    person_id,
    start_date,
    end_date_exclusive,
    STRING_AGG(CAST(sentence_inferred_group_id AS STRING) ORDER BY sentence_inferred_group_id) AS sentence_inferred_group_id_array,
    STRING_AGG(DISTINCT(CAST(sentence_id AS STRING)) ORDER BY CAST(sentence_id AS STRING)) AS sentence_id_array
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4,5
HAVING COUNT(DISTINCT(sentence_inferred_group_id))>1
"""


OVERLAPPING_SENTENCE_INFERRED_GROUP_SERVING_PERIODS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=OVERLAPPING_SENTENCE_INFERRED_GROUP_SERVING_PERIODS_VIEW_NAME,
    view_query_template=OVERLAPPING_SENTENCE_INFERRED_GROUP_SERVING_PERIODS_QUERY_TEMPLATE,
    description=OVERLAPPING_SENTENCE_INFERRED_GROUP_SERVING_PERIODS_DESCRIPTION,
    sentence_sessions_dataset=SENTENCE_SESSIONS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        OVERLAPPING_SENTENCE_INFERRED_GROUP_SERVING_PERIODS_VIEW_BUILDER.build_and_print()
