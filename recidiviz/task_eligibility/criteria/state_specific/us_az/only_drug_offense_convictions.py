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
"""Describes spans of time when someone only has drug-related offense convictions in a given span"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_AZ_ONLY_DRUG_OFFENSE_CONVICTIONS"

_DRUG_STATUTES = [
    "13-3405",  # MARIJUANA
    "13-3407",  # DANGEROUS DRUG
    "13-3408",  # NARCOTIC
    "13-3415",  # DRUG PARAPHERNALIA
]

_REASONS_FIELDS = [
    ReasonsField(
        name="ineligible_offenses",
        type=bigquery.enums.StandardSqlTypeNames.ARRAY,
        description="A list of ineligible drug offenses a resident is serving",
    )
]

_QUERY_TEMPLATE = f"""
WITH drug_spans AS (
    SELECT
        span.state_code,
        span.person_id,
        span.start_date,
        span.end_date,
        charge.description,
        charge.statute,
        {"(" + " OR ".join([f"charge.statute LIKE '%{d}%'" for d in _DRUG_STATUTES]) + ")"} AS is_drug_span,
    FROM
        `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized` span,
        UNNEST (sentences_preprocessed_id_array_actual_completion) AS sentences_preprocessed_id
    INNER JOIN
        `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sent
    USING
        (state_code,
        person_id,
        sentences_preprocessed_id)
    LEFT JOIN
        `{{project_id}}.{{normalized_state_dataset}}.state_charge_v2_state_sentence_association` assoc
        ON
        assoc.state_code = sent.state_code
        AND assoc.sentence_id = sent.sentence_id
        LEFT JOIN
        `{{project_id}}.{{sessions_dataset}}.charges_preprocessed` charge
        ON
        charge.state_code = assoc.state_code
        AND charge.charge_v2_id = assoc.charge_v2_id
    WHERE
        span.state_code = 'US_AZ'
),
{create_sub_sessions_with_attributes('drug_spans')}
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    LOGICAL_AND(is_drug_span) AS meets_criteria,
    TO_JSON(STRUCT( ARRAY_AGG(DISTINCT description ORDER BY description) AS ineligible_offenses)) AS reason,
    ARRAY_AGG(DISTINCT description ORDER BY description) AS ineligible_offenses,
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        reasons_fields=_REASONS_FIELDS,
        state_code=StateCode.US_AZ,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=False,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
