# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Builders for US_MI task eligibility criteria that compare a resident's MI_SECURITY_CLASS
assessments against their 2026 classification policy recommendations.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_intersection_spans,
)
from recidiviz.calculator.query.state.views.analyst_data.us_mi.us_mi_state_prison_security_assessment_spans import (
    US_MI_STATE_PRISON_SECURITY_ASSESSMENT_SPANS_VIEW_BUILDER,
    us_mi_state_prison_assessment_spans_query,
)
from recidiviz.calculator.query.state.views.classification.recommended_classification_spans_big_query_view_builder import (
    RECOMMENDED_CUSTODY_LEVEL_COLUMN_NAME,
    CustodyLevelVsRecommended,
    RecommendedClassificationSpansBigQueryViewBuilder,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_mi_query_fragments import (
    custody_level_to_security_level_val_clause,
)
from recidiviz.utils.string_formatting import fix_indent

# TODO(MI-7750): Once MI's classification data distinguishes which policy version an
# assessment was made under, read 2026 policy assessments from the US_MI classification
# assessment event views (state_classification_assessment_events_query) instead of
# state_assessment, and drop these placeholder values. No ingested assessment has these
# subtypes yet, so criteria built on them are currently never met.
_2026_POLICY_ASSESSMENT_SUBTYPES = [
    "INITIAL_2026_POLICY",
    "REASSESSMENT_2026_POLICY",
]


def _true_level_vs_recommended_operator(comparison: CustodyLevelVsRecommended) -> str:
    """Returns the SQL operator that is true when the true security level compares to
    the recommended custody level in the given way, on the I=1, II=2, IV=4, V=5 scale.
    """
    if comparison is CustodyLevelVsRecommended.HIGHER_THAN_RECOMMENDED:
        return ">"
    if comparison is CustodyLevelVsRecommended.LOWER_THAN_RECOMMENDED:
        return "<"
    if comparison is CustodyLevelVsRecommended.AS_RECOMMENDED:
        return "="
    raise ValueError(f"Unexpected comparison: [{comparison}]")


def latest_true_security_level_vs_recommended_criteria_view_builder(
    *,
    criteria_name: str,
    description: str,
    recommended_classification_spans_view_builder: RecommendedClassificationSpansBigQueryViewBuilder,
    comparison: CustodyLevelVsRecommended,
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """Returns the view builder for a criteria describing when the true security level
    on a resident's most recent MI_SECURITY_CLASS assessment in their current state
    prison custody stay compares to the custody level currently recommended for them in
    the given way. The recommended level is the one in effect on each day, so spans
    split whenever the recommendation changes.

    Args:
        criteria_name: Upper-case, US_MI_-prefixed name uniquely identifying this
            criteria. Must match the name of the file the builder is defined in.
        description: Human-readable description of the spans this criteria describes.
        recommended_classification_spans_view_builder: The US_MI policy whose
            recommended levels the true level is compared against.
        comparison: How the true level must compare to the recommended level for the
            criteria to be met, e.g. HIGHER_THAN_RECOMMENDED when the true level is more
            restrictive than the recommendation.
    """
    if recommended_classification_spans_view_builder.state_code is not StateCode.US_MI:
        raise ValueError(
            f"Expected a US_MI recommended classification spans view builder, found "
            f"[{recommended_classification_spans_view_builder.state_code}]"
        )
    recommended_spans_address = (
        recommended_classification_spans_view_builder.table_for_query.format_address_for_query_template()
    )
    query_template = f"""
    WITH assessment_spans AS (
        SELECT *
        FROM `{US_MI_STATE_PRISON_SECURITY_ASSESSMENT_SPANS_VIEW_BUILDER.table_for_query.format_address_for_query_template()}`
        WHERE true_level IS NOT NULL
    ),
    recommended_spans AS (
        SELECT
            state_code,
            person_id,
            start_date,
            end_date_exclusive,
            {RECOMMENDED_CUSTODY_LEVEL_COLUMN_NAME},
            {custody_level_to_security_level_val_clause(RECOMMENDED_CUSTODY_LEVEL_COLUMN_NAME)} AS recommended_custody_level_val,
        FROM `{recommended_spans_address}`
    ),
    intersection_spans AS (
        {create_intersection_spans(
            table_1_name="assessment_spans",
            table_2_name="recommended_spans",
            index_columns=["state_code", "person_id"],
            table_1_columns=["true_level", "true_level_val"],
            table_2_columns=[RECOMMENDED_CUSTODY_LEVEL_COLUMN_NAME, "recommended_custody_level_val"],
        )}
    )
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive AS end_date,
        IFNULL(
            true_level_val {_true_level_vs_recommended_operator(comparison)} recommended_custody_level_val,
            FALSE
        ) AS meets_criteria,
        TO_JSON(STRUCT(
            true_level AS latest_true_security_level,
            {RECOMMENDED_CUSTODY_LEVEL_COLUMN_NAME}
        )) AS reason,
        true_level AS latest_true_security_level,
        {RECOMMENDED_CUSTODY_LEVEL_COLUMN_NAME},
    FROM intersection_spans
    """
    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_MI,
        criteria_name=criteria_name,
        description=description,
        criteria_spans_query_template=query_template,
        meets_criteria_default=False,
        reasons_fields=[
            ReasonsField(
                name="latest_true_security_level",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="True security level on the resident's most recent MI_SECURITY_CLASS assessment in their current state prison custody stay",
            ),
            ReasonsField(
                name=RECOMMENDED_CUSTODY_LEVEL_COLUMN_NAME,
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Custody level currently recommended for the resident by the 2026 classification policy",
            ),
        ],
    )


def has_2026_policy_assessment_in_state_prison_custody_criteria_view_builder(
    *,
    criteria_name: str,
    description: str,
    lookback_months: int | None,
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """Returns the view builder for a criteria describing when a resident has had an
    MI_SECURITY_CLASS assessment under the 2026 classification policy (initial or
    review) during their current state prison custody stay, within the past
    lookback_months if given. Each 2026 policy assessment starts a span that meets the
    criteria and ends at the next 2026 policy assessment in the stay, lookback_months
    after the assessment, or the end of the stay, whichever comes first.

    For example, with lookback_months=6, a resident in a stay starting 2026-11-01 with
    2026 policy assessments on 2026-12-05 and 2027-03-10 gets these spans (some columns
    abridged):

        start_date,end_date,meets_criteria,latest_2026_policy_assessment_date
        2026-12-05,2027-03-10,TRUE,2026-12-05
        2027-03-10,2027-09-10,TRUE,2027-03-10

    The criteria is not met outside those spans. With lookback_months=None, the second
    span instead runs to the end of the stay.

    Args:
        criteria_name: Upper-case, US_MI_-prefixed name uniquely identifying this
            criteria. Must match the name of the file the builder is defined in.
        description: Human-readable description of the spans this criteria describes.
        lookback_months: How many months a 2026 policy assessment keeps the criteria
            met, or None to keep it met for the rest of the stay.
    """
    subtypes_clause = ", ".join(f"'{s}'" for s in _2026_POLICY_ASSESSMENT_SUBTYPES)
    policy_2026_assessments_query = f"""
SELECT DISTINCT
    state_code,
    person_id,
    assessment_date,
FROM `{{project_id}}.{{normalized_state_dataset}}.state_assessment`
WHERE state_code = 'US_MI'
    AND assessment_type = 'MI_SECURITY_CLASS'
    AND assessment_subtype IN ({subtypes_clause})
    AND assessment_date IS NOT NULL
"""
    query_template = f"""
    WITH assessment_spans AS (
{fix_indent(
    us_mi_state_prison_assessment_spans_query(
        assessments_query=policy_2026_assessments_query,
        assessment_columns=[],
        lookback_months=lookback_months,
    ),
    indent_level=8,
)}
    )
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive AS end_date,
        TRUE AS meets_criteria,
        TO_JSON(STRUCT(
            assessment_date AS latest_2026_policy_assessment_date
        )) AS reason,
        assessment_date AS latest_2026_policy_assessment_date,
    FROM assessment_spans
    """
    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_MI,
        criteria_name=criteria_name,
        description=description,
        criteria_spans_query_template=query_template,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        meets_criteria_default=False,
        reasons_fields=[
            ReasonsField(
                name="latest_2026_policy_assessment_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date of the resident's most recent 2026 classification policy assessment in their current state prison custody stay",
            ),
        ],
    )
