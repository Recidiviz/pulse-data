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
# =============================================================================
"""Helper SQL fragments that do standard queries against pre-processed views.
"""

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import CLASSIFICATION_VIEWS_DATASET
from recidiviz.task_eligibility.utils.general_criteria_builders import (
    num_events_within_time_interval_spans,
)
from recidiviz.utils.types import assert_type

# TODO(#20231): Ingest drug screens data into state_drug_screen
# TODO(#38834): Clean up query fragments vs. criteria builders


def has_at_least_x_negative_tests_in_time_interval(
    number_of_negative_tests: int = 1,
    date_interval: int = 12,
    date_part: str = "MONTH",
) -> str:
    """
    Args:
        number_of_negative_tests: Number of negative tests needed within time interval
        date_interval (int): Number of <date_part> when the negative drug screen
            will be counted as valid. Defaults to 12 (e.g. it could be 12 months).
        date_part (str): Supports any of the BigQuery date_part values:
            "DAY", "WEEK","MONTH","QUARTER","YEAR". Defaults to "MONTH".
    Returns:
        f-string: Spans of time where the criteria is met
    """

    return f"""
    WITH screens AS (
        SELECT
            state_code,
            person_id,
            drug_screen_date AS start_date,
            DATE_ADD(drug_screen_date, INTERVAL {date_interval} {date_part}) AS end_date,
            drug_screen_date,
            result_raw_text_primary
        FROM
            `{{project_id}}.{{sessions_dataset}}.drug_screens_preprocessed_materialized`
        WHERE
            NOT is_positive_result
    ),
    {create_sub_sessions_with_attributes('screens')},
    grouped AS (
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            COUNT(*) AS num_screens_within_time_interval,
            TO_JSON(STRUCT(
                ARRAY_AGG(
                    STRUCT(
                        drug_screen_date AS negative_screen_date,
                        result_raw_text_primary AS negative_screen_result
                    ) ORDER BY drug_screen_date
                ) AS negative_drug_screen_history_array
            )) AS reason,
            ARRAY_AGG(
                STRUCT(
                    drug_screen_date AS negative_screen_date,
                    result_raw_text_primary AS negative_screen_result
                ) ORDER BY drug_screen_date
            ) AS negative_drug_screen_history_array,
        FROM
            sub_sessions_with_attributes
        GROUP BY
            1,2,3,4
    )
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        CASE WHEN num_screens_within_time_interval >= {number_of_negative_tests} THEN TRUE ELSE FALSE END AS meets_criteria,
        reason,
        negative_drug_screen_history_array,
    FROM
        grouped
    """


def client_specific_fines_fees_balance(
    unpaid_balance_field: str,
) -> str:
    """
    Args:
        unpaid_balance_field (str, optional): Specifies which field should be used to track unpaid balance.

    Returns:
        f-string: Spans of time deduplicated to a given client and fee type showing their balance
    """

    return f"""
    WITH fines_fees AS (
        SELECT
            state_code,
            person_id,
            payment_account_external_id,
            fee_type,
            transaction_type,
            start_date,
            end_date,
            {unpaid_balance_field} AS current_balance,
        FROM
            `{{project_id}}.{{analyst_dataset}}.fines_fees_sessions_materialized`

    ),
    {create_sub_sessions_with_attributes('fines_fees')}
        SELECT 
            state_code,
            person_id,
            start_date,
            end_date,
            fee_type, 
            SUM(current_balance) AS current_balance,
        FROM sub_sessions_with_attributes
        WHERE start_date != {nonnull_end_date_clause('end_date')}
        GROUP BY 1,2,3,4,5
    """


def has_unpaid_fines_fees_balance(
    fee_type: str,
    unpaid_balance_criteria: str,
    unpaid_balance_field: str,
) -> str:
    """
    Args:
        fee_type (str, optional): Specifies the fee-type (e.g. Restitution, Supervision Fees) since there might be multiple within
         a state.
        unpaid_balance_criteria (str, optional): Specifies the criteria on unpaid balance.
        unpaid_balance_field (str, optional): Specifies which field should be used to track unpaid balance.

    Returns:
        f-string: Spans of time where the unpaid balance condition was met
    """

    return f"""
    WITH aggregated_fines_fees_per_client AS (
        {client_specific_fines_fees_balance(unpaid_balance_field=unpaid_balance_field)}
    )

    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        current_balance {unpaid_balance_criteria} AS meets_criteria,
        TO_JSON(STRUCT(current_balance AS amount_owed)) AS reason,
        current_balance AS amount_owed,
    FROM aggregated_fines_fees_per_client
    WHERE fee_type = "{fee_type}"
    """


def classification_incident_score_query_template(
    *,
    incident_filter_condition: str,
    state_code: str,
    score_definitions: dict[tuple[int, int], dict[int, int]] | None = None,
    max_total_score: int | None = None,
    initial_score_definitions: dict[tuple[int, int], dict[int, int]] | None = None,
    reclass_score_definitions: dict[tuple[int, int], dict[int, int]] | None = None,
    max_initial_score: int | None = None,
    max_reclass_score: int | None = None,
) -> str:
    """
    Generates a SQL query for calculating classification-form scores based on
    incarceration incident types occurring in various time windows, reading from
    incarceration_incidents_classification_preprocessed. Supports both single-score
    output (one `total_score` column, e.g. TN) via `score_definitions`, and dual
    initial/reclassification score output (e.g. MI) via `initial_score_definitions`/
    `reclass_score_definitions`. Exactly one of `score_definitions` or the
    (`initial_score_definitions`, `reclass_score_definitions`) pair must be set.

    The resulting query produces spans with:
    - Score column(s) representing the sum of scores across relevant time windows
    - An incidents_list JSON array containing details of each contributing incident

    Args:
        incident_filter_condition: A WHERE clause condition to filter which incidents
            are included in the scoring. Should reference columns available in
            incarceration_incidents_classification_preprocessed based on the state (e.g.,
            "incident_category = 'violent'").
        state_code: The state code to filter incidents and prison spans.
        score_definitions: Single-score mode. Maps time windows to count-to-score
            mappings. Keys are tuples of (start_months, end_months) defining the
            lookback window. Values are dicts mapping incident counts to scores. The
            last key in each inner dict is treated as a >= threshold.
            Example: {
                (0, 6): {0: 0, 1: 25, 2: 50, 3: 75},
                (6, 12): {0: 0, 1: 8, 2: 16},
            }
        max_total_score: If set, caps total_score using LEAST(). Only valid in
            single-score mode (score_definitions set).
        initial_score_definitions: Dual-score mode. Same shape as score_definitions,
            scoped to just the windows that count toward `initial_total_score`. A
            window that shouldn't contribute to the initial score simply isn't a key
            here (it can still be a key in reclass_score_definitions).
        reclass_score_definitions: Dual-score mode. Same shape as score_definitions,
            scoped to the windows that count toward `reclass_total_score`.
        max_initial_score: If set, caps initial_total_score using LEAST(). Only valid
            in dual-score mode.
        max_reclass_score: If set, caps reclass_total_score using LEAST(). Only valid
            in dual-score mode.
    """
    is_dual_mode = (
        initial_score_definitions is not None or reclass_score_definitions is not None
    )
    if is_dual_mode:
        if initial_score_definitions is None or reclass_score_definitions is None:
            raise ValueError(
                "Dual-score mode requires both initial_score_definitions and "
                "reclass_score_definitions to be set."
            )
        if score_definitions is not None:
            raise ValueError(
                "score_definitions must not be set when initial_score_definitions/"
                "reclass_score_definitions are set (dual-score mode)."
            )
        if max_total_score is not None:
            raise ValueError(
                "max_total_score is only valid in single-score mode (score_definitions "
                "set). Use max_initial_score/max_reclass_score in dual-score mode."
            )
    elif score_definitions is None:
        raise ValueError(
            "Must set either score_definitions (single-score mode) or both "
            "initial_score_definitions and reclass_score_definitions (dual-score mode)."
        )
    elif max_initial_score is not None or max_reclass_score is not None:
        raise ValueError(
            "max_initial_score and max_reclass_score are only valid in dual-score mode "
            "(initial_score_definitions/reclass_score_definitions must be set)."
        )

    def create_case_when_clause(
        score_mapping: dict[int, int],
        num_incidents_column: str,
        output_column_name: str,
    ) -> str:
        """Creates a CASE WHEN clause to map incident counts to a score column."""
        # The last key in score_mapping is treated as an open-ended ">=" threshold
        # (see docstring), not an exact match — this is what makes max_key special below.
        max_key = max(score_mapping.keys())
        case_when_cases = [
            f"WHEN {num_incidents_column} = {k} THEN {v}"
            for k, v in score_mapping.items()
            if k != max_key
        ]
        case_when_cases.append(
            f"WHEN {num_incidents_column} >= {max_key} THEN {score_mapping[max_key]}"
        )
        case_when_cases_clause = "\n".join(case_when_cases)
        return f"CASE\n{case_when_cases_clause}\nELSE NULL END AS {output_column_name}"

    if is_dual_mode:
        initial_score_defs = assert_type(initial_score_definitions, dict)
        reclass_score_defs = assert_type(reclass_score_definitions, dict)
        # Reclass windows first since reclass is a superset of initial in every
        # state we support today; order only affects generated SQL readability.
        windows = list(
            dict.fromkeys(
                list(reclass_score_defs.keys()) + list(initial_score_defs.keys())
            )
        )
        case_when_clauses = [
            create_case_when_clause(
                score_mapping,
                f"num_incidents_past_{start}_to_{end}_months",
                f"score_reclass_past_{start}_to_{end}_months",
            )
            for (start, end), score_mapping in reclass_score_defs.items()
        ] + [
            create_case_when_clause(
                score_mapping,
                f"num_incidents_past_{start}_to_{end}_months",
                f"score_initial_past_{start}_to_{end}_months",
            )
            for (start, end), score_mapping in initial_score_defs.items()
        ]
    else:
        single_score_defs = assert_type(score_definitions, dict)
        windows = list(single_score_defs.keys())
        case_when_clauses = [
            create_case_when_clause(
                score_mapping,
                f"num_incidents_past_{start}_to_{end}_months",
                f"score_past_{start}_to_{end}_months",
            )
            for (start, end), score_mapping in single_score_defs.items()
        ]
    case_when_clauses_all = ",\n".join(case_when_clauses)

    incident_count_ctes = ",\n".join(
        [
            f"""incidents_past_{start}_to_{end}_months AS
        (
            WITH {num_events_within_time_interval_spans(
                events_cte="relevant_incidents",
                date_interval=end,
                date_interval_start=start,
                date_part="MONTH",
                index_columns=["person_id", "state_code", "custodial_authority_session_id"],
                event_list_field="incarceration_incident_id",
                truncate_to_month=True,
            )}
            SELECT
                person_id,
                state_code,
                custodial_authority_session_id,
                start_date,
                end_date,
                event_count AS num_incidents_past_{start}_to_{end}_months,
                event_list AS incident_ids_past_{start}_to_{end}_months,
            FROM event_count_spans
        )
        """
            for (start, end) in windows
        ]
    )

    # Each window's incident counts must appear as its own row (with every other
    # window zero-filled) so create_sub_sessions_with_attributes can sub-session
    # them independently; the GROUP BY MAX below recombines them per person/window.
    combined_cte_clauses = []
    for active_start, active_end in windows:
        incident_fields = [
            f"num_incidents_past_{start}_to_{end}_months"
            if (start, end) == (active_start, active_end)
            else f"0 AS num_incidents_past_{start}_to_{end}_months"
            for (start, end) in windows
        ]
        incident_id_fields = [
            f"incident_ids_past_{start}_to_{end}_months"
            if (start, end) == (active_start, active_end)
            else f"CAST([] AS ARRAY<INT64>) AS incident_ids_past_{start}_to_{end}_months"
            for (start, end) in windows
        ]
        incident_fields_all = ",\n".join(incident_fields)
        incident_id_fields_all = ",\n".join(incident_id_fields)
        combined_cte_clauses.append(
            f"""
    SELECT
        person_id,
        state_code,
        custodial_authority_session_id,
        start_date,
        end_date,
        {incident_fields_all},
        {incident_id_fields_all},
    FROM incidents_past_{active_start}_to_{active_end}_months
    """
        )

    max_incidents_fields = [
        f"MAX(num_incidents_past_{start}_to_{end}_months) AS num_incidents_past_{start}_to_{end}_months"
        for (start, end) in windows
    ]
    max_incident_ids_fields = [
        f"ARRAY_CONCAT_AGG(incident_ids_past_{start}_to_{end}_months) AS incident_ids_past_{start}_to_{end}_months"
        for (start, end) in windows
    ]
    max_incidents_clause = ",\n".join(max_incidents_fields + max_incident_ids_fields)

    incident_ids_unnest_clauses = [
        f"""SELECT
            person_id,
            state_code,
            custodial_authority_session_id,
            start_date,
            end_date,
            incarceration_incident_id,
            '{start}-{end} months' AS incident_time_period
        FROM calculated_scores_separate,
        UNNEST(incident_ids_past_{start}_to_{end}_months) AS incarceration_incident_id"""
        for (start, end) in windows
    ]
    # An incident can fall in different windows at different points in time (e.g. a
    # 5-month-old incident is in the 0-6 month window; a month later the same
    # incident is in the 6-12 month window). Unnesting per window — rather than
    # once against a merged array — is what lets each occurrence get tagged with
    # the correct incident_time_period for its span.
    incident_ids_unnest_union = "\n        UNION ALL\n        ".join(
        incident_ids_unnest_clauses
    )

    incident_id_arrays_clause = ",\n".join(
        [f"incident_ids_past_{start}_to_{end}_months" for (start, end) in windows]
    )

    combined_cte = "\nUNION ALL\n".join(combined_cte_clauses)

    if is_dual_mode:
        initial_aggregate_score_clause = " + ".join(
            f"score_initial_past_{start}_to_{end}_months"
            for (start, end) in initial_score_defs
        )
        if max_initial_score is not None:
            initial_aggregate_score_clause = (
                f"LEAST({max_initial_score}, {initial_aggregate_score_clause})"
            )

        reclass_aggregate_score_clause = " + ".join(
            f"score_reclass_past_{start}_to_{end}_months"
            for (start, end) in reclass_score_defs
        )
        if max_reclass_score is not None:
            reclass_aggregate_score_clause = (
                f"LEAST({max_reclass_score}, {reclass_aggregate_score_clause})"
            )

        score_select_sql = (
            f"{initial_aggregate_score_clause} AS initial_total_score,\n"
            f"        {reclass_aggregate_score_clause} AS reclass_total_score,"
        )
    else:
        aggregate_score_clause = " + ".join(
            [f"score_past_{start}_to_{end}_months" for (start, end) in windows]
        )
        if max_total_score is not None:
            aggregate_score_clause = (
                f"LEAST({max_total_score}, {aggregate_score_clause})"
            )
        score_select_sql = f"{aggregate_score_clause} AS total_score,"

    return f"""
    WITH state_prison_spans AS (
        SELECT
            person_id,
            state_code,
            custodial_authority_session_id,
            start_date,
            end_date_exclusive,
        FROM `{{project_id}}.sessions.custodial_authority_sessions_materialized`
        WHERE
            state_code = '{state_code}'
            AND custodial_authority = 'STATE_PRISON'
    )
    ,
    relevant_incidents AS (
        SELECT
            * EXCEPT (incident_date),
            incident_date AS event_date,
        FROM `{{project_id}}.{CLASSIFICATION_VIEWS_DATASET}.incarceration_incidents_classification_preprocessed_materialized`
        WHERE state_code = '{state_code}'
            AND {incident_filter_condition}
    )
    ,
    {incident_count_ctes}
    ,
    combined_incident_spans AS (
        {combined_cte}
    )
    ,
    {create_sub_sessions_with_attributes('combined_incident_spans', index_columns=['person_id', 'state_code', 'custodial_authority_session_id'])}
    ,
    sub_sessions_deduped AS (
        SELECT
            person_id,
            state_code,
            custodial_authority_session_id,
            start_date,
            end_date,
            {max_incidents_clause},
        FROM sub_sessions_with_attributes
        GROUP BY 1, 2, 3, 4, 5
    )
    ,
    calculated_scores_separate AS (
        SELECT
            incident_counts.person_id,
            incident_counts.state_code,
            incident_counts.custodial_authority_session_id,
            incident_counts.start_date,
            LEAST(incident_counts.end_date, {nonnull_end_date_clause('state_prison_spans.end_date_exclusive')}) AS end_date,
            {case_when_clauses_all},
            {incident_id_arrays_clause},
        FROM sub_sessions_deduped incident_counts
        LEFT JOIN state_prison_spans
        USING (person_id, state_code, custodial_authority_session_id)
        WHERE incident_counts.start_date < {nonnull_end_date_clause('state_prison_spans.end_date_exclusive')}
    )
    ,
    incident_ids_with_time_period AS (
        {incident_ids_unnest_union}
    )
    ,
    incident_details_unnested AS (
        SELECT
            incident_periods.person_id,
            incident_periods.state_code,
            incident_periods.custodial_authority_session_id,
            incident_periods.start_date,
            incident_periods.end_date,
            incident_periods.incident_time_period,
            relevant_incidents.incarceration_incident_id,
            relevant_incidents.event_date AS incident_date,
            relevant_incidents.infraction_type_raw_text,
            relevant_incidents.incident_class
        FROM incident_ids_with_time_period incident_periods
        INNER JOIN relevant_incidents
            ON incident_periods.incarceration_incident_id = relevant_incidents.incarceration_incident_id
    )
    ,
    incident_details_aggregated AS (
        SELECT
            person_id,
            state_code,
            custodial_authority_session_id,
            start_date,
            end_date,
            TO_JSON(
                ARRAY_AGG(
                    STRUCT(
                        incarceration_incident_id,
                        incident_date,
                        infraction_type_raw_text,
                        incident_class,
                        incident_time_period
                    )
                    ORDER BY incident_date
                )
            ) AS incidents_list,
            MAX(incident_date) AS latest_incident_date
        FROM incident_details_unnested
        GROUP BY 1, 2, 3, 4, 5
    )
    SELECT
        person_id,
        state_code,
        start_date,
        end_date AS end_date_exclusive,
        {score_select_sql}
        IFNULL(incident_details_aggregated.incidents_list, TO_JSON([])) AS incidents_list,
        incident_details_aggregated.latest_incident_date
    FROM calculated_scores_separate incident_counts
    LEFT JOIN incident_details_aggregated
        USING (person_id, state_code, custodial_authority_session_id, start_date, end_date)
    WHERE end_date > start_date
    """
