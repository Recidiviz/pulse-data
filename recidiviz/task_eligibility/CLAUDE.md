# Task Eligibility Spans

Task Eligibility Spans (TES) determines when people are eligible for specific
opportunities (e.g., early discharge, custody downgrades, work release).

## Overview

TES computes time periods during which someone is eligible (or almost eligible)
for a task/opportunity using a composable architecture:

```
Candidate Population + Criteria Spans + Completion Events = Task Eligibility Spans
```

- **Criteria**: Time spans when conditions are met (outputs `meets_criteria` and
  `reason`). **Note: `end_date` is always exclusive.**
- **Candidate Populations**: Who to consider for a task (rarely need new ones)
- **Completion Events**: Events marking task completion
- **Eligibility Spans**: Final output with `is_eligible` and
  `is_almost_eligible`

## Data Source Priorities

When writing criteria queries, **always prefer downstream/aggregated views**:

1. **Use `sessions` dataset** (e.g., `supervision_super_sessions`) over raw
   entities
2. **Never use `state`** — always use `normalized_state` dataset
3. **Prefer aggregated views** that handle sessionization and edge cases

#### Helper Functions

Before writing new criteria: Check for existing helper functions. If the
criteria relies on repeated logic, create or modify a helper function in the
appropriate location.

| Function                              | Source                             | Description                                                                                                                               |
| ------------------------------------- | ---------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `create_sub_sessions_with_attributes` | `sessions_query_fragments.py`      | Joins multiple span tables and subdivides at boundaries to avoid overlapping spans. Used often when combining two tables with time spans. |
| `nonnull_end_date_clause`             | `bq_utils.py`                      | Replaces NULL end dates with a far-future date for comparisons                                                                            |
| `critical_date_has_passed_spans_cte`  | `critical_date_query_fragments.py` | Creates spans indicating whether a critical date has passed                                                                               |
| `aggregate_adjacent_spans`            | `sessions_query_fragments.py`      | Merges adjacent spans with identical attributes into single spans                                                                         |
| `nonnull_end_date_exclusive_clause`   | `bq_utils.py`                      | Like `nonnull_end_date_clause` but for exclusive end dates                                                                                |

Additional resources:

- `utils/general_criteria_builders.py` and
  `utils/state_dataset_query_fragments.py`
- `utils/us_xx_query_fragments.py` for state-specific helpers

## Creating Components

### Adding a New Criteria

Use `StateSpecificTaskCriteriaBigQueryViewBuilder` (name MUST start with
`US_XX_`) or `StateAgnosticTaskCriteriaBigQueryViewBuilder`. For inverted
criteria (negating an existing criteria), use
`StateSpecificInvertedTaskCriteriaBigQueryViewBuilder` or
`StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder`. Look at existing
criteria files.

### Adding a Criteria Group

Use `StateSpecificTaskCriteriaGroupBigQueryViewBuilder` or
`StateAgnosticTaskCriteriaGroupBigQueryViewBuilder` to combine criteria with
AND/OR/XOR logic.

### Adding a New Task

Use `SingleTaskEligibilitySpansBigQueryViewBuilder` to combine candidate
population, criteria, and completion event. See `eligibility_spans/` for
examples.

### Adding a Compliance Task

Use `ComplianceTaskEligibilitySpansBigQueryViewBuilder` for compliance-oriented
tasks that have due dates (e.g., contacts, assessments). This builder extends
the basic task eligibility spans with compliance and due date information. See
`eligibility_spans/` for examples.

### Creating Almost Eligible Conditions

Use the functions located in `criteria_condition.py`:

- `TimeDependentCriteriaCondition`: Almost eligible N time units before a date
- `LessThanCriteriaCondition`: Almost eligible if value < threshold
- `LessThanOrEqualCriteriaCondition`: Almost eligible if value <= threshold
- `PickNCompositeCriteriaCondition`: Combine conditions (at least N must be
  true)

## Validating in Sandbox

**Offer to validate** by loading views to sandbox. Always use `auto` when
possible:

```bash
python -m recidiviz.tools.load_views_to_sandbox \
    --sandbox_dataset_prefix <prefix> --state_code_filter <state_code> \
    auto --load_changed_views_only
```

**Suggest checking for overlapping/zero-day spans** (should return 0 rows):

```sql
-- Check criteria spans
SELECT * FROM `recidiviz-staging.<prefix>_validation_views__task_eligibility.overlapping_tes_criteria_spans` WHERE state_code = '<state_code>';
SELECT * FROM `recidiviz-staging.<prefix>_validation_views__task_eligibility.zero_day_tes_criteria_spans` WHERE state_code = '<state_code>';

-- Check eligibility spans
SELECT * FROM `recidiviz-staging.<prefix>_validation_views__task_eligibility.overlapping_task_eligibility_spans` WHERE state_code = '<state_code>';
SELECT * FROM `recidiviz-staging.<prefix>_validation_views__task_eligibility.zero_day_task_eligibility_spans` WHERE state_code = '<state_code>';
```

**Offer to compare eligible counts** with a quick SQL query:

```sql
SELECT 'staging' AS source, COUNT(DISTINCT person_id) AS eligible_count
FROM `recidiviz-staging.task_eligibility_spans_<state_code>.<task_name>_materialized`
WHERE CURRENT_DATE('US/Eastern') BETWEEN start_date AND IFNULL(end_date, '9999-12-31')
  AND is_eligible = TRUE
UNION ALL
SELECT 'sandbox', COUNT(DISTINCT person_id)
FROM `recidiviz-staging.<prefix>_task_eligibility_spans_<state_code>.<task_name>_materialized`
WHERE CURRENT_DATE('US/Eastern') BETWEEN start_date AND IFNULL(end_date, '9999-12-31')
  AND is_eligible = TRUE
```

**Or use `compare_views`** for a more robust diff:

```bash
python -m recidiviz.tools.calculator.compare_views \
    --dataset_original task_eligibility_spans_<state_code> \
    --dataset_new <prefix>_task_eligibility_spans_<state_code> \
    --view_id <task_name>_materialized \
    --output_dataset_prefix <prefix>_test \
    --grouping_columns state_code \
    --primary_keys state_code,person_id,start_date
```

## Directory Structure

```
task_eligibility/
├── criteria/{general,state_specific/us_*}     # Conditions for eligibility
├── candidate_populations/{general,state_specific}
├── completion_events/{general,state_specific}
├── eligibility_spans/us_*/                    # Task definitions
├── utils/                                     # Query helpers
│   ├── general_criteria_builders.py
│   └── us_*_query_fragments.py
└── criteria_condition.py                      # Almost eligible conditions
```

## Running/Debugging

Print generated SQL:
`uv run python recidiviz/task_eligibility/criteria/general/age_21_years_or_older.py`

Tests live in `recidiviz/tests/task_eligibility/`.
