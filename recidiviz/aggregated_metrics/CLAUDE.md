# Aggregated Metrics & Observations

Conventions and gotchas for working in `recidiviz/aggregated_metrics/` and
`recidiviz/observations/`. These two modules are tightly coupled — observations produce the
row-level signal, aggregated metrics roll it up — so this guide covers both. A short
`recidiviz/observations/CLAUDE.md` points here.

## Key terms

- **Observation:** a row-level fact, either an **event** (something that happened on a date, e.g.
  a supervision contact) or a **span** (something true between a start and end date, e.g. being
  on supervision). Each observation type (`EventType` / `SpanType`) is one view under
  `observations/views/{events,spans}/<unit>/`.
- **Attribute:** an extra column on an observation (e.g. `supervision_level`) that metrics can
  filter or slice by.
- **Unit of observation** (`MetricUnitOfObservationType`): who or what an observation is about,
  such as a person, a supervision officer, or a product user.
- **Unit of analysis** (`MetricUnitOfAnalysisType`): the level a metric reports at, such as the
  state, a district, or an officer.
- **Assignment:** a span during which a unit of observation belongs to a unit of analysis (e.g. a
  person on an officer's caseload). Assignments decide which observations count toward which unit
  of analysis; `assignment_sessions_view_builder.py` builds them.
- **Population** (`MetricPopulationType`): the group a metric covers: `SUPERVISION`,
  `INCARCERATION`, `JUSTICE_INVOLVED`, or `CUSTOM`.
- **Time period** (`MetricTimePeriod`): the window each value covers, such as a month or a year.
- **Aggregated metric:** rolls observations up into one value per population × unit of analysis ×
  time period. For example, in the supervision population, `AVG_DAILY_POPULATION` gives the
  average number of people supervised in a month statewide (`STATE_CODE`), in each district
  (`SUPERVISION_DISTRICT`), or on each officer's caseload (`SUPERVISION_OFFICER`).

## Picking a metric class (`models/aggregated_metric.py`)

Metrics come in 4 families = **(span vs event) × (whole-period vs assignment-relative)**. Pick the
**family first** — it decides *which observation rows attribute to a period*, not just the math
(`metric_time_period_to_assignment_join_type()`): period families intersect the period
(`INTERSECTION` / `INTERSECTION_EVENT_ATTRIBUTION`); assignment families key off the assignment
start date and a `window_length_days` window (default 365).

| Family (base class) | Observations | Concrete classes |
| --- | --- | --- |
| `PeriodSpanAggregatedMetric` | spans over the period | `DailyAvgSpanCountMetric`, `DailyAvgSpanValueMetric`, `DailyAvgTimeSinceSpanStartMetric`, `SumSpanDaysMetric`, `SpanDistinctUnitCountMetric` |
| `PeriodEventAggregatedMetric` | events during the period | `EventCountMetric`, `EventValueMetric`, `SumEventValueMetric`, `EventDistinctUnitCountMetric` |
| `AssignmentSpanAggregatedMetric` | spans in the window after assignment | `AssignmentSpanDaysMetric`, `AssignmentSpanMaxDaysMetric`, `AssignmentSpanValueAtStartMetric`, `AssignmentCountMetric` |
| `AssignmentEventAggregatedMetric` | events in the window after assignment | `AssignmentDaysToFirstEventMetric`, `AssignmentEventCountMetric`, `AssignmentEventBinaryMetric` |

Then pick the concrete class by aggregation, minding these per-class behaviors:

- **Dedup differs by class.** `DailyAvgSpanCountMetric` does **no** per-person dedup (see fan-out
  rules below); `EventCountMetric` dedups to one person-event per day (override with
  `event_segmentation_columns`); the `*DistinctUnitCount` classes and `AssignmentEventBinaryMetric`
  count at most one per unit / per person-assignment ("any … in window", vs
  `AssignmentEventCountMetric`'s "how many").
- **`AssignmentDaysToFirstEventMetric` returns the full `window_length_days` when no event
  occurred** (not NULL/0) — watch this in averages.
- **Average/value metrics require a companion count metric** (`EventValueMetric.event_count_metric`,
  `AssignmentSpanValueAtStartMetric.span_count_metric`); it weights the value correctly when rolling
  up across time periods. A new "average X" without its count companion aggregates wrong.
- **Any extra attribute a metric reads must be listed in `referenced_observation_attributes()`**
  (subclasses append `span_value_numeric` / `weight_col` / `event_segmentation_columns` / etc.) —
  it's what the query builder pulls and what the config test (below) checks.

## Regeneration & commit workflow (the #1 source of PR churn)

Changes here produce large checked-in diffs in generated files. Both generators run as
pre-commit hooks, so **commit through pre-commit** (never `--no-verify`) and include the
regenerated files in the PR.

- **Calc-catalog docs** (`docs/calculation/**`, `docs/SUMMARY.md`) — hook
  `generate_calc_documentation` (`.pre-commit-config.yaml`). On demand:
  ```
  uv run python -m recidiviz.calculator.calculation_documentation_generator
  ```
- **Looker LKML** (`recidiviz/tools/looker/__generated__/**.lkml`) — hook `generate_lookml`;
  its `files:` regex includes `^recidiviz/aggregated_metrics/` and
  `^recidiviz/observations/views/`. CI fails on stale output via
  `verify_lookml_autogenerated_file_contents.py`. On demand:
  ```
  uv run python -m recidiviz.tools.looker.generate_all_lookml [<modified_files>...] [--force-all]
  ```

## What each kind of change requires

- **Existing metric on a new surface:**
  - *Looker explore:* append the metric to the matching
    `recidiviz/tools/looker/aggregated_metrics/custom_<product>_metrics_configurations.py` list.
    To let the explore slice by an observation attribute, register the attribute in that file's
    JSON-field-filters dict (e.g. `TASKS_JSON_FIELD_FILTERS_WITH_SUGGESTIONS`).
  - *Materialized BigQuery collection* (`configuration/collections/`, e.g. `standard.py`): add the
    metric to a collection only when a product needs it materialized on a recurring basis. Looker
    already covers analysis and monitoring, and every metric in a collection is recomputed on every
    view refresh. To break every metric in a collection out by observation attributes, set the
    collection's `disaggregate_by_observation_attributes`.
- **New metric for an existing observation:** define the constant in
  `models/aggregated_metric_configurations.py`. That is enough for a product view or an ad-hoc
  analysis; to put it in Looker or a materialized collection, see the bullet above.
- **New observation for an existing unit of observation:** add the `EventType`/`SpanType` member
  **and** its branch in `unit_of_observation_type()` (`observations/event_type.py` /
  `span_type.py`). Then add the view as `views/{events,spans}/<unit>/<view_id>.py` with a
  `VIEW_BUILDER`; the collector picks it up from that folder, so there's no list to register it in.
- **New dimension on an existing observation:** compute the column in the view's SQL and declare
  it in the builder's `attribute_cols` list. To slice by it in an explore, also register it in the
  Looker config's JSON-field-filters dict (e.g. `TASKS_JSON_FIELD_FILTERS_WITH_SUGGESTIONS`; an
  empty suggestion list is fine). The dict key must **exactly match** the attribute column name.
- **New unit of observation:** add the enum member in
  `observations/metric_unit_of_observation_type.py` and its `primary_key_columns` in
  `observations/metric_unit_of_observation.py`; map it to each unit of analysis with an assignment
  `SELECT` in `assignment_sessions_view_builder.py`; pick its population with a `SpanSelector` per
  `MetricPopulationType` in `assignment_sessions_view_collector.py`; register the dataset in
  `view_registry/deployed_views.py`. Then add observations as above, in new
  `views/{events,spans}/<unit>/` folders named after the unit. **Mirror the closest existing unit
  1:1.**
- **New unit of analysis:** add the enum member and its `MetricUnitOfAnalysis` (primary key and
  static attribute columns) in `models/metric_unit_of_analysis_type.py`; map each unit of
  observation to it with an assignment `SELECT` in `assignment_sessions_view_builder.py`; enable it
  for the relevant populations in `assignment_sessions_view_collector.py`. Optionally add it to a
  Looker config.

## Registration & CI gates (things that fail before query time)

- **`aggregated_metric_configurations_test.py`** asserts every attribute a configured metric
  references (condition-dict keys, `*_value_numeric`, `weight_col`, `event_segmentation_columns`)
  actually exists on its source observation view. Filtering on an attribute the observation doesn't
  expose fails here, not at query time.
- **Observation collector tests** (`observation_big_query_view_collector_test.py`): exactly one view
  per `EventType`/`SpanType` and every enum member must have a view; no duplicate addresses; the view
  must `SELECT` its declared output columns, and must contain no state-specific logic (see
  [`../observations/CLAUDE.md`](../observations/CLAUDE.md)).
- **`event_type_test.py` / `span_type_test.py`** assert every enum member has a
  `unit_of_observation_type` — this backstops the enum-member + mapping edit above.
- A metric `name` becomes its output column **verbatim** (fragments end `AS {name}`; schema and
  columns are sorted by name). "lowercase, no spaces, unique within a population" is enforced only by
  convention — the attrs validator just checks non-empty — so a duplicate or non-column-safe name
  breaks the view silently. Same for each `disaggregate_by_observation_attributes` entry.

## Observation authoring rules

For how to write an observation view, see
[`../observations/CLAUDE.md`](../observations/CLAUDE.md). On the metric side, attribute values
always reach the metric as strings, whatever type the observation column declares. Match them in
`event_conditions_dict` / `span_conditions_dict` as strings (`{"is_mobile": ["true"]}`), not as
Python bools.

## Determinism & fan-out correctness (what reviewers catch)

- **`DailyAvgSpanCountMetric` has no per-person dedup.** When a span gains a dimension that can
  co-occur per person (e.g. a client owing multiple assessment types), **pin** that attribute to a
  single value in every existing metric so counts don't double-count. The convention is an `ALL`
  sentinel row in the span for the aggregate, with canonical metrics pinned to it
  (`{"assessment_type": ["ALL"]}`) so they stay byte-identical. Per-type metrics each select one
  value.
- **`EventCountMetric` counts at most one event per person per day.** If same-day events are
  genuinely distinct (e.g. two contacts of different types), list the attribute columns that tell
  them apart in `event_segmentation_columns`; rows that differ in those columns count separately.
- **Keep numerator and denominator filters in lockstep.** A rate built from two views must apply
  the identical filter to both, together, or they drift.
- **Keep observations complete; filter downstream.** Suppression/slicing belongs in the metric or
  drilldown view, not in the observation — the observation should stay a complete record.
- **Derive membership from config flags, not hardcoded lists**, and `raise ValueError` if the
  generated set is empty so a misconfiguration fails loudly (e.g. union compliance-task views by
  filtering on `config.include_in_route_planner`).

## Naming conventions

- Name a metric's constant in `models/aggregated_metric_configurations.py` after its `name` field,
  uppercased (`ABSCONSIONS_BENCH_WARRANTS = EventCountMetric(name="absconsions_bench_warrants", ...)`).
- Place a new metric next to related metrics (those built from the same observation) in that file.
- Promote a **shared SQL column name / action-label string** to one imported constant referenced by
  both the observation and the metric — don't re-inline it in each.

## Validation

`python <view>.py` only renders the template — it does not check the SQL. Validate real output by
loading to a sandbox; for the mechanics (flags, modes) see the sandbox sections in
[`../big_query/CLAUDE.md`](../big_query/CLAUDE.md) and
[`../task_eligibility/CLAUDE.md`](../task_eligibility/CLAUDE.md). Module-specific expectations:

- Prove correctness with **row-count parity vs staging + column-level invariants** (e.g. "late date
  is NULL for every non-missed contact = 0 violations"), not BigQueryEmulator tests. Small 1–2 row
  deltas over millions are materialization-timing drift, not real changes.
- **Spot-check a new metric's values** once its observation is in a sandbox (or deployed).
  Either call `get_custom_aggregated_metrics(...)` (`recidiviz/tools/analyst/aggregated_metrics_utils.py`)
  in a notebook, or render `get_custom_aggregated_metrics_query_template(...)`
  (`query_building/custom_aggregated_metrics_template.py`) for the metric, population, unit of
  analysis, and time period, and run the printed SQL in BigQuery. For a sandboxed observation,
  point the observation's dataset in the SQL at your sandbox dataset. Both compute the metric ad
  hoc, so you don't need to add it to a collection or Looker first.
- **Re-run validation after any population-scope change** — posting numbers measured before a
  denominator re-scope is a repeated footgun.
- **Test generated LookML end to end in Looker.** Regenerating the LKML and passing CI only
  proves the files are current, not that the explore works. From the PR branch, push the
  generated files to a throwaway branch in the `looker` repo:
  ```
  uv run python -m recidiviz.tools.looker.copy_lookml_to_looker_branch [--branch-name test-lookml/<name>]
  ```
  It clones `looker` into `./__TEMP_LOOKER_REPO_DIR__` with your git credentials and pushes a
  `[DO NOT MERGE]` commit (default branch `test-lookml/<git-user>-<timestamp>`; rerun with the
  same `--branch-name` to update it). Looker deletes these branches after 30 days. Then, in
  Looker's Development Mode, check out that branch, run **Validate LookML**, and open the
  explore to confirm the new fields or filters render and return results. The push goes to
  another repo, so confirm with the user before running it.
