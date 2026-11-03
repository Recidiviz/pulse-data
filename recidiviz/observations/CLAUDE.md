# Observations

Observations are typed, row-level **events** (point-in-time) and **spans** (intervals) about a
*unit of observation* (person, supervision officer, product user, …). They are the row-level
signal that aggregated metrics roll up.

**For how observations feed metrics — regeneration steps, the edits each kind of change requires, fan-out/determinism
rules, and validation — see [`recidiviz/aggregated_metrics/CLAUDE.md`](../aggregated_metrics/CLAUDE.md).**
The rules below cover writing the observation views themselves.

## Authoring rules

- A view is one file under `views/{events,spans}/<unit>/`, built with
  `EventObservationBigQueryViewBuilder` / `SpanObservationBigQueryViewBuilder`, with a `__main__`
  block calling `VIEW_BUILDER.build_and_print()` under
  `local_project_id_override(GCP_PROJECT_STAGING)`. The base schema (person_id/state_code/date) is
  auto-prepended — **add a dimension via `attribute_cols`, not by editing base columns**.
- **Observations are state-agnostic concepts.**
  `test_observation_views_have_no_state_specific_logic` (in
  `observation_big_query_view_collector_test.py`) fails any observation view that references a
  specific state code. Its `exempt_views` set covers the rare case where aggregated metrics would
  help a state's use case but the state-agnostic schema work for a general observation doesn't
  exist yet. Add to it with caution; prefer building the state-agnostic version.
- **Attribute-as-of-event-date join:** first check whether the span view you are joining can have
  overlapping spans for the same person. If it can't, join with
  `LEFT JOIN ... event_date BETWEEN start_date AND {nonnull_end_date_exclusive_clause("end_date")}`
  and unwrap with `ANY_VALUE(...)`; a date falls in at most one span, so this doesn't fan out.
  `nonnull_end_date_exclusive_clause` (from `recidiviz.calculator.query.bq_utils`) handles
  open-ended spans. If spans can overlap, `ANY_VALUE` picks an arbitrary span, so write join logic
  that chooses the span deliberately.
- **Registering a type requires two edits in lockstep** in `event_type.py` / `span_type.py`: add the
  `EventType`/`SpanType` member **and** its branch in `unit_of_observation_type()`.
- **Keep observations complete; suppress/slice downstream** (in the metric or drilldown), not in the
  observation.

Editing any view here regenerates `docs/calculation/**` and `recidiviz/tools/looker/__generated__/**`
via pre-commit — commit through pre-commit and include those diffs in the PR (see the
aggregated_metrics guide for the exact commands).
