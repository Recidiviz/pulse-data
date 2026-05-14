## Diagnosing Tasks Issues

Tasks use `compliance_task_eligibility_spans_<state_code>` (not
`task_eligibility_spans`). Key fields: `is_eligible`, `due_date`,
`last_task_completed_date`, `is_overdue`, `reasons_v2`.

#### T-i: What are the relevant contact requirements?

- **Case type / supervision level**: Query
  `tasks_views.<state_code>_case_type_supervision_level_spans_materialized`.
  Note the current supervision level and case type, as well as any recent
  changes.
- **Contact standards**: Check the static reference CSV at
  `recidiviz/calculator/query/state/views/reference/static_reference_data/<state_code>_contact_standards.csv`
  to verify the expected contact frequency for the person's supervision level
  and case type combination.
- **Contact cadence spans**: Query
  `tasks_views.<state_code>_contact_cadence_spans_materialized` to see the
  cadence periods and required contact quantities for this person. Focus on the
  current date.

#### T-ii: What does the compliance TES say?

Query the compliance task eligibility span for the person and task at the
current time:

```sql
SELECT person_id, start_date, end_date, is_eligible, due_date,
       last_task_completed_date, reasons_v2
FROM `recidiviz-123.compliance_task_eligibility_spans_<state_code>.<task_name>_materialized`
WHERE person_id IN (<PERSON_IDS>)
  AND CURRENT_DATE('US/Eastern') BETWEEN start_date
      AND IFNULL(end_date, '9999-12-31')
```

Pull the following fields and present them:

- **`is_eligible`**: Is this person eligible for the task right now?
- **`due_date`**: When is the next due date?
- **`last_task_completed_date`**: When was the last contact or task completion?
- **`reasons_v2`**: Parse the JSON to understand the criteria evaluation — look
  for contact counts, contact_cadence, and which criteria are met/unmet.
- **`candidate_population_view_builder`**: If the person is not in the TES,
  check which candidate population they are in (e.g.,
  "meets_supervision_span_triggers") and query that view to understand why they
  are not in the TES.

#### T-iii: Is the contact date wrong?

Check what contacts Recidiviz has ingested vs what the user reports. Walk
**down** the chain — each tier is closer to the original state data:

1. **Preprocessed contacts** (highest level — remapped + filtered): Query
   `tasks_views.<state_code>_contact_events_preprocessed_materialized` to see
   what contacts Recidiviz knows about.
2. **Normalized state** (post-ingest, after entity normalization): Query
   `<state_code>_normalized_state.state_supervision_contact`.
3. **Raw data** (exactly what the state sent us, before any Recidiviz
   transformation): Look up the raw table(s) that hydrate
   `state_supervision_contact` in the ingest mapping at
   `recidiviz/ingest/direct/regions/<state_code>/ingest_mappings/state_supervision_contact.yaml`
   (or wherever that entity's mapping lives), then query the corresponding
   `_latest` view in `<state_code>_raw_data_up_to_date_views.<raw_table>_latest`.

If the preprocessed view is missing a contact that exists in normalized state,
contact-type remapping or status filtering may be excluding it. If normalized
state is missing a contact that exists in raw data, the issue is in ingest
itself (mapping, enum parsing, or row filtering).

#### T-iv: Is the due date wrong?

Check what the contact cadence is according to Recidiviz vs what the user
reports. With the information from T-i and T-ii you can determine whether our
due_date logic is faulty. If the cadence looks correct but the due date is
wrong, there may be an issue with our due date calculation logic.

#### T-v: Frontend record doesn't match eligibility spans

The Tasks frontend record view
(`workflows_views.<state_code>_supervision_tasks_record_v2_materialized`)
applies additional filtering on top of the compliance task eligibility spans:

- **Due date window**: Only shows tasks with `due_date` within the past
  (overdue) or upcoming 2 months.

Read the record view in
`recidiviz/calculator/query/state/views/workflows/firestore/<state_code>_supervision_tasks_record*.py`
to understand the exact filtering logic.
