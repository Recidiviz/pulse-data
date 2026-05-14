## Diagnosing Workflows Issues

#### W-i: Query the eligibility span

Query the TES for this person and opportunity using `bq query`:

```sql
SELECT person_id, start_date, end_date, is_eligible, is_almost_eligible,
       reasons, ineligible_criteria
FROM `recidiviz-123.task_eligibility_spans_<state_code>.<task_name>_materialized`
WHERE person_id IN (<PERSON_IDS>)
  AND CURRENT_DATE('US/Eastern') BETWEEN start_date
      AND IFNULL(end_date, '9999-12-31')
```

Check `is_eligible` and `is_almost_eligible` to determine eligibility status. If
the person is not in the table at all, check the candidate population for that
opportunity and understand why they are not even a candidate.

#### W-ii: Is their eligibility status wrong?

Look at the `reasons` and `ineligible_criteria` fields to identify which
criteria is failing. Then query the specific criteria view in
`task_eligibility_criteria_<state_code>.<criteria_name>_materialized` to
understand why. **Note:** Criteria table names do NOT include a state prefix —
e.g., the table is `meets_face_to_face_contact_triggers_materialized`, not
`us_ix_meets_face_to_face_contact_triggers_materialized` (even though the
dataset name contains the state code).

#### W-iii: What about the record view?

The opportunity record view
(`workflows_views.<state_code>_<opportunity_name>_record`) transforms TES data
for the frontend and may filter differently. Read the record view query in
`recidiviz/calculator/query/state/views/workflows/firestore/<state_code>_<opportunity_name>_record.py`
to understand the logic and check if the person appears there or if any relevant
fields (e.g., contact dates) look wrong.

Common causes of discrepancies between TES and the record view:

- **Eligibility subset filtering**: The record view may filter to
  `ELIGIBLE_ONLY` or `ELIGIBLE_AND_ALMOST_ELIGIBLE_ONLY`, excluding people in
  other states.
- **External ID join**: Record views INNER JOIN to `state_person_external_id`
  with a specific `id_type`. Anyone without a matching external ID won't appear.
- **State-specific joins**: Some record views add extra joins (e.g., assessment
  scores, case notes, raw data) that further filter results.
