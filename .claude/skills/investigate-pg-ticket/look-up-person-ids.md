# Looking Up Person IDs

Use the external IDs from the PII doc to look up person_ids via the
`state_person_external_id` table:

```sql
SELECT person_id, external_id, id_type, state_code
FROM `recidiviz-123.normalized_state.state_person_external_id`
WHERE external_id IN ('<EXT_ID_1>', '<EXT_ID_2>')
  AND state_code = '<STATE_CODE>'
```

**Note on US_ID/US_IX:** This table stores `US_IX` for Idaho, not `US_ID`. If
the ticket is for US_ID, use `state_code = 'US_IX'` in this query (and all
subsequent queries against BQ datasets).

**Note on `id_type` ambiguity:** Tickets don't usually specify the `id_type`,
so the query above doesn't filter by it. In some cases, two `person_id` values
may be associated with the same `(state_code, external_id)` pair (different
`id_type`s). If this happens, disambiguate by querying `state_person` for the
candidate `person_id`s and matching `full_name` against the name in the
ticket's PII doc entry:

```sql
SELECT person_id, full_name
FROM `recidiviz-123.normalized_state.state_person`
WHERE person_id IN (<CANDIDATE_PERSON_IDS>)
```

Use the `person_id` whose `full_name` matches the ticket / PII doc for all
subsequent queries.
