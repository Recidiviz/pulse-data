# Looking Up Person IDs

Use the external IDs from the PII doc to look up person_ids via the
`state_person_external_id` table:

```sql
SELECT person_id, external_id, id_type, state_code
FROM `recidiviz-123.normalized_state.state_person_external_id`
WHERE state_code = '<STATE_CODE>'
  AND external_id IN ('<EXT_ID_1>', '<EXT_ID_2>')
```

**If that returns nothing, retry ignoring leading zeros** — and only then:

```sql
SELECT person_id, external_id, id_type, state_code
FROM `recidiviz-123.normalized_state.state_person_external_id`
WHERE state_code = '<STATE_CODE>'
  AND COALESCE(NULLIF(LTRIM(external_id, '0'), ''), external_id)
      IN (COALESCE(NULLIF(LTRIM('<EXT_ID_1>', '0'), ''), '<EXT_ID_1>'),
          COALESCE(NULLIF(LTRIM('<EXT_ID_2>', '0'), ''), '<EXT_ID_2>'))
```

Most states store these IDs zero-padded (`US_TX_TDCJ`, `US_TN_DOC` and
`US_MI_DOC` are 100% padded) while reporters write the number as it appears on
screen, so an exact comparison misses the person entirely. The `LTRIM` wrapper
strips leading zeros from both sides; it matches `zero_stripped()` in
`recidiviz/case_triage/edovo/external_id_matching.py`, which is what the
automated agent uses.

**Run the exact query first and only fall back on an empty result.** US_MI
issues both `US_MI_DOC` (padded to 7) and `US_MI_DOC_ID` (unpadded), and their
values collide once zeros are stripped — stripping up front turns 74.7% of
otherwise unambiguous US_MI lookups into multi-candidate ones. Strip only
*leading* zeros (a plain `TRIM` would wrongly match `100` to `1000`) and leave
an all-zeros ID alone rather than collapsing it to an empty string, which is
what the `NULLIF`/`COALESCE` pair does.

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
