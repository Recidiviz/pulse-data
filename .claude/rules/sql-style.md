# SQL Style

These rules apply whenever writing SQL — a BigQuery view definition, an ad-hoc
query, a SQL fragment in a config — and whenever writing code, in Python or any
other language, that generates SQL. When the generating code is Python, the
rules in [`python-style.md`](./python-style.md) apply too. For testing
SQL-generating code, see [`python-testing-style.md`](./python-testing-style.md).

## Name output columns once, in a constant

A column produced by one query (`SELECT … AS <name>`) is usually read again — by a downstream query, an output schema, or a test — so inlining the name as a raw string in each place lets a rename silently break the others (the SQL fails only at query time, if at all). Define the name once and reference it everywhere. A module-level constant is fine; use a class variable when there is a builder object it naturally belongs to. Don't bother for a name that lives entirely inside a single query string and is never referenced elsewhere.

```python
class AssignmentsByTimePeriodViewBuilder(...):
    ASSIGNMENT_START_DATE_COLUMN_NAME = "assignment_start_date"

    @classmethod
    def _build_query(cls, ...) -> str:
        return f"""
        SELECT assignment_date AS {cls.ASSIGNMENT_START_DATE_COLUMN_NAME}
        FROM assignment_sessions
        """

    @classmethod
    def _build_downstream_query(cls, ...) -> str:
        col = cls.ASSIGNMENT_START_DATE_COLUMN_NAME
        return f"SELECT {col}, COUNT(*) FROM (...) GROUP BY {col}"
```

## Assembling SQL templates in Python

Assemble multi-line SQL templates with `fix_indent(...)` (from `recidiviz.utils.string_formatting`) rather than hand-managing indentation in raw multi-line strings. `fix_indent(s, indent_level=N)` dedents `s`, strips surrounding whitespace, and re-indents every line to `N` spaces while preserving the relative indentation between lines. Its main value is composing templates: a fragment built elsewhere arrives with its own indentation, and `fix_indent(fragment, indent_level=N)` re-indents it so it nests cleanly inside the parent query. Prefer shared clause helpers such as `nonnull_end_date_clause` (from `recidiviz.calculator.query.bq_utils`) over hand-written SQL snippets.

```python
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.utils.string_formatting import fix_indent

# A subquery built elsewhere, formatted at its own (left-margin) indentation.
time_periods_query = time_period.build_query()

query_template = f"""
WITH time_periods AS (
{fix_indent(time_periods_query, indent_level=4)}
),
assignment_sessions AS (
    SELECT
        *,
        {nonnull_end_date_clause("end_date_exclusive")} AS end_date_nonnull
    FROM assignments
)
SELECT * FROM time_periods JOIN assignment_sessions USING (person_id)
"""
return fix_indent(query_template, indent_level=0)
```

Here the inner `fix_indent(time_periods_query, indent_level=4)` re-indents the embedded subquery to sit four spaces deep inside the `time_periods` CTE, and the outer `fix_indent(query_template, indent_level=0)` strips the surrounding blank lines and normalizes the whole template to a clean left margin.

## Merging an authoritative source with a fallback: branch on presence, not `COALESCE`

When merging values from two sources where one is authoritative, do not use `COALESCE(authoritative, fallback)`: it silently substitutes the fallback whenever the authoritative value is a deliberate NULL. Join so the query can tell whether an authoritative row matched, and branch on that presence:

```sql
SELECT
    raw.person_external_id,
    -- Bad: when a canonical row matched but its facility is a deliberate NULL,
    -- COALESCE silently falls back to the raw value.
    COALESCE(canonical.facility, raw.facility) AS facility_bad,
    -- Good: fall back only when no canonical row matched at all, so a NULL
    -- facility on a matched canonical row stays NULL.
    IF(canonical.person_external_id IS NULL, raw.facility, canonical.facility) AS facility
FROM raw_movements raw
LEFT JOIN canonical_movements canonical
    USING (person_external_id, movement_date)
```

`COALESCE` remains the right tool when NULL genuinely means "no value here, take the next one", e.g. defaulting a nullable column with `nonnull_end_date_clause`. This rule targets merges where a NULL from the authoritative source is meaningful data.

## Know the full primary key of every table you join

A join or `SELECT` on a subset of a table's primary key produces more rows per key than the consumer expects. Before joining, check the source table's full primary key; if you only join on a subset of it, deduplicate deliberately or aggregate first.

```sql
-- Bad: sentences is keyed on (person_id, sentence_id, status_date) — one row
-- per status change. Joining on (person_id, sentence_id) alone fans out a row
-- per status_date, silently multiplying the result.
SELECT charges.*, sentences.sentence_length_days
FROM charges
JOIN sentences USING (person_id, sentence_id)

-- Good: collapse sentences to the join key's grain first, picking the row
-- deliberately.
SELECT charges.*, latest_sentences.sentence_length_days
FROM charges
JOIN (
    SELECT *
    FROM sentences
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id, sentence_id ORDER BY status_date DESC
    ) = 1
) latest_sentences
USING (person_id, sentence_id)
```

## Never join two `@ALL` raw tables directly

A raw data table's `@ALL` view contains every historical version of each row, one per `update_datetime`. Joining two `@ALL` tables on entity keys alone pairs rows from unrelated snapshots. Filter each side to a single snapshot, or make `update_datetime` part of the join.

## Compare nullable columns with `IS DISTINCT FROM`

`!=` and `=` return NULL when either side is NULL, so a `WHERE a != b` filter silently drops rows where either column is NULL. Use `IS DISTINCT FROM` / `IS NOT DISTINCT FROM` when either side can be NULL.

```sql
-- Bad: drops any row where old_status or new_status is NULL.
WHERE old_status != new_status

-- Good: NULL is compared like any other value.
WHERE old_status IS DISTINCT FROM new_status
```

## Null out invalid column values instead of dropping the row

A per-column validity filter in a `WHERE` clause silently drops the whole row. When the row should survive with one bad column, null out that column with `IF(...)`/`CASE` in the `SELECT` instead.

```sql
-- Bad: a person with an unparseable birth_date_str disappears entirely.
SELECT person_id, PARSE_DATE('%Y-%m-%d', birth_date_str) AS birth_date
FROM raw_persons
WHERE SAFE.PARSE_DATE('%Y-%m-%d', birth_date_str) IS NOT NULL

-- Good: the row survives; only the unparseable column becomes NULL.
SELECT person_id, SAFE.PARSE_DATE('%Y-%m-%d', birth_date_str) AS birth_date
FROM raw_persons
```

## Keep view queries deterministic

- No `ANY_VALUE` to pick an arbitrary row's value — pick deliberately with explicit ordering (`QUALIFY ROW_NUMBER() OVER (...)`) or aggregate.
- No `CURRENT_DATE`/`CURRENT_TIMESTAMP` in ingest view queries — results must be reproducible for a given input.
- No `ORDER BY` in a view query outside a window function — it adds cost and guarantees nothing to consumers.

## Run cheap filters first; optimize only when measured

Write the direct, obviously correct query first; optimize once measured runtime shows the need. When you do optimize: cheap row-narrowing filters run first, and regex or full-text scans run only on surviving rows in a later stage; single-table predicates belong in `WHERE` or a pre-filtered subquery, not in a `JOIN ... ON`.

## Do not trust a column's name

A column named for one condition may be built from a broader or different one. Before filtering or joining on a column produced by an upstream query, read the SQL that produces it.

## Exported views have a frozen public contract

Any view referenced in a metric export config is consumed outside this codebase: its view id and output column names cannot change. When internals are renamed, alias them back to the public name in the outermost `SELECT`.

## Every view outputs a `state_code` column

Every deployed BigQuery view outputs a `state_code` column, so consumers can always scope results to one state. A test will fail if the view does not have a `state_code` column and you should only add an exemption if you have a valid reason. 

## `SAFE.` functions must name what they absorb

`SAFE.` silently converts failures to NULL. Use it only for a known, specific class of bad values, named in a comment on that expression; otherwise omit `SAFE.` and let the bad value fail the query loudly. Ask: "what happens if you don't use SAFE?"

```sql
-- Rows ingested before the 2019 system migration store AdmissionDate as an
-- empty string rather than a date; SAFE lets exactly those through as NULL.
SELECT SAFE.PARSE_DATE('%Y-%m-%d', AdmissionDate) AS admission_date
FROM raw_admissions
```

## Date bounds: prefer exclusive ends, and name the end column for its bound

When a query produces a start and end bound for a span, make the end bound exclusive wherever possible — every range and interval type in this codebase uses an exclusive upper bound, and exclusive ends compose cleanly: adjacent spans meet at a shared boundary value with no ±1-day arithmetic, and `DATE_DIFF(end, start, DAY)` is the span's length. Whichever bound a query produces, the end column's name must say which it is — `end_date_exclusive` or `end_date_inclusive`, never a bare `end_date` that forces every consumer to guess whether the last day is in the span. When an upstream source hands you an inclusive end, prefer converting it at that boundary; keep it inclusive only when an external contract requires it, and then the `_inclusive` suffix is mandatory.

```sql
-- Bad: a bare end_date — is the last day in the span? Every consumer guesses.
SELECT person_id, start_date, last_active_date AS end_date
FROM raw_spans

-- Good: convert the inclusive raw bound at the boundary and name the result.
SELECT
    person_id,
    start_date,
    DATE_ADD(last_active_date, INTERVAL 1 DAY) AS end_date_exclusive
FROM raw_spans
```
