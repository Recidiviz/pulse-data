## Present results of your diagnosis

**Keep it brief.** The SQL evidence and its results are the deliverable —
they're what proves the diagnosis. Surrounding prose should be minimal: the
TLDR, a one-line finding statement, and a one-line interpretation per query.
Don't restate query results in prose, don't pad with caveats or recaps, and
don't write dramatic framing. Let the evidence carry the diagnosis.

Begin the response with the **TLDR** heading itself. Do not write any preamble,
lede, or framing sentence before it — no "Here's what I found", no "This is
the smoking gun", no dramatic intros. The very first characters of the output
must be the TLDR heading.

The **TLDR** opens with the scope line, followed by a 2-3 sentence
plain-language summary of the diagnosis that anyone (including non-engineers)
can understand. The summary should answer: what was wrong, why it happened, and
whether it's fixed. Avoid jargon, table names, or implementation details in the
TLDR.

The scope line gives the counts from the **Scope** section below, so readers
can triage without scrolling:

```markdown
**Scope:** 412 clients across 37 POs in US_XX
```

Write `n/a` for a count that doesn't apply and `unknown` for one you couldn't
compute, matching what Scope reports.
Run the Scope query before writing the TLDR.

Then present the full details in the following format:

- **Ticket title and link**
- **State code**
- **Officer/user**
- **Affected clients**
- **Problem description**
- **Screenshots** — note if the ticket's private PII doc contains screenshots or
  image references (the Google Docs API text extraction won't include images, so
  mention that there may be screenshots in the doc and suggest opening the doc
  linked at the top of the ticket if visual context would help)

- **Diagnosis** — include findings in order and with all relevant details.
  **For EACH key finding, include SQL evidence:**

  Structure each finding as:

  1. **Finding statement** (what you discovered)
  2. **SQL Evidence** - The EXACT query you ran and its results (copied directly
     from bq output)
  3. **Interpretation** (what it means)

  **Put the query in a `<details>` block** so the finding and results stay
  scannable and the SQL doesn't dominate. Put the query inside the dropdown and
  keep its results visible outside it.

  Format for SQL evidence blocks:

  ````markdown
  +-------------------+------------+-----------------+
  | supervision_level | start_date | termination_date |
  +-------------------+------------+-----------------+
  | MAXIMUM           | 2025-09-16 | 2025-12-18      |
  | MEDIUM            | 2025-12-19 | NULL            |
  +-------------------+------------+-----------------+

  <details><summary>Query</summary>

  ​```sql
  SELECT supervision_level, start_date, termination_date
  FROM `recidiviz-123.normalized_state.state_supervision_period`
  WHERE person_id = 4857115069950340732
  ​```
  </details>
  ````

- **Scope**: once you know the root cause, run one query counting how many
  clients in the state hit the same problem right now and how many distinct POs
  (supervising officers) those clients are assigned to, and report both counts
  with the query as SQL evidence. For client-level problems, get each client's
  current PO from `sessions.supervision_officer_sessions` (rows with
  `end_date_exclusive IS NULL`) and count distinct `staff_id`. For problems that
  affect officers rather than clients, such as a PO missing from Insights, count
  the affected POs and report clients as not applicable. Project only counts,
  never IDs. If the cause can't be expressed as a query, say so instead of
  guessing a number.

**CRITICAL:** Always run every query using `bq query` BEFORE including it. Never
infer, guess, or hallucinate SQL syntax. If you're unsure about JSON extraction
or complex field parsing, run a simpler query first to understand the data
structure. Do not include query templates or example queries — only queries you
have actually executed and can show real results for.
