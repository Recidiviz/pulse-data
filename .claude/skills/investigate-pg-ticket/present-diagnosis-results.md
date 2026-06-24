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

The **TLDR** is a 2-3 sentence plain-language summary of the diagnosis that
anyone (including non-engineers) can understand. It should answer: what was
wrong, why it happened, and whether it's fixed. Avoid jargon, table names,
or implementation details in the TLDR.

Then present the full details in the following format:

- **Ticket title and link**
- **State code**
- **Officer/user**
- **Affected clients**
- **Problem description**
- **Screenshots** — note if the PII doc contains screenshots or image references
  related to this ticket (the Google Docs API text extraction won't include
  images, so mention that there may be screenshots in the doc and suggest the
  user check the doc directly if visual context would help)

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

**CRITICAL:** Always run every query using `bq query` BEFORE including it. Never
infer, guess, or hallucinate SQL syntax. If you're unsure about JSON extraction
or complex field parsing, run a simpler query first to understand the data
structure. Do not include query templates or example queries — only queries you
have actually executed and can show real results for.
