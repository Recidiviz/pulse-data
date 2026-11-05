---
name: classification-ingest-gaps
description: Build a report of what a state needs to ingest before the classification diagnosis metrics work for it (the Initial Diagnosis tab of Looker dashboard 319, the score-vs-violence and AUC views, and the state-agnostic violence model). Use when the user asks what a state needs to ingest for classification, whether a state is ready for the classification diagnosis dashboard, or runs this skill with a state code.
---

# Classification ingest gaps

Produces one published HTML report per state, in exactly the format of
`example_us_az.html` in this directory. The report covers:

- **Three ingest changes unlock everything.** The numbered changes (three or fewer)
  that unlock every metric, as blocker cards.
- **Defining violence: [State] decides.** How the state should define `is_violent`,
  with precedents, candidate definitions, and questions for the state.
- **Field by field.** Every entity field the metrics read, what the state has in
  staging today, a status pill, and the action.
- **Metric by metric.** Each metric, where it lives, and which numbered change
  blocks it.
- **Things to know before mapping.** Data quirks found while profiling.

## Step 1: Get the state code

Take the state code from the skill arguments. If none was given, ask with
`AskUserQuestion` ("Which state should the classification ingest report cover?"),
offering a few likely states (e.g. US_AZ, US_IA, US_CO, US_NE) and letting the user
type another through Other. Normalize to `US_XX`.

- **US_ME and US_CA:** never query their data. Stop and tell the user; continue only
  if they explicitly confirm.
- **US_ID vs US_IX:** confirm which code the user means before querying.

## Step 2: Find the custody classification instrument

Run query 1 in `queries.md`. The custody classification is usually the RISK-class
`assessment_type` given in prison that has a scored `assessment_level` and the most
rows (e.g. `AZ_CUSTODY_CLASS`). If more than one RISK instrument could be the custody
classification (e.g. a custody score and an internal risk score of the same size),
ask the user which to diagnose. Note every other RISK instrument for the "Other ...
RISK instruments stay out" bullet.

If the state has no RISK-class assessment that could be a custody classification,
tell the user the state can't be diagnosed until one is ingested, and stop.

## Step 3: Profile the data

Run every query in `queries.md` for the state and instrument. Every number in the
report must come from these results. Do not reuse numbers from the AZ example.

Use today's date for the "queried on" date and for the 3-year lookbacks.

## Step 4: Decide each field's status

Use these pills and only these:

| Pill | Use when |
|---|---|
| Blocker | The field is empty or wrong and every downstream metric that needs it shows nothing for the state. Today: `assessment_decision` not APPROVED, `is_violent` never set. |
| Needed | Empty, but only some metrics need it, or the value is already in `assessment_metadata` and only needs mapping (override level, subtype). |
| Verify | Populated, but something looks off: out-of-range dates, no dismissal outcomes, scales that disagree. |
| Later | Not used by any current metric. |
| Ready | Populated well enough for every metric that reads it. |

Number the top-level changes (1, 2, 3) in the order of the AZ example: approve the
instrument, flag violent incidents, map overrides and reclassifications. Drop a
change the state doesn't need and renumber, and update the "Metric by metric" table's
"blocked by" column to match. If nothing is blocked, say so in the first section's
heading, e.g. "Nothing blocks [State]; two fields need checking", and keep the
blocker cards only for Needed or Verify items.

## Step 5: Write the violence definition section

This section stays even when `is_violent` is already set, since the state should
still confirm the definition. Keep, in this order:

1. The opening paragraph on why `is_violent` must reflect what the state cares about
   most, and that a provisional definition gets replaced by the state's choice.
2. The precedents table (TN, MI, IX), unchanged. Leave out the row for the state the
   report is about.
3. A paragraph on whether severity alone works for this state, citing the top
   most-severe incident types (query 4) and where its assaults and fights sit.
4. The candidate starting definitions table: a **Narrow** definition (violent types in
   the most severe class) and a **Broad** one (violent types in the two most severe
   classes), each listing its incident types with counts and a total.
5. One paragraph comparing the two (how many times more outcomes the broad one gives).
6. The "Questions for [State]" box, rewritten around this state's ambiguous types
   (fights, threats, weapons, riots, staff assaults by substance, attempts).

If the state already sets `is_violent`, also report what the current definition flags
and how many incidents, and frame the candidates as alternatives for the state to
weigh.

## Step 6: Build the page

Copy `example_us_az.html` to the scratchpad as `<state>_classification_ingest.html`
(e.g. `us_ia_classification_ingest.html`) and edit the copy:

- Keep the `<style>` block, the section order, the section headings' wording
  (swapping "Arizona"/"AZ" for the state's name and code), the tables' columns, and
  the pill classes exactly as they are.
- Change only state-specific content: the title, eyebrow, lede, blocker cards, table
  rows, numbers, metadata key names, instrument names, quirks, and footer date.
- `<title>`: `[ST] Classification Ingest Gaps` (e.g. `IA Classification Ingest Gaps`).
- Remove rows that don't apply (e.g. no metadata keys) rather than leaving
  placeholders. Never leave AZ content in another state's report.
- Write prose to the repo's writing-style rules: lead with the point, plain
  sentences, no em-dash asides.

The "Metric by metric" table's metrics and locations stay the same for every state
unless the dashboard or views change. If the user shares new screenshots of the
dashboard, add or update those rows.

## Step 7: Publish

Publish with the Artifact tool:

- `file_path`: the scratchpad copy from step 6.
- `icon`: `checklist`.
- `description`: "What [ST] needs to ingest for the dashboard 319 Initial Diagnosis
  tiles, the score-vs-violence views, and the state-agnostic violence model, checked
  against staging on [date]."

Then reply with the link, a note that the page is private until the user shares it,
and a short summary: the numbered changes, what's already ready, and anything flagged
to verify.

## Step 8: Offer to file tickets

After the summary, ask with `AskUserQuestion` whether to file tickets for the gaps:

- **Question:** "Should I file a Linear ticket for each of [ST]'s classification
  ingest gaps?"
- **Options:** "Yes, one ticket per gap", "No"

If the answer is No, stop.

If Yes, invoke the `create-pulse-data-issues` skill and follow it to file one
ticket per gap. A gap is every field with a **Blocker**, **Needed**, or **Verify**
pill in the "Field by field" table. Skip **Ready** and **Later** rows. Pass that skill
everything below so it doesn't have to ask again. Let it handle templates, labels,
team, and project, and confirm with the user before filing as it normally does.

For each ticket:

- **Title:** `[ST][Ingest][Classification] <the action>`, for example
  `[US_AZ][Ingest][Classification] Mark AZ_CUSTODY_CLASS assessments approved`.
- **Description:**
  - What needs to be done: the field, its entity, and the action from the report's
    Action column.
  - Why: which metrics it unlocks, from the "Metric by metric" table, and that the
    state currently appears in none of them if the gap is a Blocker.
  - Current state: the numbers from the "[ST] today" column (e.g. "0 of 1,401,071
    set"), and the metadata keys to map from when the value is already ingested.
  - A link to the published report.
- **The `is_violent` ticket** also lists the narrow and broad candidate definitions
  with counts, and the "Questions for [State]" from the report. Say plainly that the
  state chooses the definition, and that the provisional one is a starting point to
  replace.
- **Verify tickets** describe what looked off and the query that shows it, and ask
  for an investigation, not a fix.

File the Blocker tickets first, in the report's change order (1, 2, 3), then Needed,
then Verify. After filing, add each ticket's ID to the matching row's Action cell
in the report (e.g. "… (OBT-12345)"), republish the same file path, and reply with
the ticket links grouped by pill.

## Related files

- `example_us_az.html`: the reference report; reproduce its format exactly.
- `queries.md`: every profiling query, parameterized by state and instrument.
- `create-pulse-data-issues` skill: files the gap tickets in step 8.
- `recidiviz/calculator/query/state/views/sessions/approved_assessment_events.py`:
  the `assessment_decision = APPROVED` gate.
- `recidiviz/calculator/query/state/views/classification/metrics/`: the
  score-vs-violence, AUC, and violence model views whose inputs the report checks.
