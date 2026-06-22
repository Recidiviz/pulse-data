---
name: pollen-view-structure
description: >
  Use this skill when a user wants to structure BigQuery views into the standard Pollen view pattern. All Pollen analysis is in the research/notebooks/policy folder -- do not apply this skill outside that folder. Triggers include: "structure my views",
  "create view files", "pull views from my notebook", "set up the view folder structure",
  "make my SQL into view builders", or any request to convert notebook SQL or .sql files
  into the SimpleBigQueryViewBuilder pattern used by policy analysis work.
---

# Pollen View Structure Skill

Converts SQL queries (from notebooks, .sql files, or provided directly) into the standard
Pollen view structure. **Before writing any files, read
[`recidiviz/research/notebooks/policy/CLAUDE.md`](../../../recidiviz/research/notebooks/policy/CLAUDE.md)
for the full file templates and naming conventions.**

---

## Step 1: Gather Inputs

Use `AskUserQuestion` to determine the SQL source, then ask remaining details in prose:

```python
AskUserQuestion(questions=[{
    "question": "Where is the SQL coming from?",
    "header": "SQL source",
    "options": [
        {"label": "Existing notebook", "description": "Extract SQL cells from a .ipynb file"},
        {"label": "Existing .sql files", "description": "Migrate from a queries/ folder"},
        {"label": "Provide directly", "description": "I'll describe or paste the SQL"},
    ],
    "multiSelect": True
}])
```

Then ask (in prose) for anything not already provided:
- **Prefix** — e.g. `us_co_aet`, `us_ar_paa`
- **Folder path** — where `{state_code}/{policy_project}/views/` should be created (e.g. `recidiviz/research/notebooks/policy/us_co/aet/views/`)
- **File path(s)** — notebook and/or .sql directory path(s), if applicable

---

## Step 2: Extract View Candidates

If the source includes **Existing notebook**:

Read the notebook JSON. Find cells containing SQL (`SELECT`, `FROM`, `WITH`, `JOIN`):
- `read_gbq(...)` calls with embedded SQL
- Variables assigned to SQL strings (`query = """SELECT ..."""`)
- Repeated CTEs — good candidates for dedicated views

Present candidates with cell index, 1-sentence description, and suggested view name:

```
Found 4 potential views:

[Cell 3] compartment_sessions — Incarceration sessions joined to person IDs
[Cell 7] credits — Credit records with decoded credit type descriptions
[Cell 12] programs — Program enrollments with decoded status columns
[Cell 19] session_outcomes — Aggregates disciplinaries and earned time per session

Which would you like to promote? (say "all", list specific ones, or adjust)
```

Wait for confirmation before proceeding.

If the source includes **existing .sql files**, copy the queries into `{prefix}_views/` with appropriate renaming and editing. 

If the source includes **provide directly**, ask the user to describe their desired functionality and draw on existing queries to translate that intention into queries. 

---

## Step 3: Suggest Parameters

Scan each view's SQL for hardcoded values worth extracting as named constants:

| Pattern | Example | Suggested name |
|---------|---------|----------------|
| Date literals | `>= '2012-01-01'` | `_START_DATE` |
| Year thresholds | `>= 2018` | `_START_YEAR` |
| Score/rating cutoffs | `>= '2023-01-01'` | `_RATING_CUTOFF_DATE` |
| State codes | `= 'US_CO'` | leave as-is |
| Domain filter strings | `= 'INCARCERATION'` | leave as-is |

Present suggestions and wait for approval:

```
Found 2 values to potentially extract:

[compartment_sessions] L42: `>= '2012-01-01'`
  Suggested: _START_DATE = '2012-01-01'  →  used as >= '{_START_DATE}'
  Reason: AET enactment year — likely adjusted for sensitivity analysis

[ratings] L19: `>= '2023-01-01'`
  Suggested: _RATING_CUTOFF_DATE = '2023-01-01'  →  used as >= '{_RATING_CUTOFF_DATE}'
  Reason: Rating coding changed in 2022 — this date may need adjustment

Which would you like to extract? (say "all", list specific ones, or "none")
```

---

## Step 4: Write Files

Before writing, fetch the author's GitHub handle to populate the description warning:

```bash
gh api user --jq .login
```

Create:
1. `{state_code}/{policy_project}/views/__init__.py` — empty (if it doesn't already exist)
2. One `.py` per view — follow the individual view file template in `CLAUDE.md`, substituting
   the actual GitHub handle into the `WARNING: ... don't use without review from <github-handle>` line

SQL formatting: strip leading/trailing blank lines, preserve `--` inline comments, indent
consistently (2 or 4 spaces) within the triple-quoted string.

**Project ID**: use `{project_id}` in plain-string queries, `{{project_id}}` in f-string queries
(see CLAUDE.md for details).

---

## Step 5: Consistency Check + Compile

Verify before running:

- [ ] Every view file has `VIEW_BUILDER = SimpleBigQueryViewBuilder(...)` at top level
- [ ] `description=__doc__` (not a hardcoded string) in every builder
- [ ] `__init__.py` exists in the `views/` folder
- [ ] Views with extracted constants use `f"""..."""` and correct SQL quoting
- [ ] No hardcoded `recidiviz-staging.` — use `{project_id}.` or `{{project_id}}.`

Fix any failures, then verify views compile:

```bash
uv run python -m recidiviz.tools.pollen.refresh_scratch_views \
    --project_id recidiviz-staging \
    --state_code {STATE_CODE} \
    --policy_project {policy_project}
```

If the run fails:
- **Minor errors** (import typo, wrong constant name, missing comma, SQL syntax issues): fix directly and re-run.
- **Logic issues**: flag to the user with the specific error and affected view,
  and ask whether to fix or leave for manual resolution.

---

## Step 6: Present Results

```
Created {state_code}/{policy_project}/views/ with {n} views:
  - {view1}.py  →  view_id: {prefix}_{view1}
  - {view2}.py  →  view_id: {prefix}_{view2}
  ...

All views compiled successfully. To re-deploy all:
  uv run python -m recidiviz.tools.pollen.refresh_scratch_views \
      --project_id recidiviz-staging --state_code {STATE_CODE} --policy_project {policy_project}

To re-deploy a single view:
  uv run python -m recidiviz.tools.pollen.refresh_scratch_views \
      --project_id recidiviz-staging --state_code {STATE_CODE} --policy_project {policy_project} \
      --view_id {view_name}
```
