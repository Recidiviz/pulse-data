---
name: find_recidiviz_implicated_people
description:
  Find people implicated by Recidiviz tools, in two modes. Cohort mode — given a
  description of a tool-engagement pattern (e.g. "people viewed in Workflows for
  early discharge AND granted", "people registered for the JII tablet app",
  "people whose officer was flagged as an outlier during their supervision"),
  produce a SQL CTE that emits the matching person set. Individual mode — given
  a `(state_code, person_id)`, summarize which Recidiviz tools implicated them
  and how (surfaced/viewed/granted, login counts, PSI case view dates, outlier
  officer flags).
---

# Skill: Find Recidiviz-Implicated People

Maps between people and the Recidiviz products that touched them. Two modes
share one product catalog.

- **Cohort mode** ("who?") — given a tool-engagement description, emit a SQL CTE
  that returns the matching person set. Drop-in for the
  `implicated_external_ids` CTE in
  [find_case_note_anecdotes](../find_case_note_anecdotes/SKILL.md).
- **Individual mode** ("what?") — given a `(state_code, person_id)`, summarize
  every product they (or someone on their behalf) interacted with. Drop-in for
  the "Implication evidence" section of an anecdote report.

Both modes are backed by the same per-product table catalog (below). Pick mode
by what the caller is asking for — they often want both in sequence (define a
cohort, then drill into a specific person).

---

## Product catalog

Each subsection: which table, what to filter on, mode-A cohort idiom, mode-B
drill-down. The Workflows funnel is the most common entry point; PSI and
Insights/Outliers are the two with NULL `person_id` and require bridges.

### Workflows funnel (Workflows + Tasks + Client Page + Insights — unioned)

**Table**:
`{project_id}.analyst_data.workflows_person_impact_funnel_status_sessions_materialized`
(source:
[`workflows_person_impact_funnel_status_sessions.py`](../../../recidiviz/calculator/query/state/views/analyst_data/workflows_person_impact_funnel_status_sessions.py)).
One row per `(state_code, person_id, task_type)` per time-bounded eligibility
session; session resets on eligibility change or new justice-involvement
session.

**Key columns**: `task_type` (e.g. `EARLY_DISCHARGE_FROM_SUPERVISION`; full enum
in `task_eligibility.all_completion_events_materialized`), `is_eligible`,
`surfaced`, `viewed`, `form_viewed` / `form_started` / `form_submitted`,
`marked_ineligible`, `task_completed`, `start_date`, `end_date_exclusive`.

**Note**: `viewed` is unioned across product surfaces (Workflows / Tasks /
Client Page / Insights) — see Tasks subsection if product-of-view matters.

**Cohort idiom — three variants of implication**:

```sql
-- 1. Eligible only
WHERE task_type = '<task_type>' AND is_eligible

-- 2. Tool-engaged
WHERE task_type = '<task_type>'
  AND (surfaced OR viewed OR form_viewed OR form_started OR form_submitted)

-- 3. Granted-after-engagement, same session
WHERE task_type = '<task_type>'
  AND (surfaced OR viewed) AND task_completed
```

**Individual drill-down**:

```sql
SELECT start_date, end_date_exclusive,
       is_eligible, surfaced, viewed, form_viewed, form_started, form_submitted,
       task_completed, marked_ineligible, denial_reasons
FROM `{project_id}.analyst_data.workflows_person_impact_funnel_status_sessions_materialized`
WHERE state_code = '<state_code>' AND person_id = <person_id>
  AND task_type = '<task_type>'
ORDER BY start_date
```

### Tasks tool (means two different things — pick the right one)

`product_type = 'TASKS'` appears in two unrelated contexts. Word the writeup
carefully.

| Claim                                                                                            | Source                                                                                                                          | Phrasing                                                                            |
| ------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| Tasks-tool **contact-compliance** preview (home visit, drug test, employment verification, etc.) | `segment_events.frontend_tasks_previewed_materialized` (`task_types` JSON array)                                                | "Previewed in the Tasks tool"                                                       |
| **Workflows opportunity** previewed via the Tasks UI surface (URL `/workflows/tasks/*`)          | `segment_events.frontend_opportunity_previewed_materialized` filtered to `product_type = 'TASKS'` + specific `opportunity_type` | "Workflows opportunity preview, rendered via Tasks UI" — NOT "viewed in Tasks tool" |

```sql
-- Contact-compliance preview (any category)
SELECT DISTINCT state_code, person_id
FROM `{project_id}.segment_events.frontend_tasks_previewed_materialized`
-- Narrow to a category: AND REGEXP_CONTAINS(task_types, r'"homeVisit"')

-- Workflows opportunity preview rendered on Tasks URL
SELECT DISTINCT state_code, person_id
FROM `{project_id}.segment_events.frontend_opportunity_previewed_materialized`
WHERE product_type = 'TASKS' AND opportunity_type = '<workflows_opportunity_type>'
```

### PSI (PSI Case Insights tool, pre-sentencing)

Touches the person _before_ they're in the correctional system, so segment
events carry NULL `person_id`. Events keyed by an internal PSI case ID in the
URL path. Bridge via `sentencing_views.sentencing_client_record_materialized`
(`case_ids` is a JSON array — `UNNEST(JSON_VALUE_ARRAY(case_ids))`).

**State coverage is narrow** — primarily US_IX, US_ND, light US_MO. Profile
before assuming candidates exist. **Match-rate is lossy** — cases that didn't
result in a sentence won't have a usable `external_id`.

```sql
-- Cohort: people whose PSI case was viewed during pre-sentencing
SELECT DISTINCT pei.state_code, pei.person_id
FROM `{project_id}.segment_events.frontend_sentencing_case_details_page_viewed_materialized` v
JOIN (
  SELECT scr.state_code, scr.external_id, cid AS case_id
  FROM `{project_id}.sentencing_views.sentencing_client_record_materialized` scr,
       UNNEST(JSON_VALUE_ARRAY(scr.case_ids)) AS cid
) pc ON v.state_code = pc.state_code
     AND REGEXP_EXTRACT(v.context_page_path, r'/case/([^/?#]+)') = pc.case_id
JOIN `{project_id}.normalized_state.state_person_external_id` pei
  ON pei.state_code = pc.state_code AND pei.external_id = pc.external_id
WHERE pei.id_type = '<id_type>'  -- match the consuming context's id_type
```

`frontend_sentencing_case_details_page_viewed` is the broadest "PSI viewed"
signal. ~14 narrower events exist (recommendation creation, summary copy, etc.)
— use those when the claim needs a more specific action.

### JII tablet app (incarceration-only)

Three views, all keyed by internal `person_id`. `system_type` is always
`INCARCERATION`.

1. **Registration sessions** —
   `{project_id}.analyst_data.jii_tablet_app_provisioned_user_registration_sessions_materialized`.
   One row per `(state_code, person_id)` per provisioning window.
   `is_registered` (BOOL) is the primary "registered for the app" signal.
2. **Login events** —
   `{project_id}.observations__jii_tablet_app_provisioned_user_event.jii_tablet_app_user_login_materialized`.
   One row per login (`event_date`, `state_code`, `person_id`).
3. **Page-view events** —
   `{project_id}.observations__jii_tablet_app_provisioned_user_event.jii_tablet_app_user_page_view_materialized`.
   Adds `context_page_path` for in-app feature attribution.

```sql
-- Cohort: provisioned and logged in at least once
SELECT DISTINCT state_code, person_id
FROM `{project_id}.analyst_data.jii_tablet_app_provisioned_user_registration_sessions_materialized`
WHERE is_registered

-- Cohort: logged in at least N times in a window
SELECT state_code, person_id, COUNT(*) AS login_count
FROM `{project_id}.observations__jii_tablet_app_provisioned_user_event.jii_tablet_app_user_login_materialized`
WHERE event_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 365 DAY)
GROUP BY 1, 2 HAVING login_count >= 5
```

JII coverage is sparse — the app is live in select states / select facilities.

### Insights / Outliers tool (officer-keyed, second-order signal)

Events (`SUPERVISOR_HOMEPAGE_*`) are keyed to **officers, not JIIs**. Don't
claim "the tool surfaced this person." The correct framing is **"the JII's
assigned supervising officer was flagged as an outlier during their supervision
period."** Bridge through `sessions.supervision_officer_sessions_materialized`
so the implication window aligns with when that officer actually supervised the
JII.

```sql
WITH person_officer AS (
  SELECT person_id, state_code, supervising_officer_external_id,
         start_date, end_date_exclusive
  FROM `{project_id}.sessions.supervision_officer_sessions_materialized`
  WHERE state_code = '<state_code>'  -- and optional person_id filter
)
SELECT po.person_id, po.start_date AS officer_start, po.end_date_exclusive AS officer_end,
       os.officer_id, os.metric_id, os.period, os.end_date AS metric_end_date, os.status
FROM person_officer po
JOIN `{project_id}.outliers_views.supervision_officer_outlier_status_materialized` os
  ON po.state_code = os.state_code
 AND po.supervising_officer_external_id = os.officer_id
WHERE os.status = 'FAR'  -- 'FAR' = flagged outlier; also 'NEAR', 'MET'
  AND os.end_date BETWEEN po.start_date AND COALESCE(po.end_date_exclusive, DATE '9999-12-31')
```

For narrower claims (officer-page-viewed-by-supervisor, action-strategy popups,
etc.) adapt the `frontend_outliers_*_materialized` segment-event tables; the
join shape is the same — bridge officer `external_id` to `pseudonymized_id` via
`outliers_views.supervision_officers_materialized`, then match URL paths.

### Other staff-side products (segment events with `person_id` populated)

Filter `product_type` on
`{project_id}.segment_events.all_segment_events_materialized` for these — same
shape as Workflows but no funnel-style sessionization:

- `SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE` (Insights Opportunities — same
  shape as Workflows; funnel `viewed` unions across it)
- `ROUTE_PLANNER` (Tasks sub-feature, `/workflows/tasks/route-planner`)
- `CLIENT_PAGE`, `CASE_NOTE_SEARCH`, `CASE_PLANNING_ASSISTANT`, `MILESTONES`
- `MEETINGS` — wired up but ~0 person-keyed events as of writing

**Products with NULL `person_id`** (staff dashboards / aggregate views — can't
carry per-person implication): `VITALS`, `PATHWAYS`, `PUBLIC_PATHWAYS`,
`LANTERN`, `SENTENCING_ASSESSMENT_REPORT`.

### State-system signals (not Recidiviz products, same join shape)

A state-system event (AET credit award, sentence modification, custody
downgrade) can play the same role as a Recidiviz-tool engagement — substitute
that event for the product filter, the rest of the bridge is identical.

---

## Mode A: cohort lookup

**Inputs**: human description of the pattern, state(s), optional temporal anchor
(`engagement before/after <date>` or "first engagement date" emitted as a
column), optional target column shape (defaults to `person_id`; can emit
`person_external_id` for case-notes joining).

**Output**: a SQL CTE ready to drop in as `implicated_external_ids` (or
similar). The CTE should:

1. Pick the right product subsection above
2. Apply the per-product cohort idiom
3. Bridge `person_id` → `person_external_id` if the caller's downstream query
   needs the state-specific external id (e.g. case notes)

**Bridge template** (PERSON_EXTERNAL_ID downstream):

```sql
implicated_external_ids AS (
  SELECT DISTINCT pei.external_id AS person_external_id,
                  MIN(<product_event_date>) AS first_engagement_date  -- optional
  FROM <product_table_filter_block> p
  INNER JOIN `{project_id}.normalized_state.state_person_external_id` pei
    ON pei.state_code = p.state_code AND pei.person_id = p.person_id
  WHERE pei.id_type = '<id_type from downstream YAML / context>'
  GROUP BY pei.external_id
)
```

**State-coverage check before running**: if the pattern is "JII tablet app
registered" but the state isn't live on JII, the CTE will return zero rows. Run
this kind of profile first (one-liner per product family):

```sql
SELECT state_code, COUNT(DISTINCT person_id) AS n
FROM `<product_table>`
WHERE <product_engagement_filter>
GROUP BY state_code
ORDER BY n DESC
```

If the requested state has zero (or near-zero) implicated people, tell the
caller the state isn't yet on the relevant product. Offer to (a) drop the
implication scoping, (b) switch states, or (c) confirm the gap is the answer.

---

## Mode B: individual lookup

**Inputs**: `(state_code, person_id)` — single or list. Optional time window.
Optional `depth`: `overview` (one row per product they touched) or `detail`
(drill into the highest-signal product with funnel state).

**Output (overview)**: one row per `(state_code, person_id, product)` plus
event-count and time bounds. Use this to surface a clean one-liner in a report
("appeared in `[WORKFLOWS, TASKS, JII_TABLET_APP]`").

```sql
WITH staff_side AS (
  -- Staff-facing tools with person_id populated: Workflows, Tasks, Route
  -- Planner, Client Page, Insights Opportunities, Case Note Search, CPA,
  -- Milestones. (PSI is also in this table but with NULL person_id —
  -- handled separately below.) `event_ts` is DATETIME — CAST to TIMESTAMP so
  -- the UNION matches the JII-side cast.
  SELECT state_code, person_id, product_type,
         MIN(CAST(event_ts AS TIMESTAMP)) AS first_event_ts,
         MAX(CAST(event_ts AS TIMESTAMP)) AS last_event_ts,
         COUNT(*) AS event_count
  FROM `{project_id}.segment_events.all_segment_events_materialized`
  WHERE person_id IS NOT NULL
    -- and: AND state_code = '<state_code>' AND person_id = <person_id>
  GROUP BY 1, 2, 3
),
psi_side AS (
  -- PSI: NULL person_id; bridge via PSI case_id in URL path.
  SELECT pc.state_code, pei.person_id,
         'PSI_CASE_INSIGHTS' AS product_type,
         MIN(CAST(v.event_ts AS TIMESTAMP)) AS first_event_ts,
         MAX(CAST(v.event_ts AS TIMESTAMP)) AS last_event_ts,
         COUNT(*) AS event_count
  FROM `{project_id}.segment_events.all_segment_events_materialized` v
  JOIN (
    SELECT scr.state_code, scr.external_id, cid AS case_id
    FROM `{project_id}.sentencing_views.sentencing_client_record_materialized` scr,
         UNNEST(JSON_VALUE_ARRAY(scr.case_ids)) AS cid
  ) pc ON v.state_code = pc.state_code
       AND REGEXP_EXTRACT(v.context_page_path, r'/case/([^/?#]+)') = pc.case_id
  JOIN `{project_id}.normalized_state.state_person_external_id` pei
    ON pei.state_code = pc.state_code AND pei.external_id = pc.external_id
  WHERE v.product_type = 'PSI_CASE_INSIGHTS'
    -- and: AND pei.id_type = '<id_type>' AND pei.person_id = <person_id>
  GROUP BY 1, 2, 3
),
jii_side AS (
  -- JII tablet app: separate observation table, keyed by person_id directly.
  SELECT state_code, person_id, 'JII_TABLET_APP' AS product_type,
         CAST(MIN(event_date) AS TIMESTAMP) AS first_event_ts,
         CAST(MAX(event_date) AS TIMESTAMP) AS last_event_ts,
         COUNT(*) AS event_count
  FROM `{project_id}.observations__jii_tablet_app_provisioned_user_event.jii_tablet_app_user_login_materialized`
  -- AND state_code = '<state_code>' AND person_id = <person_id>
  GROUP BY 1, 2
)
SELECT * FROM staff_side
UNION ALL SELECT * FROM psi_side
UNION ALL SELECT * FROM jii_side
```

**Output (detail)**: drill into a specific product using its individual
drill-down query (Workflows funnel SELECT \* is the most common). For
Insights/Outliers, remember it's second-order — phrase the result as "officer
was flagged" not "person was viewed."

**Note**: the overview query does _not_ cover Insights/Outliers (officer-keyed,
no JII `person_id` to join on). If the caller wants to know whether the person's
officer was an outlier, run the dedicated Insights/Outliers query above as a
separate step and append the result.

---

## Cross-cutting rules

**`person_id` ↔ `external_id` bridge**. Recidiviz internal `person_id` is an
INT64 unique to our system. Case-notes YAMLs and partner-state tables use
state-specific `external_id` (STRING). Bridge through
`{project_id}.normalized_state.state_person_external_id`, filtered to the
appropriate `id_type` (e.g., `US_IX_DOC`, `US_TN_DOC`, `US_CO_OFFENDERID`).
Per-state per-collection — confirm by reading the consuming context's YAML or
asking the caller.

**Temporal ordering**. When a downstream caller is looking for cases where an
interaction with a Recidiviz product occurred before an outcome, enforce the
outcome event postdate the tool engagement. Do not imply that any interaction
with a Recidiviz product _caused_ an outcome. Mode A should emit a
`first_engagement_date` (or per-product equivalent) column when the caller asks,
so the downstream filter can enforce `outcome_date >= first_engagement_date`.
Mode B reports observed history neutrally — temporal interpretation is the
caller's responsibility.

**Multiple `external_id` rows per person**. A person can have multiple rows in
`state_person_external_id` (different `id_type` values, or historical ids). The
bridge always needs both `state_code` and `id_type` to pick the right row.

**Don't query US_ME or US_CA data** — per CLAUDE.md, even when configs exist.

---

## Related

- [find_case_note_anecdotes](../find_case_note_anecdotes/SKILL.md) — the primary
  caller. Step 3.5 invokes this skill in cohort mode; the "Implication evidence"
  section of the anecdote report invokes it in individual mode.
- [workflows_person_impact_funnel_status_sessions.py](../../../recidiviz/calculator/query/state/views/analyst_data/workflows_person_impact_funnel_status_sessions.py)
  — source of the Workflows funnel view.
