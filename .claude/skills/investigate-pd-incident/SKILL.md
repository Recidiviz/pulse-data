---
name: investigate-pd-incident
description: Investigate a PagerDuty incident by fetching it via the PagerDuty MCP server, classifying the alert type, and dispatching to the appropriate specialist skill. Use when the user provides a PagerDuty incident URL or ID (e.g. "investigate this PD alert", "what's going on with incident Q09D28GV4W96FQ", "diagnose https://recidiviz.pagerduty.com/incidents/...").
---

# Skill: Investigate PagerDuty Incident

## Overview

This is the generic entry point for triaging any PagerDuty incident. It pulls
the incident details via the PagerDuty MCP server, classifies what kind of alert
fired, dispatches to a specialist skill that knows how to diagnose that class of
failure, and then (after user confirmation) posts the full diagnosis back to the
PagerDuty incident as a note so the same context is visible to anyone else
triaging — including future automated invocations of this skill.

The only write action this skill takes is adding a note to the incident, and
only after confirmation. Never acknowledge, resolve, or snooze the incident
unless the user explicitly asks.

## Step 1: Verify the PagerDuty MCP server is connected

The PagerDuty MCP tools are named `mcp__pagerduty__*`. If those tools are not
available in this session, halt and tell the user:

> "This skill needs the PagerDuty MCP server. Install it with:
>
> ```
> claude mcp remove pagerduty 2>/dev/null; \
> claude mcp add --transport http pagerduty https://mcp.pagerduty.com/mcp \
>   --header "Authorization: Token YOUR_PD_API_TOKEN"
> ```
>
> Get a User API token at PagerDuty → Profile → My Profile → User Settings →
> Create API User Token. Then restart Claude Code and re-run this skill.
>
> Do **not** use the `/mcp` dialog's `Authenticate` button — the hosted PD MCP
> does not support OAuth dynamic client registration, which is what Claude
> Code's built-in auth flow requires."

## Step 2: Extract the incident ID

Accept either:

- A full URL like `https://recidiviz.pagerduty.com/incidents/Q09D28GV4W96FQ` —
  the ID is the last path segment
- A bare incident ID (e.g. `Q09D28GV4W96FQ`)
- An incident number (e.g. `26962`) — the MCP accepts this too

## Step 3: Fetch the incident

Call `mcp__pagerduty__get_incident` with the ID. Also call
`mcp__pagerduty__list_alerts_from_incident` with `query_model={"limit": 10}` —
the alerts carry richer detail than the incident itself. If the alert body's
`details` field is non-empty, inspect it. For email-integration alerts it will
usually be empty; that's expected.

## Step 4: Classify the alert

Use the **incident title** and **service summary** to decide which specialist to
invoke. Current dispatch table:

| Match | Specialist |
|---|---|
| Title matches the regex `^Failure: Task Run: (?P<dag_id>[\w-]+)\.(?P<task_id>\w+), started: (?P<start>.+)$` **and** service summary contains `Airflow Tasks` | Read and follow `.claude/skills/investigate-airflow-failure/SKILL.md` — pass through the parsed `dag_id`, `task_id`, and incident `start` datetime |
| _(anything else)_ | Skip Step 5 and go directly to Step 6 |

The Airflow alert title format is produced by
`AirflowAlertingIncident.unique_incident_id` in
`recidiviz/airflow/dags/monitoring/airflow_alerting_incident.py`. If that format
changes and the regex stops matching, update both files.

## Step 5: Post the diagnosis as a PagerDuty note (specialist path only)

Runs only after a specialist was dispatched in Step 4 and returned a diagnosis.
Skip this step when no specialist matched — there is nothing to post.

### 5a: Draft the note

Take the same structured diagnosis you presented to the user and reformat it
into a PagerDuty note. Include **all** of the sections the specialist produced
— not just the TLDR — because this note may be the only context available to
a future reader (another on-caller, or another Claude invocation running
headlessly).

Target sections (in order), using Markdown bold headings rather than `#` — PD
renders notes with limited Markdown and nested fenced blocks render poorly:

1. A one-line banner: `🔍 Claude Code diagnosis — <specialist-name>`
2. `**TLDR**` — 2–3 sentences in plain English
3. `**Failure**` — DAG, task, run id, project, Airflow UI URL (one line each)
4. `**Error**` — wrap traceback header + innermost Recidiviz frame + exception
   message in a single fenced code block
5. `**Source**` — `file_path:line` reference followed by a ≤10-line code
   block of the implicated function
6. `**Recent changes to this file**` — bulleted list: `<short sha> <date> —
   <title>` with a short "(relevant: …)" or "(no obvious connection)" note
7. `**Suggested next steps**` — bullet list of concrete actions
8. A `---` separator, then an attribution line: `🤖 Posted by Claude Code via
   the 'investigate-pd-incident' skill. This is a diagnosis, not a fix.`

Keep it faithful to what you already told the user — do not soften, expand
speculation, or omit the error excerpt. Trim purely repetitive log noise
(e.g. the duplicated `reason: 'stopped'` lines) but keep the first real
error intact. If the session's diagnosis is already in that shape, you can
mostly copy it verbatim.

### 5b: Preview and confirm

Show the drafted note to the user in a fenced block and ask whether to post,
edit, or skip. Do not post without explicit confirmation — `add_note_to_incident`
is a write action and the note is visible to everyone triaging the incident.

### 5c: Post the note

On user confirmation, call `mcp__pagerduty__add_note_to_incident` with the
incident id and the drafted content. Return the URL of the incident (not the
note — the PD API does not expose a direct note URL) so the user can click
through to verify.

If the user declines, leave the incident alone and tell the user the note
content is preserved in the session transcript if they want to copy it
manually.

## Step 6: No specialist matched

If no specialist matches, present a structured summary and stop:

- **Incident**: `[#<number>] <title>` — link to the PD URL
- **Service**: `<service.summary>`
- **Status**: `<status>` (triggered / acknowledged / resolved)
- **Created**: `<created_at>` UTC
- **First alert body** (if non-empty): dump the `details` JSON
- **Recent notes**: from `mcp__pagerduty__list_incident_notes` if any

Then tell the user:

> "No specialist skill exists for this alert type yet. If this class of alert
> comes up regularly, we should add a specialist under
> `.claude/skills/investigate-<type>-failure/SKILL.md` and register it in the
> dispatch table of `investigate-pd-incident/SKILL.md`."

## Notes

- **Write action**: the only write tool this skill calls is
  `mcp__pagerduty__add_note_to_incident`, and only with user confirmation in
  Step 5. Never call `manage_incidents` (ack/resolve/snooze) or any other
  write tool unless the user explicitly asks.
- **Pre-approval of add_note_to_incident is intentionally off**: it is *not*
  in `.claude/settings.json`'s allowlist, so every post surfaces a permission
  prompt — a second confirmation layer on top of the skill's own Step 5b
  confirmation. Do not add it to the allowlist without team discussion.
- **Never include PD tokens in command output you surface to the user** — they
  live in `~/.claude.json` and should stay there.
- **Future headless use**: if this skill is ever invoked non-interactively
  (cloud job, cron, etc.), the interactive confirmation in Step 5b will block.
  That mode will need an explicit mechanism (caller-provided flag or
  pre-approved settings) before it can post autonomously — do not work
  around the confirmation in local sessions.

## Related Documentation

- [Investigate Airflow Failure](../investigate-airflow-failure/SKILL.md) — the
  specialist for Airflow task failures
- [airflow_alerting_incident.py](../../../recidiviz/airflow/dags/monitoring/airflow_alerting_incident.py) —
  source of the PD alert title format
