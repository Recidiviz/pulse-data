---
name: create-workflow-epic
description:
  Create a complete workflow initiative with an epic, core workflow sub-issues,
  and launch preparation tasks. Use when the user asks to create a workflow
  epic, file a workflow task, or start a new workflow initiative.
---

# Skill: Create Workflow Epic

## Overview

This skill creates a comprehensive GitHub workflow initiative consisting of:

- An epic issue in the format `[US_XX][Workflow] {workflow_name}`
- 4 core workflow sub-issues (TES Spans, Opportunity Query, Validate Data, Admin
  Panel Config)
- 19 launch preparation sub-issues organized across 5 phases: Month Before, Two
  Weeks Before, Week Before, Day Of, and Week After

## User Interaction Model

This skill operates in two phases:

1. **Upfront Collection** (Step 1a): Gather all required information by asking
   one question at a time using AskUserQuestion. Each question should ask for
   exactly one piece of information. Since AskUserQuestion supports up to 4
   questions per call, you may batch up to 4 questions per call, but each
   individual question must ask for only one item.
2. **Autonomous Execution** (Steps 1c-3): File all 24 issues automatically after
   user approves the collected information. Show a summary preview before filing
   to confirm.

## Role-to-Handle Mapping

During Step 1a collection, map the following abbreviations to GitHub handles:

- **PM** = Product Manager handle
- **DA** = Data Analyst handle
- **SEM** = State Engagement Manager handle
- **SWE** = Software Engineer handle
- **TL** = Tech Lead handle

## Template Processing

When processing YAML templates in Steps 2 and 3, follow this unified process for
all templates:

1. **Read the Template**: Use the Read tool to load the YAML file
2. **Extract Fields**: Extract the `title` and `body` fields from the YAML
   structure
3. **Replace Placeholders in Both Title and Body**:
   - `{STATE_CODE}` → User-provided state code (e.g., US_ME, US_ID)
   - `{ACRONYM}` → The workflow acronym created in Step 1a
4. **Use the Populated Title**: The template's `title` field becomes the final
   issue title after placeholder replacement

## Instructions

> **Default for all sub-issues** (Steps 2–4): Labels are always
> `Team: State Pod` and `Region: {STATE_CODE}`. This is not repeated in each
> step below.

### Step 1: Create Epic

#### 1a. Gather All Information

Ask the user one question at a time for each piece of information. Use
AskUserQuestion with up to 4 questions per call, but each question should ask
for exactly one item. Collect the following details in order:

1. State code (e.g., US_ME, US_ID)
2. Workflow name
3. Objective
4. Policy doc link (optional)
5. PRD link (optional)
6. GitHub handle of the Product Manager (PM)
7. GitHub handle of the Data Analyst (DA)
8. **Include launch checklist sub-issues?** (Yes/No) - Whether to create the 19
   launch checklist tickets, or just the epic + 4 core sub-issues

If the user selects **Yes** for launch preparation sub-issues, also collect:

9. GitHub handle of the State Engagement Manager (SEM)
10. GitHub handle of the Software Engineer (SWE)
11. GitHub handle of the Tech Lead (TL)

After collecting the workflow name, create an acronym by taking the first letter
of each word (e.g., "Electronic Monitoring Withdrawal" → "EMW"). This acronym
will be used to prefix all sub-issues.

#### 1b. Load and Populate Template

Load `workflow-epic-template.yaml` and populate the placeholders:

- `{WORKFLOW_NAME}`: User-provided workflow name
- `{OBJECTIVE}`: User-provided objective
- `{POLICY_DOC_LINK}`: User-provided policy doc (or "Not provided")
- `{PRD_LINK}`: User-provided PRD (or "Not provided")

#### 1c. Preview and File the Epic

Before filing, show the user a preview of what will be created:

```
Ready to create workflow initiative with [5 or 24] total issues:
- 1 epic issue
- 4 core workflow sub-issues
[- 19 launch preparation sub-issues] (if Yes selected)

Epic:
  Title: [STATE_CODE][Workflow] WORKFLOW_NAME
  Assigned to: @PM_HANDLE, @DA_HANDLE
  Labels: Team: State Pod, Epic, Region: STATE_CODE

Core sub-issues assigned to: @DA_HANDLE
[Launch sub-issues assigned to: Various roles (PM, DA, SEM, SWE, TL)]
(if Yes selected)

Proceed? (yes/no)
```

Once confirmed, call the `create_recidiviz_data_issues` skill to create
the epic with:

- Title: `[{STATE_CODE}][Workflow] {workflow_name}`
- Body: The populated template body (include mentions of the DA and PM:
  `@{DA_HANDLE}` and `@{PM_HANDLE}`)
- Labels: `Team: State Pod`, `Epic`, `Region: {STATE_CODE}`
- Assign to: Both the DA and PM
- Use the correct `Epic` label (verified to exist via `gh label list`)

Return the epic issue number and URL for use in Steps 2 and 3.

### Step 2: Create Core Workflow Sub-Issues

Create the following 4 core sub-issues for the workflow using the templates:

1. `workflow-tes-spans.yaml`
2. `workflow-opportunity-query.yaml`
3. `workflow-validate-tes-data.yaml`
4. `workflow-admin-panel-config.yaml`

#### 2a. Load and Populate Sub-Issue Templates

Process each of the 4 core workflow templates in order using the Template
Processing procedure.

#### 2b. File Sub-Issues Using create_recidiviz_data_issues

For each of the 4 core sub-issues, call the `create_recidiviz_data_issues`
skill with:

- Title: The populated title from the template (e.g.,
  `[US_ME][Workflows] EMW - Create TES Spans`)
- Body: The populated sub-issue body from the template
- Parent issue: The epic created in Step 1
- Assign to: DA

Collect all 4 issue numbers and URLs. Return these for reference in later
steps. In particular, **save the TES Spans issue number** (from
`workflow-tes-spans.yaml`) — it is needed in Step 4 to set up blocking
relationships.

### Step 3: Create Launch Preparation Sub-Issues (Conditional)

**Only proceed with this step if the user selected "Yes" for launch preparation
sub-issues in Step 1a.**

Create launch preparation sub-issues for each phase of the workflow launch.
These issues track the pre-launch, launch, and post-launch activities needed to
successfully roll out the workflow.

#### Month Before

1. `workflow-launch-create-checklist.yaml` - PM
2. `workflow-launch-schedule-bug-bash.yaml` - PM
3. `workflow-launch-ensure-tt-calls.yaml` - DA
4. `workflow-launch-confirm-sso.yaml` - SWE
5. `workflow-launch-update-demo-mode.yaml` - SWE

#### Two Weeks Before

6. `workflow-launch-update-methodology-doc.yaml` - PM
7. `workflow-launch-write-bug-bash-doc.yaml` - PM
8. `workflow-launch-run-bug-bash.yaml` - PM
9. `workflow-launch-prioritize-bug-bash.yaml` - PM
10. `workflow-launch-provide-user-list.yaml` - SEM
11. `workflow-launch-ensure-users-created.yaml` - SWE
12. `workflow-launch-record-variants-prod.yaml` - SWE
13. `workflow-launch-record-variants-staging.yaml` - SWE

#### Week Before

14. `workflow-launch-update-launch-dates.yaml` - PM
15. `workflow-launch-confirm-users-prod.yaml` - SWE
16. `workflow-launch-add-feature-variants.yaml` - SWE

#### Day Of

17. `workflow-launch-impersonate-users.yaml` - PM
18. `workflow-launch-check-logs.yaml` - TL
19. `workflow-launch-confirm-working.yaml` - TL

#### 3a. Load and Populate Launch Sub-Issue Templates

Process each of the 19 launch preparation templates listed above using the
Template Processing procedure.

#### 3b. File Launch Sub-Issues Using create_recidiviz_data_issues

For each of the 19 launch sub-issues, call the
`create_recidiviz_data_issues` skill with:

- Title: The populated title from the template (e.g.,
  `[US_TX][Workflows][MB] EMW - Write Bug Bash Documentation`)
- Body: The populated sub-issue body from the template
- Parent issue: The epic created in Step 1
- Assign to: The role listed next to each template above, using the handle
  collected in Step 1a

Process templates sequentially by phase to maintain logical grouping. Collect
all 19 issue numbers and URLs.

### Step 4: Create Criteria Sub-Issues (Conditional)

Ask the user: "Would you like to create individual tickets for each eligibility
criteria? If so, provide a list of criteria names (one per line), or type 'no'
to skip."

If the user provides a list, for each criteria name call
`create_recidiviz_data_issues` with:

- Title: `[{STATE_CODE}][Workflows][Criteria] {ACRONYM} - {criteria_name}`
- Body: A brief description noting this is an eligibility criteria ticket for
  the workflow
- Parent issue: The epic created in Step 1
- Assign to: DA

After filing each criteria sub-issue, add a **blocking relationship** so the
TES Spans issue (from Step 2) is blocked by it. Use the `addBlockedBy` GraphQL
mutation:

```bash
# Get node IDs
TES_NODE_ID=$(gh issue view <tes_spans_number> --json id --jq '.id')
CRITERIA_NODE_ID=$(gh issue view <criteria_number> --json id --jq '.id')

# Mark the TES Spans issue as blocked by this criteria issue
gh api graphql -f query='
mutation {
  addBlockedBy(input: {
    issueId: "'"$TES_NODE_ID"'",
    blockingIssueId: "'"$CRITERIA_NODE_ID"'"
  }) {
    issue { number title }
    blockingIssue { number title }
  }
}'
```

This ensures the TES Spans issue shows all criteria as blockers that must be
completed first.

**Final Summary**

After all issues are created, provide a summary with:

- Epic issue number and URL
- Count of created issues by category. For example:
  - 1 epic issue
  - 4 core workflow sub-issues
  - 19 launch preparation sub-issues
  - X criteria sub-issues (if any)
- Note that sub-issues are linked to their respective parent issues

## Related Documentation

- [Create Recidiviz Data Issues Skill](../create_recidiviz_data_issues/SKILL.md) -
  More comprehensive task filing
