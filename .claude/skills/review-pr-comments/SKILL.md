---
name: review-pr-comments
description:
  Go through a PR's review comments one by one, propose solutions, and commit
  fixes. Use when the user wants to address PR feedback, review comments, or
  resolve PR review requests.
---

# Skill: Review PR Comments

## Overview

This skill iterates through PR review comments, proposes solutions for each, and
commits fixes after user approval.

## Instructions

### Step 0: Get PR URL from User

If the user did not provide a PR URL or number in their request, **use the
`AskUserQuestion` tool** to ask for it. Do not proceed until you have a valid PR
reference.

### Step 1: Get PR Details and Comments

```bash
gh pr view <pr_number> --json title,body,state,headRefName,baseRefName
```

**Determine which comments to address:**

The user may specify a reviewer to target (e.g., "Address Anna's comments on PR
58565"). If so, find that reviewer's latest review and use those comments.
Otherwise, default to unresolved threads.

**Option A — Specific reviewer's comments:**

Find the reviewer's GitHub username, then fetch their latest review:

```bash
gh api repos/Recidiviz/pulse-data/pulls/<pr_number>/reviews --jq '[.[] | select(.user.login == "<username>")] | last'
```

Then fetch comments from that review using the review ID:

```bash
gh api repos/Recidiviz/pulse-data/pulls/<pr_number>/reviews/<review_id>/comments
```

**Option B — Unresolved threads (default):**

Get unresolved review threads with their IDs:

```bash
gh api graphql -f query='query { repository(owner: "Recidiviz", name: "pulse-data") { pullRequest(number: <pr_number>) { reviewThreads(first: 100) { nodes { id isResolved comments(first: 1) { nodes { databaseId path line body author { login } } } } } } } }'
```

Filter for unresolved threads only (`isResolved: false`). Save both IDs:

- `id` (node_id): Used for resolving the thread via GraphQL mutation
- `databaseId`: Used for replying to the comment via REST API

**For both options:**

Count the total number of actionable comments to show progress later.

**If there are no comments to address**, inform the user and stop.

### Step 2: Ensure You're on the PR Branch

First check if you're already on the correct branch:

```bash
git branch --show-current
```

Compare with the PR's `headRefName` from Step 1. If you're not on the correct
branch, checkout the PR:

```bash
gh pr checkout <pr_number>
```

If already on the correct branch, skip the checkout.

### Step 3: Build Context

Read the full set of changes in the PR to understand the overall context:

```bash
git diff <baseRefName>..HEAD
```

Also read any specific files referenced in comments if additional context is
needed beyond the diff.

### Step 4: Plan — Analyze, Group, and Order Comments

Before making any changes, analyze **all** comments together and propose a plan.

#### 4a. Categorize Comments

Skip non-actionable comments (emoji-only, simple acknowledgments, "looks good").

**Handling bot comments (e.g., coderabbit):** Bot comments are often verbose
with markdown formatting, collapsible sections, and AI prompts. Extract only the
**core request** — what change is actually being asked for.

#### 4b. Group and Order

Group interrelated comments together. Comments are interrelated when they:

- Call out the same issue in different places
- Require a shared refactor to address
- Have a natural ordering dependency (e.g., one structural change enables others)

**Propose an ordering** with structural/refactor changes first, followed by
smaller self-contained changes. Each group becomes one commit.

#### 4c. Present the Plan

Show the user the proposed plan:

```
Plan for addressing N comments:

Group 1: [Brief description] (Comments A, B)
  - [What will change]
  → 1 commit

Group 2: [Brief description] (Comments C, D, E)
  - [What will change]
  → 1 commit

Skipped: [Non-actionable comment descriptions]
```

**Use the `AskUserQuestion` tool** to ask if the user approves the plan.

Options to provide:

- "Yes, proceed with this plan"
- "No, skip all"
- (User can suggest reordering or regrouping via "Other")

### Step 5: Execute — Process Each Group

For each group in the approved plan:

#### 5a. Present the Group

Show:

- **Progress**: "Group X of Y"
- Each comment in the group: author, location (file:line), and comment text
- The current code at each location
- **Proposed changes** for all comments in this group (before/after)

For larger or structural changes, present a brief plan of the approach before
showing specific edits.

**If the reviewer provided an exact code suggestion** (e.g., coderabbit's
"Committable suggestion" or GitHub's suggestion feature), prefer using their
exact suggestion rather than rephrasing.

**Use the `AskUserQuestion` tool** to ask if the user approves:

- "Yes, make these changes"
- "No, skip this group"
- (User can suggest modifications via "Other")

#### 5b. Make Changes and Commit

After user approval:

1. Make all edits for this group
2. Use the `/commit` skill to commit the changes

#### 5c. Move to Next Group

Announce: "Done. Next group:" and repeat from step 5a.

### Step 6: Push All Changes

After all groups are processed, push to remote. If the branch was rebased before
addressing comments (which is common), a force push will be needed:

```bash
# Try regular push first
git push

# If it fails because the branch was rebased, use force push
git push --force-with-lease
```

**Tip:** Before Step 2, consider prompting the user to rebase onto the base
branch if they haven't already, since it's common practice before addressing
review comments.

### Step 7: Reply and Resolve

After pushing, handle PR comment reactions, replies, and resolution in batch.
This avoids spamming the reviewer's inbox during the process.

**Use the `AskUserQuestion` tool** to ask the user how to handle comment
threads:

- "Thumbs-up react and resolve all (default)"
- "Skip — don't react or resolve"
- (User can provide custom reply text via "Other")

**If default (thumbs-up and resolve):**

For each addressed comment:

```bash
# 1. Add thumbs-up reaction
gh api repos/Recidiviz/pulse-data/pulls/comments/<comment_databaseId>/reactions -X POST -f content="+1"

# 2. Resolve the thread
gh api graphql -f query='mutation { resolveReviewThread(input: {threadId: "<thread_node_id>"}) { thread { isResolved } } }'
```

**If custom reply:**

For each addressed comment, post the reply and then resolve:

```bash
# 1. Reply to the comment
gh api repos/Recidiviz/pulse-data/pulls/<pr_number>/comments/<comment_databaseId>/replies -X POST -f body="<message>"

# 2. Resolve the thread
gh api graphql -f query='mutation { resolveReviewThread(input: {threadId: "<thread_node_id>"}) { thread { isResolved } } }'
```

### Step 8: Summary

Tell the user:

```
Done! All PR comments addressed:

1. ✅ [Brief description of change 1]
2. ✅ [Brief description of change 2]
...

All changes pushed to the PR.
```

## Important Notes

- **Group related comments**: Interrelated comments should be addressed together
  in a single commit. Structural changes come first, small fixes after.
- **One commit per group**: Each group of related comments gets one commit.
  Single self-contained comments are their own group.
- **Skip non-actionable comments**: Emoji reactions, simple acknowledgments, or
  "looks good" comments don't need changes
- **Push at the end**: Push all commits together after all comments are
  processed
- **NEVER include PII** in commit messages or comment replies
