---
name: maintain-skill-files
description: Proactively update SKILL.md files when they become outdated, incorrect, or need improvements. Use when you notice a skill doesn't match current workflow, when user corrects your skill usage, or when discovering better practices that should be documented.
---

# Skill: Maintain Skill Files

## Overview

This skill defines when and how to proactively update SKILL files to improve
future interactions.

**Core principle**: If you notice that a skill needs updating, offer to update
it immediately. Don't wait for the user to ask.

## When to Update Skills

Watch for these signals that a skill needs updating:

### Strong signals (update immediately):

- **You notice the skill is outdated** - Commands, file paths, or processes have
  changed
- **You didn't follow the skill correctly** - If the user points out you missed
  steps or didn't follow the documented workflow
- **User complains a skill wasn't triggered** - If the user says you should have
  used a skill but didn't, check if the skill has proper YAML frontmatter with
  `name:` and `description:` fields (see "Skill Format Requirements" below)
- **User corrects your behavior on a workflow** - When the user says "actually,
  you should do X instead of Y" for a process that has a skill
- **User adds new requirements or steps** - The user teaches you additional
  steps that should be part of the workflow
- **User expresses frustration about a repeated workflow** - Phrases like "I'm
  annoyed that...", "you still...", "again...", "why do I have to..."
- **Instructions are ambiguous** - If you misunderstood a skill's instructions,
  make them clearer

### Moderate signals (consider updating):

- **You discover edge cases or gotchas** - While following a skill, you
  encounter situations not covered in the documentation
- **User shares tips or tricks** - Additional context that makes following the
  workflow easier

### When NOT to update:

- One-off corrections that don't apply to the general workflow
- Temporary workarounds
- Information that's likely to change frequently
- Details that are already well-documented in the skill

## What Makes a Good Skill

Skills are **specific, repeatable workflows or procedures**. Create or update
skills for:

- Multi-step processes that are done regularly
- Workflows with specific requirements or gotchas
- Procedures that need to be followed precisely
- Tasks that have a clear start and end state

**Location**:
`/Users/ageiduschek/src/pulse-data/.claude/skills/<skill_name>/SKILL.md`

### When to create a NEW skill:

- The workflow is distinct from existing skills
- It's referenced or used frequently enough to warrant its own documentation
- It has enough complexity that step-by-step instructions are valuable
- The user has performed this multi-step process multiple times

### When to UPDATE an existing skill:

- User points out you didn't follow the skill correctly
- User adds new requirements or steps
- You discover edge cases or gotchas
- Instructions are ambiguous and led to mistakes
- Commands or paths are outdated

## How to Update a Skill

### Step 1: Identify what needs updating

When you notice a signal from the "When to Update" section, immediately
consider:

- Which skill should be updated (or should a new one be created)?
- What specific information needs to be added/changed?
- Will this help prevent similar issues in the future?

### Step 2: Propose the change

Tell the user what you want to update and why. For example:

> "I notice you had to correct me about using the issue template. I should
> update the create_recidiviz_data_github_tasks skill to make that more
> explicit. Would you like me to do that?"

OR if it's very obvious:

> "I'm going to update the skill to include this step since it's part of the
> workflow."

### Step 3: Make the update

- Use the Edit tool to update existing skill files
- Use the Write tool to create new skill files
- Be clear and specific in the documentation
- Include examples when helpful
- Use formatting (headers, bullets, bold) to make it scannable
- Follow the skill file structure (Overview, When to Use, Steps, etc.)

### Step 4: Verify the update

After updating:

- Briefly tell the user what you updated
- Mention how it will help in the future

## Best Practices for Writing Skills

For official best practices on writing SKILL files, see the
[Skills documentation](https://code.claude.com/docs/en/skills.md).

### Skill Format Requirements

**CRITICAL**: All skills MUST have YAML frontmatter with `name:` and `description:` fields. This is how Claude autonomously discovers when to use skills.

Required format (from [official docs](https://code.claude.com/docs/en/skills.md)):

```yaml
---
name: your-skill-name
description: Brief description of what this Skill does and when to use it. Use when [specific trigger phrases].
---
```

Requirements:
- **`name`**: Lowercase letters, numbers, and hyphens only (max 64 characters)
- **`description`**: Max 1024 characters. Must include:
  - What the skill does
  - When Claude should use it (include specific trigger terms like "file", "create", "update", etc.)

**Example**:
```yaml
---
name: create-github-tasks
description: File GitHub issues and tasks. Use when the user asks to file, create, or track issues, tasks, tickets, or TODOs.
---
```

**When updating skills**: If a skill doesn't have this frontmatter, or if the user complains the skill wasn't triggered, add or improve the YAML frontmatter with clear trigger terms in the description.

## Example Scenarios

### Scenario 1: User corrects you multiple times

**Signal**: User says "I'm annoyed that you still needed to be prompted to
follow the issue template structure"

**Action**:

1. Immediately recognize this as a strong signal
2. Propose updating the skill file
3. Make the skill more explicit with numbered steps, examples, and emphasis on
   the template
4. Tell the user you've updated it

### Scenario 2: You discover edge cases

**Signal**: While following the "create GitHub task" skill, you discover that if
the task is for a different repo, the TODO format is different

**Action**:

1. Complete the current task
2. Mention to the user: "I should update the skill to document the cross-repo
   TODO format. Should I do that?"
3. If yes, add a section or step about cross-repo TODOs with examples

### Scenario 3: User adds new steps

**Signal**: User says "Also make sure to add the 'Team: Data Platform' label"

**Action**:

1. Complete the task with the new step
2. Ask: "Should I update the skill to always include the 'Team: Data Platform'
   label?"
3. If yes, add this as a step in the workflow

### Scenario 4: User teaches you a new workflow

**Signal**: User walks you through a complex multi-step process that you'll
likely do again

**Action**:

1. After completing the task, ask: "Should I document this as a new skill so I
   can follow it more precisely next time?"
2. If yes, create `.claude/skills/<workflow_name>/SKILL.md` with clear steps
3. Add a reference to it in the root CLAUDE.md file

## Maintaining the Skills List

When you create a new skill, remember to update the "Available skills" section
in the root CLAUDE.md file with a link to the new skill.

## Related Documentation

- [Main CLAUDE.md](../../../CLAUDE.md) - The root documentation file
- [Create Recidiviz Data GitHub Tasks](./../create_recidiviz_data_github_tasks/SKILL.md) -
  Example of a well-structured skill
