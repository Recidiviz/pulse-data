---
name: cleanup-jupyter
description: Clean up and reorganize Jupyter notebooks — add documentation headers, restructure cells into a standard layout, and identify dead code. Use when the user asks to clean up, document, reorganize, or refactor a notebook.
---

# Skill: Jupyter Notebook Cleanup

## Overview

This skill cleans up a Jupyter notebook in two phases:

1. **Document** — Insert a documentation cell at the top summarizing key dataframes and variables.
2. **Reorganize** — Restructure cells into a standard layout and move dead code to an Appendix.

Run both phases in order unless the user asks for only one.

---

## Phase 1: Add Documentation Header

### Goal

Insert a single new Markdown cell as the very first cell of the notebook. Do not modify any existing cells.

### Step 1: Identify Primary Dataframes

A **primary dataframe** is any dataframe directly used in 5 or more outputs (not intermediate or temporary dataframes).

For each primary dataframe, collect:
- **Name** — the variable name
- **Description** — the purpose of the dataframe
- **Source** — which previous dataframe, SQL query, or file it originates from
- **Filters** — filters applied in Python/notebook code or in SQL `WHERE` clauses (ignore filters inside SQL CTEs)

Insert a table in the new first Markdown cell:

```markdown
| Name | Description | Source | Filters applied |
|------|-------------|--------|-----------------|
| df_name | ... | ... | ... |
```

### Step 2: Identify Key Variables per Section

Sections are defined by existing Markdown headers. If no headers exist, treat each major analytical step as a section.

For each section, identify any variable that appears in 10 or more cells or is used in 5 or more final outputs or visualizations.

Append a second table to the same new first Markdown cell, grouped by section:

```markdown
## Variables

### [Section Name]

| Variable | Definition |
|----------|-----------|
| var_name | ... |
```

### Step 3: Insert the Cell

**For notebooks under ~256KB:** Use `NotebookEdit` with `edit_mode=insert` and no `cell_id` to insert at the beginning.

**For larger notebooks:** `NotebookEdit` requires a successful `Read` first, which fails above 256KB. Instead, write a Python script to a file in the same directory as the notebook (e.g. `_nb_cleanup.py`), run it with `uv run python _nb_cleanup.py`, verify the output, then delete the script. Use `json.load`/`json.dump` to manipulate the notebook directly.

---

## Phase 2: Reorganize the Notebook

### Goal

Restructure cells into the layout below, preserving all dependencies and the ordering of cleaning steps within each dataset. Do not delete any code.

### Target Structure

```
## 1. Imports & Configuration
   All import statements and global configuration settings.

## 2. Data Ingestion & Cleaning
   One subsection per loaded dataset (BigQuery SQL, CSV, or XLSX):

   ### 2a. Core Transformations
       Transformations essential to downstream functionality.

   ### 2b. Optional Transformations
       Supplementary transformations. Add a `# OPTIONAL` comment above each.
       Flag transformations whose output is never used downstream with a comment.

## 3. Dataset Construction (Joins & Merges)
   All code that joins or merges datasets, placed after all ingestion sections.

## 4. Analysis
   Analytical code organized first by dataset(s) used, then by variable(s) involved.
   Use sub-headers: `### 4.1 [Dataset] — [Variable]`
   Preserve findings/conclusions as Markdown cells above the relevant code block.

## Appendix: Exploratory & Dead Code
   Dead code moved here rather than deleted.
   Use sub-headers by subject matter.
   Add a one-line Markdown note above each block explaining why it was moved.

## Data Quality (optional section)
   Collect data findings, print statements, assertions, and exploratory cells that
   surface data quality issues (nulls, unexpected distributions, mismatches, etc.).
   Use sub-headers by variable or dataset.
```

### Comment and Dead Code Rules

**Annotation comments** — code cells whose entire content is a short explanatory note (e.g. `#takes 10 minutes`, `#try X`) with no actual code: convert to Markdown cells in place.

**Exception:** The line `#!gcloud auth application-default login` is a useful reference command; leave it as a code cell and do not move it.

**Commented-out code** — code cells where all lines are commented-out variable assignments, expressions, or statements: flag these for removal by moving them to the Appendix with a one-line note explaining why.

**Dead code:**
- A cell is **dead** if its output or resulting variable is never referenced downstream.
- If confident a cell is dead or purely exploratory: move it to the Appendix.
- If status is ambiguous: do NOT move it. Instead, insert a visible Markdown note tagged `⚠️ AMBIGUOUS:` describing why you think it may be dead code.
- Never delete any code.

### Step 1: Audit the Notebook

Read the entire notebook (using `jq` slices for large files) and produce a plan. For notebooks with 100+ cells, do not list every cell — instead:
- Show a **grouped table** mapping each existing section to its target section in the new structure.
- List only the cells that will **move, be converted, or get a new note**, with their current index and proposed action.
- Flag dead/ambiguous cells separately.
- If scope is large or unclear, ask 1–2 clarifying questions before presenting the full plan.

Show the user the plan and ask for confirmation before making any changes.

### Step 2: Reorganize

After user approval, apply changes via the same method used in Phase 1 (NotebookEdit for small notebooks, Python script for large ones). Insert new Markdown cells for section headers only where they do not already exist.

### Step 3: Validate Execution

After reorganizing, run:

```bash
jupyter nbconvert --to notebook --execute --allow-errors <notebook_path>
```

Review the output for errors caused by reordering or missing dependencies. Ignore `NameError` cascades that stem from a failed data load (e.g. BigQuery auth failure, missing file) — these are expected in non-interactive execution and are not caused by reordering. Only investigate errors whose traceback references a variable that existed before the reorder. Fix any genuine ordering errors (reorder cells, add missing imports, etc.) and report all changes made.

Do not engage with git at any point during this skill.

---

## Notes

- Always read the full notebook before making changes.
- Prefer using `NotebookEdit` over rewriting the entire notebook file.
- When in doubt about whether a cell is dead, leave it in place and add an `⚠️ AMBIGUOUS:` note.
- Never remove or edit existing code cells — only move them or add new Markdown cells.
