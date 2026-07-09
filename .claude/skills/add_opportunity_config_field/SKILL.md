---
name: add-opportunity-config-field
description: Add a new field to opportunity configs in the workflows admin panel. Use when asked to add a new config field, property, or attribute to the workflows opportunity configuration system.
---

# Skill: Add a New Field to Opportunity Configs

## Overview

Opportunity configs are the customizable settings that control how workflow
opportunities appear in the admin panel and the front-end app. Each field
propagates through up to 12 files across the backend and frontend (a few are
conditional on the field's type and nullability).

**What this skill does**: Gathers the field spec from the user, then makes
all necessary code changes and generates the database migration.

---

## Step 1: Gather Field Specification

Ask the user the following questions. Gather all answers before writing any
code.

### Required questions

1. **Field name** (snake_case): What is the Python/database column name?
   - Example: `case_notes_title`, `snooze_companion_opportunity_types`

2. **Field type**: What type of data will this field store?
   - `string` — a plain text value
   - `boolean` — true/false flag
   - `integer` — whole number
   - `string[]` — array of strings (stored as PostgreSQL ARRAY)
   - `json_object` — a single JSON object (dict)
   - `json_array` — array of JSON objects (stored as JSON)
   - For complex types, ask the user to describe the structure

3. **Optional or required?**
   - Optional: `nullable=True` in the DB; `required=False` in Marshmallow;
     `nullishAsUndefined(...)` in Zod
   - Required: needs a `server_default` in the DB column; `required=True` in
     Marshmallow; non-nullable Zod type with `.default(...)`

4. **Default value** (if required): What value should existing rows get?
   - For booleans: `"true"` or `"false"`
   - For strings: `""` or a specific string
   - For arrays/JSON: `"{}"` (PostgreSQL empty array/object notation)

5. **Description**: One sentence describing what the field controls (becomes
   the database column comment).

6. **Admin panel form placement**:
   - Which **section** should the field appear in? Current sections:
     `Copy`, `Opportunity Statuses`, `Criteria`, `Sidebar`, `Denial/Snooze`,
     `Tabs`, `Subcategories`, `Workflows Homepage Highlights`,
     `Supervisor Homepage`, `Danger Zone`
   - If creating a new section, what should it be named?
   - Position within the section: before/after which existing field?

7. **Admin panel label**: What human-readable label should show in the UI?
   (If omitted, the field name is auto-formatted.)

8. **Custom Edit/View component?**: Does this field need a custom
   component in `fieldComponents/`, or is the default plain-text input
   sufficient?
   - Most simple string/boolean fields do **not** need a custom component.
   - Complex structured data (e.g., snooze config, tab groups) do.

### Optional follow-up questions

- Does this field allow multiple items (`multiple: true` in the form spec)?
  Typically `true` for array fields with custom components.
- Should the field be in the `OPTIONAL_FIELDS` list in the form? This
  converts empty strings to `undefined` on submit. Yes for optional string
  fields; no for booleans/arrays.

---

## Step 2: Make Code Changes

Make changes to all applicable files in the order below. Read each file before
editing.

### File 1: Database schema
**`recidiviz/persistence/database/schema/workflows/schema.py`**

Add the column to `OpportunityConfiguration` at the end of the class. Follow the pattern for the matching type:

```python
# Optional string
my_field = Column(String, nullable=True)

# Required boolean with default
my_field = Column(Boolean, nullable=False, server_default="false")

# Optional JSON blob
my_field = Column(JSON, nullable=True)

# Required JSON/array with empty default
my_field = Column(JSON, nullable=False, server_default="{}")

# Required PostgreSQL string array with empty default
my_field = Column(ARRAY(String), nullable=False, server_default="{}")
```

Add a comment above the column explaining what it does (one line).

### File 2: Python type definition
**`recidiviz/workflows/types.py`**

Add the field to `OpportunityConfig` (not `FullOpportunityConfig` — it
inherits). Insert near thematically related fields. Use modern Python types
(`str | None`, not `Optional[str]`).

```python
# Optional string
my_field: str | None = attr.ib()

# Required boolean
my_field: bool = attr.ib()

# Optional list of strings
my_field: list[str] | None = attr.ib()

# Required list with default
my_field: list[dict[str, Any]] = attr.ib()
```

### File 3: Marshmallow API schema
**`recidiviz/case_triage/workflows/api_schemas.py`**

Add to `WorkflowsConfigSchema`. Match the Marshmallow field type to the
Python/DB type:

```python
# Optional string
my_field = fields.Str(required=False)

# Required string
my_field = fields.Str(required=True)

# Optional Boolean
my_field = fields.Bool(required=False)

# Required Boolean
my_field = fields.Bool(required=True)

# Optional list of strings
my_field = fields.List(fields.Str())

# Required list of strings 
my_field = fields.List(fields.Str(), required=True)

# Required list of objects (define a nested schema class first)
my_field = fields.List(fields.Nested(MyFieldSchema()), required=True)
```

Do **not** touch `OpportunityConfigurationRequestSchema` or
`OpportunityConfigurationResponseSchema` — they inherit everything via
`WorkflowsConfigSchema`.

### File 4: API route handler
**`recidiviz/admin_panel/routes/workflows.py`**

Add the field to the `WorkflowsQuerier(state_code).add_config(...)` call
inside `OpportunityConfigurationsAPI.post()`. Use `.get()` for optional
fields, direct access (`body_args["field"]`) for required fields:

```python
# Optional field
my_field=body_args.get("my_field"),

# Required field
my_field=body_args["my_field"],
```

### File 5: WorkflowsQuerier
**`recidiviz/workflows/querier/querier.py`**

Two edits in `add_config()`:

1. **Add to the method signature** (keyword-only, after `*`):
```python
my_field: str | None,       # optional
my_field: bool,              # required
my_field: list[str] | None, # optional list
```

2. **Add to the `insert_statement.values(...)` call**:
```python
my_field=my_field,
```

### File 6: Frontend Zod schema
**`frontends/admin-panel/src/WorkflowsStore/models/OpportunityConfiguration.ts`**

Add to `babyOpportunityConfigurationSchema` object. The schema uses
camelCase field names. Pick the right Zod type:

```typescript
// Optional string
myField: nullishAsUndefined(z.string()),

// Required string
myField: z.string(),

// Boolean with default
myField: z.boolean().default(false),

// Required array with default
myField: z.array(z.string()).default([]),

// Optional array
myField: nullishAsUndefined(z.array(z.string())),

// Optional object (define schema separately first)
myField: nullishAsUndefined(myFieldSchema),
```

**Important**: The schema uses `.strict()`, so every field in the backend
response must be listed here. If a backend field is missing, parsing will
fail in production even if tests pass.

### File 7: Frontend form spec
**`frontends/admin-panel/src/components/Workflows/OpportunityConfiguration/opportunityConfigurationFormSpec.tsx`**

Add to the appropriate section in `opportunityConfigFormSpec`. For simple
fields:

```typescript
myField: {
  label: "My Field Label",  // omit to auto-format from field name
},
```

For boolean (checkbox) fields:
```typescript
myField: {
  label: "Enable My Feature?",
  Edit: Checkbox,
},
```

For fields with custom components:
```typescript
myField: {
  label: "My Field",
  multiple: true,  // for array fields
  Edit: MyFieldEdit,
  View: MyFieldView,
},
```

If using a custom component, import it at the top of the file and create the
component file in `fieldComponents/`.

### File 8: Frontend form component
**`frontends/admin-panel/src/components/Workflows/OpportunityConfiguration/OpportunityConfigurationForm.tsx`**

If the field is an **optional string**, add its camelCase name to the
`OPTIONAL_FIELDS` array. This converts empty strings to `undefined` on form
submit so they aren't saved as empty strings.

```typescript
const OPTIONAL_FIELDS: OpportunityConfigurationField[] = [
  // ... existing fields ...
  "myField",
];
```
If the field is a **boolean**, add its default as the initial value for a new configuration form using its camelCase name. This renders the `Checkbox` field correctly and also ensures a controlled `false` value for **optional booleans** on new form submissions.

```typescript
const initial = {
  // ... existing fields ...
  myField: template?.myField ?? true, 
};
```

Skip this step for arrays and other required fields.

### File 9: Test factory function
**`recidiviz/tests/workflows/querier/querier_test.py`**

Add the new field to `make_add_config_arguments()`. Use a realistic test
value:

```python
"my_field": "some test value",   # optional string
"my_field": False,                # boolean
"my_field": [],                   # empty array
"my_field": None,                 # nullable field
```

### File 10: Admin panel route test fixture
**`recidiviz/tests/admin_panel/routes/workflows_test.py`**

Add the new field to the `generate_config()` helper function. Match the
same default value used in `make_add_config_arguments()`:

```python
my_field=False,   # boolean
my_field=None,    # optional string
```

### File 11: Case triage workflows route test fixture
**`recidiviz/tests/case_triage/workflows/workflows_routes_test.py`**

Add the new field to the `OpportunityConfig(...)` constructor call around
line 2110. Use the same default:

```python
my_field=False,   # boolean
my_field=None,    # optional string
```

### File 12: API schema validation test (required fields only)
**`recidiviz/tests/case_triage/workflows/workflows_api_schemas_test.py`**

**Only needed when the Marshmallow field is `required=True`.** Skip this file
entirely for optional fields.

`WorkflowsConfigSchemaTest` defines `valid_schema_test(...)` payloads that must
satisfy the schema. Adding a new required field makes every previously-valid
payload invalid until the field is present, so add it to **each**
`valid_schema_test` case (currently `test_valid_data`, `test_manual_snooze`,
`test_auto_snooze`) with a valid value:

```python
"myField": [],       # required array
"myField": False,    # required boolean
"myField": "value",  # required string
```

---

## Step 3: Generate the Alembic Migration

After making all code changes, generate the migration using the repo's
dedicated script (do **not** call `alembic` directly — it requires special
environment setup that the script handles):

```bash
uv run python -m recidiviz.tools.migrations.autogenerate_migration \
  --database WORKFLOWS \
  --message "add_<field_name>"
```

The script spins up a local PostgreSQL instance, applies all existing
migrations, then diffs against the current schema to produce the new
migration. It cleans up the local database when finished.

The generated file will appear in:
`recidiviz/persistence/database/migrations/workflows/versions/`

**Review the generated migration**: Confirm the `upgrade()` and `downgrade()`
functions match what you expect. The migration should only contain the single
`add_column` / `drop_column` for the new field. If autogenerate picks up
unrelated changes, remove them.

If the migration involves a `server_default`, confirm the value matches the
schema column definition.

---

## Step 4: Verify and Update Snapshots

Run the full set of affected tests:

```bash
uv run pytest \
  recidiviz/tests/workflows/querier/querier_test.py \
  recidiviz/tests/admin_panel/routes/workflows_test.py \
  recidiviz/tests/case_triage/workflows/workflows_routes_test.py \
  recidiviz/tests/case_triage/workflows/workflows_api_schemas_test.py \
  -x -q
```

If snapshot tests fail (snapshottest `AssertionError`), regenerate them:

```bash
uv run pytest \
  recidiviz/tests/workflows/querier/querier_test.py \
  recidiviz/tests/admin_panel/routes/workflows_test.py \
  --snapshot-update -q
```

**After updating snapshots**, run these two cleanup steps (black first, then
header restore) because `--snapshot-update` rewrites each file in snapshottest's
own style — stripping the license block *and* the Pylint docstring, and
reformatting the body — in **both** files.

1. **Run black on BOTH files.** Snapshottest writes single quotes, un-wraps the
   `snapshots[...] = [` keys, and drops blank lines — in `snap_querier_test.py`
   too (its `GenericRepr` values are untouched, but its assignment lines are
   not). Black restores the committed style:
   ```bash
   uv run black \
     recidiviz/tests/workflows/querier/snapshots/snap_querier_test.py \
     recidiviz/tests/admin_panel/routes/snapshots/snap_workflows_test.py
   ```

2. **Restore the license block AND docstring.** They sit on opposite sides of
   the `# -*- coding`/`# snapshottest` comments (license above, docstring
   below), so anchor on `from __future__` — the first line identical in HEAD and
   the regenerated file, below both — taking everything before it from HEAD and
   everything from it onward from the black-formatted file:
   ```bash
   for f in \
     recidiviz/tests/workflows/querier/snapshots/snap_querier_test.py \
     recidiviz/tests/admin_panel/routes/snapshots/snap_workflows_test.py; do
     git show "HEAD:$f" | sed '/^from __future__/,$d' > /tmp/hdr.txt
     sed -n '/^from __future__/,$p' "$f" > /tmp/body.txt
     cat /tmp/hdr.txt /tmp/body.txt > /tmp/restored.py && mv /tmp/restored.py "$f"
   done
   ```

**Verify:** `git diff` both snapshot files and confirm the *only* changed lines
are your new field's value. Any header, quote, or blank-line churn means a step
above was missed.

---

## Common Mistakes to Avoid

- **Missing from Zod schema**: Because `babyOpportunityConfigurationSchema`
  uses `.strict()`, any field returned by the backend that's not in the
  schema will cause a runtime parse error. Always add new fields there even
  if the frontend doesn't use them yet.

- **Using `Optional[str]` instead of `str | None`**: The project style
  requires modern union syntax.

- **Wrong nullability**: Optional fields → `nullable=True` in DB and
  `required=False` in Marshmallow. Required fields → need a `server_default`
  in the DB or the migration will fail on existing rows.

- **Missing from `add_config()` INSERT**: The values dict in the querier must
  include every parameter in the signature, or the value won't be persisted.

- **Missing from `make_add_config_arguments()`**: If you skip the test
  factory, every `add_config` test will fail with a missing keyword arg.

- **camelCase vs snake_case**: Python files use `snake_case`, TypeScript files
  use `camelCase`. The `CamelCaseSchema` Marshmallow mixin handles the
  conversion between them automatically.
