# Python Style

These rules apply whenever writing or modifying Python code in this repository.

## Types

- Add type information to every function definition. Use modern types (`str | None` not `Optional[str]`). Avoid `Any` where reasonable.
- Do not give parameters or attributes nullable types (e.g. `str | None`) unless there is a legitimate, non-test use case to do so. If we always expect the non-test code to have a nonnull value, the type should be nonnull.
- Do not give parameters or attributes default values (e.g. `my_var: str | None = None` or `my_var: str | None = attr.ib(default=None)`) unless there is a legitimate, non-test use case to do so. Avoiding verbose test updates is an ANTI GOAL.

## Avoiding complexity

- If the type of a variable suggests it could have multiple values but you are certain it can only have a subset, do not add branches to handle the impossible cases. Instead, make your assumptions explicit with a runtime check: use `assert_type()` from `recidiviz.utils.types` (not `typing.assert_type`) for type narrowing, or raise a `ValueError` for other invariants. `assert_type()` returns the narrowed value, so you can chain directly (e.g. `assert_type(var, str).lower()` instead of adding a None-handling branch before accessing `var`).
- When accessing values in dictionaries, always use `[]` (e.g. `my_dict[key]`) instead of `.get()` unless there is a known, good reason why the key might not exist.
- Test for emptiness or presence with the object's own truthiness — `if my_collection:` / `if not my_collection:` — rather than `if len(my_collection) > 0` / `== 0`. This works for lists, dicts, sets, strings, and any object that defines `__len__` or `__bool__` (including `YAMLDict`).

## Filesystem paths

Derive paths from the nearest importable Python package (`os.path.dirname(module.__file__)`) rather than chaining relative `..` segments from `__file__`. For example, use `from recidiviz.ingest.direct import regions` and `os.path.dirname(regions.__file__)` instead of `os.path.join(os.path.dirname(__file__), "..", "..", "ingest", "direct", "regions")`.

## Imports

Imports outside of top-level should be avoided unless absolutely necessary. If you feel the need to add one, ask the user and explain why. "Avoiding circular imports" is not a good reason - consider instead how you could restructure the code to avoid the circular import. Hiding an import inside a function to avoid updating the `validate_source_visibility` entrypoint allowlists is also not a good reason — update the allowlists to reflect the real dependency instead.

## Strings and constants

Use raw string literals sparingly in code. Always consider storing a string in a named constant. This is critical when the string is used in more than one place and changing it in one place without the others would silently break behavior — define it once as a constant and reference that constant everywhere.

A common high-value case is **SQL output column names when building queries in Python**. A column produced by one query (`SELECT … AS <name>`) is usually read again — by a downstream query, an output schema, or a test — so inlining the name as a raw string in each place lets a rename silently break the others (the SQL fails only at query time, if at all). Define the name once and reference it everywhere. A module-level constant is fine; use a class variable when there is a builder object it naturally belongs to. Don't bother for a name that lives entirely inside a single query string and is never referenced elsewhere.

```python
class AssignmentsByTimePeriodViewBuilder(...):
    ASSIGNMENT_START_DATE_COLUMN_NAME = "assignment_start_date"

    @classmethod
    def _build_query(cls, ...) -> str:
        return f"""
        SELECT assignment_date AS {cls.ASSIGNMENT_START_DATE_COLUMN_NAME}
        FROM assignment_sessions
        """

    @classmethod
    def _build_downstream_query(cls, ...) -> str:
        col = cls.ASSIGNMENT_START_DATE_COLUMN_NAME
        return f"SELECT {col}, COUNT(*) FROM (...) GROUP BY {col}"
```

## Control flow and error handling

### Prefer early returns and guard clauses to flatten logic

Whenever an `if` can `return` or `raise` early, do so, rather than nesting the rest of the function inside an `else`. Handling edge cases, invalid inputs, and special cases up front — and exiting immediately — keeps the function's main logic at the top level of indentation instead of buried inside conditionals. This applies broadly, not just when branching over an enum or type.

```python
# Prefer this:
def process(record: Record) -> Result:
    if record.is_empty:
        return Result.empty()
    if not record.is_valid:
        raise ValueError(f"Invalid record: [{record.id}]")
    ...  # main logic at the top level

# over nesting the main logic inside an else:
def process(record: Record) -> Result:
    if record.is_empty:
        return Result.empty()
    else:
        if record.is_valid:
            ...  # main logic indented two levels deep
        else:
            raise ValueError(f"Invalid record: [{record.id}]")
```

A specific case of this is dispatching on an enum or type: write a flat sequence of `if x is …: return …` clauses (compare enum members with `is`, not `==`) rather than `elif`/`else`, and end the sequence with a `raise ValueError(...)` for the unhandled case, so a newly added enum member or an unexpected value fails loudly instead of silently falling through.

```python
if join_type is MetricTimePeriodJoinType.PERIOD:
    return _build_period_join()
if join_type is MetricTimePeriodJoinType.ASSIGNMENT:
    return _build_assignment_join()
raise ValueError(f"Unexpected join_type: [{join_type}]")
```

### Wrap interpolated values in `[...]` in error messages

Bracket every interpolated value in an error or exception message — `[{value}]`, `[{type(x)}]`, `[{address.to_str()}]` — so the value's boundaries (including empty strings and trailing whitespace) are visible in logs.

```python
raise ValueError(f"Unexpected builder type [{type(builder)}]")
```

### Re-raise caught exceptions with `from e` and added context

When catching and re-wrapping an exception, raise a domain `ValueError` whose message names what failed and includes the offending value, and chain it with `from e` so the original traceback is preserved.

```python
try:
    return MetricUnitOfObservationType(unit_of_observation_str.upper())
except Exception as e:
    raise ValueError(
        f"No MetricUnitOfObservationType found for module "
        f"[{unit_of_observation_module.__name__}]"
    ) from e
```

## Functions and classes

### Make multi-argument public functions keyword-only

Public functions, methods, and constructors that take more than a couple of arguments should force keyword-only calling with a leading `*,`, and call sites should pass every argument by keyword. This keeps call sites readable as argument lists grow and prevents positional-argument mistakes. (This is the function-level counterpart to the attrs `kw_only=True` default below.)

```python
def build_assignments_by_time_period_query_template(
    *,
    time_period: MetricTimePeriod,
    population_type: MetricPopulationType,
    unit_of_observation_type: MetricUnitOfObservationType,
) -> str:
    ...
```

### Put stateless logic in `@classmethod`/`@staticmethod` helpers

Logic that does not need instance state — building an address, an ID, or a derived name — belongs in a `@classmethod` or `@staticmethod` so callers can compute it without first constructing the object. `__init__` then calls these helpers and assigns the results.

```python
@classmethod
def view_address_for_event(cls, event_type: EventType) -> BigQueryAddress:
    return BigQueryAddress(
        dataset_id=cls._DATASET_ID, table_id=cls._build_view_id(event_type)
    )
```

### Expose derived values as `@property`

Anything derivable from stored state should be a `@property` rather than recomputed at call sites or stored as a redundant field that can drift out of sync. This applies to all classes, not just attrs classes (the attrs section below shows the same convention in that context).

```python
@property
def unit_of_observation_type(self) -> MetricUnitOfObservationType:
    return self.event_type.unit_of_observation_type
```

### Document every constructor parameter

For data-holding constructors, document what each parameter is for. Either give the constructor a docstring with a per-parameter description, or put a one-line `#` comment directly above each parameter — both are acceptable. (This is a deliberate exception to the general "no obvious comments" rule, scoped to public config/data objects.)

```python
def __init__(
    self,
    # Type of event this builder encodes.
    event_type: EventType,
    # Human-readable description of the event.
    description: str,
    # SQL query or source address the observations are generated from.
    sql_source: BigQueryAddress | str,
) -> None:
    ...
```

### Restate the module's purpose as the main class docstring; document public helpers

When a module exists to provide one main class, the module docstring and that class's docstring describe the same thing (it is fine for them to be near-identical). Give every public method and helper a docstring; open value-returning helpers with "Returns …".

```python
"""View builder that encodes a collection of point-in-time observations."""  # module docstring


class EventObservationBigQueryViewBuilder(BigQueryViewBuilder):
    """View builder that encodes a collection of point-in-time observations."""

    def output_columns(self) -> list[str]:
        """Returns the names of the columns this view emits."""
        ...
```

## SQL templates

Assemble multi-line SQL templates with `fix_indent(...)` (from `recidiviz.utils.string_formatting`) rather than hand-managing indentation in raw multi-line strings. `fix_indent(s, indent_level=N)` dedents `s`, strips surrounding whitespace, and re-indents every line to `N` spaces while preserving the relative indentation between lines. Its main value is composing templates: a fragment built elsewhere arrives with its own indentation, and `fix_indent(fragment, indent_level=N)` re-indents it so it nests cleanly inside the parent query. Prefer shared clause helpers such as `nonnull_end_date_clause` (from `recidiviz.calculator.query.bq_utils`) over hand-written SQL snippets.

```python
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.utils.string_formatting import fix_indent

# A subquery built elsewhere, formatted at its own (left-margin) indentation.
time_periods_query = time_period.build_query()

query_template = f"""
WITH time_periods AS (
{fix_indent(time_periods_query, indent_level=4)}
),
assignment_sessions AS (
    SELECT
        *,
        {nonnull_end_date_clause("end_date_exclusive")} AS end_date_nonnull
    FROM assignments
)
SELECT * FROM time_periods JOIN assignment_sessions USING (person_id)
"""
return fix_indent(query_template, indent_level=0)
```

Here the inner `fix_indent(time_periods_query, indent_level=4)` re-indents the embedded subquery to sit four spaces deep inside the `time_periods` CTE, and the outer `fix_indent(query_template, indent_level=0)` strips the surrounding blank lines and normalizes the whole template to a clean left margin.

## YAML parsing and serialization

When a YAML file backs a Python model, parse and serialize it through these conventions rather than indexing raw parsed dicts.

### Parse with `YAMLDict`, never raw dict access

Read YAML through `YAMLDict` (`recidiviz/utils/yaml_dict.py`): `YAMLDict.from_path(path)` or `YAMLDict.from_io(...)`. Its typed accessors (`pop`, `pop_optional`, `pop_dict`, `pop_dicts`, `pop_list`, `peek`) check the type of each value and fail loudly on a mismatch. Never index the raw parsed dict directly (`data["name"]`) — that skips both the type checking and the unused-key detection below.

### Consume every key with `pop*`, then assert the dict is empty

Read fields with the `pop*` methods (which remove each key as they read it), and once all known keys are popped, assert nothing is left — so a misspelled or unexpected key in the YAML fails loudly instead of being silently ignored:

```python
# ... after popping all known fields ...
if file_config_dict:
    raise ValueError(
        f"Found unexpected config values for raw file [{file_tag}]: "
        f"{repr(file_config_dict.get())}"
    )
```

Prefer `pop*` over `peek*` for exactly this reason: `peek*` leaves the key in place, which defeats the empty check. Reach for `peek*` only when you genuinely need to read a value without consuming it (e.g. inspecting a type to decide how to parse the rest).

### Build models with a `from_yaml` classmethod factory

Turn YAML into a model via a `@classmethod` factory, not a free function. Standardize on two shapes:

- **Top-level configs** take a path and call `YAMLDict.from_path` internally: `from_yaml(cls, yaml_path: str | Path)`.
- **Nested sub-objects** take an already-popped `YAMLDict` handed down by their parent: `from_yaml_dict(cls, yaml_dict: YAMLDict)`.

The parent pops a sub-dict (`pop_dict`/`pop_dicts`) and passes it to the child factory, so consumption — and the empty check — composes top-down.

```python
@classmethod
def from_yaml(cls, yaml_path: str | Path) -> "ValidationRegionConfig":
    file_contents = YAMLDict.from_path(yaml_path)
    region_code = file_contents.pop("region_code", str)
    exclusions = [
        ValidationExclusion.from_yaml_dict(d)
        for d in file_contents.pop_dicts("exclusions")
    ]
    ...
```

### Parse enums at the point of `pop`

Convert enum-valued fields by passing the popped string straight into the enum constructor, so an invalid value raises immediately at parse time rather than slipping through as a bare string.

```python
exclusion_type=ValidationExclusionType(exclusion_dict.pop("exclusion_type", str))
```

### Apply defaults when parsing, not via nullable fields

Resolve optional fields to their default at parse time so the model field can stay non-nullable (consistent with the Types rules above). Use `pop_optional(...) or DEFAULT` for truthy defaults; use the walrus `is not None` form when `False`, `0`, or `""` are legitimate values, where `or` would wrongly discard them.

```python
dev_mode = file_contents.pop_optional("dev_mode", bool) or False

# When False is a legitimate value, guard on `is not None` instead of `or`:
is_pii = v if (v := column.pop_optional("is_pii", bool)) is not None else DEFAULT_IS_PII_VALUE
```

### Always load with a safe loader

`YAMLDict` already loads via `SafeLoader` (with a faster C-binding path when available). For any ad-hoc load outside `YAMLDict`, use `yaml.safe_load(...)`. Never use `yaml.load` / `yaml.full_load` — they can execute arbitrary constructors from the input, which is a security risk; existing usages are legacy holdouts to migrate.

### Writing YAML back out: choose by who owns the file

- **Machine-owned / regenerated files** — no hand-authored comments, the model is the source of truth: serialize through a model `to_dict()` and dump with `yaml.dump(..., sort_keys=False)` so the model's field order is preserved.
  ```python
  yaml_file.write(yaml.dump(source_table_config.to_dict(), sort_keys=False))
  ```
- **Human-authored files edited in place** — configs full of comments and intentional formatting you must not clobber: use `ruamel.yaml` in round-trip mode (load into a `CommentedMap`, mutate the specific values, dump back). PyYAML's `dump` discards comments, key order, and formatting; ruamel preserves them.

These are complementary, not competing: pick `yaml.dump` when you own and regenerate the whole file, and ruamel when you are surgically editing a file a human maintains.

### Validate complex/versioned formats against a JSON schema (in tests + editor)

For richly structured or versioned config formats, maintain a JSON schema for the file and assert in a unit test that the YAML files conform to it via `validate_yaml_matches_schema` (`recidiviz/utils/yaml_dict_validator.py`). Authored YAML files reference that schema with a top-of-file pragma (`# yaml-language-server: $schema=...`) so editors validate as you type. For simple configs the typed `pop*` accessors plus the empty check are sufficient — don't add a schema unless the format's complexity warrants it.

### Resolve config file paths from an importable package

Discover YAML config files by deriving their directory from the nearest importable package (`os.path.dirname(module.__file__)`) and globbing, never relative `..` chains — this is the Filesystem paths rule above, applied to config discovery.

### Testing YAML parsing

When a class of YAML files all parse into the same model, write two complementary kinds of tests.

**Fixture tests** exercise the parsing logic against small, controlled YAML:

- Store fixture YAML files in a `fixtures/` directory next to the test and load them with the `fixtures.as_filepath(...)` helper, then call the model's `from_yaml` factory on the returned path.
- Test each malformed case — wrong type, missing required key, unexpected/extra key, bad enum value, duplicate entry — with its own fixture (or inline input) and `assertRaisesRegex` against an anchored `^...$` regex matching the precise error message.
  ```python
  with self.assertRaisesRegex(
      ValueError, r"^'NOT_A_VALID_TYPE' is not a valid ValidationExclusionType$"
  ):
      ValidationRegionConfig.from_yaml(
          fixtures.as_filepath("us_xx_validation_config_bad_exclusion_type.yaml")
      )
  ```
- For the happy path, assert the whole parsed object equals a fully built expected instance (relying on attrs `__eq__`) rather than spot-checking individual fields.
  ```python
  config = ValidationRegionConfig.from_yaml(
      fixtures.as_filepath("us_xx_validation_config.yaml")
  )
  self.assertEqual(
      ValidationRegionConfig(region_code="US_XX", exclusions=expected_exclusions, ...),
      config,
  )
  ```
- Where the model has serialization methods (e.g. `to_dict` or a YAML dump path), add a round-trip test that serializes and re-parses (or parses and re-serializes) and asserts equality.

**A parse-all-real-files test** guards every real config of that shape:

- Discover the real files by looping over `StateCode` / `get_existing_region_codes()` and calling the production collector (which resolves paths from an importable package), and assert each parses without raising. Validity is usually enforced implicitly by the attrs validators and the parsing logic, so a clean parse is the assertion.
  ```python
  def test_load_all_configs(self) -> None:
      for state_code in StateCode:
          # Raises if any real config for this state fails to parse or validate.
          collect_document_collection_configs(state_code)
  ```
- Where it matters, also assert cross-file invariants that no single file can enforce — uniqueness across files, no vestigial/extraneous files, naming/suffix rules — using an explicit allowlist constant (with `TODO(#...)` references for known exceptions) and a failure message that says how to fix it.
- Add a schema-conformance test that validates every real file against the maintained JSON schema via `validate_yaml_matches_schema`, iterating with `subTest(yaml_file=...)` so one bad file doesn't mask the rest.
- For config types that live under `recidiviz/ingest/direct/regions/` (where the `fake_regions` test module provides a fake `US_XX` region), include the fake region's files in these tests so features exercised only in the test state still get coverage. This does not apply to config types that have no such fake-region module.

**Conventions:**

- Don't re-test extra/unused-key handling in per-config tests — that behavior is enforced and tested once at the `YAMLDict` layer (pop every key, then assert the dict is empty). Per-config tests rely on that, plus the schema-conformance test.
- Name tests `test_<scenario>` (e.g. `test_parse_bad_exclusion_type`); name the parse-all tests with an `all` marker (e.g. `test_load_all_configs`, `test_validate_all_raw_yaml_schemas`).

## attrs classes

When defining an attrs class, follow these conventions:

### Use the modern `@attr.define` decorator

Always use `@attr.define` (not `@attr.s`, which is the legacy API). Default to `@attr.define(frozen=True, kw_only=True)`: `frozen=True` makes the instance immutable, and `kw_only=True` forces call sites to name each argument, which keeps them readable as the field list grows. Only stray from this default when there is a strong reason to (e.g. the class genuinely needs to be mutated after construction).

```python
import attr

from recidiviz.common import attr_validators


@attr.define(frozen=True, kw_only=True)
class MyClass:
    ...
```

#### Inheriting from an existing `@attr.s` class

When adding a class that inherits from a legacy `@attr.s` parent, match the parent's exact
decorator and arguments (e.g. `@attr.s(eq=False, kw_only=True, frozen=True)`) rather than
using `@attr.define`. A mixed-decorator hierarchy gains nothing — the slots benefit of
`@attr.define` is lost when the parent isn't slotted — while making the chain inconsistent.
Migrating existing `@attr.s` hierarchies to `@attr.define` is tracked in
[#84065](https://github.com/Recidiviz/pulse-data/issues/84065); don't migrate a parent
ad hoc as a side effect of adding a child.

### Validate every field as strictly as is reasonable

Every field should have a `validator` that is as strict as is reasonable. Prefer a validator from [`recidiviz/common/attr_validators.py`](../../recidiviz/common/attr_validators.py) (generic types), [`recidiviz/common/recidiviz_attr_validators.py`](../../recidiviz/common/recidiviz_attr_validators.py) (Recidiviz domain types), or [`recidiviz/big_query/big_query_attr_validators.py`](../../recidiviz/big_query/big_query_attr_validators.py) (BigQuery types) where one exists. If a good validator does not exist, add one to the appropriate module rather than inlining a one-off check.

Common validator patterns we use:

```python
@attr.define(frozen=True, kw_only=True)
class Example:
    """A class that demonstrates the validator patterns we use."""

    # Simple type validators
    name: str = attr.ib(validator=attr_validators.is_str)
    count: int = attr.ib(validator=attr_validators.is_int)
    created_at: datetime.datetime = attr.ib(validator=attr_validators.is_datetime)

    # Optional fields use a dedicated optional validator rather than dropping
    # validation entirely.
    error_message: str | None = attr.ib(validator=attr_validators.is_opt_str)

    # Stricter-than-type validators: enforce semantic constraints, not just the type.
    retry_count: int = attr.ib(validator=attr_validators.is_positive_int)

    # Enum membership.
    status: SomeStatusEnum = attr.ib(validator=attr.validators.in_(SomeStatusEnum))

    # Instances of a non-primitive class.
    address: BigQueryAddress = attr.ib(
        validator=attr.validators.instance_of(BigQueryAddress)
    )

    # Typed collections. Compose multiple validators in a list when you need to
    # enforce more than one constraint (e.g. non-empty AND element type).
    primary_key_columns: list[SchemaField] = attr.ib(
        validator=[
            attr_validators.is_non_empty_list,
            attr_validators.is_list_of(SchemaField),
        ]
    )

    # Bounded dates.
    effective_date: datetime.date = attr.ib(
        validator=attr_validators.is_reasonable_past_date(
            min_allowed_date_inclusive=datetime.date(1900, 1, 1)
        )
    )
```

### Document the class and every field

Attrs classes should have both class-level documentation and field-level documentation. Field-level documentation should live as a docstring directly **under** the field (a bare string literal following the `attr.ib`) so that IDEs surface it on hover. (See the docstring-under-the-member style used in [`recidiviz/common/constants/identity.py`](../../recidiviz/common/constants/identity.py).)

```python
@attr.define(frozen=True, kw_only=True)
class DocumentBatch:
    """A batch of documents within a temp table to process, passed between
    Airflow tasks during a document upload run."""

    collection_name: str = attr.ib(validator=attr_validators.is_str)
    """Name that uniquely identifies the document collection within a state."""

    batch_number: int = attr.ib(validator=attr_validators.is_positive_int)
    """1-indexed position of this batch within the run."""
```

### Enforce cross-field invariants in `__attrs_post_init__`

Single-field constraints belong in that field's `validator`. Any invariant that spans more than one field — something no single-field validator can express — should be enforced in `__attrs_post_init__`, raising a descriptive `ValueError` when violated.

```python
@attr.define(frozen=True, kw_only=True)
class DocumentCollectionConfig:
    """Configuration for a document collection."""

    name: str = attr.ib(validator=attr_validators.is_str)
    """Name that uniquely identifies the collection within a state."""

    primary_key_columns: list[SchemaField] = attr.ib(
        validator=attr_validators.is_list_of(SchemaField)
    )
    """Columns that uniquely identify a document within this collection."""

    other_metadata_columns: list[SchemaField] = attr.ib(
        validator=attr_validators.is_list_of(SchemaField)
    )
    """Additional non-key metadata columns stored alongside each document."""

    def __attrs_post_init__(self) -> None:
        col_names = [
            col.name
            for col in self.primary_key_columns + self.other_metadata_columns
        ]
        duplicate_names = {n for n in col_names if col_names.count(n) > 1}
        if duplicate_names:
            raise ValueError(
                f"Document collection [{self.name}] has duplicate column names: "
                f"{duplicate_names}."
            )
```

### Derive values with `@property`, don't store them

Expose values that can be computed from other fields as a `@property` rather than storing them as a field. This keeps the constructor minimal and the instance a single source of truth (no risk of a stored value drifting out of sync with the fields it was derived from).

```python
@attr.define(frozen=True, kw_only=True)
class DocumentUploadResult:
    """The outcome of uploading a single document to GCS."""

    document_contents_id: str = attr.ib(validator=attr_validators.is_str)
    """Identifier of the uploaded document's contents row."""

    error_message: str | None = attr.ib(validator=attr_validators.is_opt_str)
    """The failure reason, or None if the upload succeeded."""

    @property
    def status(self) -> str:
        return (
            DOCUMENT_UPLOAD_SUCCESS
            if self.error_message is None
            else DOCUMENT_UPLOAD_FAILURE
        )
```

### Serialize across boundaries with explicit `to_dict`/`from_dict`

When a value object has to cross a serialization boundary (e.g. it is passed between Airflow tasks or processes), hand-write `to_dict`/`from_dict` rather than relying on reflection-based serialization — this gives you control over how non-primitive fields are encoded and decoded. Note how `state_code` below is written out as its string `value` and reconstructed back into a `StateCode` on the way in; reflection-based serialization can't reliably round-trip rich types like this on its own. Inside `from_dict`, use `assert_type()` from `recidiviz.utils.types` to narrow union-typed dict values to the concrete field type instead of adding defensive branches.

```python
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.types import assert_type


@attr.define(frozen=True, kw_only=True)
class DocumentUploadBatch:
    """A batch of documents within a temp table to process, passed between
    Airflow tasks during a document upload run."""

    state_code: StateCode = attr.ib(validator=attr.validators.instance_of(StateCode))
    """The state this batch of documents belongs to."""

    collection_name: str = attr.ib(validator=attr_validators.is_str)
    """Name that uniquely identifies the document collection within a state."""

    batch_number: int = attr.ib(validator=attr_validators.is_positive_int)
    """1-indexed position of this batch within the run."""

    def to_dict(self) -> dict[str, str | int]:
        return {
            "state_code": self.state_code.value,
            "collection_name": self.collection_name,
            "batch_number": self.batch_number,
        }

    @staticmethod
    def from_dict(data: dict[str, str | int]) -> "DocumentUploadBatch":
        return DocumentUploadBatch(
            state_code=StateCode(assert_type(data["state_code"], str)),
            collection_name=assert_type(data["collection_name"], str),
            batch_number=assert_type(data["batch_number"], int),
        )
```
