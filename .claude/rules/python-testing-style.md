---
paths:
  - "recidiviz/tests/**/*.py"
  - "recidiviz/airflow/tests/**/*.py"
---

# Python Testing Style

These rules apply whenever writing or modifying test code (anything under
`recidiviz/tests/` or `recidiviz/airflow/tests/`). They extend the general
rules in [`python-style.md`](./python-style.md), which apply to test code
too.

## Test names and docstrings

- Tests live in `recidiviz/tests/` and mirror the source path: `recidiviz/big_query/big_query_view.py` is tested in `recidiviz/tests/big_query/big_query_view.py`.
- Name each test method `test_<scenario>`, where the scenario names the exact behavior or case under test: `test_only_sessions_with_no_end_date_are_open`, `test_parse_bad_exclusion_type` — not `test_process`, `test_happy_path`, or `test_case_2`. When a test fails in CI, its name should say what broke.
- Give every test class a docstring saying what it tests, e.g. `"""Tests for AssignmentsByTimePeriodViewBuilder"""`. Give an individual test method a docstring only when the *why* isn't clear from its name (e.g. the scenario guards against a specific regression).

## Use fake state codes

- Use `US_XX` when testing generic (non-state-specific) functionality, `US_YY` when a test needs a second state, and the other `TEST_STATE_CODE_*` constants in `recidiviz/common/constants/states.py` for their documented niches (`US_WW` for docs-generation tests, etc.).
- Never use a real state's code in a test of generic functionality — it falsely implies the behavior is state-specific, and it can collide with real region configs.
- `US_OZ` is not a test placeholder either: it is the playground/demo state (`PLAYGROUND_STATE_CODE`) with a real region directory. Don't use it in unit tests unless explicitly instructed.

## Avoid logic in tests: tests read as input → output stories

Avoid logic in tests, like loops, conditionals, and string building. Write each test so it reads top to bottom as a story: the input values that matter to the case sit inline in the test body, and the expected output is spelled out as full literal rows or objects. Prefer to state a test's inputs and outputs directly rather than constructing them with programming logic. A loop that constructs an expected output may be more compact, but it's more brittle and harder to read — and when the logic mirrors the implementation, it can break the same way the implementation does and the test will keep passing. Tests aren't themselves tested, so a reader is the only thing checking them.

```python
# Bad — computes the expectation with a loop:
expected = [column.name for column in _make_test_columns()]
self.assertEqual(expected, view.schema_column_names)

# Good — a literal:
self.assertEqual(["person_id", "state_code", "start_date"], view.schema_column_names)
```

Corollaries:

- **Expected outputs are always literals.** Never build the expected value through a builder helper, and never assert through a helper that projects or reorders a subset of columns — a hidden default often also hides a missing test case. When it isn't obvious why an expected row is (or isn't) present, say so in a comment on that row (`# For person_id=1234, the assignment applied to Sept 2024, but not Oct 2024`).
- **Helpers may hide plumbing, never values.** A file-local helper that encapsulates table creation and schema boilerplate is fine when it takes the input rows verbatim (`self._create_assignments_table(..., data=input_assignments)`). A helper that fills in default field values hides part of the input from the reader — spell out every input row in full in the test body. See `TestAssignmentsByTimePeriodViewBuilder` in `recidiviz/tests/aggregated_metrics/assignments_by_time_period_view_builder_test.py` for the full pattern.
- **Vary only the field under test.** The setup must make the behavior under test the only path to the asserted result: when a test claims that one field's difference changes the outcome, the input rows should differ in only that field (plus the identifier that names each row). If the rows differ in several meaningful fields at once, a passing test can't tell which difference actually drove the outcome.

  ```python
  # Testing that closed sessions are excluded.
  # Bad — rows differ in both end_date and supervision_level, so this passes
  # even if the query is (wrongly) filtering on supervision_level:
  [
      {"person_id": 1, "supervision_level": "MEDIUM", "end_date": None},
      {"person_id": 2, "supervision_level": "HIGH", "end_date": "2024-01-15"},
  ]

  # Good — identical except end_date, the field under test:
  [
      {"person_id": 1, "supervision_level": "MEDIUM", "end_date": None},
      {"person_id": 2, "supervision_level": "MEDIUM", "end_date": "2024-01-15"},
  ]
  ```
- **Genuine shared logic goes in a named helper, not the test body.** `BigQueryEmulatorTestCase` (below) is the model: it consolidates emulator setup, table creation, query running, and result comparison, so each test body states only its input rows and its expected output.

## Assert the whole output, not fragments

When a function returns a string, assert the complete expected value with `assertEqual` rather than a chain of `assertIn` checks against fragments. A series of `assertIn`s doesn't check ordering, doesn't catch unexpected extra content, and isn't as easy to read as an `assertEqual` with a single complete string.

```python
# Prefer this:
expected = """CASE
  WHEN expr = 'A' THEN '1'
  ELSE expr
END"""
self.assertEqual(expected, build_case_expression("expr", {"A": "1"}))

# over spot-checking fragments:
result = build_case_expression("expr", {"A": "1"})
self.assertIn("CASE", result)
self.assertIn("WHEN expr = 'A' THEN '1'", result)
self.assertIn("ELSE expr", result)
```

The same applies to parsed objects: assert the whole parsed object equals a fully built expected instance rather than spot-checking individual fields. Reserve `assertIn` for genuinely partial checks against large output you do not control, e.g. an error message whose full content can vary and/or doesn't need to be tested.

## Asserting exceptions

Assert expected exceptions with `self.assertRaisesRegex(ExceptionType, r"^...$")` around the call, anchoring the regex so the whole message is checked. Never wrap assertions in a `try`/`except` that could swallow a failure, and never use `try`/`except` plus `self.fail(...)` in place of `assertRaisesRegex`.

```python
with self.assertRaisesRegex(
    ValueError, r"^Found duplicate column \[person_id\] in collection \[notes\]$"
):
    DocumentCollectionConfig.from_yaml(path)
```

## Testing SQL with the BigQuery emulator

When a function generates SQL that computes a value, test it by running the generated query against real input rows and asserting the computed output rows — substring assertions on the query text don't check that the SQL is even valid, let alone correct. Substring/equality assertions on query text are acceptable only for logic with no computed result to check (e.g. a helper that splices a clause). Conversely, don't reach for the emulator when plain unittest can check the behavior: emulator tests require Docker and are much slower.

Subclass `BigQueryEmulatorTestCase` (`recidiviz/tests/big_query/big_query_emulator_test_case.py`). Its key methods — `create_mock_table(address, schema)`, `load_rows_into_table(address, data)`, and `run_query_test(query_str, expected_result)` — keep each test body to input rows plus literal expected rows:

```python
class OpenSessionsViewTest(BigQueryEmulatorTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.create_mock_table(_SESSIONS_ADDRESS, _SESSIONS_SCHEMA)

    def test_only_sessions_with_no_end_date_are_open(self) -> None:
        self.load_rows_into_table(
            _SESSIONS_ADDRESS,
            [
                {"person_id": 1, "state_code": "US_XX", "end_date": None},
                {"person_id": 2, "state_code": "US_XX", "end_date": "2024-01-15"},
            ],
        )
        self.run_query_test(
            f"SELECT person_id FROM ({VIEW_BUILDER.build().view_query})",
            expected_result=[{"person_id": 1}],
        )
```

- **Share table setup, not expectations.** When several test files exercise views over the same tables, put the `create_mock_table`/`load_rows_into_table` boilerplate in shared helpers that take the test case as an argument — see `recidiviz/tests/task_eligibility/task_eligibility_big_query_emulator_utils.py` (e.g. `load_data_for_task_criteria_view(emulator, view_builder, criteria_data)`) — or a shared base test case with `load_*` methods. Each test still states its own rows and its own literal expected output.
- **Know the emulator's limits.** The emulator supports a large subset of BigQuery SQL, not all of it; notably its `CAST` is more lenient than real BigQuery, so an emulator test alone can hide date-cast bugs (see `recidiviz/tests/ingest/CLAUDE.md`).

## Testing YAML parsing

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

## Keep sweeps over enums and discovered cases exhaustive

- A test helper that switches on an enum must end with a `raise`, so a newly added member crashes the test instead of silently getting some default behavior (this is the test-side application of the enum-dispatch rule in `python-style.md`):

  ```python
  def fixture_file_name(fixture_type: RawDataDiffFixtureType) -> str:
      if fixture_type is RawDataDiffFixtureType.EXISTING:
          return "existing_raw_data.csv"
      if fixture_type is RawDataDiffFixtureType.NEW:
          return "new_raw_data.csv"
      raise ValueError(f"Unexpected fixture type [{fixture_type}]")
  ```

  Where a per-member expectation makes sense, loop over every member so a new one is exercised (and fails) until it's covered — see `test_getMostRelevantSupervisionType_allEnums` in `recidiviz/tests/common/constants/state/state_supervision_period_test.py`, which sweeps every `StateSupervisionPeriodSupervisionType` through a function that raises on unhandled members.

- When a test discovers its cases by enumerating classes, files, or enum members, keep the sweep exhaustive: a new dimension added to the code must extend the enumeration automatically, and any narrowing filter carries a filed `TODO(OBT-000)`-style reference (any Linear team prefix, e.g. `TN-000`) naming what is skipped.

  ```python
  # Bad — the filter silently narrows the sweep, so a config added for a new
  # state is never tested, and nothing records that US_XX is missing:
  def test_all_configs_parse(self) -> None:
      for state_code in StateCode:
          if state_code not in (StateCode.US_YY, StateCode.US_WW):
              continue
          collect_document_collection_configs(state_code)

  # Good — sweep every member; any exclusion is named and tracked:
  # TODO(OBT-12345): Add the missing US_XX fixtures and remove this skip.
  _STATES_MISSING_FIXTURES = {StateCode.US_XX}

  def test_all_configs_parse(self) -> None:
      for state_code in StateCode:
          if state_code in _STATES_MISSING_FIXTURES:
              continue
          collect_document_collection_configs(state_code)
  ```

## Never add production knobs for tests

Never add a production parameter, flag, or branch whose only caller is a test. Fix the test's setup or mocks instead. (The general-style rule that fields shouldn't be nullable or defaulted just to ease tests is the same principle.)
