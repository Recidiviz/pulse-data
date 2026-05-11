# Ingest Test Documentation

## Test Structure

Tests for ingest views live in
`recidiviz/tests/ingest/direct/regions/{state}/ingest_views/`. Each view has a
corresponding test file:

- View: `view_SentencesAndCharges.py`
- Test: `view_SentencesAndCharges_test.py`

Test classes extend `StateIngestViewAndMappingTestCase` and follow this pattern:

```python
class SentencesAndChargesTest(StateIngestViewAndMappingTestCase):
    __test__ = True

    @classmethod
    def state_code(cls) -> StateCode:
        return StateCode.US_CO

    @classmethod
    def ingest_view_builder(cls) -> DirectIngestViewQueryBuilder:
        return VIEW_BUILDER

    def test_SentencesAndCharges__for__basic(self) -> None:
        """Tests a basic sentence with a single charge."""
        self.run_ingest_view_test()
```

## Test Naming Convention

Test methods must follow the pattern: `test_{ViewName}__for__{characteristic}`

- The `{characteristic}` determines which fixture files are used
- Example: `test_SentencesAndCharges__for__basic` uses `basic.csv` fixtures

## Fixture Files

Tests require three types of fixtures in
`recidiviz/tests/ingest/direct/direct_ingest_fixtures/{state}/`:

1. **Raw Data Fixtures** (`us_xx_raw_data/{table_name}/{characteristic}.csv`):

   - Must include metadata columns: `file_id`, `update_datetime`, `is_deleted`
   - All rows with the same `update_datetime` must have the same `file_id`
   - Column order must match the raw data YAML configuration

2. **Ingest View Results**
   (`us_xx_ingest_view_results/{ViewName}/{characteristic}.csv`):

   - Expected output of the SQL view query
   - Auto-generated when running tests with `create_expected_output=True`

3. **Mapping Output Fixtures**
   (`__ingest_mapping_output_fixtures__/{ViewName}/{characteristic}.txt`):
   - Expected entity tree output from the YAML mapping
   - Auto-generated when running tests with `create_expected_output=True`

## Generating Input Fixtures

**Always use the `generate_ingest_view_fixtures_for_test` script to create raw
data fixtures.** Do not craft them by hand — the script handles PII obfuscation
and column alignment correctly. Only create fixtures manually if the test
requires data that cannot exist in production (e.g., intentionally malformed
records).

Workflow:
1. Use `bq` to find a real `external_id_value` that exhibits the behavior you
   want to test.
2. Confirm the specific example you find with the user. Explain why this is a
   good entity to use for this test case.
3. Run the script with `--skip_prompts` to generate fixtures non-interactively.
4. After the script finishes, remind the user to add the entry to
   [go/fixture-pii](https://go/fixture-pii).

Example BQ query to find examples (adapt for your test):
```
select *
from us_nc_raw_data.offenders
qualify count(distinct POSITIVE) over (partition by CDDORNUM) > 1
```

Example script invocation:
```
uv run python -m recidiviz.tools.ingest.testing.ingest_fixture_creation.generate_ingest_view_fixtures_for_test \
    --state_code US_NC --external_id_type US_NC_OPUS \
    --external_id_values <REDACTED> --ingest_view_name drug_screen \
    --test_characteristic changing_results --skip_pruning \
    --skip_code_files --skip_prompts
```

This will pull actual examples from real raw data, obfuscating columns marked with
`is_pii` in the raw data configs.

## Generating Output Fixtures

To auto-generate expected output fixtures:

```python
def test_SentencesAndCharges__for__basic(self) -> None:
    self.run_ingest_view_test(create_expected_output=True)
```

Run the test once with this flag, then remove it for normal test runs.

## Integration Tests

Each state has an integration test (`us_xx_pipeline_integration_test.py`) that
runs the full ingest pipeline with all fixtures. Key considerations:

1. **Entity Consistency**: If multiple views create the same entity type (e.g.,
   `StateSentence`), data must be consistent across views. A sentence referenced
   in one view must have complete data from other views.

2. **Required Fields**: Some entity fields are required by validation (e.g.,
   `StateSentence` requires `sentence_type` and `sentencing_authority`). Ensure
   all views that create an entity populate required fields.

3. **Validation Rules**: The pipeline enforces business logic:
   - Terminating statuses (COMPLETED, VACATED, etc.) must be the final status in
     a sequence
   - External ID types must be registered in `external_id_types.py`

## Common Issues

1. **External ID Type Not Found**: The `id_type` in YAML mappings must match a
   registered type in `recidiviz/common/constants/state/external_id_types.py`.
   Check existing types for the state (e.g., `US_CO_OFFENDERID`).

2. **CSV Column Misalignment**: When creating raw data CSVs manually, use
   Python's `csv.DictWriter` to ensure proper column alignment:

   ```python
   import csv
   with open('fixture.csv', 'w', newline='') as f:
       writer = csv.DictWriter(f, fieldnames=columns)
       writer.writeheader()
       writer.writerow(data)
   ```

3. **Integration Test Failures**: If entities from different views reference the
   same person/sentence, ensure:

   - The person exists in all required raw data tables
   - Supporting tables have matching data for JOINs to succeed
   - Status sequences are valid (no terminating status before final)

4. **Multiple file_ids per update_datetime**: Each `update_datetime` value must
   have exactly one `file_id`. All rows from the same "file import" should share
   the same `file_id`.
