# Ingest Process Documentation

This document describes the Recidiviz data ingest process, which transforms raw
criminal justice data from various states into normalized entities for analysis
and applications.

## Overview

The Recidiviz ingest system is a multi-stage pipeline that:

1. **Raw Data Import**: Receives and stores raw data files from states
2. **Ingest Views**: Transforms raw data into standardized intermediate format
3. **Normalization**: Maps transformed data to Recidiviz entities and applies
   business logic
4. **Downstream Processing**: Feeds normalized data to calc, sessions, and
   metrics processes

## 1. Raw Data Import Process

### Data Reception

Raw data arrives through various methods depending on the state jurisdiction:

- **SFTP (Secure File Transfer Protocol)**: Many states use SFTP with custom
  delegates (e.g., `us_mi_sftp_download_delegate.py`)
- **Direct Transfers**: Some states provide data through direct file transfers
  to GCS or other delivery methods

### Storage in Google Cloud Storage (GCS)

Files are stored in GCS buckets with organized naming:

- **Unprocessed**: `unprocessed_` prefix for newly arrived files. These will be
  in an ingest bucket that looks like
  `recidiviz-{staging,123}-direct-ingest-state-us-xx` depending on the state and
  the environment (staging or prod).
- **Processed**: `processed_` prefix after successful import. These will be in
  the storage bucket. An example GCS URI is
  `gs://recidiviz-staging-direct-ingest-state-storage/us_nd/raw/2025/09/15/processed_2025-09-15T07:15:20:953553_raw_docstars_contacts.csv`

### File Configuration

Each raw data file type is defined by a **YAML configuration**. A simple example
is below:

```yaml
file_tag: ADH_ARREST_INFO
file_description: "Arrest information for people under jurisdiction"
primary_key_cols: [arrest_info_id]
columns:
  - name: arrest_info_id
    description: "Database generated ID for arrest record"
  - name: arrest_date
    field_type: datetime
    datetime_sql_parsers:
      - "SAFE.PARSE_DATETIME('%b %d %Y %I:%M %p', REGEXP_REPLACE({col_name},
        r'\\:\\d\\d\\d', ''))"
```

### Raw Data Migrations

Handle schema changes and data quality issues:

- **Update Migrations**: Fix inconsistent data based on filters
- **Delete Migrations**: Remove problematic records
- **Temporal Filtering**: Apply changes only to specific date ranges

Example migration:

```python
UpdateRawTableMigration(
    filters=[("offender_sentence_id", "FAKE")],
    updates=[("offender_id", "NEW_FAKE")]
)
```

## 2. Ingest Views Process

### Purpose

**Ingest views** are SQL-based transformations that serve as the bridge between
raw state-specific data and standardized Recidiviz entities.

### Structure

Located in `recidiviz/ingest/direct/regions/{state}/ingest_views/`. A simple
example is below:

```python
VIEW_QUERY_TEMPLATE = """
WITH
base_data AS (
    SELECT
        LPAD(offender_number, 7, "0") AS normalized_id,
        last_name,
        first_name,
        birth_date
    FROM {ADH_OFFENDER_BOOKING}  -- Raw table reference
    WHERE date_field IS NOT NULL
)
SELECT
    normalized_id,
    CONCAT(first_name, ' ', last_name) AS full_name,
    birth_date
FROM base_data
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mi",
    ingest_view_name="state_person",
    view_query_template=VIEW_QUERY_TEMPLATE,
)
```

### Key Features

- **Raw Table Dependencies**: `{TABLE_NAME}` syntax automatically expands to
  full BigQuery references
- **Data Cleaning**: Handle state-specific formatting and validation
- **Deduplication**: Remove or prioritize duplicate records
- **Business Logic**: Apply state-specific rules and transformations
- **Historical Processing**: Raw table references can use modifiers to control
  how data is filtered. See "Raw Table Modifiers" section below.

### Raw Table Modifiers

Ingest views can use modifiers to control how raw data is queried:

| Modifier            | Historical Versions | Includes Deleted? | Exposes `is_deleted`? | Exposes `update_datetime`? |
| ------------------- | ------------------- | ----------------- | --------------------- | -------------------------- |
| (none) / `@LATEST`  | No (latest only)    | No                | No                    | No                         |
| `@ALL`              | Yes                 | No                | No                    | Yes                        |
| `@ALL_WITH_DELETED` | Yes                 | Yes               | **Yes**               | Yes                        |

**`@LATEST` (default):** Returns only the most recent version of each row,
filtering out deleted rows.

```sql
SELECT * FROM {offenders}  -- Same as {offenders@LATEST}
```

**`@ALL`:** Returns all historical versions of rows (filtered to non-deleted).
Useful for tracking changes over time.

```sql
SELECT * FROM {offenders@ALL}
```

**`@ALL_WITH_DELETED`:** Returns all historical versions INCLUDING deleted rows.
Useful for states that send daily snapshots where deletions are implicit
(records simply disappear from subsequent files).

```sql
-- Example: Detect when someone left supervision in NC
SELECT
    OPUS as person_id,
    CDBGDTSP as supervision_start_date,
    is_deleted,
    update_datetime,
    -- When is_deleted changes from False to True, that's the supervision end date
    LAG(is_deleted) OVER (PARTITION BY OPUS, CDBGDTSP ORDER BY update_datetime) as prev_deleted_status
FROM {offenders@ALL_WITH_DELETED}
WHERE is_deleted = True  -- Only show deletions
```

**Important:** Both `@ALL` and `@ALL_WITH_DELETED` require exemption from manual
raw data pruning. This is enforced via validation and must be configured in the
raw data YAML file.

## 3. Entity Mapping

### Ingest Mappings

**YAML mapping files** in
`recidiviz/ingest/direct/regions/{state}/ingest_mappings/` define how ingest
view outputs map to Recidiviz entities. An example mapping is below:

```yaml
output:
  StatePerson:
    external_ids:
      - StatePersonExternalId:
          external_id: offender_number
          id_type: $literal("US_MI_DOC")
    full_name:
      $person_name:
        $given_names: first_name
        $middle_names: middle_name
        $surname: last_name
    gender:
      $enum_mapping:
        $raw_text: gender_code
        $mappings:
          StateGender.MALE: "M"
          StateGender.FEMALE: "F"
```

### Entity Structure

Standardized entities are defined in
`recidiviz/persistence/entity/state/entities.py`:

```python
@attr.s(eq=False, kw_only=True)
class StatePerson(HasMultipleExternalIdsEntity, RootEntity):
    full_name: Optional[str] = attr.ib(default=None)
    birthdate: datetime.date | None = attr.ib(default=None)
    gender: Optional[StateGender] = attr.ib(default=None)

    # Relationships to child entities
    external_ids: List["StatePersonExternalId"] = attr.ib(factory=list)
    races: List["StatePersonRace"] = attr.ib(factory=list)
    incarceration_periods: List["StateIncarcerationPeriod"] = attr.ib(factory=list)
    supervision_periods: List["StateSupervisionPeriod"] = attr.ib(factory=list)
```

## 4. Normalization Process

After entities are created through ingest mappings, they undergo state-specific
normalization to ensure data quality and apply business logic that can't be
handled in SQL alone.

### State-Specific Normalization Delegates

Each state has normalization delegates in
`recidiviz/pipelines/utils/state_utils/us_xx/` that handle:

#### General Normalization (`us_xx_normalization_delegate.py`)

- **External ID Selection**: When a person has multiple external IDs of the same
  type, delegates define which should be the "display" ID (shown in UI) vs
  "stable" ID (used for persistent references)
- **Entity Generation**: Some entities are only created during normalization,
  not directly from ingest views

Example from Michigan:

```python
def select_display_id_for_person_external_ids_of_type(self, ...):
    if id_type == US_MI_DOC_BOOK:
        return select_alphabetically_highest_person_external_id(
            person_external_ids_of_type
        )
```

#### Entity-Specific Normalization

Each entity type can have specialized normalization logic:

**Supervision Period Normalization**
(`us_xx_supervision_period_normalization_delegate.py`):

- Drop invalid periods (e.g., investigation periods that shouldn't be treated as
  supervision)
- Override supervision levels based on state-specific business rules
- Handle missing data with appropriate defaults

Example from Michigan:

```python
def supervision_level_override(self, ...):
    # If supervision level is missing and period doesn't end in discharge,
    # assume person is in custody
    if (sp.supervision_level is None and
        sp.termination_reason not in SUCCESSFUL_TERMINATIONS):
        return StateSupervisionLevel.IN_CUSTODY
```

**Incarceration Period Normalization**
(`us_xx_incarceration_period_normalization_delegate.py`):

- Infer admission reasons (e.g., distinguish between revocations and other
  admissions from supervision)
- Standardize specialized purposes for incarceration
- Handle complex business logic around period relationships

Example from Arkansas:

```python
def normalize_period_if_commitment_from_supervision(self, ...):
    # Analyze supervision history to determine if admission was a revocation
    if _is_revocation_admission(...):
        return deep_entity_update(period,
               admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION)
```

### Apache Beam Normalization Pipeline

The normalization process runs through several Apache Beam stages:

1. **GenerateIngestViewResults**: Execute ingest view queries and create initial
   entities
2. **MergeIngestViewRootEntityTrees**: Combine related entities into entity
   trees
3. **NormalizeRootEntities**: Apply state-specific normalization logic using
   delegates
4. **MergeRootEntitiesAcrossDates**: Handle historical data merging and entity
   evolution
5. **ClusterRootExternalIds**: Link and deduplicate entities across different
   data sources

### Key Normalization Features

- **State-Specific Business Logic**: Each delegate can implement complex rules
  that reflect how that state's criminal justice system works
- **Data Quality Improvements**: Fix common data issues that can't be resolved
  in SQL
- **Entity Relationships**: Establish proper relationships between periods,
  sentences, and other entities
- **Historical Consistency**: Ensure data makes sense across time periods and
  system changes
- **Deduplication**: Handle cases where the same logical entity appears multiple
  times in raw data

## 5. Complete Data Flow Example

```
1. Raw Data (SFTP) → GCS Storage
   us_mi_ADH_OFFENDER_BOOKING.csv: "12345,John,Doe,M,1990-01-15"

2. Ingest View (SQL Transformation)
   SELECT LPAD("12345", 7, "0") AS offender_number, "John" AS first_name, "Doe" AS last_name

3. Ingest Mapping (YAML → Entity)
   StatePerson(
     external_ids=[StatePersonExternalId(external_id="0012345", id_type="US_MI_DOC")],
     full_name="John Doe",
     gender=StateGender.MALE
   )

4. Normalization Pipeline (Apache Beam + State Delegates)
   - Apply state-specific business logic via normalization delegates
   - Handle external ID selection and entity relationships
   - Validate data quality and fix common issues
```

## 6. Downstream Processing

The normalized data feeds into several downstream systems:

### Calculator (`calc`)

- **Metrics Calculation**: Recidivism rates, population counts, program outcomes
- **Session Assignment**: Determines supervision and incarceration sessions
- **Event Processing**: Handles admissions, releases, violations

### Sessions Processing

- **Incarceration Sessions**: Continuous periods in custody with consistent
  attributes
- **Supervision Sessions**: Community supervision periods with violation
  tracking
- **Session Deduplication**: Handles overlapping or conflicting period data

### Metrics Generation

- **Population Snapshots**: Daily counts by facility, supervision district
- **Outcome Metrics**: Success rates, recidivism measurements
- **Operational Metrics**: Capacity utilization, caseload distributions

### Workflows Views

Critical views for frontend applications:

- **`workflows_views.client_record`**: Person-level data for case management
- **`workflows_views.resident_record`**: Facility resident information These
  views aggregate normalized data for efficient frontend queries.

## Key Benefits

1. **State Agnostic**: Common entity model works across all jurisdictions
2. **Data Quality**: Multiple validation layers ensure clean, consistent data
3. **Flexibility**: YAML mappings allow state-specific customization without
   code changes
4. **Auditability**: Full lineage from raw data to final entities
5. **Scalability**: Apache Beam handles large-scale parallel processing
6. **Type Safety**: Strong typing with validation prevents data corruption

## 7. Testing Ingest Views and Mappings

Ingest tests live in `recidiviz/tests/ingest/`. See
`recidiviz/tests/ingest/CLAUDE.md` for more details.

## Important Notes

- Raw data configurations in `recidiviz/ingest/direct/regions/{state}/raw_data/`
- Ingest views in `recidiviz/ingest/direct/regions/{state}/ingest_views/`
- Ingest mappings in `recidiviz/ingest/direct/regions/{state}/ingest_mappings/`
- State codes follow `US_XX` format (US_ME = Maine, US_OZ = test state)
- Never access Maine (US_ME) or California (US_CA) data without explicit
  approval
