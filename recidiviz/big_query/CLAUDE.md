# BigQuery Tooling and Libraries

This document provides guidance on working with Recidiviz's BigQuery
infrastructure, tooling, and libraries.

## Overview

A large portion of the Recidiviz codebase defines code for generating BigQuery
"view" queries (saved queries that act like tables). These views are deployed to
BigQuery and often materialized into tables for performance. This package
(`recidiviz/big_query/`) contains the core infrastructure for:

- **Defining views** through declarative Python code
- **Building queries** with templating and address overrides
- **Collecting views** from modules for deployment
- **Interacting with BigQuery** through a high-level client wrapper
- **Managing view dependencies** and deployment workflows

## Core Building Blocks

### BigQueryAddress

**File**: `big_query_address.py`

`BigQueryAddress` represents the `(dataset_id, table_id)` address of a BigQuery
view or table. It's an immutable attrs class that provides:

- **Creation**: `BigQueryAddress(dataset_id="my_dataset", table_id="my_table")`
  or `BigQueryAddress.from_str("my_dataset.my_table")`
- **String representation**: `.to_str()` returns `"dataset_id.table_id"`
- **Query templates**: `.select_query_template()` generates `SELECT * FROM`
  queries
- **State detection**: `.state_code_for_address()` extracts state code if this
  is a state-specific view (e.g., `us_nd_*`)
- **Project-specific conversion**: `.to_project_specific_address(project_id)`
  returns a `ProjectSpecificBigQueryAddress`

**ProjectSpecificBigQueryAddress** extends this with a `project_id` field,
representing the full `project.dataset.table` address.

### BigQueryView and BigQueryViewBuilder

**File**: `big_query_view.py`

These are the core classes for defining BigQuery views:

#### BigQueryView

An extension of `bigquery.TableReference` that represents a deployed view. Key
attributes:

- **address**: The `BigQueryAddress` where this view lives
- **view_query**: The fully-formatted SQL query for this view
- **view_query_template**: The template SQL with format placeholders (e.g.,
  `{project_id}`)
- **description**: Full description (deployed to Gitbook)
- **bq_description**: Shortened description (deployed to BigQuery)
- **schema**: Expected schema for the view, as a list of BigQueryViewColumns
- **materialized_address**: Optional address where view results are materialized
  to a table
- **table_for_query**: Returns materialized address if available, otherwise view
  address (use this when querying!)
- **clustering_fields**: Columns to cluster materialized tables by
- **time_partitioning**: Optional time partitioning config for materialized
  tables

#### BigQueryViewBuilder (and SimpleBigQueryViewBuilder)

A builder that constructs `BigQueryView` instances. Unlike `BigQueryView`,
builders can be instantiated as top-level module variables (before `project_id`
is set in metadata):

```python
VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id="my_dataset",
    view_id="my_view",
    description="Description of what this view does",
    view_query_template="""
        SELECT
            person_id,
            state_code,
            event_date
        FROM `{project_id}.source_dataset.source_table`
        WHERE event_date >= '{min_date}'
    """,
    schema=[
        String(name="person_id", description="Person for this view", mode="REQUIRED"),
        String(name="state_code", description="State code for the view", mode="REQURED"),
        Date(name="event_date", description="Date of the event", mode="NULLABLE")
    ]
    should_materialize=True,  # Materialize to my_view_materialized
    clustering_fields=["state_code", "person_id"],
    min_date="2020-01-01",  # Custom query format kwargs
)
```

The builder's `.build()` method creates the actual `BigQueryView` instance.

**Key Methods**:

- `build(sandbox_context=None)`: Constructs the view
- `build_and_print()`: Builds and prints the query to stdout (useful for
  development)
- `should_deploy_in_project(project_id)`: Checks if view should be deployed in a
  specific project

### BigQueryViewCollector

**File**: `big_query_view_collector.py`

`BigQueryViewCollector` discovers and collects `BigQueryViewBuilder` instances
from Python modules.

**Discovery Pattern**: By default, collectors look for top-level variables named
`VIEW_BUILDER` in Python files.

**Usage**:

```python
class MyViewCollector(BigQueryViewCollector[SimpleBigQueryViewBuilder]):
    def collect_view_builders(self) -> List[SimpleBigQueryViewBuilder]:
        return self.collect_view_builders_in_module(
            builder_type=SimpleBigQueryViewBuilder,
            view_dir_module=my_views_module,
            recurse=True,  # Search subdirectories
        )
```

**Key Parameters**:

- `builder_type`: The expected type of view builder
- `view_dir_module`: Python module to search
- `recurse`: Whether to search submodules
- `view_file_prefix_filter`: Only collect from files with this prefix
- `validate_builder_fn`: Custom validation function (e.g.,
  `filename_matches_view_id_validator`)

### BigQueryClient

**File**: `big_query_client.py`

`BigQueryClient` is a wrapper around Google's `bigquery.Client` with convenience
methods for common operations:

**Getting a client**:

```python
from recidiviz.big_query.big_query_client import client

bq_client = client(project_id="recidiviz-123", region="us-east1")
```

**Common operations** (documented in the class - see file for full API):

- Creating/updating views and tables
- Materializing views to tables
- Running queries
- Managing datasets
- Exporting to GCS
- Copying tables across regions

### BigQueryQueryBuilder

**File**: `big_query_query_builder.py`

`BigQueryQueryBuilder` handles query templating and address overrides. It's used
internally by `BigQueryView` to:

1. **Format templates**: Replace `{project_id}` and custom kwargs in query
   templates
2. **Apply address overrides**: Rewrite table references for sandbox/test
   contexts
3. **Handle parent formatters**: Apply custom address formatting rules

This is typically transparent to users but is critical for sandbox contexts
where views need to reference sandboxed versions of parent tables.

## Specialized View Types

### SelectedColumnsBigQueryView

**File**: `selected_columns_big_query_view.py`

Extends `BigQueryView` to enforce column ordering, useful for CSV exports:

```python
VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id="exports",
    view_id="person_export",
    view_query_template="SELECT {columns} FROM `{project_id}.source.persons`",
    columns=["person_id", "full_name", "birth_date"],  # Explicit ordering
    should_materialize=True,
)
```

The `{columns}` placeholder is automatically populated with the comma-separated
column list.

### WithMetadataQueryBigQueryView

**File**: `with_metadata_query_big_query_view.py`

Wraps another view and adds a metadata query used during export:

```python
VIEW_BUILDER = WithMetadataQueryBigQueryViewBuilder(
    delegate=base_view_builder,
    metadata_query="SELECT COUNT(*) as row_count FROM `{project_id}.dataset.table`",
)
```

### UnionAllBigQueryViewBuilder

**File**: `union_all_big_query_view_builder.py`

Builds a view that unions multiple parent views/tables:

```python
VIEW_BUILDER = UnionAllBigQueryViewBuilder(
    dataset_id="combined",
    view_id="all_states_data",
    description="Union of all state-specific tables",
    parents=[state_a_builder, state_b_builder, state_c_builder],
    clustering_fields=["state_code"],
    custom_select_statement="SELECT * EXCEPT (internal_column)",  # Optional
)
```

## Common Patterns and Workflows

### Defining a New View

1. Create a new Python file in the appropriate views directory (e.g.,
   `recidiviz/calculator/query/state/views/`)
2. Define your view builder as a top-level `VIEW_BUILDER` variable:

```python
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder

VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id="my_dataset",
    view_id="my_new_view",
    description="Clear description of what this view does",
    view_query_template="""
        SELECT
            col1,
            col2
        FROM `{project_id}.other_dataset.source_table`
        WHERE condition = '{param_value}'
    """,
    should_materialize=True,
    clustering_fields=["col1"],
    param_value="some_value",  # Custom kwargs for template
)
```

3. If in a collected directory, the view will be automatically discovered by the
   appropriate collector

### Referencing Other Views in Queries

Always use the `table_for_query` property when referencing other views, as this
will use the materialized table if available:

```python
parent_builder = SimpleBigQueryViewBuilder(...)

child_builder = SimpleBigQueryViewBuilder(
    view_query_template=f"""
        SELECT *
        FROM `{{project_id}}.{parent_builder.table_for_query.to_str()}`
    """,
)
```

### Testing Views Locally

To see the generated query for a view:

```python
if __name__ == "__main__":
    VIEW_BUILDER.build_and_print()
```

Or run: `uv run python -m recidiviz.path.to.view_file`

### Materialization

Views with `should_materialize=True` get materialized to a table with
`_materialized` suffix by default:

- View: `my_dataset.my_view`
- Materialized table: `my_dataset.my_view_materialized`

Override with `materialized_address_override` if needed.

### State-Specific Views

Views with state codes in their address (e.g., `us_nd_*` or `*_us_nd`) are
automatically detected as state-specific. This affects:

- Job labels for monitoring
- Sandbox filtering by state

## View Dependency Graphs (DAGs)

The views created in this codebase form a **directed acyclic graph (DAG)** of
dependencies, starting from source tables (like raw data or ingest outputs) and
flowing down to leaf node views (like views used for exports or dashboards).

### BigQueryViewDagWalker

**File**: `big_query_view_dag_walker.py`

`BigQueryViewDagWalker` constructs and operates on DAGs of BigQuery views. When
you provide a collection of views, it:

1. **Constructs the graph**: Analyzes each view's query to identify which
   tables/views it references (parents)
2. **Identifies structure**:
   - **Root nodes**: Views with no parent views in the DAG (only reference
     source tables)
   - **Leaf nodes**: Views that no other views depend on
3. **Validates**: Checks for cycles and throws if any are found (BigQuery view
   DAGs must be acyclic)

**Key properties**:

- `views`: All views in the DAG
- `roots`: List of `BigQueryViewDagNode` instances for root views
- `leaves`: List of `BigQueryViewDagNode` instances for leaf views
- `nodes_by_address`: Dictionary mapping `BigQueryAddress` to
  `BigQueryViewDagNode`

**Creating a DAG**:

```python
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker

# Build views from builders
views = [builder.build() for builder in view_builders]

# Create the DAG
dag = BigQueryViewDagWalker(views=views)
```

### Processing the DAG

The `process_dag` method traverses the DAG and executes a function on each view
in dependency order:

```python
def view_process_fn(
    view: BigQueryView,
    parent_results: Dict[BigQueryView, ResultType]
) -> ResultType:
    """Process a single view.

    Args:
        view: The view being processed
        parent_results: Results from processing parent views (empty for root nodes)

    Returns:
        Result of processing this view
    """
    # Do something with the view...
    return result

result = dag.process_dag(
    view_process_fn=view_process_fn,
    synchronous=False,  # Use async/multi-threaded processing
    traversal_direction=TraversalDirection.ROOTS_TO_LEAVES,  # Or LEAVES_TO_ROOTS
    failure_mode=BigQueryViewDagWalkerProcessingFailureMode.FAIL_FAST,
)
```

**Processing parameters**:

- **synchronous**:
  - `False` (default): Multi-threaded async processing (better for I/O-bound
    operations)
  - `True`: Single-threaded processing (better for CPU-bound operations)
- **traversal_direction**:
  - `ROOTS_TO_LEAVES`: Start from root nodes, process dependencies first (used
    for building/deploying views)
  - `LEAVES_TO_ROOTS`: Start from leaf nodes, process dependents first (used for
    analysis)
- **failure_mode**:
  - `FAIL_FAST`: Stop at first error
  - `FAIL_EXHAUSTIVELY`: Continue processing non-descendants of failed nodes,
    then raise all errors

**Processing guarantees**:

- Parent views are always processed before their children (when traversing
  ROOTS_TO_LEAVES)
- Processing happens level-by-level in breadth-first order
- The `view_process_fn` receives results from all parent views that were
  processed

**Result object** (`ProcessDagResult`):

```python
result.view_results  # Dict[BigQueryView, ResultType] - results for each view
result.view_processing_stats  # Dict[BigQueryView, ViewProcessingMetadata] - timing/perf stats
result.total_runtime  # Total time to process entire DAG
result.log_processing_stats(n_slowest=10)  # Log stats about the run
```

### Sub-DAGs

You can extract portions of a larger DAG:

**Ancestor sub-DAG** (all views this view depends on):

```python
# Get all ancestors of a specific view
sub_dag = dag.get_ancestors_sub_dag(views=[my_view])
```

**Descendant sub-DAG** (all views that depend on this view):

```python
# Get all descendants of a specific view
sub_dag = dag.get_descendants_sub_dag(views=[my_view])
```

**Combined sub-DAG**:

```python
sub_dag = dag.get_sub_dag(
    views=[view1, view2],
    include_ancestors=True,
    include_descendants=True,
)
```

**Union DAGs**:

```python
combined_dag = BigQueryViewDagWalker.union_dags(dag1, dag2, dag3)
```

### BigQueryViewDagNode

**File**: `big_query_view_dag_walker.py`

Each node in the DAG represents a single view with its relationships:

- **view**: The `BigQueryView` instance
- **parent_node_addresses**: Addresses of parent views in the DAG
- **child_node_addresses**: Addresses of child views in the DAG
- **source_addresses**: Tables/views referenced but not in this DAG (e.g.,
  source tables)
- **is_root**: True if this view has no parents in the DAG
- **is_leaf**: True if this view has no children in the DAG
- **ancestors_sub_dag**: Cached sub-DAG of all ancestors (set via
  `populate_ancestor_sub_dags()`)
- **descendants_sub_dag**: Cached sub-DAG of all descendants (set via
  `populate_descendant_sub_dags()`)

### BigQueryViewSubDagCollector

**File**: `big_query_view_sub_dag_collector.py`

A specialized collector that builds a sub-DAG from a full set of view builders:

```python
collector = BigQueryViewSubDagCollector(
    view_builders_in_full_dag=all_view_builders,
    view_addresses_in_sub_dag={
        BigQueryAddress.from_str("dataset.view1"),
        BigQueryAddress.from_str("dataset.view2"),
    },
    include_ancestors=True,  # Include all views these depend on
    include_descendants=False,  # Don't include views that depend on these
    datasets_to_exclude={"test_dataset"},  # Exclude certain datasets
)

sub_dag_builders = collector.collect_view_builders()
```

This is useful for:

- Deploying only a subset of views and their dependencies
- Testing impact of changes to specific views
- Analyzing view lineage

### Common DAG Use Cases

**1. Deploying views in dependency order**:

```python
dag = BigQueryViewDagWalker(views)

def deploy_view(view: BigQueryView, parent_results: Dict) -> None:
    # Deploy the view to BigQuery
    bq_client.create_or_update_view(view)

dag.process_dag(deploy_view, synchronous=False)
```

**2. Materializing views**:

```python
def materialize_view(view: BigQueryView, parent_results: Dict) -> Table:
    if view.materialized_address:
        return bq_client.materialize_view_to_table(view)
    return None

result = dag.process_dag(materialize_view, synchronous=False)
```

**3. Finding all views affected by a change**:

```python
# Find all views downstream of changed views
affected_dag = full_dag.get_descendants_sub_dag(changed_views)
print(f"Need to update {len(affected_dag.views)} views")
```

**4. Analyzing view dependencies**:

```python
# Get all source tables referenced by views
source_tables = dag.get_referenced_source_tables()

# Get all parent-child edges
edges = dag.get_edges()

# Find views between source tables and final outputs
views_in_path = dag.get_all_node_addresses_between_start_and_end_collections(
    start_source_addresses=source_table_addresses,
    start_node_addresses=set(),
    end_node_addresses=leaf_view_addresses,
)
```

### Performance Considerations

- **Async processing** is generally faster for I/O-bound operations (like
  deploying views or running queries)
- **Sync processing** may be faster for CPU-bound operations (like analyzing
  queries)
- The DAG walker tracks detailed performance metrics:
  - Processing time per view
  - Queue wait time
  - Graph depth
  - Longest execution path
  - Number of distinct paths to each node
- Use `result.log_processing_stats(n_slowest=10)` to identify bottlenecks

## Deploying and Updating Views

### Overview

The view update process is how we deploy our view graph to BigQuery in
production and staging environments. This is orchestrated via our Airflow DAGs
and uses the `create_managed_dataset_and_deploy_views_for_view_builders()`
function as the main entry point.

**File**: `view_update_manager.py`

### create_managed_dataset_and_deploy_views_for_view_builders()

This is the primary function for deploying a collection of views to BigQuery.
Called from our Airflow calculation DAG, it handles the complete lifecycle of
view deployment.

**What it does**:

1. **Builds views** from builders (applying sandbox context if provided)
2. **Creates datasets** for all views in parallel
3. **Cleans up unmanaged resources** (optional - deletes views/tables/datasets
   no longer in code)
4. **Creates/updates views** in dependency order using DAG walker
5. **Materializes views** to tables when appropriate
6. **Tracks performance** and logs slow views

**Key parameters**:

```python
def create_managed_dataset_and_deploy_views_for_view_builders(
    *,
    view_builders_to_update: Sequence[BigQueryViewBuilder],
    historically_managed_datasets_to_clean: set[str] | None,
    rematerialize_changed_views_only: bool,
    failure_mode: BigQueryViewDagWalkerProcessingFailureMode,
    view_update_sandbox_context: BigQueryViewUpdateSandboxContext | None = None,
    bq_region_override: str | None = None,
    default_table_expiration_for_new_datasets: int | None = None,
    views_might_exist: bool = True,
    allow_slow_views: bool = False,
) -> tuple[ProcessDagResult[CreateOrUpdateViewResult], BigQueryViewDagWalker]:
```

- **view_builders_to_update**: List of view builders to deploy
- **historically_managed_datasets_to_clean**: Datasets to clean up (removes
  views/tables no longer in code). If `None`, skips cleanup
- **rematerialize_changed_views_only**:
  - `True`: Only re-materialize views that have changed (or whose ancestors
    changed)
  - `False`: Always materialize all views (slower but ensures fresh data)
- **failure_mode**: `FAIL_FAST` or `FAIL_EXHAUSTIVELY`
- **view_update_sandbox_context**: Optional sandbox context for testing (see
  Sandbox Contexts below)
- **views_might_exist**: If `True`, optimistically tries to update existing
  views
- **allow_slow_views**: If `True`, doesn't fail on views that take longer than
  expected to materialize

**Returns**: `(ProcessDagResult, BigQueryViewDagWalker)` containing results and
the DAG that was processed

### View Update Process Flow

The update follows this sequence:

#### 1. Build Views

```python
views_to_update = build_views_to_update(
    candidate_view_builders=view_builders_to_update,
    sandbox_context=sandbox_context,
)
```

Builds all view objects from builders, applying sandbox transformations if
needed.

#### 2. Create Datasets

All required datasets are created in parallel before processing begins:

```python
_create_all_datasets_if_necessary(
    bq_client, managed_dataset_ids, default_table_expiration_for_new_datasets
)
```

#### 3. Clean Up Unmanaged Resources (Optional)

If `historically_managed_datasets_to_clean` is provided:

- Deletes views/tables in managed datasets that are no longer in code
- Deletes entire datasets that are no longer managed
- Ensures only code-defined resources exist in production

#### 4. Process DAG

Uses DAG walker to process views in dependency order:

```python
def process_fn(
    v: BigQueryView,
    parent_results: Dict[BigQueryView, CreateOrUpdateViewResult]
) -> CreateOrUpdateViewResult:
    return _create_or_update_view_and_materialize_if_necessary(...)

results = dag_walker.process_dag(
    process_fn,
    synchronous=False,  # Async for I/O performance
    perf_config=perf_config,  # Enforces time limits per view
    failure_mode=failure_mode,
)
```

### View Creation and Materialization

For each view, `_create_or_update_view_and_materialize_if_necessary()` performs
these steps:

#### 1. Detect Changes

Checks if the view has changed by comparing:

- **View query** changes (different SQL)
- **Schema** changes (new/removed columns)
- **Configuration** changes (clustering, partitioning)
- **Parent** changes (any ancestor view updated)

#### 2. Update View

```python
# Delete existing view (required for schema updates to reflect)
if existing_view is not None:
    bq_client.delete_table(view.address, not_found_ok=True)

# Create/update the view
updated_view = bq_client.create_or_update_view(view)
```

**Note**: Views are currently deleted and recreated to ensure schema changes
from parent tables are reflected. See TODO comments in code for tracking issues.

#### 3. Materialize if Necessary

If the view has a `materialized_address`:

```python
if (
    not rematerialize_changed_views_only
    or view_changed
    or not bq_client.table_exists(view.materialized_address)
):
    materialization_result = bq_client.materialize_view_to_table(
        view=view,
        use_query_cache=True,
        view_configuration_changed=view_configuration_changed,
    )
```

**Materialization details**:

- Runs `SELECT * FROM view` into the materialized table
- Uses `WRITE_TRUNCATE` (replace all data) if configuration changed
- Uses `WRITE_TRUNCATE_DATA` (keep table structure) otherwise
- Applies clustering fields and time partitioning from view definition
- Updates table description

#### 4. Return Status

Returns a `CreateOrUpdateViewResult` with status:

- **SUCCESS_WITH_CHANGES**: View or ancestor changed
- **SUCCESS_WITHOUT_CHANGES**: No changes detected
- **SKIPPED**: View not deployed (e.g., wrong project, parent failed)

### Sandbox Contexts

Sandbox contexts allow deploying views to alternate datasets for testing without
affecting production data.

**BigQueryViewUpdateSandboxContext** - Used at view update time:

```python
sandbox_context = BigQueryViewUpdateSandboxContext(
    output_sandbox_dataset_prefix="my_prefix",  # Views deployed to "my_prefix_<dataset>"
    input_source_table_overrides=overrides,     # Point to sandbox source tables
    state_code_filter=StateCode.US_XX,          # Only include views for this state
    parent_address_formatter_provider=None,     # Optional custom formatting
)
```

**What happens with sandbox context**:

1. **View addresses** are prefixed: `dataset.view` → `my_prefix_dataset.view`
2. **Materialized addresses** are prefixed: `dataset.view_materialized` →
   `my_prefix_dataset.view_materialized`
3. **Parent references** in queries are rewritten to point to sandbox tables
4. **Union views** are filtered to only relevant state-specific views
5. **Datasets** are created with default expiration (24 hours for sandboxes)

This enables:

- Testing changes against production-like data
- Developing state-specific features in isolation
- Running experiments without affecting production views

### Performance Configuration

View updates enforce performance limits via `ProcessDagPerfConfig`:

**File**: `view_update_config.py`

```python
# Default: 6 minutes per view (7 minutes in GCP due to contention)
_MAX_SINGLE_VIEW_MATERIALIZATION_TIME_SECONDS = 60 * 6

# Overrides for known expensive views
_ALLOWED_MATERIALIZATION_TIME_OVERRIDES: Dict[BigQueryAddress, float] = {
    BigQueryAddress(
        dataset_id="sessions",
        table_id="dataflow_sessions",
    ): (60 * 10),  # 10 minutes allowed
}
```

If a view takes longer than allowed:

- **In GCP**: Logs an error (monitored for alerts)
- **Locally**: Throws an error
- **With `allow_slow_views=True`**: Only logs a warning

### Cleanup and Dataset Management

**File**: `view_update_manager_utils.py`

The cleanup process (`cleanup_datasets_and_delete_unmanaged_views`) ensures code
is the source of truth:

1. **List all tables** in each managed dataset
2. **Compare** against `managed_views_map` (from view definitions in code)
3. **Delete unmanaged** views/tables not in code
4. **Delete datasets** that are no longer managed

This prevents accumulation of obsolete views from renamed/deleted views in code.

**Important**: Cleanup only runs in production deploys (not sandboxes) to avoid
accidentally deleting production data.

### Common Update Patterns

**Full production deployment**:

```python
result, dag = create_managed_dataset_and_deploy_views_for_view_builders(
    view_builders_to_update=all_production_view_builders,
    historically_managed_datasets_to_clean=PRODUCTION_DATASETS,
    rematerialize_changed_views_only=True,  # Only refresh changed views
    failure_mode=BigQueryViewDagWalkerProcessingFailureMode.FAIL_FAST,
)
result.log_processing_stats(n_slowest=25)
```

**Sandbox testing**:

```python
result, dag = create_managed_dataset_and_deploy_views_for_view_builders(
    view_builders_to_update=test_view_builders,
    historically_managed_datasets_to_clean=None,  # Don't cleanup in sandbox
    rematerialize_changed_views_only=False,  # Always materialize for testing
    failure_mode=BigQueryViewDagWalkerProcessingFailureMode.FAIL_FAST,
    view_update_sandbox_context=BigQueryViewUpdateSandboxContext(
        output_sandbox_dataset_prefix="my_test",
        input_source_table_overrides=BigQueryAddressOverrides.empty(),
        state_code_filter=None,
    ),
    default_table_expiration_for_new_datasets=TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
)
```

**Force rematerialization** (after source data changes):

```python
result, dag = create_managed_dataset_and_deploy_views_for_view_builders(
    view_builders_to_update=view_builders,
    historically_managed_datasets_to_clean=None,
    rematerialize_changed_views_only=False,  # Materialize everything
    failure_mode=BigQueryViewDagWalkerProcessingFailureMode.FAIL_EXHAUSTIVELY,
)
```

### Understanding View Update Results

The `ProcessDagResult` provides detailed information:

```python
result, dag = create_managed_dataset_and_deploy_views_for_view_builders(...)

# Access results per view
for view, view_result in result.view_results.items():
    if view_result.status == CreateOrUpdateViewStatus.SUCCESS_WITH_CHANGES:
        print(f"Updated: {view.address.to_str()}")
        if view_result.materialization_result:
            print(f"  Materialized to: {view.materialized_address.to_str()}")
            print(f"  Rows: {view_result.materialization_result.materialized_table.num_rows}")

# View performance stats
result.log_processing_stats(n_slowest=25)  # Logs slowest 25 views
total_time = result.total_runtime
```

## Loading Views to a Sandbox

### load_views_to_sandbox Script

**File**: `recidiviz/tools/load_views_to_sandbox.py`

The `load_views_to_sandbox` script is the primary tool for loading views to
sandbox datasets during development. It wraps
`create_managed_dataset_and_deploy_views_for_view_builders()` with intelligence
to automatically detect which views have changed.

**Three modes**:

1. **`auto` mode** (recommended): Automatically detects changed views by
   comparing local code to deployed views (least error-prone, use this whenever
   possible)

   ```bash
   uv run python -m recidiviz.tools.load_views_to_sandbox \
       --sandbox_dataset_prefix my_prefix auto \
       --load_changed_views_only
   ```

2. **`manual` mode**: Manually specify which views/datasets to load

   ```bash
   uv run python -m recidiviz.tools.load_views_to_sandbox \
       --sandbox_dataset_prefix my_prefix manual \
       --view_ids_to_load dataset.view1,dataset.view2 \
       --update_descendants True
   ```

3. **`all` mode**: Load all views (rarely needed, expensive)

**Common flags**:

- `--state_code_filter US_XX`: Only load data for specific state (much
  faster/cheaper)
- `--load_up_to_addresses` / `--load_up_to_datasets`: Stop loading after these
  views
- `--changed_datasets_to_ignore`: Ignore changes in certain datasets
- `--prompt`: Ask for confirmation before loading
- `--rematerialize_changed_views_only`: Only re-materialize changed views on
  subsequent runs

**Typical workflow**:

```bash
# Load only changed views for a specific state (fast, cheap)
uv run python -m recidiviz.tools.load_views_to_sandbox \
    --sandbox_dataset_prefix my_test \
    --state_code_filter US_ND \
    auto --load_changed_views_only

# Load changed views and all descendants up to specific datasets
uv run python -m recidiviz.tools.load_views_to_sandbox \
    --sandbox_dataset_prefix my_test auto \
    --load_up_to_datasets export_dataset,analysis_dataset
```

The script compares local view definitions against the most recent deployed view
update (stored in a stats table) to detect changes. See the script's docstring
for detailed usage examples.

## Best Practices

1. **Always materialize views that are queried frequently** - Set
   `should_materialize=True` and specify appropriate `clustering_fields`

2. **Use descriptive view_ids** - View IDs should clearly indicate what the view
   contains. By convention, the filename should match the `view_id`.

3. **Document your views** - Provide clear descriptions explaining:

   - What data the view contains
   - Any important caveats or limitations
   - How it should be used

4. **Use `.table_for_query` when referencing other views** - This ensures you
   query the materialized table when available

5. **Validate view builders** - Use `filename_matches_view_id_validator` in
   collectors to enforce filename conventions

6. **Keep queries readable** - Use multi-line strings, proper indentation, and
   comments in complex queries

7. **Test query formatting** - Use `build_and_print()` to verify your query
   templates format correctly

8. **Be mindful of query length** - BigQuery has a max query length
   (`BQ_VIEW_QUERY_MAX_LENGTH`). Very complex views may need to be broken up.

9. **Understand view dependencies** - Use DAG walkers to understand how your
   views relate to each other and to identify which views will be affected by
   changes

10. **Use type annotations** - Always add type information to function
    definitions (per project style guide)
