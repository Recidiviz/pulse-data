# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Compares output for all metric views between the deployed views and the views in the sandbox datasets. To load the
views into the sandbox, see recidiviz/tools/load_views_to_sandbox, or if you have not yet loaded the sandbox views,
you can set the --load_sandbox_views flag on this script on your first run.

The script executes the following comparison. Say that we have view `metric_view_x`, which has the dimension columns
`state_code` and `year`, and a count column `count`. Say the result of querying the version of `metric_view_x` that is
currently deployed in BigQuery is:

state_code | year   | count
---------- | ------ |------
  US_XX    | 2000   | 3
  US_XX    | 2001   | 4
  US_ZZ    | 2000   | 5

Suppose you've made some changes locally to the `metric_view_x` view, but you *do not* expect the output to change. You
go to run this script, and the result of querying your local version of `metric_view_x` produces:

state_code | year   | count
---------- | ------ |------
  US_XX    | 2000   | 3
  US_XX    | 2000   | 3   <-- Duplicate of row above
  US_XX    | 2001   | 99  <-- Value is different in base/sandbox
  US_ZZ    | 2000   | 5
  US_AA    | 1999   | 1  <-- Dimension combo only present in sandbox output

This script compares all non-dimension columns for all matching combinations of dimensions. In this case, the script
will compare the `count` column for all combinations of `state_code` and `year` that are present in either the deployed
or the local view. The following row will be written in a `sandbox_dataset--metric_view_x` table in the sandbox metric
output dataset:

state_code | year   | base_output.count | sandbox_output.count | duplicate_flag
---------- | ------ |------------------ | -------------------- | ----------------
  US_XX    | 2000   |        3          |          3           | DUPLICATE_DIMENSION_COMBINATION  <-- Duplicate
  US_XX    | 2001   |        4          |          99          |                <-- Value is different in base/sandbox
  US_AA    | 1999   |       null        |          1           |                <-- Dimension combo only in sandbox

If the --allow_schema_changes flag is on, then this script will only compare columns that are in both the deployed
and sandbox views. If --allow_schema_changes is set to False, then this script will raise an error if the schemas do not
match. If you have added a new column to a view, but don't expect the values of the other columns to change, then the
--allow_schema_changes should be used to determine that the existing columns and values are unchanged.

If --check_determinism is set to True, the script instead compares the output of the sandbox views to itself to ensure
the output of the views are deterministic. In this case, all columns in the local view are compared.

This can be run on-demand whenever locally with the following command:
    python -m recidiviz.tools.compare_metric_view_output_to_sandbox
        --project_id [PROJECT_ID]
        --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX]
        [--load_sandbox_views]
        [--check_determinism]
        [--allow_schema_changes]
        [--dataset_id_filters]
"""
import argparse
import logging
import sys
from typing import Tuple, List, Optional

from google.cloud import bigquery
from google.cloud.bigquery import QueryJob
from more_itertools import peekable

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.view_update_manager import VIEW_BUILDERS_BY_NAMESPACE, TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
from recidiviz.calculator.query.state.views.dashboard.supervision.us_nd.average_change_lsir_score_by_month import \
    AVERAGE_CHANGE_LSIR_SCORE_MONTH_VIEW_BUILDER
from recidiviz.calculator.query.state.views.dashboard.supervision.us_nd.average_change_lsir_score_by_period import \
    AVERAGE_CHANGE_LSIR_SCORE_BY_PERIOD_VIEW_BUILDER
from recidiviz.calculator.query.state.views.po_report.po_monthly_report_data import PO_MONTHLY_REPORT_DATA_VIEW_BUILDER
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.tools.load_views_to_sandbox import load_views_to_sandbox
from recidiviz.utils.environment import GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION
from recidiviz.utils.metadata import local_project_id_override

OUTPUT_COMPARISON_TEMPLATE = """
    WITH base_output AS (
      SELECT {columns_to_compare}
      FROM `{project_id}.{base_dataset_id}.{view_id}`
    ), sandbox_output AS (
      SELECT {columns_to_compare}
      FROM `{project_id}.{sandbox_dataset_id}.{view_id}`
    ), joined_output AS (
      SELECT
        {dimensions},
        base_output,
        sandbox_output,
        ROW_NUMBER() OVER (PARTITION BY {dimensions}) AS dimension_combo_ranking
      FROM base_output 
      FULL OUTER JOIN sandbox_output
      USING ({dimensions})
    ), error_rows AS (
      SELECT
        * EXCEPT (dimension_combo_ranking),
        IF(dimension_combo_ranking > 1, 'DUPLICATE_DIMENSION_COMBINATION', null) AS duplicate_flag
      FROM joined_output
      WHERE TO_JSON_STRING(base_output) != TO_JSON_STRING(sandbox_output) OR dimension_combo_ranking > 1
    ), without_base_output AS (
      SELECT 
        *
        REPLACE ((SELECT AS STRUCT base_output.* EXCEPT ({dimensions})) AS base_output)
      FROM error_rows
    )
    
    SELECT
      *
      REPLACE ((SELECT AS STRUCT sandbox_output.* EXCEPT ({dimensions})) AS sandbox_output)
    FROM without_base_output
    ORDER BY {dimensions}
"""

# These views are known to not have deterministic output
VIEW_BUILDERS_WITH_KNOWN_NOT_DETERMINISTIC_OUTPUT = [
    # Averaging of float values non-deterministic
    AVERAGE_CHANGE_LSIR_SCORE_BY_PERIOD_VIEW_BUILDER,
    AVERAGE_CHANGE_LSIR_SCORE_MONTH_VIEW_BUILDER,
    # TODO(#5034): must be deterministic and have reduced complexity to be included
    PO_MONTHLY_REPORT_DATA_VIEW_BUILDER
]


def compare_metric_view_output_to_sandbox(sandbox_dataset_prefix: str,
                                          load_sandbox_views: bool,
                                          check_determinism: bool,
                                          allow_schema_changes: bool,
                                          dataset_id_filters: Optional[List[str]]) -> None:
    """Compares the output of all deployed metric views to the output of the corresponding views in the sandbox
    dataset."""
    if load_sandbox_views:
        logging.info("Loading views into sandbox datasets prefixed with %s", sandbox_dataset_prefix)
        load_views_to_sandbox(sandbox_dataset_prefix)

    bq_client = BigQueryClientImpl()
    sandbox_comparison_output_dataset_id = sandbox_dataset_prefix + '_metric_view_comparison_output'
    sandbox_comparison_output_dataset_ref = bq_client.dataset_ref_for_id(sandbox_comparison_output_dataset_id)

    if bq_client.dataset_exists(sandbox_comparison_output_dataset_ref) and \
            any(bq_client.list_tables(sandbox_comparison_output_dataset_id)):
        raise ValueError(f"Cannot write comparison output to a non-empty dataset. Please delete tables in dataset: "
                         f"{bq_client.project_id}.{sandbox_comparison_output_dataset_id}.")

    bq_client.create_dataset_if_necessary(sandbox_comparison_output_dataset_ref,
                                          TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS)

    query_jobs: List[Tuple[QueryJob, str]] = []
    skipped_views: List[str] = []

    for view_builders in VIEW_BUILDERS_BY_NAMESPACE.values():
        for view_builder in view_builders:
            # Only compare output of metric views
            if not isinstance(view_builder, MetricBigQueryViewBuilder):
                continue

            base_dataset_id = view_builder.dataset_id

            if dataset_id_filters and base_dataset_id not in dataset_id_filters:
                continue

            if view_builder in VIEW_BUILDERS_WITH_KNOWN_NOT_DETERMINISTIC_OUTPUT:
                logging.warning("View %s.%s has known non-deterministic output. Skipping output comparison.",
                                view_builder.dataset_id, view_builder.view_id)
                skipped_views.append(f"{view_builder.dataset_id}.{view_builder.view_id}")
                continue

            sandbox_dataset_id = sandbox_dataset_prefix + '_' + base_dataset_id

            if not bq_client.dataset_exists(bq_client.dataset_ref_for_id(sandbox_dataset_id)):
                raise ValueError(f"Trying to compare output to a sandbox dataset that does not exist: "
                                 f"{bq_client.project_id}.{sandbox_dataset_id}")

            base_dataset_ref = bq_client.dataset_ref_for_id(base_dataset_id)
            base_view_id = (view_builder.build().materialized_view_table_id
                            if view_builder.should_materialize and not check_determinism else view_builder.view_id)

            if not base_view_id:
                raise ValueError("Unexpected empty base_view_id. view_id or materialized_view_table_id unset"
                                 f"for {view_builder}.")

            if not check_determinism and not bq_client.table_exists(base_dataset_ref, base_view_id):
                logging.warning("View %s.%s does not exist. Skipping output comparison.",
                                base_dataset_ref.dataset_id, base_view_id)
                skipped_views.append(f"{base_dataset_id}.{base_view_id}")
                continue

            if not bq_client.table_exists(bq_client.dataset_ref_for_id(sandbox_dataset_id), base_view_id):
                logging.warning("View %s.%s does not exist in sandbox. Skipping output comparison.",
                                sandbox_dataset_id, base_view_id)
                skipped_views.append(f"{sandbox_dataset_id}.{base_view_id}")
                continue
            query_job, output_table_id = _view_output_comparison_job(bq_client,
                                                                     view_builder,
                                                                     base_view_id,
                                                                     base_dataset_id,
                                                                     sandbox_dataset_id,
                                                                     sandbox_comparison_output_dataset_id,
                                                                     check_determinism,
                                                                     allow_schema_changes)

            # Add query job to the list of running jobs
            query_jobs.append((query_job, output_table_id))

    for query_job, output_table_id in query_jobs:
        # Wait for the insert job to complete before looking for the table
        query_job.result()

        output_table = bq_client.get_table(sandbox_comparison_output_dataset_ref, output_table_id)

        if output_table.num_rows == 0:
            # If there are no rows in the output table, then the view output was identical
            bq_client.delete_table(sandbox_comparison_output_dataset_id, output_table_id)

    views_with_different_output = bq_client.list_tables(sandbox_comparison_output_dataset_id)
    views_with_different_output = peekable(views_with_different_output)

    logging.info("\n*************** METRIC VIEW OUTPUT RESULTS ***************\n")

    if dataset_id_filters:
        logging.info("Only compared metric view output for the following datasets: \n %s \n", dataset_id_filters)

    logging.info("Skipped output comparison for the following metric views: \n %s \n", skipped_views)

    if views_with_different_output:
        for view in views_with_different_output:
            base_dataset_id, base_view_id = view.table_id.split('--')

            logging.warning("View output differs for view %s.%s. See %s.%s for diverging rows.",
                            base_dataset_id, base_view_id, sandbox_comparison_output_dataset_id, view.table_id)
    else:
        output_message = ("identical between deployed views and sandbox datasets"
                          if not check_determinism else "deterministic")
        logging.info("View output %s. Deleting dataset %s.", output_message,
                     sandbox_comparison_output_dataset_ref.dataset_id)
        bq_client.delete_dataset(sandbox_comparison_output_dataset_ref, delete_contents=True)


def _view_output_comparison_job(bq_client: BigQueryClientImpl,
                                view_builder: MetricBigQueryViewBuilder,
                                base_view_id: str,
                                base_dataset_id: str,
                                sandbox_dataset_id: str,
                                sandbox_comparison_output_dataset_id: str,
                                check_determinism: bool,
                                allow_schema_changes: bool) -> \
        Tuple[bigquery.QueryJob, str]:
    """Builds and executes the query that compares the base and sandbox views. Returns a tuple with the the QueryJob and
    the table_id where the output will be written to in the sandbox_comparison_output_dataset_id dataset."""
    base_dataset_ref = bq_client.dataset_ref_for_id(base_dataset_id)
    sandbox_dataset_ref = bq_client.dataset_ref_for_id(sandbox_dataset_id)
    output_table_id = f"{view_builder.dataset_id}--{base_view_id}"

    if check_determinism:
        # Compare all columns
        columns_to_compare = ['*']
        preserve_column_types = True
    else:
        # Columns in deployed view
        deployed_base_view = bq_client.get_table(base_dataset_ref, view_builder.view_id)
        # If there are nested columns in the deployed view then we can't allow column type changes
        preserve_column_types = _table_contains_nested_columns(deployed_base_view)
        base_columns_to_compare = set(field.name for field in deployed_base_view.schema)

        # Columns in sandbox view
        deployed_sandbox_view = bq_client.get_table(sandbox_dataset_ref, view_builder.view_id)
        if not preserve_column_types:
            # If there are nested columns in the sandbox view then we can't allow column type changes
            preserve_column_types = _table_contains_nested_columns(deployed_sandbox_view)

        sandbox_columns_to_compare = set(field.name for field in deployed_sandbox_view.schema)

        if allow_schema_changes:
            # Only compare columns in both views
            shared_columns = base_columns_to_compare.intersection(sandbox_columns_to_compare)
            columns_to_compare = list(shared_columns)
        else:
            if base_columns_to_compare != sandbox_columns_to_compare:
                raise ValueError(f"Schemas of the {base_dataset_id}.{base_view_id} deployed and"
                                 f" sandbox views do not match. If this is expected, please run again"
                                 f"with the --allow_schema_changes flag.")
            columns_to_compare = list(base_columns_to_compare)

    # Only include dimensions in both views unless we are checking the determinism of the local
    # view
    metric_dimensions = [dimension for dimension in view_builder.dimensions
                         if dimension in columns_to_compare or check_determinism]

    if not preserve_column_types:
        # Cast all columns to strings to guard against column types that may have changed
        columns_to_compare = [f"CAST({col} AS STRING) as {col}"
                              for col in columns_to_compare]

    base_dataset_id_for_query = sandbox_dataset_id if check_determinism else view_builder.dataset_id

    diff_query = OUTPUT_COMPARISON_TEMPLATE.format(
        project_id=bq_client.project_id,
        base_dataset_id=base_dataset_id_for_query,
        sandbox_dataset_id=sandbox_dataset_id,
        view_id=base_view_id,
        columns_to_compare=', '.join(columns_to_compare),
        dimensions=', '.join(metric_dimensions)
    )

    return (bq_client.create_table_from_query_async(
        dataset_id=sandbox_comparison_output_dataset_id,
        table_id=output_table_id,
        query=diff_query,
        overwrite=True
    ), output_table_id)


def _table_contains_nested_columns(table: bigquery.Table) -> bool:
    """Returns whether or not the schema of the given |table| contains a nested column, which is either a STRUCT or
    RECORD type column."""
    for field in table.schema:
        if field.field_type in ('STRUCT', 'RECORD'):
            return True
    return False


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the arguments needed to call the compare_metric_view_output_to_sandbox function."""
    parser = argparse.ArgumentParser()

    parser.add_argument('--project_id',
                        dest='project_id',
                        type=str,
                        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
                        required=True)

    parser.add_argument('--sandbox_dataset_prefix',
                        dest='sandbox_dataset_prefix',
                        help='A prefix to append to all names of the datasets where these views will be loaded.',
                        type=str,
                        required=True)

    parser.add_argument('--load_sandbox_views',
                        dest='load_sandbox_views',
                        help='If True, first runs the recidiviz.tools.load_views_to_sandbox script to load the views'
                             ' into the sandbox datasets before doing the comparison.',
                        type=bool,
                        default=False,
                        required=False)

    parser.add_argument('--check_determinism',
                        dest='check_determinism',
                        help='If True, compares the output of the sandbox views to themselves to ensure the output '
                             'of the views are deterministic.',
                        type=bool,
                        default=False,
                        required=False)

    parser.add_argument('--allow_schema_changes',
                        dest='allow_schema_changes',
                        help='If True, only compares columns that are shared between the deployed and sandbox views.'
                             'If False, will throw an error if the view schemas do not match.',
                        type=bool,
                        default=False,
                        required=False)

    parser.add_argument('--dataset_id_filters',
                        dest='dataset_id_filters',
                        type=str,
                        nargs='+',
                        help='Filters the comparison to a limited set of dataset_ids.')

    return parser.parse_known_args(argv)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    if known_args.check_determinism and known_args.dataset_id_filters is None:
        logging.warning("You are running this script with the --check_determinism flag without any dataset_id_filters."
                        " This may take a long time and will query expensive views directly. Are you sure you want to"
                        " proceed? (Y/n)")
        continue_running = input()
        if continue_running != 'Y':
            sys.exit()

    with local_project_id_override(known_args.project_id):
        compare_metric_view_output_to_sandbox(known_args.sandbox_dataset_prefix,
                                              known_args.load_sandbox_views,
                                              known_args.check_determinism,
                                              known_args.allow_schema_changes,
                                              known_args.dataset_id_filters)
