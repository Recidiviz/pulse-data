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

This script only looks at the columns in the view that is *deployed*. If you have added a new column to a view, but
don't expect the values of the other columns to change, then this script should be used to determine that the existing
columns and values are unchanged.

If --check_determinism is set to True, the script instead compares the output of the sandbox views to itself to ensure
the output of the views are deterministic. In this case, all columns in the local view are compared.

This can be run on-demand whenever locally with the following command:
    python -m recidiviz.tools.compare_metric_view_output_to_sandbox
        --project_id [PROJECT_ID]
        --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX]
        [--load_sandbox_views]
        [--check_determinism]
"""
import argparse
import logging
import sys
from typing import Tuple, List

from google.cloud.bigquery import QueryJob

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.view_update_manager import VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE, \
    TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS

from recidiviz.calculator.query.state.views.dashboard.supervision.us_nd.average_change_lsir_score_by_month import \
    AVERAGE_CHANGE_LSIR_SCORE_MONTH_VIEW_BUILDER
from recidiviz.calculator.query.state.views.dashboard.supervision.us_nd.average_change_lsir_score_by_period import \
    AVERAGE_CHANGE_LSIR_SCORE_BY_PERIOD_VIEW_BUILDER
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

# These views are known to not have deterministic output, usually due to averaging of float values
VIEW_BUILDERS_WITH_KNOWN_NOT_DETERMINISTIC_OUTPUT = [
    AVERAGE_CHANGE_LSIR_SCORE_BY_PERIOD_VIEW_BUILDER,
    AVERAGE_CHANGE_LSIR_SCORE_MONTH_VIEW_BUILDER
]


def compare_metric_view_output_to_sandbox(sandbox_dataset_prefix: str,
                                          load_sandbox_views: bool,
                                          check_determinism: bool) -> None:
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

    for builders in VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE.values():
        for base_dataset_id, view_builders in builders.items():
            sandbox_dataset_id = sandbox_dataset_prefix + '_' + base_dataset_id

            if not bq_client.dataset_exists(bq_client.dataset_ref_for_id(sandbox_dataset_id)):
                raise ValueError(f"Trying to compare output to a sandbox dataset that does not exist: "
                                 f"{bq_client.project_id}.{sandbox_dataset_id}")

            for view_builder in view_builders:
                # Only compare output of metric views
                if isinstance(view_builder, MetricBigQueryViewBuilder) and \
                        view_builder not in VIEW_BUILDERS_WITH_KNOWN_NOT_DETERMINISTIC_OUTPUT:
                    deployed_view = bq_client.get_table(bq_client.dataset_ref_for_id(base_dataset_id),
                                                        view_builder.view_id)
                    base_view_id = (view_builder.build().materialized_view_table_id
                                    if view_builder.should_materialize else view_builder.view_id)

                    if not base_view_id:
                        raise ValueError("Unexpected empty base_view_id. view_id or materialized_view_table_id unset"
                                         f"for {view_builder}.")

                    base_dataset_ref = bq_client.dataset_ref_for_id(view_builder.dataset_id)

                    if not bq_client.table_exists(base_dataset_ref, base_view_id):
                        logging.warning("View %s.%s does not exist. Skipping output comparison.",
                                        base_dataset_ref.dataset_id, base_view_id)
                        continue

                    if not bq_client.table_exists(bq_client.dataset_ref_for_id(sandbox_dataset_id), base_view_id):
                        logging.warning("View %s.%s does not exist in sandbox. Skipping output comparison.",
                                        sandbox_dataset_id, base_view_id)
                        continue

                    # Only compare deployed columns, unless we're checking for view determinism
                    columns_to_compare = ([field.name for field in deployed_view.schema]
                                          if not check_determinism else ['*'])
                    # Only include dimensions in the deployed view unless we are checking the determinism of the local
                    # view
                    metric_dimensions = [dimension for dimension in view_builder.dimensions
                                         if dimension in columns_to_compare or check_determinism]

                    output_table_id = f"{view_builder.dataset_id}--{base_view_id}"
                    base_dataset_id_for_query = sandbox_dataset_id if check_determinism else view_builder.dataset_id

                    diff_query = OUTPUT_COMPARISON_TEMPLATE.format(
                        project_id=bq_client.project_id,
                        base_dataset_id=base_dataset_id_for_query,
                        sandbox_dataset_id=sandbox_dataset_id,
                        view_id=base_view_id,
                        columns_to_compare=', '.join(columns_to_compare),
                        dimensions=', '.join(metric_dimensions)
                    )

                    query_job = bq_client.create_table_from_query_async(
                        dataset_id=sandbox_comparison_output_dataset_id,
                        table_id=output_table_id,
                        query=diff_query,
                        overwrite=True
                    )

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

    if not views_with_different_output:
        output_message = ("identical between deployed views and sandbox datasets"
                          if not check_determinism else "deterministic")
        logging.info("View output %s.", output_message)
        bq_client.delete_dataset(sandbox_comparison_output_dataset_ref, delete_contents=True)

    for view in views_with_different_output:
        base_dataset_id, base_view_id = view.table_id.split('--')

        logging.warning("View output differs for view %s.%s. See %s.%s for diverging rows.",
                        base_dataset_id, base_view_id, sandbox_comparison_output_dataset_id, view.table_id)


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

    return parser.parse_known_args(argv)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    with local_project_id_override(known_args.project_id):
        compare_metric_view_output_to_sandbox(known_args.sandbox_dataset_prefix,
                                              known_args.load_sandbox_views,
                                              known_args.check_determinism)
