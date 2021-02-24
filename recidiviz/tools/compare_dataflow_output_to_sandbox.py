# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Compares output between a regularly executed Dataflow calculation job and a test job with output directed to a
sandbox Dataflow dataset. (For creating a sandbox Dataflow output dataset, see the
create_or_update_dataflow_metrics_sandbox script.) This script should be used when you have run a test pipeline in your
sandbox dataset that has the same month and metric_type parameters as a job listed in the
production_calculation_pipeline_templates.yaml file.

This script compares Dataflow output for all metric_types defined in the production_calculation_pipeline_templates.yaml
for the given job_name_to_compare.

Say that we have updated the logic that determines the supervision_type column for various supervision metrics for the
state US_XX. We want to be able to evaluate the output impact of making this change. First, we will go to the
production_calculation_pipeline_templates file to determine which the parameters of the supervision pipeline that is
regularly run for US_XX. We will then run a test pipeline from our development branch with the same parameters as this
regular job, with the output directed to the sandbox Dataflow dataset. (For help running in-development Dataflow jobs,
see go/dataflow-dev.) Once this pipeline has completed, we would run this script with the following parameters to
execute the comparison:

    python -m recidiviz.tools.compare_dataflow_output_to_sandbox
        --project_id recidiviz-staging
        --sandbox_dataset_prefix my_github_username
        --job_name_to_compare us-xx-supervision-calculations-36
        --base_output_job_id 999999999 (job_id from regular run from this morning)
        --sandbox_output_job_id 000000000 (job_id from my test run)
        --additional_columns_to_compare supervision_type

Say the result of querying the US_XX supervision population output from this morning's regularly executed run has
the following rows:

state_code | person_id  | date_of_supervision | supervision_type
---------- | --------- |--------------------- | ----------------
  US_XX    | 1234      | 2020-01-01           |  PAROLE
  US_XX    | 1234      | 2020-01-02           |  PAROLE
  US_XX    | 1234      | 2020-01-03           |  PAROLE

Now suppose the methodology change you've made has impacted which supervision_type this person is classified under.
Say the result of querying the US_XX supervision population output from your test run has the following rows:

state_code | person_id  | date_of_supervision | supervision_type
---------- | --------- |--------------------- | ----------------
  US_XX    | 1234      | 2020-01-01           |  PAROLE
  US_XX    | 1234      | 2020-01-02           |  PROBATION  <-- Value is different in base/sandbox
  US_XX    | 9999      | 2020-01-03           |  PROBATION  <-- Dimension combo only present in sandbox output

This script compares all non-dimension columns for all matching combinations of dimensions. For each dataflow metric,
the "dimensions" are defined as the state_code, the person_id, and any date fields on the metric (for example,
date_of_supervision). In this case, the script will compare the `supervision_type` column for all combinations of
`state_code`, `person_id`, and `date_of_supervision` that are present in either the base output or the sandbox output.
The following row will be written in a `metric_table` table in the sandbox dataflow comparison output dataset:

state_code | person_id  | date_of_supervision | base_output.supervision_type | sandbox_output.supervision_type
---------- | --------- |--------------------- | ---------------------------- | ----------------------------
  US_XX    | 1234      | 2020-01-02           |  PAROLE                      | PROBATION
  US_XX    | 9999      | 2020-01-03           |  null                        | PROBATION

In this case, the script was run with "--additional_columns_to_compare supervision_type". If no
--additional_columns_to_compare columns are provided, then the script just compares that the same people are counted
in each metric for each metric date.

This can be run on-demand whenever locally with the following command:
    python -m recidiviz.tools.compare_dataflow_output_to_sandbox
        --project_id [PROJECT_ID]
        --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX]
        --job_name_to_compare [JOB_NAME_TO_COMPARE]
        --base_output_job_id [BASE_OUTPUT_JOB_ID]
        --sandbox_output_job_id [SANDBOX_OUTPUT_JOB_ID]
        [--additional_columns_to_compare]
        [--allow_overwrite]
"""

import argparse
import logging
import os
import sys
from typing import Tuple, List, Type, cast

from google.cloud import bigquery
from google.cloud.bigquery import QueryJob
from more_itertools import peekable

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.view_update_manager import TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
from recidiviz.calculator import pipeline as pipeline_module
from recidiviz.calculator.dataflow_output_storage_config import DATAFLOW_METRICS_TO_TABLES
from recidiviz.calculator.pipeline.utils.metric_utils import RecidivizMetric
from recidiviz.calculator.query.state.dataset_config import DATAFLOW_METRICS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.yaml_dict import YAMLDict


PRODUCTION_TEMPLATES_PATH = os.path.join(os.path.dirname(pipeline_module.__file__),
                                         'production_calculation_pipeline_templates.yaml')

OUTPUT_COMPARISON_TEMPLATE = """
    WITH base_output AS (
      SELECT {columns_to_compare}
      FROM `{project_id}.{base_dataset_id}.{table_id}`
      WHERE job_id = '{base_output_job_id}'
    ), sandbox_output AS (
      SELECT {columns_to_compare}
      FROM `{project_id}.{sandbox_dataset_id}.{table_id}`
      WHERE job_id = '{sandbox_output_job_id}'
    ), joined_output AS (
      SELECT
        {dimensions},
        base_output,
        sandbox_output
      FROM base_output 
      FULL OUTER JOIN sandbox_output
      USING ({dimensions})
    ), error_rows AS (
      SELECT
        *
      FROM joined_output
      WHERE TO_JSON_STRING(base_output) != TO_JSON_STRING(sandbox_output)
    ), without_base_output AS (
      SELECT 
        *
        REPLACE ((SELECT AS STRUCT base_output.* {output_col_exclusion}) AS base_output)
      FROM error_rows
    )

    SELECT
      *
      REPLACE ((SELECT AS STRUCT sandbox_output.* {output_col_exclusion}) AS sandbox_output)
    FROM without_base_output
    ORDER BY {dimensions}
"""

# Date columns in the metric tables that should not be included in the dimensions
DATE_COLUMNS_TO_EXCLUDE = [
    'created_on',
    'updated_on',
    # This date is often null, and null values don't join on each other in BigQuery
    'projected_end_date'
]


# TODO(#5814): Update this to look for the last completed job that ran with the parameters defined for the job_name
def compare_dataflow_output_to_sandbox(
        sandbox_dataset_prefix: str,
        job_name_to_compare: str,
        base_output_job_id: str,
        sandbox_output_job_id: str,
        additional_columns_to_compare: List[str],
        allow_overwrite: bool = False
) -> None:
    """Compares the output for all metrics produced by the daily pipeline job with the given |job_name_to_compare|
    between the output from the |base_output_job_id| job in the dataflow_metrics dataset and the output from the
    |sandbox_output_job_id| job in the sandbox dataflow dataset."""
    bq_client = BigQueryClientImpl()
    sandbox_dataflow_dataset_id = sandbox_dataset_prefix + '_' + DATAFLOW_METRICS_DATASET

    sandbox_comparison_output_dataset_id = sandbox_dataset_prefix + '_dataflow_comparison_output'
    sandbox_comparison_output_dataset_ref = bq_client.dataset_ref_for_id(sandbox_comparison_output_dataset_id)

    if bq_client.dataset_exists(sandbox_comparison_output_dataset_ref) and \
            any(bq_client.list_tables(sandbox_comparison_output_dataset_id)) and not allow_overwrite:
        if __name__ == '__main__':
            logging.error("Dataset %s already exists in project %s. To overwrite, set --allow_overwrite.",
                          sandbox_comparison_output_dataset_id, bq_client.project_id)
            sys.exit(1)
        else:
            raise ValueError(f"Cannot write comparison output to a non-empty dataset. Please delete tables in dataset: "
                             f"{bq_client.project_id}.{sandbox_comparison_output_dataset_id}.")

    bq_client.create_dataset_if_necessary(sandbox_comparison_output_dataset_ref,
                                          TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS)

    query_jobs: List[Tuple[QueryJob, str]] = []

    pipelines = YAMLDict.from_path(PRODUCTION_TEMPLATES_PATH).pop_dicts('daily_pipelines')

    for pipeline in pipelines:
        if pipeline.pop('job_name', str) == job_name_to_compare:
            pipeline_metric_types = pipeline.peek_optional('metric_types', str)

            if not pipeline_metric_types:
                raise ValueError(f"Pipeline job {job_name_to_compare} missing required metric_types attribute.")

            metric_types_for_comparison = pipeline_metric_types.split()

            for metric_class, metric_table in DATAFLOW_METRICS_TO_TABLES.items():
                # Hack to get the value of the metric_type on this RecidivizMetric class
                metric_instance = cast(RecidivizMetric, metric_class.build_from_dictionary({
                    'job_id': 'xxx',
                    'state_code': 'US_XX'
                }))

                metric_type_value = metric_instance.metric_type.value

                if metric_type_value in metric_types_for_comparison:
                    dimensions = _get_dimension_columns_for_metric_class(metric_class)
                    columns_to_compare = dimensions.copy()

                    output_col_exclusion = ""

                    if additional_columns_to_compare:
                        columns_to_compare.extend(additional_columns_to_compare)
                        output_col_exclusion = f"EXCEPT ({', '.join(dimensions)})"

                    comparison_query = OUTPUT_COMPARISON_TEMPLATE.format(
                        project_id=bq_client.project_id,
                        base_dataset_id=DATAFLOW_METRICS_DATASET,
                        sandbox_dataset_id=sandbox_dataflow_dataset_id,
                        table_id=metric_table,
                        columns_to_compare=', '.join(columns_to_compare),
                        dimensions=', '.join(dimensions),
                        base_output_job_id=base_output_job_id,
                        sandbox_output_job_id=sandbox_output_job_id,
                        output_col_exclusion=output_col_exclusion
                    )

                    query_job = bq_client.create_table_from_query_async(
                        dataset_id=sandbox_comparison_output_dataset_id,
                        table_id=metric_table,
                        query=comparison_query,
                        overwrite=True
                    )

                    # Add query job to the list of running jobs
                    query_jobs.append((query_job, metric_table))

    for query_job, output_table_id in query_jobs:
        # Wait for the insert job to complete before looking for the table
        query_job.result()

        output_table = bq_client.get_table(sandbox_comparison_output_dataset_ref, output_table_id)

        if output_table.num_rows == 0:
            # If there are no rows in the output table, then the output was identical
            bq_client.delete_table(sandbox_comparison_output_dataset_id, output_table_id)

    metrics_with_different_output = peekable(bq_client.list_tables(sandbox_comparison_output_dataset_id))

    logging.info("\n*************** DATAFLOW OUTPUT COMPARISON RESULTS ***************\n")

    if metrics_with_different_output:
        for metric_table in metrics_with_different_output:
            # This will always be true, and is here to silence mypy warnings
            assert isinstance(metric_table, bigquery.table.TableListItem)

            logging.warning("Dataflow output differs for metric %s. See %s.%s for diverging rows.",
                            metric_table.table_id, sandbox_comparison_output_dataset_id, metric_table.table_id)
    else:
        logging.info("Dataflow output identical. Deleting dataset %s.",
                     sandbox_comparison_output_dataset_ref.dataset_id)
        bq_client.delete_dataset(sandbox_comparison_output_dataset_ref, delete_contents=True)


def _get_dimension_columns_for_metric_class(metric_class: Type[RecidivizMetric]) -> List[str]:
    """Returns the dimension columns that should be used to compare output between pipeline runs. Includes the
    state_code, the person_id, and any date fields specific to the metric (e.g. date_of_supervision)."""
    dimension_columns: List[str] = ['state_code', 'person_id']

    schema_fields = metric_class.bq_schema_for_metric_table()
    for field in schema_fields:
        if field.field_type == 'DATE' and field.name not in DATE_COLUMNS_TO_EXCLUDE:
            dimension_columns.append(field.name)

    return dimension_columns


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
                        help='The prefix of the sandbox dataflow_metrics dataset.',
                        type=str,
                        required=True)

    parser.add_argument('--job_name_to_compare',
                        dest='job_name_to_compare',
                        help='The name of the regularly-running calculation job for which you are comparing output.',
                        type=str,
                        required=True)

    parser.add_argument('--base_output_job_id',
                        dest='base_output_job_id',
                        help='The job_id that ran against which you are comparing output.',
                        type=str,
                        required=True)

    parser.add_argument('--sandbox_output_job_id',
                        dest='sandbox_output_job_id',
                        help='The job_id that ran for which you are comparing output.',
                        type=str,
                        required=True)

    parser.add_argument('--additional_columns_to_compare',
                        dest='additional_columns_to_compare',
                        type=str,
                        nargs='+',
                        help='An optional list of columns to compare between the two outputs, besides state_code,'
                             'person_id, and the date fields of the metric.',
                        required=False)

    parser.add_argument('--allow_overwrite',
                        dest='allow_overwrite',
                        action='store_true',
                        default=False,
                        help="Allow tables in existing dataset to be overwritten.")

    return parser.parse_known_args(argv)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    with local_project_id_override(known_args.project_id):
        compare_dataflow_output_to_sandbox(known_args.sandbox_dataset_prefix,
                                           known_args.job_name_to_compare,
                                           known_args.base_output_job_id,
                                           known_args.sandbox_output_job_id,
                                           known_args.additional_columns_to_compare,
                                           known_args.allow_overwrite)
