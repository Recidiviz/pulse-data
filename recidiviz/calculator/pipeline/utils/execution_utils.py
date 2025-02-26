# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Utils for executing calculation pipelines."""
import argparse
import datetime
import logging
from collections import defaultdict
from datetime import date

from typing import Dict, Tuple, Any, List, Iterable, Set, Optional

from googleapiclient.discovery import build
from more_itertools import one
from oauth2client.client import GoogleCredentials

from recidiviz.persistence.entity.state.entities import StatePerson


def get_job_id(pipeline_options: Dict[str, str]) -> str:
    """Captures the job_id of the pipeline job specified by the given options.

    For local jobs, generates a job_id using the given job_timestamp. For jobs
    running on Dataflow, finds the currently running job with the same job name
    as the current pipeline. Note: this works because there can only be one job
    with the same job name running on Dataflow at a time.

    Args:
        pipeline_options: Dictionary containing details about the pipeline.

    Returns:
        The job_id string of the current pipeline job.

    """
    runner = pipeline_options.get('runner')

    if runner == 'DataflowRunner':
        # Job is running on Dataflow. Get job_id.
        project = pipeline_options.get('project')
        region = pipeline_options.get('region')
        job_name = pipeline_options.get('job_name')

        if not project:
            raise ValueError("No project provided in pipeline options: "
                             f"{pipeline_options}")
        if not region:
            raise ValueError("No region provided in pipeline options: "
                             f"{pipeline_options}")
        if not job_name:
            raise ValueError("No job_name provided in pipeline options: "
                             f"{pipeline_options}")

        try:
            logging.info("Looking for job_id on Dataflow.")

            service_name = 'dataflow'
            dataflow_api_version = 'v1b3'
            credentials = GoogleCredentials.get_application_default()

            dataflow = build(serviceName=service_name,
                             version=dataflow_api_version,
                             credentials=credentials)

            result = dataflow.projects().locations().jobs().list(
                projectId=project,
                location=region,
            ).execute()

            pipeline_job_id = 'none'

            for job in result['jobs']:
                if job['name'] == job_name:
                    if job['currentState'] == 'JOB_STATE_RUNNING':
                        pipeline_job_id = job['id']
                    break

            if pipeline_job_id == 'none':
                msg = "Could not find currently running job with the " \
                    f"name: {job_name}."
                logging.error(msg)
                raise LookupError(msg)

        except Exception as e:
            logging.error("Error retrieving Job ID")
            raise LookupError(e) from e

    else:
        # Job is running locally. Generate id from the timestamp.
        pipeline_job_id = '_local_job'
        job_timestamp = pipeline_options.get('job_timestamp')

        if not job_timestamp:
            raise ValueError("Must provide a job_timestamp for local jobs.")

        pipeline_job_id = job_timestamp + pipeline_job_id

    return pipeline_job_id


def get_dataflow_job_with_id(project, job_id, location) -> Dict[str, str]:
    """Returns information about the Dataflow job with the given `job_id`."""
    service_name = 'dataflow'
    dataflow_api_version = 'v1b3'
    credentials = GoogleCredentials.get_application_default()

    dataflow = build(serviceName=service_name,
                     version=dataflow_api_version,
                     credentials=credentials)

    return dataflow.projects().locations().jobs().get(
        projectId=project,
        jobId=job_id,
        location=location).execute()


def calculation_month_count_arg(value) -> int:
    """Enforces the acceptable values for the calculation_month_count parameter in the pipelines."""
    int_value = int(value)

    if int_value < -1:
        raise argparse.ArgumentTypeError("Minimum calculation_month_count is -1")
    if int_value == 0:
        raise argparse.ArgumentTypeError("calculation_month_count cannot be 0")
    return int_value


def calculation_end_month_arg(value) -> str:
    """Enforces the acceptable values for the calculation_end_month parameter in the pipelines."""
    try:
        end_month_date = datetime.datetime.strptime(value, '%Y-%m').date()

        today_year, today_month = year_and_month_for_today()

        if end_month_date.year > today_year or \
                (end_month_date.year == today_year and end_month_date.month > today_month):
            raise argparse.ArgumentTypeError("calculation_end_month parameter cannot be a month in the future.")

        return value
    except ValueError as e:
        raise argparse.ArgumentTypeError("calculation_end_month parameter must be in the format YYYY-MM.") from e


def year_and_month_for_today() -> Tuple[int, int]:
    """Returns the year and month of today's date."""
    today = date.today()

    return today.year, today.month


def person_and_kwargs_for_identifier(
        arg_to_entities_map: Dict[str, Iterable[Any]]) -> Tuple[StatePerson, Dict[str, List]]:
    """In the calculation pipelines we use the CoGroupByKey function to group StatePerson entities with their associated
    entities. The output of CoGroupByKey is a dictionary where the keys are the variable names expected in the
    identifier step of the pipeline, and the values are iterables of the associated entities.

    This function unpacks the output of CoGroupByKey (the given arg_to_entities_map) into the person (StatePerson)
    entity, and a kwarg dictionary mapping all of the arguments expected by the identifier to the list of entities the
    identifier needs.

    Returns a tuple containing the StatePerson and the kwarg dictionary.
    """
    kwargs: Dict[str, Any] = {}
    person = None

    for key, values in arg_to_entities_map.items():
        if key == 'person':
            person = one(arg_to_entities_map[key])
        else:
            kwargs[key] = list(values)

    if not person:
        raise ValueError(f"No StatePerson associated with these entities: {arg_to_entities_map}")

    return person, kwargs


def select_all_by_person_query(
        dataset: str,
        table: str,
        state_code_filter: Optional[str],
        person_id_filter_set: Optional[Set[int]]) -> str:
    return select_all_query(dataset, table, state_code_filter, 'person_id', person_id_filter_set)


def select_all_query(dataset: str,
                     table: str,
                     state_code_filter: Optional[str],
                     unifying_id_field: Optional[str],
                     unifying_id_field_filter_set: Optional[Set[int]]) -> str:
    """Returns a query string formatted to select all contents of the table in the given dataset, filtering by the
    provided state code and unifying id filter sets, if necessary."""
    entity_query = f"SELECT * FROM `{dataset}.{table}`"

    if unifying_id_field_filter_set:
        if not unifying_id_field:
            raise ValueError(
                f'Expected nonnull unifying_id_field for nonnull unifying_id_field_filter_set when querying'
                f'dataset [{dataset}] and table [{table}].')

        id_str_set = {str(unifying_id) for unifying_id in unifying_id_field_filter_set if str(unifying_id)}

        entity_query = entity_query + f" WHERE {unifying_id_field} IN ({', '.join(sorted(id_str_set))})"

    if state_code_filter:
        conjunctive_word = 'AND' if unifying_id_field_filter_set else 'WHERE'
        entity_query = entity_query + f" {conjunctive_word} state_code IN ('{state_code_filter}')"

    return entity_query


def list_of_dicts_to_dict_with_keys(list_of_dicts: List[Dict[str, Any]], key: str) -> Dict[Any, Dict[str, Any]]:
    """Converts a list of dictionaries to a dictionary, where they keys are the values in each dictionary corresponding
    to the |key| argument. Each dictionary must contain the |key| key."""
    result_dict: Dict[str, Dict[str, Any]] = defaultdict()

    for dict_entry in list_of_dicts:
        key_value = dict_entry.get(key)

        if not key_value:
            raise ValueError(f"Key {key} must be present in all dictionaries: {dict_entry}.")

        result_dict[key_value] = dict_entry

    return result_dict
