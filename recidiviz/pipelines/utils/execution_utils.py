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
import logging
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

from googleapiclient.discovery import build
from more_itertools import one
from oauth2client.client import GoogleCredentials

from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state.entities import StatePerson

# The name of an entity, e.g. StatePerson.
EntityClassName = str

# The names of of two entities that are related to one another,
# eg. StateSupervisionSentence.StateCharge
EntityRelationshipKey = str

# The name of a reference table, e.g. persons_to_recent_county_of_residence
TableName = str

# The root entity id that can be used to group related objects together (e.g. person_id)
RootEntityId = int

# Primary keys of two entities that share a relationship. The first int is the parent
# object primary key and the second int is the child object primary key.
EntityAssociation = Tuple[int, int]


# The structure of table rows loaded from BigQuery
TableRow = Dict[str, Any]


def get_job_id(project_id: str, region: str, job_name: str) -> str:
    """Captures the job_id of the pipeline job specified by the given options.

    For local jobs, generates a job_id using the given job_timestamp. For jobs
    running on Dataflow, finds the currently running job with the same job name
    as the current pipeline. Note: this works because there can only be one job
    with the same job name running on Dataflow at a time.

    Args:
        project_id: The project the job is being run in
        region: The region the job is being run in
        job_name: The name of the running job

    Returns:
        The job_id string of the current pipeline job.

    """
    try:
        logging.info("Looking for job_id on Dataflow.")

        service_name = "dataflow"
        dataflow_api_version = "v1b3"
        credentials = GoogleCredentials.get_application_default()

        dataflow = build(
            serviceName=service_name,
            version=dataflow_api_version,
            credentials=credentials,
        )

        result = (
            dataflow.projects()
            .locations()
            .jobs()
            .list(
                projectId=project_id,
                location=region,
            )
            .execute()
        )

        pipeline_job_id = "none"

        for job in result["jobs"]:
            if job["name"] == job_name:
                if job["currentState"] == "JOB_STATE_RUNNING":
                    pipeline_job_id = job["id"]
                break

        if pipeline_job_id == "none":
            msg = f"Could not find currently running job with the name: {job_name}."
            logging.error(msg)
            raise LookupError(msg)

    except Exception as e:
        logging.error("Error retrieving Job ID")
        raise LookupError(e) from e

    return pipeline_job_id


def kwargs_for_entity_lists(
    arg_to_entities_map: Dict[
        Union[EntityClassName, TableName], Union[Iterable[Entity], Iterable[TableRow]]
    ]
) -> Dict[Union[EntityClassName, TableName], Union[Sequence[Entity], List[TableRow]]]:
    """In the calculation pipelines we use the CoGroupByKey function to group
    entities by their person_id values. The output of CoGroupByKey is a dictionary
    where the keys are the variable names expected by the pipeline, and the values are
    iterables of the associated entities.

    This function unpacks the output of CoGroupByKey (the given arg_to_entities_map)
    into the kwarg dictionary mapping all of the arguments expected by the pipeline to
    the list of entities the pipeline needs.

    Returns the dictionary.
    """
    entity_map: Dict[
        Union[EntityClassName, TableName], Union[Sequence[Entity], List[TableRow]]
    ] = {}

    # This builds the dictionary in a way that satisfies mypy
    for key, values in arg_to_entities_map.items():
        entity_value_list: List[Entity] = []
        table_row_value_list: List[TableRow] = []

        for value in values:
            if isinstance(value, Entity):
                entity_value_list.append(value)
            elif isinstance(value, Dict):
                table_row_value_list.append(value)
            else:
                raise ValueError(
                    "Expected value of iterable in dictionary to be "
                    "either of type Entity or a Dict[str, str]. Found: "
                    f"{value}."
                )

        if entity_value_list and table_row_value_list:
            raise ValueError(
                "All values in the same key should be of the same type. "
                f"Found {values}."
            )

        entity_map[key] = (
            entity_value_list if entity_value_list else table_row_value_list
        )

    return entity_map


def person_and_kwargs_for_identifier(
    arg_to_entities_map: Dict[
        Union[EntityClassName, TableName], Union[Iterable[Entity], Iterable[TableRow]]
    ]
) -> Tuple[
    StatePerson,
    Dict[Union[EntityClassName, TableName], Union[Sequence[Entity], List[TableRow]]],
]:
    """In the calculation pipelines we use the CoGroupByKey function to group StatePerson entities with their associated
    entities. The output of CoGroupByKey is a dictionary where the keys are the variable names expected in the
    identifier step of the pipeline, and the values are iterables of the associated entities.

    This function unpacks the output of CoGroupByKey (the given arg_to_entities_map) into the person (StatePerson)
    entity, and a kwarg dictionary mapping all of the arguments expected by the identifier to the list of entities the
    identifier needs.

    Returns a tuple containing the StatePerson and the kwarg dictionary.
    """
    entity_dict = kwargs_for_entity_lists(arg_to_entities_map)

    person_values = entity_dict.pop(StatePerson.__name__)

    if not person_values:
        raise ValueError(
            f"Found no person values in arg_to_entities_map: {arg_to_entities_map}"
        )

    person = one(person_values)

    if not person:
        raise ValueError(
            f"No StatePerson associated with these entities: {arg_to_entities_map}"
        )

    if not isinstance(person, StatePerson):
        raise ValueError(
            "The value in the entity dictionary for key "
            f"[{StatePerson.__name__}] should be of type StatePerson. "
            f"Found: {person}."
        )

    return person, entity_dict


def select_all_by_person_query(
    project_id: str,
    dataset: str,
    table: str,
    state_code_filter: str,
    person_id_filter_set: Optional[Set[int]],
) -> str:
    return select_query(
        project_id, dataset, table, state_code_filter, "person_id", person_id_filter_set
    )


def select_query(
    project_id: str,
    dataset: str,
    table: str,
    state_code_filter: str,
    root_entity_id_field: Optional[str],
    root_entity_id_filter_set: Optional[Set[RootEntityId]],
    columns_to_include: Optional[List[str]] = None,
) -> str:
    """Returns a query string formatted to select all contents of the table in the given
    dataset, filtering by the provided state code and root entity id filter sets, if
    necessary.
    """

    if not state_code_filter:
        raise ValueError(f"State code filter unexpectedly empty for table [{table}]")

    if not columns_to_include:
        columns_to_include = ["*"]

    entity_query = (
        f"SELECT {', '.join(columns_to_include)} FROM "
        f"`{project_id}.{dataset}.{table}` WHERE "
        f"state_code IN ('{state_code_filter}')"
    )

    if root_entity_id_filter_set:
        if not root_entity_id_field:
            raise ValueError(
                f"Expected non-null root_entity_id_field for nonnull root_entity_id_filter_set when querying"
                f"dataset [{dataset}] and table [{table}]."
            )

        id_str_set = {
            str(root_entity_id)
            for root_entity_id in root_entity_id_filter_set
            if str(root_entity_id)
        }

        entity_query = (
            entity_query
            + f" AND {root_entity_id_field} IN ({', '.join(sorted(id_str_set))})"
        )

    return entity_query


def build_staff_external_id_to_staff_id_map(
    state_person_to_state_staff_list: List[Dict[str, Any]]
) -> Dict[Tuple[str, str], int]:
    """Converts a list of dictionaries, each dictionary containing a
    staff_external_id+type, staff_id, and person_id, into one dictionary
    where each item has key (staff_external_id, staff_external_id_type)
    and value staff_id.
    """
    return {
        (id_set["staff_external_id"], id_set["staff_external_id_type"]): id_set[
            "staff_id"
        ]
        for id_set in state_person_to_state_staff_list
    }


def extract_county_of_residence_from_rows(
    persons_to_recent_county_of_residence: List[Dict[str, Any]]
) -> Optional[str]:
    """Extracts the single county of residence from a list of dictionaries representing rows in the
    persons_to_recent_county_of_residence table. Throws if there is more than one row (there should never be for a given
    person).
    """
    county_of_residence = None
    if persons_to_recent_county_of_residence:
        if len(persons_to_recent_county_of_residence) > 1:
            person_id = persons_to_recent_county_of_residence[0]["person_id"]
            raise ValueError(
                f"Found more than one county of residence for person with id [{person_id}]: "
                f"{persons_to_recent_county_of_residence}"
            )

        county_of_residence = persons_to_recent_county_of_residence[0][
            "county_of_residence"
        ]

    return county_of_residence
