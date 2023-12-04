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
"""BigQuery Methods for the Spark population projection simulation"""

import datetime
from typing import Any, Dict, List

import pandas as pd
import pandas_gbq

from recidiviz.calculator.modeling.population_projection.utils.bq_utils import (
    store_simulation_results,
)
from recidiviz.utils.yaml_dict import YAMLDict

# Constants for the Spark input data
SPARK_INPUT_PROJECT_ID = "recidiviz-staging"
SPARK_INPUT_DATASET = "spark_public_input_data"

ADMISSIONS_DATA_TABLE_NAME = "admissions_data_raw"
ADMISSIONS_SCHEMA = [
    {"name": "simulation_tag", "type": "STRING", "mode": "REQUIRED"},
    {"name": "time_step", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "compartment", "type": "STRING", "mode": "REQUIRED"},
    {"name": "admission_to", "type": "STRING", "mode": "REQUIRED"},
    {"name": "cohort_population", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "simulation_group", "type": "STRING", "mode": "REQUIRED"},
    {"name": "date_created", "type": "TIMESTAMP", "mode": "REQUIRED"},
]


TRANSITIONS_DATA_TABLE_NAME = "transitions_data_raw"
TRANSITIONS_SCHEMA = [
    {"name": "simulation_tag", "type": "STRING", "mode": "REQUIRED"},
    {"name": "compartment_duration", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "compartment", "type": "STRING", "mode": "REQUIRED"},
    {"name": "outflow_to", "type": "STRING", "mode": "REQUIRED"},
    {"name": "cohort_portion", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "simulation_group", "type": "STRING", "mode": "NULLABLE"},
    {"name": "date_created", "type": "TIMESTAMP", "mode": "REQUIRED"},
]

POPULATION_DATA_TABLE_NAME = "population_data_raw"
POPULATION_SCHEMA = [
    {"name": "simulation_tag", "type": "STRING", "mode": "REQUIRED"},
    {"name": "time_step", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "compartment", "type": "STRING", "mode": "REQUIRED"},
    {"name": "compartment_population", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "simulation_group", "type": "STRING", "mode": "NULLABLE"},
    {"name": "date_created", "type": "TIMESTAMP", "mode": "REQUIRED"},
]


def _validate_schema(
    schema: List[Dict[str, str]], dataframe: pd.DataFrame, title: str
) -> None:
    """Validate that dataframes match schemas"""

    required_columns = {item["name"] for item in schema if item["mode"] == "REQUIRED"}
    allowed_columns = {item["name"] for item in schema}

    missing_columns = required_columns.difference(dataframe.columns)
    if missing_columns:
        raise ValueError(f"Table '{title}' missing required columns {missing_columns}")

    extra_columns = set(dataframe.columns).difference(allowed_columns)
    if extra_columns:
        raise ValueError(f"Table '{title}' contains unexpected columns {extra_columns}")

    dtype_translation = {
        "STRING": "object",
        "INTEGER": "int64",
        "FLOAT": "float64",
        "TIMESTAMP": "datetime64[ns]",
    }
    for item in schema:
        if dataframe[item["name"]].empty:
            continue
        expected_type = dtype_translation[item["type"]]
        gotten_type = dataframe[item["name"]].dtypes
        if dataframe[item["name"]].dtypes != dtype_translation[item["type"]]:
            raise ValueError(
                f"Table '{title}' has wrong type for column '{item['name']}'. Type '{gotten_type}' should be '{expected_type}'"
            )


def _validate_data(project_id: str, uploads: List[Dict[str, Any]]) -> None:
    """ "Basic validation of data in dataframes"""

    if project_id not in ["recidiviz-staging", "recidiviz-123"]:
        raise ValueError(f"{project_id} is not a supported gcloud BigQuery project")

    # Must have exactly the right columns; must not contain null values
    incorrect_columns = []
    for params in uploads:
        table_name = params["table"][:-4]

        # check for null items in dataframe
        if params["data_df"].isnull().values.any():
            raise ValueError(f"Table '{table_name}' must not contain null values")

        # check that dataframe has the right data fields
        if params["table"] == POPULATION_DATA_TABLE_NAME:
            # population data is allowed to not be disaggregated
            if not (
                (
                    set(params["data_df"].columns)
                    == {
                        i["name"]
                        for i in POPULATION_SCHEMA
                        if i["name"] not in ["simulation_tag", "date_created"]
                    }
                )
                | (
                    set(params["data_df"].columns)
                    == {
                        i["name"]
                        for i in POPULATION_SCHEMA
                        if i["name"]
                        not in ["simulation_tag", "date_created", "simulation_group"]
                    }
                )
            ):
                incorrect_columns.append(table_name)

        if params["table"] == ADMISSIONS_DATA_TABLE_NAME:
            if set(params["data_df"].columns) != {
                i["name"]
                for i in ADMISSIONS_SCHEMA
                if i["name"] not in ["simulation_tag", "date_created"]
            }:
                incorrect_columns.append(table_name)

        if params["table"] == TRANSITIONS_DATA_TABLE_NAME:
            if set(params["data_df"].columns) != {
                i["name"]
                for i in TRANSITIONS_SCHEMA
                if i["name"] not in ["simulation_tag", "date_created"]
            }:
                incorrect_columns.append(table_name)

    if len(incorrect_columns) != 0:
        raise ValueError(f"Tables {incorrect_columns} dont have the right columns")


def _validate_yaml(yaml_path: str) -> None:
    "Validate the contents of the relevant yaml file"

    yaml_dict = YAMLDict.from_path(yaml_path)

    # Check for all required and no extra inputs
    required_inputs = {
        "user_inputs",
        "compartments_architecture",
        "reference_date",
        "time_step",
        "data_inputs",
        "per_year_costs",
    }
    given_inputs = set(yaml_dict.keys())

    missing_inputs = required_inputs.difference(given_inputs)
    if len(missing_inputs) > 0:
        raise ValueError(f"Missing yaml inputs: {missing_inputs}")

    unexpected_inputs = given_inputs.difference(required_inputs)
    if len(unexpected_inputs) > 0:
        raise ValueError(f"Unexpected yaml inputs: {unexpected_inputs}")


def upload_spark_model_inputs(
    project_id: str,
    simulation_tag: str,
    admissions_data_df: pd.DataFrame,
    transitions_data_df: pd.DataFrame,
    population_data_df: pd.DataFrame,
    yaml_path: str,
) -> None:
    """Reformat the preprocessed model input data to match the table schema and upload them to BigQuery"""
    # Set the upload timestamp for all tables
    upload_time = datetime.datetime.now()

    uploads = [
        {
            "table": ADMISSIONS_DATA_TABLE_NAME,
            "schema": ADMISSIONS_SCHEMA,
            "data_df": admissions_data_df.copy(),
        },
        {
            "table": TRANSITIONS_DATA_TABLE_NAME,
            "schema": TRANSITIONS_SCHEMA,
            "data_df": transitions_data_df.copy(),
        },
        {
            "table": POPULATION_DATA_TABLE_NAME,
            "schema": POPULATION_SCHEMA,
            "data_df": population_data_df.copy(),
        },
    ]
    # Validate that the dataframe data won't cause errors when running simulation
    _validate_data(project_id, uploads)
    # Validate the relevant yaml file
    if yaml_path is not None:
        _validate_yaml(yaml_path)

    # Process dataframe and check against schema
    for params in uploads:
        if params["data_df"].empty:
            continue
        params["data_df"].loc[:, "simulation_tag"] = simulation_tag
        params["data_df"].loc[:, "date_created"] = upload_time
        # applies for population data that's not disaggregated
        if "simulation_group" not in params["data_df"].columns:
            params["data_df"].loc[:, "simulation_group"] = None

        table_name = params["table"][: (len(params["table"]) - 4)]
        _validate_schema(params["schema"], params["data_df"], table_name)

    for params in uploads:
        if params["data_df"].empty:
            continue

        store_simulation_results(
            project_id,
            SPARK_INPUT_DATASET,
            params["table"],
            params["schema"],
            params["data_df"],
        )


def load_spark_table_from_big_query(
    table_name: str, simulation_tag: str
) -> pd.DataFrame:
    """Pull all data from a table for a specific state and run date"""

    query = f"""
        WITH latest_runs AS
        (
        SELECT simulation_tag, MAX(date_created) as latest_run
        FROM {SPARK_INPUT_DATASET}.{table_name}
        GROUP BY simulation_tag
        )
        SELECT data.*
        FROM {SPARK_INPUT_DATASET}.{table_name} data
        LEFT JOIN latest_runs ON data.simulation_tag = latest_runs.simulation_tag
        WHERE data.simulation_tag = '{simulation_tag}'
            AND date_created = latest_runs.latest_run
        """

    table_results = pandas_gbq.read_gbq(query, project_id=SPARK_INPUT_PROJECT_ID)
    return table_results
