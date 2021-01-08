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
import pandas as pd
import pandas_gbq

from recidiviz.calculator.modeling.population_projection.bq_utils import store_simulation_results, \
    add_simulation_date_column

# Constants for the Spark Output data
SPARK_OUTPUT_DATASET = 'population_projection_data'

COST_AVOIDANCE_TABLE_NAME = 'cost_avoidance_estimate_raw'
COST_AVOIDANCE_NON_CUMULATIVE_TABLE_NAME = 'cost_avoidance_non_cumulative_estimate_raw'
COST_AVOIDANCE_SCHEMA = [
    {'name': 'simulation_tag', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'simulation_date', 'type': 'DATE', 'mode': 'REQUIRED'},
    {'name': 'compartment', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'total_cost', 'type': 'FLOAT', 'mode': 'REQUIRED'},
    {'name': 'date_created', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'}
]


LIFE_YEARS_TABLE_NAME = 'life_years_estimate_raw'
LIFE_YEARS_SCHEMA = [
    {'name': 'simulation_tag', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'simulation_date', 'type': 'DATE', 'mode': 'REQUIRED'},
    {'name': 'compartment', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'life_years', 'type': 'FLOAT', 'mode': 'REQUIRED'},
    {'name': 'date_created', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'}
]

POPULATION_TABLE_NAME = 'population_estimate_raw'
POPULATION_SCHEMA = [
    {'name': 'simulation_tag', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'simulation_date', 'type': 'DATE', 'mode': 'REQUIRED'},
    {'name': 'scenario', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'compartment', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'population', 'type': 'FLOAT', 'mode': 'REQUIRED'},
    {'name': 'date_created', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'}
]

# Constants for the Spark input data
SPARK_INPUT_PROJECT_ID = 'recidiviz-staging'
SPARK_INPUT_DATASET = 'spark_public_input_data'

OUTFLOWS_DATA_TABLE_NAME = 'outflows_data_raw'
OUTFLOWS_SCHEMA = [
    {'name': 'simulation_tag', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'time_step', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    {'name': 'compartment', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'outflow_to', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'total_population', 'type': 'FLOAT', 'mode': 'REQUIRED'},
    {'name': 'crime', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'crime_type', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'age', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'race', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'date_created', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'}
]


TRANSITIONS_DATA_TABLE_NAME = 'transitions_data_raw'
TRANSITIONS_SCHEMA = [
    {'name': 'simulation_tag', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'compartment_duration', 'type': 'FLOAT', 'mode': 'REQUIRED'},
    {'name': 'compartment', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'outflow_to', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'total_population', 'type': 'FLOAT', 'mode': 'REQUIRED'},
    {'name': 'crime', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'crime_type', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'age', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'race', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'date_created', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'}
]

TOTAL_POPULATION_DATA_TABLE_NAME = 'total_population_data_raw'
TOTAL_POPULATION_SCHEMA = [
    {'name': 'simulation_tag', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'time_step', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    {'name': 'compartment', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'total_population', 'type': 'FLOAT', 'mode': 'REQUIRED'},
    {'name': 'crime', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'crime_type', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'age', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'race', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'date_created', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'}
]


def upload_spark_model_inputs(project_id: str, simulation_tag: str, outflows_data_df: pd.DataFrame,
                              transitions_data_df: pd.DataFrame, total_population_data_df: pd.DataFrame):
    """Reformat the preprocessed model input data to match the table schema and upload them to BigQuery"""

    # Set the upload timestamp for all tables
    upload_time = datetime.datetime.now()

    uploads = [
        {'table': OUTFLOWS_DATA_TABLE_NAME, 'schema': OUTFLOWS_SCHEMA, 'data_df': outflows_data_df},
        {'table': TRANSITIONS_DATA_TABLE_NAME, 'schema': TRANSITIONS_SCHEMA, 'data_df': transitions_data_df},
        {'table': TOTAL_POPULATION_DATA_TABLE_NAME, 'schema': TOTAL_POPULATION_SCHEMA,
         'data_df': total_population_data_df}
    ]
    for params in uploads:
        if params['data_df'].empty:
            continue

        params['data_df']['simulation_tag'] = simulation_tag
        params['data_df']['date_created'] = upload_time
        for disaggregation_axis in ['crime', 'crime_type', 'age', 'race']:
            if disaggregation_axis not in params['data_df'].columns:
                params['data_df'][disaggregation_axis] = None

        store_simulation_results(project_id, SPARK_INPUT_DATASET, params['table'], params['schema'], params['data_df'])


def upload_spark_results(project_id, simulation_tag, cost_avoidance_df, life_years_df, population_change_df,
                         cost_avoidance_non_cumulative_df):
    """Reformat the simulation results to match the table schema and upload them to BigQuery"""

    # Set the upload timestamp for all tables
    upload_time = datetime.datetime.now()
    data_format_args = {'simulation_tag': simulation_tag, 'upload_time': upload_time}

    cost_avoidance_table = format_spark_results(cost_avoidance_df, 'total_cost', **data_format_args)
    store_simulation_results(project_id, SPARK_OUTPUT_DATASET, COST_AVOIDANCE_TABLE_NAME, COST_AVOIDANCE_SCHEMA,
                             cost_avoidance_table)

    cost_avoidance_non_cumulative_table = format_spark_results(cost_avoidance_non_cumulative_df, 'total_cost',
                                                               **data_format_args)
    store_simulation_results(project_id, SPARK_OUTPUT_DATASET, COST_AVOIDANCE_NON_CUMULATIVE_TABLE_NAME,
                             COST_AVOIDANCE_SCHEMA, cost_avoidance_non_cumulative_table)

    life_years_table = format_spark_results(life_years_df, 'life_years', **data_format_args)
    store_simulation_results(project_id, SPARK_OUTPUT_DATASET, LIFE_YEARS_TABLE_NAME, LIFE_YEARS_SCHEMA,
                             life_years_table)

    population_table = format_spark_results(population_change_df, 'population', **data_format_args)
    store_simulation_results(project_id, SPARK_OUTPUT_DATASET, POPULATION_TABLE_NAME, POPULATION_SCHEMA,
                             population_table)


def format_spark_results(df, value_name, simulation_tag, upload_time):
    """Change the format of the results to match the table schema with 1 row per compartment, year, and month
    `df` the pandas DataFrame to update
    `value_name` the column name to use for the values in the original results table
    `simulation_tag` the simulation name string to use for the results (typically the state and policy name)
    `upload_time` the timestamp to use for the `date_created` column in the BQ table
    """

    # If the table has 1 row per compartment and 1 column per scenario, then transform to 1 row per compartment/scenario
    if 'compartment' in df.columns:
        df = df.melt(id_vars=['year', 'compartment'], var_name='scenario', value_name=value_name)
    # Otherwise, if the table has 1 column per compartment, transform to 1 row per compartment
    else:
        df = df.reset_index().melt(id_vars='year', var_name='compartment', value_name=value_name)

    # Convert the fractional year column into the integer year and month columns
    df = add_simulation_date_column(df)

    # Add metadata columns to the output table
    df['simulation_tag'] = simulation_tag
    df['date_created'] = upload_time
    return df


def load_spark_table_from_big_query(table_name: str, simulation_tag: str):
    """Pull all data from a table for a specific state and run date"""

    query = \
        f"""
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
