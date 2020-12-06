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
"""Methods to upload the population projection results to BigQuery"""

import datetime
import pandas as pd

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


def upload_spark_results(project_id, simulation_tag, cost_avoidance_df, life_years_df, population_change_df,
                         cost_avoidance_non_cumulative_df):
    """Reformat the simulation results to match the table schema and upload them to BigQuery"""

    # Set the upload timestamp for all tables
    upload_time = datetime.datetime.now()
    data_format_args = {'simulation_tag': simulation_tag, 'upload_time': upload_time}

    cost_avoidance_table = format_spark_results(cost_avoidance_df, 'total_cost', **data_format_args)
    store_simulation_results(project_id, COST_AVOIDANCE_TABLE_NAME, COST_AVOIDANCE_SCHEMA, cost_avoidance_table)

    cost_avoidance_non_cumulative_table = format_spark_results(cost_avoidance_non_cumulative_df, 'total_cost',
                                                               **data_format_args)
    store_simulation_results(project_id, COST_AVOIDANCE_NON_CUMULATIVE_TABLE_NAME, COST_AVOIDANCE_SCHEMA,
                             cost_avoidance_non_cumulative_table)

    life_years_table = format_spark_results(life_years_df, 'life_years', **data_format_args)
    store_simulation_results(project_id, LIFE_YEARS_TABLE_NAME, LIFE_YEARS_SCHEMA, life_years_table)

    population_table = format_spark_results(population_change_df, 'population', **data_format_args)
    store_simulation_results(project_id, POPULATION_TABLE_NAME, POPULATION_SCHEMA, population_table)


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
    df['year'] = round(df['year'], 5)
    df['month'] = (12 * (df['year'] % 1)).round(0).astype(int) + 1
    df['year'] = df['year'].astype(int)
    df['day'] = 1

    df['simulation_date'] = pd.to_datetime(df[['year', 'month', 'day']]).dt.date

    df = df.drop(['year', 'month', 'day'], axis=1)

    # Add metadata columns to the output table
    df['simulation_tag'] = simulation_tag
    df['date_created'] = upload_time
    return df


def store_simulation_results(project_id, table_name, table_schema, data):
    """Append the new results to the BigQuery table in the spark output dataset"""

    if project_id not in ['recidiviz-staging', 'recidiviz-123']:
        raise ValueError(f"`{project_id}` is not a supported gcloud BigQuery project")

    # Reorder the columns to match the schema ordering
    column_order = [column['name'] for column in table_schema]
    data = data[column_order]

    # Append the results in BigQuery
    data.to_gbq(
        destination_table=f'{SPARK_OUTPUT_DATASET}.{table_name}',
        project_id=project_id,
        if_exists='append',
        chunksize=100000,
        table_schema=table_schema
    )
