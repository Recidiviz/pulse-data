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

"""Export data from Cloud SQL and load it into BigQuery."""

from http import HTTPStatus
import json
import logging
from typing import Optional

import flask
from flask import request
# Importing only for typing.
from google.cloud import bigquery

from recidiviz.calculator.bq import bq_load
from recidiviz.calculator.bq import bq_utils
from recidiviz.calculator.bq import cloudsql_export
from recidiviz.calculator.bq import export_config
from recidiviz.common import queues
from recidiviz.utils.auth import authenticate_request


def export_table_then_load_table(
        table: str,
        dataset_ref: Optional[bigquery.dataset.DatasetReference] = None)-> bool:
    """Exports a Cloud SQL table to CSV, then loads it into BigQuery.

    Waits until the BigQuery load is completed.

    Args:
        table: Table to export then import. Table must be defined
            in export_config.TABLES_TO_EXPORT.
        dataset_ref: The BigQuery dataset to load the table into.
            Gets created if it does not already exist.
            Uses export_config.BASE_TABLES_BQ_DATASET if not specified.
    Returns:
        True if load succeeds, else False.
    """
    if not dataset_ref:
        dataset_ref = bq_utils.client().dataset(
            export_config.BASE_TABLES_BQ_DATASET)

    export_success = cloudsql_export.export_table(table)
    if export_success: # pylint: disable=no-else-return
        load_success = bq_load.start_table_load_and_wait(dataset_ref, table)
        return load_success
    else:
        logging.error('Skipping BigQuery load of table "%s", '
                      'which failed to export.', table)
        return False


def export_then_load_all_sequentially():
    """Exports then loads each table sequentially.

    No operations for a new table happen until all operations for
    the previous table have completed.

    For example, for Tables A, B, C:
    1. Export Table A
    2. Load Table A
    3. Export Table B
    4. Load Table B
    5. Export Table C
    6. Load Table C

    There is no reason to load sequentially, but we must export sequentially
    because Cloud SQL can only support one export operation at a time.
    """
    for table in export_config.TABLES_TO_EXPORT:
        export_table_then_load_table(table.name)


def export_all_then_load_all():
    """Export all tables from Cloud SQL, then load all tables to BigQuery.

    Exports happen in sequence (one at a time),
    then once all exports are completed, the BigQuery loads happen in parallel.

    For example, for tables A, B, C:
    1. Export Table A
    2. Export Table B
    3. Export Table C
    4. Load Tables A, B, C in parallel.
    """
    cloudsql_export.export_all_tables(export_config.TABLES_TO_EXPORT)

    BASE_TABLES_DATASET_REF = bq_utils.client().dataset(
        export_config.BASE_TABLES_BQ_DATASET)
    bq_load.load_all_tables_concurrently(
        BASE_TABLES_DATASET_REF, export_config.TABLES_TO_EXPORT)


export_manager_blueprint = flask.Blueprint('export_manager', __name__)

@export_manager_blueprint.route('/export', methods=['POST'])
@authenticate_request
def handle_bq_export_task():
    """Worker function to handle BQ export task requests.

    Form data must be a bytes-encoded JSON object with parameters listed below.

    URL Parameters:
        table_name: Table to export then import. Table must be defined
            in export_config.TABLES_TO_EXPORT.
    """
    json_data = request.get_data(as_text=True)
    data = json.loads(json_data)
    table_name = data['table_name']

    success = export_table_then_load_table(table_name)

    return ('', HTTPStatus.OK if success else HTTPStatus.INTERNAL_SERVER_ERROR)


@export_manager_blueprint.route('/create_export_tasks')
@authenticate_request
def create_all_bq_export_tasks():
    """Creates an export task for each table to be exported.

    A task is created for each table defined in export_config.TABLES_TO_EXPORT.

    Re-creates all tasks if any task fails to be created.
    """
    for table in export_config.TABLES_TO_EXPORT:
        queues.create_bq_task(table.name, '/export_manager/export')
    return ('', HTTPStatus.OK)


if __name__ == '__main__':
    export_all_then_load_all()
