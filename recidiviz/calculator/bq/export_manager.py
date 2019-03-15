# Recidiviz - a platform for tracking granular recidivism metrics in real time
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

import logging
from typing import Optional

# Importing only for typing.
from google.cloud import bigquery

from recidiviz.calculator.bq import bq_load
from recidiviz.calculator.bq import bq_utils
from recidiviz.calculator.bq import cloudsql_export
from recidiviz.calculator.bq import export_config


def export_table_then_load_table(
        table: str,
        dataset_ref: Optional[bigquery.dataset.DatasetReference] = None):
    """Exports a Cloud SQL table to CSV, then loads it into BigQuery.

    Waits until the BigQuery load is completed.

    Args:
        table: Table to export then import. Table must be defined
            in export_config.TABLES_TO_EXPORT.
        dataset_ref: The BigQuery dataset to load the table into.
            Gets created if it does not already exist.
            Uses export_config.BASE_TABLES_BQ_DATASET if not specified.
    """
    if not dataset_ref:
        dataset_ref = bq_utils.client().dataset(
            export_config.BASE_TABLES_BQ_DATASET)

    export_success = cloudsql_export.export_table(table)
    if export_success:
        bq_load.start_table_load_and_wait(
            dataset_ref, table)
    else:
        logging.error('Skipping BigQuery load of table "%s", '
                      'which failed to export.', table)


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


if __name__ == '__main__':
    export_all_then_load_all()
