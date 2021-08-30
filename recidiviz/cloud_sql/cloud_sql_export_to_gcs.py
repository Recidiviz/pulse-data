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
"""Implements admin panel route for exporting to GCS from Cloud SQL."""
from typing import List

from recidiviz.cloud_sql.cloud_sql_client import CloudSQLClientImpl
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)


def export_from_cloud_sql_to_gcs_csv(
    schema_type: SchemaType,
    table_name: str,
    gcs_uri: GcsfsFilePath,
    columns: List[str],
) -> None:
    cloud_sql_client = CloudSQLClientImpl()
    instance_name = SQLAlchemyEngineManager.get_stripped_cloudsql_instance_id(
        schema_type
    )
    if instance_name is None:
        raise ValueError("Could not find instance name.")
    operation_id = cloud_sql_client.export_to_gcs_csv(
        instance_name=instance_name,
        table_name=table_name,
        gcs_uri=gcs_uri,
        columns=columns,
    )
    if operation_id is None:
        raise RuntimeError("Cloud SQL export operation was not started successfully.")

    operation_succeeded = cloud_sql_client.wait_until_operation_completed(
        operation_id, 60
    )

    if not operation_succeeded:
        raise RuntimeError("Cloud SQL export failed.")
