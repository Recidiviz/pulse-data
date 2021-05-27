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
"""Provides and implements a CloudSQLClient interface."""
import abc
import logging
import time
from typing import List, Optional

from googleapiclient import discovery

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.utils import metadata


class CloudSQLClient:
    """Interface for a wrapper around the Cloud SQL service-discovery-based client."""

    @property
    @abc.abstractmethod
    def project_id(self) -> str:
        """The Google Cloud project id for this client."""

    @abc.abstractmethod
    def export_to_gcs_csv(
        self,
        instance_name: str,
        table_name: str,
        gcs_uri: GcsfsFilePath,
        columns: List[str],
    ) -> Optional[str]:
        """Triggers a Cloud SQL Export operation and returns the associated operation id or None if unsuccessful."""

    @abc.abstractmethod
    def import_gcs_csv(
        self,
        instance_name: str,
        table_name: str,
        gcs_uri: GcsfsFilePath,
        columns: List[str],
    ) -> Optional[str]:
        """Triggers a Cloud SQL Import operation and returns the associated operation id or None if unsuccessful."""

    @abc.abstractmethod
    def wait_until_operation_completed(
        self, operation_id: str, seconds_to_wait: int = 30
    ) -> bool:
        """Returns True if the operation succeeded and False if failed or the timeout was met."""


class CloudSQLClientImpl:
    """Base implementation for CloudSQLClient."""

    def __init__(self, project_id: Optional[str] = None):
        if not project_id:
            project_id = metadata.project_id()

        if not project_id:
            raise ValueError(
                "Must provide a project_id if metadata.project_id() returns None"
            )

        self._project_id = project_id
        self.service = discovery.build("sqladmin", "v1beta4")

    @property
    def project_id(self) -> str:
        return self._project_id

    def export_to_gcs_csv(
        self,
        instance_name: str,
        table_name: str,
        gcs_uri: GcsfsFilePath,
        columns: List[str],
    ) -> Optional[str]:
        logging.debug("Starting Cloud SQL export operation.")
        req = self.service.instances().export(
            project=self.project_id,
            instance=instance_name,
            body={
                "exportContext": {
                    "csvExportOptions": {
                        "selectQuery": f"SELECT {', '.join(columns)} FROM {table_name}",
                    },
                    "databases": ["postgres"],
                    "fileType": "CSV",
                    "uri": f"gs://{gcs_uri.abs_path()}",
                },
            },
        )
        resp = req.execute()
        return resp.get("name")

    def import_gcs_csv(
        self,
        instance_name: str,
        table_name: str,
        gcs_uri: GcsfsFilePath,
        columns: List[str],
    ) -> Optional[str]:
        logging.debug("Starting Cloud SQL import operation.")
        req = self.service.instances().import_(
            project=self.project_id,
            instance=instance_name,
            body={
                "importContext": {
                    "csvImportOptions": {
                        "columns": columns,
                        "table": table_name,
                    },
                    "database": "postgres",
                    "fileType": "CSV",
                    "uri": f"gs://{gcs_uri.abs_path()}",
                },
            },
        )
        resp = req.execute()
        return resp.get("name")

    def wait_until_operation_completed(
        self, operation_id: str, seconds_to_wait: int = 30
    ) -> bool:
        """Sleeps until the Cloud SQL operation with the given id has finished,
        returning whether or not it finished successfully/healthily."""
        start = time.time()
        while time.time() - start < seconds_to_wait:
            logging.debug("Issuing new Cloud SQL operation get request.")
            req = self.service.operations().get(
                project=self.project_id,
                operation=operation_id,
            )
            resp = req.execute()

            status = resp.get("status")
            if status == "DONE":
                error = resp.get("error")
                if not error:
                    return True
                logging.error("Error running CloudSQL operation: %s", error)
                return False
            if status == "SQL_OPERATION_STATUS_UNSPECIFIED":
                return False
            if status not in ["PENDING", "RUNNING"]:
                logging.warning(
                    "Found unexpected status for Cloud SQL operation: %s", status
                )

            time.sleep(1)
        logging.warning(
            "Cloud SQL operation passed specified timeout but will continue in the background."
        )
        return False
