# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Interface for working with the Spreadsheet model."""
import datetime
import itertools
import logging
import os
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
from sqlalchemy.orm import Session
from werkzeug.datastructures import FileStorage

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.io.flask_file_storage_contents_handle import (
    FlaskFileStorageContentsHandle,
)
from recidiviz.common.io.local_file_contents_handle import LocalFileContentsHandle
from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.agency_user_account_association import (
    AgencyUserAccountAssociationInterface,
)
from recidiviz.justice_counts.bulk_upload.workbook_uploader import WorkbookUploader
from recidiviz.justice_counts.datapoint import DatapointInterface
from recidiviz.justice_counts.exceptions import (
    BulkUploadMessageType,
    JusticeCountsBulkUploadException,
    JusticeCountsServerError,
)
from recidiviz.justice_counts.metricfiles.metricfile_registry import (
    SYSTEM_TO_FILENAME_TO_METRICFILE,
)
from recidiviz.justice_counts.metrics.metric_definition import MetricDefinition
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.justice_counts.types import DatapointJson
from recidiviz.justice_counts.utils.constants import (
    DISAGGREGATED_BY_SUPERVISION_SUBSYSTEMS,
)
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.utils import metadata


class SpreadsheetInterface:
    """Contains methods for working with Spreadsheets.
    The Spreadsheet data model contains metadata about spreadsheets that have been uploaded
    by users for ingest.
    """

    @staticmethod
    def update_spreadsheet(
        spreadsheet: schema.Spreadsheet,
        status: str,
        auth0_user_id: str,
    ) -> schema.Spreadsheet:
        spreadsheet.status = schema.SpreadsheetStatus[status]
        if status == schema.SpreadsheetStatus.INGESTED.value:
            spreadsheet.ingested_by = auth0_user_id
            spreadsheet.ingested_at = datetime.datetime.now(tz=datetime.timezone.utc)
        return spreadsheet

    @staticmethod
    def save_spreadsheet_metadata(
        session: Session,
        system: schema.System,
        agency_id: int,
        filename: str,
        auth0_user_id: Optional[str] = None,
    ) -> schema.Spreadsheet:
        uploaded_at = datetime.datetime.now(tz=datetime.timezone.utc)
        standardized_name = (
            f"{str(agency_id)}:{system.value}:{uploaded_at.timestamp()}.xlsx"
        )
        spreadsheet = schema.Spreadsheet(
            original_name=os.path.basename(filename) if filename is not None else "",
            standardized_name=standardized_name,
            agency_id=agency_id,
            system=system,
            status=schema.SpreadsheetStatus.UPLOADED,
            uploaded_at=uploaded_at,
            uploaded_by=auth0_user_id,
        )
        session.add(spreadsheet)
        return spreadsheet

    @staticmethod
    def upload_spreadsheet(
        session: Session,
        system: str,
        agency_id: int,
        auth0_user_id: str,
        file_storage: FileStorage,
    ) -> schema.Spreadsheet:
        """Uploads spreadsheets representing agency data to google cloud storage."""
        fs = GcsfsFactory.build()
        spreadsheet = SpreadsheetInterface.save_spreadsheet_metadata(
            session=session,
            system=schema.System[system],
            agency_id=agency_id,
            auth0_user_id=auth0_user_id,
            filename=file_storage.filename or "",
        )
        fs.upload_from_contents_handle_stream(
            path=SpreadsheetInterface.get_spreadsheet_path(spreadsheet=spreadsheet),
            contents_handle=FlaskFileStorageContentsHandle(file_storage=file_storage),
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        session.add(spreadsheet)
        return spreadsheet

    @staticmethod
    def get_spreadsheets_json(
        spreadsheets: List[schema.Spreadsheet],
        session: Session,
        user: schema.UserAccount,
    ) -> List[Dict[str, Any]]:
        uploader_id_to_json = (
            AgencyUserAccountAssociationInterface.get_uploader_id_to_json(
                session=session, spreadsheets=spreadsheets, user=user
            )
        )
        return [
            {
                "id": spreadsheet.id,
                "name": spreadsheet.original_name,
                "uploaded_at": spreadsheet.uploaded_at.timestamp() * 1000,
                "uploaded_by_v2": uploader_id_to_json.get(
                    spreadsheet.uploaded_by, {"name": "Automatic Upload", "role": None}
                ),
                "ingested_at": spreadsheet.ingested_at.timestamp() * 1000
                if spreadsheet.ingested_at is not None
                else None,
                "status": spreadsheet.status.value,
                "system": spreadsheet.system.value,
            }
            for spreadsheet in spreadsheets
        ]

    @staticmethod
    def get_agency_spreadsheets(
        agency_id: int,
        session: Session,
    ) -> List[schema.Spreadsheet]:
        """Returns spreadsheet for an agency"""
        spreadsheets = (
            session.query(schema.Spreadsheet)
            .filter(schema.Spreadsheet.agency_id == agency_id)
            .all()
        )

        def calc_last_edited_at(s: schema.Spreadsheet) -> float:
            return (datetime.datetime.now().timestamp() - s.uploaded_at.timestamp()) + (
                datetime.datetime.now().timestamp() - s.ingested_at.timestamp()
                if s.ingested_at is not None
                else datetime.datetime.now().timestamp()
            )

        spreadsheets = sorted(spreadsheets, key=calc_last_edited_at)
        return spreadsheets

    @staticmethod
    def get_spreadsheet_by_id(
        session: Session, spreadsheet_id: int
    ) -> schema.Spreadsheet:
        return (
            session.query(schema.Spreadsheet)
            .filter(schema.Spreadsheet.id == spreadsheet_id)
            .one()
        )

    @staticmethod
    def download_spreadsheet(
        session: Session,
        agency_ids: List[int],
        spreadsheet_id: int,
    ) -> LocalFileContentsHandle:
        """Retrieves a spreadsheet from GCS and returns the file as a temporary file."""
        spreadsheet = SpreadsheetInterface.get_spreadsheet_by_id(
            session=session, spreadsheet_id=spreadsheet_id
        )
        if spreadsheet.agency_id not in agency_ids:
            raise JusticeCountsServerError(
                code="bad_download_permissions",
                description="User does not have the permissions to download the spreadsheet because they do not belong to the correct agency",
            )
        fs = GcsfsFactory.build()
        file = fs.download_to_temp_file(
            path=SpreadsheetInterface.get_spreadsheet_path(spreadsheet=spreadsheet),
            retain_original_filename=True,
        )
        if file is None:
            raise JusticeCountsServerError(
                code="spreadsheet_download_error",
                description="The selected spreadsheet could not be downloaded",
            )
        return file

    @staticmethod
    def delete_spreadsheet(
        session: Session,
        spreadsheet_id: int,
    ) -> None:
        """Deletes a spreadsheet from GCS and its metadata from the Spreadsheet table."""
        spreadsheet = SpreadsheetInterface.get_spreadsheet_by_id(
            session=session, spreadsheet_id=spreadsheet_id
        )
        fs = GcsfsFactory.build()
        fs.delete(
            path=SpreadsheetInterface.get_spreadsheet_path(spreadsheet=spreadsheet),
        )
        session.delete(spreadsheet)

    @staticmethod
    def ingest_spreadsheet(
        session: Session,
        xls: pd.ExcelFile,
        spreadsheet: schema.Spreadsheet,
        metric_key_to_agency_datapoints: Dict[str, List[schema.Datapoint]],
        metric_definitions: List[MetricDefinition],
        agency: schema.Agency,
        auth0_user_id: Optional[str] = None,
    ) -> Tuple[
        Dict[str, List[DatapointJson]],
        Dict[Optional[str], List[JusticeCountsBulkUploadException]],
        Set[int],
        List[int],
        Set[int],
    ]:
        """Ingests spreadsheet for an agency and logs any errors."""
        user_account = None
        if auth0_user_id is not None:
            user_account = (
                session.query(schema.UserAccount)
                .filter(schema.UserAccount.auth0_user_id == auth0_user_id)
                .one()
            )
        child_agencies = AgencyInterface.get_child_agencies_by_super_agency_id(
            session=session, agency_id=agency.id
        )
        uploader = WorkbookUploader(
            agency=agency,
            system=spreadsheet.system,
            child_agency_name_to_id={
                a.name.strip().lower(): a.id for a in child_agencies
            },
            user_account=user_account,
            metric_key_to_agency_datapoints=metric_key_to_agency_datapoints,
        )
        (
            metric_key_to_datapoint_jsons,
            metric_key_to_errors,
        ) = uploader.upload_workbook(
            session=session,
            xls=xls,
            metric_definitions=metric_definitions,
        )
        is_ingest_successful = all(
            isinstance(e, JusticeCountsBulkUploadException)
            and e.message_type != BulkUploadMessageType.ERROR
            for e in itertools.chain(*metric_key_to_errors.values())
        )
        # If there are ingest-blocking errors, log errors to console and set the spreadsheet status to ERRORED
        if not is_ingest_successful:
            logging.info(
                "Failed to ingest without errors: agency_id: %i, spreadsheet_id: %i, errors: %s",
                agency.id,
                spreadsheet.id,
                metric_key_to_errors,
            )
            spreadsheet.status = schema.SpreadsheetStatus.ERRORED

        else:
            logging.info(
                "Ingest successful for agency_id %i, spreadsheet_id: %i",
                agency.id,
                spreadsheet.id,
            )
            spreadsheet.ingested_by = auth0_user_id
            spreadsheet.ingested_at = datetime.datetime.now(tz=datetime.timezone.utc)
            spreadsheet.status = schema.SpreadsheetStatus.INGESTED

        unchanged_report_ids = {
            id
            for id in uploader.uploaded_report_ids
            if id not in uploader.updated_report_ids
            and id in uploader.existing_report_ids
        }

        return (
            metric_key_to_datapoint_jsons,
            metric_key_to_errors,
            uploader.updated_report_ids,
            uploader.existing_report_ids,
            unchanged_report_ids,
        )

    @staticmethod
    def get_spreadsheet_path(spreadsheet: schema.Spreadsheet) -> GcsfsFilePath:
        bucket_name = f"{metadata.project_id()}-justice-counts-control-panel-ingest"
        return GcsfsFilePath(
            bucket_name=bucket_name,
            blob_name=spreadsheet.standardized_name,
        )

    @staticmethod
    def get_ingest_spreadsheet_json(
        metric_key_to_datapoint_jsons: Dict[str, List[DatapointJson]],
        metric_key_to_errors: Dict[
            Optional[str], List[JusticeCountsBulkUploadException]
        ],
        metric_definitions: List[MetricDefinition],
        metric_key_to_agency_datapoints: Dict[str, List[schema.Datapoint]],
        updated_report_ids: Set[int],
        new_report_jsons: List[Dict[str, Any]],
        unchanged_report_ids: Set[int],
    ) -> Dict[str, Any]:
        """Returns json response for spreadsheets ingested with the BulkUploader"""
        metrics = []
        metric_key_to_enabled = {
            metric_definition.key: None for metric_definition in metric_definitions
        }
        metric_key_to_disaggregation_status = {}
        for _, agency_datapoints in metric_key_to_agency_datapoints.items():
            for datapoint in agency_datapoints:
                if (
                    datapoint.report_id is None
                    and datapoint.value is None
                    and datapoint.get_dimension_id() is None
                ):
                    metric_key_to_enabled[
                        datapoint.metric_definition_key
                    ] = datapoint.enabled

                if datapoint.context_key == DISAGGREGATED_BY_SUPERVISION_SUBSYSTEMS:
                    metric_key_to_disaggregation_status[
                        datapoint.metric_definition_key
                    ] = datapoint.value == str(True)

        for metric_definition in metric_definitions:
            # Do not add metric to response if the metric definition has
            # been disabled by JC.
            if metric_definition.disabled:
                continue

            if (
                metric_definition.system in schema.System.supervision_subsystems()
                and metric_key_to_disaggregation_status.get(metric_definition.key)
                is not True
            ):
                # If the metric is a supervision subsystem, but the metric is reported as an aggregate,
                # don't display any messages for that metric.
                continue

            if (
                metric_definition.system == schema.System.SUPERVISION
                and metric_key_to_disaggregation_status.get(metric_definition.key)
                is True
            ):
                # If the metric is part of the supervision system, but the metric is disaggregated by
                # supervision subsystem, don't display any messages for that metric.
                continue

            sheet_name_to_metricfile = SYSTEM_TO_FILENAME_TO_METRICFILE[
                metric_definition.system.value
            ]
            # For each sheet (i.e arrests_by_type) in an excel workbook that raised
            # an exception, jsonify the exception information so that it can be rendered
            # for the user on the bulk upload error page.
            metric_errors: List[Dict[str, Any]] = []
            for sheet_name, errors in itertools.groupby(
                metric_key_to_errors.get(metric_definition.key, []),
                key=lambda e: e.sheet_name,
            ):
                metric_errors.append(
                    {
                        "display_name": sheet_name_to_metricfile[sheet_name].display_name  # type: ignore[union-attr]
                        if sheet_name in sheet_name_to_metricfile
                        else None,
                        "sheet_name": sheet_name,
                        "messages": [e.to_json() for e in errors],
                    }
                )

            metrics.append(
                {
                    "key": metric_definition.key,
                    "display_name": metric_definition.display_name,
                    "metric_errors": metric_errors,
                    "datapoints": metric_key_to_datapoint_jsons.get(
                        metric_definition.key, []
                    ),
                    "enabled": metric_key_to_enabled[metric_definition.key],
                }
            )
        # Errors that are not associated with a metric are non-metric errors.
        # For example, a non-metric error would be raised if a user uploads an
        # excel workbook that contains a sheet that is not associated with a MetricFile.
        # This is an ingest-blocking error because in this scenario we are not able
        # to convert the rows into datapoints.
        non_metric_errors = [e.to_json() for e in metric_key_to_errors.get(None, [])]
        updated_report_ids_list = list(updated_report_ids)
        unchanged_report_ids_list = list(unchanged_report_ids)

        return {
            "metrics": metrics,
            "non_metric_errors": non_metric_errors,
            "updated_report_ids": updated_report_ids_list,
            "new_reports": new_report_jsons,
            "unchanged_report_ids": unchanged_report_ids_list,
        }

    @staticmethod
    def ingest_workbook_from_gcs(
        session: Session,
        bucket_name: str,
        system: schema.System,
        agency: schema.Agency,
        filename: str,
    ) -> Tuple[
        Dict[str, List[DatapointJson]],
        Dict[Optional[str], List[JusticeCountsBulkUploadException]],
        Set[int],
        List[int],
        Set[int],
    ]:
        """Given a filename, an agency, and a system, this method copies the
        file from the agency's bulk upload bucket to GCS bucket where we store
        all workbooks uploaded in publisher, saves the workbook metadata in
        the Justice Counts DB, and ingests the workbook and saves the data in
        the Justice Counts DB.
        """
        spreadsheet = SpreadsheetInterface.save_spreadsheet_metadata(
            session=session,
            system=system,
            agency_id=agency.id,
            filename=filename,
        )

        source_path = GcsfsFilePath.from_absolute_path(f"{bucket_name}/{filename}")
        destination_path = SpreadsheetInterface.get_spreadsheet_path(
            spreadsheet=spreadsheet
        )

        gcs_file_system = GcsfsFactory.build()
        # We need to copy the file into the bucket where we store workbooks
        # uploaded in Publisher because we reference that bucket when
        # fetching data for the Uploaded Files page in the Settings tab.
        gcs_file_system.copy(source_path, destination_path)

        metric_key_to_agency_datapoints = (
            DatapointInterface.get_metric_key_to_agency_datapoints(
                session=session, agency_id=agency.id
            )
        )
        # get metric definition for spreadsheet
        metric_definitions = SpreadsheetInterface.get_metric_definitions_for_workbook(
            system=system, agency=agency
        )
        file_bytes = gcs_file_system.download_as_bytes(path=source_path)
        return SpreadsheetInterface.ingest_spreadsheet(
            session=session,
            spreadsheet=spreadsheet,
            auth0_user_id=None,
            xls=pd.ExcelFile(file_bytes),
            agency=agency,
            metric_key_to_agency_datapoints=metric_key_to_agency_datapoints,
            metric_definitions=metric_definitions,
        )

    @staticmethod
    def get_metric_definitions_for_workbook(
        system: schema.System, agency: schema.Agency
    ) -> List[MetricDefinition]:
        return MetricInterface.get_metric_definitions_by_systems(
            systems={
                schema.System[system]
                for system in agency.systems or []
                if schema.System[system] in schema.System.supervision_subsystems()
                or schema.System[system] == schema.System.SUPERVISION
            }
            if system == "SUPERVISION"
            # Only send over metric definitions for the current system unless
            # the agency is uploading for supervision, which sheets contain
            # data for many supervision systems such as OTHER_SUPERVISION, PAROLE,
            # and PROBATION
            else {system},
        )
