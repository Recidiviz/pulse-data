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
from typing import Any, Dict, List, Tuple

import pandas as pd
from sqlalchemy.orm import Session
from werkzeug.datastructures import FileStorage

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.io.flask_file_storage_contents_handle import (
    FlaskFileStorageContentsHandle,
)
from recidiviz.common.io.local_file_contents_handle import LocalFileContentsHandle
from recidiviz.justice_counts.bulk_upload.bulk_upload import BulkUploader
from recidiviz.justice_counts.datapoint import DatapointInterface
from recidiviz.justice_counts.exceptions import (
    JusticeCountsBulkUploadException,
    JusticeCountsServerError,
)
from recidiviz.justice_counts.metricfiles.metricfile_registry import (
    SYSTEM_TO_FILENAME_TO_METRICFILE,
)
from recidiviz.justice_counts.metrics.metric_registry import METRICS_BY_SYSTEM
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.utils import metadata


class SpreadsheetInterface:
    """Contains methods for working with Spreadsheets.
    The Spreadsheet data model contains metadata about spreadsheets that have been uploaded
    by users for ingest.
    """

    @staticmethod
    def update_spreadsheet(
        session: Session, spreadsheet_id: int, status: str, auth0_user_id: str
    ) -> schema.Spreadsheet:
        spreadsheet = SpreadsheetInterface.get_spreadsheet_by_id(
            session=session, spreadsheet_id=spreadsheet_id
        )
        spreadsheet.status = schema.SpreadsheetStatus[status]
        if status == schema.SpreadsheetStatus.INGESTED.value:
            spreadsheet.ingested_by = auth0_user_id
            spreadsheet.ingested_at = datetime.datetime.now(tz=datetime.timezone.utc)
        session.commit()
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
        uploaded_at = datetime.datetime.now(tz=datetime.timezone.utc)
        standardized_name = f"{str(agency_id)}:{system}:{uploaded_at.timestamp()}.xlsx"
        spreadsheet = schema.Spreadsheet(
            original_name=os.path.basename(file_storage.filename)
            if file_storage.filename is not None
            else "",
            standardized_name=standardized_name,
            agency_id=agency_id,
            system=schema.System[system],
            status=schema.SpreadsheetStatus.UPLOADED,
            uploaded_at=uploaded_at,
            uploaded_by=auth0_user_id,
        )
        fs.upload_from_contents_handle_stream(
            path=SpreadsheetInterface.get_spreadsheet_path(spreadsheet=spreadsheet),
            contents_handle=FlaskFileStorageContentsHandle(file_storage=file_storage),
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        session.add(spreadsheet)
        session.commit()
        return spreadsheet

    @staticmethod
    def get_spreadsheet_json(
        spreadsheet: schema.Spreadsheet, session: Session
    ) -> Dict[str, Any]:
        uploaded_by_user = (
            session.query(schema.UserAccount)
            .filter(schema.UserAccount.auth0_user_id == spreadsheet.uploaded_by)
            .one()
        )
        return {
            "id": spreadsheet.id,
            "name": spreadsheet.original_name,
            "uploaded_at": spreadsheet.uploaded_at.timestamp() * 1000,
            "uploaded_by": uploaded_by_user.name,
            "ingested_at": spreadsheet.ingested_at.timestamp() * 1000
            if spreadsheet.ingested_at is not None
            else None,
            "status": spreadsheet.status.value,
            "system": spreadsheet.system.value,
        }

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
        session.commit()

    @staticmethod
    def ingest_spreadsheet(
        session: Session,
        xls: pd.ExcelFile,
        spreadsheet: schema.Spreadsheet,
        auth0_user_id: str,
        agency_id: int,
    ) -> Tuple[List[schema.Datapoint], Dict[str, Exception]]:
        """Ingests spreadsheet for an agency and logs any errors."""
        uploader = BulkUploader(catch_errors=True)
        user_account = (
            session.query(schema.UserAccount)
            .filter(schema.UserAccount.auth0_user_id == auth0_user_id)
            .one()
        )
        datapoints, ingest_errors = uploader.upload_excel(
            session=session,
            xls=xls,
            agency_id=spreadsheet.agency_id,
            system=spreadsheet.system,
            user_account=user_account,
        )

        is_ingest_successful = (
            any(  # pylint: disable=use-a-generator
                [
                    isinstance(e, JusticeCountsBulkUploadException)
                    and e.message_type == "ERROR"
                    for e in ingest_errors.values()
                ]
            )
            or len(ingest_errors) == 0
        )
        # If there are ingest-blocking errors, log errors to console and set the spreadsheet status to ERRORED
        if not is_ingest_successful:
            logging.error(
                "Failed to ingest without errors: agency_id: %i, spreadsheet_id: %i, errors: %s",
                agency_id,
                spreadsheet.id,
                ingest_errors,
            )
            spreadsheet.status = schema.SpreadsheetStatus.ERRORED

        else:
            logging.info(
                "Ingest successful for agency_id %i, spreadsheet_id: %i",
                agency_id,
                spreadsheet.id,
            )
            spreadsheet.ingested_by = auth0_user_id
            spreadsheet.ingested_at = datetime.datetime.now(tz=datetime.timezone.utc)
            spreadsheet.status = schema.SpreadsheetStatus.INGESTED

        session.commit()
        return datapoints, ingest_errors

    @staticmethod
    def get_spreadsheet_path(spreadsheet: schema.Spreadsheet) -> GcsfsFilePath:
        bucket_name = f"{metadata.project_id()}-justice-counts-control-panel-ingest"
        return GcsfsFilePath(
            bucket_name=bucket_name,
            blob_name=spreadsheet.standardized_name,
        )

    @staticmethod
    def get_ingest_spreadsheet_json(
        datapoints: List[schema.Datapoint],
        sheet_to_error: dict[str, Exception],
        system: str,
    ) -> Dict[str, Any]:
        """Returns json response for spreadsheets ingested with the BulkUploader"""
        metric_key_to_datapoints = {
            k: list(v)
            for k, v in itertools.groupby(
                datapoints,
                key=lambda d: d.metric_definition_key,
            )
        }
        filename_to_metricfile = SYSTEM_TO_FILENAME_TO_METRICFILE[system]
        metric_definitions = METRICS_BY_SYSTEM[system]
        metrics = []
        for metric_definition in metric_definitions:
            if metric_definition.disabled:
                continue
            # Create datapoint json for any datapoints that are associated with the metric.
            # This information will be used on the bulk upload data summary page.
            datapoint_json = [
                DatapointInterface.to_json_response(
                    datapoint=d,
                    is_published=False,
                    frequency=schema.ReportingFrequency[d.report.type],
                )
                for d in metric_key_to_datapoints.get(metric_definition.key) or []
            ]
            # For each sheet (i.e arrests_by_type) in an excel workbook that raised
            # an exception, jsonify the exception information so that it can be rendered
            # for the user on the bulk upload error page.
            sheet_json = [
                {
                    "display_name": filename_to_metricfile[sheet_name].display_name,
                    "sheet_name": sheet_name,
                    "messages": [e.to_json()],
                }
                for sheet_name, e in sheet_to_error.items()
                if isinstance(e, JusticeCountsBulkUploadException)
                and sheet_name in filename_to_metricfile
                and filename_to_metricfile[sheet_name].definition.key
                == metric_definition.key
            ]
            metrics.append(
                {
                    "key": metric_definition.key,
                    "display_name": metric_definition.display_name,
                    "sheets": sheet_json,
                    "datapoints": datapoint_json,
                }
            )
        # Errors that are not associated with a metric are pre-ingest errors.
        # For example, a pre-ingest exception would be raised if a user uploads an
        # excel workbook that contains a sheet that is not associated with a MetricFile.
        # This is an ingest-blocking error because in this scenario we are not able
        # to convert the rows into datapoints.
        pre_ingest_errors = [
            e.to_json()
            for sheet_name, e in sheet_to_error.items()
            if isinstance(e, JusticeCountsBulkUploadException)
            and sheet_name not in filename_to_metricfile
        ]
        return {"metrics": metrics, "pre_ingest_errors": pre_ingest_errors}
