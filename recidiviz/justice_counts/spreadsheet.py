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
import os
from typing import Any, Dict

from sqlalchemy.orm import Session
from werkzeug.datastructures import FileStorage

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.io.flask_file_storage_contents_handle import (
    FlaskFileStorageContentsHandle,
)
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.utils import metadata


class SpreadsheetInterface:
    """Contains methods for working with Spreadsheets.
    The Spreadsheet data model contains metadata about spreadsheets that have been uploaded
    by users for ingest.
    """

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
        uploaded_at = datetime.datetime.utcnow()
        standardized_name = f"{str(agency_id)}:{system}:{uploaded_at.timestamp()}.xlsx"
        bucket_name = f"{metadata.project_id()}-justice-counts-control-panel-ingest"
        fs.upload_from_contents_handle_stream(
            path=GcsfsFilePath(bucket_name=bucket_name, blob_name=standardized_name),
            contents_handle=FlaskFileStorageContentsHandle(file_storage=file_storage),
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
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
            "uploaded_at": spreadsheet.uploaded_at.replace(
                tzinfo=datetime.timezone.utc
            ).timestamp(),
            "uploaded_by": uploaded_by_user.name,
            "ingested_at": spreadsheet.ingested_at.replace(
                tzinfo=datetime.timezone.utc
            ).timestamp()
            if spreadsheet.ingested_at is not None
            else None,
            "status": spreadsheet.status.value,
            "system": spreadsheet.system.value,
        }
