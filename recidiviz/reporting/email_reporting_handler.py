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
"""email reporting handler gets information for email reporting from gcs"""
import calendar
import datetime
import json
from typing import Any, Dict, List

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.reporting.context.po_monthly_report.constants import ReportType
from recidiviz.reporting.email_reporting_utils import (
    Batch,
    gcsfs_path_for_batch_metadata,
)
from recidiviz.reporting.email_sent_metadata import EmailSentMetadata
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_STAGING, in_gcp
from recidiviz.utils.metadata import local_project_id_override


class EmailMetadataReportDateError(ValueError):
    pass


class InvalidReportTypeError(ValueError):
    pass


def _metadata_matches_report_type(
    report_type: ReportType, path_to_json: GcsfsFilePath, gcs_fs: GCSFileSystem
) -> bool:
    metadata_contents = json.loads(gcs_fs.download_as_string(path_to_json))
    return metadata_contents.get("report_type") == report_type.value


class EmailReportingHandler:
    """
    Maintain email reporting utilities. Specifically responsible for building
    GCSFS only once for PO Monthly Reports Admin panel tab. Reason for only building
    once is for performance in the admin panel
    """

    def __init__(self) -> None:
        if in_gcp():
            self.project_id = metadata.project_id()
            self.monthly_reports_gcsfs = GcsfsFactory.build()
        else:
            with local_project_id_override(GCP_PROJECT_STAGING):
                self.project_id = GCP_PROJECT_STAGING
                self.monthly_reports_gcsfs = GcsfsFactory.build()

    def read_batch_metadata(
        self,
        *,
        batch: Batch,
    ) -> Dict[str, str]:
        return json.loads(
            self.monthly_reports_gcsfs.download_as_string(
                path=gcsfs_path_for_batch_metadata(batch)
            )
        )

    def get_report_type(self, batch: Batch) -> ReportType:
        """Get the report type of generated emails.
        Args:
            batch: the batch identifier
        Returns:
            ReportType that is in the ReportType enum class in /po_monthly_report/constants.py
        """
        email_metadata = self.read_batch_metadata(batch=batch)
        report_type = ReportType(email_metadata.get("report_type"))
        if report_type not in ReportType:
            raise InvalidReportTypeError(
                f"Invalid report type: Sending emails with {report_type} is not a allowed. Report type does not exist."
            )
        return report_type

    def generate_report_date(
        self,
        batch: Batch,
    ) -> datetime.date:
        """Generate a report date from the json that is created when emails are generated.

        Args:
            batch: the batch of emails

        Returns:
            Date type that contains the year, month, and day.
        """
        email_metadata = self.read_batch_metadata(batch=batch)

        if (metadata_year := email_metadata.get("review_year")) is None:
            raise EmailMetadataReportDateError("review_year not found in metadata")
        review_year = int(metadata_year)

        if (metadata_month := email_metadata.get("review_month")) is None:
            raise EmailMetadataReportDateError("review_month not found in metadata")
        review_month = int(metadata_month)

        review_day = calendar.monthrange(review_year, review_month)[1]
        report_date = datetime.date(
            year=review_year, month=review_month, day=review_day
        )
        return report_date

    def get_batch_info(
        self,
        state_code: StateCode,
        report_type: ReportType,
    ) -> List[Dict[str, Any]]:
        """Returns a sorted list of batch id numbers from the a specific state bucket from GCS"""
        buckets = self.monthly_reports_gcsfs.ls_with_blob_prefix(
            bucket_name=f"{self.project_id}-report-html",
            blob_prefix=state_code.value,
        )
        files = [file for file in buckets if isinstance(file, GcsfsFilePath)]

        batch_metadata_files = [
            file for file in files if file.blob_name.endswith("metadata.json")
        ]
        report_batch_metadata_files = [
            file
            for file in batch_metadata_files
            if _metadata_matches_report_type(
                report_type, file, self.monthly_reports_gcsfs
            )
        ]

        batch_ids = [
            file.blob_name.split("/")[1] for file in report_batch_metadata_files
        ]
        batch_ids.sort(reverse=True)
        return self._get_email_batch_info(
            [
                Batch(batch_id=batch_id, state_code=state_code, report_type=report_type)
                for batch_id in batch_ids
            ]
        )

    def _get_email_batch_info(self, batches: List[Batch]) -> List[Dict[str, Any]]:
        batch_info = []
        for batch in batches:
            email_sent_metadata = EmailSentMetadata.build_from_gcs(
                batch=batch,
                gcs_fs=self.monthly_reports_gcsfs,
            )
            batch_info.append(email_sent_metadata.to_json())
        return batch_info
