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

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.reporting.context.po_monthly_report.constants import ReportType
from recidiviz.reporting.email_reporting_utils import gcsfs_path_for_batch_metadata
from recidiviz.reporting.email_sent_metadata import EmailSentMetadata
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_STAGING, in_gcp
from recidiviz.utils.metadata import local_project_id_override


class EmailMetadataReportDateError(ValueError):
    pass


class InvalidReportTypeError(ValueError):
    pass


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
        batch_id: str,
        state_code: StateCode,
    ) -> Dict[str, str]:
        return json.loads(
            self.monthly_reports_gcsfs.download_as_string(
                path=gcsfs_path_for_batch_metadata(batch_id, state_code)
            )
        )

    def get_report_type(self, batch_id: str, state_code: StateCode) -> ReportType:
        """Get the report type of generated emails.
        Args:
            batch_id: string of the batch id of the generated emails
            state_code: state code of the generated emails
        Returns:
            ReportType that is in the ReportType enum class in /po_monthly_report/constants.py
        """
        email_metadata = self.read_batch_metadata(
            batch_id=batch_id, state_code=state_code
        )
        report_type = ReportType(email_metadata.get("report_type"))
        if report_type not in ReportType:
            raise InvalidReportTypeError(
                f"Invalid report type: Sending emails with {report_type} is not a allowed. Report type does not exist."
            )
        return report_type

    def generate_report_date(
        self, batch_id: str, state_code: StateCode
    ) -> datetime.date:
        """Generate a report date from the json that is created when emails are generated.

        Args:
            batch_id: string of the batch id of the generated emails
            state_code: state code of the generated emails

        Returns:
            Date type that contains the year, month, and day.
        """
        email_metadata = self.read_batch_metadata(
            batch_id=batch_id, state_code=state_code
        )

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

    def get_batch_info(self, state_code: StateCode) -> List[Dict[str, Any]]:
        """Returns a sorted list of batch id numbers from the a specific state bucket from GCS"""
        buckets = self.monthly_reports_gcsfs.ls_with_blob_prefix(
            bucket_name=f"{self.project_id}-report-html",
            blob_prefix=state_code.value,
        )
        files = [file for file in buckets if isinstance(file, GcsfsFilePath)]
        batch_ids = list({batch_id.blob_name.split("/")[1] for batch_id in files})
        batch_ids.sort(reverse=True)
        return self._get_email_batch_info(state_code=state_code, batch_ids=batch_ids)

    def _get_email_batch_info(
        self, state_code: StateCode, batch_ids: List[str]
    ) -> List[Dict[str, Any]]:
        batch_info = []
        for batch in batch_ids:
            email_sent_metadata = EmailSentMetadata.build_from_gcs(
                state_code=state_code,
                batch_id=batch,
                gcs_fs=self.monthly_reports_gcsfs,
            )
            batch_info.append(email_sent_metadata.to_json())
        return batch_info
