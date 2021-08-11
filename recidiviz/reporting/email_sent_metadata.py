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
"""Utilities that get and set the custom metadata in GCS for report emails"""
import datetime
import json
from typing import Any, Dict, List, Optional

import attr

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.common.constants.states import StateCode
from recidiviz.reporting.email_reporting_utils import gcsfs_path_for_batch_metadata


@attr.s
class EmailSentResult:
    """Class representing the custom metadata that is being pulled from GCS metadata.json file """

    sent_date: datetime.datetime = attr.ib()
    total_delivered: int = attr.ib()
    redirect_address: Optional[str] = attr.ib()

    def to_json(self) -> Dict[str, Any]:
        return {
            "sentDate": self.sent_date.isoformat(),
            "totalDelivered": self.total_delivered,
            "redirectAddress": self.redirect_address,
        }

    @classmethod
    def from_json(cls, result: Dict[str, Any]) -> "EmailSentResult":
        return EmailSentResult(
            sent_date=datetime.datetime.strptime(
                result["sentDate"], "%Y-%m-%dT%H:%M:%S.%f"
            ),
            total_delivered=result["totalDelivered"],
            redirect_address=result["redirectAddress"],
        )

    @classmethod
    def from_send_result(
        cls,
        sent_date: datetime.datetime,
        total_delivered: int,
        redirect_address: Optional[str],
    ) -> "EmailSentResult":
        return EmailSentResult(
            sent_date=sent_date,
            total_delivered=total_delivered,
            redirect_address=redirect_address,
        )


@attr.s
class EmailSentMetadata:
    """Class that contains a collection of all the EmailSentResult objects for each time a batch of emails are sent"""

    batch_id: str = attr.ib()
    send_results: List[EmailSentResult] = attr.ib()

    def to_json(self) -> Dict[str, Any]:
        return {
            "batchId": self.batch_id,
            "sendResults": [result.to_json() for result in self.send_results],
        }

    @classmethod
    def from_json(cls, email_metadata: Dict[str, str]) -> "EmailSentMetadata":
        batch_id = email_metadata["batchId"]
        previous_send_results = json.loads(email_metadata["sendResults"])
        converted_results = [
            EmailSentResult.from_json(result) for result in previous_send_results
        ]
        return EmailSentMetadata(batch_id=batch_id, send_results=converted_results)

    def add_new_email_send_result(
        self,
        sent_date: datetime.datetime,
        total_delivered: int,
        redirect_address: Optional[str],
    ) -> None:
        self.send_results.append(
            EmailSentResult.from_send_result(
                sent_date=sent_date,
                total_delivered=total_delivered,
                redirect_address=redirect_address,
            )
        )

    @classmethod
    def build_from_gcs(
        cls, state_code: StateCode, batch_id: str, gcs_fs: GCSFileSystem
    ) -> "EmailSentMetadata":
        gcs_path = gcsfs_path_for_batch_metadata(batch_id, state_code)
        email_metadata = gcs_fs.get_metadata(gcs_path)
        send_results: List[EmailSentResult] = []
        if email_metadata and "sendResults" in email_metadata:
            return cls.from_json(email_metadata)
        return EmailSentMetadata(batch_id=batch_id, send_results=send_results)

    def write_to_gcs(
        self, state_code: StateCode, batch_id: str, gcs_fs: GCSFileSystem
    ) -> None:
        payload = self.to_json()
        gcs_path = gcsfs_path_for_batch_metadata(batch_id, state_code)
        gcs_fs.clear_metadata(gcs_path)
        gcs_fs.update_metadata(gcs_path, payload)
