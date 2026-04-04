# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""US_ND early termination writeback implementation."""
import json
import logging
from datetime import datetime, timezone
from typing import Any

import attr
import requests

from recidiviz.case_triage.workflows.api_schemas import (
    WorkflowsUsNdUpdateDocstarsEarlyTerminationDateSchema,
)
from recidiviz.case_triage.workflows.constants import ExternalSystemRequestStatus
from recidiviz.case_triage.workflows.writeback.base import (
    WritebackConfig,
    WritebackExecutorInterface,
    WritebackStatusTracker,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.firestore.firestore_client import FirestoreClientImpl
from recidiviz.utils.environment import in_gcp_production
from recidiviz.utils.secrets import get_secret


@attr.s(frozen=True)
class UsNdEarlyTerminationRequestData:
    user_email: str = attr.ib()
    early_termination_date: str = attr.ib()
    justification_reasons: list[dict[str, str]] = attr.ib()


class UsNdEarlyTerminationStatusTracker(WritebackStatusTracker):
    def __init__(
        self, person_external_id: int, firestore_client: FirestoreClientImpl
    ) -> None:
        self.person_external_id = person_external_id
        self.firestore_client = firestore_client

    def set_status(self, status: ExternalSystemRequestStatus) -> None:
        record_id = f"{StateCode.US_ND.value.lower()}_{self.person_external_id}"
        self.firestore_client.update_document(
            f"clientUpdatesV2/{record_id}/clientOpportunityUpdates/earlyTermination",
            {
                "omsSnooze.status": status.value,
                f"omsSnooze.{self.firestore_client.timestamp_key}": datetime.now(
                    timezone.utc
                ),
            },
        )


class UsNdEarlyTerminationWritebackExecutor(
    WritebackExecutorInterface[UsNdEarlyTerminationRequestData]
):
    """Writeback implementation for ND early termination."""

    def __init__(self, person_external_id: int) -> None:
        self.person_external_id = person_external_id

    @classmethod
    def parse_request_data(
        cls, raw_request: dict[str, Any]
    ) -> UsNdEarlyTerminationRequestData:
        return UsNdEarlyTerminationRequestData(
            user_email=raw_request["user_email"],
            early_termination_date=raw_request["early_termination_date"],
            justification_reasons=raw_request["justification_reasons"],
        )

    def execute(self, request_data: UsNdEarlyTerminationRequestData) -> None:
        docstars_url = get_secret("workflows_us_nd_early_termination_url")
        docstars_key = get_secret("workflows_us_nd_early_termination_key")

        # TODO(#68802): Centralize logic for detecting a Recidiviz user in writeback code.
        if in_gcp_production() and request_data.user_email.endswith("@recidiviz.org"):
            docstars_url = get_secret("workflows_us_nd_early_termination_test_url")

        if docstars_url is None or docstars_key is None:
            logging.error("Unable to get secrets for DOCSTARS")
            raise EnvironmentError("Unable to get secrets for DOCSTARS")

        headers = {
            "Recidiviz-Credential-Token": docstars_key,
            "Content-Type": "application/json",
        }

        request_body = json.dumps(
            {
                "sid": self.person_external_id,
                "userEmail": request_data.user_email,
                "earlyTerminationDate": request_data.early_termination_date,
                "justificationReasons": request_data.justification_reasons,
            }
        )

        try:
            docstars_response = requests.put(
                docstars_url,
                headers=headers,
                data=request_body,
                timeout=360,
            )
            if docstars_response.status_code != requests.codes.ok:
                docstars_response.raise_for_status()
        except requests.exceptions.HTTPError:
            logging.error(
                "Request to DOCSTARS failed with code %s: %s",
                docstars_response.status_code,
                docstars_response.text,
            )
            raise
        except Exception as e:
            logging.error("Request to DOCSTARS failed with error: %s", e)
            raise

        logging.info(
            "Request to DOCSTARS completed with status code %s",
            docstars_response.status_code,
        )

    def create_status_tracker(self) -> UsNdEarlyTerminationStatusTracker:
        return UsNdEarlyTerminationStatusTracker(
            self.person_external_id, FirestoreClientImpl()
        )

    @classmethod
    def config(cls) -> WritebackConfig:
        return WritebackConfig(
            state_code=StateCode.US_ND,
            operation_action_description="Updating early termination date in DOCSTARS",
            api_schema_cls=WorkflowsUsNdUpdateDocstarsEarlyTerminationDateSchema,
        )
