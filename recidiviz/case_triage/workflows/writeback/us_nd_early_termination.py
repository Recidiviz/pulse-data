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
from datetime import date, datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

from recidiviz.case_triage.workflows.constants import ExternalSystemRequestStatus
from recidiviz.case_triage.workflows.writeback.base import (
    WritebackExecutorInterface,
    WritebackRequestData,
    WritebackStatusTracker,
)
from recidiviz.case_triage.workflows.writeback.transports.rest import (
    RestTransport,
    RestTransportConfig,
    TokenHeaderAuth,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.firestore.firestore_client import FirestoreClientImpl
from recidiviz.utils.environment import in_gcp_production

DOCSTARS_TRANSPORT_CONFIG = RestTransportConfig(
    system_name="DOCSTARS",
    url_secret="workflows_us_nd_early_termination_url",  # nosec
    credential_secret="workflows_us_nd_early_termination_key",
    test_url_secret="workflows_us_nd_early_termination_test_url",
    auth_strategy=TokenHeaderAuth("Recidiviz-Credential-Token"),
)


class JustificationReason(BaseModel):
    code: str
    description: str


class UsNdEarlyTerminationRequestData(WritebackRequestData):
    person_external_id: int
    user_email: str
    early_termination_date: date
    justification_reasons: list[JustificationReason]


class DocstarsEarlyTerminationRequest(BaseModel):
    """Models the request body sent to DOCSTARS for early termination updates."""

    model_config = ConfigDict(
        alias_generator=to_camel, populate_by_name=True, frozen=True
    )

    sid: int
    user_email: str
    early_termination_date: date
    justification_reasons: list[JustificationReason]


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

    def __init__(self, request: UsNdEarlyTerminationRequestData) -> None:
        self.request = request

    def to_cloud_task_payload(self) -> dict[str, Any]:
        return self.request.model_copy(update={"should_queue_task": False}).model_dump(
            mode="json"
        )

    def execute(self) -> None:
        transport = RestTransport(
            DOCSTARS_TRANSPORT_CONFIG,
            # TODO(#68802): Centralize logic for detecting a Recidiviz user in writeback code.
            use_test_url=in_gcp_production()
            and self.request.user_email.endswith("@recidiviz.org"),
        )

        docstars_request = DocstarsEarlyTerminationRequest(
            sid=self.request.person_external_id,
            user_email=self.request.user_email,
            early_termination_date=self.request.early_termination_date,
            justification_reasons=self.request.justification_reasons,
        )
        transport.send(body=docstars_request.model_dump_json(by_alias=True))

    def create_status_tracker(self) -> UsNdEarlyTerminationStatusTracker:
        return UsNdEarlyTerminationStatusTracker(
            self.request.person_external_id, FirestoreClientImpl()
        )

    @property
    def operation_action_description(self) -> str:
        return "Updating early termination date in DOCSTARS"
