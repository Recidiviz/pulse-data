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
"""Ingest view parser tests for US_ID direct ingest."""
import datetime
import unittest

from recidiviz.common.constants.state.external_id_types import US_ID_DOC
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentDischargeReason,
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.entity.state.entities import (
    StateAgent,
    StatePerson,
    StatePersonExternalId,
    StateProgramAssignment,
)
from recidiviz.tests.ingest.direct.regions.state_ingest_view_parser_test_base import (
    StateIngestViewParserTestBase,
)


class UsIdIngestViewParserTest(StateIngestViewParserTestBase, unittest.TestCase):
    """Parser unit tests for each US_ID ingest view file to be ingested."""

    @classmethod
    def schema_type(cls) -> SchemaType:
        return SchemaType.STATE

    @classmethod
    def region_code(cls) -> str:
        return "US_ID"

    @property
    def test(self) -> unittest.TestCase:
        return self

    def test_parse_treatment_agnt_case_updt(self) -> None:
        expected_output = [
            StatePerson(
                state_code="US_ID",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ID", external_id="1111", id_type=US_ID_DOC
                    )
                ],
                program_assignments=[
                    StateProgramAssignment(
                        state_code="US_ID",
                        external_id="1111-150",
                        referring_agent=StateAgent(
                            state_code="US_ID",
                            external_id="PO1",
                            agent_type=StateAgentType.SUPERVISION_OFFICER,
                        ),
                        participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
                        participation_status_raw_text="TREATMENT",
                        discharge_reason=StateProgramAssignmentDischargeReason.EXTERNAL_UNKNOWN,
                        discharge_reason_raw_text="TREATMENT",
                        referral_date=datetime.date(2020, 3, 1),
                        start_date=datetime.date(2020, 3, 1),
                    ),
                ],
            ),
            StatePerson(
                state_code="US_ID",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ID", external_id="1111", id_type=US_ID_DOC
                    )
                ],
                program_assignments=[
                    StateProgramAssignment(
                        state_code="US_ID",
                        external_id="1111-162",
                        referring_agent=StateAgent(
                            state_code="US_ID",
                            external_id="PO1",
                            agent_type=StateAgentType.SUPERVISION_OFFICER,
                        ),
                        participation_status=StateProgramAssignmentParticipationStatus.DISCHARGED,
                        participation_status_raw_text="TREATMENT COMPLETION",
                        discharge_reason=StateProgramAssignmentDischargeReason.COMPLETED,
                        discharge_reason_raw_text="TREATMENT COMPLETION",
                        discharge_date=datetime.date(2020, 4, 1),
                    ),
                ],
            ),
            StatePerson(
                state_code="US_ID",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ID", external_id="3333", id_type=US_ID_DOC
                    )
                ],
                program_assignments=[
                    StateProgramAssignment(
                        state_code="US_ID",
                        external_id="3333-170",
                        referring_agent=StateAgent(
                            state_code="US_ID",
                            external_id="PO3",
                            agent_type=StateAgentType.SUPERVISION_OFFICER,
                        ),
                        participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
                        participation_status_raw_text="TX",
                        discharge_reason=StateProgramAssignmentDischargeReason.EXTERNAL_UNKNOWN,
                        discharge_reason_raw_text="TX",
                        referral_date=datetime.date(2020, 6, 1),
                        start_date=datetime.date(2020, 6, 1),
                    )
                ],
            ),
            StatePerson(
                state_code="US_ID",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ID", external_id="4444", id_type=US_ID_DOC
                    )
                ],
                program_assignments=[
                    StateProgramAssignment(
                        state_code="US_ID",
                        external_id="4444-175",
                        referring_agent=StateAgent(
                            state_code="US_ID",
                            external_id="PO4",
                            agent_type=StateAgentType.SUPERVISION_OFFICER,
                        ),
                        participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
                        participation_status_raw_text="SO TREATMENT",
                        discharge_reason=StateProgramAssignmentDischargeReason.EXTERNAL_UNKNOWN,
                        discharge_reason_raw_text="SO TREATMENT",
                        referral_date=datetime.date(2021, 11, 17),
                        start_date=datetime.date(2021, 11, 17),
                    )
                ],
            ),
            StatePerson(
                state_code="US_ID",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ID", external_id="3333", id_type=US_ID_DOC
                    )
                ],
                program_assignments=[
                    StateProgramAssignment(
                        state_code="US_ID",
                        external_id="3333-180",
                        referring_agent=StateAgent(
                            state_code="US_ID",
                            external_id="PO3",
                            agent_type=StateAgentType.SUPERVISION_OFFICER,
                        ),
                        participation_status=StateProgramAssignmentParticipationStatus.EXTERNAL_UNKNOWN,
                        participation_status_raw_text="COMPLETE OTHER",
                        discharge_reason=StateProgramAssignmentDischargeReason.EXTERNAL_UNKNOWN,
                        discharge_reason_raw_text="COMPLETE OTHER",
                    )
                ],
            ),
        ]
        self._run_parse_ingest_view_test("treatment_agnt_case_updt", expected_output)
