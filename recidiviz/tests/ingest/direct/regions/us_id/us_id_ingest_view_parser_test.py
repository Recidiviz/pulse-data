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
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentDischargeReason,
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
    StateSupervisionContactMethod,
    StateSupervisionContactReason,
    StateSupervisionContactStatus,
    StateSupervisionContactType,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecidingBodyType,
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseType,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.entity.state.entities import (
    StateAgent,
    StateIncarcerationPeriod,
    StatePerson,
    StatePersonExternalId,
    StateProgramAssignment,
    StateSupervisionContact,
    StateSupervisionPeriod,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
    StateSupervisionViolationResponseDecisionEntry,
    StateSupervisionViolationTypeEntry,
)
from recidiviz.tests.ingest.direct.regions.state_ingest_view_parser_test_base import (
    StateIngestViewParserTestBase,
)


class UsIdIngestViewParserTest(StateIngestViewParserTestBase, unittest.TestCase):
    """Parser unit tests for each US_ID ingest view query results to be ingested."""

    @classmethod
    def schema_type(cls) -> SchemaType:
        return SchemaType.STATE

    @classmethod
    def region_code(cls) -> str:
        return "US_ID"

    @property
    def test(self) -> unittest.TestCase:
        return self

    def test_parse_agnt_case_updt(self) -> None:
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
                        external_id="1111-150-FUZZY_MATCHED",
                        referring_agent=StateAgent(
                            state_code="US_ID",
                            external_id="PO1",
                            agent_type=StateAgentType.SUPERVISION_OFFICER,
                        ),
                        participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
                        participation_status_raw_text="TREATMENT",
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
                        external_id="1111-162-FUZZY_MATCHED",
                        referring_agent=StateAgent(
                            state_code="US_ID",
                            external_id="PO1",
                            agent_type=StateAgentType.SUPERVISION_OFFICER,
                        ),
                        participation_status=StateProgramAssignmentParticipationStatus.DISCHARGED,
                        participation_status_raw_text="TREATMENT COMPLETION",
                        discharge_reason=StateProgramAssignmentDischargeReason.COMPLETED,
                        discharge_date=datetime.date(2020, 4, 1),
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
                supervision_violations=[
                    StateSupervisionViolation(
                        state_code="US_ID",
                        external_id="1111-168-FUZZY_MATCHED",
                        violation_date=datetime.date(2020, 5, 1),
                        supervision_violation_responses=[
                            StateSupervisionViolationResponse(
                                external_id="1111-168-FUZZY_MATCHED",
                                state_code="US_ID",
                                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                                response_date=datetime.date(2020, 5, 1),
                                deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
                                decision_agents=[
                                    StateAgent(
                                        state_code="US_ID",
                                        external_id="PO1",
                                        agent_type=StateAgentType.SUPERVISION_OFFICER,
                                    )
                                ],
                            )
                        ],
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
                        external_id="3333-170-FUZZY_MATCHED",
                        referring_agent=StateAgent(
                            state_code="US_ID",
                            external_id="PO3",
                            agent_type=StateAgentType.SUPERVISION_OFFICER,
                        ),
                        participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
                        participation_status_raw_text="TX",
                        start_date=datetime.date(2020, 6, 1),
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
            ),
            StatePerson(
                state_code="US_ID",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ID", external_id="3333", id_type=US_ID_DOC
                    )
                ],
                supervision_violations=[
                    StateSupervisionViolation(
                        state_code="US_ID",
                        external_id="3333-173-FUZZY_MATCHED",
                        violation_date=datetime.date(2020, 9, 1),
                        supervision_violation_responses=[
                            StateSupervisionViolationResponse(
                                state_code="US_ID",
                                external_id="3333-173-FUZZY_MATCHED",
                                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                                response_date=datetime.date(2020, 9, 1),
                                deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
                                decision_agents=[
                                    StateAgent(
                                        state_code="US_ID",
                                        external_id="PO3",
                                        agent_type=StateAgentType.SUPERVISION_OFFICER,
                                    )
                                ],
                            )
                        ],
                        supervision_violation_types=[
                            StateSupervisionViolationTypeEntry(
                                state_code="US_ID",
                                violation_type=StateSupervisionViolationType.ABSCONDED,
                            )
                        ],
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
                        external_id="4444-175-FUZZY_MATCHED",
                        referring_agent=StateAgent(
                            state_code="US_ID",
                            external_id="PO4",
                            agent_type=StateAgentType.SUPERVISION_OFFICER,
                        ),
                        participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
                        participation_status_raw_text="SO TREATMENT",
                        start_date=datetime.date(2021, 11, 17),
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
                supervision_violations=[
                    StateSupervisionViolation(
                        state_code="US_ID",
                        external_id="4444-177-FUZZY_MATCHED",
                        violation_date=datetime.date(2021, 12, 1),
                        supervision_violation_responses=[
                            StateSupervisionViolationResponse(
                                state_code="US_ID",
                                external_id="4444-177-FUZZY_MATCHED",
                                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                                response_date=datetime.date(2021, 12, 1),
                                deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
                                decision_agents=[
                                    StateAgent(
                                        state_code="US_ID",
                                        external_id="PO4",
                                        agent_type=StateAgentType.SUPERVISION_OFFICER,
                                    )
                                ],
                                supervision_violation_response_decisions=[
                                    StateSupervisionViolationResponseDecisionEntry(
                                        state_code="US_ID",
                                        decision=StateSupervisionViolationResponseDecision.REVOCATION,
                                        decision_raw_text="REVOKE",
                                    )
                                ],
                            )
                        ],
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
            ),
            StatePerson(
                state_code="US_ID",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ID", external_id="3333", id_type=US_ID_DOC
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
            ),
            StatePerson(
                state_code="US_ID",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ID", external_id="4444", id_type=US_ID_DOC
                    )
                ],
                supervision_violations=[
                    StateSupervisionViolation(
                        state_code="US_ID",
                        external_id="4444-193-FUZZY_MATCHED",
                        violation_date=datetime.date(2022, 1, 4),
                        supervision_violation_responses=[
                            StateSupervisionViolationResponse(
                                state_code="US_ID",
                                external_id="4444-193-FUZZY_MATCHED",
                                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                                response_date=datetime.date(2022, 1, 4),
                                deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
                                decision_agents=[
                                    StateAgent(
                                        state_code="US_ID",
                                        external_id="PO4",
                                        agent_type=StateAgentType.SUPERVISION_OFFICER,
                                    )
                                ],
                                supervision_violation_response_decisions=[
                                    StateSupervisionViolationResponseDecisionEntry(
                                        state_code="US_ID",
                                        decision=StateSupervisionViolationResponseDecision.WARNING,
                                        decision_raw_text="AW",
                                    )
                                ],
                            )
                        ],
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
                supervision_periods=[
                    StateSupervisionPeriod(
                        state_code="US_ID",
                        external_id="4444-194-FUZZY_MATCHED",
                        supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
                        admission_reason=StateSupervisionPeriodAdmissionReason.INVESTIGATION,
                        start_date=datetime.date(2022, 1, 7),
                        termination_date=datetime.date(2022, 1, 7),
                        termination_reason=StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN,
                        supervising_officer=StateAgent(
                            state_code="US_ID",
                            external_id="PO4",
                            agent_type=StateAgentType.SUPERVISION_OFFICER,
                        ),
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
                incarceration_periods=[
                    StateIncarcerationPeriod(
                        state_code="US_ID",
                        external_id="4444-195-FUZZY_MATCHED",
                        admission_date=datetime.date(2022, 1, 9),
                        admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
                        release_date=datetime.date(2022, 1, 9),
                        release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
                        incarceration_type=StateIncarcerationType.STATE_PRISON,
                    )
                ],
            ),
        ]
        self._run_parse_ingest_view_test("agnt_case_updt", expected_output)

    def test_parse_sprvsn_cntc_v3(self) -> None:
        expected_output = [
            StatePerson(
                state_code="US_ID",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ID", external_id="1111", id_type=US_ID_DOC
                    )
                ],
                supervision_contacts=[
                    StateSupervisionContact(
                        state_code="US_ID",
                        external_id="1",
                        verified_employment=True,
                        resulted_in_arrest=False,
                        status=StateSupervisionContactStatus.COMPLETED,
                        status_raw_text="SUCCESSFUL",
                        location_raw_text="TELEPHONE",
                        contact_type=StateSupervisionContactType.DIRECT,
                        contact_type_raw_text="FACE TO FACE",
                        contact_method=StateSupervisionContactMethod.TELEPHONE,
                        contact_method_raw_text="TELEPHONE##FACE TO FACE",
                        contact_reason=StateSupervisionContactReason.GENERAL_CONTACT,
                        contact_reason_raw_text="GENERAL",
                        contact_date=datetime.date(2018, 2, 1),
                        contacted_agent=StateAgent(
                            state_code="US_ID",
                            external_id="PO1",
                            full_name='{"full_name": "NAME1"}',
                            agent_type=StateAgentType.SUPERVISION_OFFICER,
                        ),
                    )
                ],
            ),
            StatePerson(
                state_code="US_ID",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ID", external_id="1111", id_type=US_ID_DOC
                    )
                ],
                supervision_contacts=[
                    StateSupervisionContact(
                        state_code="US_ID",
                        external_id="2",
                        verified_employment=False,
                        resulted_in_arrest=True,
                        status=StateSupervisionContactStatus.COMPLETED,
                        status_raw_text="ARREST",
                        location=StateSupervisionContactLocation.RESIDENCE,
                        location_raw_text="RESIDENCE",
                        contact_type=StateSupervisionContactType.DIRECT,
                        contact_type_raw_text="FACE TO FACE",
                        contact_method=StateSupervisionContactMethod.IN_PERSON,
                        contact_method_raw_text="RESIDENCE##FACE TO FACE",
                        contact_reason=StateSupervisionContactReason.EMERGENCY_CONTACT,
                        contact_reason_raw_text="CRITICAL",
                        contact_date=datetime.date(2020, 2, 1),
                        contacted_agent=StateAgent(
                            state_code="US_ID",
                            external_id="PO1",
                            full_name='{"full_name": "NAME1"}',
                            agent_type=StateAgentType.SUPERVISION_OFFICER,
                        ),
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
                supervision_contacts=[
                    StateSupervisionContact(
                        state_code="US_ID",
                        external_id="3",
                        verified_employment=True,
                        resulted_in_arrest=False,
                        status=StateSupervisionContactStatus.ATTEMPTED,
                        status_raw_text="ATTEMPTED",
                        location=StateSupervisionContactLocation.ALTERNATIVE_WORK_SITE,
                        location_raw_text="ALTERNATE WORK SITE",
                        contact_type=StateSupervisionContactType.DIRECT,
                        contact_type_raw_text="FACE TO FACE",
                        contact_method=StateSupervisionContactMethod.IN_PERSON,
                        contact_method_raw_text="ALTERNATE WORK SITE##FACE TO FACE",
                        contact_reason=StateSupervisionContactReason.INITIAL_CONTACT,
                        contact_reason_raw_text="72 HOUR INITIAL",
                        contact_date=datetime.date(2016, 1, 1),
                        contacted_agent=StateAgent(
                            state_code="US_ID",
                            external_id="PO3",
                            full_name='{"full_name": "NAME3"}',
                            agent_type=StateAgentType.SUPERVISION_OFFICER,
                        ),
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
                supervision_contacts=[
                    StateSupervisionContact(
                        state_code="US_ID",
                        external_id="4",
                        verified_employment=True,
                        resulted_in_arrest=False,
                        status=StateSupervisionContactStatus.ATTEMPTED,
                        status_raw_text="ATTEMPTED",
                        location=StateSupervisionContactLocation.RESIDENCE,
                        location_raw_text="RESIDENCE",
                        contact_type=StateSupervisionContactType.DIRECT,
                        contact_type_raw_text="VIRTUAL",
                        contact_method=StateSupervisionContactMethod.VIRTUAL,
                        contact_method_raw_text="RESIDENCE##VIRTUAL",
                        contact_reason=StateSupervisionContactReason.INITIAL_CONTACT,
                        contact_reason_raw_text="72 HOUR INITIAL",
                        contact_date=datetime.date(2017, 1, 1),
                        contacted_agent=StateAgent(
                            state_code="US_ID",
                            external_id="PO4",
                            full_name='{"full_name": "NAME4"}',
                            agent_type=StateAgentType.SUPERVISION_OFFICER,
                        ),
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
                supervision_contacts=[
                    StateSupervisionContact(
                        state_code="US_ID",
                        external_id="5",
                        verified_employment=True,
                        resulted_in_arrest=False,
                        status=StateSupervisionContactStatus.COMPLETED,
                        status_raw_text="SUCCESSFUL",
                        location=StateSupervisionContactLocation.PLACE_OF_EMPLOYMENT,
                        location_raw_text="EMPLOYER",
                        contact_type=StateSupervisionContactType.COLLATERAL,
                        contact_type_raw_text="COLLATERAL",
                        contact_method=StateSupervisionContactMethod.IN_PERSON,
                        contact_method_raw_text="EMPLOYER##COLLATERAL",
                        contact_reason=StateSupervisionContactReason.GENERAL_CONTACT,
                        contact_reason_raw_text="GENERAL",
                        contact_date=datetime.date(2016, 1, 8),
                        contacted_agent=StateAgent(
                            state_code="US_ID",
                            external_id="PO3",
                            full_name='{"full_name": "NAME3"}',
                            agent_type=StateAgentType.SUPERVISION_OFFICER,
                        ),
                    )
                ],
            ),
        ]

        self._run_parse_ingest_view_test("sprvsn_cntc_v3", expected_output)

    def test_parse_sprvsn_cntc_v3_SupervisionContactMethod(self) -> None:
        manifest_ast = self._parse_manifest("sprvsn_cntc_v3")
        enum_parser_manifest = (
            manifest_ast.field_manifests["supervision_contacts"]
            .child_manifests[0]  # type: ignore[attr-defined]
            .field_manifests["contact_method"]
        )
        self._parse_enum_manifest_test(
            "supervision_contact_method", enum_parser_manifest
        )

    def test_parse_sprvsn_cntc_v3_SupervisionContactLocation(self) -> None:
        manifest_ast = self._parse_manifest("sprvsn_cntc_v3")
        enum_parser_manifest = (
            manifest_ast.field_manifests["supervision_contacts"]
            .child_manifests[0]  # type: ignore[attr-defined]
            .field_manifests["location"]
        )
        self._parse_enum_manifest_test(
            "supervision_contact_location", enum_parser_manifest
        )

    def test_parse_sprvsn_cntc_v3_SupervisionContactType(self) -> None:
        manifest_ast = self._parse_manifest("sprvsn_cntc_v3")
        enum_parser_manifest = (
            manifest_ast.field_manifests["supervision_contacts"]
            .child_manifests[0]  # type: ignore[attr-defined]
            .field_manifests["contact_type"]
        )
        self._parse_enum_manifest_test("supervision_contact_type", enum_parser_manifest)
