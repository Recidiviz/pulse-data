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
"""Ingest view parser tests for US_ND direct ingest."""
import datetime
import unittest

from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.state.external_id_types import US_ND_ELITE_BOOKING
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_court_case import (
    StateCourtCaseStatus,
    StateCourtType,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateIncarcerationPeriodStatus,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.state.state_supervision_sentence import (
    StateSupervisionSentenceSupervisionType,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseType,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.entity.state.entities import (
    StateAgent,
    StateCharge,
    StateCourtCase,
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StatePerson,
    StatePersonExternalId,
    StateSentenceGroup,
    StateSupervisionPeriod,
    StateSupervisionSentence,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
    StateSupervisionViolationResponseDecisionEntry,
    StateSupervisionViolationTypeEntry,
)
from recidiviz.tests.ingest.direct.regions.state_ingest_view_parser_test_base import (
    StateIngestViewParserTestBase,
)


class UsNdIngestViewParserTest(StateIngestViewParserTestBase, unittest.TestCase):
    """Parser unit tests for each US_ND ingest view file to be ingested."""

    state_code = "US_ND"

    @classmethod
    def schema_type(cls) -> SchemaType:
        return SchemaType.STATE

    @classmethod
    def region_code(cls) -> str:
        return "US_ND"

    @property
    def test(self) -> unittest.TestCase:
        return self

    def test_parse_docstars_offendercasestable_with_officers(self) -> None:
        expected_output = [
            StatePerson(
                state_code=self.state_code,
                external_ids=[
                    StatePersonExternalId(
                        state_code=self.state_code,
                        external_id="92237",
                        id_type="US_ND_SID",
                    )
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        state_code=self.state_code,
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        supervision_sentences=[
                            StateSupervisionSentence(
                                external_id="117109",
                                state_code=self.state_code,
                                status=StateSentenceStatus.COMPLETED,
                                supervision_type=StateSupervisionSentenceSupervisionType.PRE_CONFINEMENT,
                                supervision_type_raw_text="PRE-TRIAL",
                                start_date=datetime.date(2013, 1, 1),
                                projected_completion_date=datetime.date(2013, 3, 3),
                                completion_date=datetime.date(2013, 2, 2),
                                max_length_days=59,
                                supervision_periods=[
                                    StateSupervisionPeriod(
                                        external_id="117109",
                                        state_code=self.state_code,
                                        supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
                                        supervision_type_raw_text="PRE-TRIAL",
                                        start_date=datetime.date(2013, 1, 1),
                                        termination_date=datetime.date(2013, 2, 2),
                                        county_code="US_ND_CASS",
                                        supervision_site="4",
                                        termination_reason=StateSupervisionPeriodTerminationReason.EXPIRATION,
                                        termination_reason_raw_text="4",
                                        supervising_officer=StateAgent(
                                            external_id="63",
                                            state_code=self.state_code,
                                            agent_type=StateAgentType.SUPERVISION_OFFICER,
                                            full_name='{"given_names": "DAVID", "middle_names": "", "name_suffix": "", "surname": "BORG"}',
                                        ),
                                    )
                                ],
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code=self.state_code,
                external_ids=[
                    StatePersonExternalId(
                        state_code=self.state_code,
                        external_id="92237",
                        id_type="US_ND_SID",
                    )
                ],
                supervision_violations=[],
                sentence_groups=[
                    StateSentenceGroup(
                        state_code=self.state_code,
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        supervision_sentences=[
                            StateSupervisionSentence(
                                external_id="117110",
                                state_code=self.state_code,
                                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                                supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
                                supervision_type_raw_text="PAROLE",
                                start_date=datetime.date(2014, 7, 17),
                                projected_completion_date=datetime.date(2014, 10, 6),
                                max_length_days=92,
                                charges=[
                                    StateCharge(
                                        state_code=self.state_code,
                                        status=ChargeStatus.PRESENT_WITHOUT_INFO,
                                        court_case=StateCourtCase(
                                            state_code=self.state_code,
                                            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
                                            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
                                            judge=StateAgent(
                                                state_code=self.state_code,
                                                agent_type=StateAgentType.JUDGE,
                                                full_name='{"full_name": "THE JUDGE"}',
                                            ),
                                        ),
                                    )
                                ],
                                supervision_periods=[
                                    StateSupervisionPeriod(
                                        external_id="117110",
                                        state_code=self.state_code,
                                        supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                                        supervision_type_raw_text="PAROLE",
                                        start_date=datetime.date(2014, 7, 17),
                                        county_code="US_ND_CASS",
                                        supervision_site="4",
                                        supervision_level=StateSupervisionLevel.EXTERNAL_UNKNOWN,
                                        supervision_level_raw_text="5",
                                        supervising_officer=StateAgent(
                                            external_id="154",
                                            state_code=self.state_code,
                                            agent_type=StateAgentType.SUPERVISION_OFFICER,
                                            full_name='{"given_names": "JOSEPH", "middle_names": "", "name_suffix": "", "surname": "LUND"}',
                                        ),
                                    )
                                ],
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code=self.state_code,
                external_ids=[
                    StatePersonExternalId(
                        state_code=self.state_code,
                        external_id="92237",
                        id_type="US_ND_SID",
                    )
                ],
                supervision_violations=[
                    StateSupervisionViolation(
                        state_code=self.state_code,
                        external_id="117111",
                        supervision_violation_types=[
                            StateSupervisionViolationTypeEntry(
                                state_code=self.state_code,
                                violation_type=StateSupervisionViolationType.ABSCONDED,
                            )
                        ],
                        supervision_violation_responses=[
                            StateSupervisionViolationResponse(
                                state_code=self.state_code,
                                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
                                response_date=datetime.date(2014, 12, 8),
                                supervision_violation_response_decisions=[
                                    StateSupervisionViolationResponseDecisionEntry(
                                        state_code=self.state_code,
                                        decision=StateSupervisionViolationResponseDecision.REVOCATION,
                                        decision_raw_text="DOCR INMATE SENTENCE",
                                    )
                                ],
                                decision_agents=[
                                    StateAgent(
                                        external_id="63",
                                        state_code=self.state_code,
                                        agent_type=StateAgentType.SUPERVISION_OFFICER,
                                        full_name='{"given_names": "DAVID", "middle_names": "", "name_suffix": "", "surname": "BORG"}',
                                    )
                                ],
                            )
                        ],
                    )
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        state_code=self.state_code,
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        supervision_sentences=[
                            StateSupervisionSentence(
                                external_id="117111",
                                state_code=self.state_code,
                                status=StateSentenceStatus.COMPLETED,
                                supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
                                supervision_type_raw_text="PAROLE",
                                start_date=datetime.date(2014, 7, 17),
                                projected_completion_date=datetime.date(2015, 8, 7),
                                completion_date=datetime.date(2014, 12, 8),
                                max_length_days=580,
                                charges=[
                                    StateCharge(
                                        state_code=self.state_code,
                                        status=ChargeStatus.PRESENT_WITHOUT_INFO,
                                        court_case=StateCourtCase(
                                            state_code=self.state_code,
                                            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
                                            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
                                            judge=StateAgent(
                                                state_code=self.state_code,
                                                agent_type=StateAgentType.JUDGE,
                                                full_name='{"full_name": "THE JUDGE"}',
                                            ),
                                        ),
                                    )
                                ],
                                supervision_periods=[
                                    StateSupervisionPeriod(
                                        external_id="117111",
                                        state_code=self.state_code,
                                        supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                                        supervision_type_raw_text="PAROLE",
                                        start_date=datetime.date(2014, 7, 17),
                                        termination_date=datetime.date(2014, 12, 8),
                                        county_code="INVALID",
                                        supervision_site="4",
                                        termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
                                        termination_reason_raw_text="9",
                                        supervising_officer=StateAgent(
                                            external_id="63",
                                            state_code=self.state_code,
                                            agent_type=StateAgentType.SUPERVISION_OFFICER,
                                            full_name='{"given_names": "DAVID", "middle_names": "", "name_suffix": "", "surname": "BORG"}',
                                        ),
                                    )
                                ],
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code=self.state_code,
                external_ids=[
                    StatePersonExternalId(
                        state_code=self.state_code,
                        external_id="92237",
                        id_type="US_ND_SID",
                    )
                ],
                supervision_violations=[
                    StateSupervisionViolation(
                        state_code=self.state_code,
                        external_id="117111",
                        supervision_violation_types=[
                            StateSupervisionViolationTypeEntry(
                                state_code=self.state_code,
                                violation_type=StateSupervisionViolationType.ABSCONDED,
                            )
                        ],
                        supervision_violation_responses=[
                            StateSupervisionViolationResponse(
                                state_code=self.state_code,
                                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
                                response_date=datetime.date(2014, 12, 8),
                                supervision_violation_response_decisions=[
                                    StateSupervisionViolationResponseDecisionEntry(
                                        state_code=self.state_code,
                                        decision=StateSupervisionViolationResponseDecision.REVOCATION,
                                        decision_raw_text="DOCR INMATE SENTENCE",
                                    )
                                ],
                                decision_agents=[
                                    StateAgent(
                                        external_id="63",
                                        state_code=self.state_code,
                                        agent_type=StateAgentType.SUPERVISION_OFFICER,
                                        full_name='{"given_names": "DAVID", "middle_names": "", "name_suffix": "", "surname": "BORG"}',
                                    )
                                ],
                            )
                        ],
                    )
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        state_code=self.state_code,
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        supervision_sentences=[
                            StateSupervisionSentence(
                                external_id="117111",
                                state_code=self.state_code,
                                status=StateSentenceStatus.COMPLETED,
                                supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
                                supervision_type_raw_text="PAROLE",
                                start_date=datetime.date(2014, 7, 17),
                                projected_completion_date=datetime.date(2015, 8, 7),
                                completion_date=datetime.date(2014, 12, 8),
                                max_length_days=580,
                                charges=[
                                    StateCharge(
                                        state_code=self.state_code,
                                        status=ChargeStatus.PRESENT_WITHOUT_INFO,
                                        court_case=StateCourtCase(
                                            state_code=self.state_code,
                                            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
                                            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
                                            judge=StateAgent(
                                                state_code=self.state_code,
                                                agent_type=StateAgentType.JUDGE,
                                                full_name='{"full_name": "THE JUDGE"}',
                                            ),
                                        ),
                                    )
                                ],
                                supervision_periods=[
                                    StateSupervisionPeriod(
                                        external_id="117111",
                                        state_code=self.state_code,
                                        supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                                        supervision_type_raw_text="PAROLE",
                                        start_date=datetime.date(2014, 7, 17),
                                        termination_date=datetime.date(2014, 12, 8),
                                        county_code="INVALID",
                                        supervision_site="4",
                                        termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
                                        termination_reason_raw_text="9",
                                        supervising_officer=StateAgent(
                                            external_id="63",
                                            state_code=self.state_code,
                                            agent_type=StateAgentType.SUPERVISION_OFFICER,
                                            full_name='{"given_names": "DAVID", "middle_names": "", "name_suffix": "", "surname": "BORG"}',
                                        ),
                                    )
                                ],
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code=self.state_code,
                external_ids=[
                    StatePersonExternalId(
                        state_code=self.state_code,
                        external_id="241896",
                        id_type="US_ND_SID",
                    )
                ],
                supervision_violations=[
                    StateSupervisionViolation(
                        state_code=self.state_code,
                        external_id="140408",
                        supervision_violation_types=[
                            StateSupervisionViolationTypeEntry(
                                state_code=self.state_code,
                                violation_type=StateSupervisionViolationType.TECHNICAL,
                            )
                        ],
                        supervision_violation_responses=[
                            StateSupervisionViolationResponse(
                                state_code=self.state_code,
                                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
                                response_date=datetime.date(2018, 10, 27),
                                supervision_violation_response_decisions=[
                                    StateSupervisionViolationResponseDecisionEntry(
                                        state_code=self.state_code,
                                        decision=StateSupervisionViolationResponseDecision.REVOCATION,
                                        decision_raw_text="DOCR INMATE SENTENCE",
                                    )
                                ],
                                decision_agents=[
                                    StateAgent(
                                        external_id="77",
                                        state_code=self.state_code,
                                        agent_type=StateAgentType.SUPERVISION_OFFICER,
                                        full_name='{"given_names": "COREY", "middle_names": "", "name_suffix": "", "surname": "KOLPIN"}',
                                    )
                                ],
                            )
                        ],
                    )
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        state_code=self.state_code,
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        supervision_sentences=[
                            StateSupervisionSentence(
                                external_id="140408",
                                state_code=self.state_code,
                                status=StateSentenceStatus.COMPLETED,
                                supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
                                supervision_type_raw_text="SUSPENDED",
                                start_date=datetime.date(2017, 3, 24),
                                projected_completion_date=datetime.date(2019, 3, 23),
                                completion_date=datetime.date(2018, 2, 27),
                                charges=[
                                    StateCharge(
                                        state_code=self.state_code,
                                        status=ChargeStatus.PRESENT_WITHOUT_INFO,
                                        court_case=StateCourtCase(
                                            state_code=self.state_code,
                                            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
                                            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
                                            judge=StateAgent(
                                                state_code=self.state_code,
                                                agent_type=StateAgentType.JUDGE,
                                                full_name='{"full_name": "JUDGE PERSON"}',
                                            ),
                                        ),
                                    )
                                ],
                                supervision_periods=[
                                    StateSupervisionPeriod(
                                        external_id="140408",
                                        state_code=self.state_code,
                                        supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                                        supervision_type_raw_text="SUSPENDED",
                                        start_date=datetime.date(2017, 3, 24),
                                        termination_date=datetime.date(2018, 2, 27),
                                        county_code="US_ND_GRIGGS",
                                        supervision_site="2",
                                        termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
                                        termination_reason_raw_text="9",
                                        supervising_officer=StateAgent(
                                            external_id="77",
                                            state_code=self.state_code,
                                            agent_type=StateAgentType.SUPERVISION_OFFICER,
                                            full_name='{"given_names": "COREY", "middle_names": "", "name_suffix": "", "surname": "KOLPIN"}',
                                        ),
                                    )
                                ],
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code=self.state_code,
                external_ids=[
                    StatePersonExternalId(
                        state_code=self.state_code,
                        external_id="241896",
                        id_type="US_ND_SID",
                    )
                ],
                supervision_violations=[
                    StateSupervisionViolation(
                        state_code=self.state_code,
                        is_violent=True,
                        external_id="147777",
                        supervision_violation_types=[
                            StateSupervisionViolationTypeEntry(
                                state_code=self.state_code,
                                violation_type=StateSupervisionViolationType.LAW,
                            )
                        ],
                        supervision_violation_responses=[
                            StateSupervisionViolationResponse(
                                state_code=self.state_code,
                                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
                                response_date=datetime.date(2016, 2, 27),
                                supervision_violation_response_decisions=[
                                    StateSupervisionViolationResponseDecisionEntry(
                                        state_code=self.state_code,
                                        decision=StateSupervisionViolationResponseDecision.REVOCATION,
                                        decision_raw_text="DOCR INMATE SENTENCE",
                                    )
                                ],
                                decision_agents=[
                                    StateAgent(
                                        external_id="77",
                                        state_code=self.state_code,
                                        agent_type=StateAgentType.SUPERVISION_OFFICER,
                                        full_name='{"given_names": "COREY", "middle_names": "", "name_suffix": "", "surname": "KOLPIN"}',
                                    )
                                ],
                            )
                        ],
                    )
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        state_code=self.state_code,
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        supervision_sentences=[
                            StateSupervisionSentence(
                                external_id="147777",
                                state_code=self.state_code,
                                status=StateSentenceStatus.COMPLETED,
                                supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
                                supervision_type_raw_text="SUSPENDED",
                                start_date=datetime.date(2013, 3, 24),
                                projected_completion_date=datetime.date(2015, 3, 23),
                                completion_date=datetime.date(2016, 2, 27),
                                charges=[
                                    StateCharge(
                                        state_code=self.state_code,
                                        status=ChargeStatus.PRESENT_WITHOUT_INFO,
                                        court_case=StateCourtCase(
                                            state_code=self.state_code,
                                            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
                                            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
                                            judge=StateAgent(
                                                state_code=self.state_code,
                                                agent_type=StateAgentType.JUDGE,
                                                full_name='{"full_name": "JUDGE PERSON"}',
                                            ),
                                        ),
                                    )
                                ],
                                supervision_periods=[
                                    StateSupervisionPeriod(
                                        external_id="147777",
                                        state_code=self.state_code,
                                        supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                                        supervision_type_raw_text="SUSPENDED",
                                        start_date=datetime.date(2013, 3, 24),
                                        termination_date=datetime.date(2016, 2, 27),
                                        county_code="US_ND_GRIGGS",
                                        supervision_site="2",
                                        termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
                                        termination_reason_raw_text="9",
                                        supervising_officer=StateAgent(
                                            external_id="77",
                                            state_code=self.state_code,
                                            agent_type=StateAgentType.SUPERVISION_OFFICER,
                                            full_name='{"given_names": "COREY", "middle_names": "", "name_suffix": "", "surname": "KOLPIN"}',
                                        ),
                                    )
                                ],
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code=self.state_code,
                external_ids=[
                    StatePersonExternalId(
                        state_code=self.state_code,
                        external_id="241896",
                        id_type="US_ND_SID",
                    )
                ],
                supervision_violations=[
                    StateSupervisionViolation(
                        state_code=self.state_code,
                        is_violent=False,
                        external_id="147778",
                        supervision_violation_types=[
                            StateSupervisionViolationTypeEntry(
                                state_code=self.state_code,
                                violation_type=StateSupervisionViolationType.LAW,
                            )
                        ],
                        supervision_violation_responses=[
                            StateSupervisionViolationResponse(
                                state_code=self.state_code,
                                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
                                response_date=datetime.date(2002, 12, 13),
                                supervision_violation_response_decisions=[],
                                decision_agents=[
                                    StateAgent(
                                        external_id="76",
                                        state_code=self.state_code,
                                        agent_type=StateAgentType.SUPERVISION_OFFICER,
                                        full_name='{"given_names": "DONNA", "middle_names": "", "name_suffix": "", "surname": "KOLBORG"}',
                                    )
                                ],
                            )
                        ],
                    )
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        state_code=self.state_code,
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        supervision_sentences=[
                            StateSupervisionSentence(
                                external_id="147778",
                                state_code=self.state_code,
                                status=StateSentenceStatus.COMPLETED,
                                supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
                                supervision_type_raw_text="SUSPENDED",
                                start_date=datetime.date(2000, 9, 14),
                                projected_completion_date=datetime.date(2005, 9, 14),
                                completion_date=datetime.date(2002, 12, 13),
                                max_length_days=1826,
                                charges=[
                                    StateCharge(
                                        state_code=self.state_code,
                                        status=ChargeStatus.PRESENT_WITHOUT_INFO,
                                        court_case=StateCourtCase(
                                            state_code=self.state_code,
                                            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
                                            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
                                            judge=StateAgent(
                                                state_code=self.state_code,
                                                agent_type=StateAgentType.JUDGE,
                                                full_name='{"full_name": "JUDGE PERSON"}',
                                            ),
                                        ),
                                    )
                                ],
                                supervision_periods=[
                                    StateSupervisionPeriod(
                                        external_id="147778",
                                        state_code=self.state_code,
                                        supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                                        supervision_type_raw_text="SUSPENDED",
                                        start_date=datetime.date(2000, 9, 14),
                                        termination_date=datetime.date(2002, 12, 13),
                                        county_code="US_ND_GRAND_FORKS",
                                        supervision_site="5",
                                        termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
                                        termination_reason_raw_text="9",
                                        supervising_officer=StateAgent(
                                            external_id="76",
                                            state_code=self.state_code,
                                            agent_type=StateAgentType.SUPERVISION_OFFICER,
                                            full_name='{"given_names": "DONNA", "middle_names": "", "name_suffix": "", "surname": "KOLBORG"}',
                                        ),
                                    )
                                ],
                            )
                        ],
                    )
                ],
            ),
        ]

        self._run_parse_ingest_view_test(
            "docstars_offendercasestable_with_officers", expected_output
        )

    def test_parse_elite_externalmovements_incarceration_periods(self) -> None:
        incarceration_period_113377_1 = StateIncarcerationPeriod(
            state_code=self.state_code,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id="113377-1",
            facility="NDSP",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="NDSP",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="NDSP-2/28/2016",
            admission_date=datetime.date(year=2016, month=2, day=28),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION,
            admission_reason_raw_text="OOS",
            release_date=datetime.date(year=2016, month=7, day=26),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="RPAR",
        )

        incarceration_period_113377_2 = StateIncarcerationPeriod(
            state_code=self.state_code,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id="113377-2",
            facility="CJ",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            incarceration_type_raw_text="CJ",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="CJ-10/4/2016",
            admission_date=datetime.date(year=2016, month=10, day=4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            admission_reason_raw_text="PV",
            release_date=datetime.date(year=2016, month=10, day=20),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="INT",
        )

        incarceration_period_114909_1 = StateIncarcerationPeriod(
            state_code=self.state_code,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id="114909-1",
            facility="NDSP",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="NDSP",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="NDSP-11/9/2018",
            admission_date=datetime.date(year=2018, month=11, day=9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            admission_reason_raw_text="PV",
            release_date=datetime.date(year=2019, month=1, day=8),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="RPRB",
        )

        incarceration_period_555555_1 = StateIncarcerationPeriod(
            state_code=self.state_code,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id="555555-1",
            facility="NTAD",
            incarceration_type=StateIncarcerationType.EXTERNAL_UNKNOWN,
            incarceration_type_raw_text="NTAD",
            custodial_authority=StateCustodialAuthority.EXTERNAL_UNKNOWN,
            custodial_authority_raw_text="NTAD-2/28/2018",
            admission_date=datetime.date(year=2018, month=2, day=28),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            admission_reason_raw_text="PV",
            release_date=datetime.date(year=2018, month=3, day=1),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="INT",
        )

        incarceration_period_555555_2 = StateIncarcerationPeriod(
            state_code=self.state_code,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id="555555-2",
            facility="CJ",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            incarceration_type_raw_text="CJ",
            custodial_authority=StateCustodialAuthority.COURT,
            custodial_authority_raw_text="CJ-3/1/2018",
            admission_date=datetime.date(year=2018, month=3, day=1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            admission_reason_raw_text="PV",
            release_date=datetime.date(year=2018, month=3, day=8),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="INT",
        )

        incarceration_period_555555_3 = StateIncarcerationPeriod(
            state_code=self.state_code,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            external_id="555555-3",
            facility="NDSP",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="NDSP",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="NDSP-3/8/2018",
            admission_date=datetime.date(year=2018, month=3, day=8),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="INT",
        )

        incarceration_period_105640_1 = StateIncarcerationPeriod(
            state_code=self.state_code,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id="105640-1",
            facility="NDSP",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="NDSP",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="NDSP-1/1/2019",
            admission_date=datetime.date(year=2019, month=1, day=1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="ADMN",
            release_date=datetime.date(year=2019, month=2, day=1),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="INT",
        )

        incarceration_period_105640_2 = StateIncarcerationPeriod(
            state_code=self.state_code,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id="105640-2",
            facility="JRCC",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="JRCC",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="JRCC-2/1/2019",
            admission_date=datetime.date(year=2019, month=2, day=1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="INT",
            release_date=datetime.date(year=2019, month=3, day=1),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="HOSPS",
        )

        incarceration_period_105640_3 = StateIncarcerationPeriod(
            state_code=self.state_code,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            external_id="105640-3",
            facility="JRCC",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="JRCC",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="JRCC-4/1/2019",
            admission_date=datetime.date(year=2019, month=4, day=1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="HOSPS",
        )

        expected_output = [
            StatePerson(
                state_code=self.state_code,
                external_ids=[
                    StatePersonExternalId(
                        state_code=self.state_code,
                        external_id="113377",
                        id_type=US_ND_ELITE_BOOKING,
                    )
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        state_code=self.state_code,
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        external_id="113377",
                        incarceration_sentences=[
                            StateIncarcerationSentence(
                                state_code=self.state_code,
                                incarceration_type=StateIncarcerationType.STATE_PRISON,
                                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                                incarceration_periods=[incarceration_period_113377_1],
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code=self.state_code,
                external_ids=[
                    StatePersonExternalId(
                        state_code=self.state_code,
                        external_id="113377",
                        id_type=US_ND_ELITE_BOOKING,
                    )
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        state_code=self.state_code,
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        external_id="113377",
                        incarceration_sentences=[
                            StateIncarcerationSentence(
                                state_code=self.state_code,
                                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                                incarceration_type=StateIncarcerationType.STATE_PRISON,
                                incarceration_periods=[incarceration_period_113377_2],
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code=self.state_code,
                external_ids=[
                    StatePersonExternalId(
                        state_code=self.state_code,
                        external_id="114909",
                        id_type=US_ND_ELITE_BOOKING,
                    )
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        state_code=self.state_code,
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        external_id="114909",
                        incarceration_sentences=[
                            StateIncarcerationSentence(
                                state_code=self.state_code,
                                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                                incarceration_type=StateIncarcerationType.STATE_PRISON,
                                incarceration_periods=[incarceration_period_114909_1],
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code=self.state_code,
                external_ids=[
                    StatePersonExternalId(
                        state_code=self.state_code,
                        external_id="555555",
                        id_type=US_ND_ELITE_BOOKING,
                    )
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        state_code=self.state_code,
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        external_id="555555",
                        incarceration_sentences=[
                            StateIncarcerationSentence(
                                state_code=self.state_code,
                                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                                incarceration_type=StateIncarcerationType.STATE_PRISON,
                                incarceration_periods=[incarceration_period_555555_1],
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code=self.state_code,
                external_ids=[
                    StatePersonExternalId(
                        state_code=self.state_code,
                        external_id="555555",
                        id_type=US_ND_ELITE_BOOKING,
                    )
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        state_code=self.state_code,
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        external_id="555555",
                        incarceration_sentences=[
                            StateIncarcerationSentence(
                                state_code=self.state_code,
                                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                                incarceration_type=StateIncarcerationType.STATE_PRISON,
                                incarceration_periods=[incarceration_period_555555_2],
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code=self.state_code,
                external_ids=[
                    StatePersonExternalId(
                        state_code=self.state_code,
                        external_id="555555",
                        id_type=US_ND_ELITE_BOOKING,
                    )
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        state_code=self.state_code,
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        external_id="555555",
                        incarceration_sentences=[
                            StateIncarcerationSentence(
                                state_code=self.state_code,
                                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                                incarceration_type=StateIncarcerationType.STATE_PRISON,
                                incarceration_periods=[incarceration_period_555555_3],
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code=self.state_code,
                external_ids=[
                    StatePersonExternalId(
                        state_code=self.state_code,
                        external_id="105640",
                        id_type=US_ND_ELITE_BOOKING,
                    )
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        state_code=self.state_code,
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        external_id="105640",
                        incarceration_sentences=[
                            StateIncarcerationSentence(
                                state_code=self.state_code,
                                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                                incarceration_type=StateIncarcerationType.STATE_PRISON,
                                incarceration_periods=[incarceration_period_105640_1],
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code=self.state_code,
                external_ids=[
                    StatePersonExternalId(
                        state_code=self.state_code,
                        external_id="105640",
                        id_type=US_ND_ELITE_BOOKING,
                    )
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        state_code=self.state_code,
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        external_id="105640",
                        incarceration_sentences=[
                            StateIncarcerationSentence(
                                state_code=self.state_code,
                                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                                incarceration_type=StateIncarcerationType.STATE_PRISON,
                                incarceration_periods=[incarceration_period_105640_2],
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code=self.state_code,
                external_ids=[
                    StatePersonExternalId(
                        state_code=self.state_code,
                        external_id="105640",
                        id_type=US_ND_ELITE_BOOKING,
                    )
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        state_code=self.state_code,
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        external_id="105640",
                        incarceration_sentences=[
                            StateIncarcerationSentence(
                                state_code=self.state_code,
                                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                                incarceration_type=StateIncarcerationType.STATE_PRISON,
                                incarceration_periods=[incarceration_period_105640_3],
                            )
                        ],
                    )
                ],
            ),
        ]

        self._run_parse_ingest_view_test(
            "elite_externalmovements_incarceration_periods", expected_output
        )
