# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for the ingest info state_converter."""
import datetime
import unittest
from typing import List

from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.person_characteristics import Ethnicity, Race
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_assessment import StateAssessmentClass
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_charge import StateChargeClassificationType
from recidiviz.common.constants.state.state_court_case import (
    StateCourtCaseStatus,
    StateCourtType,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_incident import (
    StateIncarcerationIncidentOutcomeType,
    StateIncarcerationIncidentType,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodStatus,
)
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentDischargeReason,
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseType,
)
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import IngestInfo
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state.entities import (
    StateAgent,
    StateAssessment,
    StateCharge,
    StateCourtCase,
    StateIncarcerationIncident,
    StateIncarcerationIncidentOutcome,
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StateParoleDecision,
    StatePerson,
    StatePersonAlias,
    StatePersonEthnicity,
    StatePersonExternalId,
    StatePersonRace,
    StateProgramAssignment,
    StateSentenceGroup,
    StateSupervisionCaseTypeEntry,
    StateSupervisionContact,
    StateSupervisionPeriod,
    StateSupervisionSentence,
    StateSupervisionViolatedConditionEntry,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
    StateSupervisionViolationResponseDecisionEntry,
    StateSupervisionViolationTypeEntry,
)
from recidiviz.persistence.ingest_info_converter import ingest_info_converter
from recidiviz.persistence.ingest_info_converter.ingest_info_converter import (
    EntityDeserializationResult,
)
from recidiviz.tests.persistence.database.database_test_utils import FakeIngestMetadata

_INGEST_TIME = datetime.datetime(year=2019, month=2, day=13, hour=12)
_JURISDICTION_ID = "JURISDICTION_ID"

US_XX_DOC_ID = "US_XX_DOC_ID"
US_XX_SID = "US_XX_SID"


class TestIngestInfoStateConverter(unittest.TestCase):
    """Test converting IngestInfo objects to Persistence layer objects."""

    def setUp(self) -> None:
        self.maxDiff = None

    @staticmethod
    def _convert_and_throw_on_errors(
        ingest_info: IngestInfo, metadata: IngestMetadata
    ) -> List[state_entities.StatePerson]:
        conversion_result: EntityDeserializationResult = (
            ingest_info_converter.convert_to_persistence_entities(ingest_info, metadata)
        )
        if conversion_result.enum_parsing_errors > 0:
            raise ValueError(
                f"Had [{conversion_result.enum_parsing_errors}] enum parsing errors"
            )

        if conversion_result.general_parsing_errors > 0:
            raise ValueError(
                f"Had [{conversion_result.general_parsing_errors}] general parsing errors"
            )

        if conversion_result.protected_class_errors > 0:
            raise ValueError(
                f"Had [{conversion_result.protected_class_errors}] protected class errors"
            )
        return conversion_result.people

    def testConvert_FullIngestInfo(self) -> None:
        # Arrange
        metadata = FakeIngestMetadata.for_state(region="US_XX")

        ingest_info = IngestInfo()
        ingest_info.state_agents.add(
            state_agent_id="AGENT_ID1", full_name="AGENT WILLIAMS"
        )
        ingest_info.state_agents.add(
            state_agent_id="AGENT_ID2", full_name="AGENT HERNANDEZ"
        )
        ingest_info.state_agents.add(
            state_agent_id="AGENT_ID3", full_name="AGENT SMITH"
        )
        ingest_info.state_agents.add(state_agent_id="AGENT_ID4", full_name="AGENT PO")
        ingest_info.state_agents.add(
            state_agent_id="JUDGE_AGENT_ID_1", full_name="JUDGE JUDY"
        )
        ingest_info.state_agents.add(
            state_agent_id="AGENT_ID_PO", full_name="AGENT PAROLEY"
        )
        ingest_info.state_agents.add(
            state_agent_id="AGENT_ID_TERM",
            full_name="AGENT TERMY",
            agent_type="SUPERVISION_OFFICER",
        )
        ingest_info.state_agents.add(
            state_agent_id="AGENT_ID_SUPERVISING",
            full_name="SUPERVISING AGENT",
        )

        # We expect the external_ids coming in to have the format
        # [type]:[external_id]
        ii_person_external_id_1 = US_XX_DOC_ID + ":" + "EXTERNAL_ID1"
        ii_person_external_id_2 = US_XX_SID + ":" + "EXTERNAL_ID2"

        ingest_info.state_people.add(
            state_person_id="PERSON_ID",
            state_person_race_ids=["RACE_ID1", "RACE_ID2"],
            state_person_ethnicity_ids=["ETHNICITY_ID"],
            state_alias_ids=["ALIAS_ID1", "ALIAS_ID2"],
            state_person_external_ids_ids=[
                ii_person_external_id_1,
                ii_person_external_id_2,
            ],
            state_assessment_ids=["ASSESSMENT_ID"],
            state_program_assignment_ids=["PROGRAM_ASSIGNMENT_ID"],
            state_incarceration_incident_ids=["INCIDENT_ID"],
            state_supervision_violation_ids=["VIOLATION_ID"],
            state_supervision_contact_ids=["SUPERVISION_CONTACT_ID"],
            state_sentence_group_ids=["GROUP_ID1", "GROUP_ID2"],
            supervising_officer_id="AGENT_ID_SUPERVISING",
        )
        ingest_info.state_person_races.add(
            state_person_race_id="RACE_ID1",
            race="WHITE",
        )
        ingest_info.state_person_races.add(
            state_person_race_id="RACE_ID2", race="OTHER"
        )
        ingest_info.state_person_ethnicities.add(
            state_person_ethnicity_id="ETHNICITY_ID", ethnicity="HISPANIC"
        )
        ingest_info.state_aliases.add(
            state_alias_id="ALIAS_ID1", full_name="LONNY BREAUX"
        )
        ingest_info.state_aliases.add(
            state_alias_id="ALIAS_ID2", full_name="FRANK OCEAN"
        )
        ingest_info.state_person_external_ids.add(
            state_person_external_id_id=ii_person_external_id_1, id_type=US_XX_DOC_ID
        )
        ingest_info.state_person_external_ids.add(
            state_person_external_id_id=ii_person_external_id_2, id_type=US_XX_SID
        )
        ingest_info.state_assessments.add(
            state_assessment_id="ASSESSMENT_ID",
            assessment_class="MENTAL_HEALTH",
            conducting_agent_id="AGENT_ID1",
        )
        ingest_info.state_program_assignments.add(
            state_program_assignment_id="PROGRAM_ASSIGNMENT_ID",
            participation_status="DISCHARGED",
            referral_date="2019/02/10",
            start_date="2019/02/11",
            discharge_date="2019/02/12",
            program_id="PROGRAM_ID",
            program_location_id="PROGRAM_LOCATION_ID",
            discharge_reason="COMPLETED",
            referring_agent_id="AGENT_ID4",
        )
        ingest_info.state_sentence_groups.add(
            state_sentence_group_id="GROUP_ID1",
            state_supervision_sentence_ids=["SUPERVISION_SENTENCE_ID1"],
            state_incarceration_sentence_ids=[
                "INCARCERATION_SENTENCE_ID1",
                "INCARCERATION_SENTENCE_ID2",
            ],
        )
        ingest_info.state_sentence_groups.add(
            state_sentence_group_id="GROUP_ID2",
            state_supervision_sentence_ids=["SUPERVISION_SENTENCE_ID2"],
        )
        ingest_info.state_supervision_sentences.add(
            state_supervision_sentence_id="SUPERVISION_SENTENCE_ID1",
            state_charge_ids=["CHARGE_ID1", "CHARGE_ID2"],
            state_supervision_period_ids=["S_PERIOD_ID1"],
        )
        ingest_info.state_supervision_sentences.add(
            state_supervision_sentence_id="SUPERVISION_SENTENCE_ID2",
            state_charge_ids=["CHARGE_ID2"],
            state_supervision_period_ids=["S_PERIOD_ID2"],
        )
        ingest_info.state_incarceration_sentences.add(
            state_incarceration_sentence_id="INCARCERATION_SENTENCE_ID1",
            state_charge_ids=["CHARGE_ID1"],
            state_incarceration_period_ids=["I_PERIOD_ID"],
        )
        ingest_info.state_incarceration_sentences.add(
            state_incarceration_sentence_id="INCARCERATION_SENTENCE_ID2",
            state_charge_ids=["CHARGE_ID2", "CHARGE_ID3"],
            state_supervision_period_ids=["S_PERIOD_ID3"],
        )
        ingest_info.state_charges.add(
            state_charge_id="CHARGE_ID1",
            state_court_case_id="CASE_ID",
            classification_type="M",
            classification_subtype="1",
            ncic_code="5006",
        )
        ingest_info.state_charges.add(
            state_charge_id="CHARGE_ID2",
            state_court_case_id="CASE_ID",
            classification_type="M",
            classification_subtype="2",
        )
        ingest_info.state_charges.add(
            state_charge_id="CHARGE_ID3",
            state_court_case_id="CASE_ID",
            classification_type="F",
            classification_subtype="3",
            ncic_code="5006",
            description="Obstruction of investigation",
        )
        ingest_info.state_court_cases.add(
            state_court_case_id="CASE_ID",
            judge_id="JUDGE_AGENT_ID_1",
        )
        ingest_info.state_supervision_periods.add(
            state_supervision_period_id="S_PERIOD_ID1",
            supervision_type="PAROLE",
            supervision_level="MED",
        )
        ingest_info.state_supervision_periods.add(
            state_supervision_period_id="S_PERIOD_ID2",
            supervision_type="PAROLE",
        )
        ingest_info.state_supervision_periods.add(
            state_supervision_period_id="S_PERIOD_ID3",
            supervising_officer_id="AGENT_ID_PO",
            supervision_type="PROBATION",
            state_supervision_case_type_entry_ids=["CASE_TYPE_ID"],
        )
        ingest_info.state_supervision_case_type_entries.add(
            state_supervision_case_type_entry_id="CASE_TYPE_ID",
            case_type="DOMESTIC_VIOLENCE",
        )

        ingest_info.state_incarceration_periods.add(
            state_incarceration_period_id="I_PERIOD_ID",
            state_parole_decision_ids=["DECISION_ID"],
        )

        ingest_info.state_supervision_violation_type_entries.add(
            state_supervision_violation_type_entry_id="VIOLATION_TYPE_ENTRY_ID",
            violation_type="FELONY",
            state_code="US_XX",
        )

        ingest_info.state_supervision_violated_condition_entries.add(
            state_supervision_violated_condition_entry_id="VIOLATED_CONDITION_ENTRY_ID",
            condition="CURFEW",
            state_code="US_XX",
        )

        ingest_info.state_supervision_violations.add(
            state_supervision_violation_id="VIOLATION_ID",
            state_supervision_violation_response_ids=["RESPONSE_ID"],
            state_supervision_violated_condition_entry_ids=[
                "VIOLATED_CONDITION_ENTRY_ID"
            ],
            state_supervision_violation_type_entry_ids=["VIOLATION_TYPE_ENTRY_ID"],
        )

        ingest_info.state_supervision_violated_condition_entries.add(
            state_supervision_violated_condition_entry_id="VIOLATED_CONDITION_ENTRY_ID",
            condition="CURFEW",
            state_code="US_XX",
        )

        ingest_info.state_supervision_violation_response_decision_entries.add(
            state_supervision_violation_response_decision_entry_id="VIOLATION_RESPONSE_DECISION_ENTRY_ID",
            decision="REVOCATION",
            state_code="US_XX",
        )

        ingest_info.state_supervision_violation_responses.add(
            state_supervision_violation_response_id="RESPONSE_ID",
            decision_agent_ids=["AGENT_ID_TERM"],
            state_supervision_violation_response_decision_entry_ids=[
                "VIOLATION_RESPONSE_DECISION_ENTRY_ID"
            ],
            response_type="CITATION",
        )
        ingest_info.state_incarceration_incidents.add(
            state_incarceration_incident_id="INCIDENT_ID",
            incident_type="CONTRABAND",
            responding_officer_id="AGENT_ID2",
            state_incarceration_incident_outcome_ids=["INCIDENT_OUTCOME_ID"],
        )

        ingest_info.state_incarceration_incident_outcomes.add(
            state_incarceration_incident_outcome_id="INCIDENT_OUTCOME_ID",
            outcome_type="GOOD_TIME_LOSS",
            date_effective="2/10/2018",
            hearing_date="2/6/2018",
            report_date="2/8/2018",
            state_code="US_XX",
            outcome_description="Good time",
            punishment_length_days="7",
        )
        ingest_info.state_parole_decisions.add(
            state_parole_decision_id="DECISION_ID",
            decision_agent_ids=["AGENT_ID2", "AGENT_ID3"],
        )
        ingest_info.state_supervision_contacts.add(
            state_supervision_contact_id="SUPERVISION_CONTACT_ID",
            contacted_agent_id="AGENT_ID_PO",
        )

        # Act
        result = self._convert_and_throw_on_errors(ingest_info, metadata)

        # Assert
        supervision_contact = StateSupervisionContact.new_with_defaults(
            external_id="SUPERVISION_CONTACT_ID",
            state_code="US_XX",
            contacted_agent=StateAgent.new_with_defaults(
                external_id="AGENT_ID_PO",
                state_code="US_XX",
                agent_type=StateAgentType.PRESENT_WITHOUT_INFO,
                full_name='{"full_name": "AGENT PAROLEY"}',
            ),
        )

        incident_outcome = StateIncarcerationIncidentOutcome.new_with_defaults(
            external_id="INCIDENT_OUTCOME_ID",
            outcome_type=StateIncarcerationIncidentOutcomeType.GOOD_TIME_LOSS,
            outcome_type_raw_text="GOOD_TIME_LOSS",
            date_effective=datetime.date(year=2018, month=2, day=10),
            hearing_date=datetime.date(year=2018, month=2, day=6),
            report_date=datetime.date(year=2018, month=2, day=8),
            state_code="US_XX",
            outcome_description="GOOD TIME",
            punishment_length_days=7,
        )

        incident = StateIncarcerationIncident.new_with_defaults(
            external_id="INCIDENT_ID",
            state_code="US_XX",
            incident_type=StateIncarcerationIncidentType.CONTRABAND,
            incident_type_raw_text="CONTRABAND",
            responding_officer=StateAgent.new_with_defaults(
                external_id="AGENT_ID2",
                state_code="US_XX",
                agent_type=StateAgentType.PRESENT_WITHOUT_INFO,
                full_name='{"full_name": "AGENT HERNANDEZ"}',
            ),
            incarceration_incident_outcomes=[incident_outcome],
        )

        assessment = StateAssessment.new_with_defaults(
            external_id="ASSESSMENT_ID",
            state_code="US_XX",
            assessment_class=StateAssessmentClass.MENTAL_HEALTH,
            assessment_class_raw_text="MENTAL_HEALTH",
            conducting_agent=StateAgent.new_with_defaults(
                external_id="AGENT_ID1",
                agent_type=StateAgentType.PRESENT_WITHOUT_INFO,
                state_code="US_XX",
                full_name='{"full_name": "AGENT WILLIAMS"}',
            ),
        )

        program_assignment = StateProgramAssignment.new_with_defaults(
            external_id="PROGRAM_ASSIGNMENT_ID",
            state_code="US_XX",
            participation_status=StateProgramAssignmentParticipationStatus.DISCHARGED,
            participation_status_raw_text="DISCHARGED",
            referral_date=datetime.date(year=2019, month=2, day=10),
            start_date=datetime.date(year=2019, month=2, day=11),
            discharge_date=datetime.date(year=2019, month=2, day=12),
            program_id="PROGRAM_ID",
            program_location_id="PROGRAM_LOCATION_ID",
            discharge_reason=StateProgramAssignmentDischargeReason.COMPLETED,
            discharge_reason_raw_text="COMPLETED",
            referring_agent=StateAgent.new_with_defaults(
                external_id="AGENT_ID4",
                agent_type=StateAgentType.PRESENT_WITHOUT_INFO,
                state_code="US_XX",
                full_name='{"full_name": "AGENT PO"}',
            ),
        )

        response = StateSupervisionViolationResponse.new_with_defaults(
            external_id="RESPONSE_ID",
            state_code="US_XX",
            response_type=StateSupervisionViolationResponseType.CITATION,
            response_type_raw_text="CITATION",
            decision_agents=[
                StateAgent.new_with_defaults(
                    external_id="AGENT_ID_TERM",
                    state_code="US_XX",
                    full_name='{"full_name": "AGENT TERMY"}',
                    agent_type=StateAgentType.SUPERVISION_OFFICER,
                    agent_type_raw_text="SUPERVISION_OFFICER",
                )
            ],
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_XX",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    decision_raw_text="REVOCATION",
                )
            ],
        )

        violation = StateSupervisionViolation.new_with_defaults(
            external_id="VIOLATION_ID",
            state_code="US_XX",
            supervision_violation_responses=[response],
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.FELONY,
                    violation_type_raw_text="FELONY",
                )
            ],
            supervision_violated_conditions=[
                StateSupervisionViolatedConditionEntry.new_with_defaults(
                    state_code="US_XX",
                    condition="CURFEW",
                )
            ],
        )

        court_case = StateCourtCase.new_with_defaults(
            external_id="CASE_ID",
            state_code="US_XX",
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            judge=StateAgent.new_with_defaults(
                external_id="JUDGE_AGENT_ID_1",
                state_code="US_XX",
                agent_type=StateAgentType.PRESENT_WITHOUT_INFO,
                full_name='{"full_name": "JUDGE JUDY"}',
            ),
        )

        charge_1 = StateCharge.new_with_defaults(
            external_id="CHARGE_ID1",
            classification_type=StateChargeClassificationType.MISDEMEANOR,
            classification_type_raw_text="M",
            classification_subtype="1",
            ncic_code="5006",
            state_code="US_XX",
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            court_case=court_case,
        )

        charge_2 = StateCharge.new_with_defaults(
            external_id="CHARGE_ID2",
            classification_type=StateChargeClassificationType.MISDEMEANOR,
            classification_type_raw_text="M",
            classification_subtype="2",
            state_code="US_XX",
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            court_case=court_case,
        )

        charge_3 = StateCharge.new_with_defaults(
            external_id="CHARGE_ID3",
            state_code="US_XX",
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="F",
            classification_subtype="3",
            ncic_code="5006",
            description="OBSTRUCTION OF INVESTIGATION",
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            court_case=court_case,
        )

        incarceration_sentence_1 = StateIncarcerationSentence.new_with_defaults(
            external_id="INCARCERATION_SENTENCE_ID1",
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            charges=[charge_1],
            incarceration_periods=[
                StateIncarcerationPeriod.new_with_defaults(
                    external_id="I_PERIOD_ID",
                    status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
                    incarceration_type=StateIncarcerationType.STATE_PRISON,
                    state_code="US_XX",
                    parole_decisions=[
                        StateParoleDecision.new_with_defaults(
                            external_id="DECISION_ID",
                            state_code="US_XX",
                            decision_agents=[
                                StateAgent.new_with_defaults(
                                    external_id="AGENT_ID2",
                                    agent_type=StateAgentType.PRESENT_WITHOUT_INFO,
                                    state_code="US_XX",
                                    full_name='{"full_name": "AGENT HERNANDEZ"}',
                                ),
                                StateAgent.new_with_defaults(
                                    external_id="AGENT_ID3",
                                    state_code="US_XX",
                                    agent_type=StateAgentType.PRESENT_WITHOUT_INFO,
                                    full_name='{"full_name": "AGENT SMITH"}',
                                ),
                            ],
                        )
                    ],
                )
            ],
        )

        incarceration_sentence_2 = StateIncarcerationSentence.new_with_defaults(
            external_id="INCARCERATION_SENTENCE_ID2",
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            charges=[charge_2, charge_3],
            supervision_periods=[
                StateSupervisionPeriod.new_with_defaults(
                    external_id="S_PERIOD_ID3",
                    state_code="US_XX",
                    supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                    supervision_type_raw_text="PROBATION",
                    supervising_officer=StateAgent.new_with_defaults(
                        external_id="AGENT_ID_PO",
                        state_code="US_XX",
                        agent_type=StateAgentType.PRESENT_WITHOUT_INFO,
                        full_name='{"full_name": "AGENT PAROLEY"}',
                    ),
                    case_type_entries=[
                        StateSupervisionCaseTypeEntry.new_with_defaults(
                            case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                            case_type_raw_text="DOMESTIC_VIOLENCE",
                            state_code="US_XX",
                            external_id="CASE_TYPE_ID",
                        )
                    ],
                )
            ],
        )

        expected_result = [
            StatePerson.new_with_defaults(
                state_code="US_XX",
                external_ids=[
                    StatePersonExternalId.new_with_defaults(
                        external_id="EXTERNAL_ID1",
                        state_code="US_XX",
                        id_type=US_XX_DOC_ID,
                    ),
                    StatePersonExternalId.new_with_defaults(
                        external_id="EXTERNAL_ID2",
                        state_code="US_XX",
                        id_type=US_XX_SID,
                    ),
                ],
                races=[
                    StatePersonRace(
                        race=Race.WHITE, race_raw_text="WHITE", state_code="US_XX"
                    ),
                    StatePersonRace(
                        race=Race.OTHER, race_raw_text="OTHER", state_code="US_XX"
                    ),
                ],
                ethnicities=[
                    StatePersonEthnicity(
                        ethnicity=Ethnicity.HISPANIC,
                        ethnicity_raw_text="HISPANIC",
                        state_code="US_XX",
                    )
                ],
                aliases=[
                    StatePersonAlias.new_with_defaults(
                        full_name='{"full_name": "LONNY BREAUX"}', state_code="US_XX"
                    ),
                    StatePersonAlias.new_with_defaults(
                        full_name='{"full_name": "FRANK OCEAN"}', state_code="US_XX"
                    ),
                ],
                supervising_officer=StateAgent.new_with_defaults(
                    external_id="AGENT_ID_SUPERVISING",
                    state_code="US_XX",
                    agent_type=StateAgentType.PRESENT_WITHOUT_INFO,
                    full_name='{"full_name": "SUPERVISING AGENT"}',
                ),
                assessments=[assessment],
                program_assignments=[program_assignment],
                incarceration_incidents=[incident],
                supervision_violations=[violation],
                supervision_contacts=[supervision_contact],
                sentence_groups=[
                    StateSentenceGroup.new_with_defaults(
                        external_id="GROUP_ID1",
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        state_code="US_XX",
                        supervision_sentences=[
                            StateSupervisionSentence.new_with_defaults(
                                external_id="SUPERVISION_SENTENCE_ID1",
                                state_code="US_XX",
                                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                                charges=[charge_1, charge_2],
                                supervision_periods=[
                                    StateSupervisionPeriod.new_with_defaults(
                                        external_id="S_PERIOD_ID1",
                                        supervision_level=StateSupervisionLevel.MEDIUM,
                                        supervision_level_raw_text="MED",
                                        state_code="US_XX",
                                        supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                                        supervision_type_raw_text="PAROLE",
                                    )
                                ],
                            )
                        ],
                        incarceration_sentences=[
                            incarceration_sentence_1,
                            incarceration_sentence_2,
                        ],
                    ),
                    StateSentenceGroup.new_with_defaults(
                        external_id="GROUP_ID2",
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        state_code="US_XX",
                        supervision_sentences=[
                            StateSupervisionSentence.new_with_defaults(
                                external_id="SUPERVISION_SENTENCE_ID2",
                                state_code="US_XX",
                                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                                charges=[charge_2],
                                supervision_periods=[
                                    StateSupervisionPeriod.new_with_defaults(
                                        external_id="S_PERIOD_ID2",
                                        state_code="US_XX",
                                        supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                                        supervision_type_raw_text="PAROLE",
                                    )
                                ],
                            )
                        ],
                    ),
                ],
            )
        ]

        print("\n", expected_result, "\n\n\n", result)

        self.assertCountEqual(expected_result, result)

    def testConvert_CannotConvertField_RaisesValueError(self) -> None:
        # Arrange
        metadata = metadata = FakeIngestMetadata.for_state(region="us_xx")

        ingest_info = IngestInfo()
        ingest_info.state_people.add(birthdate="NOT_A_DATE")

        # Act + Assert
        with self.assertRaises(ValueError):
            self._convert_and_throw_on_errors(ingest_info, metadata)
