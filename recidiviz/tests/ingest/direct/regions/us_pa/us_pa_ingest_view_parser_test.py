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
"""Ingest view parser tests for US_PA direct ingest."""
import datetime
import unittest

from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.person_characteristics import (
    Ethnicity,
    Gender,
    Race,
    ResidencyStatus,
)
from recidiviz.common.constants.state.external_id_types import (
    US_PA_CONTROL,
    US_PA_INMATE,
    US_PA_PBPP,
)
from recidiviz.common.constants.state.state_charge import StateChargeClassificationType
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_incident import (
    StateIncarcerationIncidentOutcomeType,
    StateIncarcerationIncidentType,
)
from recidiviz.common.constants.state.state_person_alias import StatePersonAliasType
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
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
    StateCharge,
    StateIncarcerationIncident,
    StateIncarcerationIncidentOutcome,
    StateIncarcerationSentence,
    StatePerson,
    StatePersonAlias,
    StatePersonEthnicity,
    StatePersonExternalId,
    StatePersonRace,
    StateSentenceGroup,
    StateSupervisionViolatedConditionEntry,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
    StateSupervisionViolationResponseDecisionEntry,
    StateSupervisionViolationTypeEntry,
)
from recidiviz.tests.ingest.direct.regions.state_ingest_view_parser_test_base import (
    StateIngestViewParserTestBase,
)


class UsPaIngestViewParserTest(StateIngestViewParserTestBase, unittest.TestCase):
    """Parser unit tests for each US_PA ingest view file to be ingested."""

    @classmethod
    def schema_type(cls) -> SchemaType:
        return SchemaType.STATE

    @classmethod
    def region_code(cls) -> str:
        return "US_PA"

    @property
    def test(self) -> unittest.TestCase:
        return self

    def test_parse_person_external_ids(self) -> None:
        expected_output = [
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="123456", id_type=US_PA_CONTROL
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="AB7413", id_type=US_PA_INMATE
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="123A", id_type=US_PA_PBPP
                    ),
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="654321", id_type=US_PA_CONTROL
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="GF3374", id_type=US_PA_INMATE
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="456B", id_type=US_PA_PBPP
                    ),
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="445566", id_type=US_PA_CONTROL
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="CJ1991", id_type=US_PA_INMATE
                    ),
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="778899", id_type=US_PA_CONTROL
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="889900", id_type=US_PA_CONTROL
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="JE1989", id_type=US_PA_INMATE
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="345E", id_type=US_PA_PBPP
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="999Z", id_type=US_PA_PBPP
                    ),
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="789C", id_type=US_PA_PBPP
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA",
                        external_id="888888888",
                        id_type=US_PA_INMATE,
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="888P", id_type=US_PA_PBPP
                    ),
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="090909", id_type=US_PA_CONTROL
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="9999999", id_type=US_PA_INMATE
                    ),
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="060606", id_type=US_PA_CONTROL
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="060607", id_type=US_PA_CONTROL
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="66666666", id_type=US_PA_INMATE
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="66666667", id_type=US_PA_INMATE
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="666P", id_type=US_PA_PBPP
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="777M", id_type=US_PA_PBPP
                    ),
                ],
            ),
        ]

        self._run_parse_ingest_view_test("person_external_ids", expected_output)

    def test_parse_doc_person_info(self) -> None:
        expected_output = [
            StatePerson(
                state_code="US_PA",
                current_address="123 EASY STREET, PITTSBURGH, PA 16161",
                full_name='{"given_names": "BERTRAND", "middle_names": "", "name_suffix": "", "surname": "RUSSELL"}',
                birthdate=datetime.date(1976, 3, 18),
                gender=Gender.MALE,
                gender_raw_text="MALE",
                residency_status=ResidencyStatus.PERMANENT,
                residency_status_raw_text="123 EASY STREET",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="123456", id_type="US_PA_CONT"
                    )
                ],
                aliases=[
                    StatePersonAlias(
                        state_code="US_PA",
                        alias_type=StatePersonAliasType.GIVEN_NAME,
                        full_name='{"given_names": "BERTRAND", "middle_names": "", "name_suffix": "", "surname": "RUSSELL"}',
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code="US_PA", race=Race.BLACK, race_raw_text="BLACK"
                    )
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        external_id="AB7413",
                        state_code="US_PA",
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                    ),
                    StateSentenceGroup(
                        external_id="BC8524",
                        state_code="US_PA",
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                    ),
                ],
            ),
            StatePerson(
                state_code="US_PA",
                current_address="555 FLATBUSH DR, NEW YORK, NY 10031",
                full_name='{"given_names": "JEAN-PAUL", "middle_names": "", "name_suffix": "", "surname": "SARTRE"}',
                birthdate=datetime.date(1982, 10, 2),
                gender=Gender.MALE,
                gender_raw_text="MALE",
                residency_status=ResidencyStatus.PERMANENT,
                residency_status_raw_text="555 FLATBUSH DR",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="654321", id_type="US_PA_CONT"
                    )
                ],
                aliases=[
                    StatePersonAlias(
                        state_code="US_PA",
                        alias_type=StatePersonAliasType.GIVEN_NAME,
                        full_name='{"given_names": "JEAN-PAUL", "middle_names": "", "name_suffix": "", "surname": "SARTRE"}',
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code="US_PA", race=Race.BLACK, race_raw_text="BLACK"
                    )
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        external_id="GF3374",
                        state_code="US_PA",
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                current_address="5000 SUNNY LANE, APT. 55D, PHILADELPHIA, PA 19129",
                full_name='{"given_names": "SOREN", "middle_names": "", "name_suffix": "JR", "surname": "KIERKEGAARD"}',
                birthdate=datetime.date(1991, 11, 20),
                gender=Gender.FEMALE,
                gender_raw_text="FEMALE",
                residency_status=ResidencyStatus.PERMANENT,
                residency_status_raw_text="5000 SUNNY LANE-APT. 55D",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="445566", id_type="US_PA_CONT"
                    )
                ],
                aliases=[
                    StatePersonAlias(
                        state_code="US_PA",
                        alias_type=StatePersonAliasType.GIVEN_NAME,
                        full_name='{"given_names": "SOREN", "middle_names": "", "name_suffix": "JR", "surname": "KIERKEGAARD"}',
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code="US_PA", race=Race.WHITE, race_raw_text="WHITE"
                    )
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        external_id="CJ1991",
                        state_code="US_PA",
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                current_address="214 HAPPY PLACE, PHILADELPHIA, PA 19129",
                full_name='{"given_names": "JOHN", "middle_names": "", "name_suffix": "", "surname": "RAWLS"}',
                birthdate=datetime.date(1989, 6, 17),
                gender=Gender.MALE,
                gender_raw_text="MALE",
                residency_status=ResidencyStatus.PERMANENT,
                residency_status_raw_text="214 HAPPY PLACE",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="778899", id_type="US_PA_CONT"
                    )
                ],
                aliases=[
                    StatePersonAlias(
                        state_code="US_PA",
                        alias_type=StatePersonAliasType.GIVEN_NAME,
                        full_name='{"given_names": "JOHN", "middle_names": "", "name_suffix": "", "surname": "RAWLS"}',
                    )
                ],
                ethnicities=[
                    StatePersonEthnicity(
                        state_code="US_PA",
                        ethnicity=Ethnicity.HISPANIC,
                        ethnicity_raw_text="HISPANIC",
                    )
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        external_id="JE1989",
                        state_code="US_PA",
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                    )
                ],
            ),
        ]
        self._run_parse_ingest_view_test("doc_person_info", expected_output)

    def test_parse_dbo_Offender_v2(self) -> None:
        expected_output = [
            StatePerson(
                state_code="US_PA",
                gender=Gender.MALE,
                gender_raw_text="M",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="123A", id_type="US_PA_PBPP"
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code="US_PA", race=Race.ASIAN, race_raw_text="A"
                    ),
                ],
            ),
            StatePerson(
                state_code="US_PA",
                gender=Gender.MALE,
                gender_raw_text="M",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="456B", id_type="US_PA_PBPP"
                    )
                ],
                ethnicities=[
                    StatePersonEthnicity(
                        state_code="US_PA",
                        ethnicity=Ethnicity.HISPANIC,
                        ethnicity_raw_text="H",
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                full_name='{"given_names": "KEN", "middle_names": "", "name_suffix": "JR", "surname": "GRIFFEY"}',
                gender=Gender.FEMALE,
                gender_raw_text="F",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="789C", id_type="US_PA_PBPP"
                    )
                ],
                aliases=[
                    StatePersonAlias(
                        state_code="US_PA",
                        alias_type=StatePersonAliasType.GIVEN_NAME,
                        full_name='{"given_names": "KEN", "middle_names": "", "name_suffix": "JR", "surname": "GRIFFEY"}',
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code="US_PA", race=Race.OTHER, race_raw_text="N"
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                full_name='{"given_names": "JOHN", "middle_names": "", "name_suffix": "", "surname": "RAWLS"}',
                gender=Gender.MALE,
                gender_raw_text="M",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="345E", id_type="US_PA_PBPP"
                    )
                ],
                aliases=[
                    StatePersonAlias(
                        state_code="US_PA",
                        alias_type=StatePersonAliasType.GIVEN_NAME,
                        full_name='{"given_names": "JOHN", "middle_names": "", "name_suffix": "", "surname": "RAWLS"}',
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code="US_PA", race=Race.WHITE, race_raw_text="W"
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                gender=Gender.MALE,
                gender_raw_text="M",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="999Z", id_type="US_PA_PBPP"
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code="US_PA", race=Race.ASIAN, race_raw_text="A"
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="111A", id_type="US_PA_PBPP"
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code="US_PA", race=Race.WHITE, race_raw_text="W"
                    )
                ],
            ),
        ]

        self._run_parse_ingest_view_test("dbo_Offender_v2", expected_output)

    def test_parse_dbo_Miscon(self) -> None:
        expected_output = [
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="445566", id_type="US_PA_CONT"
                    )
                ],
                incarceration_incidents=[
                    StateIncarcerationIncident(
                        external_id="A123456",
                        state_code="US_PA",
                        incident_type=StateIncarcerationIncidentType.REPORT,
                        incident_date=datetime.date(2018, 5, 10),
                        facility="WAM",
                        incident_details='{"CATEGORY_1": "", "CATEGORY_2": "", "CATEGORY_3": "", "CATEGORY_4": "", "CATEGORY_5": ""}',
                        incarceration_incident_outcomes=[
                            StateIncarcerationIncidentOutcome(
                                external_id="A123456",
                                state_code="US_PA",
                                outcome_type=StateIncarcerationIncidentOutcomeType.RESTRICTED_CONFINEMENT,
                                outcome_type_raw_text="Y",
                                date_effective=datetime.date(2018, 5, 17),
                                report_date=datetime.date(2018, 5, 16),
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="778899", id_type="US_PA_CONT"
                    )
                ],
                incarceration_incidents=[
                    StateIncarcerationIncident(
                        external_id="A234567",
                        state_code="US_PA",
                        incident_type=StateIncarcerationIncidentType.REPORT,
                        incident_date=datetime.date(1991, 3, 6),
                        facility="GRA",
                        location_within_facility="CELL-AA UNIT",
                        incident_details='{"CATEGORY_1": "", "CATEGORY_2": "X", "CATEGORY_3": "X", "CATEGORY_4": "", "CATEGORY_5": ""}',
                        incarceration_incident_outcomes=[
                            StateIncarcerationIncidentOutcome(
                                external_id="A234567",
                                state_code="US_PA",
                                outcome_type=StateIncarcerationIncidentOutcomeType.CELL_CONFINEMENT,
                                outcome_type_raw_text="C",
                                date_effective=datetime.date(1991, 3, 8),
                                hearing_date=datetime.date(1991, 3, 6),
                                report_date=datetime.date(1991, 3, 7),
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="778899", id_type="US_PA_CONT"
                    )
                ],
                incarceration_incidents=[
                    StateIncarcerationIncident(
                        external_id="B222333",
                        state_code="US_PA",
                        incident_type=StateIncarcerationIncidentType.REPORT,
                        incident_date=datetime.date(1993, 7, 6),
                        facility="SMI",
                        incident_details='{"CATEGORY_1": "", "CATEGORY_2": "", "CATEGORY_3": "", "CATEGORY_4": "", "CATEGORY_5": ""}',
                        incarceration_incident_outcomes=[
                            StateIncarcerationIncidentOutcome(
                                external_id="B222333",
                                state_code="US_PA",
                                outcome_type=StateIncarcerationIncidentOutcomeType.RESTRICTED_CONFINEMENT,
                                outcome_type_raw_text="Y",
                                date_effective=datetime.date(1993, 7, 6),
                                report_date=datetime.date(1993, 7, 6),
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="778899", id_type="US_PA_CONT"
                    )
                ],
                incarceration_incidents=[
                    StateIncarcerationIncident(
                        external_id="B444555",
                        state_code="US_PA",
                        incident_type=StateIncarcerationIncidentType.CONTRABAND,
                        incident_date=datetime.date(1993, 12, 17),
                        facility="SMI",
                        location_within_facility="RHU-A 200",
                        incident_details='{"CATEGORY_1": "", "CATEGORY_2": "X", "CATEGORY_3": "", "CATEGORY_4": "", "CATEGORY_5": ""}',
                        incarceration_incident_outcomes=[
                            StateIncarcerationIncidentOutcome(
                                external_id="B444555",
                                state_code="US_PA",
                                outcome_type_raw_text="N",
                                hearing_date=datetime.date(1993, 12, 18),
                                report_date=datetime.date(1993, 12, 17),
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="778899", id_type="US_PA_CONT"
                    )
                ],
                incarceration_incidents=[
                    StateIncarcerationIncident(
                        external_id="B444556",
                        state_code="US_PA",
                        incident_type=StateIncarcerationIncidentType.CONTRABAND,
                        incident_date=datetime.date(1993, 12, 17),
                        facility="SMI",
                        location_within_facility="RHU-A 200",
                        incident_details='{"CATEGORY_1": "", "CATEGORY_2": "X", "CATEGORY_3": "", "CATEGORY_4": "", "CATEGORY_5": ""}',
                        incarceration_incident_outcomes=[
                            StateIncarcerationIncidentOutcome(
                                external_id="B444556",
                                state_code="US_PA",
                                outcome_type_raw_text="N",
                            )
                        ],
                    )
                ],
            ),
        ]

        self._run_parse_ingest_view_test("dbo_Miscon", expected_output)

    def test_parse_supervision_violation(self) -> None:
        expected_output = [
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="456B", id_type="US_PA_PBPP"
                    )
                ],
                supervision_violations=[
                    StateSupervisionViolation(
                        external_id="456B-1-1",
                        state_code="US_PA",
                        violation_date=datetime.date(2014, 1, 1),
                        supervision_violation_types=[
                            StateSupervisionViolationTypeEntry(
                                state_code="US_PA",
                                violation_type=StateSupervisionViolationType.TECHNICAL,
                                violation_type_raw_text="H10",
                            ),
                            StateSupervisionViolationTypeEntry(
                                state_code="US_PA",
                                violation_type=StateSupervisionViolationType.TECHNICAL,
                                violation_type_raw_text="L03",
                            ),
                            StateSupervisionViolationTypeEntry(
                                state_code="US_PA",
                                violation_type=StateSupervisionViolationType.TECHNICAL,
                                violation_type_raw_text="L05",
                            ),
                            StateSupervisionViolationTypeEntry(
                                state_code="US_PA",
                                violation_type=StateSupervisionViolationType.TECHNICAL,
                                violation_type_raw_text="M01",
                            ),
                            StateSupervisionViolationTypeEntry(
                                state_code="US_PA",
                                violation_type=StateSupervisionViolationType.TECHNICAL,
                                violation_type_raw_text="M02",
                            ),
                            StateSupervisionViolationTypeEntry(
                                state_code="US_PA",
                                violation_type=StateSupervisionViolationType.TECHNICAL,
                                violation_type_raw_text="H03",
                            ),
                            StateSupervisionViolationTypeEntry(
                                state_code="US_PA",
                                violation_type=StateSupervisionViolationType.TECHNICAL,
                                violation_type_raw_text="H07",
                            ),
                        ],
                        supervision_violated_conditions=[
                            StateSupervisionViolatedConditionEntry(
                                state_code="US_PA",
                                condition="5",
                            ),
                            StateSupervisionViolatedConditionEntry(
                                state_code="US_PA",
                                condition="7",
                            ),
                            StateSupervisionViolatedConditionEntry(
                                state_code="US_PA",
                                condition="7",
                            ),
                            StateSupervisionViolatedConditionEntry(
                                state_code="US_PA",
                                condition="3",
                            ),
                            StateSupervisionViolatedConditionEntry(
                                state_code="US_PA",
                                condition="3",
                            ),
                            StateSupervisionViolatedConditionEntry(
                                state_code="US_PA",
                                condition="7",
                            ),
                            StateSupervisionViolatedConditionEntry(
                                state_code="US_PA",
                                condition="7",
                            ),
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="456B", id_type="US_PA_PBPP"
                    )
                ],
                supervision_violations=[
                    StateSupervisionViolation(
                        external_id="456B-2-1",
                        state_code="US_PA",
                        violation_date=datetime.date(2015, 4, 13),
                        supervision_violation_types=[
                            StateSupervisionViolationTypeEntry(
                                state_code="US_PA",
                                violation_type=StateSupervisionViolationType.TECHNICAL,
                                violation_type_raw_text="H08",
                            )
                        ],
                        supervision_violated_conditions=[
                            StateSupervisionViolatedConditionEntry(
                                state_code="US_PA",
                                condition="5",
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="789C", id_type="US_PA_PBPP"
                    )
                ],
                supervision_violations=[
                    StateSupervisionViolation(
                        external_id="789C-3-1",
                        state_code="US_PA",
                        violation_date=datetime.date(2006, 8, 11),
                        supervision_violation_types=[
                            StateSupervisionViolationTypeEntry(
                                state_code="US_PA",
                                violation_type=StateSupervisionViolationType.TECHNICAL,
                                violation_type_raw_text="H12",
                            ),
                            StateSupervisionViolationTypeEntry(
                                state_code="US_PA",
                                violation_type=StateSupervisionViolationType.LAW,
                                violation_type_raw_text="H04",
                            ),
                        ],
                        supervision_violated_conditions=[
                            StateSupervisionViolatedConditionEntry(
                                state_code="US_PA",
                                condition="5",
                            ),
                            StateSupervisionViolatedConditionEntry(
                                state_code="US_PA",
                                condition="7",
                            ),
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="345E", id_type="US_PA_PBPP"
                    )
                ],
                supervision_violations=[
                    StateSupervisionViolation(
                        external_id="345E-1-1",
                        state_code="US_PA",
                        violation_date=datetime.date(2018, 3, 17),
                        supervision_violation_types=[
                            StateSupervisionViolationTypeEntry(
                                state_code="US_PA",
                                violation_type=StateSupervisionViolationType.TECHNICAL,
                                violation_type_raw_text="L08",
                            )
                        ],
                        supervision_violated_conditions=[
                            StateSupervisionViolatedConditionEntry(
                                state_code="US_PA",
                                condition="5",
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="345E", id_type="US_PA_PBPP"
                    )
                ],
                supervision_violations=[
                    StateSupervisionViolation(
                        external_id="345E-1-2",
                        state_code="US_PA",
                        violation_date=datetime.date(2018, 5, 12),
                        supervision_violation_types=[
                            StateSupervisionViolationTypeEntry(
                                state_code="US_PA",
                                violation_type=StateSupervisionViolationType.LAW,
                                violation_type_raw_text="M13",
                            ),
                            StateSupervisionViolationTypeEntry(
                                state_code="US_PA",
                                violation_type=StateSupervisionViolationType.TECHNICAL,
                                violation_type_raw_text="M14",
                            ),
                        ],
                        supervision_violated_conditions=[
                            StateSupervisionViolatedConditionEntry(
                                state_code="US_PA",
                                condition="4",
                            ),
                            StateSupervisionViolatedConditionEntry(
                                state_code="US_PA",
                                condition="7",
                            ),
                        ],
                    )
                ],
            ),
        ]

        self._run_parse_ingest_view_test("supervision_violation", expected_output)

    def test_parse_supervision_violation_response(self) -> None:
        expected_output = [
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA",
                        external_id="456B",
                        id_type="US_PA_PBPP",
                    )
                ],
                supervision_violations=[
                    StateSupervisionViolation(
                        external_id="456B-1-1",
                        state_code="US_PA",
                        supervision_violation_responses=[
                            StateSupervisionViolationResponse(
                                external_id="456B-1-1",
                                state_code="US_PA",
                                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                                response_date=datetime.date(2013, 1, 2),
                                supervision_violation_response_decisions=[
                                    StateSupervisionViolationResponseDecisionEntry(
                                        state_code="US_PA",
                                        decision=StateSupervisionViolationResponseDecision.WARNING,
                                        decision_raw_text="WTWR",
                                    ),
                                    StateSupervisionViolationResponseDecisionEntry(
                                        state_code="US_PA",
                                        decision=StateSupervisionViolationResponseDecision.NEW_CONDITIONS,
                                        decision_raw_text="DJBS",
                                    ),
                                ],
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA",
                        external_id="456B",
                        id_type="US_PA_PBPP",
                    )
                ],
                supervision_violations=[
                    StateSupervisionViolation(
                        external_id="456B-2-1",
                        state_code="US_PA",
                        supervision_violation_responses=[
                            StateSupervisionViolationResponse(
                                external_id="456B-2-1",
                                state_code="US_PA",
                                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                                response_date=datetime.date(2015, 4, 13),
                                supervision_violation_response_decisions=[
                                    StateSupervisionViolationResponseDecisionEntry(
                                        state_code="US_PA",
                                        decision=StateSupervisionViolationResponseDecision.REVOCATION,
                                        decision_raw_text="VCCF",
                                    ),
                                    StateSupervisionViolationResponseDecisionEntry(
                                        state_code="US_PA",
                                        decision=StateSupervisionViolationResponseDecision.REVOCATION,
                                        decision_raw_text="ARR2",
                                    ),
                                ],
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA",
                        external_id="789C",
                        id_type="US_PA_PBPP",
                    )
                ],
                supervision_violations=[
                    StateSupervisionViolation(
                        external_id="789C-3-1",
                        state_code="US_PA",
                        supervision_violation_responses=[
                            StateSupervisionViolationResponse(
                                external_id="789C-3-1",
                                state_code="US_PA",
                                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                                response_date=datetime.date(2006, 8, 16),
                                supervision_violation_response_decisions=[
                                    StateSupervisionViolationResponseDecisionEntry(
                                        state_code="US_PA",
                                        decision=StateSupervisionViolationResponseDecision.REVOCATION,
                                        decision_raw_text="VCCF",
                                    ),
                                    StateSupervisionViolationResponseDecisionEntry(
                                        state_code="US_PA",
                                        decision=StateSupervisionViolationResponseDecision.REVOCATION,
                                        decision_raw_text="ARR2",
                                    ),
                                ],
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA",
                        external_id="345E",
                        id_type="US_PA_PBPP",
                    )
                ],
                supervision_violations=[
                    StateSupervisionViolation(
                        external_id="345E-1-1",
                        state_code="US_PA",
                        supervision_violation_responses=[
                            StateSupervisionViolationResponse(
                                external_id="345E-1-1",
                                state_code="US_PA",
                                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                                response_date=datetime.date(2018, 3, 23),
                                supervision_violation_response_decisions=[
                                    StateSupervisionViolationResponseDecisionEntry(
                                        state_code="US_PA",
                                        decision=StateSupervisionViolationResponseDecision.WARNING,
                                        decision_raw_text="WTWR",
                                    )
                                ],
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA",
                        external_id="345E",
                        id_type="US_PA_PBPP",
                    )
                ],
                supervision_violations=[
                    StateSupervisionViolation(
                        external_id="345E-1-2",
                        state_code="US_PA",
                        supervision_violation_responses=[
                            StateSupervisionViolationResponse(
                                external_id="345E-1-2",
                                state_code="US_PA",
                                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                                response_date=datetime.date(2018, 5, 13),
                                supervision_violation_response_decisions=[
                                    StateSupervisionViolationResponseDecisionEntry(
                                        state_code="US_PA",
                                        decision=StateSupervisionViolationResponseDecision.REVOCATION,
                                        decision_raw_text="ARR2",
                                    ),
                                    StateSupervisionViolationResponseDecisionEntry(
                                        state_code="US_PA",
                                        decision=StateSupervisionViolationResponseDecision.INTERNAL_UNKNOWN,
                                        decision_raw_text="PV01",
                                    ),
                                ],
                            )
                        ],
                    )
                ],
            ),
        ]

        self._run_parse_ingest_view_test(
            "supervision_violation_response", expected_output
        )

    def test_parse_board_action(self) -> None:
        expected_output = [
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA",
                        external_id="456B",
                        id_type="US_PA_PBPP",
                    )
                ],
                supervision_violations=[
                    StateSupervisionViolation(
                        state_code="US_PA",
                        supervision_violation_responses=[
                            StateSupervisionViolationResponse(
                                external_id="BOARD-456B-0-04",
                                state_code="US_PA",
                                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
                                response_date=datetime.date(2014, 2, 24),
                                deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
                                supervision_violation_response_decisions=[
                                    StateSupervisionViolationResponseDecisionEntry(
                                        state_code="US_PA",
                                        decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
                                        decision_raw_text="RESCR9",
                                    )
                                ],
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA",
                        external_id="789C",
                        id_type="US_PA_PBPP",
                    )
                ],
                supervision_violations=[
                    StateSupervisionViolation(
                        state_code="US_PA",
                        supervision_violation_responses=[
                            StateSupervisionViolationResponse(
                                external_id="BOARD-789C-0-02",
                                state_code="US_PA",
                                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
                                response_date=datetime.date(2014, 7, 9),
                                deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
                                supervision_violation_response_decisions=[
                                    StateSupervisionViolationResponseDecisionEntry(
                                        state_code="US_PA",
                                        decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
                                        decision_raw_text="RESCR9",
                                    )
                                ],
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA",
                        external_id="123A",
                        id_type="US_PA_PBPP",
                    )
                ],
                supervision_violations=[
                    StateSupervisionViolation(
                        state_code="US_PA",
                        supervision_violation_responses=[
                            StateSupervisionViolationResponse(
                                external_id="BOARD-123A-1-09",
                                state_code="US_PA",
                                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
                                response_date=datetime.date(2004, 6, 16),
                                deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
                                supervision_violation_response_decisions=[
                                    StateSupervisionViolationResponseDecisionEntry(
                                        state_code="US_PA",
                                        decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
                                        decision_raw_text="RESCR",
                                    )
                                ],
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA",
                        external_id="345E",
                        id_type="US_PA_PBPP",
                    )
                ],
                supervision_violations=[
                    StateSupervisionViolation(
                        state_code="US_PA",
                        supervision_violation_responses=[
                            StateSupervisionViolationResponse(
                                external_id="BOARD-345E-3-11",
                                state_code="US_PA",
                                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
                                response_date=datetime.date(2006, 2, 21),
                                deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
                                supervision_violation_response_decisions=[
                                    StateSupervisionViolationResponseDecisionEntry(
                                        state_code="US_PA",
                                        decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
                                        decision_raw_text="RESCR",
                                    )
                                ],
                            )
                        ],
                    )
                ],
            ),
        ]

        self._run_parse_ingest_view_test("board_action", expected_output)

    def test_parse_dbo_Senrec_v2(self) -> None:
        expected_output = [
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="AB7413", id_type=US_PA_INMATE
                    ),
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        state_code="US_PA",
                        external_id="AB7413",
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        incarceration_sentences=[
                            StateIncarcerationSentence(
                                state_code="US_PA",
                                external_id="AB7413-01",
                                status=StateSentenceStatus.COMPLETED,
                                status_raw_text="SC",
                                incarceration_type=StateIncarcerationType.STATE_PRISON,
                                incarceration_type_raw_text="S",
                                county_code="PHI",
                                date_imposed=datetime.date(2008, 8, 15),
                                start_date=datetime.date(2008, 8, 15),
                                completion_date=datetime.date(2009, 1, 4),
                                projected_min_release_date=datetime.date(2007, 7, 4),
                                projected_max_release_date=datetime.date(2009, 1, 4),
                                min_length_days=549,
                                max_length_days=1095,
                                is_life=False,
                                is_capital_punishment=False,
                                charges=[
                                    StateCharge(
                                        state_code="US_PA",
                                        external_id="N7825555",
                                        statute="CC3502",
                                        status=ChargeStatus.PRESENT_WITHOUT_INFO,
                                        description="BURGLARY (GENERAL)",
                                        offense_type="BURGLARY-VIOLENT-PROPERTY",
                                        classification_type=StateChargeClassificationType.FELONY,
                                        classification_type_raw_text="FELONY",
                                        classification_subtype="F1",
                                        is_violent=True,
                                    )
                                ],
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="GF3374", id_type=US_PA_INMATE
                    ),
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        external_id="GF3374",
                        state_code="US_PA",
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        incarceration_sentences=[
                            StateIncarcerationSentence(
                                state_code="US_PA",
                                external_id="GF3374-01",
                                status=StateSentenceStatus.SERVING,
                                status_raw_text="AS",
                                incarceration_type=StateIncarcerationType.STATE_PRISON,
                                incarceration_type_raw_text="S",
                                county_code="PHI",
                                date_imposed=datetime.date(2008, 8, 16),
                                projected_min_release_date=datetime.date(2020, 1, 14),
                                projected_max_release_date=datetime.date(2022, 4, 14),
                                min_length_days=4287,
                                max_length_days=5113,
                                is_life=False,
                                is_capital_punishment=False,
                                charges=[
                                    StateCharge(
                                        state_code="US_PA",
                                        external_id="U1196666",
                                        statute="CC4101",
                                        description="FORGERY",
                                        offense_type="FORGERY",
                                        status=ChargeStatus.PRESENT_WITHOUT_INFO,
                                        classification_type=StateChargeClassificationType.FELONY,
                                        classification_type_raw_text="FELONY",
                                        classification_subtype="F2",
                                        is_violent=False,
                                    )
                                ],
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="CJ1991", id_type=US_PA_INMATE
                    ),
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        external_id="CJ1991",
                        state_code="US_PA",
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        incarceration_sentences=[
                            StateIncarcerationSentence(
                                state_code="US_PA",
                                external_id="CJ1991-01",
                                status=StateSentenceStatus.SERVING,
                                status_raw_text="AS",
                                incarceration_type=StateIncarcerationType.STATE_PRISON,
                                incarceration_type_raw_text="S",
                                county_code="BUC",
                                date_imposed=datetime.date(2016, 8, 20),
                                start_date=datetime.date(2016, 8, 20),
                                projected_min_release_date=datetime.date(2017, 7, 5),
                                projected_max_release_date=datetime.date(2018, 10, 5),
                                min_length_days=457,
                                max_length_days=914,
                                is_life=False,
                                is_capital_punishment=False,
                                charges=[
                                    StateCharge(
                                        state_code="US_PA",
                                        external_id="L3947777",
                                        statute="CC2502B",
                                        description="MURDER (2ND DEGREE)",
                                        offense_type="MURDER 2-HOMICIDE",
                                        status=ChargeStatus.PRESENT_WITHOUT_INFO,
                                        classification_type=StateChargeClassificationType.FELONY,
                                        classification_type_raw_text="FELONY",
                                        classification_subtype="F",
                                        is_violent=True,
                                    )
                                ],
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="JE1989", id_type=US_PA_INMATE
                    ),
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        external_id="JE1989",
                        state_code="US_PA",
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        incarceration_sentences=[
                            StateIncarcerationSentence(
                                state_code="US_PA",
                                external_id="JE1989-01",
                                status=StateSentenceStatus.SERVING,
                                status_raw_text="AS",
                                incarceration_type=StateIncarcerationType.STATE_PRISON,
                                incarceration_type_raw_text="S",
                                county_code="BUC",
                                date_imposed=datetime.date(2016, 8, 20),
                                start_date=datetime.date(2016, 8, 20),
                                projected_min_release_date=datetime.date(2017, 10, 22),
                                projected_max_release_date=datetime.date(2019, 4, 22),
                                max_length_days=1095,
                                is_life=False,
                                is_capital_punishment=False,
                                charges=[
                                    StateCharge(
                                        state_code="US_PA",
                                        external_id="L7858888",
                                        statute="XX0500",
                                        description="THEFT",
                                        offense_type="THEFT",
                                        status=ChargeStatus.PRESENT_WITHOUT_INFO,
                                        classification_type=StateChargeClassificationType.MISDEMEANOR,
                                        classification_type_raw_text="MISDEMEANOR",
                                        classification_subtype="M1",
                                        is_violent=False,
                                    )
                                ],
                            ),
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="JE1989", id_type=US_PA_INMATE
                    ),
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        external_id="JE1989",
                        state_code="US_PA",
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        incarceration_sentences=[
                            StateIncarcerationSentence(
                                state_code="US_PA",
                                external_id="JE1989-02",
                                status=StateSentenceStatus.SERVING,
                                status_raw_text="AS",
                                incarceration_type=StateIncarcerationType.STATE_PRISON,
                                incarceration_type_raw_text="S",
                                county_code="BUC",
                                date_imposed=datetime.date(2016, 8, 20),
                                start_date=datetime.date(2016, 8, 20),
                                projected_min_release_date=datetime.date(2017, 10, 22),
                                projected_max_release_date=datetime.date(2019, 4, 22),
                                min_length_days=549,
                                is_life=False,
                                is_capital_punishment=False,
                                charges=[
                                    StateCharge(
                                        state_code="US_PA",
                                        external_id="L7858890",
                                        statute="CC2702A1",
                                        description="AGGRAVATED ASSAULT W/SERIOUS BODILY INJURY AGAINST ELDERLY/YOUNG PERSON",
                                        offense_type="AGGRAVATED ASSAULT-ASSAULT",
                                        status=ChargeStatus.PRESENT_WITHOUT_INFO,
                                        classification_type=StateChargeClassificationType.FELONY,
                                        classification_type_raw_text="FELONY",
                                        classification_subtype="F1",
                                        is_violent=True,
                                    )
                                ],
                            ),
                        ],
                    ),
                ],
            ),
        ]

        self._run_parse_ingest_view_test("dbo_Senrec_v2", expected_output)

    def test_parse_dbo_Senrec_v2_SentenceStatuses(self) -> None:
        manifest_ast = self._parse_manifest("dbo_Senrec_v2")
        enum_parser_manifest = (
            manifest_ast.field_manifests["sentence_groups"]
            .child_manifests[0]  # type: ignore[attr-defined]
            .field_manifests["incarceration_sentences"]
            .child_manifests[0]  # type: ignore[attr-defined]
            .field_manifests["status"]
        )
        self._parse_enum_manifest_test("sentence_status_raw_text", enum_parser_manifest)

    def test_parse_dbo_Senrec_v2_IncarcerationType(self) -> None:
        manifest_ast = self._parse_manifest("dbo_Senrec_v2")
        enum_parser_manifest = (
            manifest_ast.field_manifests["sentence_groups"]
            .child_manifests[0]  # type: ignore[attr-defined]
            .field_manifests["incarceration_sentences"]
            .child_manifests[0]  # type: ignore[attr-defined]
            .field_manifests["incarceration_type"]
        )
        self._parse_enum_manifest_test(
            "incarceration_type_raw_text", enum_parser_manifest
        )
