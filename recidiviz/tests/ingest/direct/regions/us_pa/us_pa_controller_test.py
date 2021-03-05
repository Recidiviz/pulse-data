# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""Unit and integration tests for Pennsylvania direct ingest."""
import datetime
import json
from typing import Type

from recidiviz import IngestInfo
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.person_characteristics import (
    Gender,
    Race,
    Ethnicity,
    ResidencyStatus,
)
from recidiviz.common.constants.state.external_id_types import (
    US_PA_CONTROL,
    US_PA_SID,
    US_PA_PBPP,
)
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentType,
    StateAssessmentLevel,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_incident import (
    StateIncarcerationIncidentType,
    StateIncarcerationIncidentOutcomeType,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodStatus,
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_person_alias import StatePersonAliasType
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodStatus,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodTerminationReason,
    StateSupervisionLevel,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseType,
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseRevocationType,
    StateSupervisionViolationResponseDecidingBodyType,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import (
    GcsfsDirectIngestController,
)
from recidiviz.ingest.direct.regions.us_pa.us_pa_controller import UsPaController
from recidiviz.ingest.models.ingest_info import (
    StatePerson,
    StatePersonExternalId,
    StatePersonRace,
    StateAlias,
    StatePersonEthnicity,
    StateAssessment,
    StateSentenceGroup,
    StateIncarcerationSentence,
    StateCharge,
    StateAgent,
    StateIncarcerationPeriod,
    StateIncarcerationIncident,
    StateIncarcerationIncidentOutcome,
    StateSupervisionSentence,
    StateSupervisionPeriod,
    StateSupervisionViolation,
    StateSupervisionViolationTypeEntry,
    StateSupervisionViolatedConditionEntry,
    StateSupervisionViolationResponse,
    StateSupervisionViolationResponseDecisionEntry,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.entity.state import entities
from recidiviz.tests.ingest.direct.regions.base_direct_ingest_controller_tests import (
    BaseDirectIngestControllerTests,
)
from recidiviz.tests.ingest.direct.regions.utils import populate_person_backedges

_STATE_CODE_UPPER = "US_PA"


class TestUsPaController(BaseDirectIngestControllerTests):
    """Unit tests for each Idaho file to be ingested by the UsNdController."""

    @classmethod
    def region_code(cls) -> str:
        return _STATE_CODE_UPPER.lower()

    @classmethod
    def controller_cls(cls) -> Type[GcsfsDirectIngestController]:
        return UsPaController

    @classmethod
    def schema_type(cls) -> SchemaType:
        return SchemaType.STATE

    def test_populate_data_person_external_ids(self) -> None:
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="RECIDIVIZ_MASTER_STATE_ID_12345678",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="12345678", id_type=US_PA_SID
                        ),
                        StatePersonExternalId(
                            state_person_external_id_id="123456", id_type=US_PA_CONTROL
                        ),
                        StatePersonExternalId(
                            state_person_external_id_id="123A", id_type=US_PA_PBPP
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="RECIDIVIZ_MASTER_STATE_ID_55554444",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="55554444", id_type=US_PA_SID
                        ),
                        StatePersonExternalId(
                            state_person_external_id_id="66665555", id_type=US_PA_SID
                        ),
                        StatePersonExternalId(
                            state_person_external_id_id="654321", id_type=US_PA_CONTROL
                        ),
                        StatePersonExternalId(
                            state_person_external_id_id="456B", id_type=US_PA_PBPP
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="RECIDIVIZ_MASTER_CONTROL_NUMBER_445566",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="445566", id_type=US_PA_CONTROL
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="RECIDIVIZ_MASTER_STATE_ID_09876543",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="09876543", id_type=US_PA_SID
                        ),
                        StatePersonExternalId(
                            state_person_external_id_id="778899", id_type=US_PA_CONTROL
                        ),
                        StatePersonExternalId(
                            state_person_external_id_id="889900", id_type=US_PA_CONTROL
                        ),
                        StatePersonExternalId(
                            state_person_external_id_id="345E", id_type=US_PA_PBPP
                        ),
                        StatePersonExternalId(
                            state_person_external_id_id="999Z", id_type=US_PA_PBPP
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="RECIDIVIZ_MASTER_PAROLE_NUMBER_789C",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="789C", id_type=US_PA_PBPP
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="RECIDIVIZ_MASTER_STATE_ID_888888888",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="888888888", id_type=US_PA_SID
                        ),
                        StatePersonExternalId(
                            state_person_external_id_id="888P", id_type=US_PA_PBPP
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="RECIDIVIZ_MASTER_STATE_ID_9999999",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="9999999", id_type=US_PA_SID
                        ),
                        StatePersonExternalId(
                            state_person_external_id_id="090909", id_type=US_PA_CONTROL
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="RECIDIVIZ_MASTER_STATE_ID_66666666",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="66666666", id_type=US_PA_SID
                        ),
                        StatePersonExternalId(
                            state_person_external_id_id="66666667", id_type=US_PA_SID
                        ),
                        StatePersonExternalId(
                            state_person_external_id_id="060606", id_type=US_PA_CONTROL
                        ),
                        StatePersonExternalId(
                            state_person_external_id_id="060607", id_type=US_PA_CONTROL
                        ),
                        StatePersonExternalId(
                            state_person_external_id_id="666P", id_type=US_PA_PBPP
                        ),
                        StatePersonExternalId(
                            state_person_external_id_id="777M", id_type=US_PA_PBPP
                        ),
                    ],
                ),
            ]
        )

        self.run_parse_file_test(expected, "person_external_ids")

    def test_populate_data_doc_person_info(self) -> None:
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="123456",
                    surname="RUSSELL",
                    given_names="BERTRAND",
                    gender="MALE",
                    birthdate="19760318",
                    current_address="123 Easy Street, PITTSBURGH, PA 16161",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="123456", id_type=US_PA_CONTROL
                        ),
                    ],
                    state_person_races=[StatePersonRace(race="BLACK")],
                    state_aliases=[
                        StateAlias(
                            surname="RUSSELL",
                            given_names="BERTRAND",
                            alias_type="GIVEN_NAME",
                        )
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(state_sentence_group_id="AB7413"),
                        StateSentenceGroup(state_sentence_group_id="BC8524"),
                    ],
                ),
                StatePerson(
                    state_person_id="654321",
                    surname="SARTRE",
                    given_names="JEAN-PAUL",
                    gender="MALE",
                    birthdate="19821002",
                    current_address="555 FLATBUSH DR, NEW YORK, NY 10031",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="654321", id_type=US_PA_CONTROL
                        ),
                    ],
                    state_person_races=[StatePersonRace(race="BLACK")],
                    state_aliases=[
                        StateAlias(
                            surname="SARTRE",
                            given_names="JEAN-PAUL",
                            alias_type="GIVEN_NAME",
                        )
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(state_sentence_group_id="GF3374"),
                    ],
                ),
                StatePerson(
                    state_person_id="445566",
                    surname="KIERKEGAARD",
                    given_names="SOREN",
                    name_suffix="JR",
                    gender="FEMALE",
                    birthdate="19911120",
                    current_address="5000 SUNNY LANE, APT. 55D, PHILADELPHIA, PA 19129",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="445566", id_type=US_PA_CONTROL
                        ),
                    ],
                    state_person_races=[StatePersonRace(race="WHITE")],
                    state_aliases=[
                        StateAlias(
                            surname="KIERKEGAARD",
                            given_names="SOREN",
                            name_suffix="JR",
                            alias_type="GIVEN_NAME",
                        )
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(state_sentence_group_id="CJ1991"),
                    ],
                ),
                StatePerson(
                    state_person_id="778899",
                    surname="RAWLS",
                    given_names="JOHN",
                    gender="MALE",
                    birthdate="19890617",
                    current_address="214 HAPPY PLACE, PHILADELPHIA, PA 19129",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="778899", id_type=US_PA_CONTROL
                        ),
                    ],
                    state_person_ethnicities=[
                        StatePersonEthnicity(ethnicity="HISPANIC")
                    ],
                    state_aliases=[
                        StateAlias(
                            surname="RAWLS", given_names="JOHN", alias_type="GIVEN_NAME"
                        )
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(state_sentence_group_id="JE1989"),
                    ],
                ),
            ]
        )

        self.run_parse_file_test(expected, "doc_person_info_v2")

    def test_populate_data_dbo_tblInmTestScore(self) -> None:
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="123456",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="123456", id_type=US_PA_CONTROL
                        ),
                    ],
                    state_assessments=[
                        StateAssessment(
                            state_assessment_id="123456-AB7413-1-1",
                            assessment_type="CSS-M                                             ",
                            assessment_class="SOCIAL",
                            assessment_date="6/22/2008 13:20:54",
                            assessment_score="19",
                        ),
                        StateAssessment(
                            state_assessment_id="123456-BC8524-1-1",
                            assessment_type="CSS-M                                             ",
                            assessment_class="SOCIAL",
                            assessment_date="1/1/2010 01:01:01",
                            assessment_score="11",
                        ),
                        StateAssessment(
                            state_assessment_id="123456-AB7413-2-1",
                            assessment_type="HIQ                                               ",
                            assessment_class="SOCIAL",
                            assessment_date="7/12/2004 8:23:28",
                            assessment_score="62",
                        ),
                        StateAssessment(
                            state_assessment_id="123456-AB7413-3-3",
                            assessment_type="LSI-R                                             ",
                            assessment_class="RISK",
                            assessment_date="10/3/2010 12:11:41",
                            assessment_score="27",
                            assessment_level=StateAssessmentLevel.HIGH.value,
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="654321",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="654321", id_type=US_PA_CONTROL
                        ),
                    ],
                    state_assessments=[
                        StateAssessment(
                            state_assessment_id="654321-GF3374-1-1",
                            assessment_type="CSS-M                                             ",
                            assessment_class="SOCIAL",
                            assessment_date="4/1/2003 11:42:17",
                            assessment_score="22",
                        ),
                        StateAssessment(
                            state_assessment_id="654321-GF3374-3-1",
                            assessment_type="LSI-R                                             ",
                            assessment_class="RISK",
                            assessment_date="6/8/2004 11:07:48",
                            assessment_score="19",
                            assessment_level=StateAssessmentLevel.LOW.value,
                        ),
                        StateAssessment(
                            state_assessment_id="654321-GF3374-4-1",
                            assessment_type="TCU                                               ",
                            assessment_class="SUBSTANCE_ABUSE",
                            assessment_date="1/4/2004 11:09:52",
                            assessment_score="6",
                        ),
                        StateAssessment(
                            state_assessment_id="654321-GF3374-5-1",
                            assessment_type="ST99                                              ",
                            assessment_class="SEX_OFFENSE",
                            assessment_date="7/5/2004 15:30:59",
                            assessment_score="4",
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="445566",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="445566", id_type=US_PA_CONTROL
                        ),
                    ],
                    state_assessments=[
                        StateAssessment(
                            state_assessment_id="445566-CJ1991-2-1",
                            assessment_type="HIQ                                               ",
                            assessment_class="SOCIAL",
                            assessment_date="7/28/2005 10:33:31",
                            assessment_score="61",
                        ),
                        StateAssessment(
                            state_assessment_id="445566-CJ1991-3-2",
                            assessment_type="LSI-R                                             ",
                            assessment_class="RISK",
                            assessment_date="12/19/2016 15:21:56",
                            assessment_score="13",
                            assessment_level=StateAssessmentLevel.LOW.value,
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="778899",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="778899", id_type=US_PA_CONTROL
                        ),
                    ],
                    state_assessments=[
                        StateAssessment(
                            state_assessment_id="778899-JE1989-3-3",
                            assessment_type="LSI-R                                             ",
                            assessment_class="RISK",
                            assessment_date="1/6/2017 18:16:56",
                            assessment_score=None,
                            assessment_level="UNKNOWN (70-REFUSED)",
                        ),
                        StateAssessment(
                            state_assessment_id="778899-JE1989-6-1",
                            assessment_type="RST                                               ",
                            assessment_class="RISK",
                            assessment_date="12/8/2012 15:09:08",
                            assessment_score="9",
                            assessment_metadata=json.dumps({"latest_version": False}),
                        ),
                        StateAssessment(
                            state_assessment_id="778899-JE1989-6-2",
                            assessment_type="RST                                               ",
                            assessment_class="RISK",
                            assessment_date="5/11/2018 15:54:06",
                            assessment_score="7",
                            assessment_metadata=json.dumps({"latest_version": True}),
                        ),
                    ],
                ),
            ]
        )

        self.run_parse_file_test(expected, "dbo_tblInmTestScore")

    def test_populate_data_dbo_Senrec(self) -> None:
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="123456",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="123456", id_type=US_PA_CONTROL
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="AB7413",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="AB7413-01",
                                    status="SC",
                                    incarceration_type="S",
                                    county_code="PHI",
                                    date_imposed="20080815",
                                    start_date="20080815",
                                    completion_date="20090104",
                                    min_length="549",
                                    max_length="1095",
                                    is_life="False",
                                    is_capital_punishment="False",
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id="N7825555",
                                            statute="CC3701A ",
                                        )
                                    ],
                                )
                            ],
                        )
                    ],
                ),
                StatePerson(
                    state_person_id="654321",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="654321", id_type=US_PA_CONTROL
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="GF3374",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="GF3374-01",
                                    status="AS",
                                    incarceration_type="S",
                                    county_code="PHI",
                                    date_imposed="20080816",
                                    start_date="00000000",
                                    completion_date="00000000",
                                    min_length="4287",
                                    max_length="5113",
                                    is_life="False",
                                    is_capital_punishment="False",
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id="U1196666",
                                            statute="CC3701A ",
                                        )
                                    ],
                                )
                            ],
                        )
                    ],
                ),
                StatePerson(
                    state_person_id="445566",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="445566", id_type=US_PA_CONTROL
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="CJ1991",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="CJ1991-01",
                                    status="AS",
                                    incarceration_type="S",
                                    county_code="BUC",
                                    date_imposed="20160820",
                                    start_date="20160820",
                                    completion_date="00000000",
                                    min_length="457",
                                    max_length="914",
                                    is_life="False",
                                    is_capital_punishment="False",
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id="L3947777",
                                            statute="CC6318A1",
                                        )
                                    ],
                                )
                            ],
                        )
                    ],
                ),
                StatePerson(
                    state_person_id="778899",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="778899", id_type=US_PA_CONTROL
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="JE1989",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="JE1989-01",
                                    status="AS",
                                    incarceration_type="S",
                                    county_code="BUC",
                                    date_imposed="20160820",
                                    start_date="20160820",
                                    completion_date="00000000",
                                    max_length="1095",
                                    is_life="False",
                                    is_capital_punishment="False",
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id="L7858888",
                                            statute="CC3503A ",
                                        )
                                    ],
                                ),
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="JE1989-02",
                                    status="AS",
                                    incarceration_type="S",
                                    county_code="BUC",
                                    date_imposed="20160820",
                                    start_date="20160820",
                                    completion_date="00000000",
                                    min_length="549",
                                    is_life="False",
                                    is_capital_punishment="False",
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id="L7858890",
                                            statute="CC3503B ",
                                        )
                                    ],
                                ),
                            ],
                        ),
                    ],
                ),
            ]
        )

        self.run_parse_file_test(expected, "dbo_Senrec")

    def test_populate_data_ccis_incarceration_period(self) -> None:
        je1989_incarceration_periods = [
            StateIncarcerationPeriod(
                state_incarceration_period_id="12345",
                admission_date="2016-12-21 00:00:00",
                admission_reason="CCIS-true-INRS",
                release_date="2017-04-26 00:00:00",
                release_reason="CCIS-TRGH",
                facility="136: KINTOCK ERIE",
                incarceration_type="CCIS",
                specialized_purpose_for_incarceration="CCIS-26",
                custodial_authority="26",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="23456",
                admission_date="2017-07-03 00:00:00",
                admission_reason="CCIS-false-TRRC",
                release_date="2018-01-03 00:00:00",
                release_reason="CCIS-DC2P",
                facility="198: CHESTER COUNTY",
                incarceration_type="CCIS",
                specialized_purpose_for_incarceration="CCIS-46",
                custodial_authority="46",
            ),
        ]

        cj1991_incarceration_periods = [
            StateIncarcerationPeriod(
                state_incarceration_period_id="34567",
                admission_date="2014-11-18 00:00:00",
                admission_reason="CCIS-true-INRS",
                release_date="2015-01-20 00:00:00",
                release_reason="CCIS-DC2P",
                facility="231: WERNERSVILLE CCC#30",
                incarceration_type="CCIS",
                specialized_purpose_for_incarceration="CCIS-51",
                custodial_authority="51",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="45678",
                admission_date="2016-10-03 00:00:00",
                admission_reason="CCIS-true-PRCH",
                release_date="2016-11-14 00:00:00",
                release_reason="CCIS-PTST",
                facility="195: LACKAWANNA COUNTY",
                incarceration_type="CCIS",
                specialized_purpose_for_incarceration="CCIS-26",
                custodial_authority="26",
            ),
        ]

        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="778899",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="778899", id_type=US_PA_CONTROL
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="JE1989",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="JE1989-01",
                                    state_incarceration_periods=je1989_incarceration_periods,
                                )
                            ],
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="445566",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="445566", id_type=US_PA_CONTROL
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="CJ1991",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="CJ1991-01",
                                    state_incarceration_periods=cj1991_incarceration_periods,
                                )
                            ],
                        ),
                    ],
                ),
            ]
        )

        self.run_parse_file_test(expected, "ccis_incarceration_period")

    def test_populate_data_sci_incarceration_period(self) -> None:
        gf3374_incarceration_periods = [
            StateIncarcerationPeriod(
                state_incarceration_period_id="GF3374-1",
                admission_date="20081010",
                admission_reason="NA-false-AC",
                facility="PNG",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="NA-P",
                custodial_authority="STATE_PRISON",
            ),
        ]

        cj1991_incarceration_periods = [
            StateIncarcerationPeriod(
                state_incarceration_period_id="CJ1991-1",
                admission_date="20161011",
                release_date="20161022",
                admission_reason="NA-false-AC",
                release_reason="DC-NA-TRN",
                facility="GRA",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="NA-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="CJ1991-2",
                admission_date="20161022",
                release_date="20170602",
                admission_reason="NA-false-TRN",
                release_reason="AS-NA-TRN",
                facility="CAM",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="NA-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="CJ1991-4",
                admission_date="20170602",
                release_date="20170714",
                admission_reason="NA-false-TRN",
                release_reason="P-SP-D",
                facility="WAM",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="NA-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="CJ1991-6",
                admission_date="20180310",
                release_date="20180401",
                admission_reason="PVP-false-APV",
                release_reason="AS-PVP-TRN",
                facility="DAL",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="PVP-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="CJ1991-7",
                admission_date="20180401",
                release_date="20180723",
                admission_reason="PVP-false-TRN",
                release_reason="AS-TPV-SC",
                facility="WAM",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="PVP-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="CJ1991-8",
                admission_date="20180723",
                release_date="20180914",
                admission_reason="TPV-true-SC",
                release_reason="P-RP-D",
                facility="WAM",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="TPV-S",
                custodial_authority="STATE_PRISON",
            ),
        ]

        je1977_incarceration_periods = [
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1977-1",
                admission_date="19910305",
                release_date="19910308",
                admission_reason="NA-false-AC",
                release_reason="DC-NA-TRN",
                facility="GRA",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="NA-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1977-2",
                admission_date="19910308",
                release_date="19910425",
                admission_reason="NA-false-TRN",
                release_reason="WT-NA-SC",
                facility="CAM",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="NA-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1977-3",
                admission_date="19910425",
                release_date="19910425",
                admission_reason="NA-false-SC",
                release_reason="AS-NA-SC",
                facility="PHI",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="NA-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1977-4",
                admission_date="19910425",
                release_date="19910430",
                admission_reason="NA-false-SC",
                release_reason="WT-NA-SC",
                facility="GRA",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="NA-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1977-5",
                admission_date="19910430",
                release_date="19910430",
                admission_reason="NA-false-SC",
                release_reason="AS-NA-SC",
                facility="PHI",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="NA-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1977-6",
                admission_date="19910430",
                release_date="19910501",
                admission_reason="NA-false-SC",
                release_reason="WT-NA-SC",
                facility="GRA",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="NA-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1977-7",
                admission_date="19910501",
                release_date="19910501",
                admission_reason="NA-false-SC",
                release_reason="DC-NA-SC",
                facility="PHI",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="NA-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1977-8",
                admission_date="19910501",
                release_date="19911113",
                admission_reason="NA-false-SC",
                release_reason="AS-NA-SC",
                facility="CAM",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="NA-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1977-9",
                admission_date="19911113",
                release_date="19911113",
                admission_reason="NA-false-SC",
                release_reason="AS-NA-TRN",
                facility="CAM",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="NA-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1977-10",
                admission_date="19911113",
                release_date="19940714",
                admission_reason="NA-false-TRN",
                release_reason="P-SP-D",
                facility="SMI",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="NA-S",
                custodial_authority="STATE_PRISON",
            ),
        ]

        je1989_incarceration_periods = [
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1989-12",
                admission_date="20050421",
                release_date="20050614",
                admission_reason="PVP-false-APV",
                release_reason="AS-PVP-TRN",
                facility="GRA",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="PVP-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1989-15",
                admission_date="20050614",
                release_date="20051002",
                admission_reason="PVP-false-TRN",
                release_reason="AS-TCV-SC",
                facility="SMI",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="PVP-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1989-16",
                admission_date="20051002",
                release_date="20070904",
                admission_reason="TCV-true-SC",
                release_reason="P-RP-D",
                facility="SMI",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="TCV-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1989-18",
                admission_date="20080331",
                release_date="20080422",
                admission_reason="PVP-false-APV",
                release_reason="AS-PVP-TRN",
                facility="GRA",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="PVP-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1989-21",
                admission_date="20080422",
                release_date="20080514",
                admission_reason="PVP-false-TRN",
                release_reason="AS-TPV-SC",
                facility="SMI",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="PVP-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1989-22",
                admission_date="20080514",
                release_date="20080819",
                admission_reason="TPV-true-SC",
                release_reason="AS-TPV-TRN",
                facility="SMI",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="TPV-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1989-24",
                admission_date="20080819",
                release_date="20090813",
                admission_reason="TPV-false-TRN",
                release_reason="P-RP-D",
                facility="CHS",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="TPV-S",
                custodial_authority="STATE_PRISON",
            ),
        ]

        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="445566",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="445566", id_type=US_PA_CONTROL
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="CJ1991",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="CJ1991-01",
                                    state_incarceration_periods=cj1991_incarceration_periods,
                                )
                            ],
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="654321",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="654321", id_type=US_PA_CONTROL
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="GF3374",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="GF3374-01",
                                    state_incarceration_periods=gf3374_incarceration_periods,
                                )
                            ],
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="778899",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="778899", id_type=US_PA_CONTROL
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="JE1977",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="JE1977-01",
                                    state_incarceration_periods=je1977_incarceration_periods,
                                )
                            ],
                        ),
                        StateSentenceGroup(
                            state_sentence_group_id="JE1989",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="JE1989-01",
                                    state_incarceration_periods=je1989_incarceration_periods,
                                )
                            ],
                        ),
                    ],
                ),
            ]
        )

        self.run_parse_file_test(expected, "sci_incarceration_period")

    def test_populate_data_dbo_Miscon(self) -> None:
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="445566",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="445566", id_type=US_PA_CONTROL
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_periods=[
                                        StateIncarcerationPeriod(
                                            state_incarceration_incidents=[
                                                StateIncarcerationIncident(
                                                    state_incarceration_incident_id="A123456",
                                                    incident_type="REPORT",
                                                    incident_date="20180510",
                                                    facility="WAM",
                                                    incident_details=json.dumps(
                                                        {
                                                            "category_1": "",
                                                            "category_2": "",
                                                            "category_3": "",
                                                            "category_4": "",
                                                            "category_5": "",
                                                        }
                                                    ),
                                                    state_incarceration_incident_outcomes=[
                                                        StateIncarcerationIncidentOutcome(
                                                            state_incarceration_incident_outcome_id="A123456",
                                                            outcome_type="Y",
                                                            date_effective="20180517",
                                                            report_date="20180516",
                                                            hearing_date="00000000",
                                                        )
                                                    ],
                                                )
                                            ]
                                        )
                                    ]
                                )
                            ]
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="778899",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="778899", id_type=US_PA_CONTROL
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_periods=[
                                        StateIncarcerationPeriod(
                                            state_incarceration_incidents=[
                                                StateIncarcerationIncident(
                                                    state_incarceration_incident_id="A234567",
                                                    incident_type="REPORT",
                                                    incident_date="19910306",
                                                    facility="GRA",
                                                    location_within_facility="CELL-AA UNIT",
                                                    incident_details=json.dumps(
                                                        {
                                                            "category_1": "",
                                                            "category_2": "X",
                                                            "category_3": "X",
                                                            "category_4": "",
                                                            "category_5": "",
                                                        }
                                                    ),
                                                    state_incarceration_incident_outcomes=[
                                                        StateIncarcerationIncidentOutcome(
                                                            state_incarceration_incident_outcome_id="A234567",
                                                            outcome_type="C",
                                                            date_effective="19910308",
                                                            report_date="19910307",
                                                            hearing_date="19910306",
                                                        )
                                                    ],
                                                ),
                                                StateIncarcerationIncident(
                                                    state_incarceration_incident_id="B222333",
                                                    incident_type="REPORT",
                                                    incident_date="19930706",
                                                    facility="SMI",
                                                    incident_details=json.dumps(
                                                        {
                                                            "category_1": "",
                                                            "category_2": "",
                                                            "category_3": "",
                                                            "category_4": "",
                                                            "category_5": "",
                                                        }
                                                    ),
                                                    state_incarceration_incident_outcomes=[
                                                        StateIncarcerationIncidentOutcome(
                                                            state_incarceration_incident_outcome_id="B222333",
                                                            outcome_type="Y",
                                                            date_effective="19930706",
                                                            report_date="19930706",
                                                            hearing_date="00000000",
                                                        )
                                                    ],
                                                ),
                                                StateIncarcerationIncident(
                                                    state_incarceration_incident_id="B444555",
                                                    incident_type="CONTRABAND",
                                                    incident_date="19931217",
                                                    facility="SMI",
                                                    location_within_facility="RHU-A 200",
                                                    incident_details=json.dumps(
                                                        {
                                                            "category_1": "",
                                                            "category_2": "X",
                                                            "category_3": "",
                                                            "category_4": "",
                                                            "category_5": "",
                                                        }
                                                    ),
                                                    state_incarceration_incident_outcomes=[
                                                        StateIncarcerationIncidentOutcome(
                                                            state_incarceration_incident_outcome_id="B444555",
                                                            report_date="19931217",
                                                            hearing_date="19931218",
                                                        )
                                                    ],
                                                ),
                                                StateIncarcerationIncident(
                                                    state_incarceration_incident_id="B444556",
                                                    incident_type="CONTRABAND",
                                                    incident_date="19931217",
                                                    facility="SMI",
                                                    location_within_facility="RHU-A 200",
                                                    incident_details=json.dumps(
                                                        {
                                                            "category_1": "",
                                                            "category_2": "X",
                                                            "category_3": "",
                                                            "category_4": "",
                                                            "category_5": "",
                                                        }
                                                    ),
                                                    state_incarceration_incident_outcomes=[
                                                        StateIncarcerationIncidentOutcome(
                                                            state_incarceration_incident_outcome_id="B444556",
                                                            report_date="0 0 0",
                                                            hearing_date="0 0 0",
                                                        )
                                                    ],
                                                ),
                                            ]
                                        )
                                    ],
                                )
                            ]
                        ),
                    ],
                ),
            ]
        )

        self.run_parse_file_test(expected, "dbo_Miscon_v2")

    def test_populate_data_dbo_Offender(self) -> None:
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="123A",
                    gender="M",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="123A", id_type=US_PA_PBPP
                        ),
                    ],
                    state_person_races=[StatePersonRace(race="B")],
                ),
                StatePerson(
                    state_person_id="456B",
                    gender="M",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="456B", id_type=US_PA_PBPP
                        ),
                    ],
                    state_person_races=[StatePersonRace(race="I")],
                    state_person_ethnicities=[StatePersonEthnicity(ethnicity="H")],
                ),
                StatePerson(
                    state_person_id="789C",
                    gender="F",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="789C", id_type=US_PA_PBPP
                        ),
                    ],
                    state_person_races=[StatePersonRace(race="N")],
                ),
                StatePerson(
                    state_person_id="345E",
                    gender="M",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="345E", id_type=US_PA_PBPP
                        ),
                    ],
                    state_person_races=[StatePersonRace(race="W")],
                ),
                StatePerson(
                    state_person_id="111A",
                    gender="N",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="111A", id_type=US_PA_PBPP
                        ),
                    ],
                    state_person_races=[StatePersonRace(race="W")],
                ),
            ]
        )

        self.run_parse_file_test(expected, "dbo_Offender")

    def test_populate_data_dbo_LSIR(self) -> None:
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="789C",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="789C", id_type=US_PA_PBPP
                        ),
                    ],
                    state_assessments=[
                        StateAssessment(
                            state_assessment_id="789C-0-1",
                            assessment_type="LSIR",
                            assessment_class="RISK",
                            assessment_date="01312001",
                            assessment_score="14",
                            assessment_level=StateAssessmentLevel.LOW.value,
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="456B",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="456B", id_type=US_PA_PBPP
                        ),
                    ],
                    state_assessments=[
                        StateAssessment(
                            state_assessment_id="456B-1-1",
                            assessment_type="LSIR",
                            assessment_class="RISK",
                            assessment_date="12222005",
                            assessment_score="23",
                            assessment_level=StateAssessmentLevel.MEDIUM.value,
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="345E",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="345E", id_type=US_PA_PBPP
                        ),
                    ],
                    state_assessments=[
                        StateAssessment(
                            state_assessment_id="345E-3-1",
                            assessment_type="LSIR",
                            assessment_class="RISK",
                            assessment_date="01192006",
                            assessment_score="30",
                            assessment_level=StateAssessmentLevel.HIGH.value,
                        ),
                        StateAssessment(
                            state_assessment_id="345E-3-2",
                            assessment_type="LSIR",
                            assessment_class="RISK",
                            assessment_date="08032006",
                            assessment_score=None,
                            assessment_level="UNKNOWN (60-ATTEMPTED_INCOMPLETE)",
                        ),
                        StateAssessment(
                            state_assessment_id="345E-3-3",
                            assessment_type="LSIR",
                            assessment_class="RISK",
                            assessment_date="01152007",
                            assessment_score="31",
                            assessment_level=StateAssessmentLevel.HIGH.value,
                        ),
                        StateAssessment(
                            state_assessment_id="345E-4-1",
                            assessment_type="LSIR",
                            assessment_class="RISK",
                            assessment_date="07142007",
                            assessment_score="33",
                            assessment_level=StateAssessmentLevel.HIGH.value,
                        ),
                    ],
                ),
            ]
        )

        self.run_parse_file_test(expected, "dbo_LSIR")

    def test_populate_data_supervision_period(self) -> None:
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="456B",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="456B", id_type=US_PA_PBPP
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        StateSupervisionPeriod(
                                            state_supervision_period_id="456B-1",
                                            supervision_period_supervision_type="C2",
                                            admission_reason="C2",
                                            start_date="2012-03-16",
                                            termination_reason="TRANSFER_WITHIN_STATE",
                                            termination_date="2013-04-01",
                                            county_code="ALLEGH",
                                            supervision_site="02|02 - North Shore|7124",
                                            supervision_level="MAX",
                                            custodial_authority="C2",
                                            conditions=[
                                                "MEST",
                                                "ACT35",
                                                "GPAR",
                                                "MVICT",
                                                "REMC",
                                                "END",
                                            ],
                                            supervising_officer=StateAgent(
                                                state_agent_id="555678",
                                                full_name="Kramer, Cosmo",
                                                agent_type="SUPERVISION_OFFICER",
                                            ),
                                        ),
                                        StateSupervisionPeriod(
                                            state_supervision_period_id="456B-2",
                                            supervision_period_supervision_type="04,C2",
                                            admission_reason="TRANSFER_WITHIN_STATE",
                                            start_date="2013-04-01",
                                            termination_reason="TRANSFER_WITHIN_STATE",
                                            termination_date="2014-08-10",
                                            county_code="ALLEGH",
                                            supervision_site="02|02 - North Shore|7113",
                                            supervision_level="MAX",
                                            custodial_authority="04",
                                            conditions=[
                                                "MEST",
                                                "ACT35",
                                                "GPAR",
                                                "MVICT",
                                                "REMC",
                                                "END",
                                            ],
                                        ),
                                        StateSupervisionPeriod(
                                            state_supervision_period_id="456B-3",
                                            supervision_period_supervision_type="04",
                                            admission_reason="TRANSFER_WITHIN_STATE",
                                            start_date="2014-08-10",
                                            termination_reason="43",
                                            termination_date="2018-01-01",
                                            county_code="ALLEGH",
                                            supervision_site="02|02 - North Shore|7115",
                                            supervision_level="MED",
                                            custodial_authority="04",
                                            conditions=[
                                                "PN",
                                                "EST",
                                                "BL",
                                                "END",
                                                "SUBD",
                                                "AANA",
                                            ],
                                            supervising_officer=StateAgent(
                                                state_agent_id="444123",
                                                full_name="Benes, Elaine",
                                                agent_type="SUPERVISION_OFFICER",
                                            ),
                                        ),
                                    ]
                                ),
                            ],
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="789C",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="789C", id_type=US_PA_PBPP
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        StateSupervisionPeriod(
                                            state_supervision_period_id="789C-1",
                                            supervision_period_supervision_type="05",
                                            admission_reason="05",
                                            start_date="2003-10-10",
                                            termination_reason="42",
                                            termination_date="2004-08-10",
                                            county_code="MERCER",
                                            supervision_site="08|08 - Mercer DO|7307",
                                            supervision_level="MIN",
                                            custodial_authority="05",
                                            conditions=[
                                                "START",
                                                "EST",
                                                "END",
                                                "AANA",
                                                "REL",
                                                "DAM",
                                                "PARAB",
                                                "ACT35",
                                                "BL",
                                                "MISC",
                                                "DDU",
                                                "URI",
                                                "GPAR",
                                                "EMP",
                                                "ALC",
                                                "PM",
                                                "PF",
                                                "PA",
                                            ],
                                            supervising_officer=StateAgent(
                                                state_agent_id="888888",
                                                full_name="Bania, K",
                                                agent_type="SUPERVISION_OFFICER",
                                            ),
                                        ),
                                        StateSupervisionPeriod(
                                            state_supervision_period_id="789C-2",
                                            supervision_period_supervision_type="05",
                                            admission_reason="05",
                                            start_date="2004-08-10",
                                            termination_reason="44",
                                            termination_date="2005-12-31",
                                            county_code="MERCER",
                                            supervision_site="08|08 - Mercer DO|7307",
                                            supervision_level="MIN",
                                            custodial_authority="05",
                                            supervising_officer=StateAgent(
                                                state_agent_id="555",
                                                full_name="Seinfeld, Jerry",
                                                agent_type="SUPERVISION_OFFICER",
                                            ),
                                        ),
                                        StateSupervisionPeriod(
                                            state_supervision_period_id="789C-3",
                                            supervision_period_supervision_type="04",
                                            admission_reason="04",
                                            start_date="2005-12-31",
                                            termination_reason="43",
                                            termination_date="2006-10-10",
                                            county_code="MERCER",
                                            supervision_level="ADM",
                                            custodial_authority="04",
                                            supervising_officer=StateAgent(
                                                full_name="Newman",
                                                agent_type="SUPERVISION_OFFICER",
                                            ),
                                        ),
                                    ]
                                ),
                            ],
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="345E",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="345E", id_type=US_PA_PBPP
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        StateSupervisionPeriod(
                                            state_supervision_period_id="345E-1",
                                            supervision_period_supervision_type="03",
                                            admission_reason="03",
                                            start_date="2016-01-14",
                                            county_code="PHILAD",
                                            supervision_site="01|01 - Northwest|5112",
                                            supervision_level="MED",
                                            custodial_authority="03",
                                            conditions=[
                                                "DOMV",
                                                "AUTO",
                                                "DDU",
                                                "REFR",
                                                "MNOAM",
                                                "END",
                                                "RI",
                                                "RC",
                                                "MEMON",
                                                "MCURF",
                                                "REFC",
                                            ],
                                            supervising_officer=StateAgent(
                                                state_agent_id="101010",
                                                full_name="Talker, Close",
                                                agent_type="SUPERVISION_OFFICER",
                                            ),
                                        ),
                                    ]
                                ),
                            ],
                        ),
                    ],
                ),
            ]
        )

        self.run_parse_file_test(expected, "supervision_period_v2")

    def test_populate_data_supervision_violation(self) -> None:
        violation_456B_1_1 = StateSupervisionViolation(
            state_supervision_violation_id="456B-1-1",
            violation_date="2014-01-01",
            state_supervision_violation_types=[
                StateSupervisionViolationTypeEntry(
                    state_supervision_violation_type_entry_id="456B-1-1-10",
                    violation_type="H10",
                ),
                StateSupervisionViolationTypeEntry(
                    state_supervision_violation_type_entry_id="456B-1-1-4",
                    violation_type="L03",
                ),
                StateSupervisionViolationTypeEntry(
                    state_supervision_violation_type_entry_id="456B-1-1-5",
                    violation_type="L05",
                ),
                StateSupervisionViolationTypeEntry(
                    state_supervision_violation_type_entry_id="456B-1-1-6",
                    violation_type="M01",
                ),
                StateSupervisionViolationTypeEntry(
                    state_supervision_violation_type_entry_id="456B-1-1-7",
                    violation_type="M02",
                ),
                StateSupervisionViolationTypeEntry(
                    state_supervision_violation_type_entry_id="456B-1-1-8",
                    violation_type="H03",
                ),
                StateSupervisionViolationTypeEntry(
                    state_supervision_violation_type_entry_id="456B-1-1-9",
                    violation_type="H07",
                ),
            ],
            state_supervision_violated_conditions=[
                StateSupervisionViolatedConditionEntry(
                    condition="5",
                ),
                StateSupervisionViolatedConditionEntry(
                    condition="7",
                ),
                StateSupervisionViolatedConditionEntry(
                    condition="3",
                ),
            ],
        )

        violation_456B_2_1 = StateSupervisionViolation(
            state_supervision_violation_id="456B-2-1",
            violation_date="2015-04-13",
            state_supervision_violation_types=[
                StateSupervisionViolationTypeEntry(
                    state_supervision_violation_type_entry_id="456B-2-1-14",
                    violation_type="H08",
                ),
            ],
            state_supervision_violated_conditions=[
                StateSupervisionViolatedConditionEntry(
                    condition="5",
                ),
            ],
        )

        violation_789C_3_1 = StateSupervisionViolation(
            state_supervision_violation_id="789C-3-1",
            violation_date="2006-08-11",
            state_supervision_violation_types=[
                StateSupervisionViolationTypeEntry(
                    state_supervision_violation_type_entry_id="789C-3-1-3",
                    violation_type="H12",
                ),
                StateSupervisionViolationTypeEntry(
                    state_supervision_violation_type_entry_id="789C-3-1-7",
                    violation_type="H04",
                ),
            ],
            state_supervision_violated_conditions=[
                StateSupervisionViolatedConditionEntry(
                    condition="5",
                ),
                StateSupervisionViolatedConditionEntry(
                    condition="7",
                ),
            ],
        )

        violation_345E_1_1 = StateSupervisionViolation(
            state_supervision_violation_id="345E-1-1",
            violation_date="2018-03-17",
            state_supervision_violation_types=[
                StateSupervisionViolationTypeEntry(
                    state_supervision_violation_type_entry_id="345E-1-1-3",
                    violation_type="L08",
                ),
            ],
            state_supervision_violated_conditions=[
                StateSupervisionViolatedConditionEntry(
                    condition="5",
                ),
            ],
        )

        violation_345E_1_2 = StateSupervisionViolation(
            state_supervision_violation_id="345E-1-2",
            violation_date="2018-05-12",
            state_supervision_violation_types=[
                StateSupervisionViolationTypeEntry(
                    state_supervision_violation_type_entry_id="345E-1-2-7",
                    violation_type="M13",
                ),
                StateSupervisionViolationTypeEntry(
                    state_supervision_violation_type_entry_id="345E-1-2-8",
                    violation_type="M14",
                ),
            ],
            state_supervision_violated_conditions=[
                StateSupervisionViolatedConditionEntry(
                    condition="4",
                ),
                StateSupervisionViolatedConditionEntry(
                    condition="7",
                ),
            ],
        )

        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="456B",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="456B", id_type=US_PA_PBPP
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        StateSupervisionPeriod(
                                            state_supervision_violation_entries=[
                                                violation_456B_1_1,
                                                violation_456B_2_1,
                                            ]
                                        ),
                                    ]
                                ),
                            ],
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="789C",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="789C", id_type=US_PA_PBPP
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        StateSupervisionPeriod(
                                            state_supervision_violation_entries=[
                                                violation_789C_3_1,
                                            ]
                                        ),
                                    ]
                                ),
                            ],
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="345E",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="345E", id_type=US_PA_PBPP
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        StateSupervisionPeriod(
                                            state_supervision_violation_entries=[
                                                violation_345E_1_1,
                                                violation_345E_1_2,
                                            ]
                                        ),
                                    ]
                                ),
                            ],
                        ),
                    ],
                ),
            ]
        )

        self.run_parse_file_test(expected, "supervision_violation")

    def test_populate_data_supervision_violation_response(self) -> None:
        violation_456B_1_1 = StateSupervisionViolation(
            state_supervision_violation_id="456B-1-1",
            state_supervision_violation_responses=[
                StateSupervisionViolationResponse(
                    state_supervision_violation_response_id="456B-1-1",
                    response_type="VIOLATION_REPORT",
                    response_date="2013-01-02",
                    supervision_violation_response_decisions=[
                        StateSupervisionViolationResponseDecisionEntry(
                            state_supervision_violation_response_decision_entry_id="456B-1-1-2",
                            decision="WTWR",
                            revocation_type="WTWR",
                        ),
                        StateSupervisionViolationResponseDecisionEntry(
                            state_supervision_violation_response_decision_entry_id="456B-1-1-3",
                            decision="DJBS",
                            revocation_type="DJBS",
                        ),
                    ],
                )
            ],
        )

        violation_456B_2_1 = StateSupervisionViolation(
            state_supervision_violation_id="456B-2-1",
            state_supervision_violation_responses=[
                StateSupervisionViolationResponse(
                    state_supervision_violation_response_id="456B-2-1",
                    response_type="VIOLATION_REPORT",
                    response_date="2015-04-13",
                    supervision_violation_response_decisions=[
                        StateSupervisionViolationResponseDecisionEntry(
                            state_supervision_violation_response_decision_entry_id="456B-2-1-12",
                            decision="VCCF",
                            revocation_type="VCCF",
                        ),
                        StateSupervisionViolationResponseDecisionEntry(
                            state_supervision_violation_response_decision_entry_id="456B-2-1-13",
                            decision="ARR2",
                            revocation_type="ARR2",
                        ),
                    ],
                )
            ],
        )

        violation_789C_3_1 = StateSupervisionViolation(
            state_supervision_violation_id="789C-3-1",
            state_supervision_violation_responses=[
                StateSupervisionViolationResponse(
                    state_supervision_violation_response_id="789C-3-1",
                    response_type="VIOLATION_REPORT",
                    response_date="2006-08-16",
                    supervision_violation_response_decisions=[
                        StateSupervisionViolationResponseDecisionEntry(
                            state_supervision_violation_response_decision_entry_id="789C-3-1-4",
                            decision="VCCF",
                            revocation_type="VCCF",
                        ),
                        StateSupervisionViolationResponseDecisionEntry(
                            state_supervision_violation_response_decision_entry_id="789C-3-1-5",
                            decision="ARR2",
                            revocation_type="ARR2",
                        ),
                    ],
                )
            ],
        )

        violation_345E_1_1 = StateSupervisionViolation(
            state_supervision_violation_id="345E-1-1",
            state_supervision_violation_responses=[
                StateSupervisionViolationResponse(
                    state_supervision_violation_response_id="345E-1-1",
                    response_type="VIOLATION_REPORT",
                    response_date="2018-03-23",
                    supervision_violation_response_decisions=[
                        StateSupervisionViolationResponseDecisionEntry(
                            state_supervision_violation_response_decision_entry_id="345E-1-1-2",
                            decision="WTWR",
                            revocation_type="WTWR",
                        ),
                    ],
                )
            ],
        )

        violation_345E_1_2 = StateSupervisionViolation(
            state_supervision_violation_id="345E-1-2",
            state_supervision_violation_responses=[
                StateSupervisionViolationResponse(
                    state_supervision_violation_response_id="345E-1-2",
                    response_type="VIOLATION_REPORT",
                    response_date="2018-05-13",
                    supervision_violation_response_decisions=[
                        StateSupervisionViolationResponseDecisionEntry(
                            state_supervision_violation_response_decision_entry_id="345E-1-2-5",
                            decision="ARR2",
                            revocation_type="ARR2",
                        ),
                        StateSupervisionViolationResponseDecisionEntry(
                            state_supervision_violation_response_decision_entry_id="345E-1-2-6",
                            decision="PV01",
                            revocation_type="PV01",
                        ),
                    ],
                )
            ],
        )

        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="456B",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="456B", id_type=US_PA_PBPP
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        StateSupervisionPeriod(
                                            state_supervision_violation_entries=[
                                                violation_456B_1_1,
                                                violation_456B_2_1,
                                            ]
                                        ),
                                    ]
                                ),
                            ],
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="789C",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="789C", id_type=US_PA_PBPP
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        StateSupervisionPeriod(
                                            state_supervision_violation_entries=[
                                                violation_789C_3_1,
                                            ]
                                        ),
                                    ]
                                ),
                            ],
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="345E",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="345E", id_type=US_PA_PBPP
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        StateSupervisionPeriod(
                                            state_supervision_violation_entries=[
                                                violation_345E_1_1,
                                                violation_345E_1_2,
                                            ]
                                        ),
                                    ]
                                ),
                            ],
                        ),
                    ],
                ),
            ]
        )

        self.run_parse_file_test(expected, "supervision_violation_response")

    def test_populate_board_action(self) -> None:
        violation_456B_0_04 = StateSupervisionViolation(
            state_supervision_violation_responses=[
                StateSupervisionViolationResponse(
                    state_supervision_violation_response_id="456B-0-04",
                    response_type="PERMANENT_DECISION",
                    response_date="20140224",
                    deciding_body_type="PAROLE_BOARD",
                    supervision_violation_response_decisions=[
                        StateSupervisionViolationResponseDecisionEntry(
                            state_supervision_violation_response_decision_entry_id="456B-0-04",
                            decision="RESCR9",
                            revocation_type="RESCR9",
                        )
                    ],
                )
            ]
        )

        violation_789C_0_02 = StateSupervisionViolation(
            state_supervision_violation_responses=[
                StateSupervisionViolationResponse(
                    state_supervision_violation_response_id="789C-0-02",
                    response_type="PERMANENT_DECISION",
                    response_date="20140709",
                    deciding_body_type="PAROLE_BOARD",
                    supervision_violation_response_decisions=[
                        StateSupervisionViolationResponseDecisionEntry(
                            state_supervision_violation_response_decision_entry_id="789C-0-02",
                            decision="RESCR9",
                            revocation_type="RESCR9",
                        )
                    ],
                )
            ]
        )

        violation_123A_1_09 = StateSupervisionViolation(
            state_supervision_violation_responses=[
                StateSupervisionViolationResponse(
                    state_supervision_violation_response_id="123A-1-09",
                    response_type="PERMANENT_DECISION",
                    response_date="20040616",
                    deciding_body_type="PAROLE_BOARD",
                    supervision_violation_response_decisions=[
                        StateSupervisionViolationResponseDecisionEntry(
                            state_supervision_violation_response_decision_entry_id="123A-1-09",
                            decision="RESCR",
                            revocation_type="RESCR",
                        )
                    ],
                )
            ]
        )

        violation_345E_3_11 = StateSupervisionViolation(
            state_supervision_violation_responses=[
                StateSupervisionViolationResponse(
                    state_supervision_violation_response_id="345E-3-11",
                    response_type="PERMANENT_DECISION",
                    response_date="20060221",
                    deciding_body_type="PAROLE_BOARD",
                    supervision_violation_response_decisions=[
                        StateSupervisionViolationResponseDecisionEntry(
                            state_supervision_violation_response_decision_entry_id="345E-3-11",
                            decision="RESCR",
                            revocation_type="RESCR",
                        )
                    ],
                )
            ]
        )

        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="456B",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="456B", id_type=US_PA_PBPP
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        StateSupervisionPeriod(
                                            state_supervision_violation_entries=[
                                                violation_456B_0_04,
                                            ]
                                        ),
                                    ]
                                ),
                            ],
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="789C",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="789C", id_type=US_PA_PBPP
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        StateSupervisionPeriod(
                                            state_supervision_violation_entries=[
                                                violation_789C_0_02,
                                            ]
                                        ),
                                    ]
                                ),
                            ],
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="123A",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="123A", id_type=US_PA_PBPP
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        StateSupervisionPeriod(
                                            state_supervision_violation_entries=[
                                                violation_123A_1_09
                                            ]
                                        ),
                                    ]
                                ),
                            ],
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="345E",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="345E", id_type=US_PA_PBPP
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        StateSupervisionPeriod(
                                            state_supervision_violation_entries=[
                                                violation_345E_3_11,
                                            ]
                                        ),
                                    ]
                                ),
                            ],
                        ),
                    ],
                ),
            ]
        )

        self.run_parse_file_test(expected, "board_action")

    def test_run_full_ingest_all_files_specific_order(self) -> None:
        self.maxDiff = None
        ######################################
        # person_external_ids
        ######################################
        # Arrange
        person_1 = entities.StatePerson.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id="123456",
                    id_type=US_PA_CONTROL,
                ),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, external_id="123A", id_type=US_PA_PBPP
                ),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id="12345678",
                    id_type=US_PA_SID,
                ),
            ],
        )

        person_2 = entities.StatePerson.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id="654321",
                    id_type=US_PA_CONTROL,
                ),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, external_id="456B", id_type=US_PA_PBPP
                ),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id="55554444",
                    id_type=US_PA_SID,
                ),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id="66665555",
                    id_type=US_PA_SID,
                ),
            ],
        )

        person_3 = entities.StatePerson.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id="445566",
                    id_type=US_PA_CONTROL,
                ),
            ],
        )

        person_4 = entities.StatePerson.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id="778899",
                    id_type=US_PA_CONTROL,
                ),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id="889900",
                    id_type=US_PA_CONTROL,
                ),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, external_id="345E", id_type=US_PA_PBPP
                ),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, external_id="999Z", id_type=US_PA_PBPP
                ),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id="09876543",
                    id_type=US_PA_SID,
                ),
            ],
        )

        person_5 = entities.StatePerson.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, external_id="789C", id_type=US_PA_PBPP
                ),
            ],
        )

        person_6 = entities.StatePerson.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id="888888888",
                    id_type=US_PA_SID,
                ),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, external_id="888P", id_type=US_PA_PBPP
                ),
            ],
        )

        person_7 = entities.StatePerson.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id="9999999",
                    id_type=US_PA_SID,
                ),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id="090909",
                    id_type=US_PA_CONTROL,
                ),
            ],
        )

        person_8 = entities.StatePerson.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id="66666666",
                    id_type=US_PA_SID,
                ),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id="66666667",
                    id_type=US_PA_SID,
                ),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id="060606",
                    id_type=US_PA_CONTROL,
                ),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id="060607",
                    id_type=US_PA_CONTROL,
                ),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, external_id="666P", id_type=US_PA_PBPP
                ),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, external_id="777M", id_type=US_PA_PBPP
                ),
            ],
        )

        expected_people = [
            person_1,
            person_2,
            person_3,
            person_4,
            person_5,
            person_8,
            person_7,
            person_6,
        ]

        populate_person_backedges(expected_people)

        # Act
        self._run_ingest_job_for_filename("person_external_ids.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # doc_person_info
        ######################################
        # Arrange
        person_1.full_name = '{"given_names": "BERTRAND", "surname": "RUSSELL"}'
        person_1.gender = Gender.MALE
        person_1.gender_raw_text = "MALE"
        person_1.birthdate = datetime.date(year=1976, month=3, day=18)
        person_1.birthdate_inferred_from_age = False
        person_1.current_address = "123 EASY STREET, PITTSBURGH, PA 16161"
        person_1.residency_status = ResidencyStatus.PERMANENT
        person_1.state_code = _STATE_CODE_UPPER
        person_1.aliases = [
            entities.StatePersonAlias.new_with_defaults(
                full_name='{"given_names": "BERTRAND", "surname": "RUSSELL"}',
                state_code=_STATE_CODE_UPPER,
                alias_type=StatePersonAliasType.GIVEN_NAME,
                alias_type_raw_text="GIVEN_NAME",
            )
        ]
        person_1.races = [
            entities.StatePersonRace.new_with_defaults(
                state_code=_STATE_CODE_UPPER, race=Race.BLACK, race_raw_text="BLACK"
            ),
        ]

        p1_sg = entities.StateSentenceGroup.new_with_defaults(
            external_id="AB7413",
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_1,
        )
        p1_sg2 = entities.StateSentenceGroup.new_with_defaults(
            external_id="BC8524",
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_1,
        )

        person_1.sentence_groups.append(p1_sg)
        person_1.sentence_groups.append(p1_sg2)

        person_2.full_name = '{"given_names": "JEAN-PAUL", "surname": "SARTRE"}'
        person_2.gender = Gender.MALE
        person_2.gender_raw_text = "MALE"
        person_2.birthdate = datetime.date(year=1982, month=10, day=2)
        person_2.birthdate_inferred_from_age = False
        person_2.current_address = "555 FLATBUSH DR, NEW YORK, NY 10031"
        person_2.residency_status = ResidencyStatus.PERMANENT
        person_2.state_code = _STATE_CODE_UPPER
        person_2.aliases = [
            entities.StatePersonAlias.new_with_defaults(
                full_name='{"given_names": "JEAN-PAUL", "surname": "SARTRE"}',
                state_code=_STATE_CODE_UPPER,
                alias_type=StatePersonAliasType.GIVEN_NAME,
                alias_type_raw_text="GIVEN_NAME",
            )
        ]
        person_2.races = [
            entities.StatePersonRace.new_with_defaults(
                state_code=_STATE_CODE_UPPER, race=Race.BLACK, race_raw_text="BLACK"
            ),
        ]

        p2_sg = entities.StateSentenceGroup.new_with_defaults(
            external_id="GF3374",
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_2,
        )
        person_2.sentence_groups.append(p2_sg)

        person_3.full_name = (
            '{"given_names": "SOREN", "name_suffix": "JR", "surname": "KIERKEGAARD"}'
        )
        person_3.gender = Gender.FEMALE
        person_3.gender_raw_text = "FEMALE"
        person_3.birthdate = datetime.date(year=1991, month=11, day=20)
        person_3.birthdate_inferred_from_age = False
        person_3.current_address = "5000 SUNNY LANE, APT. 55D, PHILADELPHIA, PA 19129"
        person_3.residency_status = ResidencyStatus.PERMANENT
        person_3.state_code = _STATE_CODE_UPPER
        person_3.aliases = [
            entities.StatePersonAlias.new_with_defaults(
                full_name='{"given_names": "SOREN", "name_suffix": "JR", "surname": "KIERKEGAARD"}',
                state_code=_STATE_CODE_UPPER,
                alias_type=StatePersonAliasType.GIVEN_NAME,
                alias_type_raw_text="GIVEN_NAME",
            )
        ]
        person_3.races = [
            entities.StatePersonRace.new_with_defaults(
                state_code=_STATE_CODE_UPPER, race=Race.WHITE, race_raw_text="WHITE"
            ),
        ]

        p3_sg = entities.StateSentenceGroup.new_with_defaults(
            external_id="CJ1991",
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_3,
        )
        person_3.sentence_groups.append(p3_sg)

        person_4.full_name = '{"given_names": "JOHN", "surname": "RAWLS"}'
        person_4.gender = Gender.MALE
        person_4.gender_raw_text = "MALE"
        person_4.birthdate = datetime.date(year=1989, month=6, day=17)
        person_4.birthdate_inferred_from_age = False
        person_4.current_address = "214 HAPPY PLACE, PHILADELPHIA, PA 19129"
        person_4.residency_status = ResidencyStatus.PERMANENT
        person_4.state_code = _STATE_CODE_UPPER
        person_4.aliases = [
            entities.StatePersonAlias.new_with_defaults(
                full_name='{"given_names": "JOHN", "surname": "RAWLS"}',
                state_code=_STATE_CODE_UPPER,
                alias_type=StatePersonAliasType.GIVEN_NAME,
                alias_type_raw_text="GIVEN_NAME",
            )
        ]
        person_4.ethnicities = [
            entities.StatePersonEthnicity.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                ethnicity=Ethnicity.HISPANIC,
                ethnicity_raw_text="HISPANIC",
            ),
        ]

        p4_sg = entities.StateSentenceGroup.new_with_defaults(
            external_id="JE1989",
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_4,
        )
        person_4.sentence_groups.append(p4_sg)

        populate_person_backedges(expected_people)

        # Act
        self._run_ingest_job_for_filename("doc_person_info_v2.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # dbo_tblInmTestScore
        ######################################

        person_1_doc_assessments = [
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.SOCIAL,
                assessment_class_raw_text="SOCIAL",
                assessment_type=StateAssessmentType.CSSM,
                assessment_type_raw_text="CSS-M",
                assessment_score=19,
                assessment_date=datetime.date(year=2008, month=6, day=22),
                external_id="123456-AB7413-1-1",
                state_code=_STATE_CODE_UPPER,
                person=person_1,
            ),
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.SOCIAL,
                assessment_class_raw_text="SOCIAL",
                assessment_type=StateAssessmentType.CSSM,
                assessment_type_raw_text="CSS-M",
                assessment_score=11,
                assessment_date=datetime.date(year=2010, month=1, day=1),
                external_id="123456-BC8524-1-1",
                state_code=_STATE_CODE_UPPER,
                person=person_1,
            ),
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.SOCIAL,
                assessment_class_raw_text="SOCIAL",
                assessment_type=StateAssessmentType.HIQ,
                assessment_type_raw_text="HIQ",
                assessment_score=62,
                assessment_date=datetime.date(year=2004, month=7, day=12),
                external_id="123456-AB7413-2-1",
                state_code=_STATE_CODE_UPPER,
                person=person_1,
            ),
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.RISK,
                assessment_class_raw_text="RISK",
                assessment_type=StateAssessmentType.LSIR,
                assessment_type_raw_text="LSI-R",
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_level_raw_text="HIGH",
                assessment_score=27,
                assessment_date=datetime.date(year=2010, month=10, day=3),
                external_id="123456-AB7413-3-3",
                state_code=_STATE_CODE_UPPER,
                person=person_1,
            ),
        ]
        person_1.assessments = person_1_doc_assessments

        person_2_doc_assessments = [
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.SOCIAL,
                assessment_class_raw_text="SOCIAL",
                assessment_type=StateAssessmentType.CSSM,
                assessment_type_raw_text="CSS-M",
                assessment_score=22,
                assessment_date=datetime.date(year=2003, month=4, day=1),
                external_id="654321-GF3374-1-1",
                state_code=_STATE_CODE_UPPER,
                person=person_2,
            ),
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.RISK,
                assessment_class_raw_text="RISK",
                assessment_type=StateAssessmentType.LSIR,
                assessment_type_raw_text="LSI-R",
                assessment_level=StateAssessmentLevel.LOW,
                assessment_level_raw_text="LOW",
                assessment_score=19,
                assessment_date=datetime.date(year=2004, month=6, day=8),
                external_id="654321-GF3374-3-1",
                state_code=_STATE_CODE_UPPER,
                person=person_2,
            ),
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.SUBSTANCE_ABUSE,
                assessment_class_raw_text="SUBSTANCE_ABUSE",
                assessment_type=StateAssessmentType.TCU_DRUG_SCREEN,
                assessment_type_raw_text="TCU",
                assessment_score=6,
                assessment_date=datetime.date(year=2004, month=1, day=4),
                external_id="654321-GF3374-4-1",
                state_code=_STATE_CODE_UPPER,
                person=person_2,
            ),
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.SEX_OFFENSE,
                assessment_class_raw_text="SEX_OFFENSE",
                assessment_type=StateAssessmentType.STATIC_99,
                assessment_type_raw_text="ST99",
                assessment_score=4,
                assessment_date=datetime.date(year=2004, month=7, day=5),
                external_id="654321-GF3374-5-1",
                state_code=_STATE_CODE_UPPER,
                person=person_2,
            ),
        ]
        person_2.assessments = person_2_doc_assessments

        person_3_doc_assessments = [
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.SOCIAL,
                assessment_class_raw_text="SOCIAL",
                assessment_type=StateAssessmentType.HIQ,
                assessment_type_raw_text="HIQ",
                assessment_score=61,
                assessment_date=datetime.date(year=2005, month=7, day=28),
                external_id="445566-CJ1991-2-1",
                state_code=_STATE_CODE_UPPER,
                person=person_3,
            ),
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.RISK,
                assessment_class_raw_text="RISK",
                assessment_type=StateAssessmentType.LSIR,
                assessment_type_raw_text="LSI-R",
                assessment_level=StateAssessmentLevel.LOW,
                assessment_level_raw_text="LOW",
                assessment_score=13,
                assessment_date=datetime.date(year=2016, month=12, day=19),
                external_id="445566-CJ1991-3-2",
                state_code=_STATE_CODE_UPPER,
                person=person_3,
            ),
        ]
        person_3.assessments = person_3_doc_assessments

        person_4_doc_assessments = [
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.RISK,
                assessment_class_raw_text="RISK",
                assessment_type=StateAssessmentType.LSIR,
                assessment_type_raw_text="LSI-R",
                assessment_level=StateAssessmentLevel.EXTERNAL_UNKNOWN,
                assessment_level_raw_text="UNKNOWN (70-REFUSED)",
                assessment_score=None,
                assessment_date=datetime.date(year=2017, month=1, day=6),
                external_id="778899-JE1989-3-3",
                state_code=_STATE_CODE_UPPER,
                person=person_4,
            ),
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.RISK,
                assessment_class_raw_text="RISK",
                assessment_type=StateAssessmentType.PA_RST,
                assessment_type_raw_text="RST",
                assessment_score=9,
                assessment_date=datetime.date(year=2012, month=12, day=8),
                assessment_metadata='{"LATEST_VERSION": FALSE}',
                external_id="778899-JE1989-6-1",
                state_code=_STATE_CODE_UPPER,
                person=person_4,
            ),
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.RISK,
                assessment_class_raw_text="RISK",
                assessment_type=StateAssessmentType.PA_RST,
                assessment_type_raw_text="RST",
                assessment_score=7,
                assessment_date=datetime.date(year=2018, month=5, day=11),
                assessment_metadata='{"LATEST_VERSION": TRUE}',
                external_id="778899-JE1989-6-2",
                state_code=_STATE_CODE_UPPER,
                person=person_4,
            ),
        ]
        person_4.assessments = person_4_doc_assessments

        populate_person_backedges(expected_people)

        # Act
        self._run_ingest_job_for_filename("dbo_tblInmTestScore.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # dbo_Senrec
        ######################################

        # Person 1 updates
        p1_is = entities.StateIncarcerationSentence.new_with_defaults(
            external_id="AB7413-01",
            state_code=_STATE_CODE_UPPER,
            county_code="PHI",
            status=StateSentenceStatus.COMPLETED,
            status_raw_text="SC",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="S",
            date_imposed=datetime.date(year=2008, month=8, day=15),
            start_date=datetime.date(year=2008, month=8, day=15),
            completion_date=datetime.date(year=2009, month=1, day=4),
            min_length_days=549,
            max_length_days=1095,
            is_life=False,
            is_capital_punishment=False,
            person=person_1,
            sentence_group=p1_sg,
        )
        p1_sg.incarceration_sentences.append(p1_is)

        p1_is_charge = entities.StateCharge.new_with_defaults(
            external_id="N7825555",
            statute="CC3701A",
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE_UPPER,
            person=person_1,
            incarceration_sentences=[p1_is],
        )
        p1_is.charges.append(p1_is_charge)

        # Person 2 updates
        p2_is = entities.StateIncarcerationSentence.new_with_defaults(
            external_id="GF3374-01",
            state_code=_STATE_CODE_UPPER,
            county_code="PHI",
            status=StateSentenceStatus.SERVING,
            status_raw_text="AS",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="S",
            date_imposed=datetime.date(year=2008, month=8, day=16),
            min_length_days=4287,
            max_length_days=5113,
            is_life=False,
            is_capital_punishment=False,
            person=person_2,
            sentence_group=p2_sg,
        )
        p2_sg.incarceration_sentences.append(p2_is)

        p2_is_charge = entities.StateCharge.new_with_defaults(
            external_id="U1196666",
            statute="CC3701A",
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE_UPPER,
            person=person_2,
            incarceration_sentences=[p2_is],
        )
        p2_is.charges.append(p2_is_charge)

        # Person 3 updates
        p3_is = entities.StateIncarcerationSentence.new_with_defaults(
            external_id="CJ1991-01",
            state_code=_STATE_CODE_UPPER,
            county_code="BUC",
            status=StateSentenceStatus.SERVING,
            status_raw_text="AS",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="S",
            date_imposed=datetime.date(year=2016, month=8, day=20),
            start_date=datetime.date(year=2016, month=8, day=20),
            min_length_days=457,
            max_length_days=914,
            is_life=False,
            is_capital_punishment=False,
            person=person_3,
            sentence_group=p3_sg,
        )
        p3_sg.incarceration_sentences.append(p3_is)

        p3_is_charge = entities.StateCharge.new_with_defaults(
            external_id="L3947777",
            statute="CC6318A1",
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE_UPPER,
            person=person_3,
            incarceration_sentences=[p3_is],
        )
        p3_is.charges.append(p3_is_charge)

        # Person 4 updates
        p4_is_1 = entities.StateIncarcerationSentence.new_with_defaults(
            external_id="JE1989-01",
            state_code=_STATE_CODE_UPPER,
            county_code="BUC",
            status=StateSentenceStatus.SERVING,
            status_raw_text="AS",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="S",
            date_imposed=datetime.date(year=2016, month=8, day=20),
            start_date=datetime.date(year=2016, month=8, day=20),
            max_length_days=1095,
            is_life=False,
            is_capital_punishment=False,
            person=person_4,
            sentence_group=p4_sg,
        )
        p4_is_2 = entities.StateIncarcerationSentence.new_with_defaults(
            external_id="JE1989-02",
            state_code=_STATE_CODE_UPPER,
            county_code="BUC",
            status=StateSentenceStatus.SERVING,
            status_raw_text="AS",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="S",
            date_imposed=datetime.date(year=2016, month=8, day=20),
            start_date=datetime.date(year=2016, month=8, day=20),
            min_length_days=549,
            is_life=False,
            is_capital_punishment=False,
            person=person_4,
            sentence_group=p4_sg,
        )
        p4_sg.incarceration_sentences.extend([p4_is_1, p4_is_2])

        p4_is_1_charge = entities.StateCharge.new_with_defaults(
            external_id="L7858888",
            statute="CC3503A",
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_sentences=[p4_is_1],
        )
        p4_is_1.charges.append(p4_is_1_charge)

        p4_is_2_charge = entities.StateCharge.new_with_defaults(
            external_id="L7858890",
            statute="CC3503B",
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_sentences=[p4_is_2],
        )
        p4_is_2.charges.append(p4_is_2_charge)

        populate_person_backedges(expected_people)

        # Act
        self._run_ingest_job_for_filename("dbo_Senrec.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # incarceration_period
        ######################################

        # Person 2 Incarceration Periods
        p2_is_ip1 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="GF3374-1",
            state_code=_STATE_CODE_UPPER,
            person=person_2,
            incarceration_sentences=[p2_is],
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=datetime.date(year=2008, month=10, day=10),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="NA-FALSE-AC",
            facility="PNG",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="SCI",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            specialized_purpose_for_incarceration_raw_text="NA-P",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text=StateCustodialAuthority.STATE_PRISON.value,
        )

        p2_is.incarceration_periods.append(p2_is_ip1)

        # Person 3 Incarceration Periods
        p3_is_ip_1 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="CJ1991-1",
            state_code=_STATE_CODE_UPPER,
            person=person_3,
            incarceration_sentences=[p3_is],
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=2016, month=10, day=11),
            release_date=datetime.date(year=2016, month=10, day=22),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="NA-FALSE-AC",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="DC-NA-TRN",
            facility="GRA",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="SCI",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="NA-S",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text=StateCustodialAuthority.STATE_PRISON.value,
        )
        p3_is_ip_2 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="CJ1991-2",
            state_code=_STATE_CODE_UPPER,
            person=person_3,
            incarceration_sentences=[p3_is],
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=2016, month=10, day=22),
            release_date=datetime.date(year=2017, month=6, day=2),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="NA-FALSE-TRN",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="AS-NA-TRN",
            facility="CAM",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="SCI",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="NA-S",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text=StateCustodialAuthority.STATE_PRISON.value,
        )
        p3_is_ip_4 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="CJ1991-4",
            state_code=_STATE_CODE_UPPER,
            person=person_3,
            incarceration_sentences=[p3_is],
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=2017, month=6, day=2),
            release_date=datetime.date(year=2017, month=7, day=14),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="NA-FALSE-TRN",
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="P-SP-D",
            facility="WAM",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="SCI",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="NA-S",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text=StateCustodialAuthority.STATE_PRISON.value,
        )
        p3_is_ip_6 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="CJ1991-6",
            state_code=_STATE_CODE_UPPER,
            person=person_3,
            incarceration_sentences=[p3_is],
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=2018, month=3, day=10),
            release_date=datetime.date(year=2018, month=4, day=1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            admission_reason_raw_text="PVP-FALSE-APV",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="AS-PVP-TRN",
            facility="DAL",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="SCI",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            specialized_purpose_for_incarceration_raw_text="PVP-S",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text=StateCustodialAuthority.STATE_PRISON.value,
        )
        p3_is_ip_7 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="CJ1991-7",
            state_code=_STATE_CODE_UPPER,
            person=person_3,
            incarceration_sentences=[p3_is],
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=2018, month=4, day=1),
            release_date=datetime.date(year=2018, month=7, day=23),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="PVP-FALSE-TRN",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="AS-TPV-SC",
            facility="WAM",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="SCI",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            specialized_purpose_for_incarceration_raw_text="PVP-S",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text=StateCustodialAuthority.STATE_PRISON.value,
        )
        p3_is_ip_8 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="CJ1991-8",
            state_code=_STATE_CODE_UPPER,
            person=person_3,
            incarceration_sentences=[p3_is],
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=2018, month=7, day=23),
            release_date=datetime.date(year=2018, month=9, day=14),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            admission_reason_raw_text="TPV-TRUE-SC",
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="P-RP-D",
            facility="WAM",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="SCI",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="TPV-S",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text=StateCustodialAuthority.STATE_PRISON.value,
        )

        p3_is.incarceration_periods = [
            p3_is_ip_1,
            p3_is_ip_2,
            p3_is_ip_4,
            p3_is_ip_6,
            p3_is_ip_7,
            p3_is_ip_8,
        ]

        # Person 4 New Sentence Group with Sentence And Periods
        p4_sg_2 = entities.StateSentenceGroup.new_with_defaults(
            external_id="JE1977",
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_4,
        )

        p4_is_2_1 = entities.StateIncarcerationSentence.new_with_defaults(
            external_id="JE1977-01",
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            person=person_4,
            sentence_group=p4_sg_2,
        )

        p4_sg_2.incarceration_sentences.append(p4_is_2_1)
        person_4.sentence_groups.append(p4_sg_2)

        p4_is_2_ip_1 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="JE1977-1",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_sentences=[p4_is_2_1],
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=1991, month=3, day=5),
            release_date=datetime.date(year=1991, month=3, day=8),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="NA-FALSE-AC",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="DC-NA-TRN",
            facility="GRA",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="SCI",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="NA-S",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text=StateCustodialAuthority.STATE_PRISON.value,
        )
        p4_is_2_ip_2 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="JE1977-2",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_sentences=[p4_is_2_1],
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=1991, month=3, day=8),
            release_date=datetime.date(year=1991, month=4, day=25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="NA-FALSE-TRN",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="WT-NA-SC",
            facility="CAM",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="SCI",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="NA-S",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text=StateCustodialAuthority.STATE_PRISON.value,
        )
        p4_is_2_ip_3 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="JE1977-3",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_sentences=[p4_is_2_1],
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=1991, month=4, day=25),
            release_date=datetime.date(year=1991, month=4, day=25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="NA-FALSE-SC",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="AS-NA-SC",
            facility="PHI",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="SCI",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="NA-S",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text=StateCustodialAuthority.STATE_PRISON.value,
        )
        p4_is_2_ip_4 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="JE1977-4",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_sentences=[p4_is_2_1],
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=1991, month=4, day=25),
            release_date=datetime.date(year=1991, month=4, day=30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="NA-FALSE-SC",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="WT-NA-SC",
            facility="GRA",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="SCI",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="NA-S",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text=StateCustodialAuthority.STATE_PRISON.value,
        )
        p4_is_2_ip_5 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="JE1977-5",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_sentences=[p4_is_2_1],
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=1991, month=4, day=30),
            release_date=datetime.date(year=1991, month=4, day=30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="NA-FALSE-SC",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="AS-NA-SC",
            facility="PHI",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="SCI",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="NA-S",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text=StateCustodialAuthority.STATE_PRISON.value,
        )
        p4_is_2_ip_6 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="JE1977-6",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_sentences=[p4_is_2_1],
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=1991, month=4, day=30),
            release_date=datetime.date(year=1991, month=5, day=1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="NA-FALSE-SC",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="WT-NA-SC",
            facility="GRA",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="SCI",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="NA-S",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text=StateCustodialAuthority.STATE_PRISON.value,
        )
        p4_is_2_ip_7 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="JE1977-7",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_sentences=[p4_is_2_1],
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=1991, month=5, day=1),
            release_date=datetime.date(year=1991, month=5, day=1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="NA-FALSE-SC",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="DC-NA-SC",
            facility="PHI",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="SCI",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="NA-S",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text=StateCustodialAuthority.STATE_PRISON.value,
        )
        p4_is_2_ip_8 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="JE1977-8",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_sentences=[p4_is_2_1],
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=1991, month=5, day=1),
            release_date=datetime.date(year=1991, month=11, day=13),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="NA-FALSE-SC",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="AS-NA-SC",
            facility="CAM",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="SCI",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="NA-S",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text=StateCustodialAuthority.STATE_PRISON.value,
        )
        p4_is_2_ip_9 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="JE1977-9",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_sentences=[p4_is_2_1],
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=1991, month=11, day=13),
            release_date=datetime.date(year=1991, month=11, day=13),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="NA-FALSE-SC",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="AS-NA-TRN",
            facility="CAM",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="SCI",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="NA-S",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text=StateCustodialAuthority.STATE_PRISON.value,
        )
        p4_is_2_ip_10 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="JE1977-10",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_sentences=[p4_is_2_1],
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=1991, month=11, day=13),
            release_date=datetime.date(year=1994, month=7, day=14),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="NA-FALSE-TRN",
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="P-SP-D",
            facility="SMI",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="SCI",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="NA-S",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text=StateCustodialAuthority.STATE_PRISON.value,
        )
        p4_is_2_1.incarceration_periods = [
            p4_is_2_ip_1,
            p4_is_2_ip_2,
            p4_is_2_ip_3,
            p4_is_2_ip_4,
            p4_is_2_ip_5,
            p4_is_2_ip_6,
            p4_is_2_ip_7,
            p4_is_2_ip_8,
            p4_is_2_ip_9,
            p4_is_2_ip_10,
        ]

        p4_is_1_ip_12 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="JE1989-12",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_sentences=[p4_is_1],
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=2005, month=4, day=21),
            release_date=datetime.date(year=2005, month=6, day=14),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            admission_reason_raw_text="PVP-FALSE-APV",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="AS-PVP-TRN",
            facility="GRA",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="SCI",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            specialized_purpose_for_incarceration_raw_text="PVP-S",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text=StateCustodialAuthority.STATE_PRISON.value,
        )
        p4_is_1_ip_15 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="JE1989-15",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_sentences=[p4_is_1],
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=2005, month=6, day=14),
            release_date=datetime.date(year=2005, month=10, day=2),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="PVP-FALSE-TRN",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="AS-TCV-SC",
            facility="SMI",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="SCI",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            specialized_purpose_for_incarceration_raw_text="PVP-S",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text=StateCustodialAuthority.STATE_PRISON.value,
        )
        p4_is_1_ip_16 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="JE1989-16",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_sentences=[p4_is_1],
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=2005, month=10, day=2),
            release_date=datetime.date(year=2007, month=9, day=4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            admission_reason_raw_text="TCV-TRUE-SC",
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="P-RP-D",
            facility="SMI",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="SCI",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="TCV-S",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text=StateCustodialAuthority.STATE_PRISON.value,
        )
        p4_is_1_ip_18 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="JE1989-18",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_sentences=[p4_is_1],
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=2008, month=3, day=31),
            release_date=datetime.date(year=2008, month=4, day=22),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            admission_reason_raw_text="PVP-FALSE-APV",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="AS-PVP-TRN",
            facility="GRA",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="SCI",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            specialized_purpose_for_incarceration_raw_text="PVP-S",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text=StateCustodialAuthority.STATE_PRISON.value,
        )
        p4_is_1_ip_21 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="JE1989-21",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_sentences=[p4_is_1],
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=2008, month=4, day=22),
            release_date=datetime.date(year=2008, month=5, day=14),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="PVP-FALSE-TRN",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="AS-TPV-SC",
            facility="SMI",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="SCI",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            specialized_purpose_for_incarceration_raw_text="PVP-S",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text=StateCustodialAuthority.STATE_PRISON.value,
        )
        p4_is_1_ip_22 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="JE1989-22",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_sentences=[p4_is_1],
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=2008, month=5, day=14),
            release_date=datetime.date(year=2008, month=8, day=19),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            admission_reason_raw_text="TPV-TRUE-SC",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="AS-TPV-TRN",
            facility="SMI",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="SCI",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="TPV-S",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text=StateCustodialAuthority.STATE_PRISON.value,
        )
        p4_is_1_ip_24 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="JE1989-24",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_sentences=[p4_is_1],
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=2008, month=8, day=19),
            release_date=datetime.date(year=2009, month=8, day=13),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="TPV-FALSE-TRN",
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="P-RP-D",
            facility="CHS",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="SCI",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="TPV-S",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text=StateCustodialAuthority.STATE_PRISON.value,
        )

        p4_is_1.incarceration_periods = [
            p4_is_1_ip_12,
            p4_is_1_ip_15,
            p4_is_1_ip_16,
            p4_is_1_ip_18,
            p4_is_1_ip_21,
            p4_is_1_ip_22,
            p4_is_1_ip_24,
        ]

        populate_person_backedges(expected_people)

        # Act
        self._run_ingest_job_for_filename("sci_incarceration_period.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # ccis_incarceration_period
        ######################################

        p3_is_ip_9 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="34567",
            state_code=_STATE_CODE_UPPER,
            person=person_3,
            incarceration_sentences=[p3_is],
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=2014, month=11, day=18),
            release_date=datetime.date(year=2015, month=1, day=20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            admission_reason_raw_text="CCIS-TRUE-INRS",
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="CCIS-DC2P",
            facility="231: WERNERSVILLE CCC#30",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            incarceration_type_raw_text="CCIS",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            specialized_purpose_for_incarceration_raw_text="CCIS-51",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="51",
        )
        p3_is_ip_10 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="45678",
            state_code=_STATE_CODE_UPPER,
            person=person_3,
            incarceration_sentences=[p3_is],
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=2016, month=10, day=3),
            release_date=datetime.date(year=2016, month=11, day=14),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            admission_reason_raw_text="CCIS-TRUE-PRCH",
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="CCIS-PTST",
            facility="195: LACKAWANNA COUNTY",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            incarceration_type_raw_text="CCIS",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            specialized_purpose_for_incarceration_raw_text="CCIS-26",
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            custodial_authority_raw_text="26",
        )

        p3_is.incarceration_periods.extend([p3_is_ip_9, p3_is_ip_10])

        p4_is_1_ip_25 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="12345",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_sentences=[p4_is_1],
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=2016, month=12, day=21),
            release_date=datetime.date(year=2017, month=4, day=26),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            admission_reason_raw_text="CCIS-TRUE-INRS",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="CCIS-TRGH",
            facility="136: KINTOCK ERIE",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            incarceration_type_raw_text="CCIS",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            specialized_purpose_for_incarceration_raw_text="CCIS-26",
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            custodial_authority_raw_text="26",
        )

        p4_is_1_ip_26 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="23456",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_sentences=[p4_is_1],
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=2017, month=7, day=3),
            release_date=datetime.date(year=2018, month=1, day=3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="CCIS-FALSE-TRRC",
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="CCIS-DC2P",
            facility="198: CHESTER COUNTY",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            incarceration_type_raw_text="CCIS",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            specialized_purpose_for_incarceration_raw_text="CCIS-46",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="46",
        )

        p4_is_1.incarceration_periods.extend([p4_is_1_ip_25, p4_is_1_ip_26])

        # Act
        self._run_ingest_job_for_filename("ccis_incarceration_period.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # dbo_Miscon
        ######################################

        # Arrange
        p3_sg_placeholder = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_3,
        )
        person_3.sentence_groups.append(p3_sg_placeholder)
        p3_is_placeholder = entities.StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            person=person_3,
            sentence_group=p3_sg_placeholder,
        )
        p3_sg_placeholder.incarceration_sentences.append(p3_is_placeholder)

        p3_ip_placeholder = entities.StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            person=person_3,
            incarceration_sentences=[p3_is_placeholder],
        )
        p3_is_placeholder.incarceration_periods.append(p3_ip_placeholder)

        p3_ii = entities.StateIncarcerationIncident.new_with_defaults(
            external_id="A123456",
            state_code=_STATE_CODE_UPPER,
            person=person_3,
            incarceration_period=p3_ip_placeholder,
            incident_type=StateIncarcerationIncidentType.REPORT,
            incident_type_raw_text="REPORT",
            incident_date=datetime.date(year=2018, month=5, day=10),
            facility="WAM",
            incident_details=json.dumps(
                {
                    "CATEGORY_1": "",
                    "CATEGORY_2": "",
                    "CATEGORY_3": "",
                    "CATEGORY_4": "",
                    "CATEGORY_5": "",
                }
            ),
        )
        p3_ii_outcome = entities.StateIncarcerationIncidentOutcome.new_with_defaults(
            external_id="A123456",
            state_code=_STATE_CODE_UPPER,
            person=person_3,
            incarceration_incident=p3_ii,
            outcome_type=StateIncarcerationIncidentOutcomeType.RESTRICTED_CONFINEMENT,
            outcome_type_raw_text="Y",
            date_effective=datetime.date(year=2018, month=5, day=17),
            report_date=datetime.date(year=2018, month=5, day=16),
        )
        p3_ii.incarceration_incident_outcomes.append(p3_ii_outcome)
        p3_ip_placeholder.incarceration_incidents.append(p3_ii)

        p4_sg_placeholder = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_4,
        )
        person_4.sentence_groups.append(p4_sg_placeholder)

        p4_is_placeholder = entities.StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            person=person_4,
            sentence_group=p4_sg_placeholder,
        )
        p4_sg_placeholder.incarceration_sentences.append(p4_is_placeholder)

        p4_ip_placeholder = entities.StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            person=person_4,
            incarceration_sentences=[p4_is_placeholder],
        )
        p4_is_placeholder.incarceration_periods.append(p4_ip_placeholder)

        p4_ii_1 = entities.StateIncarcerationIncident.new_with_defaults(
            external_id="A234567",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_period=p4_ip_placeholder,
            incident_type=StateIncarcerationIncidentType.REPORT,
            incident_type_raw_text="REPORT",
            incident_date=datetime.date(year=1991, month=3, day=6),
            facility="GRA",
            location_within_facility="CELL-AA UNIT",
            incident_details=json.dumps(
                {
                    "CATEGORY_1": "",
                    "CATEGORY_2": "X",
                    "CATEGORY_3": "X",
                    "CATEGORY_4": "",
                    "CATEGORY_5": "",
                }
            ),
        )
        p4_ii_1_outcome = entities.StateIncarcerationIncidentOutcome.new_with_defaults(
            external_id="A234567",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_incident=p4_ii_1,
            outcome_type=StateIncarcerationIncidentOutcomeType.CELL_CONFINEMENT,
            outcome_type_raw_text="C",
            date_effective=datetime.date(year=1991, month=3, day=8),
            report_date=datetime.date(year=1991, month=3, day=7),
            hearing_date=datetime.date(year=1991, month=3, day=6),
        )
        p4_ii_1.incarceration_incident_outcomes.append(p4_ii_1_outcome)
        p4_ip_placeholder.incarceration_incidents.append(p4_ii_1)

        p4_ii_2 = entities.StateIncarcerationIncident.new_with_defaults(
            external_id="B222333",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_period=p4_ip_placeholder,
            incident_type=StateIncarcerationIncidentType.REPORT,
            incident_type_raw_text="REPORT",
            incident_date=datetime.date(year=1993, month=7, day=6),
            facility="SMI",
            incident_details=json.dumps(
                {
                    "CATEGORY_1": "",
                    "CATEGORY_2": "",
                    "CATEGORY_3": "",
                    "CATEGORY_4": "",
                    "CATEGORY_5": "",
                }
            ),
        )
        p4_ii_2_outcome = entities.StateIncarcerationIncidentOutcome.new_with_defaults(
            external_id="B222333",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_incident=p4_ii_2,
            outcome_type=StateIncarcerationIncidentOutcomeType.RESTRICTED_CONFINEMENT,
            outcome_type_raw_text="Y",
            date_effective=datetime.date(year=1993, month=7, day=6),
            report_date=datetime.date(year=1993, month=7, day=6),
        )
        p4_ii_2.incarceration_incident_outcomes.append(p4_ii_2_outcome)
        p4_ip_placeholder.incarceration_incidents.append(p4_ii_2)

        p4_ii_3 = entities.StateIncarcerationIncident.new_with_defaults(
            external_id="B444555",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_period=p4_ip_placeholder,
            incident_type=StateIncarcerationIncidentType.CONTRABAND,
            incident_type_raw_text="CONTRABAND",
            incident_date=datetime.date(year=1993, month=12, day=17),
            facility="SMI",
            location_within_facility="RHU-A 200",
            incident_details=json.dumps(
                {
                    "CATEGORY_1": "",
                    "CATEGORY_2": "X",
                    "CATEGORY_3": "",
                    "CATEGORY_4": "",
                    "CATEGORY_5": "",
                }
            ),
        )
        p4_ii_3_outcome = entities.StateIncarcerationIncidentOutcome.new_with_defaults(
            external_id="B444555",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_incident=p4_ii_3,
            report_date=datetime.date(year=1993, month=12, day=17),
            hearing_date=datetime.date(year=1993, month=12, day=18),
        )
        p4_ii_3.incarceration_incident_outcomes.append(p4_ii_3_outcome)
        p4_ip_placeholder.incarceration_incidents.append(p4_ii_3)

        p4_ii_4 = entities.StateIncarcerationIncident.new_with_defaults(
            external_id="B444556",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_period=p4_ip_placeholder,
            incident_type=StateIncarcerationIncidentType.CONTRABAND,
            incident_type_raw_text="CONTRABAND",
            incident_date=datetime.date(year=1993, month=12, day=17),
            facility="SMI",
            location_within_facility="RHU-A 200",
            incident_details=json.dumps(
                {
                    "CATEGORY_1": "",
                    "CATEGORY_2": "X",
                    "CATEGORY_3": "",
                    "CATEGORY_4": "",
                    "CATEGORY_5": "",
                }
            ),
        )
        p4_ii_4_outcome = entities.StateIncarcerationIncidentOutcome.new_with_defaults(
            external_id="B444556",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_incident=p4_ii_4,
        )
        p4_ii_4.incarceration_incident_outcomes.append(p4_ii_4_outcome)
        p4_ip_placeholder.incarceration_incidents.append(p4_ii_4)

        populate_person_backedges(expected_people)

        # Act
        self._run_ingest_job_for_filename("dbo_Miscon_v2.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # dbo_Offender
        ######################################
        # Arrange
        person_1.gender_raw_text = "M"
        person_1.races[0].race_raw_text = "B"

        person_2.gender_raw_text = "M"
        person_2.races.append(
            entities.StatePersonRace.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                race=Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
                race_raw_text="I",
            )
        )
        person_2.ethnicities.append(
            entities.StatePersonEthnicity.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                ethnicity=Ethnicity.HISPANIC,
                ethnicity_raw_text="H",
            )
        )

        person_4.gender_raw_text = "M"
        person_4.races = [
            entities.StatePersonRace.new_with_defaults(
                state_code=_STATE_CODE_UPPER, race=Race.WHITE, race_raw_text="W"
            )
        ]

        person_5.gender = Gender.FEMALE
        person_5.gender_raw_text = "F"
        person_5.races = [
            entities.StatePersonRace.new_with_defaults(
                state_code=_STATE_CODE_UPPER, race=Race.OTHER, race_raw_text="N"
            ),
        ]

        person_6 = entities.StatePerson.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            gender=Gender.OTHER,
            gender_raw_text="N",
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, external_id="111A", id_type=US_PA_PBPP
                ),
            ],
            races=[
                entities.StatePersonRace.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, race=Race.WHITE, race_raw_text="W"
                )
            ],
        )

        expected_people.append(person_6)

        populate_person_backedges(expected_people)

        # Act
        self._run_ingest_job_for_filename("dbo_Offender.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # dbo_LSIR
        ######################################
        # Arrange
        person_2_pbpp_assessments = [
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.RISK,
                assessment_class_raw_text="RISK",
                assessment_type=StateAssessmentType.LSIR,
                assessment_type_raw_text="LSIR",
                assessment_level=StateAssessmentLevel.MEDIUM,
                assessment_level_raw_text="MEDIUM",
                assessment_score=23,
                assessment_date=datetime.date(year=2005, month=12, day=22),
                external_id="456B-1-1",
                state_code=_STATE_CODE_UPPER,
                person=person_2,
            ),
        ]
        person_2.assessments.extend(person_2_pbpp_assessments)

        person_4_pbpp_assessments = [
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.RISK,
                assessment_class_raw_text="RISK",
                assessment_type=StateAssessmentType.LSIR,
                assessment_type_raw_text="LSIR",
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_level_raw_text="HIGH",
                assessment_score=30,
                assessment_date=datetime.date(year=2006, month=1, day=19),
                external_id="345E-3-1",
                state_code=_STATE_CODE_UPPER,
                person=person_4,
            ),
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.RISK,
                assessment_class_raw_text="RISK",
                assessment_type=StateAssessmentType.LSIR,
                assessment_type_raw_text="LSIR",
                assessment_level=StateAssessmentLevel.EXTERNAL_UNKNOWN,
                assessment_level_raw_text="UNKNOWN (60-ATTEMPTED_INCOMPLETE)",
                assessment_score=None,
                assessment_date=datetime.date(year=2006, month=8, day=3),
                external_id="345E-3-2",
                state_code=_STATE_CODE_UPPER,
                person=person_4,
            ),
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.RISK,
                assessment_class_raw_text="RISK",
                assessment_type=StateAssessmentType.LSIR,
                assessment_type_raw_text="LSIR",
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_level_raw_text="HIGH",
                assessment_score=31,
                assessment_date=datetime.date(year=2007, month=1, day=15),
                external_id="345E-3-3",
                state_code=_STATE_CODE_UPPER,
                person=person_4,
            ),
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.RISK,
                assessment_class_raw_text="RISK",
                assessment_type=StateAssessmentType.LSIR,
                assessment_type_raw_text="LSIR",
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_level_raw_text="HIGH",
                assessment_score=33,
                assessment_date=datetime.date(year=2007, month=7, day=14),
                external_id="345E-4-1",
                state_code=_STATE_CODE_UPPER,
                person=person_4,
            ),
        ]
        person_4.assessments.extend(person_4_pbpp_assessments)

        person_5_pbpp_assessments = [
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.RISK,
                assessment_class_raw_text="RISK",
                assessment_type=StateAssessmentType.LSIR,
                assessment_type_raw_text="LSIR",
                assessment_level=StateAssessmentLevel.LOW,
                assessment_level_raw_text="LOW",
                assessment_score=14,
                assessment_date=datetime.date(year=2001, month=1, day=31),
                external_id="789C-0-1",
                state_code=_STATE_CODE_UPPER,
                person=person_5,
            ),
        ]
        person_5.assessments.extend(person_5_pbpp_assessments)

        populate_person_backedges(expected_people)

        # Act
        self._run_ingest_job_for_filename("dbo_LSIR.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # supervision_period
        ######################################

        # Arrange
        p2_sg_ph = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_2,
        )
        person_2.sentence_groups.append(p2_sg_ph)

        p2_ss_ph = entities.StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_2,
            sentence_group=p2_sg_ph,
        )
        p2_sg_ph.supervision_sentences.append(p2_ss_ph)

        p2_sp_1_1 = entities.StateSupervisionPeriod.new_with_defaults(
            external_id="456B-1",
            state_code=_STATE_CODE_UPPER,
            status=StateSupervisionPeriodStatus.TERMINATED,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_period_supervision_type_raw_text="C2",
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            admission_reason_raw_text="C2",
            start_date=datetime.date(year=2012, month=3, day=16),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            termination_reason_raw_text="TRANSFER_WITHIN_STATE",
            termination_date=datetime.date(year=2013, month=4, day=1),
            county_code="ALLEGH",
            supervision_site="02|02 - NORTH SHORE|7124",
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            custodial_authority_raw_text="C2",
            supervision_level=StateSupervisionLevel.MAXIMUM,
            supervision_level_raw_text="MAX",
            conditions="MEST, ACT35, GPAR, MVICT, REMC, END",
            supervising_officer=entities.StateAgent.new_with_defaults(
                external_id="555678",
                state_code=_STATE_CODE_UPPER,
                full_name='{"full_name": "KRAMER, COSMO"}',
                agent_type=StateAgentType.SUPERVISION_OFFICER,
                agent_type_raw_text="SUPERVISION_OFFICER",
            ),
            supervision_sentences=[p2_ss_ph],
        )

        p2_sp_1_2 = entities.StateSupervisionPeriod.new_with_defaults(
            external_id="456B-2",
            state_code=_STATE_CODE_UPPER,
            status=StateSupervisionPeriodStatus.TERMINATED,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
            supervision_period_supervision_type_raw_text="04,C2",
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            admission_reason_raw_text="TRANSFER_WITHIN_STATE",
            start_date=datetime.date(year=2013, month=4, day=1),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            termination_reason_raw_text="TRANSFER_WITHIN_STATE",
            termination_date=datetime.date(year=2014, month=8, day=10),
            county_code="ALLEGH",
            supervision_site="02|02 - NORTH SHORE|7113",
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            custodial_authority_raw_text="04",
            supervision_level=StateSupervisionLevel.MAXIMUM,
            supervision_level_raw_text="MAX",
            conditions="MEST, ACT35, GPAR, MVICT, REMC, END",
            supervision_sentences=[p2_ss_ph],
        )

        p2_sp_2_1 = entities.StateSupervisionPeriod.new_with_defaults(
            external_id="456B-3",
            state_code=_STATE_CODE_UPPER,
            status=StateSupervisionPeriodStatus.TERMINATED,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_period_supervision_type_raw_text="04",
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            admission_reason_raw_text="TRANSFER_WITHIN_STATE",
            start_date=datetime.date(year=2014, month=8, day=10),
            termination_reason=StateSupervisionPeriodTerminationReason.EXPIRATION,
            termination_reason_raw_text="43",
            termination_date=datetime.date(year=2018, month=1, day=1),
            county_code="ALLEGH",
            supervision_site="02|02 - NORTH SHORE|7115",
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            custodial_authority_raw_text="04",
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MED",
            conditions="PN, EST, BL, END, SUBD, AANA",
            supervising_officer=entities.StateAgent.new_with_defaults(
                external_id="444123",
                state_code=_STATE_CODE_UPPER,
                full_name='{"full_name": "BENES, ELAINE"}',
                agent_type=StateAgentType.SUPERVISION_OFFICER,
                agent_type_raw_text="SUPERVISION_OFFICER",
            ),
            supervision_sentences=[p2_ss_ph],
        )

        p2_ss_ph.supervision_periods.extend([p2_sp_1_1, p2_sp_1_2, p2_sp_2_1])

        p5_sg_ph = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_5,
        )
        person_5.sentence_groups.append(p5_sg_ph)

        p5_ss_ph = entities.StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_5,
            sentence_group=p5_sg_ph,
        )
        p5_sg_ph.supervision_sentences.append(p5_ss_ph)

        p5_sp_1_1 = entities.StateSupervisionPeriod.new_with_defaults(
            external_id="789C-1",
            state_code=_STATE_CODE_UPPER,
            status=StateSupervisionPeriodStatus.TERMINATED,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_period_supervision_type_raw_text="05",
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            admission_reason_raw_text="05",
            start_date=datetime.date(year=2003, month=10, day=10),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            termination_reason_raw_text="42",
            termination_date=datetime.date(year=2004, month=8, day=10),
            county_code="MERCER",
            supervision_site="08|08 - MERCER DO|7307",
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            custodial_authority_raw_text="05",
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text="MIN",
            conditions="START, EST, END, AANA, REL, DAM, PARAB, ACT35, BL, MISC, DDU, URI, GPAR, EMP, ALC, PM, PF, PA",
            supervising_officer=entities.StateAgent.new_with_defaults(
                external_id="888888",
                state_code=_STATE_CODE_UPPER,
                full_name='{"full_name": "BANIA, K"}',
                agent_type=StateAgentType.SUPERVISION_OFFICER,
                agent_type_raw_text="SUPERVISION_OFFICER",
            ),
            supervision_sentences=[p5_ss_ph],
        )
        p5_sp_2_1 = entities.StateSupervisionPeriod.new_with_defaults(
            external_id="789C-2",
            state_code=_STATE_CODE_UPPER,
            status=StateSupervisionPeriodStatus.TERMINATED,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_period_supervision_type_raw_text="05",
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            admission_reason_raw_text="05",
            start_date=datetime.date(year=2004, month=8, day=10),
            termination_reason=StateSupervisionPeriodTerminationReason.RETURN_TO_INCARCERATION,
            termination_reason_raw_text="44",
            termination_date=datetime.date(year=2005, month=12, day=31),
            county_code="MERCER",
            supervision_site="08|08 - MERCER DO|7307",
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            custodial_authority_raw_text="05",
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text="MIN",
            supervising_officer=entities.StateAgent.new_with_defaults(
                external_id="555",
                state_code=_STATE_CODE_UPPER,
                full_name='{"full_name": "SEINFELD, JERRY"}',
                agent_type=StateAgentType.SUPERVISION_OFFICER,
                agent_type_raw_text="SUPERVISION_OFFICER",
            ),
            supervision_sentences=[p5_ss_ph],
        )
        p5_sp_3_1 = entities.StateSupervisionPeriod.new_with_defaults(
            external_id="789C-3",
            state_code=_STATE_CODE_UPPER,
            status=StateSupervisionPeriodStatus.TERMINATED,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_period_supervision_type_raw_text="04",
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            admission_reason_raw_text="04",
            start_date=datetime.date(year=2005, month=12, day=31),
            termination_reason=StateSupervisionPeriodTerminationReason.EXPIRATION,
            termination_reason_raw_text="43",
            termination_date=datetime.date(year=2006, month=10, day=10),
            county_code="MERCER",
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            custodial_authority_raw_text="04",
            supervision_level=StateSupervisionLevel.LIMITED,
            supervision_level_raw_text="ADM",
            supervising_officer=entities.StateAgent.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                full_name='{"full_name": "NEWMAN"}',
                agent_type=StateAgentType.SUPERVISION_OFFICER,
                agent_type_raw_text="SUPERVISION_OFFICER",
            ),
            supervision_sentences=[p5_ss_ph],
        )
        p5_ss_ph.supervision_periods.extend([p5_sp_1_1, p5_sp_2_1, p5_sp_3_1])

        p4_sg_ph = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_4,
        )
        person_4.sentence_groups.append(p4_sg_ph)

        p4_ss_ph = entities.StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_4,
            sentence_group=p4_sg_ph,
        )
        p4_sg_ph.supervision_sentences.append(p4_ss_ph)

        p4_sp_1_1 = entities.StateSupervisionPeriod.new_with_defaults(
            external_id="345E-1",
            state_code=_STATE_CODE_UPPER,
            status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_period_supervision_type_raw_text="03",
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            admission_reason_raw_text="03",
            start_date=datetime.date(year=2016, month=1, day=14),
            county_code="PHILAD",
            supervision_site="01|01 - NORTHWEST|5112",
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            custodial_authority_raw_text="03",
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MED",
            conditions="DOMV, AUTO, DDU, REFR, MNOAM, END, RI, RC, MEMON, MCURF, REFC",
            supervising_officer=entities.StateAgent.new_with_defaults(
                external_id="101010",
                state_code=_STATE_CODE_UPPER,
                full_name='{"full_name": "TALKER, CLOSE"}',
                agent_type=StateAgentType.SUPERVISION_OFFICER,
                agent_type_raw_text="SUPERVISION_OFFICER",
            ),
            supervision_sentences=[p4_ss_ph],
        )

        p4_ss_ph.supervision_periods.append(p4_sp_1_1)

        populate_person_backedges(expected_people)

        # Act
        self._run_ingest_job_for_filename("supervision_period_v2.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # supervision_violation
        ######################################

        # Arrange
        p2_sv_1 = entities.StateSupervisionViolation.new_with_defaults(
            external_id="456B-1-1",
            state_code=_STATE_CODE_UPPER,
            violation_date=datetime.date(year=2014, month=1, day=1),
            person=person_2,
            supervision_periods=[p2_sp_1_2],
        )
        p2_sv_1_te_10 = entities.StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            person=person_2,
            supervision_violation=p2_sv_1,
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="H10",
        )
        p2_sv_1_te_4 = entities.StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            person=person_2,
            supervision_violation=p2_sv_1,
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="L03",
        )
        p2_sv_1_te_5 = entities.StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            person=person_2,
            supervision_violation=p2_sv_1,
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="L05",
        )
        p2_sv_1_te_6 = entities.StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            person=person_2,
            supervision_violation=p2_sv_1,
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="M01",
        )
        p2_sv_1_te_7 = entities.StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            person=person_2,
            supervision_violation=p2_sv_1,
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="M02",
        )
        p2_sv_1_te_8 = entities.StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            person=person_2,
            supervision_violation=p2_sv_1,
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="H03",
        )
        p2_sv_1_te_9 = entities.StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            person=person_2,
            supervision_violation=p2_sv_1,
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="H07",
        )
        p2_sv_1.supervision_violation_types.extend(
            [
                p2_sv_1_te_10,
                p2_sv_1_te_4,
                p2_sv_1_te_5,
                p2_sv_1_te_6,
                p2_sv_1_te_7,
                p2_sv_1_te_8,
                p2_sv_1_te_9,
            ]
        )

        p2_sv_1_c_10 = (
            entities.StateSupervisionViolatedConditionEntry.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                person=person_2,
                supervision_violation=p2_sv_1,
                condition="5",
            )
        )
        p2_sv_1_c_5 = entities.StateSupervisionViolatedConditionEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            person=person_2,
            supervision_violation=p2_sv_1,
            condition="7",
        )
        p2_sv_1_c_6 = entities.StateSupervisionViolatedConditionEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            person=person_2,
            supervision_violation=p2_sv_1,
            condition="3",
        )
        p2_sv_1.supervision_violated_conditions.extend(
            [p2_sv_1_c_10, p2_sv_1_c_5, p2_sv_1_c_6]
        )

        p2_sv_2 = entities.StateSupervisionViolation.new_with_defaults(
            external_id="456B-2-1",
            state_code=_STATE_CODE_UPPER,
            violation_date=datetime.date(year=2015, month=4, day=13),
            person=person_2,
            supervision_periods=[p2_sp_2_1],
        )
        p2_sv_2_te_14 = entities.StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            person=person_2,
            supervision_violation=p2_sv_2,
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="H08",
        )
        p2_sv_2.supervision_violation_types.append(p2_sv_2_te_14)

        p2_sv_2_c_14 = (
            entities.StateSupervisionViolatedConditionEntry.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                person=person_2,
                supervision_violation=p2_sv_2,
                condition="5",
            )
        )
        p2_sv_2.supervision_violated_conditions.append(p2_sv_2_c_14)

        p2_sp_1_2.supervision_violation_entries.append(p2_sv_1)
        p2_sp_2_1.supervision_violation_entries.append(p2_sv_2)

        p2_sg_placeholder = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_2,
        )
        person_2.sentence_groups.append(p2_sg_placeholder)

        p2_ss_placeholder = entities.StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group=p2_sg_placeholder,
            person=person_2,
        )
        p2_sg_placeholder.supervision_sentences.append(p2_ss_placeholder)
        p2_sp_placeholder = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[p2_ss_placeholder],
            person=person_2,
        )
        p2_ss_placeholder.supervision_periods.append(p2_sp_placeholder)

        p5_sv_3 = entities.StateSupervisionViolation.new_with_defaults(
            external_id="789C-3-1",
            state_code=_STATE_CODE_UPPER,
            violation_date=datetime.date(year=2006, month=8, day=11),
            person=person_5,
            supervision_periods=[p5_sp_3_1],
        )
        p5_sv_3_te_3 = entities.StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            person=person_5,
            supervision_violation=p5_sv_3,
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="H12",
        )
        p5_sv_3_te_7 = entities.StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            person=person_5,
            supervision_violation=p5_sv_3,
            violation_type=StateSupervisionViolationType.LAW,
            violation_type_raw_text="H04",
        )
        p5_sv_3.supervision_violation_types.extend([p5_sv_3_te_3, p5_sv_3_te_7])

        p5_sv_3_c_3 = entities.StateSupervisionViolatedConditionEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            person=person_5,
            supervision_violation=p5_sv_3,
            condition="5",
        )
        p5_sv_3_c_7 = entities.StateSupervisionViolatedConditionEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            person=person_5,
            supervision_violation=p5_sv_3,
            condition="7",
        )
        p5_sv_3.supervision_violated_conditions.extend([p5_sv_3_c_3, p5_sv_3_c_7])

        p5_sp_3_1.supervision_violation_entries.append(p5_sv_3)

        p5_sg_placeholder = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_5,
        )
        person_5.sentence_groups.append(p5_sg_placeholder)

        p5_ss_placeholder = entities.StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group=p5_sg_placeholder,
            person=person_5,
        )
        p5_sg_placeholder.supervision_sentences.append(p5_ss_placeholder)
        p5_sp_placeholder = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[p5_ss_placeholder],
            person=person_5,
        )
        p5_ss_placeholder.supervision_periods.append(p5_sp_placeholder)

        p4_sv_1 = entities.StateSupervisionViolation.new_with_defaults(
            external_id="345E-1-1",
            state_code=_STATE_CODE_UPPER,
            violation_date=datetime.date(year=2018, month=3, day=17),
            person=person_4,
            supervision_periods=[p4_sp_1_1],
        )
        p4_sv_1_te_3 = entities.StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            supervision_violation=p4_sv_1,
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="L08",
        )
        p4_sv_1.supervision_violation_types.append(p4_sv_1_te_3)

        p4_sv_1_c_3 = entities.StateSupervisionViolatedConditionEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            supervision_violation=p4_sv_1,
            condition="5",
        )
        p4_sv_1.supervision_violated_conditions.append(p4_sv_1_c_3)

        p4_sv_2 = entities.StateSupervisionViolation.new_with_defaults(
            external_id="345E-1-2",
            state_code=_STATE_CODE_UPPER,
            violation_date=datetime.date(year=2018, month=5, day=12),
            person=person_4,
            supervision_periods=[p4_sp_1_1],
        )
        p4_sv_2_te_7 = entities.StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            supervision_violation=p4_sv_2,
            violation_type=StateSupervisionViolationType.LAW,
            violation_type_raw_text="M13",
        )
        p4_sv_2_te_8 = entities.StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            supervision_violation=p4_sv_2,
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="M14",
        )
        p4_sv_2.supervision_violation_types.extend([p4_sv_2_te_7, p4_sv_2_te_8])

        p4_sv_2_c_7 = entities.StateSupervisionViolatedConditionEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            supervision_violation=p4_sv_2,
            condition="4",
        )
        p4_sv_2_c_8 = entities.StateSupervisionViolatedConditionEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            supervision_violation=p4_sv_2,
            condition="7",
        )
        p4_sv_2.supervision_violated_conditions.extend([p4_sv_2_c_7, p4_sv_2_c_8])

        p4_sp_1_1.supervision_violation_entries.extend([p4_sv_1, p4_sv_2])

        p4_sg_placeholder_2 = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_4,
        )
        person_4.sentence_groups.append(p4_sg_placeholder_2)

        p4_ss_placeholder = entities.StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group=p4_sg_placeholder_2,
            person=person_4,
        )
        p4_sg_placeholder_2.supervision_sentences.append(p4_ss_placeholder)
        p4_sp_placeholder = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[p4_ss_placeholder],
            person=person_4,
        )
        p4_ss_placeholder.supervision_periods.append(p4_sp_placeholder)

        populate_person_backedges(expected_people)

        # Act
        self._run_ingest_job_for_filename("supervision_violation.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # supervision_violation_response
        ######################################

        # Arrange
        p2_svr_1 = entities.StateSupervisionViolationResponse.new_with_defaults(
            external_id="456B-1-1",
            state_code=_STATE_CODE_UPPER,
            response_date=datetime.date(year=2013, month=1, day=2),
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_type_raw_text="VIOLATION_REPORT",
            person=person_2,
            supervision_violation=p2_sv_1,
        )
        p2_svr_1_d_2 = entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            person=person_2,
            supervision_violation_response=p2_svr_1,
            decision=StateSupervisionViolationResponseDecision.WARNING,
            decision_raw_text="WTWR",
            revocation_type=StateSupervisionViolationResponseRevocationType.RETURN_TO_SUPERVISION,
            revocation_type_raw_text="WTWR",
        )
        p2_svr_1_d_3 = entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            person=person_2,
            supervision_violation_response=p2_svr_1,
            decision=StateSupervisionViolationResponseDecision.NEW_CONDITIONS,
            decision_raw_text="DJBS",
            revocation_type=StateSupervisionViolationResponseRevocationType.RETURN_TO_SUPERVISION,
            revocation_type_raw_text="DJBS",
        )
        p2_svr_1.supervision_violation_response_decisions.extend(
            [p2_svr_1_d_2, p2_svr_1_d_3]
        )
        p2_sv_1.supervision_violation_responses.append(p2_svr_1)

        p2_svr_2 = entities.StateSupervisionViolationResponse.new_with_defaults(
            external_id="456B-2-1",
            state_code=_STATE_CODE_UPPER,
            response_date=datetime.date(year=2015, month=4, day=13),
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_type_raw_text="VIOLATION_REPORT",
            person=person_2,
            supervision_violation=p2_sv_2,
        )
        p2_svr_2_d_12 = entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            person=person_2,
            supervision_violation_response=p2_svr_2,
            decision=StateSupervisionViolationResponseDecision.REVOCATION,
            decision_raw_text="VCCF",
            revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
            revocation_type_raw_text="VCCF",
        )
        p2_svr_2_d_13 = entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            person=person_2,
            supervision_violation_response=p2_svr_2,
            decision=StateSupervisionViolationResponseDecision.REVOCATION,
            decision_raw_text="ARR2",
            revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
            revocation_type_raw_text="ARR2",
        )
        p2_svr_2.supervision_violation_response_decisions.extend(
            [p2_svr_2_d_12, p2_svr_2_d_13]
        )
        p2_sv_2.supervision_violation_responses.append(p2_svr_2)

        p5_svr_3 = entities.StateSupervisionViolationResponse.new_with_defaults(
            external_id="789C-3-1",
            state_code=_STATE_CODE_UPPER,
            response_date=datetime.date(year=2006, month=8, day=16),
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_type_raw_text="VIOLATION_REPORT",
            person=person_5,
            supervision_violation=p5_sv_3,
        )
        p5_svr_3_d_4 = entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            person=person_5,
            supervision_violation_response=p5_svr_3,
            decision=StateSupervisionViolationResponseDecision.REVOCATION,
            decision_raw_text="VCCF",
            revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
            revocation_type_raw_text="VCCF",
        )
        p5_svr_3_d_5 = entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            person=person_5,
            supervision_violation_response=p5_svr_3,
            decision=StateSupervisionViolationResponseDecision.REVOCATION,
            decision_raw_text="ARR2",
            revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
            revocation_type_raw_text="ARR2",
        )
        p5_svr_3.supervision_violation_response_decisions.extend(
            [p5_svr_3_d_4, p5_svr_3_d_5]
        )
        p5_sv_3.supervision_violation_responses.append(p5_svr_3)

        p4_svr_1 = entities.StateSupervisionViolationResponse.new_with_defaults(
            external_id="345E-1-1",
            state_code=_STATE_CODE_UPPER,
            response_date=datetime.date(year=2018, month=3, day=23),
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_type_raw_text="VIOLATION_REPORT",
            person=person_4,
            supervision_violation=p4_sv_1,
        )
        p4_svr_1_d_2 = entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            supervision_violation_response=p4_svr_1,
            decision=StateSupervisionViolationResponseDecision.WARNING,
            decision_raw_text="WTWR",
            revocation_type=StateSupervisionViolationResponseRevocationType.RETURN_TO_SUPERVISION,
            revocation_type_raw_text="WTWR",
        )
        p4_svr_1.supervision_violation_response_decisions.append(p4_svr_1_d_2)
        p4_sv_1.supervision_violation_responses.append(p4_svr_1)

        p4_svr_2 = entities.StateSupervisionViolationResponse.new_with_defaults(
            external_id="345E-1-2",
            state_code=_STATE_CODE_UPPER,
            response_date=datetime.date(year=2018, month=5, day=13),
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_type_raw_text="VIOLATION_REPORT",
            person=person_4,
            supervision_violation=p4_sv_2,
        )
        p4_svr_1_d_5 = entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            supervision_violation_response=p4_svr_2,
            decision=StateSupervisionViolationResponseDecision.REVOCATION,
            decision_raw_text="ARR2",
            revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
            revocation_type_raw_text="ARR2",
        )
        p4_svr_1_d_6 = entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            supervision_violation_response=p4_svr_2,
            decision=StateSupervisionViolationResponseDecision.INTERNAL_UNKNOWN,
            decision_raw_text="PV01",
            revocation_type=StateSupervisionViolationResponseRevocationType.RETURN_TO_SUPERVISION,
            revocation_type_raw_text="PV01",
        )
        p4_svr_2.supervision_violation_response_decisions.extend(
            [p4_svr_1_d_5, p4_svr_1_d_6]
        )
        p4_sv_2.supervision_violation_responses.append(p4_svr_2)

        populate_person_backedges(expected_people)

        # Act
        self._run_ingest_job_for_filename("supervision_violation_response.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # board_action
        ######################################

        # Arrange
        p1_placeholder_sg = entities.StateSentenceGroup.new_with_defaults(
            person=person_1,
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        p1_placeholder_ss = entities.StateSupervisionSentence.new_with_defaults(
            person=person_1,
            sentence_group=p1_placeholder_sg,
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        p1_placeholder_sp = entities.StateSupervisionPeriod.new_with_defaults(
            person=person_1,
            supervision_sentences=p1_placeholder_sg.supervision_sentences,
            state_code=_STATE_CODE_UPPER,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )
        p1_placeholder_sv = entities.StateSupervisionViolation.new_with_defaults(
            person=person_1,
            supervision_periods=p1_placeholder_ss.supervision_periods,
            state_code=_STATE_CODE_UPPER,
        )
        p1_placeholder_vr = entities.StateSupervisionViolationResponse.new_with_defaults(
            person=person_1,
            supervision_violation=p1_placeholder_sv,
            external_id="123A-1-09",
            state_code=_STATE_CODE_UPPER,
            response_date=datetime.date(year=2004, month=6, day=16),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="PERMANENT_DECISION",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text="PAROLE_BOARD",
        )
        p1_placeholder_de = entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
            person=person_1,
            supervision_violation_response=p1_placeholder_vr,
            state_code=_STATE_CODE_UPPER,
            decision_raw_text="RESCR",
            revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
            decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            revocation_type_raw_text="RESCR",
        )

        p1_placeholder_sg.supervision_sentences.append(p1_placeholder_ss)
        p1_placeholder_ss.supervision_periods.append(p1_placeholder_sp)
        p1_placeholder_sp.supervision_violation_entries.append(p1_placeholder_sv)
        p1_placeholder_sv.supervision_violation_responses.append(p1_placeholder_vr)
        p1_placeholder_vr.supervision_violation_response_decisions.append(
            p1_placeholder_de
        )
        person_1.sentence_groups.append(p1_placeholder_sg)

        p2_placeholder_sg = entities.StateSentenceGroup.new_with_defaults(
            person=person_2,
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        p2_placeholder_ss = entities.StateSupervisionSentence.new_with_defaults(
            person=person_2,
            sentence_group=p2_placeholder_sg,
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        p2_placeholder_sp = entities.StateSupervisionPeriod.new_with_defaults(
            person=person_2,
            supervision_sentences=p2_placeholder_sg.supervision_sentences,
            state_code=_STATE_CODE_UPPER,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )
        p2_placeholder_sv = entities.StateSupervisionViolation.new_with_defaults(
            person=person_2,
            supervision_periods=[p2_sp_1_2],
            state_code=_STATE_CODE_UPPER,
        )
        p2_placeholder_vr = entities.StateSupervisionViolationResponse.new_with_defaults(
            person=person_2,
            supervision_violation=p2_placeholder_sv,
            external_id="456B-0-04",
            state_code=_STATE_CODE_UPPER,
            response_date=datetime.date(year=2014, month=2, day=24),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="PERMANENT_DECISION",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text="PAROLE_BOARD",
        )
        p2_placeholder_de = entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
            person=person_2,
            supervision_violation_response=p2_placeholder_vr,
            state_code=_STATE_CODE_UPPER,
            decision_raw_text="RESCR9",
            revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
            decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            revocation_type_raw_text="RESCR9",
        )

        p2_placeholder_sg.supervision_sentences.append(p2_placeholder_ss)
        p2_placeholder_ss.supervision_periods.append(p2_placeholder_sp)
        person_2.sentence_groups[1].supervision_sentences[0].supervision_periods[
            1
        ].supervision_violation_entries.append(p2_placeholder_sv)
        p2_placeholder_sv.supervision_violation_responses.append(p2_placeholder_vr)
        p2_placeholder_vr.supervision_violation_response_decisions.append(
            p2_placeholder_de
        )
        person_2.sentence_groups.append(p2_placeholder_sg)

        p5_placeholder_sg = entities.StateSentenceGroup.new_with_defaults(
            person=person_5,
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        p5 = entities.StateSupervisionSentence.new_with_defaults(
            person=person_5,
            sentence_group=p5_placeholder_sg,
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        p5_placeholder_sp = entities.StateSupervisionPeriod.new_with_defaults(
            person=person_5,
            supervision_sentences=p5_placeholder_sg.supervision_sentences,
            state_code=_STATE_CODE_UPPER,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )
        p5_placeholder_sv = entities.StateSupervisionViolation.new_with_defaults(
            person=person_5,
            supervision_periods=p5.supervision_periods,
            state_code=_STATE_CODE_UPPER,
        )
        p5_placeholder_vr = entities.StateSupervisionViolationResponse.new_with_defaults(
            person=person_5,
            supervision_violation=p5_placeholder_sv,
            external_id="789C-0-02",
            state_code=_STATE_CODE_UPPER,
            response_date=datetime.date(year=2014, month=7, day=9),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="PERMANENT_DECISION",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text="PAROLE_BOARD",
        )
        p5_placeholder_de = entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
            person=person_5,
            supervision_violation_response=p5_placeholder_vr,
            state_code=_STATE_CODE_UPPER,
            decision_raw_text="RESCR9",
            revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
            decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            revocation_type_raw_text="RESCR9",
        )

        p5_placeholder_sg.supervision_sentences.append(p5)
        p5.supervision_periods.append(p5_placeholder_sp)
        p5_placeholder_sp.supervision_violation_entries.append(p5_placeholder_sv)
        p5_placeholder_sv.supervision_violation_responses.append(p5_placeholder_vr)
        p5_placeholder_vr.supervision_violation_response_decisions.append(
            p5_placeholder_de
        )
        person_5.sentence_groups.append(p5_placeholder_sg)

        p4_placeholder_sg = entities.StateSentenceGroup.new_with_defaults(
            person=person_4,
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        p4_placeholder_ss = entities.StateSupervisionSentence.new_with_defaults(
            person=person_4,
            sentence_group=p4_placeholder_sg,
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        p4_placeholder_sp = entities.StateSupervisionPeriod.new_with_defaults(
            person=person_4,
            supervision_sentences=p4_placeholder_sg.supervision_sentences,
            state_code=_STATE_CODE_UPPER,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )
        p4_placeholder_sv = entities.StateSupervisionViolation.new_with_defaults(
            person=person_4,
            supervision_periods=p4_placeholder_ss.supervision_periods,
            state_code=_STATE_CODE_UPPER,
        )
        p4_placeholder_vr = entities.StateSupervisionViolationResponse.new_with_defaults(
            person=person_4,
            supervision_violation=p4_placeholder_sv,
            external_id="345E-3-11",
            state_code=_STATE_CODE_UPPER,
            response_date=datetime.date(year=2006, month=2, day=21),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="PERMANENT_DECISION",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text="PAROLE_BOARD",
        )
        p4_placeholder_de = entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
            person=person_4,
            supervision_violation_response=p4_placeholder_vr,
            state_code=_STATE_CODE_UPPER,
            decision_raw_text="RESCR",
            revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
            decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            revocation_type_raw_text="RESCR",
        )

        p4_placeholder_sg.supervision_sentences.append(p4_placeholder_ss)
        p4_placeholder_ss.supervision_periods.append(p4_placeholder_sp)
        p4_placeholder_sp.supervision_violation_entries.append(p4_placeholder_sv)
        p4_placeholder_sv.supervision_violation_responses.append(p4_placeholder_vr)
        p4_placeholder_vr.supervision_violation_response_decisions.append(
            p4_placeholder_de
        )
        person_4.sentence_groups.append(p4_placeholder_sg)

        # Act
        self._run_ingest_job_for_filename("board_action.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # Full Rerun for Idempotence
        ######################################

        # Rerun for sanity
        self._do_ingest_job_rerun_for_tags(self.controller.get_file_tag_rank_list())
        # TODO(#5292): The following changes account for entity matching behavior we're seeing on rerun.
        new_placeholder_sp = entities.StateSupervisionPeriod.new_with_defaults(
            person=person_4,
            supervision_sentences=person_4.sentence_groups[3].supervision_sentences,
            state_code="US_PA",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )
        p4_placeholder_sv.supervision_periods = [new_placeholder_sp]
        new_placeholder_sp.supervision_violation_entries.append(p4_placeholder_sv)

        person_4.sentence_groups[3].supervision_sentences[0].supervision_periods.append(
            new_placeholder_sp
        )
        person_4.sentence_groups[5].supervision_sentences[0].supervision_periods[
            0
        ].supervision_violation_entries = []

        # These two placeholder sentence groups get merged during rerun, with the periods from one being moved to the
        # other.
        person_5.sentence_groups.remove(p5_placeholder_sg)
        p5_placeholder_sp.supervision_sentences = person_5.sentence_groups[
            0
        ].supervision_sentences
        person_5.sentence_groups[0].supervision_sentences[0].supervision_periods.append(
            p5_placeholder_sp
        )

        self.assert_expected_db_people(
            expected_people, ignore_dangling_placeholders=True
        )
