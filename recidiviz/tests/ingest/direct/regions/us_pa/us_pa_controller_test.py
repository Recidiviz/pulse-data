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

"""Unit and integration tests for Pennsylvania direct ingest."""
import datetime
import json
from typing import Type

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
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_charge import StateChargeClassificationType
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_incident import (
    StateIncarcerationIncidentOutcomeType,
    StateIncarcerationIncidentType,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateIncarcerationPeriodStatus,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_person_alias import StatePersonAliasType
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
    StateSupervisionContactMethod,
    StateSupervisionContactStatus,
    StateSupervisionContactType,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
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
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.regions.us_pa.us_pa_controller import UsPaController
from recidiviz.ingest.models.ingest_info import (
    IngestInfo,
    StateAgent,
    StateAssessment,
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StatePerson,
    StatePersonExternalId,
    StateSentenceGroup,
    StateSupervisionContact,
    StateSupervisionPeriod,
    StateSupervisionSentence,
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
    def controller_cls(cls) -> Type[BaseDirectIngestController]:
        return UsPaController

    @classmethod
    def schema_type(cls) -> SchemaType:
        return SchemaType.STATE

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

        self.run_legacy_parse_file_test(expected, "dbo_tblInmTestScore")

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
            StateIncarcerationPeriod(
                state_incarceration_period_id="56789",
                incarceration_type="CCIS",
                admission_date="2016-11-23 00:00:00",
                release_date="2016-11-24 00:00:00",
                facility="195: LACKAWANNA COUNTY",
                admission_reason="CCIS-false-PRCH",
                release_reason="CCIS-PTST",
                specialized_purpose_for_incarceration="CCIS-60",
                custodial_authority="60",
                state_parole_decisions=[],
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

        self.run_legacy_parse_file_test(expected, "ccis_incarceration_period")

    def test_populate_data_sci_incarceration_period(self) -> None:
        gf3374_incarceration_periods = [
            StateIncarcerationPeriod(
                state_incarceration_period_id="GF3374-1",
                admission_date="20081010",
                admission_reason="NA-false-AC-FALSE",
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
                admission_reason="NA-false-AC-FALSE",
                release_reason="DC-NA-TRN-FALSE",
                facility="GRA",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="NA-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="CJ1991-2",
                admission_date="20161022",
                release_date="20170602",
                admission_reason="NA-false-TRN-FALSE",
                release_reason="AS-NA-TRN-FALSE",
                facility="CAM",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="NA-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="CJ1991-4",
                admission_date="20170602",
                release_date="20170714",
                admission_reason="NA-false-TRN-FALSE",
                release_reason="P-SP-D-FALSE",
                facility="WAM",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="NA-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="CJ1991-6",
                admission_date="20180310",
                release_date="20180401",
                admission_reason="PVP-false-APV-FALSE",
                release_reason="AS-PVP-TRN-FALSE",
                facility="DAL",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="PVP-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="CJ1991-7",
                admission_date="20180401",
                release_date="20180723",
                admission_reason="PVP-false-TRN-FALSE",
                release_reason="AS-TPV-SC-FALSE",
                facility="WAM",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="PVP-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="CJ1991-8",
                admission_date="20180723",
                release_date="20180914",
                admission_reason="TPV-true-SC-FALSE",
                release_reason="P-RP-D-FALSE",
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
                admission_reason="NA-false-AC-FALSE",
                release_reason="DC-NA-TRN-FALSE",
                facility="GRA",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="NA-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1977-2",
                admission_date="19910308",
                release_date="19910425",
                admission_reason="NA-false-TRN-FALSE",
                release_reason="WT-NA-SC-FALSE",
                facility="CAM",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="NA-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1977-3",
                admission_date="19910425",
                release_date="19910425",
                admission_reason="NA-false-SC-FALSE",
                release_reason="AS-NA-SC-FALSE",
                facility="PHI",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="NA-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1977-4",
                admission_date="19910425",
                release_date="19910430",
                admission_reason="NA-false-SC-FALSE",
                release_reason="WT-NA-SC-FALSE",
                facility="GRA",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="NA-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1977-5",
                admission_date="19910430",
                release_date="19910430",
                admission_reason="NA-false-SC-FALSE",
                release_reason="AS-NA-SC-FALSE",
                facility="PHI",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="NA-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1977-6",
                admission_date="19910430",
                release_date="19910501",
                admission_reason="NA-false-SC-FALSE",
                release_reason="WT-NA-SC-FALSE",
                facility="GRA",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="NA-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1977-7",
                admission_date="19910501",
                release_date="19910501",
                admission_reason="NA-false-SC-FALSE",
                release_reason="DC-NA-SC-FALSE",
                facility="PHI",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="NA-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1977-8",
                admission_date="19910501",
                release_date="19911113",
                admission_reason="NA-false-SC-FALSE",
                release_reason="AS-NA-SC-FALSE",
                facility="CAM",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="NA-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1977-9",
                admission_date="19911113",
                release_date="19911113",
                admission_reason="NA-false-SC-FALSE",
                release_reason="AS-NA-TRN-FALSE",
                facility="CAM",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="NA-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1977-10",
                admission_date="19911113",
                release_date="19940714",
                admission_reason="NA-false-TRN-FALSE",
                release_reason="P-SP-D-FALSE",
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
                admission_reason="PVP-false-APV-FALSE",
                release_reason="AS-PVP-TRN-FALSE",
                facility="GRA",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="PVP-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1989-15",
                admission_date="20050614",
                release_date="20051002",
                admission_reason="PVP-false-TRN-FALSE",
                release_reason="AS-TCV-SC-FALSE",
                facility="SMI",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="PVP-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1989-16",
                admission_date="20051002",
                release_date="20070904",
                admission_reason="TCV-true-SC-FALSE",
                release_reason="P-RP-D-FALSE",
                facility="SMI",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="TCV-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1989-18",
                admission_date="20080331",
                release_date="20080422",
                admission_reason="PVP-false-APV-FALSE",
                release_reason="AS-PVP-TRN-FALSE",
                facility="GRA",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="PVP-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1989-21",
                admission_date="20080422",
                release_date="20080514",
                admission_reason="PVP-false-TRN-FALSE",
                release_reason="AS-TPV-SC-FALSE",
                facility="SMI",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="PVP-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1989-22",
                admission_date="20080514",
                release_date="20080819",
                admission_reason="TPV-true-SC-FALSE",
                release_reason="AS-TPV-TRN-FALSE",
                facility="SMI",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="TPV-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1989-24",
                admission_date="20080819",
                release_date="20090813",
                admission_reason="TPV-false-TRN-FALSE",
                release_reason="P-RP-D-TRUE",
                facility="CHS",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="TPV-S",
                custodial_authority="STATE_PRISON",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="JE1989-26",
                admission_date="20090813",
                admission_reason="NA-false-AOTH-TRUE",
                facility="CHS",
                incarceration_type="SCI",
                specialized_purpose_for_incarceration="NA-S",
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

        self.run_legacy_parse_file_test(expected, "sci_incarceration_period")

    def test_populate_data_dbo_LSIHistory(self) -> None:
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
                            state_assessment_id="789C-0-1-N",
                            assessment_type="LSIR",
                            assessment_class="RISK",
                            assessment_date="2001-01-31",
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
                            state_assessment_id="456B-1-1-N",
                            assessment_type="LSIR",
                            assessment_class="RISK",
                            assessment_date="2005-12-22",
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
                            state_assessment_id="345E-3-1-N",
                            assessment_type="LSIR",
                            assessment_class="RISK",
                            assessment_date="2006-01-19",
                            assessment_score="30",
                            assessment_level=StateAssessmentLevel.HIGH.value,
                        ),
                        StateAssessment(
                            state_assessment_id="345E-3-2-N",
                            assessment_type="LSIR",
                            assessment_class="RISK",
                            assessment_date="2006-08-03",
                            assessment_score=None,
                            assessment_level="UNKNOWN (60-ATTEMPTED_INCOMPLETE)",
                        ),
                        StateAssessment(
                            state_assessment_id="345E-3-3-N",
                            assessment_type="LSIR",
                            assessment_class="RISK",
                            assessment_date="2007-01-15",
                            assessment_score="31",
                            assessment_level=StateAssessmentLevel.HIGH.value,
                        ),
                        StateAssessment(
                            state_assessment_id="345E-3-3-Y",
                            assessment_type="LSIR",
                            assessment_class="RISK",
                            assessment_date="2009-04-29",
                            assessment_score="15",
                            assessment_level=StateAssessmentLevel.LOW.value,
                        ),
                        StateAssessment(
                            state_assessment_id="345E-4-1-Y",
                            assessment_type="LSIR",
                            assessment_class="RISK",
                            assessment_date="2007-07-14",
                            assessment_score="33",
                            assessment_level=StateAssessmentLevel.HIGH.value,
                        ),
                    ],
                ),
            ]
        )

        self.run_legacy_parse_file_test(expected, "dbo_LSIHistory")

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
                                            supervision_type="C2",
                                            admission_reason="C2",
                                            start_date="2012-03-16",
                                            termination_reason="TRANSFER_WITHIN_STATE",
                                            termination_date="2013-04-01",
                                            county_code="ALLEGH",
                                            supervision_site="02|02 - North Shore|7124",
                                            supervision_level="MAX",
                                            custodial_authority="C2",
                                            conditions="MEST, ACT35, GPAR, MVICT, REMC, END",
                                            supervising_officer=StateAgent(
                                                state_agent_id="876555",
                                                given_names="Cosmo",
                                                surname="Kramer",
                                                agent_type="SUPERVISION_OFFICER",
                                            ),
                                        ),
                                        StateSupervisionPeriod(
                                            state_supervision_period_id="456B-2",
                                            supervision_type="04,C2",
                                            admission_reason="TRANSFER_WITHIN_STATE",
                                            start_date="2013-04-01",
                                            termination_reason="TRANSFER_WITHIN_STATE",
                                            termination_date="2014-08-10",
                                            county_code="ALLEGH",
                                            supervision_site="02||",
                                            supervision_level="MAX",
                                            custodial_authority="04",
                                            conditions="MEST, ACT35, GPAR, MVICT, REMC, END",
                                        ),
                                        StateSupervisionPeriod(
                                            state_supervision_period_id="456B-3",
                                            supervision_type="04",
                                            admission_reason="TRANSFER_WITHIN_STATE",
                                            start_date="2014-08-10",
                                            termination_reason="43",
                                            termination_date="2018-01-01",
                                            county_code="ALLEGH",
                                            supervision_site="02|02 - North Shore|7115",
                                            supervision_level="MED",
                                            custodial_authority="04",
                                            conditions="PN, EST, BL, END, SUBD, AANA",
                                            supervising_officer=StateAgent(
                                                state_agent_id="321444",
                                                given_names="Elaine",
                                                surname="Benes",
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
                                            supervision_type="05",
                                            admission_reason="05",
                                            start_date="2003-10-10",
                                            termination_reason="42",
                                            termination_date="2004-08-10",
                                            county_code="MERCER",
                                            supervision_site="08|08 - Mercer DO|7307",
                                            supervision_level="MIN",
                                            custodial_authority="05",
                                            conditions="START, EST, END, AANA, REL, DAM, PARAB, ACT35, BL, MISC, DDU, URI, GPAR, EMP, ALC, PM, PF, PA",
                                            supervising_officer=StateAgent(
                                                state_agent_id="888888",
                                                given_names="K",
                                                surname="Bania",
                                                agent_type="SUPERVISION_OFFICER",
                                            ),
                                        ),
                                        StateSupervisionPeriod(
                                            state_supervision_period_id="789C-2",
                                            supervision_type="05",
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
                                                given_names="Jerry",
                                                surname="Seinfeld",
                                                agent_type="SUPERVISION_OFFICER",
                                            ),
                                        ),
                                        StateSupervisionPeriod(
                                            state_supervision_period_id="789C-3",
                                            supervision_type="04",
                                            admission_reason="04",
                                            start_date="2005-12-31",
                                            termination_reason="43",
                                            termination_date="2006-10-10",
                                            county_code="MERCER",
                                            supervision_level="ADM",
                                            custodial_authority="04",
                                            supervising_officer=StateAgent(
                                                state_agent_id="247",
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
                                            supervision_type="03",
                                            admission_reason="03",
                                            start_date="2016-01-14",
                                            county_code="PHILAD",
                                            supervision_site="01|01 - Northwest|5112",
                                            supervision_level="MED",
                                            custodial_authority="03",
                                            conditions="DOMV, AUTO, DDU, REFR, MNOAM, END, RI, RC, MEMON, MCURF, REFC",
                                            supervising_officer=StateAgent(
                                                state_agent_id="10101",
                                                given_names="Close",
                                                surname="Talker",
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

        self.run_legacy_parse_file_test(expected, "supervision_period_v4")

    def test_populate_data_supervision_contact(self) -> None:
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="456B",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="456B", id_type=US_PA_PBPP
                        ),
                    ],
                    state_supervision_contacts=[
                        StateSupervisionContact(
                            state_supervision_contact_id="456B-2014-09-15-Offender-30",
                            contact_date="2014-09-15",
                            contact_type="Offender",
                            contact_method="Home",
                            location="None-Home",
                            status="Yes",
                            contacted_agent=StateAgent(
                                state_agent_id="321444",
                                agent_type="SUPERVISION_OFFICER",
                                given_names="ELAINE",
                                surname="BENES",
                            ),
                        ),
                        StateSupervisionContact(
                            state_supervision_contact_id="456B-2016-10-02-Both-50",
                            contact_date="2016-10-01",
                            contact_type="Both",
                            contact_method="Email",
                            location="Employer-Email",
                            status="No",
                            contacted_agent=StateAgent(
                                state_agent_id="321444",
                                agent_type="SUPERVISION_OFFICER",
                                given_names="ELAINE",
                                surname="BENES",
                            ),
                        ),
                        StateSupervisionContact(
                            state_supervision_contact_id="456B-2016-10-04-Collateral-50",
                            contact_date="2016-10-03",
                            contact_type="Collateral",
                            contact_method="Home",
                            location="None-Home",
                            status="No",
                            contacted_agent=StateAgent(
                                state_agent_id="321444",
                                agent_type="SUPERVISION_OFFICER",
                                given_names="ELAINE",
                                surname="BENES",
                            ),
                        ),
                        StateSupervisionContact(
                            state_supervision_contact_id="456B-2016-10-05-Collateral-30",
                            contact_date="2016-10-04",
                            contact_type="Collateral",
                            contact_method="Field",
                            location="CourtProbationStaf-Field",
                            status="Yes",
                            contacted_agent=StateAgent(
                                state_agent_id="321444",
                                agent_type="SUPERVISION_OFFICER",
                                given_names="ELAINE",
                                surname="BENES",
                            ),
                        ),
                    ],
                ),
            ]
        )
        self.run_legacy_parse_file_test(expected, "supervision_contacts")

    def test_run_full_ingest_all_files_specific_order(self) -> None:
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
                    external_id="AB7413",
                    id_type=US_PA_INMATE,
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
                    external_id="GF3374",
                    id_type=US_PA_INMATE,
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
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id="CJ1991",
                    id_type=US_PA_INMATE,
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
                    external_id="JE1989",
                    id_type=US_PA_INMATE,
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
                    id_type=US_PA_INMATE,
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
                    id_type=US_PA_INMATE,
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
                    id_type=US_PA_INMATE,
                ),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id="66666667",
                    id_type=US_PA_INMATE,
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
        person_1.full_name = '{"given_names": "BERTRAND", "middle_names": "", "name_suffix": "", "surname": "RUSSELL"}'
        person_1.gender = Gender.MALE
        person_1.gender_raw_text = "MALE"
        person_1.birthdate = datetime.date(year=1976, month=3, day=18)
        person_1.current_address = "123 EASY STREET, PITTSBURGH, PA 16161"
        person_1.residency_status = ResidencyStatus.PERMANENT
        person_1.residency_status_raw_text = "123 EASY STREET"
        person_1.state_code = _STATE_CODE_UPPER
        person_1.aliases = [
            entities.StatePersonAlias.new_with_defaults(
                full_name='{"given_names": "BERTRAND", "middle_names": "", "name_suffix": "", "surname": "RUSSELL"}',
                state_code=_STATE_CODE_UPPER,
                alias_type=StatePersonAliasType.GIVEN_NAME,
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

        person_2.full_name = '{"given_names": "JEAN-PAUL", "middle_names": "", "name_suffix": "", "surname": "SARTRE"}'
        person_2.gender = Gender.MALE
        person_2.gender_raw_text = "MALE"
        person_2.birthdate = datetime.date(year=1982, month=10, day=2)
        person_2.current_address = "555 FLATBUSH DR, NEW YORK, NY 10031"
        person_2.residency_status = ResidencyStatus.PERMANENT
        person_2.residency_status_raw_text = "555 FLATBUSH DR"
        person_2.state_code = _STATE_CODE_UPPER
        person_2.aliases = [
            entities.StatePersonAlias.new_with_defaults(
                full_name='{"given_names": "JEAN-PAUL", "middle_names": "", "name_suffix": "", "surname": "SARTRE"}',
                state_code=_STATE_CODE_UPPER,
                alias_type=StatePersonAliasType.GIVEN_NAME,
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

        person_3.full_name = '{"given_names": "SOREN", "middle_names": "", "name_suffix": "JR", "surname": "KIERKEGAARD"}'
        person_3.gender = Gender.FEMALE
        person_3.gender_raw_text = "FEMALE"
        person_3.birthdate = datetime.date(year=1991, month=11, day=20)
        person_3.current_address = "5000 SUNNY LANE, APT. 55D, PHILADELPHIA, PA 19129"
        person_3.residency_status = ResidencyStatus.PERMANENT
        person_3.residency_status_raw_text = "5000 SUNNY LANE-APT. 55D"
        person_3.state_code = _STATE_CODE_UPPER
        person_3.aliases = [
            entities.StatePersonAlias.new_with_defaults(
                full_name='{"given_names": "SOREN", "middle_names": "", "name_suffix": "JR", "surname": "KIERKEGAARD"}',
                state_code=_STATE_CODE_UPPER,
                alias_type=StatePersonAliasType.GIVEN_NAME,
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

        person_4.full_name = '{"given_names": "JOHN", "middle_names": "", "name_suffix": "", "surname": "RAWLS"}'
        person_4.gender = Gender.MALE
        person_4.gender_raw_text = "MALE"
        person_4.birthdate = datetime.date(year=1989, month=6, day=17)
        person_4.current_address = "214 HAPPY PLACE, PHILADELPHIA, PA 19129"
        person_4.residency_status = ResidencyStatus.PERMANENT
        person_4.residency_status_raw_text = "214 HAPPY PLACE"
        person_4.state_code = _STATE_CODE_UPPER
        person_4.aliases = [
            entities.StatePersonAlias.new_with_defaults(
                full_name='{"given_names": "JOHN", "middle_names": "", "name_suffix": "", "surname": "RAWLS"}',
                state_code=_STATE_CODE_UPPER,
                alias_type=StatePersonAliasType.GIVEN_NAME,
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
        self._run_ingest_job_for_filename("doc_person_info.csv")

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
            projected_min_release_date=datetime.date(2007, 7, 4),
            projected_max_release_date=datetime.date(2009, 1, 4),
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
            statute="CC3502",
            description="BURGLARY (GENERAL)",
            offense_type="BURGLARY-VIOLENT-PROPERTY",
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="FELONY",
            classification_subtype="F1",
            is_violent=True,
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
            projected_min_release_date=datetime.date(2020, 1, 14),
            projected_max_release_date=datetime.date(2022, 4, 14),
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
            statute="CC4101",
            description="FORGERY",
            offense_type="FORGERY",
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="FELONY",
            classification_subtype="F2",
            is_violent=False,
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
            projected_min_release_date=datetime.date(2017, 7, 5),
            projected_max_release_date=datetime.date(2018, 10, 5),
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
            statute="CC2502B",
            description="MURDER (2ND DEGREE)",
            offense_type="MURDER 2-HOMICIDE",
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="FELONY",
            classification_subtype="F",
            is_violent=True,
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
            projected_min_release_date=datetime.date(2017, 10, 22),
            projected_max_release_date=datetime.date(2019, 4, 22),
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
            projected_min_release_date=datetime.date(2017, 10, 22),
            projected_max_release_date=datetime.date(2019, 4, 22),
            min_length_days=549,
            is_life=False,
            is_capital_punishment=False,
            person=person_4,
            sentence_group=p4_sg,
        )
        p4_sg.incarceration_sentences.extend([p4_is_1, p4_is_2])

        p4_is_1_charge = entities.StateCharge.new_with_defaults(
            external_id="L7858888",
            statute="XX0500",
            description="THEFT",
            offense_type="THEFT",
            classification_type=StateChargeClassificationType.MISDEMEANOR,
            classification_type_raw_text="MISDEMEANOR",
            classification_subtype="M1",
            is_violent=False,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_sentences=[p4_is_1],
        )
        p4_is_1.charges.append(p4_is_1_charge)

        p4_is_2_charge = entities.StateCharge.new_with_defaults(
            external_id="L7858890",
            statute="CC2702A1",
            description="AGGRAVATED ASSAULT W/SERIOUS BODILY INJURY AGAINST ELDERLY/YOUNG PERSON",
            offense_type="AGGRAVATED ASSAULT-ASSAULT",
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="FELONY",
            classification_subtype="F1",
            is_violent=True,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_sentences=[p4_is_2],
        )
        p4_is_2.charges.append(p4_is_2_charge)

        populate_person_backedges(expected_people)

        # Act
        self._run_ingest_job_for_filename("dbo_Senrec_v2.csv")

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
            admission_reason_raw_text="NA-FALSE-AC-FALSE",
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
            admission_reason_raw_text="NA-FALSE-AC-FALSE",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="DC-NA-TRN-FALSE",
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
            admission_reason_raw_text="NA-FALSE-TRN-FALSE",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="AS-NA-TRN-FALSE",
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
            admission_reason_raw_text="NA-FALSE-TRN-FALSE",
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="P-SP-D-FALSE",
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
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text="PVP-FALSE-APV-FALSE",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="AS-PVP-TRN-FALSE",
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
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text="PVP-FALSE-TRN-FALSE",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="AS-TPV-SC-FALSE",
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
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="TPV-TRUE-SC-FALSE",
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="P-RP-D-FALSE",
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
            admission_reason_raw_text="NA-FALSE-AC-FALSE",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="DC-NA-TRN-FALSE",
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
            admission_reason_raw_text="NA-FALSE-TRN-FALSE",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="WT-NA-SC-FALSE",
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
            admission_reason_raw_text="NA-FALSE-SC-FALSE",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="AS-NA-SC-FALSE",
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
            admission_reason_raw_text="NA-FALSE-SC-FALSE",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="WT-NA-SC-FALSE",
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
            admission_reason_raw_text="NA-FALSE-SC-FALSE",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="AS-NA-SC-FALSE",
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
            admission_reason_raw_text="NA-FALSE-SC-FALSE",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="WT-NA-SC-FALSE",
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
            admission_reason_raw_text="NA-FALSE-SC-FALSE",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="DC-NA-SC-FALSE",
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
            admission_reason_raw_text="NA-FALSE-SC-FALSE",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="AS-NA-SC-FALSE",
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
            admission_reason_raw_text="NA-FALSE-SC-FALSE",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="AS-NA-TRN-FALSE",
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
            admission_reason_raw_text="NA-FALSE-TRN-FALSE",
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="P-SP-D-FALSE",
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
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text="PVP-FALSE-APV-FALSE",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="AS-PVP-TRN-FALSE",
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
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text="PVP-FALSE-TRN-FALSE",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="AS-TCV-SC-FALSE",
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
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="TCV-TRUE-SC-FALSE",
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="P-RP-D-FALSE",
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
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text="PVP-FALSE-APV-FALSE",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="AS-PVP-TRN-FALSE",
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
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text="PVP-FALSE-TRN-FALSE",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="AS-TPV-SC-FALSE",
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
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="TPV-TRUE-SC-FALSE",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="AS-TPV-TRN-FALSE",
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
            admission_reason_raw_text="TPV-FALSE-TRN-FALSE",
            release_reason=StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
            release_reason_raw_text="P-RP-D-TRUE",
            facility="CHS",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="SCI",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="TPV-S",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text=StateCustodialAuthority.STATE_PRISON.value,
        )

        p4_is_1_ip_26 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="JE1989-26",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_sentences=[p4_is_1],
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=datetime.date(year=2009, month=8, day=13),
            admission_reason=StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
            admission_reason_raw_text="NA-FALSE-AOTH-TRUE",
            facility="CHS",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="SCI",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="NA-S",
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
            p4_is_1_ip_26,
        ]

        populate_person_backedges(expected_people)

        # Act
        self._run_ingest_job_for_filename("sci_incarceration_period.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # dbo_Miscon
        ######################################

        # Arrange
        p3_ii = entities.StateIncarcerationIncident.new_with_defaults(
            external_id="A123456",
            state_code=_STATE_CODE_UPPER,
            person=person_3,
            incident_type=StateIncarcerationIncidentType.REPORT,
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
        person_3.incarceration_incidents.append(p3_ii)

        p4_ii_1 = entities.StateIncarcerationIncident.new_with_defaults(
            external_id="A234567",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incident_type=StateIncarcerationIncidentType.REPORT,
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
        person_4.incarceration_incidents.append(p4_ii_1)

        p4_ii_2 = entities.StateIncarcerationIncident.new_with_defaults(
            external_id="B222333",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incident_type=StateIncarcerationIncidentType.REPORT,
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
        person_4.incarceration_incidents.append(p4_ii_2)

        p4_ii_3 = entities.StateIncarcerationIncident.new_with_defaults(
            external_id="B444555",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incident_type=StateIncarcerationIncidentType.CONTRABAND,
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
            outcome_type_raw_text="N",
        )
        p4_ii_3.incarceration_incident_outcomes.append(p4_ii_3_outcome)
        person_4.incarceration_incidents.append(p4_ii_3)

        p4_ii_4 = entities.StateIncarcerationIncident.new_with_defaults(
            external_id="B444556",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incident_type=StateIncarcerationIncidentType.CONTRABAND,
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
            outcome_type_raw_text="N",
        )
        p4_ii_4.incarceration_incident_outcomes.append(p4_ii_4_outcome)
        person_4.incarceration_incidents.append(p4_ii_4)

        populate_person_backedges(expected_people)

        # Act
        self._run_ingest_job_for_filename("dbo_Miscon.csv")

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
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
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
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
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
        p3_is_ip_11 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="56789",
            state_code=_STATE_CODE_UPPER,
            person=person_3,
            incarceration_sentences=[p3_is],
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=2016, month=11, day=23),
            release_date=datetime.date(year=2016, month=11, day=24),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="CCIS-FALSE-PRCH",
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="CCIS-PTST",
            facility="195: LACKAWANNA COUNTY",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            incarceration_type_raw_text="CCIS",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.INTERNAL_UNKNOWN,
            specialized_purpose_for_incarceration_raw_text="CCIS-60",
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            custodial_authority_raw_text="60",
        )

        p3_is.incarceration_periods.extend([p3_is_ip_9, p3_is_ip_10, p3_is_ip_11])

        p4_is_1_ip_25 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="12345",
            state_code=_STATE_CODE_UPPER,
            person=person_4,
            incarceration_sentences=[p4_is_1],
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=2016, month=12, day=21),
            release_date=datetime.date(year=2017, month=4, day=26),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
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

        p4_is_1_ip_26_ccis = entities.StateIncarcerationPeriod.new_with_defaults(
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

        p4_is_1.incarceration_periods.extend([p4_is_1_ip_25, p4_is_1_ip_26_ccis])

        # Act
        self._run_ingest_job_for_filename("ccis_incarceration_period.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # dbo_Offender
        ######################################
        # Arrange
        person_1.gender_raw_text = "M"
        person_1.races.append(
            entities.StatePersonRace.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                race=Race.ASIAN,
                race_raw_text="A",
            )
        )

        person_2.gender_raw_text = "M"
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
                state_code=_STATE_CODE_UPPER, race=Race.ASIAN, race_raw_text="A"
            ),
            entities.StatePersonRace.new_with_defaults(
                state_code=_STATE_CODE_UPPER, race=Race.WHITE, race_raw_text="W"
            ),
        ]

        person_5.full_name = '{"given_names": "KEN", "middle_names": "", "name_suffix": "JR", "surname": "GRIFFEY"}'
        person_5.gender = Gender.FEMALE
        person_5.gender_raw_text = "F"
        person_5.aliases = [
            entities.StatePersonAlias.new_with_defaults(
                full_name='{"given_names": "KEN", "middle_names": "", "name_suffix": "JR", "surname": "GRIFFEY"}',
                state_code=_STATE_CODE_UPPER,
                alias_type=StatePersonAliasType.GIVEN_NAME,
            )
        ]
        person_5.races = [
            entities.StatePersonRace.new_with_defaults(
                state_code=_STATE_CODE_UPPER, race=Race.OTHER, race_raw_text="N"
            ),
        ]

        person_6 = entities.StatePerson.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
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
        self._run_ingest_job_for_filename("dbo_Offender_v2.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # dbo_LSIHistory
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
                external_id="456B-1-1-N",
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
                external_id="345E-3-1-N",
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
                external_id="345E-3-2-N",
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
                external_id="345E-3-3-N",
                state_code=_STATE_CODE_UPPER,
                person=person_4,
            ),
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.RISK,
                assessment_class_raw_text="RISK",
                assessment_type=StateAssessmentType.LSIR,
                assessment_type_raw_text="LSIR",
                assessment_level=StateAssessmentLevel.LOW,
                assessment_level_raw_text="LOW",
                assessment_score=15,
                assessment_date=datetime.date(year=2009, month=4, day=29),
                external_id="345E-3-3-Y",
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
                external_id="345E-4-1-Y",
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
                external_id="789C-0-1-N",
                state_code=_STATE_CODE_UPPER,
                person=person_5,
            ),
        ]
        person_5.assessments.extend(person_5_pbpp_assessments)

        populate_person_backedges(expected_people)

        # Act
        self._run_ingest_job_for_filename("dbo_LSIHistory.csv")

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

        person_2.supervision_violations.append(p2_sv_1)
        person_2.supervision_violations.append(p2_sv_2)

        p5_sv_3 = entities.StateSupervisionViolation.new_with_defaults(
            external_id="789C-3-1",
            state_code=_STATE_CODE_UPPER,
            violation_date=datetime.date(year=2006, month=8, day=11),
            person=person_5,
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

        person_5.supervision_violations.append(p5_sv_3)

        p4_sv_1 = entities.StateSupervisionViolation.new_with_defaults(
            external_id="345E-1-1",
            state_code=_STATE_CODE_UPPER,
            violation_date=datetime.date(year=2018, month=3, day=17),
            person=person_4,
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

        person_4.supervision_violations.extend([p4_sv_1, p4_sv_2])

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
            person=person_2,
            supervision_violation=p2_sv_1,
        )
        p2_svr_1_d_2 = (
            entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                person=person_2,
                supervision_violation_response=p2_svr_1,
                decision=StateSupervisionViolationResponseDecision.WARNING,
                decision_raw_text="WTWR",
            )
        )
        p2_svr_1_d_3 = (
            entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                person=person_2,
                supervision_violation_response=p2_svr_1,
                decision=StateSupervisionViolationResponseDecision.NEW_CONDITIONS,
                decision_raw_text="DJBS",
            )
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
            person=person_2,
            supervision_violation=p2_sv_2,
        )
        p2_svr_2_d_12 = (
            entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                person=person_2,
                supervision_violation_response=p2_svr_2,
                decision=StateSupervisionViolationResponseDecision.REVOCATION,
                decision_raw_text="VCCF",
            )
        )
        p2_svr_2_d_13 = (
            entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                person=person_2,
                supervision_violation_response=p2_svr_2,
                decision=StateSupervisionViolationResponseDecision.REVOCATION,
                decision_raw_text="ARR2",
            )
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
            person=person_5,
            supervision_violation=p5_sv_3,
        )
        p5_svr_3_d_4 = (
            entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                person=person_5,
                supervision_violation_response=p5_svr_3,
                decision=StateSupervisionViolationResponseDecision.REVOCATION,
                decision_raw_text="VCCF",
            )
        )
        p5_svr_3_d_5 = (
            entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                person=person_5,
                supervision_violation_response=p5_svr_3,
                decision=StateSupervisionViolationResponseDecision.REVOCATION,
                decision_raw_text="ARR2",
            )
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
            person=person_4,
            supervision_violation=p4_sv_1,
        )
        p4_svr_1_d_2 = (
            entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                person=person_4,
                supervision_violation_response=p4_svr_1,
                decision=StateSupervisionViolationResponseDecision.WARNING,
                decision_raw_text="WTWR",
            )
        )
        p4_svr_1.supervision_violation_response_decisions.append(p4_svr_1_d_2)
        p4_sv_1.supervision_violation_responses.append(p4_svr_1)

        p4_svr_2 = entities.StateSupervisionViolationResponse.new_with_defaults(
            external_id="345E-1-2",
            state_code=_STATE_CODE_UPPER,
            response_date=datetime.date(year=2018, month=5, day=13),
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            person=person_4,
            supervision_violation=p4_sv_2,
        )
        p4_svr_1_d_5 = (
            entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                person=person_4,
                supervision_violation_response=p4_svr_2,
                decision=StateSupervisionViolationResponseDecision.REVOCATION,
                decision_raw_text="ARR2",
            )
        )
        p4_svr_1_d_6 = (
            entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                person=person_4,
                supervision_violation_response=p4_svr_2,
                decision=StateSupervisionViolationResponseDecision.INTERNAL_UNKNOWN,
                decision_raw_text="PV01",
            )
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
        p1_placeholder_sv = entities.StateSupervisionViolation.new_with_defaults(
            person=person_1,
            state_code=_STATE_CODE_UPPER,
        )
        p1_vr = entities.StateSupervisionViolationResponse.new_with_defaults(
            person=person_1,
            supervision_violation=p1_placeholder_sv,
            external_id="BOARD-123A-1-09",
            state_code=_STATE_CODE_UPPER,
            response_date=datetime.date(year=2004, month=6, day=16),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
        )
        p1_de = (
            entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                person=person_1,
                supervision_violation_response=p1_vr,
                state_code=_STATE_CODE_UPPER,
                decision_raw_text="RESCR",
                decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            )
        )

        person_1.supervision_violations.append(p1_placeholder_sv)
        p1_placeholder_sv.supervision_violation_responses.append(p1_vr)
        p1_vr.supervision_violation_response_decisions.append(p1_de)

        p2_placeholder_sv = entities.StateSupervisionViolation.new_with_defaults(
            person=person_2,
            state_code=_STATE_CODE_UPPER,
        )
        p2_vr = entities.StateSupervisionViolationResponse.new_with_defaults(
            person=person_2,
            supervision_violation=p2_placeholder_sv,
            external_id="BOARD-456B-0-04",
            state_code=_STATE_CODE_UPPER,
            response_date=datetime.date(year=2014, month=2, day=24),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
        )
        p2_de = (
            entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                person=person_2,
                supervision_violation_response=p2_vr,
                state_code=_STATE_CODE_UPPER,
                decision_raw_text="RESCR9",
                decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            )
        )

        person_2.supervision_violations.append(p2_placeholder_sv)
        p2_placeholder_sv.supervision_violation_responses.append(p2_vr)
        p2_vr.supervision_violation_response_decisions.append(p2_de)

        p5_placeholder_sv = entities.StateSupervisionViolation.new_with_defaults(
            person=person_5,
            state_code=_STATE_CODE_UPPER,
        )
        p5_vr = entities.StateSupervisionViolationResponse.new_with_defaults(
            person=person_5,
            supervision_violation=p5_placeholder_sv,
            external_id="BOARD-789C-0-02",
            state_code=_STATE_CODE_UPPER,
            response_date=datetime.date(year=2014, month=7, day=9),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
        )
        p5_de = (
            entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                person=person_5,
                supervision_violation_response=p5_vr,
                state_code=_STATE_CODE_UPPER,
                decision_raw_text="RESCR9",
                decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            )
        )

        person_5.supervision_violations.append(p5_placeholder_sv)
        p5_placeholder_sv.supervision_violation_responses.append(p5_vr)
        p5_vr.supervision_violation_response_decisions.append(p5_de)

        p4_placeholder_sv = entities.StateSupervisionViolation.new_with_defaults(
            person=person_4,
            state_code=_STATE_CODE_UPPER,
        )
        p4_vr = entities.StateSupervisionViolationResponse.new_with_defaults(
            person=person_4,
            supervision_violation=p4_placeholder_sv,
            external_id="BOARD-345E-3-11",
            state_code=_STATE_CODE_UPPER,
            response_date=datetime.date(year=2006, month=2, day=21),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
        )
        p4_de = (
            entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                person=person_4,
                supervision_violation_response=p4_vr,
                state_code=_STATE_CODE_UPPER,
                decision_raw_text="RESCR",
                decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            )
        )

        person_4.supervision_violations.append(p4_placeholder_sv)
        p4_placeholder_sv.supervision_violation_responses.append(p4_vr)
        p4_vr.supervision_violation_response_decisions.append(p4_de)

        # Act
        self._run_ingest_job_for_filename("board_action.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # supervision_contacts
        ######################################

        supervising_officer_1 = entities.StateAgent.new_with_defaults(
            external_id="321444",
            state_code=_STATE_CODE_UPPER,
            full_name='{"given_names": "ELAINE", "surname": "BENES"}',
            agent_type=StateAgentType.SUPERVISION_OFFICER,
            agent_type_raw_text="SUPERVISION_OFFICER",
        )

        p2_sc_2_1 = entities.StateSupervisionContact.new_with_defaults(
            external_id="456B-2014-09-15-OFFENDER-30",
            person=person_2,
            state_code=_STATE_CODE_UPPER,
            contact_date=datetime.date(year=2014, month=9, day=15),
            contact_type=StateSupervisionContactType.DIRECT,
            contact_type_raw_text="OFFENDER",
            contact_method=StateSupervisionContactMethod.IN_PERSON,
            contact_method_raw_text="HOME",
            location=StateSupervisionContactLocation.RESIDENCE,
            location_raw_text="NONE-HOME",
            status=StateSupervisionContactStatus.ATTEMPTED,
            status_raw_text="YES",
            contacted_agent=supervising_officer_1,
        )

        p2_sc_2_2 = entities.StateSupervisionContact.new_with_defaults(
            external_id="456B-2016-10-02-BOTH-50",
            person=person_2,
            state_code=_STATE_CODE_UPPER,
            contact_date=datetime.date(year=2016, month=10, day=1),
            contact_type=StateSupervisionContactType.BOTH_COLLATERAL_AND_DIRECT,
            contact_type_raw_text="BOTH",
            contact_method=StateSupervisionContactMethod.WRITTEN_MESSAGE,
            contact_method_raw_text="EMAIL",
            location=StateSupervisionContactLocation.PLACE_OF_EMPLOYMENT,
            location_raw_text="EMPLOYER-EMAIL",
            status=StateSupervisionContactStatus.COMPLETED,
            status_raw_text="NO",
            contacted_agent=supervising_officer_1,
        )

        p2_sc_2_3 = entities.StateSupervisionContact.new_with_defaults(
            external_id="456B-2016-10-04-COLLATERAL-50",
            person=person_2,
            state_code=_STATE_CODE_UPPER,
            contact_date=datetime.date(year=2016, month=10, day=3),
            contact_type=StateSupervisionContactType.COLLATERAL,
            contact_type_raw_text="COLLATERAL",
            contact_method=StateSupervisionContactMethod.IN_PERSON,
            contact_method_raw_text="HOME",
            location=StateSupervisionContactLocation.RESIDENCE,
            location_raw_text="NONE-HOME",
            status=StateSupervisionContactStatus.COMPLETED,
            status_raw_text="NO",
            contacted_agent=supervising_officer_1,
        )

        p2_sc_2_4 = entities.StateSupervisionContact.new_with_defaults(
            external_id="456B-2016-10-05-COLLATERAL-30",
            person=person_2,
            state_code=_STATE_CODE_UPPER,
            contact_date=datetime.date(year=2016, month=10, day=4),
            contact_type=StateSupervisionContactType.COLLATERAL,
            contact_type_raw_text="COLLATERAL",
            contact_method=StateSupervisionContactMethod.IN_PERSON,
            contact_method_raw_text="FIELD",
            location=StateSupervisionContactLocation.COURT,
            location_raw_text="COURTPROBATIONSTAF-FIELD",
            status=StateSupervisionContactStatus.ATTEMPTED,
            status_raw_text="YES",
            contacted_agent=supervising_officer_1,
        )
        person_2.supervision_contacts.extend(
            [p2_sc_2_4, p2_sc_2_3, p2_sc_2_2, p2_sc_2_1]
        )

        populate_person_backedges(expected_people)

        # Act
        self._run_ingest_job_for_filename("supervision_contacts.csv")

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
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_type_raw_text="C2",
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
                external_id="876555",
                state_code=_STATE_CODE_UPPER,
                full_name='{"given_names": "COSMO", "surname": "KRAMER"}',
                agent_type=StateAgentType.SUPERVISION_OFFICER,
                agent_type_raw_text="SUPERVISION_OFFICER",
            ),
            supervision_sentences=[p2_ss_ph],
        )

        p2_sp_1_2 = entities.StateSupervisionPeriod.new_with_defaults(
            external_id="456B-2",
            state_code=_STATE_CODE_UPPER,
            supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
            supervision_type_raw_text="04,C2",
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            admission_reason_raw_text="TRANSFER_WITHIN_STATE",
            start_date=datetime.date(year=2013, month=4, day=1),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            termination_reason_raw_text="TRANSFER_WITHIN_STATE",
            termination_date=datetime.date(year=2014, month=8, day=10),
            county_code="ALLEGH",
            supervision_site="02||",
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
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_type_raw_text="04",
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
            supervising_officer=supervising_officer_1,
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
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_type_raw_text="05",
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
                full_name='{"given_names": "K", "surname": "BANIA"}',
                agent_type=StateAgentType.SUPERVISION_OFFICER,
                agent_type_raw_text="SUPERVISION_OFFICER",
            ),
            supervision_sentences=[p5_ss_ph],
        )
        p5_sp_2_1 = entities.StateSupervisionPeriod.new_with_defaults(
            external_id="789C-2",
            state_code=_STATE_CODE_UPPER,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_type_raw_text="05",
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
                full_name='{"given_names": "JERRY", "surname": "SEINFELD"}',
                agent_type=StateAgentType.SUPERVISION_OFFICER,
                agent_type_raw_text="SUPERVISION_OFFICER",
            ),
            supervision_sentences=[p5_ss_ph],
        )
        p5_sp_3_1 = entities.StateSupervisionPeriod.new_with_defaults(
            external_id="789C-3",
            state_code=_STATE_CODE_UPPER,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_type_raw_text="04",
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
                external_id="247",
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
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_type_raw_text="03",
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
                external_id="10101",
                state_code=_STATE_CODE_UPPER,
                full_name='{"given_names": "CLOSE", "surname": "TALKER"}',
                agent_type=StateAgentType.SUPERVISION_OFFICER,
                agent_type_raw_text="SUPERVISION_OFFICER",
            ),
            supervision_sentences=[p4_ss_ph],
        )

        p4_ss_ph.supervision_periods.append(p4_sp_1_1)

        populate_person_backedges(expected_people)

        # Act
        self._run_ingest_job_for_filename("supervision_period_v4.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # FULL RERUN FOR IDEMPOTENCE
        ######################################

        self._do_ingest_job_rerun_for_tags(self.controller.get_file_tag_rank_list())

        self.assert_expected_db_people(
            expected_people, ignore_dangling_placeholders=True
        )
