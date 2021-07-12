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
"""Tests for the UsMoController."""

import datetime
from typing import List, Type, cast

from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.person_characteristics import Ethnicity, Gender, Race
from recidiviz.common.constants.state.external_id_types import US_MO_DOC
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_charge import StateChargeClassificationType
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateIncarcerationPeriodStatus,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_person_alias import StatePersonAliasType
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodStatus,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecidingBodyType,
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseRevocationType,
    StateSupervisionViolationResponseType,
)
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.regions.us_mo.us_mo_controller import UsMoController
from recidiviz.ingest.models.ingest_info import (
    IngestInfo,
    StateAgent,
    StateAlias,
    StateAssessment,
    StateCharge,
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StatePerson,
    StatePersonEthnicity,
    StatePersonExternalId,
    StatePersonRace,
    StateSentenceGroup,
    StateSupervisionCaseTypeEntry,
    StateSupervisionPeriod,
    StateSupervisionSentence,
    StateSupervisionViolatedConditionEntry,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
    StateSupervisionViolationResponseDecisionEntry,
    StateSupervisionViolationTypeEntry,
)
from recidiviz.persistence.database.schema.state import dao
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.entity.state import entities
from recidiviz.tests.ingest.direct.regions.base_direct_ingest_controller_tests import (
    BaseDirectIngestControllerTests,
)
from recidiviz.tests.ingest.direct.regions.utils import populate_person_backedges

_STATE_CODE_UPPER = "US_MO"


class TestUsMoController(BaseDirectIngestControllerTests):
    """Tests for the UsMoController."""

    @classmethod
    def region_code(cls) -> str:
        return _STATE_CODE_UPPER.lower()

    @classmethod
    def controller_cls(cls) -> Type[BaseDirectIngestController]:
        return UsMoController

    @classmethod
    def schema_type(cls) -> SchemaType:
        return SchemaType.STATE

    def test_parse_mo_julian_date(self) -> None:
        self.assertEqual(UsMoController.mo_julian_date_to_iso(""), None)
        self.assertEqual(UsMoController.mo_julian_date_to_iso("0"), None)
        self.assertEqual(UsMoController.mo_julian_date_to_iso("99001"), "1999-01-01")
        self.assertEqual(UsMoController.mo_julian_date_to_iso("99365"), "1999-12-31")
        self.assertEqual(UsMoController.mo_julian_date_to_iso("100001"), "2000-01-01")
        self.assertEqual(UsMoController.mo_julian_date_to_iso("115104"), "2015-04-14")
        self.assertEqual(UsMoController.mo_julian_date_to_iso("118365"), "2018-12-31")

    def test_populate_data_tak001_offender_identification(self) -> None:
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="110035",
                    surname="ABAGNALE",
                    given_names="FRANK",
                    name_suffix="JR",
                    gender="M",
                    birthdate="19711120",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="110035", id_type=US_MO_DOC
                        )
                    ],
                    state_person_races=[StatePersonRace(race="I")],
                    state_person_ethnicities=[StatePersonEthnicity(ethnicity="H")],
                    state_aliases=[
                        StateAlias(
                            surname="ABAGNALE",
                            given_names="FRANK",
                            name_suffix="JR",
                            alias_type="GIVEN_NAME",
                        )
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(state_sentence_group_id="110035-19890901")
                    ],
                ),
                StatePerson(
                    state_person_id="310261",
                    surname="STEWART",
                    given_names="MARTHA",
                    middle_names="HELEN",
                    gender="F",
                    birthdate="19690617",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="310261", id_type=US_MO_DOC
                        )
                    ],
                    state_person_races=[StatePersonRace(race="W")],
                    state_person_ethnicities=[StatePersonEthnicity(ethnicity="U")],
                    state_aliases=[
                        StateAlias(
                            surname="STEWART",
                            given_names="MARTHA",
                            middle_names="HELEN",
                            alias_type="GIVEN_NAME",
                        )
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(state_sentence_group_id="310261-19890821")
                    ],
                ),
                StatePerson(
                    state_person_id="710448",
                    surname="WINNIFIELD",
                    given_names="JULES",
                    birthdate="19640831",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="710448", id_type=US_MO_DOC
                        )
                    ],
                    state_person_races=[StatePersonRace(race="B")],
                    state_aliases=[
                        StateAlias(
                            surname="WINNIFIELD",
                            given_names="JULES",
                            alias_type="GIVEN_NAME",
                        )
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(state_sentence_group_id="710448-19890901")
                    ],
                ),
                StatePerson(
                    state_person_id="910324",
                    given_names="KAONASHI",
                    gender="U",
                    birthdate="19580213",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="910324", id_type=US_MO_DOC
                        )
                    ],
                    state_person_races=[StatePersonRace(race="A")],
                    state_person_ethnicities=[StatePersonEthnicity(ethnicity="N")],
                    state_aliases=[
                        StateAlias(given_names="KAONASHI", alias_type="GIVEN_NAME")
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(state_sentence_group_id="910324-19890825")
                    ],
                ),
            ]
        )

        self.run_parse_file_test(expected, "tak001_offender_identification")

    def test_populate_data_oras_assessments_weekly(self) -> None:
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="110035",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="110035", id_type=US_MO_DOC
                        ),
                    ],
                    state_assessments=[
                        StateAssessment(
                            assessment_type="Prison Intake Tool",
                            assessment_score="10",
                            assessment_level="Moderate",
                            assessment_date="2010-01-01",
                            state_assessment_id="110035-1",
                        )
                    ],
                ),
                StatePerson(
                    state_person_id="310261",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="310261", id_type=US_MO_DOC
                        ),
                    ],
                    state_assessments=[
                        StateAssessment(
                            assessment_type="Community Supervision Tool",
                            assessment_score="5",
                            assessment_level="Low",
                            assessment_date="2000-01-01",
                            state_assessment_id="310261-2",
                        ),
                        StateAssessment(
                            assessment_type="Reentry Tool",
                            assessment_score="7",
                            assessment_level="Low/Moderate",
                            assessment_date="1900-01-01",
                            state_assessment_id="310261-1",
                        ),
                    ],
                ),
            ]
        )

        self.run_parse_file_test(expected, "oras_assessments_weekly")

    def test_populate_data_tak040_offender_identification(self) -> None:
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="110035",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="110035", id_type=US_MO_DOC
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(state_sentence_group_id="110035-19890901"),
                        StateSentenceGroup(state_sentence_group_id="110035-20010414"),
                    ],
                ),
                StatePerson(
                    state_person_id="310261",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="310261", id_type=US_MO_DOC
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(state_sentence_group_id="310261-19890821")
                    ],
                ),
                StatePerson(
                    state_person_id="710448",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="710448", id_type=US_MO_DOC
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(state_sentence_group_id="710448-19890901"),
                        StateSentenceGroup(state_sentence_group_id="710448-20010414"),
                    ],
                ),
                StatePerson(
                    state_person_id="910324",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="910324", id_type=US_MO_DOC
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(state_sentence_group_id="910324-19890825")
                    ],
                ),
            ]
        )

        self.run_parse_file_test(expected, "tak040_offender_cycles")

    def test_populate_data_tak022_tak023_offender_sentence_institutional(self) -> None:
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="110035",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="110035", id_type=US_MO_DOC
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="110035-19890901",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="110035-19890901-1",
                                    status="COMPLETED",
                                    date_imposed="19560316",
                                    start_date="19560425",
                                    projected_min_release_date=None,
                                    projected_max_release_date=None,
                                    completion_date="19800926",
                                    parole_eligibility_date="1956-04-25",
                                    county_code="US_MO_ST_LOUIS_CITY",
                                    max_length="3655253",
                                    is_life="True",
                                    is_capital_punishment="Y",
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id="110035-19890901-1",
                                            offense_date=None,
                                            county_code="US_MO_JACKSON",
                                            ncic_code="0904",
                                            statute="10021040",
                                            description="TC: MURDER 1ST - FIST",
                                            is_violent="True",
                                            classification_type="F",
                                            classification_subtype="O",
                                        )
                                    ],
                                )
                            ],
                        ),
                        StateSentenceGroup(
                            state_sentence_group_id="110035-20010414",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="110035-20010414-1",
                                    status="COMPLETED",
                                    date_imposed="20030110",
                                    start_date="20030103",
                                    projected_min_release_date="20070102",
                                    projected_max_release_date="20070102",
                                    completion_date="20070102",
                                    parole_eligibility_date="2003-01-03",
                                    county_code="US_MO_ST_LOUIS_CITY",
                                    max_length="4Y 0M 0D",
                                    is_life="False",
                                    is_capital_punishment="N",
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id="110035-20010414-1",
                                            offense_date="20000604",
                                            county_code="US_MO_ST_LOUIS_CITY",
                                            ncic_code="5299",
                                            statute="31020990",
                                            description="UNLAWFUL USE OF WEAPON",
                                            is_violent="False",
                                            classification_type="F",
                                            classification_subtype="D",
                                        )
                                    ],
                                )
                            ],
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="310261",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="310261", id_type=US_MO_DOC
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="310261-19890821",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="310261-19890821-3",
                                    status="SERVING",
                                    date_imposed="20150428",
                                    start_date="20161206",
                                    projected_min_release_date="20211205",
                                    projected_max_release_date="20211205",
                                    parole_eligibility_date="2016-12-06",
                                    county_code="US_MO_ST_LOUIS_COUNTY",
                                    max_length="5Y 0M 0D",
                                    is_life="False",
                                    is_capital_punishment=None,
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id="310261-19890821-3",
                                            offense_date="20141004",
                                            county_code="US_MO_ST_LOUIS_COUNTY",
                                            ncic_code="3599",
                                            statute="91335990",
                                            description="POSSESSION OF CONTROLLED "
                                            "SUBSTANCE",
                                            is_violent="False",
                                            classification_type="L",
                                        )
                                    ],
                                )
                            ],
                        )
                    ],
                ),
                StatePerson(
                    state_person_id="710448",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="710448", id_type=US_MO_DOC
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="710448-20010414",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="710448-20010414-1",
                                    status="COMPLETED",
                                    date_imposed="20010627",
                                    start_date="20010805",
                                    projected_min_release_date="20050804",
                                    projected_max_release_date="20050804",
                                    parole_eligibility_date="2001-08-05",
                                    completion_date="20060911",
                                    county_code="US_MO_ST_LOUIS_CITY",
                                    max_length="4Y 0M 0D",
                                    is_life="False",
                                    is_capital_punishment="N",
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id="710448-20010414-1",
                                            offense_date="20000731",
                                            county_code="US_MO_ST_LOUIS_CITY",
                                            ncic_code="3599",
                                            statute="32450990",
                                            description="POSSESSION OF C/S",
                                            is_violent="False",
                                            classification_type="F",
                                            classification_subtype="C",
                                        )
                                    ],
                                ),
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="710448-20010414-2",
                                    status="COMPLETED",
                                    date_imposed="20021031",
                                    start_date="20021101",
                                    projected_min_release_date="20061031",
                                    projected_max_release_date="20061031",
                                    parole_eligibility_date="2002-11-01",
                                    completion_date="20060911",
                                    county_code="US_MO_ST_LOUIS_COUNTY",
                                    max_length="5Y 0M 0D",
                                    is_life="False",
                                    is_capital_punishment="N",
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id="710448-20010414-2",
                                            offense_date="20010915",
                                            ncic_code="3599",
                                            statute="32500990",
                                            description="TRAFFICKING 2ND DEGREE",
                                            is_violent="False",
                                            classification_type="F",
                                            classification_subtype="B",
                                        )
                                    ],
                                ),
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="710448-20010414-3",
                                    status="COMPLETED",
                                    date_imposed="20021031",
                                    start_date="20021101",
                                    projected_min_release_date="20051031",
                                    projected_max_release_date="20051031",
                                    parole_eligibility_date="2002-11-01",
                                    completion_date="20060911",
                                    county_code="US_MO_ST_LOUIS_COUNTY",
                                    max_length="4Y 0M 0D",
                                    is_life="False",
                                    is_capital_punishment="N",
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id="710448-20010414-3",
                                            offense_date="20010915",
                                            ncic_code="4899",
                                            statute="27020990",
                                            description="RESISTING ARREST",
                                            is_violent="False",
                                            classification_type="F",
                                            classification_subtype="D",
                                        )
                                    ],
                                ),
                            ],
                        )
                    ],
                ),
                StatePerson(
                    state_person_id="910324",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="910324", id_type=US_MO_DOC
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="910324-19890825",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="910324-19890825-1",
                                    status="COMPLETED",
                                    date_imposed="19890829",
                                    start_date="19890430",
                                    projected_min_release_date="19911229",
                                    projected_max_release_date="19930429",
                                    parole_eligibility_date="1989-04-30",
                                    completion_date="19900629",
                                    county_code="US_MO_LIVINGSTON",
                                    max_length="4Y 0M 0D",
                                    is_life="False",
                                    is_capital_punishment="N",
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id="910324-19890825-1",
                                            offense_date=None,
                                            ncic_code="3572",
                                            statute="32040720",
                                            description="POSSESSION OF METHAMPHETAMINE",
                                            is_violent="False",
                                            classification_type="F",
                                            classification_subtype="N",
                                        )
                                    ],
                                )
                            ],
                        )
                    ],
                ),
            ]
        )

        self.run_parse_file_test(
            expected, "tak022_tak023_tak025_tak026_offender_sentence_institution"
        )

    def test_populate_data_tak022_tak024_tak025_tak026_offender_sentence_supervision(
        self,
    ) -> None:
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="910324",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="910324", id_type=US_MO_DOC
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="910324-19890825",
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_sentence_id="910324-19890825-1",
                                    status="REVOKED",
                                    supervision_type="PROBATION",
                                    start_date="19870126",
                                    projected_completion_date="19920125",
                                    completion_date="19890811",
                                    county_code="US_MO_DEKALB",
                                    max_length="5Y 0M 0D",
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id="910324-19890825-1",
                                            ncic_code="5404",
                                            statute="47410040",
                                            description="DRIVING WHILE INTOXICATED - "
                                            "THIRD OFFENSE",
                                            is_violent="True",
                                            classification_type="F",
                                            classification_subtype="D",
                                        )
                                    ],
                                ),
                                StateSupervisionSentence(
                                    state_supervision_sentence_id="910324-19890825-2",
                                    status="COMPLETED",
                                    supervision_type="PROBATION",
                                    start_date="19870416",
                                    projected_completion_date="19880206",
                                    completion_date="19880222",
                                    county_code="US_MO_PLATTE",
                                    max_length="1Y 0M 0D",
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id="910324-19890825-2",
                                            ncic_code="5404",
                                            description="DWI",
                                            is_violent="True",
                                            classification_type="F",
                                        )
                                    ],
                                ),
                            ],
                        )
                    ],
                ),
                StatePerson(
                    state_person_id="310261",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="310261", id_type=US_MO_DOC
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="310261-19890821",
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_sentence_id="310261-19890821-1",
                                    status="COMPLETED",
                                    supervision_type="PROBATION",
                                    projected_completion_date="19801006",
                                    completion_date="19790607",
                                    county_code="OUT_OF_STATE",
                                    max_length="0Y 0M 0D",
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id="310261-19890821-1",
                                            ncic_code="5707",
                                            description="BRK & END",
                                            is_violent="False",
                                        )
                                    ],
                                )
                            ],
                        )
                    ],
                ),
                StatePerson(
                    state_person_id="110035",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="110035", id_type=US_MO_DOC
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="110035-20040712",
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_sentence_id="110035-20040712-1",
                                    status="COMPLETED",
                                    supervision_type="PROBATION",
                                    start_date="20040201",
                                    projected_completion_date="20080120",
                                    completion_date="20080119",
                                    county_code="US_MO_GREENE",
                                    max_length="3Y 0M 0D",
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id="110035-20040712-1",
                                            ncic_code="2399",
                                            description="BURG&STEAL",
                                            is_violent="False",
                                            classification_type="F",
                                        )
                                    ],
                                ),
                                StateSupervisionSentence(
                                    state_supervision_sentence_id="110035-20040712-3",
                                    status="COMPLETED",
                                    supervision_type="UNKNOWN",
                                    start_date="20040201",
                                    completion_date="20061010",
                                    county_code="US_MO_GREENE",
                                    max_length="2436835",
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id="110035-20040712-3",
                                            ncic_code="1399",
                                            statute="1310099",
                                            description="ASSAULT OF A LAW ENFORCEMENT OFFICER",
                                            classification_type="F",
                                            classification_subtype="A",
                                        )
                                    ],
                                ),
                            ],
                        ),
                        StateSentenceGroup(
                            state_sentence_group_id="110035-20081010",
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_sentence_id="110035-20081010-1",
                                    status="COMPLETED",
                                    supervision_type="PAROLE",
                                    start_date="20081101",
                                    projected_completion_date="20100113",
                                    completion_date="20100213",
                                    county_code="OUT_OF_STATE",
                                    max_length="0Y 0M 0D",
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id="110035-20081010-1",
                                            ncic_code="3532",
                                            offense_date="20070701",
                                            county_code="OUT_OF_STATE",
                                            description="POSSESSION OF COCAINE  (SHAWNEE CO 00CR1806)",
                                            is_violent="False",
                                        )
                                    ],
                                )
                            ],
                        ),
                    ],
                ),
            ]
        )

        self.run_parse_file_test(
            expected, "tak022_tak024_tak025_tak026_offender_sentence_supervision"
        )

    def test_populate_data_tak158_tak023_tak026_incarceration_period_from_incarceration_sentence(
        self,
    ) -> None:
        vr_110035_19890901_5 = StateSupervisionViolationResponse(
            response_type="PERMANENT_DECISION",
            response_date="19940609",
            decision="REVOCATION",
            revocation_type="S",
            deciding_body_type="PAROLE_BOARD",
        )

        vr_110035_20010414_2 = StateSupervisionViolationResponse(
            response_type="PERMANENT_DECISION",
            response_date="20010420",
            decision="REVOCATION",
            revocation_type="X",
            deciding_body_type="PAROLE_BOARD",
        )

        vr_110035_20010414_7_0 = StateSupervisionViolationResponse(
            response_type="PERMANENT_DECISION",
            response_date="20160328",
            decision="REVOCATION",
            revocation_type="S",
            deciding_body_type="PAROLE_BOARD",
        )
        vr_110035_20010414_7_3 = StateSupervisionViolationResponse(
            response_type="PERMANENT_DECISION",
            response_date="20160428",
            decision="REVOCATION",
            revocation_type="S",
            deciding_body_type="PAROLE_BOARD",
        )

        ip_110035_19890901_1_0 = StateIncarcerationPeriod(
            state_incarceration_period_id="110035-19890901-1-0",
            status="NOT_IN_CUSTODY",
            admission_date="19890901",
            admission_reason="10I1000",
            release_date="19921006",
            release_reason="IT-EM",
            specialized_purpose_for_incarceration="S",
        )
        ip_110035_19890901_3_0 = StateIncarcerationPeriod(
            state_incarceration_period_id="110035-19890901-3-0",
            status="NOT_IN_CUSTODY",
            admission_date="19930701",
            admission_reason="40I1060",
            release_date="19931102",
            release_reason="IT-BP",
            specialized_purpose_for_incarceration="I",
        )
        ip_110035_19890901_5_0 = StateIncarcerationPeriod(
            state_incarceration_period_id="110035-19890901-5-0",
            status="NOT_IN_CUSTODY",
            admission_date="19940609",
            admission_reason="40I1020,40I2000",
            release_date="19950206",
            release_reason="IT-BD",
            specialized_purpose_for_incarceration="S",
            source_supervision_violation_response=vr_110035_19890901_5,
        )

        ip_110035_20010414_2_0 = StateIncarcerationPeriod(
            state_incarceration_period_id="110035-20010414-2-0",
            status="NOT_IN_CUSTODY",
            admission_date="20010420",
            admission_reason="40I2000,50N1010",
            release_date="20121102",
            release_reason="IT-BP",
            specialized_purpose_for_incarceration="X",
            source_supervision_violation_response=vr_110035_20010414_2,
        )
        ip_110035_20010414_4_0 = StateIncarcerationPeriod(
            state_incarceration_period_id="110035-20010414-4-0",
            status="NOT_IN_CUSTODY",
            admission_date="20130521",
            admission_reason="45O0050,40I0050",
            release_date="20131127",
            release_reason="IT-BP",
            specialized_purpose_for_incarceration="S",
        )
        ip_110035_20010414_7_0 = StateIncarcerationPeriod(
            state_incarceration_period_id="110035-20010414-7-0",
            status="NOT_IN_CUSTODY",
            admission_date="20160328",
            admission_reason="45O0050,65N9999,40I1010",
            release_date="20160428",
            release_reason="50N1010",
            specialized_purpose_for_incarceration="S",
            source_supervision_violation_response=vr_110035_20010414_7_0,
        )
        ip_110035_20010414_7_3 = StateIncarcerationPeriod(
            state_incarceration_period_id="110035-20010414-7-3",
            status="NOT_IN_CUSTODY",
            admission_date="20160428",
            admission_reason="50N1010",
            release_date="20161011",
            release_reason="ID-DR",
            specialized_purpose_for_incarceration="S",
            source_supervision_violation_response=vr_110035_20010414_7_3,
        )

        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="110035",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="110035", id_type=US_MO_DOC
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="110035-19890901",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="110035-19890901-1",
                                    state_incarceration_periods=[
                                        ip_110035_19890901_1_0,
                                        ip_110035_19890901_3_0,
                                        ip_110035_19890901_5_0,
                                    ],
                                )
                            ],
                        ),
                        StateSentenceGroup(
                            state_sentence_group_id="110035-20010414",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="110035-20010414-1",
                                    state_incarceration_periods=[
                                        ip_110035_20010414_2_0,
                                        ip_110035_20010414_4_0,
                                        ip_110035_20010414_7_0,
                                        ip_110035_20010414_7_3,
                                    ],
                                )
                            ],
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="310261",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="310261", id_type=US_MO_DOC
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="310261-19890821",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="310261-19890821-3",
                                    state_incarceration_periods=[
                                        StateIncarcerationPeriod(
                                            state_incarceration_period_id="310261-19890821-1-0",
                                            status="IN_CUSTODY",
                                            admission_date="19900329",
                                            admission_reason="10I1000",
                                            specialized_purpose_for_incarceration="S",
                                        )
                                    ],
                                )
                            ],
                        )
                    ],
                ),
                StatePerson(
                    state_person_id="710448",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="710448", id_type=US_MO_DOC
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="710448-20010414",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="710448-20010414-1",
                                    state_incarceration_periods=[
                                        StateIncarcerationPeriod(
                                            state_incarceration_period_id="710448-20010414-1-0",
                                            status="NOT_IN_CUSTODY",
                                            admission_date="20010705",
                                            admission_reason="10I1000",
                                            release_date="20020117",
                                            release_reason="IT-EM",
                                            specialized_purpose_for_incarceration="S",
                                        )
                                    ],
                                ),
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="710448-20010414-3",
                                    state_incarceration_periods=[
                                        StateIncarcerationPeriod(
                                            state_incarceration_period_id="710448-20010414-3-0",
                                            status="NOT_IN_CUSTODY",
                                            admission_date="20020912",
                                            admission_reason="40I1060",
                                            release_date="20040928",
                                            release_reason="IT-BP",
                                            specialized_purpose_for_incarceration="S",
                                        )
                                    ],
                                ),
                            ],
                        )
                    ],
                ),
                StatePerson(
                    state_person_id="910324",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="910324", id_type=US_MO_DOC
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="910324-19890825",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="910324-19890825-1",
                                    state_incarceration_periods=[
                                        StateIncarcerationPeriod(
                                            state_incarceration_period_id="910324-19890825-1-0",
                                            status="NOT_IN_CUSTODY",
                                            admission_date="19891023",
                                            admission_reason="10I1000",
                                            release_date="19890516",
                                            release_reason="IE-IE",
                                            specialized_purpose_for_incarceration="O",
                                        )
                                    ],
                                )
                            ],
                        )
                    ],
                ),
                StatePerson(
                    state_person_id="523523",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="523523", id_type=US_MO_DOC
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="523523-19890617",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="523523-19890617-1",
                                    state_incarceration_periods=[
                                        StateIncarcerationPeriod(
                                            state_incarceration_period_id="523523-19890617-1-0",
                                            status="NOT_IN_CUSTODY",
                                            admission_date="19890617",
                                            admission_reason="10I1000",
                                            release_date="20101020",
                                            specialized_purpose_for_incarceration="S",
                                        )
                                    ],
                                )
                            ],
                        )
                    ],
                ),
                StatePerson(
                    state_person_id="867530",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="867530", id_type=US_MO_DOC
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="867530-19970224",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="867530-19970224-1",
                                    state_incarceration_periods=[
                                        StateIncarcerationPeriod(
                                            state_incarceration_period_id="867530-19970224-1-0",
                                            status="NOT_IN_CUSTODY",
                                            admission_date="19970224",
                                            admission_reason="10I1000",
                                            release_date="20161031",
                                            release_reason="DE-EX",
                                            specialized_purpose_for_incarceration="S",
                                        )
                                    ],
                                )
                            ],
                        )
                    ],
                ),
            ]
        )

        self.run_parse_file_test(
            expected,
            "tak158_tak023_tak026_incarceration_period_from_incarceration_sentence",
        )

    def test_populate_data_tak158_tak024_tak026_incarceration_period_from_supervision_sentence(
        self,
    ) -> None:
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="910324",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="910324", id_type=US_MO_DOC
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="910324-19890825",
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_sentence_id="910324-19890825-1",
                                    state_incarceration_periods=[
                                        StateIncarcerationPeriod(
                                            state_incarceration_period_id="910324-19890825-1-0",
                                            status="NOT_IN_CUSTODY",
                                            admission_date="19891023",
                                            admission_reason="10I1000",
                                            release_date="19890516",
                                            release_reason="IE-IE",
                                            specialized_purpose_for_incarceration="O",
                                        )
                                    ],
                                )
                            ],
                        )
                    ],
                ),
                StatePerson(
                    state_person_id="523523",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="523523", id_type=US_MO_DOC
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="523523-19890617",
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_sentence_id="523523-19890617-1",
                                    state_incarceration_periods=[
                                        StateIncarcerationPeriod(
                                            state_incarceration_period_id="523523-19890617-1-0",
                                            status="NOT_IN_CUSTODY",
                                            admission_date="19890617",
                                            admission_reason="10I1000",
                                            release_date="20101020",
                                            specialized_purpose_for_incarceration="S",
                                        )
                                    ],
                                )
                            ],
                        )
                    ],
                ),
            ]
        )

        self.run_parse_file_test(
            expected,
            "tak158_tak024_tak026_incarceration_period_from_supervision_sentence",
        )

    def test_populate_data_tak034_tak026_tak039_apfx90_apfx91_supervision_enhancements_supervision_periods(
        self,
    ) -> None:

        po_E123 = StateAgent(
            agent_type="PROBATION/PAROLE UNIT SPV",
            state_agent_id="E123",
            given_names="FIRST",
            surname="LAST",
            middle_names="MIDDLE",
        )

        po_E234 = StateAgent(
            agent_type="CORRECTIONS CASE MANAGER",
            state_agent_id="E234",
            given_names="F",
            surname="L",
        )

        po_E567 = StateAgent(state_agent_id="E567")

        sp_110035_19890901_2_0 = StateSupervisionPeriod(
            state_supervision_period_id="110035-19890901-2-0",
            start_date="19921006",
            admission_reason="40O1010,40O0000",
            termination_date="19930701",
            termination_reason="40I1050,45O1010",
            supervision_site="14",
            supervision_level="ISP",
            supervising_officer=po_E123,
        )
        sp_110035_19890901_4_0 = StateSupervisionPeriod(
            state_supervision_period_id="110035-19890901-4-0",
            start_date="19931102",
            admission_reason="40O1030,40O0000",
            termination_date="19940609",
            termination_reason="40I1050,45O1010",
            supervision_site="14",
            supervising_officer=po_E123,
        )
        sp_110035_19890901_6_0 = StateSupervisionPeriod(
            state_supervision_period_id="110035-19890901-6-0",
            start_date="19950206",
            admission_reason="40O1030,40O0000",
            termination_date="19950323",
            termination_reason="99O2010",
            supervision_site="14",
            supervising_officer=po_E123,
        )

        sp_110035_20040712_1_0 = StateSupervisionPeriod(
            state_supervision_period_id="110035-20040712-1-0",
            start_date="20040712",
            admission_reason="40O1010",
            termination_date="20050808",
            termination_reason="65L9100",
            supervision_site="14",
            supervising_officer=po_E123,
            supervision_level="ITC",
            state_supervision_case_type_entries=[
                StateSupervisionCaseTypeEntry(case_type="DSO"),
                StateSupervisionCaseTypeEntry(case_type="DVS"),
            ],
        )

        sp_110035_20040712_1_8 = StateSupervisionPeriod(
            state_supervision_period_id="110035-20040712-1-8",
            start_date="20050808",
            admission_reason="65L9100",
            termination_date="20050909",
            termination_reason="65N9500",
            supervision_site="14",
            supervising_officer=po_E123,
            supervision_level="ITC",
            state_supervision_case_type_entries=[
                StateSupervisionCaseTypeEntry(case_type="DSO"),
                StateSupervisionCaseTypeEntry(case_type="DVS"),
            ],
        )
        sp_110035_20040712_1_9 = StateSupervisionPeriod(
            state_supervision_period_id="110035-20040712-1-9",
            start_date="20050909",
            admission_reason="65N9500",
            termination_date="20080119",
            termination_reason="99O2010",
            supervision_site="14",
            supervising_officer=po_E123,
            supervision_level="ITC",
            state_supervision_case_type_entries=[
                StateSupervisionCaseTypeEntry(case_type="DSO"),
                StateSupervisionCaseTypeEntry(case_type="DVS"),
            ],
        )

        sp_110035_20010414_1_0 = StateSupervisionPeriod(
            state_supervision_period_id="110035-20010414-1-0",
            start_date="20010414",
            admission_reason="15I1000",
            termination_date="20010420",
            termination_reason="65O2015",
            supervision_site="14",
            supervising_officer=po_E123,
        )
        sp_110035_20010414_3_0 = StateSupervisionPeriod(
            state_supervision_period_id="110035-20010414-3-0",
            start_date="20121102",
            admission_reason="65I2015",
            termination_date="20130521",
            termination_reason="45O0050,65N9999,40I0050",
            supervision_site="14",
            supervising_officer=po_E123,
        )
        sp_110035_20010414_5_0 = StateSupervisionPeriod(
            state_supervision_period_id="110035-20010414-5-0",
            start_date="20131127",
            admission_reason="40O1030",
            termination_date="20150706",
            termination_reason="65L9100",
            supervision_site="14",
            supervising_officer=po_E123,
        )
        sp_110035_20010414_6_0 = StateSupervisionPeriod(
            state_supervision_period_id="110035-20010414-6-0",
            start_date="20160328",
            admission_reason="65N9500",
            termination_date="20160328",
            termination_reason="45O1010,40I1010",
            supervision_site="14",
            supervising_officer=po_E123,
        )

        sp_710448_20010414_2_0 = StateSupervisionPeriod(
            state_supervision_period_id="710448-20010414-2-0",
            start_date="20020117",
            admission_reason="40O4199",
            termination_date="20020401",
            termination_reason="TRANSFER_WITHIN_STATE",
            supervision_site="ERA",
            supervising_officer=po_E567,
        )

        sp_710448_20010414_3_0 = StateSupervisionPeriod(
            state_supervision_period_id="710448-20010414-3-0",
            start_date="20020401",
            admission_reason="TRANSFER_WITHIN_STATE",
            termination_date="20020912",
            termination_reason="45O1010",
            supervision_site="14",
            supervising_officer=po_E123,
        )

        sp_710448_20010414_4_0 = StateSupervisionPeriod(
            state_supervision_period_id="710448-20010414-4-0",
            start_date="20040928",
            admission_reason="40O1010",
            termination_date="20060911",
            termination_reason="99O2010",
            supervision_site="ERA",
            supervising_officer=po_E567,
        )

        sp_910324_19890825_2_0 = StateSupervisionPeriod(
            state_supervision_period_id="910324-19890825-2-0",
            start_date="19890516",
            admission_reason="40O1010",
            supervision_site="17",
            supervising_officer=po_E234,
            supervision_level="IAP",
            state_supervision_case_type_entries=[
                StateSupervisionCaseTypeEntry(case_type="DSO"),
            ],
        )

        sp_624624_19890617_1_0 = StateSupervisionPeriod(
            state_supervision_period_id="624624-19890617-1-0",
            start_date="19890617",
            admission_reason="15I1000",
            supervision_site="17",
            supervising_officer=po_E234,
            supervision_level="III",
            state_supervision_case_type_entries=[
                StateSupervisionCaseTypeEntry(case_type="DOM"),
                StateSupervisionCaseTypeEntry(case_type="DVS"),
                StateSupervisionCaseTypeEntry(case_type="SMI"),
            ],
        )

        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="110035",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="110035", id_type=US_MO_DOC
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="110035-19890901",
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        sp_110035_19890901_2_0,
                                        sp_110035_19890901_4_0,
                                        sp_110035_19890901_6_0,
                                    ]
                                )
                            ],
                        ),
                        StateSentenceGroup(
                            state_sentence_group_id="110035-20010414",
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        sp_110035_20010414_1_0,
                                        sp_110035_20010414_3_0,
                                        sp_110035_20010414_5_0,
                                        sp_110035_20010414_6_0,
                                    ]
                                )
                            ],
                        ),
                        StateSentenceGroup(
                            state_sentence_group_id="110035-20040712",
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        sp_110035_20040712_1_0,
                                        sp_110035_20040712_1_8,
                                        sp_110035_20040712_1_9,
                                    ]
                                )
                            ],
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="710448",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="710448", id_type=US_MO_DOC
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="710448-20010414",
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        sp_710448_20010414_2_0,
                                        sp_710448_20010414_3_0,
                                        sp_710448_20010414_4_0,
                                    ]
                                ),
                            ],
                        )
                    ],
                ),
                StatePerson(
                    state_person_id="910324",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="910324", id_type=US_MO_DOC
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="910324-19890825",
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[sp_910324_19890825_2_0]
                                )
                            ],
                        )
                    ],
                ),
                StatePerson(
                    state_person_id="624624",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="624624", id_type=US_MO_DOC
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="624624-19890617",
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[sp_624624_19890617_1_0]
                                )
                            ],
                        )
                    ],
                ),
            ]
        )

        self.run_parse_file_test(
            expected,
            "tak034_tak026_tak039_apfx90_apfx91_supervision_enhancements_supervision_periods",
        )

    def test_populate_data_tak028_tak042_tak076_tak024_violation_reports(self) -> None:
        sss_110035_20040712_1 = StateSupervisionSentence(
            state_supervision_sentence_id="110035-20040712-1",
            state_supervision_periods=[
                StateSupervisionPeriod(
                    state_supervision_violation_entries=[
                        StateSupervisionViolation(
                            state_supervision_violation_id="110035-20040712-R1-1-1",
                            violation_date="0",
                            state_supervision_violation_types=[
                                StateSupervisionViolationTypeEntry(violation_type="T"),
                            ],
                            state_supervision_violated_conditions=[
                                StateSupervisionViolatedConditionEntry(condition="DIR"),
                                StateSupervisionViolatedConditionEntry(condition="EMP"),
                                StateSupervisionViolatedConditionEntry(condition="RES"),
                            ],
                            state_supervision_violation_responses=[
                                StateSupervisionViolationResponse(
                                    state_supervision_violation_response_id="110035-20040712-R1-1-1",
                                    response_type="VIOLATION_REPORT",
                                    response_subtype="ITR",
                                    response_date="2005-01-11",
                                    is_draft="False",
                                    deciding_body_type="SUPERVISION_OFFICER",
                                    decision_agents=[
                                        StateAgent(
                                            agent_type="PROBATION/PAROLE UNIT SPV",
                                            state_agent_id="E123",
                                            given_names="FIRST",
                                            surname="LAST",
                                            middle_names="MIDDLE",
                                        )
                                    ],
                                    supervision_violation_response_decisions=[
                                        StateSupervisionViolationResponseDecisionEntry(
                                            decision="A",
                                        ),
                                        StateSupervisionViolationResponseDecisionEntry(
                                            decision="C",
                                        ),
                                        StateSupervisionViolationResponseDecisionEntry(
                                            decision="R",
                                            revocation_type="REINCARCERATION",
                                        ),
                                    ],
                                )
                            ],
                        ),
                        StateSupervisionViolation(
                            state_supervision_violation_id="110035-20040712-R2-1-1",
                            violation_date="20060101",
                            state_supervision_violation_types=[
                                StateSupervisionViolationTypeEntry(violation_type="A"),
                                StateSupervisionViolationTypeEntry(violation_type="E"),
                            ],
                            state_supervision_violated_conditions=[
                                StateSupervisionViolatedConditionEntry(condition="SPC"),
                            ],
                            state_supervision_violation_responses=[
                                StateSupervisionViolationResponse(
                                    state_supervision_violation_response_id="110035-20040712-R2-1-1",
                                    response_type="VIOLATION_REPORT",
                                    response_subtype="ITR",
                                    is_draft="False",
                                    response_date="2006-01-11",
                                    deciding_body_type="SUPERVISION_OFFICER",
                                    decision_agents=[
                                        StateAgent(
                                            agent_type="PROBATION/PAROLE UNIT SPV",
                                            state_agent_id="E123",
                                            given_names="FIRST",
                                            surname="LAST",
                                            middle_names="MIDDLE",
                                        )
                                    ],
                                )
                            ],
                        ),
                    ]
                )
            ],
        )
        sss_110035_20040712_2 = StateSupervisionSentence(
            state_supervision_sentence_id="110035-20040712-2",
            state_supervision_periods=[
                StateSupervisionPeriod(
                    state_supervision_violation_entries=[
                        StateSupervisionViolation(
                            state_supervision_violation_id="110035-20040712-R2-2-1",
                            violation_date="20060101",
                            state_supervision_violated_conditions=[
                                StateSupervisionViolatedConditionEntry(condition="SPC"),
                            ],
                            state_supervision_violation_types=[
                                StateSupervisionViolationTypeEntry(violation_type="A"),
                                StateSupervisionViolationTypeEntry(violation_type="E"),
                            ],
                            state_supervision_violation_responses=[
                                StateSupervisionViolationResponse(
                                    state_supervision_violation_response_id="110035-20040712-R2-2-1",
                                    response_type="VIOLATION_REPORT",
                                    response_subtype="ITR",
                                    is_draft="False",
                                    response_date="2006-01-11",
                                    deciding_body_type="SUPERVISION_OFFICER",
                                    decision_agents=[
                                        StateAgent(
                                            agent_type="PROBATION/PAROLE UNIT SPV",
                                            state_agent_id="E123",
                                            given_names="FIRST",
                                            surname="LAST",
                                            middle_names="MIDDLE",
                                        )
                                    ],
                                )
                            ],
                        )
                    ]
                )
            ],
        )
        sis_910324_19890825_1 = StateIncarcerationSentence(
            state_incarceration_sentence_id="910324-19890825-1",
            state_supervision_periods=[
                StateSupervisionPeriod(
                    state_supervision_violation_entries=[
                        StateSupervisionViolation(
                            state_supervision_violation_id="910324-19890825-R1-1-0",
                            violation_date="19890617",
                            state_supervision_violation_types=[
                                StateSupervisionViolationTypeEntry(violation_type="M"),
                            ],
                            state_supervision_violated_conditions=[
                                StateSupervisionViolatedConditionEntry(condition="EMP"),
                            ],
                            state_supervision_violation_responses=[
                                StateSupervisionViolationResponse(
                                    response_type="VIOLATION_REPORT",
                                    response_subtype="INI",
                                    state_supervision_violation_response_id="910324-19890825-R1-1-0",
                                    is_draft="True",
                                    response_date="19890616",
                                    deciding_body_type="SUPERVISION_OFFICER",
                                    decision_agents=[
                                        StateAgent(
                                            agent_type="CORRECTIONS CASE MANAGER",
                                            state_agent_id="E234",
                                            given_names="F",
                                            surname="L",
                                        )
                                    ],
                                    supervision_violation_response_decisions=[
                                        StateSupervisionViolationResponseDecisionEntry(
                                            decision="CO",
                                            revocation_type="SHOCK_INCARCERATION",
                                        ),
                                    ],
                                )
                            ],
                        )
                    ]
                )
            ],
        )

        sss_910324_19890825_1 = StateSupervisionSentence(
            state_supervision_sentence_id="910324-19890825-1",
            state_supervision_periods=[
                StateSupervisionPeriod(
                    state_supervision_violation_entries=[
                        StateSupervisionViolation(
                            state_supervision_violation_id="910324-19890825-R1-1-1",
                            violation_date="19890617",
                            state_supervision_violation_types=[
                                StateSupervisionViolationTypeEntry(violation_type="M"),
                            ],
                            state_supervision_violated_conditions=[
                                StateSupervisionViolatedConditionEntry(condition="EMP"),
                            ],
                            state_supervision_violation_responses=[
                                StateSupervisionViolationResponse(
                                    response_type="VIOLATION_REPORT",
                                    response_subtype="INI",
                                    state_supervision_violation_response_id="910324-19890825-R1-1-1",
                                    is_draft="True",
                                    response_date="19890616",
                                    deciding_body_type="SUPERVISION_OFFICER",
                                    decision_agents=[
                                        StateAgent(
                                            agent_type="CORRECTIONS CASE MANAGER",
                                            state_agent_id="E234",
                                            given_names="F",
                                            surname="L",
                                        )
                                    ],
                                    supervision_violation_response_decisions=[
                                        StateSupervisionViolationResponseDecisionEntry(
                                            decision="CO",
                                            revocation_type="SHOCK_INCARCERATION",
                                        ),
                                    ],
                                )
                            ],
                        ),
                        StateSupervisionViolation(
                            state_supervision_violation_id="910324-19890825-R2-1-1",
                            violation_date="19890804",
                            state_supervision_violation_types=[
                                StateSupervisionViolationTypeEntry(violation_type="T"),
                            ],
                            state_supervision_violated_conditions=[
                                StateSupervisionViolatedConditionEntry(condition="SPC"),
                            ],
                            state_supervision_violation_responses=[
                                StateSupervisionViolationResponse(
                                    response_type="VIOLATION_REPORT",
                                    response_subtype="SUP",
                                    state_supervision_violation_response_id="910324-19890825-R2-1-1",
                                    response_date="1989-09-03",
                                    is_draft="False",
                                    deciding_body_type="SUPERVISION_OFFICER",
                                    decision_agents=[
                                        StateAgent(
                                            agent_type="LIBRARIAN I",
                                            state_agent_id="E345",
                                            given_names="F",
                                            surname="L",
                                        )
                                    ],
                                    supervision_violation_response_decisions=[
                                        StateSupervisionViolationResponseDecisionEntry(
                                            decision="RN",
                                        ),
                                    ],
                                )
                            ],
                        ),
                    ]
                )
            ],
        )

        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="110035",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="110035", id_type=US_MO_DOC
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="110035-20040712",
                            state_supervision_sentences=[
                                sss_110035_20040712_1,
                                sss_110035_20040712_2,
                            ],
                        )
                    ],
                ),
                StatePerson(
                    state_person_id="910324",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="910324", id_type=US_MO_DOC
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="910324-19890825",
                            state_supervision_sentences=[sss_910324_19890825_1],
                            state_incarceration_sentences=[sis_910324_19890825_1],
                        )
                    ],
                ),
            ]
        )

        self.run_parse_file_test(
            expected, "tak028_tak042_tak076_tak024_violation_reports"
        )

    def test_populate_data_tak291_tak292_tak024_citations(self) -> None:
        sss_110035_20040712_1 = StateSupervisionSentence(
            state_supervision_sentence_id="110035-20040712-1",
            state_supervision_periods=[
                StateSupervisionPeriod(
                    state_supervision_violation_entries=[
                        StateSupervisionViolation(
                            state_supervision_violation_id="110035-20040712-C1-1-1",
                            state_supervision_violated_conditions=[
                                StateSupervisionViolatedConditionEntry(condition="DRG"),
                                StateSupervisionViolatedConditionEntry(condition="LAW"),
                            ],
                            state_supervision_violation_responses=[
                                StateSupervisionViolationResponse(
                                    state_supervision_violation_response_id="110035-20040712-C1-1-1",
                                    response_type="CITATION",
                                    response_date="20070210",
                                    is_draft="True",
                                    deciding_body_type="SUPERVISION_OFFICER",
                                )
                            ],
                        )
                    ]
                )
            ],
        )
        sss_110035_20040712_2 = StateSupervisionSentence(
            state_supervision_sentence_id="110035-20040712-2",
            state_supervision_periods=[
                StateSupervisionPeriod(
                    state_supervision_violation_entries=[
                        StateSupervisionViolation(
                            state_supervision_violation_id="110035-20040712-C1-2-2",
                            state_supervision_violated_conditions=[
                                StateSupervisionViolatedConditionEntry(condition="DRG"),
                                StateSupervisionViolatedConditionEntry(condition="LAW"),
                            ],
                            state_supervision_violation_responses=[
                                StateSupervisionViolationResponse(
                                    state_supervision_violation_response_id="110035-20040712-C1-2-2",
                                    response_type="CITATION",
                                    response_date="20070210",
                                    is_draft="True",
                                    deciding_body_type="SUPERVISION_OFFICER",
                                )
                            ],
                        )
                    ]
                )
            ],
        )
        sis_910324_19890825_1 = StateIncarcerationSentence(
            state_incarceration_sentence_id="910324-19890825-1",
            state_supervision_periods=[
                StateSupervisionPeriod(
                    state_supervision_violation_entries=[
                        StateSupervisionViolation(
                            state_supervision_violation_id="910324-19890825-C1-1-0",
                            state_supervision_violated_conditions=[
                                StateSupervisionViolatedConditionEntry(condition="DRG"),
                            ],
                            state_supervision_violation_responses=[
                                StateSupervisionViolationResponse(
                                    state_supervision_violation_response_id="910324-19890825-C1-1-0",
                                    response_type="CITATION",
                                    response_date="1990-01-01",
                                    is_draft="False",
                                    deciding_body_type="SUPERVISION_OFFICER",
                                )
                            ],
                        ),
                    ]
                )
            ],
        )
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="110035",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="110035", id_type=US_MO_DOC
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="110035-20040712",
                            state_supervision_sentences=[
                                sss_110035_20040712_1,
                                sss_110035_20040712_2,
                            ],
                        )
                    ],
                ),
                StatePerson(
                    state_person_id="910324",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="910324", id_type=US_MO_DOC
                        ),
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="910324-19890825",
                            state_incarceration_sentences=[sis_910324_19890825_1],
                        )
                    ],
                ),
            ]
        )

        self.run_parse_file_test(expected, "tak291_tak292_tak024_citations")

    def test_run_full_ingest_all_files_specific_order(self) -> None:
        self.maxDiff = None

        ######################################
        # TAK001 OFFENDER IDENTIFICATION
        ######################################
        # Arrange
        sg_110035_19890901 = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-19890901",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        person_110035 = entities.StatePerson.new_with_defaults(
            full_name='{"given_names": "FRANK", "name_suffix": "JR", '
            '"surname": "ABAGNALE"}',
            gender=Gender.MALE,
            gender_raw_text="M",
            birthdate=datetime.date(year=1971, month=11, day=20),
            birthdate_inferred_from_age=False,
            state_code=_STATE_CODE_UPPER,
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id="110035",
                    id_type=US_MO_DOC,
                )
            ],
            aliases=[
                entities.StatePersonAlias.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    full_name='{"given_names": "FRANK", "name_suffix": "JR", '
                    '"surname": "ABAGNALE"}',
                    alias_type=StatePersonAliasType.GIVEN_NAME,
                    alias_type_raw_text="GIVEN_NAME",
                )
            ],
            races=[
                entities.StatePersonRace.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    race=Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
                    race_raw_text="I",
                ),
            ],
            ethnicities=[
                entities.StatePersonEthnicity.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    ethnicity=Ethnicity.HISPANIC,
                    ethnicity_raw_text="H",
                )
            ],
            sentence_groups=[sg_110035_19890901],
        )

        sg_310261_19890821 = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="310261-19890821",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        person_310261 = entities.StatePerson.new_with_defaults(
            full_name='{"given_names": "MARTHA", "middle_names": "HELEN", '
            '"surname": "STEWART"}',
            gender=Gender.FEMALE,
            gender_raw_text="F",
            birthdate=datetime.date(year=1969, month=6, day=17),
            birthdate_inferred_from_age=False,
            state_code=_STATE_CODE_UPPER,
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id="310261",
                    id_type=US_MO_DOC,
                )
            ],
            aliases=[
                entities.StatePersonAlias.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    full_name='{"given_names": "MARTHA", '
                    '"middle_names": "HELEN", "surname": "STEWART"}',
                    alias_type=StatePersonAliasType.GIVEN_NAME,
                    alias_type_raw_text="GIVEN_NAME",
                )
            ],
            races=[
                entities.StatePersonRace.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    race=Race.WHITE,
                    race_raw_text="W",
                ),
            ],
            ethnicities=[
                entities.StatePersonEthnicity.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    ethnicity=Ethnicity.EXTERNAL_UNKNOWN,
                    ethnicity_raw_text="U",
                )
            ],
            sentence_groups=[
                sg_310261_19890821,
            ],
        )
        person_710448 = entities.StatePerson.new_with_defaults(
            full_name='{"given_names": "JULES", "surname": "WINNIFIELD"}',
            birthdate=datetime.date(year=1964, month=8, day=31),
            birthdate_inferred_from_age=False,
            state_code=_STATE_CODE_UPPER,
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id="710448",
                    id_type=US_MO_DOC,
                )
            ],
            aliases=[
                entities.StatePersonAlias.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    full_name='{"given_names": "JULES", "surname": "WINNIFIELD"}',
                    alias_type=StatePersonAliasType.GIVEN_NAME,
                    alias_type_raw_text="GIVEN_NAME",
                )
            ],
            races=[
                entities.StatePersonRace.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    race=Race.BLACK,
                    race_raw_text="B",
                ),
            ],
            sentence_groups=[
                entities.StateSentenceGroup.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id="710448-19890901",
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                )
            ],
        )

        sg_910324_19890825 = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="910324-19890825",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        person_910324 = entities.StatePerson.new_with_defaults(
            full_name='{"given_names": "KAONASHI"}',
            gender=Gender.EXTERNAL_UNKNOWN,
            gender_raw_text="U",
            birthdate=datetime.date(year=1958, month=2, day=13),
            birthdate_inferred_from_age=False,
            state_code="US_MO",
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id="910324",
                    id_type=US_MO_DOC,
                )
            ],
            aliases=[
                entities.StatePersonAlias.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    full_name='{"given_names": "KAONASHI"}',
                    alias_type=StatePersonAliasType.GIVEN_NAME,
                    alias_type_raw_text="GIVEN_NAME",
                )
            ],
            races=[
                entities.StatePersonRace.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    race=Race.ASIAN,
                    race_raw_text="A",
                ),
            ],
            ethnicities=[
                entities.StatePersonEthnicity.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    ethnicity=Ethnicity.NOT_HISPANIC,
                    ethnicity_raw_text="N",
                )
            ],
            sentence_groups=[
                sg_910324_19890825,
            ],
        )

        expected_people = [person_910324, person_710448, person_310261, person_110035]

        populate_person_backedges(expected_people)

        # SQL Preprocessing View
        # Act
        self._run_ingest_job_for_filename("tak001_offender_identification.csv")
        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # ORAS_ASSESSMENTS_WEEKLY
        ######################################
        # Arrange
        assessment_110035 = entities.StateAssessment.new_with_defaults(
            assessment_type=StateAssessmentType.ORAS_PRISON_INTAKE,
            assessment_type_raw_text="PRISON INTAKE TOOL",
            assessment_score=10,
            assessment_level=StateAssessmentLevel.MEDIUM,
            assessment_level_raw_text="MODERATE",
            assessment_date=datetime.date(year=2010, month=1, day=1),
            external_id="110035-1",
            state_code=_STATE_CODE_UPPER,
            person=person_110035,
        )

        assessment_310261_1 = entities.StateAssessment.new_with_defaults(
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_type_raw_text="COMMUNITY SUPERVISION TOOL",
            assessment_score=5,
            assessment_level=StateAssessmentLevel.LOW,
            assessment_level_raw_text="LOW",
            assessment_date=datetime.date(year=2000, month=1, day=1),
            external_id="310261-2",
            state_code=_STATE_CODE_UPPER,
            person=person_310261,
        )

        assessment_310261_2 = entities.StateAssessment.new_with_defaults(
            assessment_type=StateAssessmentType.ORAS_REENTRY,
            assessment_type_raw_text="REENTRY TOOL",
            assessment_score=7,
            assessment_level=StateAssessmentLevel.LOW_MEDIUM,
            assessment_level_raw_text="LOW/MODERATE",
            assessment_date=datetime.date(year=1900, month=1, day=1),
            external_id="310261-1",
            state_code=_STATE_CODE_UPPER,
            person=person_310261,
        )

        person_110035.assessments.append(assessment_110035)
        person_310261.assessments.append(assessment_310261_1)
        person_310261.assessments.append(assessment_310261_2)

        # SQL Preprocessing View
        # Act
        self._run_ingest_job_for_filename("oras_assessments_weekly.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # TAK040 OFFENDER CYCLES
        ######################################
        # Arrange
        sg_110035_19890901.person = person_110035

        sg_110035_20010414 = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-20010414",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_110035,
        )
        person_110035.sentence_groups.append(sg_110035_20010414)

        sg_710448_20010414 = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="710448-20010414",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_710448,
        )
        person_710448.sentence_groups.append(sg_710448_20010414)

        # SQL Preprocessing View
        # Act
        self._run_ingest_job_for_filename("tak040_offender_cycles.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ##############################################################
        # TAK022_TAK023_TAK025_TAK026 OFFENDER SENTENCE INSTITUTION
        ##############################################################
        # Arrange
        sis_110035_19890901_1 = entities.StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-19890901-1",
            status=StateSentenceStatus.COMPLETED,
            status_raw_text="COMPLETED",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            date_imposed=datetime.date(year=1956, month=3, day=16),
            start_date=datetime.date(year=1956, month=4, day=25),
            projected_min_release_date=None,
            projected_max_release_date=None,
            completion_date=datetime.date(year=1980, month=9, day=26),
            parole_eligibility_date=datetime.date(year=1956, month=4, day=25),
            county_code="US_MO_ST_LOUIS_CITY",
            max_length_days=3655253,
            is_life=True,
            is_capital_punishment=True,
            person=person_110035,
            sentence_group=sg_110035_19890901,
        )
        sg_110035_19890901.incarceration_sentences.append(sis_110035_19890901_1)

        charge_110035_19890901 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            external_id="110035-19890901-1",
            county_code="US_MO_JACKSON",
            ncic_code="0904",
            statute="10021040",
            description="TC: MURDER 1ST - FIST",
            is_violent=True,
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="F",
            classification_subtype="O",
            incarceration_sentences=[sis_110035_19890901_1],
            person=person_110035,
        )
        sis_110035_19890901_1.charges = [charge_110035_19890901]

        sis_110035_20010414_1 = entities.StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-20010414-1",
            status=StateSentenceStatus.COMPLETED,
            status_raw_text="COMPLETED",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            date_imposed=datetime.date(year=2003, month=1, day=10),
            start_date=datetime.date(year=2003, month=1, day=3),
            projected_min_release_date=datetime.date(year=2007, month=1, day=2),
            projected_max_release_date=datetime.date(year=2007, month=1, day=2),
            parole_eligibility_date=datetime.date(year=2003, month=1, day=3),
            completion_date=datetime.date(year=2007, month=1, day=2),
            county_code="US_MO_ST_LOUIS_CITY",
            max_length_days=1461,
            is_life=False,
            is_capital_punishment=False,
            person=person_110035,
            sentence_group=sg_110035_20010414,
        )
        sg_110035_20010414.incarceration_sentences.append(sis_110035_20010414_1)

        charge_110035_20010414 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-20010414-1",
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            offense_date=datetime.date(year=2000, month=6, day=4),
            county_code="US_MO_ST_LOUIS_CITY",
            ncic_code="5299",
            statute="31020990",
            description="UNLAWFUL USE OF WEAPON",
            is_violent=False,
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="F",
            classification_subtype="D",
            incarceration_sentences=[sis_110035_20010414_1],
            person=person_110035,
        )
        sis_110035_20010414_1.charges = [charge_110035_20010414]

        sg_310261_19890821.person = person_310261

        sis_310261_19890821_3 = entities.StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="310261-19890821-3",
            status=StateSentenceStatus.SERVING,
            status_raw_text="SERVING",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            date_imposed=datetime.date(year=2015, month=4, day=28),
            start_date=datetime.date(year=2016, month=12, day=6),
            projected_min_release_date=datetime.date(year=2021, month=12, day=5),
            projected_max_release_date=datetime.date(year=2021, month=12, day=5),
            parole_eligibility_date=datetime.date(year=2016, month=12, day=6),
            county_code="US_MO_ST_LOUIS_COUNTY",
            max_length_days=1826,
            is_life=False,
            is_capital_punishment=None,
            person=person_310261,
            sentence_group=sg_310261_19890821,
        )
        sg_310261_19890821.incarceration_sentences.append(sis_310261_19890821_3)

        charge_310261 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            external_id="310261-19890821-3",
            offense_date=datetime.date(year=2014, month=10, day=4),
            county_code="US_MO_ST_LOUIS_COUNTY",
            ncic_code="3599",
            statute="91335990",
            description="POSSESSION OF CONTROLLED SUBSTANCE",
            is_violent=False,
            classification_type=StateChargeClassificationType.INFRACTION,
            classification_type_raw_text="L",
            incarceration_sentences=[sis_310261_19890821_3],
            person=person_310261,
        )
        sis_310261_19890821_3.charges = [charge_310261]

        sis_710448_20010414_1 = entities.StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="710448-20010414-1",
            status=StateSentenceStatus.COMPLETED,
            status_raw_text="COMPLETED",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            date_imposed=datetime.date(year=2001, month=6, day=27),
            start_date=datetime.date(year=2001, month=8, day=5),
            projected_min_release_date=datetime.date(year=2005, month=8, day=4),
            projected_max_release_date=datetime.date(year=2005, month=8, day=4),
            parole_eligibility_date=datetime.date(year=2001, month=8, day=5),
            completion_date=datetime.date(year=2006, month=9, day=11),
            county_code="US_MO_ST_LOUIS_CITY",
            max_length_days=1461,
            is_life=False,
            is_capital_punishment=False,
            person=person_710448,
            sentence_group=sg_710448_20010414,
        )
        charge_710448_1 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="710448-20010414-1",
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            offense_date=datetime.date(year=2000, month=7, day=31),
            county_code="US_MO_ST_LOUIS_CITY",
            ncic_code="3599",
            statute="32450990",
            description="POSSESSION OF C/S",
            is_violent=False,
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="F",
            classification_subtype="C",
            incarceration_sentences=[sis_710448_20010414_1],
            person=person_710448,
        )
        sis_710448_20010414_1.charges = [charge_710448_1]

        sis_710448_20010414_2 = entities.StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="710448-20010414-2",
            status=StateSentenceStatus.COMPLETED,
            status_raw_text="COMPLETED",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            date_imposed=datetime.date(year=2002, month=10, day=31),
            start_date=datetime.date(year=2002, month=11, day=1),
            projected_min_release_date=datetime.date(year=2006, month=10, day=31),
            projected_max_release_date=datetime.date(year=2006, month=10, day=31),
            parole_eligibility_date=datetime.date(year=2002, month=11, day=1),
            completion_date=datetime.date(year=2006, month=9, day=11),
            county_code="US_MO_ST_LOUIS_COUNTY",
            max_length_days=1826,
            is_life=False,
            is_capital_punishment=False,
            person=person_710448,
            sentence_group=sg_710448_20010414,
        )
        charge_710448_2 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            external_id="710448-20010414-2",
            offense_date=datetime.date(year=2001, month=9, day=15),
            ncic_code="3599",
            statute="32500990",
            description="TRAFFICKING 2ND DEGREE",
            is_violent=False,
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="F",
            classification_subtype="B",
            incarceration_sentences=[sis_710448_20010414_2],
            person=person_710448,
        )
        sis_710448_20010414_2.charges = [charge_710448_2]

        sis_710448_20010414_3 = entities.StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="710448-20010414-3",
            status=StateSentenceStatus.COMPLETED,
            status_raw_text="COMPLETED",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            date_imposed=datetime.date(year=2002, month=10, day=31),
            start_date=datetime.date(year=2002, month=11, day=1),
            projected_min_release_date=datetime.date(year=2005, month=10, day=31),
            projected_max_release_date=datetime.date(year=2005, month=10, day=31),
            parole_eligibility_date=datetime.date(year=2002, month=11, day=1),
            completion_date=datetime.date(year=2006, month=9, day=11),
            county_code="US_MO_ST_LOUIS_COUNTY",
            max_length_days=1461,
            is_life=False,
            is_capital_punishment=False,
            person=person_710448,
            sentence_group=sg_710448_20010414,
        )
        charge_710448_3 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="710448-20010414-3",
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            offense_date=datetime.date(year=2001, month=9, day=15),
            ncic_code="4899",
            statute="27020990",
            description="RESISTING ARREST",
            is_violent=False,
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="F",
            classification_subtype="D",
            incarceration_sentences=[sis_710448_20010414_3],
            person=person_710448,
        )
        sis_710448_20010414_3.charges = [charge_710448_3]

        sg_710448_20010414.incarceration_sentences.append(sis_710448_20010414_1)
        sg_710448_20010414.incarceration_sentences.append(sis_710448_20010414_2)
        sg_710448_20010414.incarceration_sentences.append(sis_710448_20010414_3)

        sg_910324_19890825.person = person_910324

        sis_910324_19890825_1 = entities.StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="910324-19890825-1",
            status=StateSentenceStatus.COMPLETED,
            status_raw_text="COMPLETED",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            date_imposed=datetime.date(year=1989, month=8, day=29),
            start_date=datetime.date(year=1989, month=4, day=30),
            projected_min_release_date=datetime.date(year=1991, month=12, day=29),
            projected_max_release_date=datetime.date(year=1993, month=4, day=29),
            parole_eligibility_date=datetime.date(year=1989, month=4, day=30),
            completion_date=datetime.date(year=1990, month=6, day=29),
            county_code="US_MO_LIVINGSTON",
            max_length_days=1461,
            is_life=False,
            is_capital_punishment=False,
            person=person_910324,
            sentence_group=sg_910324_19890825,
        )
        sg_910324_19890825.incarceration_sentences.append(sis_910324_19890825_1)

        charge_910324 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            external_id="910324-19890825-1",
            ncic_code="3572",
            statute="32040720",
            description="POSSESSION OF METHAMPHETAMINE",
            is_violent=False,
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="F",
            classification_subtype="N",
            incarceration_sentences=[sis_910324_19890825_1],
            person=person_910324,
        )
        sis_910324_19890825_1.charges = [charge_910324]

        # SQL Preprocessing View
        # Act
        self._run_ingest_job_for_filename(
            "tak022_tak023_tak025_tak026_offender_sentence_institution.csv"
        )

        # Assert
        self.assert_expected_db_people(expected_people)

        ##############################################################
        # TAK022_TAK024_TAK025_TAK026 OFFENDER SENTENCE PROBATION
        ##############################################################
        # Arrange
        sss_910324_19890825_1 = entities.StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="910324-19890825-1",
            status=StateSentenceStatus.REVOKED,
            status_raw_text="REVOKED",
            supervision_type=StateSupervisionType.PROBATION,
            supervision_type_raw_text="PROBATION",
            start_date=datetime.date(year=1987, month=1, day=26),
            projected_completion_date=datetime.date(year=1992, month=1, day=25),
            completion_date=datetime.date(year=1989, month=8, day=11),
            county_code="US_MO_DEKALB",
            max_length_days=1826,
            person=person_910324,
            sentence_group=sg_910324_19890825,
        )
        sg_910324_19890825.supervision_sentences.append(sss_910324_19890825_1)

        # A charge already exists in the DB for this external_id, so we just
        # update the fields rather than create a whole new entity
        charge_910324.ncic_code = "5404"
        charge_910324.statute = "47410040"
        charge_910324.description = "DRIVING WHILE INTOXICATED - THIRD OFFENSE"
        charge_910324.is_violent = True
        charge_910324.classification_type = StateChargeClassificationType.FELONY
        charge_910324.classification_type_raw_text = "F"
        charge_910324.classification_subtype = "D"
        charge_910324.supervision_sentences.append(sss_910324_19890825_1)
        sss_910324_19890825_1.charges = [charge_910324]

        sss_910324_19890825_2 = entities.StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="910324-19890825-2",
            status=StateSentenceStatus.COMPLETED,
            status_raw_text="COMPLETED",
            supervision_type=StateSupervisionType.PROBATION,
            supervision_type_raw_text="PROBATION",
            start_date=datetime.date(year=1987, month=4, day=16),
            projected_completion_date=datetime.date(year=1988, month=2, day=6),
            completion_date=datetime.date(year=1988, month=2, day=22),
            county_code="US_MO_PLATTE",
            max_length_days=365,
            person=person_910324,
            sentence_group=sg_910324_19890825,
        )
        sg_910324_19890825.supervision_sentences.append(sss_910324_19890825_2)

        charge_910324_ss_2 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            external_id="910324-19890825-2",
            ncic_code="5404",
            description="DWI",
            is_violent=True,
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="F",
            supervision_sentences=[sss_910324_19890825_2],
            person=person_910324,
        )
        sss_910324_19890825_2.charges = [charge_910324_ss_2]

        sss_310261_19890821_1 = entities.StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="310261-19890821-1",
            status=StateSentenceStatus.COMPLETED,
            status_raw_text="COMPLETED",
            supervision_type=StateSupervisionType.PROBATION,
            supervision_type_raw_text="PROBATION",
            projected_completion_date=datetime.date(year=1980, month=10, day=6),
            completion_date=datetime.date(year=1979, month=6, day=7),
            county_code="OUT_OF_STATE",
            max_length_days=0,
            person=person_310261,
            sentence_group=sg_310261_19890821,
        )
        sg_310261_19890821.supervision_sentences.append(sss_310261_19890821_1)

        charge_310261_ss_1 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            external_id="310261-19890821-1",
            ncic_code="5707",
            description="BRK & END",
            is_violent=False,
            supervision_sentences=[sss_310261_19890821_1],
            person=person_310261,
        )
        sss_310261_19890821_1.charges = [charge_310261_ss_1]

        sg_110035_20040712 = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-20040712",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_110035,
        )
        person_110035.sentence_groups.append(sg_110035_20040712)

        sss_110035_20040712_1 = entities.StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-20040712-1",
            status=StateSentenceStatus.COMPLETED,
            status_raw_text="COMPLETED",
            supervision_type=StateSupervisionType.PROBATION,
            supervision_type_raw_text="PROBATION",
            start_date=datetime.date(year=2004, month=2, day=1),
            projected_completion_date=datetime.date(year=2008, month=1, day=20),
            completion_date=datetime.date(year=2008, month=1, day=19),
            county_code="US_MO_GREENE",
            max_length_days=1095,
            person=person_110035,
            sentence_group=sg_110035_20040712,
        )
        sg_110035_20040712.supervision_sentences.append(sss_110035_20040712_1)

        charge_110035_ss_1 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            external_id="110035-20040712-1",
            ncic_code="2399",
            description="BURG&STEAL",
            is_violent=False,
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="F",
            supervision_sentences=[sss_110035_20040712_1],
            person=person_110035,
        )
        sss_110035_20040712_1.charges = [charge_110035_ss_1]

        sss_110035_20040712_3 = entities.StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-20040712-3",
            status=StateSentenceStatus.COMPLETED,
            status_raw_text="COMPLETED",
            supervision_type=StateSupervisionType.INTERNAL_UNKNOWN,
            supervision_type_raw_text="UNKNOWN",
            start_date=datetime.date(year=2004, month=2, day=1),
            completion_date=datetime.date(year=2006, month=10, day=10),
            county_code="US_MO_GREENE",
            max_length_days=2436835,
            person=person_110035,
            sentence_group=sg_110035_20040712,
        )
        sg_110035_20040712.supervision_sentences.append(sss_110035_20040712_3)

        charge_110035_ss_3 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            external_id="110035-20040712-3",
            ncic_code="1399",
            statute="1310099",
            description="ASSAULT OF A LAW ENFORCEMENT OFFICER",
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="F",
            classification_subtype="A",
            supervision_sentences=[sss_110035_20040712_3],
            person=person_110035,
        )
        sss_110035_20040712_3.charges = [charge_110035_ss_3]

        sg_110035_20081010 = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-20081010",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_110035,
        )
        person_110035.sentence_groups.append(sg_110035_20081010)

        sss_110035_20081010_1 = entities.StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-20081010-1",
            status=StateSentenceStatus.COMPLETED,
            status_raw_text="COMPLETED",
            supervision_type=StateSupervisionType.PAROLE,
            supervision_type_raw_text="PAROLE",
            start_date=datetime.date(year=2008, month=11, day=1),
            projected_completion_date=datetime.date(year=2010, month=1, day=13),
            completion_date=datetime.date(year=2010, month=2, day=13),
            county_code="OUT_OF_STATE",
            max_length_days=0,
            person=person_110035,
            sentence_group=sg_110035_20081010,
        )
        sg_110035_20081010.supervision_sentences.append(sss_110035_20081010_1)

        charge_110035_20081010_1 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            external_id="110035-20081010-1",
            ncic_code="3532",
            county_code="OUT_OF_STATE",
            description="POSSESSION OF COCAINE (SHAWNEE CO 00CR1806)",
            is_violent=False,
            offense_date=datetime.date(year=2007, month=7, day=1),
            supervision_sentences=[sss_110035_20081010_1],
            person=person_110035,
        )
        sss_110035_20081010_1.charges = [charge_110035_20081010_1]

        # SQL Preprocessing View
        # Act
        self._run_ingest_job_for_filename(
            "tak022_tak024_tak025_tak026_offender_sentence_supervision.csv"
        )

        # Assert
        self.assert_expected_db_people(expected_people)

        ################################################################
        # TAK158_TAK023 INCARCERATION PERIOD FROM INCARCERATION SENTENCE
        ################################################################
        # Arrange
        ip_110035_19890901_1_0 = entities.StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-19890901-1-0",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            status_raw_text="NOT_IN_CUSTODY",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=datetime.date(year=1989, month=9, day=1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="10I1000",
            release_date=datetime.date(year=1992, month=10, day=6),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="IT-EM",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="S",
            person=person_110035,
            incarceration_sentences=[sis_110035_19890901_1],
        )

        ip_110035_19890901_3_0 = entities.StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-19890901-3-0",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            status_raw_text="NOT_IN_CUSTODY",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=datetime.date(year=1993, month=7, day=1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
            admission_reason_raw_text="40I1060",
            release_date=datetime.date(year=1993, month=11, day=2),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="IT-BP",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            specialized_purpose_for_incarceration_raw_text="I",
            person=person_110035,
            incarceration_sentences=[sis_110035_19890901_1],
        )

        ip_110035_19890901_5_0 = entities.StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-19890901-5-0",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            status_raw_text="NOT_IN_CUSTODY",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=datetime.date(year=1994, month=6, day=9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.DUAL_REVOCATION,
            admission_reason_raw_text="40I1020,40I2000",
            release_date=datetime.date(year=1995, month=2, day=6),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="IT-BD",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="S",
            person=person_110035,
            incarceration_sentences=[sis_110035_19890901_1],
        )
        vr_110035_19890901_5 = entities.StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="PERMANENT_DECISION",
            response_date=datetime.date(year=1994, month=6, day=9),
            decision=StateSupervisionViolationResponseDecision.REVOCATION,
            decision_raw_text="REVOCATION",
            revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
            revocation_type_raw_text="S",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text="PAROLE_BOARD",
            person=person_110035,
        )
        ip_110035_19890901_5_0.source_supervision_violation_response = (
            vr_110035_19890901_5
        )

        sis_110035_19890901_1.incarceration_periods = [
            ip_110035_19890901_1_0,
            ip_110035_19890901_3_0,
            ip_110035_19890901_5_0,
        ]

        ip_110035_20010414_2_0 = entities.StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-20010414-2-0",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            status_raw_text="NOT_IN_CUSTODY",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=datetime.date(year=2001, month=4, day=20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.DUAL_REVOCATION,
            admission_reason_raw_text="40I2000,50N1010",
            release_date=datetime.date(year=2012, month=11, day=2),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="IT-BP",
            specialized_purpose_for_incarceration_raw_text="X",
            person=person_110035,
            incarceration_sentences=[sis_110035_20010414_1],
        )
        vr_110035_20010414_2 = entities.StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="PERMANENT_DECISION",
            response_date=datetime.date(year=2001, month=4, day=20),
            decision=StateSupervisionViolationResponseDecision.REVOCATION,
            decision_raw_text="REVOCATION",
            revocation_type=None,
            revocation_type_raw_text="X",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text="PAROLE_BOARD",
            person=person_110035,
        )
        ip_110035_20010414_2_0.source_supervision_violation_response = (
            vr_110035_20010414_2
        )

        ip_110035_20010414_4_0 = entities.StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-20010414-4-0",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            status_raw_text="NOT_IN_CUSTODY",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=datetime.date(year=2013, month=5, day=21),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text="45O0050,40I0050",
            release_date=datetime.date(year=2013, month=11, day=27),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="IT-BP",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="S",
            person=person_110035,
            incarceration_sentences=[sis_110035_20010414_1],
        )
        ip_110035_20010414_7_0 = entities.StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-20010414-7-0",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            status_raw_text="NOT_IN_CUSTODY",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=datetime.date(year=2016, month=3, day=28),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            admission_reason_raw_text="45O0050,65N9999,40I1010",
            release_date=datetime.date(year=2016, month=4, day=28),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            release_reason_raw_text="50N1010",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="S",
            person=person_110035,
            incarceration_sentences=[sis_110035_20010414_1],
        )

        ip_110035_20010414_7_3 = entities.StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-20010414-7-3",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            status_raw_text="NOT_IN_CUSTODY",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=datetime.date(year=2016, month=4, day=28),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            admission_reason_raw_text="50N1010",
            release_date=datetime.date(year=2016, month=10, day=11),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            release_reason_raw_text="ID-DR",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="S",
            person=person_110035,
            incarceration_sentences=[sis_110035_20010414_1],
        )

        vr_110035_20010414_7_0 = entities.StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="PERMANENT_DECISION",
            response_date=datetime.date(year=2016, month=3, day=28),
            decision=StateSupervisionViolationResponseDecision.REVOCATION,
            decision_raw_text="REVOCATION",
            revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
            revocation_type_raw_text="S",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text="PAROLE_BOARD",
            person=person_110035,
        )
        ip_110035_20010414_7_0.source_supervision_violation_response = (
            vr_110035_20010414_7_0
        )

        vr_110035_20010414_7_3 = entities.StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="PERMANENT_DECISION",
            response_date=datetime.date(year=2016, month=4, day=28),
            decision=StateSupervisionViolationResponseDecision.REVOCATION,
            decision_raw_text="REVOCATION",
            revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
            revocation_type_raw_text="S",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text="PAROLE_BOARD",
            person=person_110035,
        )
        ip_110035_20010414_7_3.source_supervision_violation_response = (
            vr_110035_20010414_7_3
        )

        sis_110035_20010414_1.incarceration_periods = [
            ip_110035_20010414_2_0,
            ip_110035_20010414_4_0,
            ip_110035_20010414_7_0,
            ip_110035_20010414_7_3,
        ]

        ip_310261_19890821_1_0 = entities.StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="310261-19890821-1-0",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            status_raw_text="IN_CUSTODY",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=datetime.date(year=1990, month=3, day=29),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="10I1000",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="S",
            person=person_310261,
            incarceration_sentences=[sis_310261_19890821_3],
        )
        sis_310261_19890821_3.incarceration_periods = [ip_310261_19890821_1_0]

        ip_710448_20010414_1_0 = entities.StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="710448-20010414-1-0",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            status_raw_text="NOT_IN_CUSTODY",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=datetime.date(year=2001, month=7, day=5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="10I1000",
            release_date=datetime.date(year=2002, month=1, day=17),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="IT-EM",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="S",
            person=person_710448,
            incarceration_sentences=[sis_710448_20010414_1],
        )
        sis_710448_20010414_1.incarceration_periods = [ip_710448_20010414_1_0]

        ip_710448_20010414_3_0 = entities.StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="710448-20010414-3-0",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            status_raw_text="NOT_IN_CUSTODY",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=datetime.date(year=2002, month=9, day=12),
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
            admission_reason_raw_text="40I1060",
            release_date=datetime.date(year=2004, month=9, day=28),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="IT-BP",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="S",
            person=person_710448,
            incarceration_sentences=[sis_710448_20010414_3],
        )

        sis_710448_20010414_3.incarceration_periods = [ip_710448_20010414_3_0]

        ip_910324_19890825_1_0 = entities.StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="910324-19890825-1-0",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            status_raw_text="NOT_IN_CUSTODY",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=datetime.date(year=1989, month=10, day=23),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="10I1000",
            release_date=datetime.date(year=1989, month=5, day=16),
            release_reason=StateIncarcerationPeriodReleaseReason.ESCAPE,
            release_reason_raw_text="IE-IE",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            specialized_purpose_for_incarceration_raw_text="O",
            person=person_910324,
            incarceration_sentences=[sis_910324_19890825_1],
        )
        sis_910324_19890825_1.incarceration_periods = [ip_910324_19890825_1_0]

        # New person and entity tree introduced at this point
        person_523523 = entities.StatePerson.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
        )
        spei_523523 = entities.StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="523523",
            id_type=US_MO_DOC,
            person=person_523523,
        )
        person_523523.external_ids.append(spei_523523)

        sg_523523_19890617 = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="523523-19890617",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_523523,
        )
        person_523523.sentence_groups.append(sg_523523_19890617)

        sis_523523_19890617_1 = entities.StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="523523-19890617-1",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            person=person_523523,
            sentence_group=sg_523523_19890617,
        )
        sg_523523_19890617.incarceration_sentences.append(sis_523523_19890617_1)

        ip_523523_19890617_1_0 = entities.StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="523523-19890617-1-0",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            status_raw_text="IN_CUSTODY",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=datetime.date(year=1989, month=6, day=17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="10I1000",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="S",
            person=person_523523,
            incarceration_sentences=[sis_523523_19890617_1],
        )
        sis_523523_19890617_1.incarceration_periods.append(ip_523523_19890617_1_0)

        expected_people.append(person_523523)

        # New person and entity tree introduced at this point
        person_867530 = entities.StatePerson.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
        )
        spei_867530 = entities.StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="867530",
            id_type=US_MO_DOC,
            person=person_867530,
        )
        person_867530.external_ids.append(spei_867530)

        sg_867530_19970224 = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="867530-19970224",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_867530,
        )
        person_867530.sentence_groups.append(sg_867530_19970224)

        sis_867530_19970224_1 = entities.StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="867530-19970224-1",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            person=person_867530,
            sentence_group=sg_867530_19970224,
        )
        sg_867530_19970224.incarceration_sentences.append(sis_867530_19970224_1)

        ip_867530_19970224_1_0 = entities.StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="867530-19970224-1-0",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            status_raw_text="NOT_IN_CUSTODY",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=datetime.date(year=1997, month=2, day=24),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="10I1000",
            release_date=datetime.date(year=2016, month=10, day=31),
            release_reason=StateIncarcerationPeriodReleaseReason.EXECUTION,
            release_reason_raw_text="DE-EX",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="S",
            person=person_867530,
            incarceration_sentences=[sis_867530_19970224_1],
        )
        sis_867530_19970224_1.incarceration_periods.append(ip_867530_19970224_1_0)

        expected_people.append(person_867530)

        ip_523523_19890617_1_0.release_date = datetime.date(year=2010, month=10, day=20)
        ip_523523_19890617_1_0.status = StateIncarcerationPeriodStatus.NOT_IN_CUSTODY
        ip_523523_19890617_1_0.status_raw_text = "NOT_IN_CUSTODY"

        # SQL Preprocessing View
        # Act
        self._run_ingest_job_for_filename(
            "tak158_tak023_tak026_incarceration_period_from_incarceration_sentence.csv"
        )

        # Assert
        self.assert_expected_db_people(expected_people)

        ################################################################
        # TAK158_TAK024 INCARCERATION PERIOD FROM SUPERVISION SENTENCE
        ################################################################
        # Arrange
        ip_910324_19890825_1_0.supervision_sentences = [sss_910324_19890825_1]
        sss_910324_19890825_1.incarceration_periods = [ip_910324_19890825_1_0]

        sss_523523_19890617_1 = entities.StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="523523-19890617-1",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_523523,
            sentence_group=sg_523523_19890617,
        )

        sg_523523_19890617.supervision_sentences.append(sss_523523_19890617_1)
        ip_523523_19890617_1_0.supervision_sentences = [sss_523523_19890617_1]
        sss_523523_19890617_1.incarceration_periods = [ip_523523_19890617_1_0]

        # SQL Preprocessing View
        # Act
        self._run_ingest_job_for_filename(
            "tak158_tak024_tak026_incarceration_period_from_supervision_sentence.csv"
        )

        # Assert
        self.assert_expected_db_people(expected_people)

        ################################################################
        # TAK034_TAK026_TAK039_APFX90_APFX91 SUPERVISION PERIODS
        ################################################################

        # Arrange
        agent_123_name = (
            '{"given_names": "FIRST", "middle_names": "MIDDLE", "surname": "LAST"}'
        )
        agent_234_name = '{"given_names": "F", "surname": "L"}'
        agent_123 = entities.StateAgent.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="E123",
            agent_type=StateAgentType.SUPERVISION_OFFICER,
            agent_type_raw_text="PROBATION/PAROLE UNIT SPV",
            full_name=agent_123_name,
        )
        agent_234 = entities.StateAgent.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="E234",
            agent_type=StateAgentType.INTERNAL_UNKNOWN,
            agent_type_raw_text="CORRECTIONS CASE MANAGER",
            full_name=agent_234_name,
        )
        agent_567 = entities.StateAgent.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="E567",
            agent_type=StateAgentType.PRESENT_WITHOUT_INFO,
        )

        person_110035.supervising_officer = agent_123
        person_710448.supervising_officer = agent_567
        person_910324.supervising_officer = agent_234

        placeholder_sss_110035_19890901 = (
            entities.StateSupervisionSentence.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                sentence_group=sg_110035_19890901,
                person=person_110035,
            )
        )

        sg_110035_19890901.supervision_sentences.append(placeholder_sss_110035_19890901)

        sp_110035_19890901_2_0 = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-19890901-2-0",
            status=StateSupervisionPeriodStatus.TERMINATED,
            start_date=datetime.date(year=1992, month=10, day=6),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            admission_reason_raw_text="40O1010,40O0000",
            termination_date=datetime.date(year=1993, month=7, day=1),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            termination_reason_raw_text="40I1050,45O1010",
            supervision_site="14",
            supervision_level=StateSupervisionLevel.MAXIMUM,
            supervision_level_raw_text="ISP",
            person=person_110035,
            supervision_sentences=[placeholder_sss_110035_19890901],
            supervising_officer=agent_123,
        )
        sp_110035_19890901_4_0 = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-19890901-4-0",
            status=StateSupervisionPeriodStatus.TERMINATED,
            start_date=datetime.date(year=1993, month=11, day=2),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            admission_reason_raw_text="40O1030,40O0000",
            termination_date=datetime.date(year=1994, month=6, day=9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            termination_reason_raw_text="40I1050,45O1010",
            supervision_site="14",
            person=person_110035,
            supervision_sentences=[placeholder_sss_110035_19890901],
            supervising_officer=agent_123,
        )
        sp_110035_19890901_6_0 = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-19890901-6-0",
            status=StateSupervisionPeriodStatus.TERMINATED,
            start_date=datetime.date(year=1995, month=2, day=6),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            admission_reason_raw_text="40O1030,40O0000",
            termination_date=datetime.date(year=1995, month=3, day=23),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            termination_reason_raw_text="99O2010",
            supervision_site="14",
            person=person_110035,
            supervision_sentences=[placeholder_sss_110035_19890901],
            supervising_officer=agent_123,
        )

        placeholder_sss_110035_19890901.supervision_periods = [
            sp_110035_19890901_2_0,
            sp_110035_19890901_4_0,
            sp_110035_19890901_6_0,
        ]

        placeholder_sss_110035_20010414 = (
            entities.StateSupervisionSentence.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                sentence_group=sg_110035_20010414,
                person=person_110035,
            )
        )
        sg_110035_20010414.supervision_sentences.append(placeholder_sss_110035_20010414)

        sp_110035_20010414_1_0 = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-20010414-1-0",
            status=StateSupervisionPeriodStatus.TERMINATED,
            start_date=datetime.date(year=2001, month=4, day=14),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            admission_reason_raw_text="15I1000",
            termination_date=datetime.date(year=2001, month=4, day=20),
            termination_reason=StateSupervisionPeriodTerminationReason.SUSPENSION,
            termination_reason_raw_text="65O2015",
            supervision_site="14",
            person=person_110035,
            supervision_sentences=[placeholder_sss_110035_20010414],
            supervising_officer=agent_123,
        )
        sp_110035_20010414_3_0 = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-20010414-3-0",
            status=StateSupervisionPeriodStatus.TERMINATED,
            start_date=datetime.date(year=2012, month=11, day=2),
            admission_reason=StateSupervisionPeriodAdmissionReason.RETURN_FROM_SUSPENSION,
            admission_reason_raw_text="65I2015",
            termination_date=datetime.date(year=2013, month=5, day=21),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            termination_reason_raw_text="45O0050,65N9999,40I0050",
            supervision_site="14",
            person=person_110035,
            supervision_sentences=[placeholder_sss_110035_20010414],
            supervising_officer=agent_123,
        )
        sp_110035_20010414_5_0 = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-20010414-5-0",
            status=StateSupervisionPeriodStatus.TERMINATED,
            start_date=datetime.date(year=2013, month=11, day=27),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            admission_reason_raw_text="40O1030",
            termination_date=datetime.date(year=2015, month=7, day=6),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            termination_reason_raw_text="65L9100",
            supervision_site="14",
            person=person_110035,
            supervision_sentences=[placeholder_sss_110035_20010414],
            supervising_officer=agent_123,
        )
        sp_110035_20010414_6_0 = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-20010414-6-0",
            status=StateSupervisionPeriodStatus.TERMINATED,
            start_date=datetime.date(year=2016, month=3, day=28),
            admission_reason=StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
            admission_reason_raw_text="65N9500",
            termination_date=datetime.date(year=2016, month=3, day=28),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            termination_reason_raw_text="45O1010,40I1010",
            supervision_site="14",
            person=person_110035,
            supervision_sentences=[placeholder_sss_110035_20010414],
            supervising_officer=agent_123,
        )

        placeholder_sss_110035_20010414.supervision_periods = [
            sp_110035_20010414_1_0,
            sp_110035_20010414_3_0,
            sp_110035_20010414_5_0,
            sp_110035_20010414_6_0,
        ]

        sp_710448_20010414_2_0 = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="710448-20010414-2-0",
            status=StateSupervisionPeriodStatus.TERMINATED,
            start_date=datetime.date(year=2002, month=1, day=17),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            admission_reason_raw_text="40O4199",
            termination_date=datetime.date(year=2002, month=4, day=1),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            termination_reason_raw_text="TRANSFER_WITHIN_STATE",
            supervision_site="ERA",
            supervising_officer=agent_567,
            person=person_710448,
            incarceration_sentences=[sis_710448_20010414_1],
        )
        sis_710448_20010414_1.supervision_periods = [sp_710448_20010414_2_0]

        sp_710448_20010414_3_0 = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="710448-20010414-3-0",
            status=StateSupervisionPeriodStatus.TERMINATED,
            start_date=datetime.date(year=2002, month=4, day=1),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            admission_reason_raw_text="TRANSFER_WITHIN_STATE",
            termination_date=datetime.date(year=2002, month=9, day=12),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            termination_reason_raw_text="45O1010",
            supervision_site="14",
            supervising_officer=agent_123,
            person=person_710448,
            incarceration_sentences=[sis_710448_20010414_1],
        )
        sis_710448_20010414_1.supervision_periods.append(sp_710448_20010414_3_0)

        sp_710448_20010414_4_0 = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="710448-20010414-4-0",
            status=StateSupervisionPeriodStatus.TERMINATED,
            start_date=datetime.date(year=2004, month=9, day=28),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            admission_reason_raw_text="40O1010",
            termination_date=datetime.date(year=2006, month=9, day=11),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            termination_reason_raw_text="99O2010",
            supervision_site="ERA",
            supervising_officer=agent_567,
            person=person_710448,
            incarceration_sentences=[
                sis_710448_20010414_1,
                sis_710448_20010414_2,
                sis_710448_20010414_3,
            ],
        )
        sis_710448_20010414_1.supervision_periods.append(sp_710448_20010414_4_0)
        sis_710448_20010414_2.supervision_periods.append(sp_710448_20010414_4_0)
        sis_710448_20010414_3.supervision_periods.append(sp_710448_20010414_4_0)

        placeholder_sss_710448_20010414 = (
            entities.StateSupervisionSentence.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                sentence_group=sg_710448_20010414,
                person=person_710448,
            )
        )

        sg_710448_20010414.supervision_sentences.append(placeholder_sss_710448_20010414)

        sp_910324_19890825_2_0 = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="910324-19890825-2-0",
            status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
            start_date=datetime.date(year=1989, month=5, day=16),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            admission_reason_raw_text="40O1010",
            supervision_site="17",
            supervising_officer=agent_234,
            person=person_910324,
            supervision_level=StateSupervisionLevel.INTERNAL_UNKNOWN,
            supervision_level_raw_text="IAP",
            incarceration_sentences=[sis_910324_19890825_1],
            supervision_sentences=[sss_910324_19890825_1],
        )
        ct_910324_19890825_2_0_dso = entities.StateSupervisionCaseTypeEntry(
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            case_type_raw_text="DSO",
            state_code=_STATE_CODE_UPPER,
            supervision_period=sp_910324_19890825_2_0,
            person=person_910324,
        )
        sp_910324_19890825_2_0.case_type_entries.append(ct_910324_19890825_2_0_dso)
        sis_910324_19890825_1.supervision_periods = [sp_910324_19890825_2_0]
        sss_910324_19890825_1.supervision_periods = [sp_910324_19890825_2_0]

        person_624624 = entities.StatePerson.new_with_defaults(
            state_code=_STATE_CODE_UPPER
        )
        spei_624624 = entities.StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="624624",
            id_type=US_MO_DOC,
            person=person_624624,
        )
        person_624624.external_ids.append(spei_624624)

        sg_624624_19890617 = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="624624-19890617",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_624624,
        )
        person_624624.sentence_groups.append(sg_624624_19890617)
        person_624624.supervising_officer = agent_234

        placeholder_sss_624624_19890617_1 = (
            entities.StateSupervisionSentence.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                person=person_624624,
                sentence_group=sg_624624_19890617,
            )
        )
        sg_624624_19890617.supervision_sentences.append(
            placeholder_sss_624624_19890617_1
        )

        sp_624624_19890617_1_0 = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="624624-19890617-1-0",
            status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
            start_date=datetime.date(year=1989, month=6, day=17),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            admission_reason_raw_text="15I1000",
            supervision_site="17",
            supervising_officer=agent_234,
            person=person_624624,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="III",
            supervision_sentences=[placeholder_sss_624624_19890617_1],
        )
        ct_624624_19890617_1_0_dom = entities.StateSupervisionCaseTypeEntry(
            case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
            case_type_raw_text="DOM",
            state_code=_STATE_CODE_UPPER,
            supervision_period=sp_624624_19890617_1_0,
            person=person_624624,
        )
        ct_624624_19890617_1_0_dvs = entities.StateSupervisionCaseTypeEntry(
            case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
            case_type_raw_text="DVS",
            state_code=_STATE_CODE_UPPER,
            supervision_period=sp_624624_19890617_1_0,
            person=person_624624,
        )
        ct_624624_19890617_1_0_smi = entities.StateSupervisionCaseTypeEntry(
            case_type=StateSupervisionCaseType.SERIOUS_MENTAL_ILLNESS,
            case_type_raw_text="SMI",
            state_code=_STATE_CODE_UPPER,
            supervision_period=sp_624624_19890617_1_0,
            person=person_624624,
        )
        sp_624624_19890617_1_0.case_type_entries.extend(
            [
                ct_624624_19890617_1_0_dom,
                ct_624624_19890617_1_0_dvs,
                ct_624624_19890617_1_0_smi,
            ]
        )
        placeholder_sss_624624_19890617_1.supervision_periods.append(
            sp_624624_19890617_1_0
        )

        expected_people.append(person_624624)

        placeholder_sss_110035_20040712 = (
            entities.StateSupervisionSentence.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                sentence_group=sg_110035_20040712,
                person=person_110035,
            )
        )

        sg_110035_20040712.supervision_sentences.append(placeholder_sss_110035_20040712)

        sp_110035_20040712_1_0 = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-20040712-1-0",
            status=StateSupervisionPeriodStatus.TERMINATED,
            start_date=datetime.date(year=2004, month=7, day=12),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            admission_reason_raw_text="40O1010",
            termination_date=datetime.date(year=2005, month=8, day=8),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            termination_reason_raw_text="65L9100",
            supervision_site="14",
            supervising_officer=agent_123,
            supervision_level=StateSupervisionLevel.INCARCERATED,
            supervision_level_raw_text="ITC",
            person=person_110035,
            supervision_sentences=[sss_110035_20040712_1, sss_110035_20040712_3],
        )
        ct_110035_20040712_1_0_dso = entities.StateSupervisionCaseTypeEntry(
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            case_type_raw_text="DSO",
            state_code=_STATE_CODE_UPPER,
            supervision_period=sp_110035_20040712_1_0,
            person=person_110035,
        )
        ct_110035_20040712_1_0_dvs = entities.StateSupervisionCaseTypeEntry(
            case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
            case_type_raw_text="DVS",
            state_code=_STATE_CODE_UPPER,
            supervision_period=sp_110035_20040712_1_0,
            person=person_110035,
        )
        sp_110035_20040712_1_0.case_type_entries.extend(
            [ct_110035_20040712_1_0_dso, ct_110035_20040712_1_0_dvs]
        )
        sp_110035_20040712_1_8 = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-20040712-1-8",
            status=StateSupervisionPeriodStatus.TERMINATED,
            start_date=datetime.date(year=2005, month=8, day=8),
            admission_reason=StateSupervisionPeriodAdmissionReason.ABSCONSION,
            admission_reason_raw_text="65L9100",
            termination_date=datetime.date(year=2005, month=9, day=9),
            termination_reason=StateSupervisionPeriodTerminationReason.RETURN_FROM_ABSCONSION,
            termination_reason_raw_text="65N9500",
            supervision_site="14",
            supervising_officer=agent_123,
            supervision_level=StateSupervisionLevel.INCARCERATED,
            supervision_level_raw_text="ITC",
            person=person_110035,
            supervision_sentences=[sss_110035_20040712_1, sss_110035_20040712_3],
        )
        ct_110035_20040712_1_8_dso = entities.StateSupervisionCaseTypeEntry(
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            case_type_raw_text="DSO",
            state_code=_STATE_CODE_UPPER,
            supervision_period=sp_110035_20040712_1_8,
            person=person_110035,
        )
        ct_110035_20040712_1_8_dvs = entities.StateSupervisionCaseTypeEntry(
            case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
            case_type_raw_text="DVS",
            state_code=_STATE_CODE_UPPER,
            supervision_period=sp_110035_20040712_1_8,
            person=person_110035,
        )
        sp_110035_20040712_1_8.case_type_entries.extend(
            [ct_110035_20040712_1_8_dso, ct_110035_20040712_1_8_dvs]
        )
        sp_110035_20040712_1_9 = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-20040712-1-9",
            status=StateSupervisionPeriodStatus.TERMINATED,
            start_date=datetime.date(year=2005, month=9, day=9),
            admission_reason=StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
            admission_reason_raw_text="65N9500",
            termination_date=datetime.date(year=2008, month=1, day=19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            termination_reason_raw_text="99O2010",
            supervision_site="14",
            supervising_officer=agent_123,
            supervision_level=StateSupervisionLevel.INCARCERATED,
            supervision_level_raw_text="ITC",
            person=person_110035,
            supervision_sentences=[sss_110035_20040712_1, sss_110035_20040712_3],
        )
        ct_110035_20040712_1_9_dso = entities.StateSupervisionCaseTypeEntry(
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            case_type_raw_text="DSO",
            state_code=_STATE_CODE_UPPER,
            supervision_period=sp_110035_20040712_1_9,
            person=person_110035,
        )
        ct_110035_20040712_1_9_dvs = entities.StateSupervisionCaseTypeEntry(
            case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
            case_type_raw_text="DVS",
            state_code=_STATE_CODE_UPPER,
            supervision_period=sp_110035_20040712_1_9,
            person=person_110035,
        )
        sp_110035_20040712_1_9.case_type_entries.extend(
            [ct_110035_20040712_1_9_dso, ct_110035_20040712_1_9_dvs]
        )

        sss_110035_20040712_1.supervision_periods.extend(
            [sp_110035_20040712_1_0, sp_110035_20040712_1_8, sp_110035_20040712_1_9]
        )
        sss_110035_20040712_3.supervision_periods.extend(
            [sp_110035_20040712_1_0, sp_110035_20040712_1_8, sp_110035_20040712_1_9]
        )

        placeholder_sss_910324_19890825 = (
            entities.StateSupervisionSentence.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                sentence_group=sg_910324_19890825,
                person=person_910324,
            )
        )

        sg_910324_19890825.supervision_sentences.append(placeholder_sss_910324_19890825)

        # SQL Preprocessing View
        # Act
        self._run_ingest_job_for_filename(
            "tak034_tak026_tak039_apfx90_apfx91_supervision_enhancements_supervision_periods.csv"
        )

        # Assert
        self.assert_expected_db_people(expected_people)

        ##############################################################
        # TAK028_TAK076_TAK042_TAK024 VIOLATION REPORTS
        ##############################################################
        # Arrange
        placeholder_ssp_110035_20040712_1 = (
            entities.StateSupervisionPeriod.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
                supervision_sentences=[sss_110035_20040712_1],
                person=person_110035,
            )
        )
        ssv_110035_20040712_r1_1 = entities.StateSupervisionViolation.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-20040712-R1",
            supervision_periods=[sp_110035_20040712_1_0],
            person=person_110035,
        )
        ssvt_110035_20040712_r1_1_t = (
            entities.StateSupervisionViolationTypeEntry.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_raw_text="T",
                supervision_violation=ssv_110035_20040712_r1_1,
                person=person_110035,
            )
        )
        ssvc_110035_20040712_r1_1_dir = entities.StateSupervisionViolatedConditionEntry(
            state_code=_STATE_CODE_UPPER,
            condition="DIR",
            supervision_violation=ssv_110035_20040712_r1_1,
            person=person_110035,
        )
        ssvc_110035_20040712_r1_1_emp = entities.StateSupervisionViolatedConditionEntry(
            state_code=_STATE_CODE_UPPER,
            condition="EMP",
            supervision_violation=ssv_110035_20040712_r1_1,
            person=person_110035,
        )
        ssvc_110035_20040712_r1_1_res = entities.StateSupervisionViolatedConditionEntry(
            state_code=_STATE_CODE_UPPER,
            condition="RES",
            supervision_violation=ssv_110035_20040712_r1_1,
            person=person_110035,
        )
        ssvr_110035_20040712_r1_1 = entities.StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-20040712-R1",
            response_date=datetime.date(year=2005, month=1, day=11),
            is_draft=False,
            supervision_violation=ssv_110035_20040712_r1_1,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_type_raw_text="VIOLATION_REPORT",
            response_subtype="ITR",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
            deciding_body_type_raw_text="SUPERVISION_OFFICER",
            decision_agents=[agent_123],
            person=person_110035,
        )
        ssvrd_110035_20040712_r1_1_a = (
            entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                decision=StateSupervisionViolationResponseDecision.WARRANT_ISSUED,
                decision_raw_text="A",
                supervision_violation_response=ssvr_110035_20040712_r1_1,
                person=person_110035,
            )
        )
        ssvrd_110035_20040712_r1_1_c = (
            entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                decision_raw_text="C",
                supervision_violation_response=ssvr_110035_20040712_r1_1,
                person=person_110035,
            )
        )
        ssvrd_110035_20040712_r1_1_r = entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            decision=StateSupervisionViolationResponseDecision.REVOCATION,
            decision_raw_text="R",
            revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
            revocation_type_raw_text="REINCARCERATION",
            supervision_violation_response=ssvr_110035_20040712_r1_1,
            person=person_110035,
        )
        ssv_110035_20040712_r2 = entities.StateSupervisionViolation.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-20040712-R2",
            violation_date=datetime.date(year=2006, month=1, day=1),
            supervision_periods=[sp_110035_20040712_1_9],
            person=person_110035,
        )
        ssvt_110035_20040712_r2_1_a = (
            entities.StateSupervisionViolationTypeEntry.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                violation_type=StateSupervisionViolationType.ABSCONDED,
                violation_type_raw_text="A",
                supervision_violation=ssv_110035_20040712_r2,
                person=person_110035,
            )
        )
        ssvt_110035_20040712_r2_1_e = (
            entities.StateSupervisionViolationTypeEntry.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                violation_type=StateSupervisionViolationType.ESCAPED,
                violation_type_raw_text="E",
                supervision_violation=ssv_110035_20040712_r2,
                person=person_110035,
            )
        )
        ssvc_110035_20040712_r2_1_res = (
            entities.StateSupervisionViolatedConditionEntry.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                condition="SPC",
                supervision_violation=ssv_110035_20040712_r2,
                person=person_110035,
            )
        )
        ssvr_110035_20040712_r2_1 = entities.StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-20040712-R2",
            response_date=datetime.date(year=2006, month=1, day=11),
            is_draft=False,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_type_raw_text="VIOLATION_REPORT",
            response_subtype="ITR",
            supervision_violation=ssv_110035_20040712_r2,
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
            deciding_body_type_raw_text="SUPERVISION_OFFICER",
            decision_agents=[agent_123],
            person=person_110035,
        )
        sss_110035_20040712_1.supervision_periods.append(
            placeholder_ssp_110035_20040712_1
        )
        ssvr_110035_20040712_r1_1.supervision_violation_response_decisions.extend(
            [
                ssvrd_110035_20040712_r1_1_a,
                ssvrd_110035_20040712_r1_1_c,
                ssvrd_110035_20040712_r1_1_r,
            ]
        )
        ssv_110035_20040712_r1_1.supervision_violation_responses.append(
            ssvr_110035_20040712_r1_1
        )
        ssv_110035_20040712_r1_1.supervision_violated_conditions.extend(
            [
                ssvc_110035_20040712_r1_1_dir,
                ssvc_110035_20040712_r1_1_emp,
                ssvc_110035_20040712_r1_1_res,
            ]
        )
        ssv_110035_20040712_r1_1.supervision_violation_types.append(
            ssvt_110035_20040712_r1_1_t
        )

        ssv_110035_20040712_r2.supervision_violation_responses.append(
            ssvr_110035_20040712_r2_1
        )
        ssv_110035_20040712_r2.supervision_violated_conditions.append(
            ssvc_110035_20040712_r2_1_res
        )
        ssv_110035_20040712_r2.supervision_violation_types.extend(
            [ssvt_110035_20040712_r2_1_e, ssvt_110035_20040712_r2_1_a]
        )

        # Violations matched by date
        sp_110035_20040712_1_0.supervision_violation_entries.append(
            ssv_110035_20040712_r1_1
        )
        sp_110035_20040712_1_9.supervision_violation_entries.append(
            ssv_110035_20040712_r2
        )

        sss_110035_20040712_2 = entities.StateSupervisionSentence.new_with_defaults(
            external_id="110035-20040712-2",
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group=sg_110035_20040712,
            person=person_110035,
        )
        placeholder_ssp_110035_20040712_2 = (
            entities.StateSupervisionPeriod.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
                supervision_sentences=[sss_110035_20040712_2],
                person=person_110035,
            )
        )
        ssv_110035_20040712_r2.supervision_periods.append(
            placeholder_ssp_110035_20040712_2
        )
        placeholder_ssp_110035_20040712_2.supervision_violation_entries.append(
            ssv_110035_20040712_r2
        )
        sss_110035_20040712_2.supervision_periods.append(
            placeholder_ssp_110035_20040712_2
        )
        sg_110035_20040712.supervision_sentences.append(sss_110035_20040712_2)

        placeholder_ssp_910324_19890825_from_is = (
            entities.StateSupervisionPeriod.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
                incarceration_sentences=[sis_910324_19890825_1],
                person=person_910324,
            )
        )
        ssv_910324_19890825_r1 = entities.StateSupervisionViolation.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="910324-19890825-R1",
            violation_date=datetime.date(year=1989, month=6, day=17),
            supervision_periods=[sp_910324_19890825_2_0],
            person=person_910324,
        )
        ssvc_910324_19890825_r1_1_emp = (
            entities.StateSupervisionViolatedConditionEntry.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                condition="EMP",
                supervision_violation=ssv_910324_19890825_r1,
                person=person_910324,
            )
        )
        ssvt_910324_19890825_r1_1_m = (
            entities.StateSupervisionViolationTypeEntry.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                violation_type=StateSupervisionViolationType.MISDEMEANOR,
                violation_type_raw_text="M",
                supervision_violation=ssv_910324_19890825_r1,
                person=person_910324,
            )
        )
        ssvr_910324_19890825_r1_1 = entities.StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="910324-19890825-R1",
            response_date=datetime.date(year=1989, month=6, day=16),
            is_draft=True,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_type_raw_text="VIOLATION_REPORT",
            response_subtype="INI",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
            deciding_body_type_raw_text="SUPERVISION_OFFICER",
            decision_agents=[agent_234],
            supervision_violation=ssv_910324_19890825_r1,
            person=person_910324,
        )
        ssvrd_910324_19890825_r1_1_co = entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            decision_raw_text="CO",
            revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
            revocation_type_raw_text="SHOCK_INCARCERATION",
            supervision_violation_response=ssvr_910324_19890825_r1_1,
            person=person_910324,
        )
        ssv_910324_19890825_r1.supervision_violation_responses.append(
            ssvr_910324_19890825_r1_1
        )
        ssv_910324_19890825_r1.supervision_violated_conditions.append(
            ssvc_910324_19890825_r1_1_emp
        )
        ssv_910324_19890825_r1.supervision_violation_types.append(
            ssvt_910324_19890825_r1_1_m
        )
        ssvr_910324_19890825_r1_1.supervision_violation_response_decisions.append(
            ssvrd_910324_19890825_r1_1_co
        )
        sis_910324_19890825_1.supervision_periods.append(
            placeholder_ssp_910324_19890825_from_is
        )

        # Violation matched by date
        sp_910324_19890825_2_0.supervision_violation_entries.append(
            ssv_910324_19890825_r1
        )

        placeholder_ssp_910324_19890825_from_ss = (
            entities.StateSupervisionPeriod.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
                supervision_sentences=[sss_910324_19890825_1],
                person=person_910324,
            )
        )

        ssv_910324_19890825_r2 = entities.StateSupervisionViolation.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="910324-19890825-R2",
            violation_date=datetime.date(year=1989, month=8, day=4),
            supervision_periods=[sp_910324_19890825_2_0],
            person=person_910324,
        )
        ssvc_910324_19890825_r2_1_spc = (
            entities.StateSupervisionViolatedConditionEntry.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                condition="SPC",
                supervision_violation=ssv_910324_19890825_r2,
                person=person_910324,
            )
        )
        ssvt_910324_19890825_r2_1_t = (
            entities.StateSupervisionViolationTypeEntry.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_raw_text="T",
                supervision_violation=ssv_910324_19890825_r2,
                person=person_910324,
            )
        )
        agent_345_name = '{"given_names": "F", "surname": "L"}'
        agent_345 = entities.StateAgent.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="E345",
            agent_type=StateAgentType.INTERNAL_UNKNOWN,
            agent_type_raw_text="LIBRARIAN I",
            full_name=agent_345_name,
        )
        ssvr_910324_19890825_r2_1 = entities.StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="910324-19890825-R2",
            response_date=datetime.date(year=1989, month=9, day=3),
            is_draft=False,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_type_raw_text="VIOLATION_REPORT",
            response_subtype="SUP",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
            deciding_body_type_raw_text="SUPERVISION_OFFICER",
            decision_agents=[agent_345],
            supervision_violation=ssv_910324_19890825_r2,
            person=person_910324,
        )
        ssvrd_910324_19890825_r2_1_rn = (
            entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
                decision_raw_text="RN",
                supervision_violation_response=ssvr_910324_19890825_r2_1,
                person=person_910324,
            )
        )

        ssv_910324_19890825_r2.supervision_violation_responses.append(
            ssvr_910324_19890825_r2_1
        )
        ssv_910324_19890825_r2.supervision_violated_conditions.append(
            ssvc_910324_19890825_r2_1_spc
        )
        ssv_910324_19890825_r2.supervision_violation_types.append(
            ssvt_910324_19890825_r2_1_t
        )
        ssvr_910324_19890825_r2_1.supervision_violation_response_decisions.append(
            ssvrd_910324_19890825_r2_1_rn
        )

        sp_910324_19890825_2_0.supervision_violation_entries.append(
            ssv_910324_19890825_r2
        )

        sss_910324_19890825_1.supervision_periods.append(
            placeholder_ssp_910324_19890825_from_ss
        )

        # SQL Preprocessing View
        # Act
        self._run_ingest_job_for_filename(
            "tak028_tak042_tak076_tak024_violation_reports.csv"
        )

        # Assert
        self.assert_expected_db_people(expected_people)

        ##############################################################
        # TAK292_TAK291_TAK024 CITATIONS
        ##############################################################
        # Arrange
        placeholder_ssp2_110035_20040712_1 = (
            entities.StateSupervisionPeriod.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
                supervision_sentences=[sss_110035_20040712_1],
                person=person_110035,
            )
        )
        ssv_110035_20040712_c1 = entities.StateSupervisionViolation.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-20040712-C1",
            supervision_periods=[sp_110035_20040712_1_9],
            person=person_110035,
        )
        ssvc_110035_20040712_c1_1_drg = (
            entities.StateSupervisionViolatedConditionEntry.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                condition="DRG",
                supervision_violation=ssv_110035_20040712_c1,
                person=person_110035,
            )
        )
        ssvc_110035_20040712_c1_1_law = (
            entities.StateSupervisionViolatedConditionEntry.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                condition="LAW",
                supervision_violation=ssv_110035_20040712_c1,
                person=person_110035,
            )
        )
        ssvr_110035_20040712_c1_1 = entities.StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="110035-20040712-C1",
            response_date=datetime.date(year=2007, month=2, day=10),
            is_draft=True,
            supervision_violation=ssv_110035_20040712_c1,
            response_type=StateSupervisionViolationResponseType.CITATION,
            response_type_raw_text="CITATION",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
            deciding_body_type_raw_text="SUPERVISION_OFFICER",
            person=person_110035,
        )
        ssv_110035_20040712_c1.supervision_violation_responses.append(
            ssvr_110035_20040712_c1_1
        )
        ssv_110035_20040712_c1.supervision_violated_conditions.extend(
            [ssvc_110035_20040712_c1_1_drg, ssvc_110035_20040712_c1_1_law]
        )
        sp_110035_20040712_1_9.supervision_violation_entries.append(
            ssv_110035_20040712_c1
        )
        sss_110035_20040712_1.supervision_periods.append(
            placeholder_ssp2_110035_20040712_1
        )

        placeholder_ssp2_110035_20040712_2 = (
            entities.StateSupervisionPeriod.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
                supervision_sentences=[sss_110035_20040712_2],
                person=person_110035,
            )
        )
        sss_110035_20040712_2.supervision_periods.append(
            placeholder_ssp2_110035_20040712_2
        )

        # Unmatched violations are added to the first placeholder supervision
        # period on that sentence.
        ssv_110035_20040712_c1.supervision_periods.append(
            placeholder_ssp_110035_20040712_2
        )
        placeholder_ssp_110035_20040712_2.supervision_violation_entries.append(
            ssv_110035_20040712_c1
        )

        placeholder_ssp2_910324_19890825_1 = (
            entities.StateSupervisionPeriod.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
                incarceration_sentences=[sis_910324_19890825_1],
                person=person_910324,
            )
        )
        ssv_910324_19890825_c1_1 = entities.StateSupervisionViolation.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="910324-19890825-C1",
            violated_conditions=None,
            supervision_periods=[sp_910324_19890825_2_0],
            person=person_910324,
        )
        ssvc_910324_19890825_c1_1_drg = (
            entities.StateSupervisionViolatedConditionEntry.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                condition="DRG",
                supervision_violation=ssv_910324_19890825_c1_1,
                person=person_910324,
            )
        )
        ssvr_910324_19890825_c1_1 = entities.StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id="910324-19890825-C1",
            response_date=datetime.date(year=1990, month=1, day=1),
            is_draft=False,
            supervision_violation=ssv_910324_19890825_c1_1,
            response_type=StateSupervisionViolationResponseType.CITATION,
            response_type_raw_text="CITATION",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
            deciding_body_type_raw_text="SUPERVISION_OFFICER",
            person=person_910324,
        )
        ssv_910324_19890825_c1_1.supervision_violation_responses.append(
            ssvr_910324_19890825_c1_1
        )
        ssv_910324_19890825_c1_1.supervision_violated_conditions.append(
            ssvc_910324_19890825_c1_1_drg
        )
        sp_910324_19890825_2_0.supervision_violation_entries.append(
            ssv_910324_19890825_c1_1
        )
        sis_910324_19890825_1.supervision_periods.append(
            placeholder_ssp2_910324_19890825_1
        )

        # SQL Preprocessing View
        # Act
        self._run_ingest_job_for_filename("tak291_tak292_tak024_citations.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        # Rerun for sanity
        self._do_ingest_job_rerun_for_tags(self.controller.get_file_tag_rank_list())

        # TODO(#2492): Until we implement proper cleanup of dangling
        #   placeholders, reruns of certain files will create new dangling
        #   placeholders with each rerun.
        self.assert_expected_db_people(
            expected_people, ignore_dangling_placeholders=True
        )

    def test_run_incarceration_period_na_not_converted_to_NaN(self) -> None:
        """Tests that values of 'NA' are not automatically converted to NaN
        (Not A Number) by our CSV reader, Pandas, thus essentially converting
        them into empty values that are lost.

        The test works by turning the file line limit down to 1, because the
        issue is triggered particularly when a file is split: the process of
        writing the parsed chunk of CSV back out to a new file persists the
        conversion from 'NA' to NaN that Pandas produced when it read the raw
        file in to be split.
        """
        self.controller.ingest_file_split_line_limit = 1

        # SQL Preprocessing View
        self._run_ingest_job_for_filename(
            "tak158_tak023_tak026_incarceration_period_from_incarceration_sentence.csv"
        )

        with SessionFactory.using_database(
            self.main_database_key, autocommit=False
        ) as session:
            found_people_from_db = dao.read_people(session)
            found_people = cast(
                List[entities.StatePerson],
                self.convert_and_clear_db_ids(found_people_from_db),
            )

        compliant_periods = 0
        for person in found_people:
            for sg in person.sentence_groups:
                for sentence in sg.incarceration_sentences:
                    for period in sentence.incarceration_periods:
                        self.assertIsNotNone(period.admission_reason)
                        self.assertIsNotNone(period.admission_reason_raw_text)
                        compliant_periods += 1

        # Asserting that we processed every row in the file successfully
        self.assertEqual(compliant_periods, 13)
