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

"""Unit and integration tests for North Dakota direct ingest."""

import datetime
import json
from typing import Type

import attr

from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.person_characteristics import (
    Ethnicity,
    Gender,
    Race,
    ResidencyStatus,
)
from recidiviz.common.constants.state.external_id_types import US_ND_ELITE, US_ND_SID
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentLevel,
    StateAssessmentType,
)
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
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateIncarcerationPeriodStatus,
)
from recidiviz.common.constants.state.state_person_alias import StatePersonAliasType
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
    StateSupervisionContactMethod,
    StateSupervisionContactReason,
    StateSupervisionContactStatus,
    StateSupervisionContactType,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseType,
)
from recidiviz.common.str_field_utils import normalize
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.regions.us_nd.us_nd_controller import UsNdController
from recidiviz.ingest.models.ingest_info import (
    IngestInfo,
    StateAgent,
    StateAlias,
    StateAssessment,
    StateCharge,
    StateCourtCase,
    StateIncarcerationIncident,
    StateIncarcerationIncidentOutcome,
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StatePerson,
    StatePersonEthnicity,
    StatePersonExternalId,
    StatePersonRace,
    StateProgramAssignment,
    StateSentenceGroup,
    StateSupervisionCaseTypeEntry,
    StateSupervisionContact,
    StateSupervisionPeriod,
    StateSupervisionSentence,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
    StateSupervisionViolationResponseDecisionEntry,
    StateSupervisionViolationTypeEntry,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.entity.state import entities
from recidiviz.tests.ingest.direct.regions.base_direct_ingest_controller_tests import (
    BaseDirectIngestControllerTests,
)

_INCIDENT_DETAILS_1 = (
    "230\nInmate Jon Hopkins would not follow directives to stop and be pat searched when coming "
    "from the IDR past traffic."
)
_INCIDENT_DETAILS_2 = (
    "210R 221R\nInmate Hopkins was walking through the lunch line in the IDR when he cut through "
    "one of the last rows of tables and when confronted he threatened Geer."
)
_INCIDENT_DETAILS_3 = (
    "230E\nInmate Hopkins was observed putting peanut butter and jelly in his pockets in the IDR.  "
    "He ignored staff multiple times when they called his name to get his attention.  He looked "
    "at staff, but continued to ignore him and then walked out of the IDR.\n\nThis report was "
    "reheard and resolved informally on 3/27/18."
)
_INCIDENT_DETAILS_4 = (
    "101\nInmate Knowles and another inmate were involved in a possible fight."
)
_INCIDENT_DETAILS_5 = (
    "305 215E\nInmate Knowles was involved in a gang related assault on another inmate."
)
_INCIDENT_DETAILS_6 = (
    "227\nStaff saw Martha Stewart with a cigarette to her mouth in the bathroom. Stewart admitted "
    "to smoking the cigarette."
)
_STATE_CODE = "US_ND"


class TestUsNdController(BaseDirectIngestControllerTests):
    """Unit tests for each North Dakota file to be ingested by the UsNdController."""

    @classmethod
    def region_code(cls) -> str:
        return _STATE_CODE.lower()

    @classmethod
    def controller_cls(cls) -> Type[BaseDirectIngestController]:
        return UsNdController

    @classmethod
    def schema_type(cls) -> SchemaType:
        return SchemaType.STATE

    def test_populate_data_elite_offenderidentifier(self) -> None:
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="39768",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="92237", id_type=US_ND_SID
                        ),
                        StatePersonExternalId(
                            state_person_external_id_id="39768", id_type=US_ND_ELITE
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="92237",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="12345", id_type=US_ND_SID
                        ),
                        StatePersonExternalId(
                            state_person_external_id_id="92237", id_type=US_ND_ELITE
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="52163",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="241896", id_type=US_ND_SID
                        ),
                        StatePersonExternalId(
                            state_person_external_id_id="52163", id_type=US_ND_ELITE
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="11111",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="241896", id_type=US_ND_SID
                        ),
                        StatePersonExternalId(
                            state_person_external_id_id="11111", id_type=US_ND_ELITE
                        ),
                    ],
                ),
            ]
        )

        self.run_legacy_parse_file_test(expected, "elite_offenderidentifier")

    def test_populate_data_elite_offenders(self) -> None:
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="39768",
                    surname="HOPKINS",
                    given_names="JON",
                    birthdate="8/15/1979",
                    gender="M",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="39768", id_type=US_ND_ELITE
                        )
                    ],
                    state_person_races=[StatePersonRace(race="CAUCASIAN")],
                    state_aliases=[
                        StateAlias(
                            surname="HOPKINS",
                            given_names="JON",
                            alias_type="GIVEN_NAME",
                        )
                    ],
                ),
                StatePerson(
                    state_person_id="52163",
                    surname="KNOWLES",
                    given_names="SOLANGE",
                    birthdate="6/24/1986",
                    gender="F",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="52163", id_type=US_ND_ELITE
                        )
                    ],
                    state_person_races=[StatePersonRace(race="BLACK")],
                    state_aliases=[
                        StateAlias(
                            surname="KNOWLES",
                            given_names="SOLANGE",
                            alias_type="GIVEN_NAME",
                        )
                    ],
                ),
            ]
        )

        self.run_legacy_parse_file_test(expected, "elite_offenders")

    def test_populate_data_elite_alias(self) -> None:
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="39768",
                    gender="M",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="39768", id_type=US_ND_ELITE
                        )
                    ],
                    state_person_ethnicities=[
                        StatePersonEthnicity(ethnicity="HISPANIC")
                    ],
                    state_aliases=[
                        StateAlias(
                            surname="HOPKINS",
                            given_names="TODD",
                            name_suffix="III",
                            alias_type="A",
                        ),
                        StateAlias(
                            surname="HODGSON",
                            given_names="JON",
                            middle_names="R",
                            alias_type="A",
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="52163",
                    gender="F",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="52163", id_type=US_ND_ELITE
                        )
                    ],
                    state_aliases=[StateAlias(given_names="SOLANGE", alias_type="A")],
                    state_person_races=[StatePersonRace(race="BLACK")],
                ),
            ]
        )

        self.run_legacy_parse_file_test(expected, "elite_alias")

    def test_populate_data_elite_offenderbookingstable(self) -> None:
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="52163",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="52163", id_type=US_ND_ELITE
                        )
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="113377", status="C"
                        ),
                        StateSentenceGroup(
                            state_sentence_group_id="114909", status="C"
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="39768",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="39768", id_type=US_ND_ELITE
                        )
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(state_sentence_group_id="105640", status="C")
                    ],
                ),
            ]
        )

        self.run_legacy_parse_file_test(expected, "elite_offenderbookingstable")

    def test_populate_data_elite_offendersentenceaggs(self) -> None:
        incarceration_sentence_105640 = StateIncarcerationSentence(
            parole_eligibility_date="11/24/2005"
        )

        incarceration_sentence_113377 = StateIncarcerationSentence(
            parole_eligibility_date="7/26/2018"
        )

        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="105640",
                            date_imposed="2/12/2004",
                            max_length="759",
                            state_incarceration_sentences=[
                                incarceration_sentence_105640
                            ],
                        ),
                        StateSentenceGroup(
                            state_sentence_group_id="114909",
                            date_imposed="2/27/2018",
                            max_length="315",
                        ),
                        StateSentenceGroup(
                            state_sentence_group_id="113377",
                            date_imposed="3/27/2018",
                            max_length="285",
                            state_incarceration_sentences=[
                                incarceration_sentence_113377
                            ],
                        ),
                        StateSentenceGroup(
                            state_sentence_group_id="115077",
                            date_imposed="10/29/2018",
                            max_length="1316",
                        ),
                        StateSentenceGroup(
                            state_sentence_group_id="44444",
                            date_imposed="10/29/2018",
                            is_life="True",
                        ),
                    ]
                ),
            ]
        )

        self.run_legacy_parse_file_test(expected, "elite_offendersentenceaggs")

    def test_populate_data_elite_offendersentences(self) -> None:
        sentences_114909 = [
            StateIncarcerationSentence(
                state_incarceration_sentence_id="114909-1",
                start_date="2/27/18  12:00:00 AM",
                projected_min_release_date="12/16/18  12:00:00 AM",
                projected_max_release_date="12/26/18  12:00:00 AM",
                is_life="False",
            ),
            StateIncarcerationSentence(
                state_incarceration_sentence_id="114909-2",
                start_date="2/27/18  12:00:00 AM",
                projected_min_release_date="1/8/19  12:00:00 AM",
                projected_max_release_date="1/8/19  12:00:00 AM",
                is_life="False",
            ),
        ]

        sentences_105640 = [
            StateIncarcerationSentence(
                state_incarceration_sentence_id="105640-1",
                start_date="1/8/90  12:00:00 AM",
                projected_min_release_date="10/21/96  12:00:00 AM",
                projected_max_release_date="6/18/99  12:00:00 AM",
                is_life="False",
            ),
            StateIncarcerationSentence(
                state_incarceration_sentence_id="105640-2",
                start_date="1/8/90  12:00:00 AM",
                projected_min_release_date="10/21/96  12:00:00 AM",
                projected_max_release_date="6/18/99  12:00:00 AM",
                is_life="False",
            ),
            StateIncarcerationSentence(
                state_incarceration_sentence_id="105640-5",
                start_date="10/21/96  12:00:00 AM",
                projected_min_release_date="2/15/12  12:00:00 AM",
                projected_max_release_date="10/21/16  12:00:00 AM",
                is_life="False",
            ),
            StateIncarcerationSentence(
                state_incarceration_sentence_id="105640-6",
                start_date="2/15/12  12:00:00 AM",
                projected_min_release_date="6/26/15  12:00:00 AM",
                projected_max_release_date="2/15/17  12:00:00 AM",
                is_life="False",
            ),
            StateIncarcerationSentence(
                state_incarceration_sentence_id="105640-7",
                start_date="2/15/12  12:00:00 AM",
                projected_min_release_date="10/18/13  12:00:00 AM",
                projected_max_release_date="2/15/14  12:00:00 AM",
                is_life="False",
            ),
        ]

        sentences_113377 = [
            StateIncarcerationSentence(
                state_incarceration_sentence_id="113377-4",
                start_date="2/27/18  12:00:00 AM",
                projected_min_release_date="9/30/18  12:00:00 AM",
                projected_max_release_date="11/24/18  12:00:00 AM",
                is_life="False",
            )
        ]

        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="114909",
                            state_incarceration_sentences=sentences_114909,
                        ),
                        StateSentenceGroup(
                            state_sentence_group_id="105640",
                            state_incarceration_sentences=sentences_105640,
                        ),
                        StateSentenceGroup(
                            state_sentence_group_id="113377",
                            state_incarceration_sentences=sentences_113377,
                        ),
                    ]
                ),
            ]
        )

        self.run_legacy_parse_file_test(expected, "elite_offendersentences")

    def test_populate_data_elite_offendersentenceterms(self) -> None:
        supervision_sentence_105640_2 = StateSupervisionSentence(
            state_supervision_sentence_id="105640-2",
            supervision_type="PROBATION",
            max_length="10Y 0M 0D",
        )

        supervision_sentence_105640_6 = StateSupervisionSentence(
            state_supervision_sentence_id="105640-6",
            supervision_type="PROBATION",
            max_length="5Y 0M 0D",
        )

        incarceration_sentence_105640_1 = StateIncarcerationSentence(
            state_incarceration_sentence_id="105640-1", max_length="10Y 0M 0D"
        )

        incarceration_sentence_105640_2 = StateIncarcerationSentence(
            state_incarceration_sentence_id="105640-2", max_length="10Y 0M 0D"
        )

        incarceration_sentence_105640_5 = StateIncarcerationSentence(
            state_incarceration_sentence_id="105640-5", max_length="20Y 0M 0D"
        )

        incarceration_sentence_105640_6 = StateIncarcerationSentence(
            state_incarceration_sentence_id="105640-6", max_length="5Y 0M 0D"
        )

        incarceration_sentence_105640_7 = StateIncarcerationSentence(
            state_incarceration_sentence_id="105640-7", max_length="2Y 0M 0D"
        )

        incarceration_sentence_114909_1 = StateIncarcerationSentence(
            state_incarceration_sentence_id="114909-1", max_length="1Y 0M 1D"
        )

        incarceration_sentence_114909_2 = StateIncarcerationSentence(
            state_incarceration_sentence_id="114909-2", max_length="0Y 0M 360D"
        )

        incarceration_sentence_113377_1 = StateIncarcerationSentence(
            state_incarceration_sentence_id="113377-1", max_length="1Y 0M 1D"
        )

        incarceration_sentence_113377_4 = StateIncarcerationSentence(
            state_incarceration_sentence_id="113377-4", max_length="0Y 0M 360D"
        )

        incarceration_sentence_113377_5 = StateIncarcerationSentence(
            state_incarceration_sentence_id="113377-5", max_length="0Y 0M 1000D"
        )

        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="105640",
                            state_supervision_sentences=[
                                supervision_sentence_105640_2,
                                supervision_sentence_105640_6,
                            ],
                            state_incarceration_sentences=[
                                incarceration_sentence_105640_1,
                                incarceration_sentence_105640_2,
                                incarceration_sentence_105640_5,
                                incarceration_sentence_105640_6,
                                incarceration_sentence_105640_7,
                            ],
                        ),
                        StateSentenceGroup(
                            state_sentence_group_id="114909",
                            state_incarceration_sentences=[
                                incarceration_sentence_114909_1,
                                incarceration_sentence_114909_2,
                            ],
                        ),
                        StateSentenceGroup(
                            state_sentence_group_id="113377",
                            state_incarceration_sentences=[
                                incarceration_sentence_113377_1,
                                incarceration_sentence_113377_4,
                                incarceration_sentence_113377_5,
                            ],
                        ),
                    ]
                ),
            ]
        )

        self.run_legacy_parse_file_test(expected, "elite_offendersentenceterms")

    def test_populate_data_elite_offenderchargestable(self) -> None:
        state_charge_105640_1 = StateCharge(
            state_charge_id="105640-1",
            status="SENTENCED",
            offense_date="6/19/89  12:00:00 AM",
            statute="1801",
            description="KIDNAPPING",
            classification_type="F",
            classification_subtype="B",
            offense_type="VIOLENT",
            is_violent="True",
            counts="1",
            is_controlling="False",
            state_court_case=StateCourtCase(state_court_case_id="5190"),
        )

        state_charge_105640_2 = StateCharge(
            state_charge_id="105640-2",
            status="SENTENCED",
            offense_date="6/19/89  12:00:00 AM",
            statute="A2003",
            description="GROSS SEXUAL IMPOSITION",
            classification_type="F",
            classification_subtype="A",
            offense_type="SEX",
            is_violent="False",
            counts="1",
            charge_notes="TO REGISTER AS A SEX OFFENDER (PLEA OF GUILTY)",
            is_controlling="False",
            state_court_case=StateCourtCase(state_court_case_id="5190"),
        )

        state_charge_105640_5 = StateCharge(
            state_charge_id="105640-5",
            status="SENTENCED",
            offense_date="6/19/89  12:00:00 AM",
            statute="1601",
            description="MURDER",
            classification_type="F",
            classification_subtype="A",
            offense_type="VIOLENT",
            is_violent="True",
            counts="1",
            charge_notes="ATTEMPTED",
            is_controlling="False",
            state_court_case=StateCourtCase(state_court_case_id="5192"),
        )

        state_charge_105640_6 = StateCharge(
            state_charge_id="105640-6",
            status="SENTENCED",
            offense_date="4/29/91  12:00:00 AM",
            statute="A2003",
            description="GROSS SEXUAL IMPOSITION",
            classification_type="F",
            classification_subtype="B",
            offense_type="SEX",
            is_violent="False",
            counts="1",
            is_controlling="True",
            state_court_case=StateCourtCase(state_court_case_id="5193"),
        )

        state_charge_105640_7 = StateCharge(
            state_charge_id="105640-7",
            status="SENTENCED",
            offense_date="6/28/10  12:00:00 AM",
            statute="17011",
            description="ASSAULT",
            classification_type="F",
            classification_subtype="C",
            offense_type="VIOLENT",
            is_violent="True",
            counts="1",
            charge_notes="On a CO",
            is_controlling="False",
            state_court_case=StateCourtCase(state_court_case_id="154576"),
        )

        state_charge_113377_2 = StateCharge(
            state_charge_id="113377-2",
            status="SENTENCED",
            offense_date="11/13/16  12:00:00 AM",
            statute="2203",
            description="CRIMINAL TRESSPASS",
            classification_type="M",
            classification_subtype="A",
            offense_type="PROPERTY",
            is_violent="False",
            counts="1",
            is_controlling="False",
            state_court_case=StateCourtCase(state_court_case_id="178986"),
        )

        state_charge_113377_4 = StateCharge(
            state_charge_id="113377-4",
            status="SENTENCED",
            offense_date="7/30/16  12:00:00 AM",
            statute="2203",
            description="CRIMINAL TRESSPASS",
            classification_type="M",
            classification_subtype="A",
            offense_type="PROPERTY",
            is_violent="False",
            counts="1",
            is_controlling="False",
            state_court_case=StateCourtCase(state_court_case_id="178987"),
        )

        state_charge_113377_1 = StateCharge(
            state_charge_id="113377-1",
            status="SENTENCED",
            offense_date="8/23/17  12:00:00 AM",
            statute="362102",
            description="ABUSE OF ANIMALS PROHIBITED",
            classification_type="F",
            classification_subtype="C",
            offense_type="OTHPO",
            is_violent="False",
            counts="1",
            is_controlling="True",
            state_court_case=StateCourtCase(state_court_case_id="178768"),
        )

        state_charge_114909_1 = StateCharge(
            state_charge_id="114909-1",
            status="SENTENCED",
            offense_date="8/23/17  12:00:00 AM",
            statute="362102",
            description="ABUSE OF ANIMALS PROHIBITED",
            classification_type="F",
            classification_subtype="C",
            offense_type="OTHPO",
            is_violent="False",
            counts="1",
            is_controlling="False",
            state_court_case=StateCourtCase(state_court_case_id="181820"),
        )

        state_charge_114909_2 = StateCharge(
            state_charge_id="114909-2",
            status="SENTENCED",
            offense_date="11/13/16  12:00:00 AM",
            statute="2203",
            description="CRIMINAL TRESSPASS",
            classification_type="M",
            classification_subtype="A",
            offense_type="PROPERTY",
            is_violent="False",
            counts="1",
            is_controlling="True",
            state_court_case=StateCourtCase(state_court_case_id="181821"),
        )

        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="105640",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="105640-1",
                                    state_charges=[state_charge_105640_1],
                                ),
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="105640-2",
                                    state_charges=[state_charge_105640_2],
                                ),
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="105640-5",
                                    state_charges=[state_charge_105640_5],
                                ),
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="105640-6",
                                    state_charges=[state_charge_105640_6],
                                ),
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="105640-7",
                                    state_charges=[state_charge_105640_7],
                                ),
                            ],
                        ),
                        StateSentenceGroup(
                            state_sentence_group_id="113377",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="113377-2",
                                    state_charges=[state_charge_113377_2],
                                ),
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="113377-4",
                                    state_charges=[state_charge_113377_4],
                                ),
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="113377-1",
                                    state_charges=[state_charge_113377_1],
                                ),
                            ],
                        ),
                        StateSentenceGroup(
                            state_sentence_group_id="114909",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="114909-1",
                                    state_charges=[state_charge_114909_1],
                                ),
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id="114909-2",
                                    state_charges=[state_charge_114909_2],
                                ),
                            ],
                        ),
                    ]
                ),
            ]
        )

        self.run_legacy_parse_file_test(expected, "elite_offenderchargestable")

    def test_populate_data_elite_orderstable(self) -> None:
        court_case_5190 = StateCourtCase(
            state_court_case_id="5190",
            status="A",
            date_convicted="6/19/89  12:00:00 AM",
            next_court_date="6/19/89  12:00:00 AM",
            county_code="US_ND_CASS",
            judicial_district_code="EAST_CENTRAL",
            judge=StateAgent(agent_type="JUDGE", full_name="Sheindlin, Judy"),
        )

        court_case_5192 = StateCourtCase(
            state_court_case_id="5192",
            status="A",
            date_convicted="6/19/89  12:00:00 AM",
            next_court_date="6/19/89  12:00:00 AM",
            county_code="US_ND_CASS",
            judicial_district_code="EAST_CENTRAL",
            judge=StateAgent(agent_type="JUDGE", full_name="Sheindlin, Judy"),
        )

        court_case_5193 = StateCourtCase(
            state_court_case_id="5193",
            status="A",
            date_convicted="4/29/91  12:00:00 AM",
            next_court_date="4/29/91  12:00:00 AM",
            county_code="US_ND_BURLEIGH",
            judicial_district_code="SOUTH_CENTRAL",
            judge=StateAgent(agent_type="JUDGE", full_name="BIRDMAN, HARVEY"),
        )

        court_case_154576 = StateCourtCase(
            state_court_case_id="154576",
            status="A",
            date_convicted="8/10/10  12:00:00 AM",
            next_court_date="8/10/10  12:00:00 AM",
            county_code="FEDERAL",
            judicial_district_code="FEDERAL",
            judge=StateAgent(agent_type="JUDGE", full_name="Hollywood, Paul"),
        )

        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="105640",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_charges=[
                                        StateCharge(state_court_case=court_case_5190),
                                        StateCharge(state_court_case=court_case_5192),
                                        StateCharge(state_court_case=court_case_5193),
                                        StateCharge(state_court_case=court_case_154576),
                                    ]
                                ),
                            ],
                        ),
                    ]
                ),
            ]
        )

        self.run_legacy_parse_file_test(expected, "elite_orderstable")

    def test_populate_data_elite_externalmovements(self) -> None:
        incarceration_periods_113377 = [
            StateIncarcerationPeriod(
                state_incarceration_period_id="113377-2",
                status="OUT-Y",
                release_date="7/26/18  12:00:00 AM",
                facility="NDSP",
                release_reason="RPAR",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="113377-1",
                status="IN-N",
                admission_date="2/28/18  12:00:00 AM",
                facility="NDSP",
                admission_reason="OOS",
            ),
        ]

        incarceration_periods_105640 = [
            StateIncarcerationPeriod(
                state_incarceration_period_id="105640-1",
                status="IN-N",
                facility="NDSP",
                admission_date="1/1/19  12:00:00 AM",
                admission_reason="ADMN",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="105640-3",
                status="IN-N",
                facility="JRCC",
                admission_date="2/1/19  12:00:00 AM",
                admission_reason="INT",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="105640-5",
                status="IN-Y",
                facility="JRCC",
                admission_date="4/1/19  12:00:00 AM",
                admission_reason="HOSPS",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="105640-2",
                status="OUT-N",
                facility="NDSP",
                release_date="2/1/19  12:00:00 AM",
                release_reason="INT",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="105640-4",
                status="OUT-N",
                facility="JRCC",
                release_date="3/1/19  12:00:00 AM",
                release_reason="HOSPS",
            ),
        ]

        incarceration_periods_114909 = [
            StateIncarcerationPeriod(
                state_incarceration_period_id="114909-2",
                status="OUT-Y",
                release_date="1/8/19  12:00:00 AM",
                facility="NDSP",
                release_reason="RPRB",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="114909-1",
                status="IN-N",
                admission_date="11/9/18  12:00:00 AM",
                facility="NDSP",
                admission_reason="PV",
            ),
        ]

        incarceration_periods_555555 = [
            StateIncarcerationPeriod(
                state_incarceration_period_id="555555-1",
                status="IN-N",
                incarceration_type="EXTERNAL_UNKNOWN",
                admission_date="2/28/18  12:00:00 AM",
                facility="NTAD",
                admission_reason="PV",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="555555-2",
                status="OUT-N",
                incarceration_type="EXTERNAL_UNKNOWN",
                release_date="3/1/18  12:00:00 AM",
                facility="NTAD",
                release_reason="INT",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="555555-3",
                status="IN-N",
                incarceration_type="COUNTY_JAIL",
                admission_date="3/1/18  12:00:00 AM",
                facility="CJ",
                admission_reason="PV",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="555555-4",
                status="OUT-N",
                incarceration_type="COUNTY_JAIL",
                release_date="3/8/18  12:00:00 AM",
                facility="CJ",
                release_reason="INT",
            ),
            StateIncarcerationPeriod(
                state_incarceration_period_id="555555-5",
                status="IN-N",
                admission_date="3/8/18  12:00:00 AM",
                facility="NDSP",
                admission_reason="INT",
            ),
        ]

        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="113377",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_periods=incarceration_periods_113377
                                )
                            ],
                        ),
                        StateSentenceGroup(
                            state_sentence_group_id="105640",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_periods=incarceration_periods_105640
                                )
                            ],
                        ),
                        StateSentenceGroup(
                            state_sentence_group_id="114909",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_periods=incarceration_periods_114909
                                )
                            ],
                        ),
                        StateSentenceGroup(
                            state_sentence_group_id="555555",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_periods=incarceration_periods_555555
                                )
                            ],
                        ),
                    ]
                ),
            ]
        )

        self.run_legacy_parse_file_test(expected, "elite_externalmovements")

    def test_populate_data_elite_elite_offense_in_custody_and_pos_report_data(
        self,
    ) -> None:
        state_incarceration_incident_353844 = StateIncarcerationIncident(
            state_incarceration_incident_id="347484-353844",
            incident_type=None,  # TODO(#1948): Make MISC types more specific
            incident_date="1/27/2019",
            facility="NDSP",
            location_within_facility="TRAF",
            incident_details=_INCIDENT_DETAILS_1,
            state_incarceration_incident_outcomes=[
                StateIncarcerationIncidentOutcome(
                    state_incarceration_incident_outcome_id="347484-353844-41",
                    outcome_type="LCP",
                    date_effective="3/19/2019",
                    outcome_description="Loss of Commissary Privileges",
                    punishment_length_days="7",
                )
            ],
        )

        state_incarceration_incident_354527 = StateIncarcerationIncident(
            state_incarceration_incident_id="348086-354527",
            incident_type="MINOR",
            incident_date="4/10/2019",
            facility="JRCC",
            location_within_facility="IDR",
            incident_details=_INCIDENT_DETAILS_2,
        )

        state_incarceration_incident_378515 = StateIncarcerationIncident(
            state_incarceration_incident_id="369791-378515",
            incident_type="INS",
            incident_date="1/17/2019",
            facility="NDSP",
            location_within_facility="IDR",
            incident_details=_INCIDENT_DETAILS_3,
            state_incarceration_incident_outcomes=[
                StateIncarcerationIncidentOutcome(
                    state_incarceration_incident_outcome_id="369791-378515-57",
                    outcome_type="RTQ",
                    date_effective="1/27/2019",
                    outcome_description="RESTRICTED TO QUARTERS",
                    punishment_length_days="31",
                )
            ],
        )

        state_incarceration_incident_363863 = StateIncarcerationIncident(
            state_incarceration_incident_id="356508-363863",
            incident_type="MINOR",
            incident_date="3/22/2018",
            facility="NDSP",
            location_within_facility="SU1",
            incident_details=_INCIDENT_DETAILS_4,
        )

        state_incarceration_incident_366571 = StateIncarcerationIncident(
            state_incarceration_incident_id="359062-366571",
            incident_type="IIASSAULT",
            incident_date="1/27/2019",
            facility="NDSP",
            location_within_facility="TRAF",
            incident_details=_INCIDENT_DETAILS_5,
            state_incarceration_incident_outcomes=[
                StateIncarcerationIncidentOutcome(
                    state_incarceration_incident_outcome_id="359062-366571-29",
                    outcome_type="NOT_GUILTY",
                    date_effective="8/18/2017",
                    outcome_description="LOSS OF PRIVILEGES",
                    punishment_length_days="120",
                )
            ],
        )

        state_incarceration_incident_381647 = StateIncarcerationIncident(
            state_incarceration_incident_id="372645-381647",
            incident_type=None,  # TODO(#1948): Make MISC types more specific
            incident_date="4/17/2018",
            facility="MRCC",
            location_within_facility="HRT1",
            incident_details=_INCIDENT_DETAILS_6,
        )

        state_incarceration_incident_381647.state_incarceration_incident_outcomes = [
            StateIncarcerationIncidentOutcome(
                state_incarceration_incident_outcome_id="372645-381647-36",
                outcome_type="PAY",
                date_effective="4/20/2018",
                outcome_description="LOSS OF PAY",
                punishment_length_days=None,
            ),
            StateIncarcerationIncidentOutcome(
                state_incarceration_incident_outcome_id="372645-381647-37",
                outcome_type="EXD",
                date_effective="4/20/2018",
                outcome_description="EXTRA DUTY",
                punishment_length_days=None,
            ),
        ]

        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="39768",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="39768", id_type=US_ND_ELITE
                        )
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="105640",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_periods=[
                                        StateIncarcerationPeriod(
                                            state_incarceration_incidents=[
                                                state_incarceration_incident_353844,
                                                state_incarceration_incident_354527,
                                                state_incarceration_incident_378515,
                                            ]
                                        )
                                    ]
                                )
                            ],
                        )
                    ],
                ),
                StatePerson(
                    state_person_id="52163",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="52163", id_type=US_ND_ELITE
                        )
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="113377",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_periods=[
                                        StateIncarcerationPeriod(
                                            state_incarceration_incidents=[
                                                state_incarceration_incident_363863
                                            ]
                                        )
                                    ]
                                )
                            ],
                        ),
                        StateSentenceGroup(
                            state_sentence_group_id="110651",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_periods=[
                                        StateIncarcerationPeriod(
                                            state_incarceration_incidents=[
                                                state_incarceration_incident_366571
                                            ]
                                        )
                                    ]
                                )
                            ],
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="21109",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="21109", id_type=US_ND_ELITE
                        )
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id="5129",
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_periods=[
                                        StateIncarcerationPeriod(
                                            state_incarceration_incidents=[
                                                state_incarceration_incident_381647
                                            ]
                                        )
                                    ]
                                )
                            ],
                        )
                    ],
                ),
            ]
        )
        self.run_legacy_parse_file_test(
            expected, "elite_offense_in_custody_and_pos_report_data"
        )

    def test_populate_data_docstars_offenders(self) -> None:
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="241896",
                    surname="Knowles",
                    given_names="Solange",
                    middle_names="P",
                    birthdate="24-Jun-86",
                    gender="2",
                    current_address="000 1st AVE APT 1, WEST FARGO, ND, 58078",
                    residency_status="N",
                    state_person_races=[StatePersonRace(race="2")],
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="52163", id_type=US_ND_ELITE
                        ),
                        StatePersonExternalId(
                            state_person_external_id_id="241896", id_type=US_ND_SID
                        ),
                    ],
                    state_assessments=[
                        StateAssessment(
                            assessment_level="MODERATE",
                            assessment_class="RISK",
                            assessment_type="SORAC",
                        )
                    ],
                    state_aliases=[
                        StateAlias(
                            surname="Knowles",
                            given_names="Solange",
                            middle_names="P",
                            alias_type="GIVEN_NAME",
                        )
                    ],
                    supervising_officer=StateAgent(
                        state_agent_id="74", agent_type="SUPERVISION_OFFICER"
                    ),
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        StateSupervisionPeriod(
                                            state_supervision_case_type_entries=[
                                                StateSupervisionCaseTypeEntry(
                                                    case_type="0",
                                                    state_supervision_case_type_entry_id="241896",
                                                )
                                            ]
                                        )
                                    ]
                                )
                            ]
                        )
                    ],
                ),
                StatePerson(
                    state_person_id="92237",
                    surname="Hopkins",
                    given_names="Jon",
                    birthdate="15-Aug-79",
                    gender="1",
                    current_address="123 2nd ST N, FARGO, ND, 58102",
                    residency_status="N",
                    state_person_races=[StatePersonRace(race="1")],
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="39768", id_type=US_ND_ELITE
                        ),
                        StatePersonExternalId(
                            state_person_external_id_id="92237", id_type=US_ND_SID
                        ),
                    ],
                    state_assessments=[
                        StateAssessment(
                            assessment_level="HIGH",
                            assessment_class="RISK",
                            assessment_type="SORAC",
                        ),
                    ],
                    state_aliases=[
                        StateAlias(
                            surname="Hopkins",
                            given_names="Jon",
                            alias_type="GIVEN_NAME",
                        )
                    ],
                    supervising_officer=StateAgent(
                        state_agent_id="40", agent_type="SUPERVISION_OFFICER"
                    ),
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        StateSupervisionPeriod(
                                            state_supervision_case_type_entries=[
                                                StateSupervisionCaseTypeEntry(
                                                    case_type="0",
                                                    state_supervision_case_type_entry_id="92237",
                                                )
                                            ]
                                        )
                                    ]
                                )
                            ]
                        )
                    ],
                ),
                StatePerson(
                    state_person_id="92307",
                    surname="Sandison",
                    given_names="Mike",
                    birthdate="1-Jun-70",
                    gender="1",
                    current_address="111 3rd ST S #6, FARGO, ND, 58103",
                    state_person_ethnicities=[
                        StatePersonEthnicity(ethnicity="HISPANIC")
                    ],
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="92307", id_type=US_ND_SID
                        )
                    ],
                    state_aliases=[
                        StateAlias(
                            surname="Sandison",
                            given_names="Mike",
                            alias_type="GIVEN_NAME",
                        )
                    ],
                    supervising_officer=StateAgent(
                        state_agent_id="70", agent_type="SUPERVISION_OFFICER"
                    ),
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        StateSupervisionPeriod(
                                            state_supervision_case_type_entries=[
                                                StateSupervisionCaseTypeEntry(
                                                    case_type="-1",
                                                    state_supervision_case_type_entry_id="92307",
                                                )
                                            ]
                                        )
                                    ]
                                )
                            ]
                        )
                    ],
                ),
                StatePerson(
                    state_person_id="40404",
                    surname="Hopper",
                    given_names="Grace",
                    birthdate="1-Jul-85",
                    gender="1",
                    current_address="111 8th ST S #5, FARGO, ND, 58103",
                    state_person_ethnicities=[
                        StatePersonEthnicity(ethnicity="HISPANIC")
                    ],
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="40404", id_type=US_ND_SID
                        )
                    ],
                    state_aliases=[
                        StateAlias(
                            surname="Hopper",
                            given_names="Grace",
                            alias_type="GIVEN_NAME",
                        )
                    ],
                    supervising_officer=StateAgent(
                        state_agent_id="22", agent_type="SUPERVISION_OFFICER"
                    ),
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        StateSupervisionPeriod(
                                            state_supervision_case_type_entries=[
                                                StateSupervisionCaseTypeEntry(
                                                    case_type="0",
                                                    state_supervision_case_type_entry_id="40404",
                                                )
                                            ]
                                        )
                                    ]
                                )
                            ]
                        )
                    ],
                ),
            ]
        )

        self.run_legacy_parse_file_test(expected, "docstars_offenders")

    def test_populate_data_docstars_offendercasestable_with_officers(self) -> None:
        agent_63 = StateAgent(
            state_agent_id="63",
            agent_type="SUPERVISION_OFFICER",
            given_names="DAVID",
            surname="BORG",
        )
        agent_77 = StateAgent(
            state_agent_id="77",
            agent_type="SUPERVISION_OFFICER",
            given_names="COREY",
            surname="KOLPIN",
        )
        agent_154 = StateAgent(
            state_agent_id="154",
            agent_type="SUPERVISION_OFFICER",
            given_names="JOSEPH",
            surname="LUND",
        )

        violation_for_17111 = StateSupervisionViolation(
            state_supervision_violation_types=[
                StateSupervisionViolationTypeEntry(violation_type="ABSCONDED")
            ],
            state_supervision_violation_responses=[
                StateSupervisionViolationResponse(
                    response_type="PERMANENT_DECISION",
                    response_date="12/8/2014",
                    decision_agents=[agent_63],
                    supervision_violation_response_decisions=[
                        StateSupervisionViolationResponseDecisionEntry(
                            decision="DOCR Inmate Sentence",
                        )
                    ],
                )
            ],
        )

        violation_for_140408 = StateSupervisionViolation(
            state_supervision_violation_types=[
                StateSupervisionViolationTypeEntry(violation_type="TECHNICAL")
            ],
            state_supervision_violation_responses=[
                StateSupervisionViolationResponse(
                    response_type="PERMANENT_DECISION",
                    response_date="10/27/2018",
                    decision_agents=[agent_77],
                    supervision_violation_response_decisions=[
                        StateSupervisionViolationResponseDecisionEntry(
                            decision="DOCR Inmate Sentence",
                        )
                    ],
                )
            ],
        )

        violation_for_147777 = StateSupervisionViolation(
            state_supervision_violation_types=[
                StateSupervisionViolationTypeEntry(violation_type="LAW")
            ],
            is_violent="True",
            state_supervision_violation_responses=[
                StateSupervisionViolationResponse(
                    response_type="PERMANENT_DECISION",
                    response_date="2/27/2016",
                    decision_agents=[agent_77],
                    supervision_violation_response_decisions=[
                        StateSupervisionViolationResponseDecisionEntry(
                            decision="DOCR Inmate Sentence",
                        )
                    ],
                )
            ],
        )
        supervision_period_117109 = StateSupervisionPeriod(
            state_supervision_period_id="117109",
            start_date="1/1/2013",
            termination_date="2/2/2013",
            termination_reason="4",
            supervision_type="Pre-Trial",
            supervising_officer=agent_63,
            supervision_site="4",
            county_code="US_ND_CASS",
        )
        supervision_period_117110 = StateSupervisionPeriod(
            state_supervision_period_id="117110",
            start_date="7/17/2014",
            supervision_type="Parole",
            county_code="US_ND_CASS",
            supervising_officer=agent_154,
            supervision_site="4",
            supervision_level="5",
        )
        supervision_period_117111 = StateSupervisionPeriod(
            state_supervision_period_id="117111",
            supervision_type="Parole",
            start_date="7/17/2014",
            termination_date="12/8/2014",
            termination_reason="9",
            supervising_officer=agent_63,
            supervision_site="4",
            state_supervision_violation_entries=[violation_for_17111],
            county_code="INVALID",
        )
        supervision_period_140408 = StateSupervisionPeriod(
            state_supervision_period_id="140408",
            supervision_type="Suspended",
            start_date="3/24/2017",
            termination_date="2/27/2018",
            termination_reason="9",
            supervising_officer=agent_77,
            supervision_site="2",
            state_supervision_violation_entries=[violation_for_140408],
            county_code="US_ND_GRIGGS",
        )
        supervision_period_147777 = StateSupervisionPeriod(
            state_supervision_period_id="147777",
            supervision_type="Suspended",
            start_date="3/24/2013",
            termination_date="2/27/2016",
            termination_reason="9",
            supervising_officer=agent_77,
            supervision_site="2",
            state_supervision_violation_entries=[violation_for_147777],
            county_code="US_ND_GRIGGS",
        )

        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="92237",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="92237", id_type=US_ND_SID
                        )
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_sentence_id="117109",
                                    supervision_type="Pre-Trial",
                                    start_date="1/1/2013",
                                    projected_completion_date="3/3/2013",
                                    completion_date="2/2/2013",
                                    max_length="59d",
                                    state_supervision_periods=[
                                        supervision_period_117109
                                    ],
                                ),
                                StateSupervisionSentence(
                                    state_supervision_sentence_id="117110",
                                    supervision_type="Parole",
                                    start_date="7/17/2014",
                                    projected_completion_date="10/6/2014",
                                    max_length="92d",
                                    state_supervision_periods=[
                                        supervision_period_117110
                                    ],
                                    state_charges=[
                                        StateCharge(
                                            state_court_case=StateCourtCase(
                                                judge=StateAgent(
                                                    agent_type="JUDGE",
                                                    full_name="The Judge",
                                                ),
                                            )
                                        )
                                    ],
                                ),
                                StateSupervisionSentence(
                                    state_supervision_sentence_id="117111",
                                    supervision_type="Parole",
                                    start_date="7/17/2014",
                                    projected_completion_date="8/7/2015",
                                    completion_date="12/8/2014",
                                    max_length="580d",
                                    state_supervision_periods=[
                                        supervision_period_117111
                                    ],
                                    state_charges=[
                                        StateCharge(
                                            state_court_case=StateCourtCase(
                                                judge=StateAgent(
                                                    agent_type="JUDGE",
                                                    full_name="The Judge",
                                                ),
                                            )
                                        )
                                    ],
                                ),
                            ]
                        )
                    ],
                ),
                StatePerson(
                    state_person_id="241896",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="241896", id_type=US_ND_SID
                        )
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_sentence_id="140408",
                                    supervision_type="Suspended",
                                    start_date="3/24/2017",
                                    projected_completion_date="3/23/2019",
                                    completion_date="2/27/2018",
                                    state_supervision_periods=[
                                        supervision_period_140408
                                    ],
                                    state_charges=[
                                        StateCharge(
                                            state_court_case=StateCourtCase(
                                                judge=StateAgent(
                                                    agent_type="JUDGE",
                                                    full_name="Judge Person",
                                                ),
                                            )
                                        )
                                    ],
                                ),
                                StateSupervisionSentence(
                                    state_supervision_sentence_id="147777",
                                    supervision_type="Suspended",
                                    start_date="3/24/2013",
                                    projected_completion_date="3/23/2015",
                                    completion_date="2/27/2016",
                                    state_supervision_periods=[
                                        supervision_period_147777
                                    ],
                                    state_charges=[
                                        StateCharge(
                                            state_court_case=StateCourtCase(
                                                judge=StateAgent(
                                                    agent_type="JUDGE",
                                                    full_name="Judge Person",
                                                ),
                                            )
                                        )
                                    ],
                                ),
                            ]
                        )
                    ],
                ),
            ]
        )

        self.run_legacy_parse_file_test(
            expected, "docstars_offendercasestable_with_officers"
        )

    def test_populate_data_docstars_offensestable(self) -> None:
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="92237",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="92237", id_type=US_ND_SID
                        )
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_sentence_id="117111",
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id="122553",
                                            county_code="US_ND_BURLEIGH",
                                            ncic_code="3522",
                                            description="OPIUM OR DERIV-POSSESS",
                                            classification_type="F",
                                            is_violent="False",
                                            counts="1",
                                        ),
                                        StateCharge(
                                            state_charge_id="122554",
                                            county_code="US_ND_BURLEIGH",
                                            ncic_code="3550",
                                            description="NARCOTIC EQUIP-POSSESS",
                                            classification_type="F",
                                            is_violent="False",
                                            counts="1",
                                        ),
                                    ],
                                ),
                                StateSupervisionSentence(
                                    state_supervision_sentence_id="117110",
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id="122552",
                                            county_code="US_ND_BURLEIGH",
                                            ncic_code="3562",
                                            description="MARIJUANA-POSSESS",
                                            classification_type="F",
                                            is_violent="False",
                                            counts="1",
                                        )
                                    ],
                                ),
                            ]
                        )
                    ],
                ),
                StatePerson(
                    state_person_id="241896",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="241896", id_type=US_ND_SID
                        )
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_sentence_id="140408",
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id="149349",
                                            offense_date="7/30/2016",
                                            county_code="US_ND_GRIGGS",
                                            ncic_code="2204",
                                            description="CRIMINAL TRESPASS",
                                            classification_type="M",
                                            classification_subtype="A",
                                            is_violent="False",
                                            counts="1",
                                        )
                                    ],
                                )
                            ]
                        )
                    ],
                ),
            ]
        )

        self.run_legacy_parse_file_test(expected, "docstars_offensestable")

    def test_populate_data_docstars_lsi_chronology(self) -> None:
        metadata_12345 = json.dumps(
            {
                "domain_criminal_history": 7,
                "domain_education_employment": 7,
                "domain_financial": 1,
                "domain_family_marital": 2,
                "domain_accommodation": 0,
                "domain_leisure_recreation": 2,
                "domain_companions": 3,
                "domain_alcohol_drug_problems": 5,
                "domain_emotional_personal": 2,
                "domain_attitudes_orientation": 1,
                "question_18": 0,
                "question_19": 0,
                "question_20": 0,
                "question_21": 1,
                "question_23": 3,
                "question_24": 1,
                "question_25": 1,
                "question_27": 3,
                "question_31": 1,
                "question_39": 2,
                "question_40": 1,
                "question_51": 2,
                "question_52": 3,
            }
        )

        metadata_12346 = json.dumps(
            {
                "domain_criminal_history": 6,
                "domain_education_employment": 8,
                "domain_financial": 1,
                "domain_family_marital": 2,
                "domain_accommodation": 0,
                "domain_leisure_recreation": 2,
                "domain_companions": 2,
                "domain_alcohol_drug_problems": 1,
                "domain_emotional_personal": 0,
                "domain_attitudes_orientation": 2,
                "question_18": 0,
                "question_19": 0,
                "question_20": 0,
                "question_21": 0,
                "question_23": 2,
                "question_24": 2,
                "question_25": 0,
                "question_27": 2,
                "question_31": 0,
                "question_39": 2,
                "question_40": 3,
                "question_51": 2,
                "question_52": 2,
            }
        )

        metadata_55555 = json.dumps(
            {
                "domain_criminal_history": 6,
                "domain_education_employment": 10,
                "domain_financial": 2,
                "domain_family_marital": 3,
                "domain_accommodation": 0,
                "domain_leisure_recreation": 2,
                "domain_companions": 2,
                "domain_alcohol_drug_problems": 8,
                "domain_emotional_personal": 0,
                "domain_attitudes_orientation": 0,
                "question_18": 0,
                "question_19": 0,
                "question_20": 0,
                "question_21": 0,
                "question_23": 2,
                "question_24": 0,
                "question_25": 0,
                "question_27": 2,
                "question_31": 1,
                "question_39": 1,
                "question_40": 0,
                "question_51": 3,
                "question_52": 3,
            }
        )

        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="92237",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="92237", id_type=US_ND_SID
                        )
                    ],
                    state_assessments=[
                        StateAssessment(
                            state_assessment_id="12345",
                            assessment_class="RISK",
                            assessment_type="LSIR",
                            assessment_date="7/14/16",
                            assessment_score="30",
                            assessment_metadata=metadata_12345,
                        ),
                        StateAssessment(
                            state_assessment_id="12346",
                            assessment_class="RISK",
                            assessment_type="LSIR",
                            assessment_date="1/13/17",
                            assessment_score="24",
                            assessment_metadata=metadata_12346,
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="241896",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="241896", id_type=US_ND_SID
                        )
                    ],
                    state_assessments=[
                        StateAssessment(
                            state_assessment_id="55555",
                            assessment_class="RISK",
                            assessment_type="LSIR",
                            assessment_date="12/10/18",
                            assessment_score="33",
                            assessment_metadata=metadata_55555,
                        ),
                    ],
                ),
            ]
        )

        self.run_legacy_parse_file_test(expected, "docstars_lsi_chronology")

    def test_populate_data_ftr_episode(self) -> None:
        metadata_1231 = json.dumps(
            {
                "preferred_provider_id": "49",
                "preferred_location_id": "126",
                "strengths": "Wants lifestyle change",
                "needs": "Substance abuse treatment",
                "is_clinical_assessment": "Yes",
                "functional_impairments": "ADR,REC",
                "assessment_location": "NDSP",
                "referral_reason": "Support for drug usage",
                "specialist_first_name": "",
                "specialist_last_name": "",
                "specialist_initial": "",
                "submitted_by": "usrname_1",
                "submitted_by_name": "User 1",
            }
        )
        metadata_2342 = json.dumps(
            {
                "preferred_provider_id": "51",
                "preferred_location_id": "128",
                "strengths": "",
                "needs": "",
                "is_clinical_assessment": "Yes",
                "functional_impairments": "ATO",
                "assessment_location": "Lake Region Human Service Center",
                "referral_reason": "Needs support finding employment and housing",
                "specialist_first_name": "",
                "specialist_last_name": "",
                "specialist_initial": "",
                "submitted_by": "username_2",
                "submitted_by_name": "User 2",
            }
        )
        metadata_3453 = json.dumps(
            {
                "preferred_provider_id": "49",
                "preferred_location_id": "126",
                "strengths": "Wants to take action",
                "needs": "Community support",
                "is_clinical_assessment": "Yes",
                "functional_impairments": "EDE",
                "assessment_location": "DWCRC",
                "referral_reason": "ongoing community support",
                "specialist_first_name": "",
                "specialist_last_name": "",
                "specialist_initial": "",
                "submitted_by": "username_3",
                "submitted_by_name": "User 3",
            }
        )
        metadata_4564 = json.dumps(
            {
                "preferred_provider_id": "20",
                "preferred_location_id": "",
                "strengths": "",
                "needs": "",
                "is_clinical_assessment": "No",
                "functional_impairments": "",
                "assessment_location": "",
                "referral_reason": "Mental health concerns",
                "specialist_first_name": "",
                "specialist_last_name": "",
                "specialist_initial": "",
                "submitted_by": "username_4",
                "submitted_by_name": "User 4",
            }
        )
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="92237",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="92237", id_type=US_ND_SID
                        )
                    ],
                    state_program_assignments=[
                        StateProgramAssignment(
                            state_program_assignment_id="1231",
                            program_id="24",
                            program_location_id="83",
                            participation_status="Discharged",
                            referral_date="1/2/2018 12:00:00AM",
                            start_date="1/3/2018",
                            discharge_date="1/28/2018 2:12:48PM",
                            referral_metadata=metadata_1231,
                        ),
                        StateProgramAssignment(
                            state_program_assignment_id="2342",
                            participation_status="Submitted",
                            referral_date="7/20/2019 2:09:18PM",
                            referral_metadata=metadata_2342,
                        ),
                    ],
                ),
                StatePerson(
                    state_person_id="241896",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="241896", id_type=US_ND_SID
                        )
                    ],
                    state_program_assignments=[
                        StateProgramAssignment(
                            state_program_assignment_id="3453",
                            program_id="27",
                            program_location_id="87",
                            participation_status="In Progress",
                            referral_date="8/12/2019 12:00:00AM",
                            start_date="08/13/2019",
                            referral_metadata=metadata_3453,
                        ),
                        StateProgramAssignment(
                            state_program_assignment_id="4564",
                            program_id="20",
                            program_location_id="22",
                            participation_status="Denied",
                            referral_date="7/12/2019 12:00:00AM",
                            referral_metadata=metadata_4564,
                        ),
                    ],
                ),
            ]
        )
        self.run_legacy_parse_file_test(expected, "docstars_ftr_episode")

    def test_populate_docstars_contacts(self) -> None:
        agent_22 = StateAgent(
            state_agent_id="22",
            agent_type="SUPERVISION_OFFICER",
            full_name="FIRSTNAME LASTNAME",
        )

        state_supervision_contact_1 = StateSupervisionContact(
            state_supervision_contact_id="1231",
            contact_date="4/15/2020 12:00:00AM",
            contact_type="OO",
            location="OO",
            contacted_agent=agent_22,
            contact_reason="Supervision",
            status="OO",
            contact_method="OO",
        )
        state_supervision_contact_2 = StateSupervisionContact(
            state_supervision_contact_id="1232",
            contact_date="4/16/2020 12:00:00AM",
            contact_type="OV-UA-FF",
            location="OV-UA-FF",
            contacted_agent=agent_22,
            contact_reason="Supervision",
            status="OV-UA-FF",
            contact_method="OV-UA-FF",
        )
        state_supervision_contact_3 = StateSupervisionContact(
            state_supervision_contact_id="1233",
            contact_date="4/17/2020 12:00:00AM",
            contact_type="HV-CC-AC-NS",
            location="HV-CC-AC-NS",
            contacted_agent=agent_22,
            contact_reason="Supervision",
            status="HV-CC-AC-NS",
            contact_method="HV-CC-AC-NS",
        )

        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id="40404",
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id="40404", id_type=US_ND_SID
                        )
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        StateSupervisionPeriod(
                                            state_supervision_contacts=[
                                                state_supervision_contact_1,
                                                state_supervision_contact_2,
                                                state_supervision_contact_3,
                                            ]
                                        )
                                    ],
                                )
                            ]
                        )
                    ],
                )
            ]
        )

        self.run_legacy_parse_file_test(expected, "docstars_contacts_v2")

    # TODO(#2157): Move into integration specific file
    def test_run_full_ingest_all_files_specific_order(self) -> None:
        ######################################
        # ELITE OFFENDER IDENTIFIERS
        ######################################
        # Arrange
        person_1 = entities.StatePerson.new_with_defaults(state_code=_STATE_CODE)
        person_1_external_id = entities.StatePersonExternalId.new_with_defaults(
            external_id="39768",
            id_type=US_ND_ELITE,
            state_code=_STATE_CODE,
            person=person_1,
        )
        person_1_external_id_2 = entities.StatePersonExternalId.new_with_defaults(
            external_id="92237",
            id_type=US_ND_SID,
            state_code=_STATE_CODE,
            person=person_1,
        )
        person_1.external_ids.append(person_1_external_id)
        person_1.external_ids.append(person_1_external_id_2)

        person_2 = entities.StatePerson.new_with_defaults(state_code=_STATE_CODE)
        person_2_external_id = entities.StatePersonExternalId.new_with_defaults(
            external_id="52163",
            id_type=US_ND_ELITE,
            state_code=_STATE_CODE,
            person=person_2,
        )
        person_2_external_id_2 = entities.StatePersonExternalId.new_with_defaults(
            external_id="241896",
            id_type=US_ND_SID,
            state_code=_STATE_CODE,
            person=person_2,
        )
        person_2_external_id_3 = entities.StatePersonExternalId.new_with_defaults(
            external_id="11111",
            id_type=US_ND_ELITE,
            state_code=_STATE_CODE,
            person=person_2,
        )
        person_2.external_ids.append(person_2_external_id)
        person_2.external_ids.append(person_2_external_id_2)
        person_2.external_ids.append(person_2_external_id_3)

        person_3 = entities.StatePerson.new_with_defaults(state_code=_STATE_CODE)
        person_3_external_id_1 = entities.StatePersonExternalId.new_with_defaults(
            external_id="12345",
            id_type=US_ND_SID,
            state_code=_STATE_CODE,
            person=person_3,
        )
        person_3_external_id_2 = entities.StatePersonExternalId.new_with_defaults(
            external_id="92237",
            id_type=US_ND_ELITE,
            state_code=_STATE_CODE,
            person=person_3,
        )
        person_3.external_ids.append(person_3_external_id_1)
        person_3.external_ids.append(person_3_external_id_2)
        expected_people = [person_3, person_2, person_1]

        # Act
        self._run_ingest_job_for_filename("elite_offenderidentifier.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # ELITE OFFENDERS
        ######################################
        # Arrange
        person_1.full_name = '{"given_names": "JON", "surname": "HOPKINS"}'
        person_1.birthdate = datetime.date(year=1979, month=8, day=15)
        person_1.birthdate_inferred_from_age = False
        person_1.gender = Gender.MALE
        person_1.gender_raw_text = "M"
        person_1.state_code = _STATE_CODE
        person_1_race = entities.StatePersonRace.new_with_defaults(
            state_code=_STATE_CODE,
            race=Race.WHITE,
            race_raw_text="CAUCASIAN",
            person=person_1,
        )
        person_1_alias_1 = entities.StatePersonAlias.new_with_defaults(
            full_name='{"given_names": "JON", "surname": "HOPKINS"}',
            alias_type=StatePersonAliasType.GIVEN_NAME,
            alias_type_raw_text="GIVEN_NAME",
            state_code=_STATE_CODE,
            person=person_1,
        )
        person_1.races = [person_1_race]
        person_1.aliases = [person_1_alias_1]

        person_2.full_name = '{"given_names": "SOLANGE", "surname": "KNOWLES"}'
        person_2.birthdate = datetime.date(year=1986, month=6, day=24)
        person_2.birthdate_inferred_from_age = False
        person_2.gender = Gender.FEMALE
        person_2.gender_raw_text = "F"
        person_2.state_code = _STATE_CODE
        person_2_race = entities.StatePersonRace.new_with_defaults(
            state_code=_STATE_CODE,
            race=Race.BLACK,
            race_raw_text="BLACK",
            person=person_2,
        )
        person_2_alias_1 = entities.StatePersonAlias.new_with_defaults(
            full_name='{"given_names": "SOLANGE", "surname": "KNOWLES"}',
            alias_type=StatePersonAliasType.GIVEN_NAME,
            alias_type_raw_text="GIVEN_NAME",
            state_code=_STATE_CODE,
            person=person_2,
        )
        person_2.races = [person_2_race]
        person_2.aliases = [person_2_alias_1]

        # Act
        self._run_ingest_job_for_filename("elite_offenders.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # ELITE ALIASES
        ######################################
        # Arrange
        # TODO(#2158): Why do we fill out full_name and keep the distinct
        # parts (in comparison to Person).
        person_1_ethnicity = entities.StatePersonEthnicity.new_with_defaults(
            state_code=_STATE_CODE,
            ethnicity=Ethnicity.HISPANIC,
            ethnicity_raw_text="HISPANIC",
            person=person_1,
        )
        person_1_alias_2 = entities.StatePersonAlias.new_with_defaults(
            full_name='{"given_names": "TODD", "name_suffix": "III", "surname": "HOPKINS"}',
            alias_type=StatePersonAliasType.ALIAS,
            alias_type_raw_text="A",
            state_code=_STATE_CODE,
            person=person_1,
        )
        person_1_alias_3 = entities.StatePersonAlias.new_with_defaults(
            full_name='{"given_names": "JON", "middle_names": "R", "surname": "HODGSON"}',
            alias_type=StatePersonAliasType.ALIAS,
            alias_type_raw_text="A",
            state_code=_STATE_CODE,
            person=person_1,
        )
        person_2_alias_2 = entities.StatePersonAlias.new_with_defaults(
            full_name='{"given_names": "SOLANGE"}',
            alias_type=StatePersonAliasType.ALIAS,
            alias_type_raw_text="A",
            state_code=_STATE_CODE,
            person=person_2,
        )
        person_1.ethnicities.append(person_1_ethnicity)
        person_1.aliases.append(person_1_alias_2)
        person_1.aliases.append(person_1_alias_3)
        person_2.aliases.append(person_2_alias_2)

        # Act
        self._run_ingest_job_for_filename("elite_alias.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # ELITE BOOKINGS
        ######################################
        # Arrange
        sentence_group_105640 = entities.StateSentenceGroup.new_with_defaults(
            external_id="105640",
            status=StateSentenceStatus.COMPLETED,
            status_raw_text="C",
            state_code=_STATE_CODE,
            person=person_1,
        )
        sentence_group_113377 = entities.StateSentenceGroup.new_with_defaults(
            external_id="113377",
            status=StateSentenceStatus.COMPLETED,
            status_raw_text="C",
            state_code=_STATE_CODE,
            person=person_2,
        )
        sentence_group_114909 = entities.StateSentenceGroup.new_with_defaults(
            external_id="114909",
            status=StateSentenceStatus.COMPLETED,
            status_raw_text="C",
            state_code=_STATE_CODE,
            person=person_2,
        )
        person_1.sentence_groups.append(sentence_group_105640)
        person_2.sentence_groups.append(sentence_group_113377)
        person_2.sentence_groups.append(sentence_group_114909)

        # Act
        self._run_ingest_job_for_filename("elite_offenderbookingstable.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # ELITE SENTENCE AGGS
        ######################################
        # Arrange
        # TODO(#2155): Should these have an external_id?
        incarceration_sentence_1 = (
            entities.StateIncarcerationSentence.new_with_defaults(
                state_code=_STATE_CODE,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                parole_eligibility_date=datetime.date(year=2005, month=11, day=24),
                sentence_group=sentence_group_105640,
                person=person_1,
            )
        )
        incarceration_sentence_2 = (
            entities.StateIncarcerationSentence.new_with_defaults(
                state_code=_STATE_CODE,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                parole_eligibility_date=datetime.date(year=2018, month=7, day=26),
                sentence_group=sentence_group_113377,
                person=person_2,
            )
        )
        sentence_group_105640.incarceration_sentences.append(incarceration_sentence_1)
        sentence_group_105640.date_imposed = datetime.date(year=2004, month=2, day=12)
        sentence_group_105640.max_length_days = 759

        sentence_group_113377.incarceration_sentences.append(incarceration_sentence_2)
        sentence_group_113377.date_imposed = datetime.date(year=2018, month=3, day=27)
        sentence_group_113377.max_length_days = 285

        sentence_group_114909.date_imposed = datetime.date(year=2018, month=2, day=27)
        sentence_group_114909.max_length_days = 315

        person_4 = entities.StatePerson.new_with_defaults(state_code=_STATE_CODE)
        sentence_group_115077 = entities.StateSentenceGroup.new_with_defaults(
            external_id="115077",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE,
            max_length_days=1316,
            date_imposed=datetime.date(year=2018, month=10, day=29),
            person=person_4,
        )
        person_4.sentence_groups.append(sentence_group_115077)
        expected_people.append(person_4)

        sentence_group_44444 = entities.StateSentenceGroup.new_with_defaults(
            external_id="44444",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE,
            max_length_days=None,
            date_imposed=datetime.date(year=2018, month=10, day=29),
            is_life=True,
            person=person_4,
        )
        person_4.sentence_groups.append(sentence_group_44444)

        # Act
        self._run_ingest_job_for_filename("elite_offendersentenceaggs.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # ELITE SENTENCES
        ######################################
        # Arrange
        incarceration_sentence_114909_1 = (
            entities.StateIncarcerationSentence.new_with_defaults(
                external_id="114909-1",
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                start_date=datetime.date(year=2018, month=2, day=27),
                is_life=False,
                projected_min_release_date=datetime.date(year=2018, month=12, day=16),
                projected_max_release_date=datetime.date(year=2018, month=12, day=26),
                state_code=_STATE_CODE,
                sentence_group=sentence_group_114909,
                person=sentence_group_114909.person,
            )
        )
        incarceration_sentence_114909_2 = (
            entities.StateIncarcerationSentence.new_with_defaults(
                external_id="114909-2",
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                start_date=datetime.date(year=2018, month=2, day=27),
                is_life=False,
                projected_min_release_date=datetime.date(year=2019, month=1, day=8),
                projected_max_release_date=datetime.date(year=2019, month=1, day=8),
                state_code=_STATE_CODE,
                sentence_group=sentence_group_114909,
                person=sentence_group_114909.person,
            )
        )
        incarceration_sentence_105640_1 = (
            entities.StateIncarcerationSentence.new_with_defaults(
                external_id="105640-1",
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                start_date=datetime.date(year=1990, month=1, day=8),
                is_life=False,
                projected_min_release_date=datetime.date(year=1996, month=10, day=21),
                projected_max_release_date=datetime.date(year=1999, month=6, day=18),
                state_code=_STATE_CODE,
                sentence_group=sentence_group_105640,
                person=sentence_group_105640.person,
            )
        )
        incarceration_sentence_105640_2 = (
            entities.StateIncarcerationSentence.new_with_defaults(
                external_id="105640-2",
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                start_date=datetime.date(year=1990, month=1, day=8),
                is_life=False,
                projected_min_release_date=datetime.date(year=1996, month=10, day=21),
                projected_max_release_date=datetime.date(year=1999, month=6, day=18),
                state_code=_STATE_CODE,
                sentence_group=sentence_group_105640,
                person=sentence_group_105640.person,
            )
        )
        incarceration_sentence_105640_5 = (
            entities.StateIncarcerationSentence.new_with_defaults(
                external_id="105640-5",
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                start_date=datetime.date(year=1996, month=10, day=21),
                is_life=False,
                projected_min_release_date=datetime.date(year=2012, month=2, day=15),
                projected_max_release_date=datetime.date(year=2016, month=10, day=21),
                state_code=_STATE_CODE,
                sentence_group=sentence_group_105640,
                person=sentence_group_105640.person,
            )
        )
        incarceration_sentence_105640_6 = (
            entities.StateIncarcerationSentence.new_with_defaults(
                external_id="105640-6",
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                start_date=datetime.date(year=2012, month=2, day=15),
                is_life=False,
                projected_min_release_date=datetime.date(year=2015, month=6, day=26),
                projected_max_release_date=datetime.date(year=2017, month=2, day=15),
                state_code=_STATE_CODE,
                sentence_group=sentence_group_105640,
                person=sentence_group_105640.person,
            )
        )
        incarceration_sentence_105640_7 = (
            entities.StateIncarcerationSentence.new_with_defaults(
                external_id="105640-7",
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                start_date=datetime.date(year=2012, month=2, day=15),
                is_life=False,
                projected_min_release_date=datetime.date(year=2013, month=10, day=18),
                projected_max_release_date=datetime.date(year=2014, month=2, day=15),
                state_code=_STATE_CODE,
                sentence_group=sentence_group_105640,
                person=sentence_group_105640.person,
            )
        )
        incarceration_sentence_113377_4 = (
            entities.StateIncarcerationSentence.new_with_defaults(
                external_id="113377-4",
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                start_date=datetime.date(year=2018, month=2, day=27),
                is_life=False,
                projected_min_release_date=datetime.date(year=2018, month=9, day=30),
                projected_max_release_date=datetime.date(year=2018, month=11, day=24),
                state_code=_STATE_CODE,
                sentence_group=sentence_group_113377,
                person=sentence_group_113377.person,
            )
        )
        sentence_group_114909.incarceration_sentences.append(
            incarceration_sentence_114909_1
        )
        sentence_group_114909.incarceration_sentences.append(
            incarceration_sentence_114909_2
        )
        sentence_group_105640.incarceration_sentences.append(
            incarceration_sentence_105640_1
        )
        sentence_group_105640.incarceration_sentences.append(
            incarceration_sentence_105640_2
        )
        sentence_group_105640.incarceration_sentences.append(
            incarceration_sentence_105640_5
        )
        sentence_group_105640.incarceration_sentences.append(
            incarceration_sentence_105640_6
        )
        sentence_group_105640.incarceration_sentences.append(
            incarceration_sentence_105640_7
        )
        sentence_group_113377.incarceration_sentences.append(
            incarceration_sentence_113377_4
        )

        # Act
        self._run_ingest_job_for_filename("elite_offendersentences.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # ELITE SENTENCE TERMS
        ######################################
        # Arrange
        supervision_sentence_105640_2 = (
            entities.StateSupervisionSentence.new_with_defaults(
                external_id="105640-2",
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                supervision_type=StateSupervisionType.PROBATION,
                supervision_type_raw_text="PROBATION",
                max_length_days=3652,
                state_code=_STATE_CODE,
                sentence_group=sentence_group_105640,
                person=sentence_group_105640.person,
            )
        )
        supervision_sentence_105640_6 = (
            entities.StateSupervisionSentence.new_with_defaults(
                external_id="105640-6",
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                supervision_type=StateSupervisionType.PROBATION,
                supervision_type_raw_text="PROBATION",
                max_length_days=1826,
                state_code=_STATE_CODE,
                sentence_group=sentence_group_105640,
                person=sentence_group_105640.person,
            )
        )
        incarceration_sentence_113377_1 = (
            entities.StateIncarcerationSentence.new_with_defaults(
                external_id="113377-1",
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                max_length_days=366,
                state_code=_STATE_CODE,
                sentence_group=sentence_group_113377,
                person=sentence_group_113377.person,
            )
        )
        incarceration_sentence_113377_5 = (
            entities.StateIncarcerationSentence.new_with_defaults(
                external_id="113377-5",
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                max_length_days=1000,
                state_code=_STATE_CODE,
                sentence_group=sentence_group_113377,
                person=sentence_group_113377.person,
            )
        )
        incarceration_sentence_105640_1.max_length_days = 3652
        incarceration_sentence_105640_2.max_length_days = 3652
        incarceration_sentence_105640_5.max_length_days = 7305
        incarceration_sentence_105640_6.max_length_days = 1826
        incarceration_sentence_105640_7.max_length_days = 730
        incarceration_sentence_114909_1.max_length_days = 366
        incarceration_sentence_114909_2.max_length_days = 360
        incarceration_sentence_113377_4.max_length_days = 360
        sentence_group_105640.supervision_sentences.append(
            supervision_sentence_105640_2
        )
        sentence_group_105640.supervision_sentences.append(
            supervision_sentence_105640_6
        )
        sentence_group_113377.incarceration_sentences.append(
            incarceration_sentence_113377_1
        )
        sentence_group_113377.incarceration_sentences.append(
            incarceration_sentence_113377_5
        )

        # Act
        self._run_ingest_job_for_filename("elite_offendersentenceterms.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # ELITE CHARGES
        ######################################
        # Arrange
        incarceration_sentence_113377_2 = (
            entities.StateIncarcerationSentence.new_with_defaults(
                external_id="113377-2",
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                state_code=_STATE_CODE,
                sentence_group=sentence_group_113377,
                person=sentence_group_113377.person,
            )
        )
        sentence_group_113377.incarceration_sentences.append(
            incarceration_sentence_113377_2
        )
        charge_105640_1 = entities.StateCharge.new_with_defaults(
            external_id="105640-1",
            status=ChargeStatus.SENTENCED,
            status_raw_text="SENTENCED",
            offense_date=datetime.date(year=1989, month=6, day=19),
            statute="1801",
            description="KIDNAPPING",
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="F",
            classification_subtype="B",
            offense_type="VIOLENT",
            is_violent=True,
            counts=1,
            is_controlling=False,
            state_code=_STATE_CODE,
            incarceration_sentences=[incarceration_sentence_105640_1],
            person=incarceration_sentence_105640_1.person,
        )
        charge_105640_2 = entities.StateCharge.new_with_defaults(
            external_id="105640-2",
            status=ChargeStatus.SENTENCED,
            status_raw_text="SENTENCED",
            offense_date=datetime.date(year=1989, month=6, day=19),
            statute="A2003",
            description="GROSS SEXUAL IMPOSITION",
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="F",
            classification_subtype="A",
            offense_type="SEX",
            is_violent=False,
            counts=1,
            is_controlling=False,
            charge_notes="TO REGISTER AS A SEX OFFENDER (PLEA OF GUILTY)",
            state_code=_STATE_CODE,
            incarceration_sentences=[incarceration_sentence_105640_2],
            person=incarceration_sentence_105640_2.person,
        )
        charge_105640_5 = entities.StateCharge.new_with_defaults(
            external_id="105640-5",
            status=ChargeStatus.SENTENCED,
            status_raw_text="SENTENCED",
            offense_date=datetime.date(year=1989, month=6, day=19),
            statute="1601",
            description="MURDER",
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="F",
            classification_subtype="A",
            offense_type="VIOLENT",
            is_violent=True,
            counts=1,
            charge_notes="ATTEMPTED",
            is_controlling=False,
            state_code=_STATE_CODE,
            incarceration_sentences=[incarceration_sentence_105640_5],
            person=incarceration_sentence_105640_5.person,
        )
        charge_105640_6 = entities.StateCharge.new_with_defaults(
            external_id="105640-6",
            status=ChargeStatus.SENTENCED,
            status_raw_text="SENTENCED",
            offense_date=datetime.date(year=1991, month=4, day=29),
            statute="A2003",
            description="GROSS SEXUAL IMPOSITION",
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="F",
            classification_subtype="B",
            offense_type="SEX",
            is_violent=False,
            counts=1,
            is_controlling=True,
            state_code=_STATE_CODE,
            incarceration_sentences=[incarceration_sentence_105640_6],
            person=incarceration_sentence_105640_6.person,
        )
        charge_105640_7 = entities.StateCharge.new_with_defaults(
            external_id="105640-7",
            status=ChargeStatus.SENTENCED,
            status_raw_text="SENTENCED",
            offense_date=datetime.date(year=2010, month=6, day=28),
            statute="17011",
            description="ASSAULT",
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="F",
            classification_subtype="C",
            offense_type="VIOLENT",
            is_violent=True,
            counts=1,
            charge_notes="ON A CO",
            is_controlling=False,
            state_code=_STATE_CODE,
            incarceration_sentences=[incarceration_sentence_105640_7],
            person=incarceration_sentence_105640_7.person,
        )
        charge_113377_1 = entities.StateCharge.new_with_defaults(
            external_id="113377-1",
            status=ChargeStatus.SENTENCED,
            status_raw_text="SENTENCED",
            offense_date=datetime.date(year=2017, month=8, day=23),
            statute="362102",
            description="ABUSE OF ANIMALS PROHIBITED",
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="F",
            classification_subtype="C",
            offense_type="OTHPO",
            is_violent=False,
            counts=1,
            is_controlling=True,
            state_code=_STATE_CODE,
            incarceration_sentences=[incarceration_sentence_113377_1],
            person=incarceration_sentence_113377_1.person,
        )
        charge_113377_2 = entities.StateCharge.new_with_defaults(
            external_id="113377-2",
            status=ChargeStatus.SENTENCED,
            status_raw_text="SENTENCED",
            offense_date=datetime.date(year=2016, month=11, day=13),
            statute="2203",
            description="CRIMINAL TRESSPASS",
            classification_type=StateChargeClassificationType.MISDEMEANOR,
            classification_type_raw_text="M",
            classification_subtype="A",
            offense_type="PROPERTY",
            is_violent=False,
            counts=1,
            is_controlling=False,
            state_code=_STATE_CODE,
            incarceration_sentences=[incarceration_sentence_113377_2],
            person=incarceration_sentence_113377_2.person,
        )
        charge_113377_4 = entities.StateCharge.new_with_defaults(
            external_id="113377-4",
            status=ChargeStatus.SENTENCED,
            status_raw_text="SENTENCED",
            offense_date=datetime.date(year=2016, month=7, day=30),
            statute="2203",
            description="CRIMINAL TRESSPASS",
            classification_type=StateChargeClassificationType.MISDEMEANOR,
            classification_type_raw_text="M",
            classification_subtype="A",
            offense_type="PROPERTY",
            is_violent=False,
            counts=1,
            is_controlling=False,
            state_code=_STATE_CODE,
            incarceration_sentences=[incarceration_sentence_113377_4],
            person=incarceration_sentence_113377_4.person,
        )
        charge_114909_1 = entities.StateCharge.new_with_defaults(
            external_id="114909-1",
            status=ChargeStatus.SENTENCED,
            status_raw_text="SENTENCED",
            offense_date=datetime.date(year=2017, month=8, day=23),
            statute="362102",
            description="ABUSE OF ANIMALS PROHIBITED",
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="F",
            classification_subtype="C",
            offense_type="OTHPO",
            is_violent=False,
            counts=1,
            is_controlling=False,
            state_code=_STATE_CODE,
            incarceration_sentences=[incarceration_sentence_114909_1],
            person=incarceration_sentence_114909_1.person,
        )
        charge_114909_2 = entities.StateCharge.new_with_defaults(
            external_id="114909-2",
            status=ChargeStatus.SENTENCED,
            status_raw_text="SENTENCED",
            offense_date=datetime.date(year=2016, month=11, day=13),
            statute="2203",
            description="CRIMINAL TRESSPASS",
            classification_type=StateChargeClassificationType.MISDEMEANOR,
            classification_type_raw_text="M",
            classification_subtype="A",
            offense_type="PROPERTY",
            is_violent=False,
            counts=1,
            is_controlling=True,
            state_code=_STATE_CODE,
            incarceration_sentences=[incarceration_sentence_114909_2],
            person=incarceration_sentence_114909_2.person,
        )

        court_case_5190 = entities.StateCourtCase.new_with_defaults(
            external_id="5190",
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            charges=[charge_105640_1, charge_105640_2],
            state_code=_STATE_CODE,
            person=sentence_group_105640.person,
        )
        court_case_5192 = entities.StateCourtCase.new_with_defaults(
            external_id="5192",
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            charges=[charge_105640_5],
            state_code=_STATE_CODE,
            person=sentence_group_105640.person,
        )
        court_case_5193 = entities.StateCourtCase.new_with_defaults(
            external_id="5193",
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            charges=[charge_105640_6],
            state_code=_STATE_CODE,
            person=sentence_group_105640.person,
        )
        court_case_154576 = entities.StateCourtCase.new_with_defaults(
            external_id="154576",
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            charges=[charge_105640_7],
            state_code=_STATE_CODE,
            person=sentence_group_105640.person,
        )
        court_case_178768 = entities.StateCourtCase.new_with_defaults(
            external_id="178768",
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            charges=[charge_113377_1],
            state_code=_STATE_CODE,
            person=sentence_group_113377.person,
        )
        court_case_178986 = entities.StateCourtCase.new_with_defaults(
            external_id="178986",
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            charges=[charge_113377_2],
            state_code=_STATE_CODE,
            person=sentence_group_113377.person,
        )
        court_case_178987 = entities.StateCourtCase.new_with_defaults(
            external_id="178987",
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            charges=[charge_113377_4],
            state_code=_STATE_CODE,
            person=sentence_group_113377.person,
        )
        court_case_181820 = entities.StateCourtCase.new_with_defaults(
            external_id="181820",
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            charges=[charge_114909_1],
            state_code=_STATE_CODE,
            person=sentence_group_114909.person,
        )
        court_case_181821 = entities.StateCourtCase.new_with_defaults(
            external_id="181821",
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            charges=[charge_114909_2],
            state_code=_STATE_CODE,
            person=sentence_group_114909.person,
        )

        charge_105640_1.court_case = court_case_5190
        charge_105640_2.court_case = court_case_5190
        charge_105640_5.court_case = court_case_5192
        charge_105640_6.court_case = court_case_5193
        charge_105640_7.court_case = court_case_154576
        charge_113377_1.court_case = court_case_178768
        charge_113377_2.court_case = court_case_178986
        charge_113377_4.court_case = court_case_178987
        charge_114909_1.court_case = court_case_181820
        charge_114909_2.court_case = court_case_181821

        incarceration_sentence_105640_1.charges.append(charge_105640_1)
        incarceration_sentence_105640_2.charges.append(charge_105640_2)
        incarceration_sentence_105640_5.charges.append(charge_105640_5)
        incarceration_sentence_105640_6.charges.append(charge_105640_6)
        incarceration_sentence_105640_7.charges.append(charge_105640_7)
        incarceration_sentence_113377_1.charges.append(charge_113377_1)
        incarceration_sentence_113377_2.charges.append(charge_113377_2)
        incarceration_sentence_113377_4.charges.append(charge_113377_4)
        incarceration_sentence_114909_1.charges.append(charge_114909_1)
        incarceration_sentence_114909_2.charges.append(charge_114909_2)

        # Act
        self._run_ingest_job_for_filename("elite_offenderchargestable.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # ELITE ORDERS
        ######################################
        # Arrange
        # TODO(#2158): Parse full_name in the same way we do for person
        # TODO(#2111): Remove duplicates once we have external_ids.
        agent_judy = entities.StateAgent.new_with_defaults(
            agent_type=StateAgentType.JUDGE,
            agent_type_raw_text="JUDGE",
            state_code=_STATE_CODE,
            full_name='{"full_name": "SHEINDLIN, JUDY"}',
        )
        agent_judy_dup = attr.evolve(agent_judy)
        agent_harvey = entities.StateAgent.new_with_defaults(
            agent_type=StateAgentType.JUDGE,
            agent_type_raw_text="JUDGE",
            state_code=_STATE_CODE,
            full_name='{"full_name": "BIRDMAN, HARVEY"}',
        )
        agent_paul = entities.StateAgent.new_with_defaults(
            agent_type=StateAgentType.JUDGE,
            agent_type_raw_text="JUDGE",
            state_code=_STATE_CODE,
            full_name='{"full_name": "HOLLYWOOD, PAUL"}',
        )

        court_case_5190.date_convicted = datetime.date(year=1989, month=6, day=19)
        court_case_5190.next_court_date = datetime.date(year=1989, month=6, day=19)
        court_case_5190.county_code = "US_ND_CASS"
        court_case_5190.judicial_district_code = "EAST_CENTRAL"
        court_case_5190.status_raw_text = "A"
        court_case_5190.judge = agent_judy

        court_case_5192.date_convicted = datetime.date(year=1989, month=6, day=19)
        court_case_5192.next_court_date = datetime.date(year=1989, month=6, day=19)
        court_case_5192.county_code = "US_ND_CASS"
        court_case_5192.judicial_district_code = "EAST_CENTRAL"
        court_case_5192.status_raw_text = "A"
        court_case_5192.judge = agent_judy_dup

        court_case_5193.date_convicted = datetime.date(year=1991, month=4, day=29)
        court_case_5193.next_court_date = datetime.date(year=1991, month=4, day=29)
        court_case_5193.county_code = "US_ND_BURLEIGH"
        court_case_5193.judicial_district_code = "SOUTH_CENTRAL"
        court_case_5193.status_raw_text = "A"
        court_case_5193.judge = agent_harvey

        court_case_154576.date_convicted = datetime.date(year=2010, month=8, day=10)
        court_case_154576.next_court_date = datetime.date(year=2010, month=8, day=10)
        court_case_154576.county_code = "FEDERAL"
        court_case_154576.judicial_district_code = "FEDERAL"
        court_case_154576.status_raw_text = "A"
        court_case_154576.judge = agent_paul

        # Act
        self._run_ingest_job_for_filename("elite_orderstable.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # ELITE EXTERNAL MOVEMENTS
        ######################################
        # Arrange
        incarceration_sentence_113377_ips = (
            entities.StateIncarcerationSentence.new_with_defaults(
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                state_code=_STATE_CODE,
                sentence_group=sentence_group_113377,
                person=sentence_group_113377.person,
            )
        )
        sentence_group_113377.incarceration_sentences.append(
            incarceration_sentence_113377_ips
        )
        incarceration_period_113377_1 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="113377-1|113377-2",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            status_raw_text="OUT-Y",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            facility="NDSP",
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION,
            admission_reason_raw_text="OOS",
            admission_date=datetime.date(year=2018, month=2, day=28),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="RPAR",
            release_date=datetime.date(year=2018, month=7, day=26),
            state_code=_STATE_CODE,
            incarceration_sentences=[incarceration_sentence_113377_ips],
            person=incarceration_sentence_113377_ips.person,
        )
        incarceration_sentence_105640_ips = (
            entities.StateIncarcerationSentence.new_with_defaults(
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                state_code=_STATE_CODE,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                sentence_group=sentence_group_105640,
                person=sentence_group_105640.person,
            )
        )
        sentence_group_105640.incarceration_sentences.append(
            incarceration_sentence_105640_ips
        )
        incarceration_period_105640_1 = (
            entities.StateIncarcerationPeriod.new_with_defaults(
                external_id="105640-1|105640-2",
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                status_raw_text="OUT-N",
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                facility="NDSP",
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                admission_reason_raw_text="ADMN",
                admission_date=datetime.date(year=2019, month=1, day=1),
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
                release_reason_raw_text="INT",
                state_code=_STATE_CODE,
                release_date=datetime.date(year=2019, month=2, day=1),
                incarceration_sentences=[incarceration_sentence_105640_ips],
                person=incarceration_sentence_105640_ips.person,
            )
        )
        incarceration_period_105640_2 = (
            entities.StateIncarcerationPeriod.new_with_defaults(
                external_id="105640-3|105640-4",
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                status_raw_text="OUT-N",
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                facility="JRCC",
                admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
                admission_reason_raw_text="INT",
                admission_date=datetime.date(year=2019, month=2, day=1),
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
                release_reason_raw_text="HOSPS",
                state_code=_STATE_CODE,
                release_date=datetime.date(year=2019, month=3, day=1),
                incarceration_sentences=[incarceration_sentence_105640_ips],
                person=incarceration_sentence_105640_ips.person,
            )
        )
        incarceration_period_105640_3 = (
            entities.StateIncarcerationPeriod.new_with_defaults(
                external_id="105640-5",
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                status_raw_text="IN-Y",
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                facility="JRCC",
                admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
                admission_reason_raw_text="HOSPS",
                admission_date=datetime.date(year=2019, month=4, day=1),
                state_code=_STATE_CODE,
                incarceration_sentences=[incarceration_sentence_105640_ips],
                person=incarceration_sentence_105640_ips.person,
            )
        )

        incarceration_sentence_114909_ips = (
            entities.StateIncarcerationSentence.new_with_defaults(
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                state_code=_STATE_CODE,
                sentence_group=sentence_group_114909,
                person=sentence_group_114909.person,
            )
        )
        sentence_group_114909.incarceration_sentences.append(
            incarceration_sentence_114909_ips
        )
        incarceration_period_114909_1 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="114909-1|114909-2",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            status_raw_text="OUT-Y",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            facility="NDSP",
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            admission_reason_raw_text="PV",
            admission_date=datetime.date(year=2018, month=11, day=9),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="RPRB",
            release_date=datetime.date(year=2019, month=1, day=8),
            state_code=_STATE_CODE,
            incarceration_sentences=[incarceration_sentence_114909_ips],
            person=incarceration_sentence_114909_ips.person,
        )

        person_555555 = entities.StatePerson.new_with_defaults(state_code=_STATE_CODE)
        sentence_group_555555 = entities.StateSentenceGroup.new_with_defaults(
            external_id="555555",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE,
            person=person_555555,
        )
        person_555555.sentence_groups.append(sentence_group_555555)
        incarceration_sentence_555555_ips = (
            entities.StateIncarcerationSentence.new_with_defaults(
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                state_code=_STATE_CODE,
                sentence_group=sentence_group_555555,
                person=sentence_group_555555.person,
            )
        )
        sentence_group_555555.incarceration_sentences.append(
            incarceration_sentence_555555_ips
        )
        incarceration_period_555555_1 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="555555-1|555555-2",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            status_raw_text="OUT-N",
            incarceration_type=StateIncarcerationType.EXTERNAL_UNKNOWN,
            incarceration_type_raw_text="EXTERNAL_UNKNOWN",
            facility="NTAD",
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text="PV",
            admission_date=datetime.date(year=2018, month=2, day=28),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            release_reason_raw_text="INT",
            release_date=datetime.date(year=2018, month=3, day=1),
            state_code=_STATE_CODE,
            incarceration_sentences=[incarceration_sentence_555555_ips],
            person=incarceration_sentence_555555_ips.person,
        )
        incarceration_period_555555_2 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="555555-3|555555-4",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            status_raw_text="OUT-N",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            incarceration_type_raw_text="COUNTY_JAIL",
            facility="CJ",
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text="PV",
            admission_date=datetime.date(year=2018, month=3, day=1),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            release_reason_raw_text="INT",
            release_date=datetime.date(year=2018, month=3, day=8),
            state_code=_STATE_CODE,
            incarceration_sentences=[incarceration_sentence_555555_ips],
            person=incarceration_sentence_555555_ips.person,
        )
        incarceration_period_555555_3 = entities.StateIncarcerationPeriod.new_with_defaults(
            external_id="555555-5",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            status_raw_text="IN-N",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            facility="NDSP",
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            admission_reason_raw_text="INT",
            admission_date=datetime.date(year=2018, month=3, day=8),
            state_code=_STATE_CODE,
            incarceration_sentences=[incarceration_sentence_555555_ips],
            person=incarceration_sentence_555555_ips.person,
        )
        expected_people.append(person_555555)

        incarceration_sentence_113377_ips.incarceration_periods.append(
            incarceration_period_113377_1
        )
        incarceration_sentence_105640_ips.incarceration_periods.append(
            incarceration_period_105640_1
        )
        incarceration_sentence_105640_ips.incarceration_periods.append(
            incarceration_period_105640_2
        )
        incarceration_sentence_105640_ips.incarceration_periods.append(
            incarceration_period_105640_3
        )
        incarceration_sentence_114909_ips.incarceration_periods.append(
            incarceration_period_114909_1
        )
        incarceration_sentence_555555_ips.incarceration_periods.append(
            incarceration_period_555555_1
        )
        incarceration_sentence_555555_ips.incarceration_periods.append(
            incarceration_period_555555_2
        )
        incarceration_sentence_555555_ips.incarceration_periods.append(
            incarceration_period_555555_3
        )

        # Act
        self._run_ingest_job_for_filename("elite_externalmovements.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ##############################################
        # ELITE OFFENSE AND IN CUSTODY POS REPORT DATA
        ##############################################
        # Arrange
        incarceration_sentence_105640_placeholder = (
            entities.StateIncarcerationSentence.new_with_defaults(
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                state_code=_STATE_CODE,
                sentence_group=sentence_group_105640,
                person=sentence_group_105640.person,
            )
        )
        incarceration_period_105640_placeholder = (
            entities.StateIncarcerationPeriod.new_with_defaults(
                status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                state_code=_STATE_CODE,
                incarceration_sentences=[incarceration_sentence_105640_placeholder],
                person=sentence_group_105640.person,
            )
        )
        incarceration_sentence_105640_placeholder.incarceration_periods.append(
            incarceration_period_105640_placeholder
        )
        sentence_group_105640.incarceration_sentences.append(
            incarceration_sentence_105640_placeholder
        )
        incident_353844 = entities.StateIncarcerationIncident.new_with_defaults(
            external_id="347484-353844",
            incident_date=datetime.date(year=2019, month=1, day=27),
            facility="NDSP",
            location_within_facility="TRAF",
            state_code=_STATE_CODE,
            incident_details=normalize(_INCIDENT_DETAILS_1),
            incarceration_period=incarceration_period_105640_1,
            person=incarceration_sentence_105640_1.person,
        )
        outcome_353844_41 = (
            entities.StateIncarcerationIncidentOutcome.new_with_defaults(
                external_id="347484-353844-41",
                outcome_type=StateIncarcerationIncidentOutcomeType.PRIVILEGE_LOSS,
                outcome_type_raw_text="LCP",
                state_code=_STATE_CODE,
                outcome_description="LOSS OF COMMISSARY PRIVILEGES",
                date_effective=datetime.date(year=2019, month=3, day=19),
                punishment_length_days=7,
                incarceration_incident=incident_353844,
                person=incident_353844.person,
            )
        )
        incident_353844.incarceration_incident_outcomes.append(outcome_353844_41)
        incarceration_period_105640_1.incarceration_incidents.append(incident_353844)

        incident_354527 = entities.StateIncarcerationIncident.new_with_defaults(
            external_id="348086-354527",
            incident_date=datetime.date(year=2019, month=4, day=10),
            incident_type=StateIncarcerationIncidentType.MINOR_OFFENSE,
            incident_type_raw_text="MINOR",
            facility="JRCC",
            state_code=_STATE_CODE,
            location_within_facility="IDR",
            incident_details=normalize(_INCIDENT_DETAILS_2),
            incarceration_period=incarceration_period_105640_3,
            person=incarceration_period_105640_3.person,
        )
        incarceration_period_105640_3.incarceration_incidents.append(incident_354527)

        incident_378515 = entities.StateIncarcerationIncident.new_with_defaults(
            external_id="369791-378515",
            incident_type=StateIncarcerationIncidentType.DISORDERLY_CONDUCT,
            incident_type_raw_text="INS",
            incident_date=datetime.date(year=2019, month=1, day=17),
            facility="NDSP",
            state_code=_STATE_CODE,
            location_within_facility="IDR",
            incident_details=normalize(_INCIDENT_DETAILS_3.upper()),
            incarceration_period=incarceration_period_105640_1,
            person=incarceration_sentence_105640_1.person,
        )
        outcome_378515_57 = (
            entities.StateIncarcerationIncidentOutcome.new_with_defaults(
                external_id="369791-378515-57",
                outcome_type=StateIncarcerationIncidentOutcomeType.PRIVILEGE_LOSS,
                outcome_type_raw_text="RTQ",
                date_effective=datetime.date(year=2019, month=1, day=27),
                state_code=_STATE_CODE,
                outcome_description="RESTRICTED TO QUARTERS",
                punishment_length_days=31,
                incarceration_incident=incident_378515,
                person=incident_378515.person,
            )
        )
        incident_378515.incarceration_incident_outcomes.append(outcome_378515_57)
        incarceration_period_105640_1.incarceration_incidents.append(incident_378515)

        # TODO(#2131): Remove placeholders automatically
        incarceration_sentence_113377_placeholder = (
            entities.StateIncarcerationSentence.new_with_defaults(
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                state_code=_STATE_CODE,
                sentence_group=sentence_group_113377,
                person=sentence_group_113377.person,
            )
        )
        incarceration_period_113377_placeholder = (
            entities.StateIncarcerationPeriod.new_with_defaults(
                status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                state_code=_STATE_CODE,
                incarceration_sentences=[incarceration_sentence_113377_placeholder],
                person=sentence_group_113377.person,
            )
        )
        incarceration_sentence_113377_placeholder.incarceration_periods.append(
            incarceration_period_113377_placeholder
        )
        sentence_group_113377.incarceration_sentences.append(
            incarceration_sentence_113377_placeholder
        )

        incident_363863 = entities.StateIncarcerationIncident.new_with_defaults(
            external_id="356508-363863",
            incident_type=StateIncarcerationIncidentType.MINOR_OFFENSE,
            incident_type_raw_text="MINOR",
            incident_date=datetime.date(year=2018, month=3, day=22),
            facility="NDSP",
            state_code=_STATE_CODE,
            location_within_facility="SU1",
            incident_details=normalize(_INCIDENT_DETAILS_4),
            incarceration_period=incarceration_period_113377_1,
            person=incarceration_period_113377_1.person,
        )
        incarceration_period_113377_1.incarceration_incidents.append(incident_363863)

        # 110651 ---
        sentence_group_110651 = entities.StateSentenceGroup.new_with_defaults(
            external_id="110651",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE,
            person=person_2,
        )
        incarceration_sentence_110651_placeholder = (
            entities.StateIncarcerationSentence.new_with_defaults(
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                state_code=_STATE_CODE,
                sentence_group=sentence_group_110651,
                person=sentence_group_110651.person,
            )
        )
        incarceration_period_110651_placeholder = (
            entities.StateIncarcerationPeriod.new_with_defaults(
                status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                state_code=_STATE_CODE,
                incarceration_sentences=[incarceration_sentence_110651_placeholder],
                person=sentence_group_110651.person,
            )
        )
        incident_366571 = entities.StateIncarcerationIncident.new_with_defaults(
            external_id="359062-366571",
            incident_type=StateIncarcerationIncidentType.VIOLENCE,
            incident_type_raw_text="IIASSAULT",
            incident_date=datetime.date(year=2019, month=1, day=27),
            facility="NDSP",
            state_code=_STATE_CODE,
            location_within_facility="TRAF",
            incident_details=normalize(_INCIDENT_DETAILS_5),
            incarceration_period=incarceration_period_110651_placeholder,
            person=incarceration_period_110651_placeholder.person,
        )
        outcome_366571_29 = (
            entities.StateIncarcerationIncidentOutcome.new_with_defaults(
                external_id="359062-366571-29",
                outcome_type=StateIncarcerationIncidentOutcomeType.NOT_GUILTY,
                outcome_type_raw_text="NOT_GUILTY",
                date_effective=datetime.date(year=2017, month=8, day=18),
                outcome_description="LOSS OF PRIVILEGES",
                state_code=_STATE_CODE,
                punishment_length_days=120,
                incarceration_incident=incident_366571,
                person=incident_366571.person,
            )
        )
        incident_366571.incarceration_incident_outcomes.append(outcome_366571_29)
        incarceration_period_110651_placeholder.incarceration_incidents.append(
            incident_366571
        )
        incarceration_sentence_110651_placeholder.incarceration_periods.append(
            incarceration_period_110651_placeholder
        )
        sentence_group_110651.incarceration_sentences.append(
            incarceration_sentence_110651_placeholder
        )
        person_2.sentence_groups.append(sentence_group_110651)

        person_5 = entities.StatePerson.new_with_defaults(state_code=_STATE_CODE)
        person_5_external_id = entities.StatePersonExternalId.new_with_defaults(
            external_id="21109",
            id_type=US_ND_ELITE,
            state_code=_STATE_CODE,
            person=person_5,
        )
        sentence_group_5129 = entities.StateSentenceGroup.new_with_defaults(
            external_id="5129",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE,
            person=person_5,
        )
        incarceration_sentence_5129_placeholder = (
            entities.StateIncarcerationSentence.new_with_defaults(
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                state_code=_STATE_CODE,
                sentence_group=sentence_group_5129,
                person=sentence_group_5129.person,
            )
        )
        incarceration_period_5129_placeholder = (
            entities.StateIncarcerationPeriod.new_with_defaults(
                status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                state_code=_STATE_CODE,
                incarceration_sentences=[incarceration_sentence_5129_placeholder],
                person=sentence_group_5129.person,
            )
        )

        person_5.external_ids.append(person_5_external_id)
        person_5.sentence_groups.append(sentence_group_5129)
        sentence_group_5129.incarceration_sentences.append(
            incarceration_sentence_5129_placeholder
        )
        incarceration_sentence_5129_placeholder.incarceration_periods.append(
            incarceration_period_5129_placeholder
        )
        expected_people.append(person_5)

        incident_381647 = entities.StateIncarcerationIncident.new_with_defaults(
            external_id="372645-381647",
            incident_date=datetime.date(year=2018, month=4, day=17),
            facility="MRCC",
            location_within_facility="HRT1",
            incident_details=normalize(_INCIDENT_DETAILS_6),
            state_code=_STATE_CODE,
            incarceration_period=incarceration_period_5129_placeholder,
            person=incarceration_sentence_5129_placeholder.person,
        )
        outcome_381647_36 = (
            entities.StateIncarcerationIncidentOutcome.new_with_defaults(
                external_id="372645-381647-36",
                outcome_type=StateIncarcerationIncidentOutcomeType.FINANCIAL_PENALTY,
                outcome_type_raw_text="PAY",
                date_effective=datetime.date(year=2018, month=4, day=20),
                outcome_description="LOSS OF PAY",
                state_code=_STATE_CODE,
                incarceration_incident=incident_381647,
                person=incident_381647.person,
            )
        )
        outcome_381647_37 = (
            entities.StateIncarcerationIncidentOutcome.new_with_defaults(
                external_id="372645-381647-37",
                outcome_type=StateIncarcerationIncidentOutcomeType.DISCIPLINARY_LABOR,
                outcome_type_raw_text="EXD",
                date_effective=datetime.date(year=2018, month=4, day=20),
                outcome_description="EXTRA DUTY",
                state_code=_STATE_CODE,
                incarceration_incident=incident_381647,
                person=incident_381647.person,
            )
        )

        incident_381647.incarceration_incident_outcomes.append(outcome_381647_36)
        incident_381647.incarceration_incident_outcomes.append(outcome_381647_37)
        incarceration_period_5129_placeholder.incarceration_incidents.append(
            incident_381647
        )

        # Act
        self._run_ingest_job_for_filename(
            "elite_offense_in_custody_and_pos_report_data.csv"
        )

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # DOCSTARS OFFENDERS
        ######################################
        # Arrange
        # TODO(#2156): custom matching logic for assessments?
        agent_40 = entities.StateAgent.new_with_defaults(
            agent_type=StateAgentType.SUPERVISION_OFFICER,
            agent_type_raw_text="SUPERVISION_OFFICER",
            external_id="40",
            state_code=_STATE_CODE,
        )
        person_1_assessment_1 = entities.StateAssessment.new_with_defaults(
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_level_raw_text="HIGH",
            assessment_class=StateAssessmentClass.RISK,
            assessment_class_raw_text="RISK",
            assessment_type=StateAssessmentType.SORAC,
            assessment_type_raw_text="SORAC",
            state_code=_STATE_CODE,
            person=person_1,
        )
        # TODO(#2134): Currently overwriting race entity when the Enum value
        # is unchanged.
        person_1_race_2 = entities.StatePersonRace.new_with_defaults(
            state_code=_STATE_CODE, race=Race.WHITE, race_raw_text="1", person=person_1
        )
        # No additional alias for this Docstars Offenders record
        # because it's identical to a previous alias
        person_1.races = [person_1_race_2]
        person_1.gender_raw_text = "1"
        person_1.assessments = [person_1_assessment_1]
        person_1.current_address = "123 2ND ST N, FARGO, ND, 58102"
        person_1.residency_status = ResidencyStatus.PERMANENT
        person_1.supervising_officer = agent_40
        person_1_sentence_group_placeholder = (
            entities.StateSentenceGroup.new_with_defaults(
                state_code=_STATE_CODE,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                person=person_1,
            )
        )
        person_1_supervision_sentence_placeholder = (
            entities.StateSupervisionSentence.new_with_defaults(
                state_code=_STATE_CODE,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                sentence_group=person_1_sentence_group_placeholder,
                person=person_1,
            )
        )
        person_1_supervision_period_placeholder = (
            entities.StateSupervisionPeriod.new_with_defaults(
                state_code=_STATE_CODE,
                supervision_sentences=[person_1_supervision_sentence_placeholder],
                person=person_1,
            )
        )
        person_1_case_type = entities.StateSupervisionCaseTypeEntry.new_with_defaults(
            state_code=_STATE_CODE,
            case_type=StateSupervisionCaseType.GENERAL,
            case_type_raw_text="0",
            supervision_period=person_1_supervision_period_placeholder,
            person=person_1,
            external_id="92237",
        )
        person_1_supervision_period_placeholder.case_type_entries = [person_1_case_type]
        person_1_supervision_sentence_placeholder.supervision_periods = [
            person_1_supervision_period_placeholder
        ]
        person_1_sentence_group_placeholder.supervision_sentences = [
            person_1_supervision_sentence_placeholder
        ]
        person_1.sentence_groups.append(person_1_sentence_group_placeholder)

        person_2_supervising_officer = entities.StateAgent.new_with_defaults(
            agent_type=StateAgentType.SUPERVISION_OFFICER,
            agent_type_raw_text="SUPERVISION_OFFICER",
            external_id="74",
            state_code=_STATE_CODE,
        )
        person_2_assessment_1 = entities.StateAssessment.new_with_defaults(
            assessment_level=StateAssessmentLevel.MODERATE,
            assessment_level_raw_text="MODERATE",
            assessment_class=StateAssessmentClass.RISK,
            assessment_class_raw_text="RISK",
            assessment_type=StateAssessmentType.SORAC,
            assessment_type_raw_text="SORAC",
            state_code=_STATE_CODE,
            person=person_2,
        )
        person_2_race_2 = entities.StatePersonRace.new_with_defaults(
            state_code=_STATE_CODE, race=Race.BLACK, race_raw_text="2", person=person_2
        )
        person_2_alias_3 = entities.StatePersonAlias.new_with_defaults(
            full_name='{"given_names": "SOLANGE", "middle_names": "P", "surname": "KNOWLES"}',
            alias_type=StatePersonAliasType.GIVEN_NAME,
            alias_type_raw_text="GIVEN_NAME",
            state_code=_STATE_CODE,
            person=person_2,
        )
        person_2.aliases.append(person_2_alias_3)
        person_2.races = [person_2_race_2]
        person_2.assessments = [person_2_assessment_1]
        person_2.full_name = (
            '{"given_names": "SOLANGE", "middle_names": "P", "surname": "KNOWLES"}'
        )
        person_2.current_address = "000 1ST AVE APT 1, WEST FARGO, ND, 58078"
        person_2.gender_raw_text = "2"
        person_2.residency_status = ResidencyStatus.PERMANENT
        person_2.supervising_officer = person_2_supervising_officer
        person_2_sentence_group_placeholder = (
            entities.StateSentenceGroup.new_with_defaults(
                state_code=_STATE_CODE,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                person=person_2,
            )
        )
        person_2_supervision_sentence_placeholder = (
            entities.StateSupervisionSentence.new_with_defaults(
                state_code=_STATE_CODE,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                sentence_group=person_2_sentence_group_placeholder,
                person=person_2,
            )
        )
        person_2_supervision_period_placeholder = (
            entities.StateSupervisionPeriod.new_with_defaults(
                state_code=_STATE_CODE,
                supervision_sentences=[person_2_supervision_sentence_placeholder],
                person=person_2,
            )
        )
        person_2_case_type = entities.StateSupervisionCaseTypeEntry.new_with_defaults(
            state_code=_STATE_CODE,
            case_type=StateSupervisionCaseType.GENERAL,
            case_type_raw_text="0",
            supervision_period=person_2_supervision_period_placeholder,
            person=person_2,
            external_id="241896",
        )
        person_2_supervision_period_placeholder.case_type_entries = [person_2_case_type]
        person_2_supervision_sentence_placeholder.supervision_periods = [
            person_2_supervision_period_placeholder
        ]
        person_2_sentence_group_placeholder.supervision_sentences = [
            person_2_supervision_sentence_placeholder
        ]
        person_2.sentence_groups.append(person_2_sentence_group_placeholder)

        person_6 = entities.StatePerson.new_with_defaults(
            full_name='{"given_names": "MIKE", "surname": "SANDISON"}',
            birthdate=datetime.date(year=1970, month=6, day=1),
            birthdate_inferred_from_age=False,
            current_address="111 3RD ST S #6, FARGO, ND, 58103",
            residency_status=ResidencyStatus.PERMANENT,
            gender=Gender.MALE,
            gender_raw_text="1",
            state_code=_STATE_CODE,
        )
        person_6_external_id = entities.StatePersonExternalId.new_with_defaults(
            external_id="92307",
            id_type=US_ND_SID,
            state_code=_STATE_CODE,
            person=person_6,
        )
        person_6_ethnicity = entities.StatePersonEthnicity.new_with_defaults(
            state_code=_STATE_CODE,
            ethnicity=Ethnicity.HISPANIC,
            ethnicity_raw_text="HISPANIC",
            person=person_6,
        )
        person_6_alias = entities.StatePersonAlias.new_with_defaults(
            full_name='{"given_names": "MIKE", "surname": "SANDISON"}',
            alias_type=StatePersonAliasType.GIVEN_NAME,
            alias_type_raw_text="GIVEN_NAME",
            state_code=_STATE_CODE,
            person=person_6,
        )
        person_6_supervising_officer = entities.StateAgent.new_with_defaults(
            agent_type=StateAgentType.SUPERVISION_OFFICER,
            agent_type_raw_text="SUPERVISION_OFFICER",
            external_id="70",
            state_code=_STATE_CODE,
        )
        person_6.aliases = [person_6_alias]
        person_6.external_ids = [person_6_external_id]
        person_6.ethnicities = [person_6_ethnicity]
        person_6.supervising_officer = person_6_supervising_officer
        person_6_sentence_group_placeholder = (
            entities.StateSentenceGroup.new_with_defaults(
                state_code=_STATE_CODE,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                person=person_6,
            )
        )
        person_6_supervision_sentence_placeholder = (
            entities.StateSupervisionSentence.new_with_defaults(
                state_code=_STATE_CODE,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                sentence_group=person_6_sentence_group_placeholder,
                person=person_6,
            )
        )
        person_6_supervision_period_placeholder = (
            entities.StateSupervisionPeriod.new_with_defaults(
                state_code=_STATE_CODE,
                supervision_sentences=[person_6_supervision_sentence_placeholder],
                person=person_6,
            )
        )
        person_6_case_type = entities.StateSupervisionCaseTypeEntry.new_with_defaults(
            state_code=_STATE_CODE,
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            case_type_raw_text="-1",
            supervision_period=person_6_supervision_period_placeholder,
            person=person_6,
            external_id="92307",
        )
        person_6_supervision_period_placeholder.case_type_entries = [person_6_case_type]
        person_6_supervision_sentence_placeholder.supervision_periods = [
            person_6_supervision_period_placeholder
        ]
        person_6_sentence_group_placeholder.supervision_sentences = [
            person_6_supervision_sentence_placeholder
        ]
        person_6.sentence_groups = [person_6_sentence_group_placeholder]

        expected_people.append(person_6)

        person_7 = entities.StatePerson.new_with_defaults(
            full_name='{"given_names": "GRACE", "surname": "HOPPER"}',
            birthdate=datetime.date(year=1985, month=7, day=1),
            birthdate_inferred_from_age=False,
            current_address="111 8TH ST S #5, FARGO, ND, 58103",
            residency_status=ResidencyStatus.PERMANENT,
            gender=Gender.MALE,
            gender_raw_text="1",
            state_code=_STATE_CODE,
        )

        person_7_external_id = entities.StatePersonExternalId.new_with_defaults(
            external_id="40404",
            id_type=US_ND_SID,
            state_code=_STATE_CODE,
            person=person_7,
        )

        person_7_alias = entities.StatePersonAlias.new_with_defaults(
            full_name='{"given_names": "GRACE", "surname": "HOPPER"}',
            alias_type=StatePersonAliasType.GIVEN_NAME,
            alias_type_raw_text="GIVEN_NAME",
            state_code=_STATE_CODE,
            person=person_7,
        )

        person_7_ethnicity = entities.StatePersonEthnicity.new_with_defaults(
            state_code=_STATE_CODE,
            ethnicity=Ethnicity.HISPANIC,
            ethnicity_raw_text="HISPANIC",
            person=person_7,
        )

        person_7_sentence_group_placeholder = (
            entities.StateSentenceGroup.new_with_defaults(
                state_code=_STATE_CODE,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                person=person_7,
            )
        )
        person_7_supervision_sentence_placeholder = (
            entities.StateSupervisionSentence.new_with_defaults(
                state_code=_STATE_CODE,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                sentence_group=person_7_sentence_group_placeholder,
                person=person_7,
            )
        )
        person_7_supervision_period_placeholder = (
            entities.StateSupervisionPeriod.new_with_defaults(
                state_code=_STATE_CODE,
                supervision_sentences=[person_7_supervision_sentence_placeholder],
                person=person_7,
            )
        )

        person_7_supervising_officer = entities.StateAgent.new_with_defaults(
            agent_type=StateAgentType.SUPERVISION_OFFICER,
            agent_type_raw_text="SUPERVISION_OFFICER",
            external_id="22",
            state_code=_STATE_CODE,
        )

        person_7_case_type = entities.StateSupervisionCaseTypeEntry.new_with_defaults(
            state_code=_STATE_CODE,
            case_type=StateSupervisionCaseType.GENERAL,
            case_type_raw_text="0",
            supervision_period=person_7_supervision_period_placeholder,
            person=person_7,
            external_id="40404",
        )

        person_7.external_ids = [person_7_external_id]
        person_7.aliases = [person_7_alias]
        person_7.ethnicities = [person_7_ethnicity]
        person_7.supervising_officer = person_7_supervising_officer

        person_7_supervision_period_placeholder.case_type_entries = [person_7_case_type]
        person_7_supervision_sentence_placeholder.supervision_periods = [
            person_7_supervision_period_placeholder
        ]
        person_7_sentence_group_placeholder.supervision_sentences = [
            person_7_supervision_sentence_placeholder
        ]
        person_7.sentence_groups = [person_7_sentence_group_placeholder]

        expected_people.append(person_7)

        # Act
        self._run_ingest_job_for_filename("docstars_offenders.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # DOCSTARS OFFENDERCASES
        ######################################
        # Arrange
        agent_63 = entities.StateAgent.new_with_defaults(
            external_id="63",
            state_code=_STATE_CODE,
            agent_type=StateAgentType.SUPERVISION_OFFICER,
            agent_type_raw_text="SUPERVISION_OFFICER",
            full_name='{"given_names": "DAVID", "surname": "BORG"}',
        )
        agent_77 = entities.StateAgent.new_with_defaults(
            external_id="77",
            state_code=_STATE_CODE,
            agent_type=StateAgentType.SUPERVISION_OFFICER,
            agent_type_raw_text="SUPERVISION_OFFICER",
            full_name='{"given_names": "COREY", "surname": "KOLPIN"}',
        )
        agent_154 = entities.StateAgent.new_with_defaults(
            external_id="154",
            state_code=_STATE_CODE,
            agent_type=StateAgentType.SUPERVISION_OFFICER,
            agent_type_raw_text="SUPERVISION_OFFICER",
            full_name='{"given_names": "JOSEPH", "surname": "LUND"}',
        )
        sentence_group_placeholder_ss = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_1,
        )
        supervision_sentence_117109 = (
            entities.StateSupervisionSentence.new_with_defaults(
                external_id="117109",
                supervision_type=StateSupervisionType.PRE_CONFINEMENT,
                supervision_type_raw_text="PRE-TRIAL",
                start_date=datetime.date(year=2013, month=1, day=1),
                projected_completion_date=datetime.date(year=2013, month=3, day=3),
                completion_date=datetime.date(year=2013, month=2, day=2),
                max_length_days=59,
                state_code=_STATE_CODE,
                status=StateSentenceStatus.COMPLETED,
                person=sentence_group_placeholder_ss.person,
                sentence_group=sentence_group_placeholder_ss,
            )
        )
        supervision_period_117109 = entities.StateSupervisionPeriod.new_with_defaults(
            external_id="117109",
            start_date=datetime.date(year=2013, month=1, day=1),
            termination_date=datetime.date(year=2013, month=2, day=2),
            termination_reason=StateSupervisionPeriodTerminationReason.EXPIRATION,
            termination_reason_raw_text="4",
            supervising_officer=agent_63,
            supervision_type=StateSupervisionType.PRE_CONFINEMENT,
            supervision_type_raw_text="PRE-TRIAL",
            state_code=_STATE_CODE,
            county_code="US_ND_CASS",
            supervision_site="4",
            supervision_sentences=[supervision_sentence_117109],
            person=supervision_sentence_117109.person,
        )
        supervision_sentence_117109.supervision_periods.append(
            supervision_period_117109
        )
        sentence_group_placeholder_ss.supervision_sentences.append(
            supervision_sentence_117109
        )
        supervision_sentence_117110 = (
            entities.StateSupervisionSentence.new_with_defaults(
                external_id="117110",
                supervision_type=StateSupervisionType.PAROLE,
                supervision_type_raw_text="PAROLE",
                start_date=datetime.date(year=2014, month=7, day=17),
                projected_completion_date=datetime.date(year=2014, month=10, day=6),
                max_length_days=92,
                state_code=_STATE_CODE,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                person=sentence_group_placeholder_ss.person,
                sentence_group=sentence_group_placeholder_ss,
            )
        )
        supervision_period_117110 = entities.StateSupervisionPeriod.new_with_defaults(
            external_id="117110",
            start_date=datetime.date(year=2014, month=7, day=17),
            supervision_type=StateSupervisionType.PAROLE,
            supervision_type_raw_text="PAROLE",
            supervision_level=StateSupervisionLevel.EXTERNAL_UNKNOWN,
            supervision_level_raw_text="5",
            state_code=_STATE_CODE,
            county_code="US_ND_CASS",
            supervision_site="4",
            supervising_officer=agent_154,
            supervision_sentences=[supervision_sentence_117110],
            person=supervision_sentence_117110.person,
        )
        charge_117110 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[supervision_sentence_117110],
            person=supervision_sentence_117110.person,
        )
        agent_judge = entities.StateAgent.new_with_defaults(
            agent_type=StateAgentType.JUDGE,
            agent_type_raw_text="JUDGE",
            full_name='{"full_name": "THE JUDGE"}',
            state_code=_STATE_CODE,
        )
        court_case_117110 = entities.StateCourtCase.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            judge=agent_judge,
            charges=[charge_117110],
            person=charge_117110.person,
        )
        charge_117110.court_case = court_case_117110
        supervision_sentence_117110.charges = [charge_117110]
        supervision_sentence_117110.supervision_periods.append(
            supervision_period_117110
        )
        sentence_group_placeholder_ss.supervision_sentences.append(
            supervision_sentence_117110
        )

        supervision_sentence_117111 = (
            entities.StateSupervisionSentence.new_with_defaults(
                external_id="117111",
                supervision_type=StateSupervisionType.PAROLE,
                supervision_type_raw_text="PAROLE",
                start_date=datetime.date(year=2014, month=7, day=17),
                projected_completion_date=datetime.date(year=2015, month=8, day=7),
                completion_date=datetime.date(year=2014, month=12, day=8),
                max_length_days=580,
                state_code=_STATE_CODE,
                status=StateSentenceStatus.COMPLETED,
                person=sentence_group_placeholder_ss.person,
                sentence_group=sentence_group_placeholder_ss,
            )
        )
        charge_117111 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[supervision_sentence_117111],
            person=supervision_sentence_117111.person,
        )
        agent_judge_dup = attr.evolve(agent_judge)
        court_case_117111 = entities.StateCourtCase.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            judge=agent_judge_dup,
            charges=[charge_117111],
            person=charge_117111.person,
        )
        charge_117111.court_case = court_case_117111
        supervision_sentence_117111.charges = [charge_117111]

        supervision_period_117111 = entities.StateSupervisionPeriod.new_with_defaults(
            external_id="117111",
            start_date=datetime.date(year=2014, month=7, day=17),
            termination_date=datetime.date(year=2014, month=12, day=8),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            termination_reason_raw_text="9",
            supervision_type=StateSupervisionType.PAROLE,
            supervision_type_raw_text="PAROLE",
            state_code=_STATE_CODE,
            county_code="INVALID",
            supervision_site="4",
            supervising_officer=agent_63,
            supervision_sentences=[supervision_sentence_117111],
            person=supervision_sentence_117111.person,
        )
        supervision_violation_117111 = (
            entities.StateSupervisionViolation.new_with_defaults(
                state_code=_STATE_CODE,
                supervision_periods=[supervision_period_117111],
                person=supervision_period_117111.person,
            )
        )
        supervision_violation_type_entry_117111 = (
            entities.StateSupervisionViolationTypeEntry(
                state_code=_STATE_CODE,
                violation_type=StateSupervisionViolationType.ABSCONDED,
                violation_type_raw_text="ABSCONDED",
                person=supervision_period_117111.person,
                supervision_violation=supervision_violation_117111,
            )
        )
        supervision_violation_response_117111 = (
            entities.StateSupervisionViolationResponse.new_with_defaults(
                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
                response_type_raw_text="PERMANENT_DECISION",
                response_date=datetime.date(year=2014, month=12, day=8),
                state_code=_STATE_CODE,
                decision_agents=[agent_63],
                supervision_violation=supervision_violation_117111,
                person=supervision_violation_117111.person,
            )
        )
        supervision_violation_response_decision_entry_117111 = (
            entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=_STATE_CODE,
                decision=StateSupervisionViolationResponseDecision.REVOCATION,
                decision_raw_text="DOCR INMATE SENTENCE",
                person=supervision_violation_117111.person,
            )
        )

        supervision_violation_response_decision_entry_117111.supervision_violation_response = (
            supervision_violation_response_117111
        )
        supervision_violation_response_117111.supervision_violation_response_decisions = [
            supervision_violation_response_decision_entry_117111
        ]

        supervision_violation_117111.supervision_violation_responses.append(
            supervision_violation_response_117111
        )
        supervision_violation_117111.supervision_violation_types.append(
            supervision_violation_type_entry_117111
        )
        supervision_period_117111.supervision_violation_entries.append(
            supervision_violation_117111
        )
        supervision_sentence_117111.supervision_periods.append(
            supervision_period_117111
        )
        sentence_group_placeholder_ss.supervision_sentences.append(
            supervision_sentence_117111
        )
        person_1.sentence_groups.append(sentence_group_placeholder_ss)

        sentence_group_person_2_placeholder_ss = (
            entities.StateSentenceGroup.new_with_defaults(
                state_code=_STATE_CODE,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                person=person_2,
            )
        )

        supervision_sentence_140408 = (
            entities.StateSupervisionSentence.new_with_defaults(
                external_id="140408",
                supervision_type=StateSupervisionType.PROBATION,
                supervision_type_raw_text="SUSPENDED",
                start_date=datetime.date(year=2017, month=3, day=24),
                projected_completion_date=datetime.date(year=2019, month=3, day=23),
                completion_date=datetime.date(year=2018, month=2, day=27),
                state_code=_STATE_CODE,
                status=StateSentenceStatus.COMPLETED,
                person=sentence_group_person_2_placeholder_ss.person,
                sentence_group=sentence_group_person_2_placeholder_ss,
            )
        )
        supervision_period_140408 = entities.StateSupervisionPeriod.new_with_defaults(
            external_id="140408",
            supervision_type=StateSupervisionType.PROBATION,
            supervision_type_raw_text="SUSPENDED",
            start_date=datetime.date(year=2017, month=3, day=24),
            termination_date=datetime.date(year=2018, month=2, day=27),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            termination_reason_raw_text="9",
            state_code=_STATE_CODE,
            county_code="US_ND_GRIGGS",
            supervision_site="2",
            supervising_officer=agent_77,
            supervision_sentences=[supervision_sentence_140408],
            person=supervision_sentence_140408.person,
        )
        supervision_violation_140408 = (
            entities.StateSupervisionViolation.new_with_defaults(
                state_code=_STATE_CODE,
                supervision_periods=[supervision_period_140408],
                person=supervision_period_140408.person,
            )
        )
        supervision_violation_type_entry_140408 = (
            entities.StateSupervisionViolationTypeEntry(
                state_code=_STATE_CODE,
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_raw_text="TECHNICAL",
                person=supervision_period_140408.person,
                supervision_violation=supervision_violation_140408,
            )
        )
        supervision_violation_response_140408 = (
            entities.StateSupervisionViolationResponse.new_with_defaults(
                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
                response_type_raw_text="PERMANENT_DECISION",
                response_date=datetime.date(year=2018, month=10, day=27),
                state_code=_STATE_CODE,
                decision_agents=[agent_77],
                supervision_violation=supervision_violation_140408,
                person=supervision_violation_140408.person,
            )
        )
        supervision_violation_response_decision_entry_140408 = (
            entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=_STATE_CODE,
                decision=StateSupervisionViolationResponseDecision.REVOCATION,
                decision_raw_text="DOCR INMATE SENTENCE",
                person=supervision_violation_140408.person,
            )
        )

        supervision_violation_response_decision_entry_140408.supervision_violation_response = (
            supervision_violation_response_140408
        )

        supervision_violation_response_140408.supervision_violation_response_decisions = [
            supervision_violation_response_decision_entry_140408
        ]

        charge_140408 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[supervision_sentence_140408],
            person=supervision_sentence_140408.person,
        )
        agent_person = entities.StateAgent.new_with_defaults(
            agent_type=StateAgentType.JUDGE,
            agent_type_raw_text="JUDGE",
            full_name='{"full_name": "JUDGE PERSON"}',
            state_code=_STATE_CODE,
        )
        court_case_140408 = entities.StateCourtCase.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            judge=agent_person,
            charges=[charge_140408],
            person=charge_140408.person,
        )

        supervision_sentence_147777 = (
            entities.StateSupervisionSentence.new_with_defaults(
                external_id="147777",
                supervision_type=StateSupervisionType.PROBATION,
                supervision_type_raw_text="SUSPENDED",
                start_date=datetime.date(year=2013, month=3, day=24),
                projected_completion_date=datetime.date(year=2015, month=3, day=23),
                completion_date=datetime.date(year=2016, month=2, day=27),
                state_code=_STATE_CODE,
                status=StateSentenceStatus.COMPLETED,
                person=sentence_group_person_2_placeholder_ss.person,
                sentence_group=sentence_group_person_2_placeholder_ss,
            )
        )
        supervision_period_147777 = entities.StateSupervisionPeriod.new_with_defaults(
            external_id="147777",
            start_date=datetime.date(year=2013, month=3, day=24),
            termination_date=datetime.date(year=2016, month=2, day=27),
            supervision_type=StateSupervisionType.PROBATION,
            supervision_type_raw_text="SUSPENDED",
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            termination_reason_raw_text="9",
            state_code=_STATE_CODE,
            county_code="US_ND_GRIGGS",
            supervision_site="2",
            supervising_officer=agent_77,
            supervision_sentences=[supervision_sentence_147777],
            person=supervision_sentence_147777.person,
        )
        supervision_violation_147777 = (
            entities.StateSupervisionViolation.new_with_defaults(
                is_violent=True,
                state_code=_STATE_CODE,
                supervision_periods=[supervision_period_147777],
                person=supervision_period_147777.person,
            )
        )
        supervision_violation_type_entry_147777 = (
            entities.StateSupervisionViolationTypeEntry(
                state_code=_STATE_CODE,
                violation_type=StateSupervisionViolationType.LAW,
                violation_type_raw_text="LAW",
                person=supervision_period_147777.person,
                supervision_violation=supervision_violation_147777,
            )
        )
        supervision_violation_response_147777 = (
            entities.StateSupervisionViolationResponse.new_with_defaults(
                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
                response_type_raw_text="PERMANENT_DECISION",
                response_date=datetime.date(year=2016, month=2, day=27),
                decision_agents=[agent_77],
                state_code=_STATE_CODE,
                supervision_violation=supervision_violation_147777,
                person=supervision_violation_147777.person,
            )
        )
        supervision_violation_response_decision_entry_147777 = (
            entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=_STATE_CODE,
                decision=StateSupervisionViolationResponseDecision.REVOCATION,
                decision_raw_text="DOCR INMATE SENTENCE",
                person=supervision_violation_147777.person,
            )
        )

        supervision_violation_response_decision_entry_147777.supervision_violation_response = (
            supervision_violation_response_147777
        )

        supervision_violation_response_147777.supervision_violation_response_decisions = [
            supervision_violation_response_decision_entry_147777
        ]
        charge_147777 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[supervision_sentence_147777],
            person=supervision_sentence_147777.person,
        )
        agent_person = entities.StateAgent.new_with_defaults(
            agent_type=StateAgentType.JUDGE,
            agent_type_raw_text="JUDGE",
            full_name='{"full_name": "JUDGE PERSON"}',
            state_code=_STATE_CODE,
        )
        court_case_147777 = entities.StateCourtCase.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            judge=agent_person,
            charges=[charge_147777],
            person=charge_147777.person,
        )

        charge_140408.court_case = court_case_140408
        supervision_sentence_140408.charges = [charge_140408]
        supervision_violation_140408.supervision_violation_types.append(
            supervision_violation_type_entry_140408
        )
        supervision_violation_140408.supervision_violation_responses.append(
            supervision_violation_response_140408
        )
        supervision_period_140408.supervision_violation_entries.append(
            supervision_violation_140408
        )
        supervision_sentence_140408.supervision_periods.append(
            supervision_period_140408
        )
        sentence_group_person_2_placeholder_ss.supervision_sentences.append(
            supervision_sentence_140408
        )

        charge_147777.court_case = court_case_147777
        supervision_sentence_147777.charges = [charge_147777]
        supervision_violation_147777.supervision_violation_types.append(
            supervision_violation_type_entry_147777
        )
        supervision_violation_147777.supervision_violation_responses.append(
            supervision_violation_response_147777
        )
        supervision_period_147777.supervision_violation_entries.append(
            supervision_violation_147777
        )
        supervision_sentence_147777.supervision_periods.append(
            supervision_period_147777
        )
        sentence_group_person_2_placeholder_ss.supervision_sentences.append(
            supervision_sentence_147777
        )

        person_2.sentence_groups.append(sentence_group_person_2_placeholder_ss)

        # Act
        self._run_ingest_job_for_filename(
            "docstars_offendercasestable_with_officers.csv"
        )

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # DOCSTARS OFFENSES
        ######################################
        # Arrange
        charge_122553 = entities.StateCharge.new_with_defaults(
            external_id="122553",
            county_code="US_ND_BURLEIGH",
            ncic_code="3522",
            description="OPIUM OR DERIV-POSSESS",
            is_violent=False,
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="F",
            counts=1,
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[supervision_sentence_117111],
            person=supervision_sentence_117111.person,
        )
        charge_122554 = entities.StateCharge.new_with_defaults(
            external_id="122554",
            county_code="US_ND_BURLEIGH",
            ncic_code="3550",
            description="NARCOTIC EQUIP-POSSESS",
            is_violent=False,
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="F",
            counts=1,
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[supervision_sentence_117111],
            person=supervision_sentence_117111.person,
        )
        supervision_sentence_117111.charges.append(charge_122553)
        supervision_sentence_117111.charges.append(charge_122554)

        charge_122552 = entities.StateCharge.new_with_defaults(
            external_id="122552",
            county_code="US_ND_BURLEIGH",
            ncic_code="3562",
            description="MARIJUANA-POSSESS",
            is_violent=False,
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="F",
            counts=1,
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[supervision_sentence_117110],
            person=supervision_sentence_117110.person,
        )
        supervision_sentence_117110.charges.append(charge_122552)

        charge_149349 = entities.StateCharge.new_with_defaults(
            external_id="149349",
            county_code="US_ND_GRIGGS",
            ncic_code="2204",
            description="CRIMINAL TRESPASS",
            is_violent=False,
            offense_date=datetime.date(year=2016, month=7, day=30),
            classification_type=StateChargeClassificationType.MISDEMEANOR,
            classification_type_raw_text="M",
            counts=1,
            classification_subtype="A",
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[supervision_sentence_140408],
            person=supervision_sentence_140408.person,
        )
        supervision_sentence_140408.charges.append(charge_149349)

        # Act
        self._run_ingest_job_for_filename("docstars_offensestable.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # FTR EPISODES
        ######################################
        # Arrange
        metadata_1231 = json.dumps(
            {
                "PREFERRED_PROVIDER_ID": "49",
                "PREFERRED_LOCATION_ID": "126",
                "STRENGTHS": "WANTS LIFESTYLE CHANGE",
                "NEEDS": "SUBSTANCE ABUSE TREATMENT",
                "IS_CLINICAL_ASSESSMENT": "YES",
                "FUNCTIONAL_IMPAIRMENTS": "ADR,REC",
                "ASSESSMENT_LOCATION": "NDSP",
                "REFERRAL_REASON": "SUPPORT FOR DRUG USAGE",
                "SPECIALIST_FIRST_NAME": "",
                "SPECIALIST_LAST_NAME": "",
                "SPECIALIST_INITIAL": "",
                "SUBMITTED_BY": "USRNAME_1",
                "SUBMITTED_BY_NAME": "USER 1",
            }
        )
        metadata_2342 = json.dumps(
            {
                "PREFERRED_PROVIDER_ID": "51",
                "PREFERRED_LOCATION_ID": "128",
                "STRENGTHS": "",
                "NEEDS": "",
                "IS_CLINICAL_ASSESSMENT": "YES",
                "FUNCTIONAL_IMPAIRMENTS": "ATO",
                "ASSESSMENT_LOCATION": "LAKE REGION HUMAN SERVICE CENTER",
                "REFERRAL_REASON": "NEEDS SUPPORT FINDING EMPLOYMENT AND HOUSING",
                "SPECIALIST_FIRST_NAME": "",
                "SPECIALIST_LAST_NAME": "",
                "SPECIALIST_INITIAL": "",
                "SUBMITTED_BY": "USERNAME_2",
                "SUBMITTED_BY_NAME": "USER 2",
            }
        )
        metadata_3453 = json.dumps(
            {
                "PREFERRED_PROVIDER_ID": "49",
                "PREFERRED_LOCATION_ID": "126",
                "STRENGTHS": "WANTS TO TAKE ACTION",
                "NEEDS": "COMMUNITY SUPPORT",
                "IS_CLINICAL_ASSESSMENT": "YES",
                "FUNCTIONAL_IMPAIRMENTS": "EDE",
                "ASSESSMENT_LOCATION": "DWCRC",
                "REFERRAL_REASON": "ONGOING COMMUNITY SUPPORT",
                "SPECIALIST_FIRST_NAME": "",
                "SPECIALIST_LAST_NAME": "",
                "SPECIALIST_INITIAL": "",
                "SUBMITTED_BY": "USERNAME_3",
                "SUBMITTED_BY_NAME": "USER 3",
            }
        )
        metadata_4564 = json.dumps(
            {
                "PREFERRED_PROVIDER_ID": "20",
                "PREFERRED_LOCATION_ID": "",
                "STRENGTHS": "",
                "NEEDS": "",
                "IS_CLINICAL_ASSESSMENT": "NO",
                "FUNCTIONAL_IMPAIRMENTS": "",
                "ASSESSMENT_LOCATION": "",
                "REFERRAL_REASON": "MENTAL HEALTH CONCERNS",
                "SPECIALIST_FIRST_NAME": "",
                "SPECIALIST_LAST_NAME": "",
                "SPECIALIST_INITIAL": "",
                "SUBMITTED_BY": "USERNAME_4",
                "SUBMITTED_BY_NAME": "USER 4",
            }
        )
        program_assignment_1231 = entities.StateProgramAssignment.new_with_defaults(
            person=person_1,
            state_code="US_ND",
            external_id="1231",
            program_id="24",
            program_location_id="83",
            participation_status=StateProgramAssignmentParticipationStatus.DISCHARGED,
            participation_status_raw_text="DISCHARGED",
            referral_date=datetime.date(year=2018, month=1, day=2),
            start_date=datetime.date(year=2018, month=1, day=3),
            discharge_date=datetime.date(year=2018, month=1, day=28),
            referral_metadata=metadata_1231,
        )
        program_assignment_2342 = entities.StateProgramAssignment.new_with_defaults(
            person=person_1,
            state_code="US_ND",
            external_id="2342",
            participation_status=StateProgramAssignmentParticipationStatus.PENDING,
            participation_status_raw_text="SUBMITTED",
            referral_date=datetime.date(year=2019, month=7, day=20),
            referral_metadata=metadata_2342,
        )
        person_1.program_assignments = [
            program_assignment_1231,
            program_assignment_2342,
        ]
        program_assignment_3453 = entities.StateProgramAssignment.new_with_defaults(
            person=person_2,
            state_code="US_ND",
            external_id="3453",
            program_id="27",
            program_location_id="87",
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
            participation_status_raw_text="IN PROGRESS",
            referral_date=datetime.date(year=2019, month=8, day=12),
            start_date=datetime.date(year=2019, month=8, day=13),
            referral_metadata=metadata_3453,
        )
        program_assignment_4564 = entities.StateProgramAssignment.new_with_defaults(
            person=person_2,
            state_code="US_ND",
            external_id="4564",
            program_id="20",
            program_location_id="22",
            participation_status=StateProgramAssignmentParticipationStatus.DENIED,
            participation_status_raw_text="DENIED",
            referral_date=datetime.date(year=2019, month=7, day=12),
            referral_metadata=metadata_4564,
        )
        person_2.program_assignments = [
            program_assignment_3453,
            program_assignment_4564,
        ]

        # Act
        self._run_ingest_job_for_filename("docstars_ftr_episode.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # DOCSTARS LSI CHRONOLOGY
        ######################################
        # Arrange
        metadata_12345 = json.dumps(
            {
                "DOMAIN_CRIMINAL_HISTORY": 7,
                "DOMAIN_EDUCATION_EMPLOYMENT": 7,
                "DOMAIN_FINANCIAL": 1,
                "DOMAIN_FAMILY_MARITAL": 2,
                "DOMAIN_ACCOMMODATION": 0,
                "DOMAIN_LEISURE_RECREATION": 2,
                "DOMAIN_COMPANIONS": 3,
                "DOMAIN_ALCOHOL_DRUG_PROBLEMS": 5,
                "DOMAIN_EMOTIONAL_PERSONAL": 2,
                "DOMAIN_ATTITUDES_ORIENTATION": 1,
                "QUESTION_18": 0,
                "QUESTION_19": 0,
                "QUESTION_20": 0,
                "QUESTION_21": 1,
                "QUESTION_23": 3,
                "QUESTION_24": 1,
                "QUESTION_25": 1,
                "QUESTION_27": 3,
                "QUESTION_31": 1,
                "QUESTION_39": 2,
                "QUESTION_40": 1,
                "QUESTION_51": 2,
                "QUESTION_52": 3,
            }
        )

        metadata_12346 = json.dumps(
            {
                "DOMAIN_CRIMINAL_HISTORY": 6,
                "DOMAIN_EDUCATION_EMPLOYMENT": 8,
                "DOMAIN_FINANCIAL": 1,
                "DOMAIN_FAMILY_MARITAL": 2,
                "DOMAIN_ACCOMMODATION": 0,
                "DOMAIN_LEISURE_RECREATION": 2,
                "DOMAIN_COMPANIONS": 2,
                "DOMAIN_ALCOHOL_DRUG_PROBLEMS": 1,
                "DOMAIN_EMOTIONAL_PERSONAL": 0,
                "DOMAIN_ATTITUDES_ORIENTATION": 2,
                "QUESTION_18": 0,
                "QUESTION_19": 0,
                "QUESTION_20": 0,
                "QUESTION_21": 0,
                "QUESTION_23": 2,
                "QUESTION_24": 2,
                "QUESTION_25": 0,
                "QUESTION_27": 2,
                "QUESTION_31": 0,
                "QUESTION_39": 2,
                "QUESTION_40": 3,
                "QUESTION_51": 2,
                "QUESTION_52": 2,
            }
        )

        metadata_55555 = json.dumps(
            {
                "DOMAIN_CRIMINAL_HISTORY": 6,
                "DOMAIN_EDUCATION_EMPLOYMENT": 10,
                "DOMAIN_FINANCIAL": 2,
                "DOMAIN_FAMILY_MARITAL": 3,
                "DOMAIN_ACCOMMODATION": 0,
                "DOMAIN_LEISURE_RECREATION": 2,
                "DOMAIN_COMPANIONS": 2,
                "DOMAIN_ALCOHOL_DRUG_PROBLEMS": 8,
                "DOMAIN_EMOTIONAL_PERSONAL": 0,
                "DOMAIN_ATTITUDES_ORIENTATION": 0,
                "QUESTION_18": 0,
                "QUESTION_19": 0,
                "QUESTION_20": 0,
                "QUESTION_21": 0,
                "QUESTION_23": 2,
                "QUESTION_24": 0,
                "QUESTION_25": 0,
                "QUESTION_27": 2,
                "QUESTION_31": 1,
                "QUESTION_39": 1,
                "QUESTION_40": 0,
                "QUESTION_51": 3,
                "QUESTION_52": 3,
            }
        )

        assessment_12345 = entities.StateAssessment.new_with_defaults(
            external_id="12345",
            state_code="US_ND",
            assessment_class=StateAssessmentClass.RISK,
            assessment_class_raw_text="RISK",
            assessment_type=StateAssessmentType.LSIR,
            assessment_type_raw_text="LSIR",
            assessment_date=datetime.date(year=2016, month=7, day=14),
            assessment_score=30,
            assessment_metadata=metadata_12345,
            person=person_1,
        )
        assessment_12346 = entities.StateAssessment.new_with_defaults(
            external_id="12346",
            state_code="US_ND",
            assessment_class=StateAssessmentClass.RISK,
            assessment_class_raw_text="RISK",
            assessment_type=StateAssessmentType.LSIR,
            assessment_type_raw_text="LSIR",
            assessment_date=datetime.date(year=2017, month=1, day=13),
            assessment_score=24,
            assessment_metadata=metadata_12346,
            person=person_1,
        )
        person_1.assessments.append(assessment_12345)
        person_1.assessments.append(assessment_12346)

        assessment_55555 = entities.StateAssessment.new_with_defaults(
            external_id="55555",
            state_code="US_ND",
            assessment_class=StateAssessmentClass.RISK,
            assessment_class_raw_text="RISK",
            assessment_type=StateAssessmentType.LSIR,
            assessment_type_raw_text="LSIR",
            assessment_date=datetime.date(year=2018, month=12, day=10),
            assessment_score=33,
            assessment_metadata=metadata_55555,
            person=person_2,
        )
        person_2.assessments.append(assessment_55555)

        # Act
        self._run_ingest_job_for_filename("docstars_lsi_chronology.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # DOCSTARS CONTACTS
        ######################################

        # Arrange
        supervision_period_1231 = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            person=person_7,
        )

        person_7.supervising_officer.full_name = '{"full_name": "FIRSTNAME LASTNAME"}'

        supervision_contact_1231 = entities.StateSupervisionContact.new_with_defaults(
            external_id="1231",
            state_code=_STATE_CODE,
            contact_date=datetime.date(year=2020, month=4, day=15),
            contacted_agent=person_7_supervising_officer,
            contact_type=StateSupervisionContactType.DIRECT,
            contact_type_raw_text="OO",
            contact_method=StateSupervisionContactMethod.IN_PERSON,
            contact_method_raw_text="OO",
            location=StateSupervisionContactLocation.PLACE_OF_EMPLOYMENT,
            location_raw_text="OO",
            contact_reason=StateSupervisionContactReason.GENERAL_CONTACT,
            contact_reason_raw_text="SUPERVISION",
            person=person_7,
            supervision_periods=[supervision_period_1231],
            status=StateSupervisionContactStatus.COMPLETED,
            status_raw_text="OO",
        )
        supervision_contact_1232 = entities.StateSupervisionContact.new_with_defaults(
            external_id="1232",
            state_code=_STATE_CODE,
            contact_date=datetime.date(year=2020, month=4, day=16),
            contacted_agent=person_7_supervising_officer,
            contact_type=StateSupervisionContactType.DIRECT,
            contact_type_raw_text="OV-UA-FF",
            contact_method=StateSupervisionContactMethod.IN_PERSON,
            contact_method_raw_text="OV-UA-FF",
            location=StateSupervisionContactLocation.SUPERVISION_OFFICE,
            location_raw_text="OV-UA-FF",
            contact_reason=StateSupervisionContactReason.GENERAL_CONTACT,
            contact_reason_raw_text="SUPERVISION",
            person=person_7,
            supervision_periods=[supervision_period_1231],
            status=StateSupervisionContactStatus.COMPLETED,
            status_raw_text="OV-UA-FF",
        )
        supervision_contact_1233 = entities.StateSupervisionContact.new_with_defaults(
            external_id="1233",
            state_code=_STATE_CODE,
            contact_date=datetime.date(year=2020, month=4, day=17),
            contacted_agent=person_7_supervising_officer,
            contact_type=StateSupervisionContactType.COLLATERAL,
            contact_type_raw_text="HV-CC-AC-NS",
            contact_method=StateSupervisionContactMethod.IN_PERSON,
            contact_method_raw_text="HV-CC-AC-NS",
            location=StateSupervisionContactLocation.RESIDENCE,
            location_raw_text="HV-CC-AC-NS",
            contact_reason=StateSupervisionContactReason.GENERAL_CONTACT,
            contact_reason_raw_text="SUPERVISION",
            person=person_7,
            supervision_periods=[supervision_period_1231],
            status=StateSupervisionContactStatus.ATTEMPTED,
            status_raw_text="HV-CC-AC-NS",
        )

        sentence_group_1231 = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE,
            person=person_7,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_sentence_1231 = entities.StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE,
            person=person_7,
            supervision_periods=[supervision_period_1231],
            sentence_group=sentence_group_1231,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[],
        )

        supervision_period_1231.supervision_contacts = [
            supervision_contact_1231,
            supervision_contact_1232,
            supervision_contact_1233,
        ]
        supervision_period_1231.supervision_sentences = [supervision_sentence_1231]
        sentence_group_1231.supervision_sentences.append(supervision_sentence_1231)
        person_7.sentence_groups.append(sentence_group_1231)

        # Act
        self._run_ingest_job_for_filename("docstars_contacts_v2.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        # Rerun for sanity
        self._do_ingest_job_rerun_for_tags(self.controller.get_file_tag_rank_list())

        self.assert_expected_db_people(expected_people)

    def test_runOutOfOrder_mergeTwoDatabasePeople(self) -> None:
        """Tests that our system correctly handles the situation where we commit
        2 separate people into our DB, but after ingesting a later file, we
        realize that those 2 people should actually be merged into one person.
        This could happen in production either because we process files out of
        our desired order, or if all files aren't always in sync/fully up to
        date at the time we receive them.
        """

        ######################################
        # ELITE OFFENDERS
        ######################################
        # Arrange
        person_1 = entities.StatePerson.new_with_defaults(state_code=_STATE_CODE)
        person_1.full_name = '{"given_names": "JON", "surname": "HOPKINS"}'
        person_1.birthdate = datetime.date(year=1979, month=8, day=15)
        person_1.birthdate_inferred_from_age = False
        person_1.gender = Gender.MALE
        person_1.gender_raw_text = "M"
        person_1_race = entities.StatePersonRace.new_with_defaults(
            state_code=_STATE_CODE,
            race=Race.WHITE,
            race_raw_text="CAUCASIAN",
            person=person_1,
        )
        person_1_alias_1 = entities.StatePersonAlias.new_with_defaults(
            full_name='{"given_names": "JON", "surname": "HOPKINS"}',
            alias_type=StatePersonAliasType.GIVEN_NAME,
            alias_type_raw_text="GIVEN_NAME",
            state_code=_STATE_CODE,
            person=person_1,
        )
        person_1_external_id = entities.StatePersonExternalId.new_with_defaults(
            external_id="39768",
            id_type=US_ND_ELITE,
            state_code=_STATE_CODE,
            person=person_1,
        )
        person_1.races = [person_1_race]
        person_1.aliases = [person_1_alias_1]
        person_1.external_ids.append(person_1_external_id)

        person_2 = entities.StatePerson.new_with_defaults(state_code=_STATE_CODE)
        person_2_external_id = entities.StatePersonExternalId.new_with_defaults(
            external_id="52163",
            id_type=US_ND_ELITE,
            state_code=_STATE_CODE,
            person=person_2,
        )
        person_2.full_name = '{"given_names": "SOLANGE", "surname": "KNOWLES"}'
        person_2.birthdate = datetime.date(year=1986, month=6, day=24)
        person_2.birthdate_inferred_from_age = False
        person_2.gender = Gender.FEMALE
        person_2.gender_raw_text = "F"
        person_2_race = entities.StatePersonRace.new_with_defaults(
            state_code=_STATE_CODE,
            race=Race.BLACK,
            race_raw_text="BLACK",
            person=person_2,
        )
        person_2_alias_1 = entities.StatePersonAlias.new_with_defaults(
            full_name='{"given_names": "SOLANGE", "surname": "KNOWLES"}',
            alias_type=StatePersonAliasType.GIVEN_NAME,
            alias_type_raw_text="GIVEN_NAME",
            state_code=_STATE_CODE,
            person=person_2,
        )
        person_2.races = [person_2_race]
        person_2.aliases = [person_2_alias_1]
        person_2.external_ids.append(person_2_external_id)
        expected_people = [person_1, person_2]

        # Act
        self._run_ingest_job_for_filename("elite_offenders.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # DOCSTARS LSI CHRONOLOGY
        ######################################

        # Arrange
        person_1_dup = entities.StatePerson.new_with_defaults(state_code=_STATE_CODE)
        person_1_dup_external_id = entities.StatePersonExternalId.new_with_defaults(
            external_id="92237",
            id_type=US_ND_SID,
            state_code=_STATE_CODE,
            person=person_1_dup,
        )
        person_1_dup.external_ids.append(person_1_dup_external_id)
        metadata_12345 = json.dumps(
            {
                "DOMAIN_CRIMINAL_HISTORY": 7,
                "DOMAIN_EDUCATION_EMPLOYMENT": 7,
                "DOMAIN_FINANCIAL": 1,
                "DOMAIN_FAMILY_MARITAL": 2,
                "DOMAIN_ACCOMMODATION": 0,
                "DOMAIN_LEISURE_RECREATION": 2,
                "DOMAIN_COMPANIONS": 3,
                "DOMAIN_ALCOHOL_DRUG_PROBLEMS": 5,
                "DOMAIN_EMOTIONAL_PERSONAL": 2,
                "DOMAIN_ATTITUDES_ORIENTATION": 1,
                "QUESTION_18": 0,
                "QUESTION_19": 0,
                "QUESTION_20": 0,
                "QUESTION_21": 1,
                "QUESTION_23": 3,
                "QUESTION_24": 1,
                "QUESTION_25": 1,
                "QUESTION_27": 3,
                "QUESTION_31": 1,
                "QUESTION_39": 2,
                "QUESTION_40": 1,
                "QUESTION_51": 2,
                "QUESTION_52": 3,
            }
        )

        metadata_12346 = json.dumps(
            {
                "DOMAIN_CRIMINAL_HISTORY": 6,
                "DOMAIN_EDUCATION_EMPLOYMENT": 8,
                "DOMAIN_FINANCIAL": 1,
                "DOMAIN_FAMILY_MARITAL": 2,
                "DOMAIN_ACCOMMODATION": 0,
                "DOMAIN_LEISURE_RECREATION": 2,
                "DOMAIN_COMPANIONS": 2,
                "DOMAIN_ALCOHOL_DRUG_PROBLEMS": 1,
                "DOMAIN_EMOTIONAL_PERSONAL": 0,
                "DOMAIN_ATTITUDES_ORIENTATION": 2,
                "QUESTION_18": 0,
                "QUESTION_19": 0,
                "QUESTION_20": 0,
                "QUESTION_21": 0,
                "QUESTION_23": 2,
                "QUESTION_24": 2,
                "QUESTION_25": 0,
                "QUESTION_27": 2,
                "QUESTION_31": 0,
                "QUESTION_39": 2,
                "QUESTION_40": 3,
                "QUESTION_51": 2,
                "QUESTION_52": 2,
            }
        )

        metadata_55555 = json.dumps(
            {
                "DOMAIN_CRIMINAL_HISTORY": 6,
                "DOMAIN_EDUCATION_EMPLOYMENT": 10,
                "DOMAIN_FINANCIAL": 2,
                "DOMAIN_FAMILY_MARITAL": 3,
                "DOMAIN_ACCOMMODATION": 0,
                "DOMAIN_LEISURE_RECREATION": 2,
                "DOMAIN_COMPANIONS": 2,
                "DOMAIN_ALCOHOL_DRUG_PROBLEMS": 8,
                "DOMAIN_EMOTIONAL_PERSONAL": 0,
                "DOMAIN_ATTITUDES_ORIENTATION": 0,
                "QUESTION_18": 0,
                "QUESTION_19": 0,
                "QUESTION_20": 0,
                "QUESTION_21": 0,
                "QUESTION_23": 2,
                "QUESTION_24": 0,
                "QUESTION_25": 0,
                "QUESTION_27": 2,
                "QUESTION_31": 1,
                "QUESTION_39": 1,
                "QUESTION_40": 0,
                "QUESTION_51": 3,
                "QUESTION_52": 3,
            }
        )

        assessment_12345 = entities.StateAssessment.new_with_defaults(
            external_id="12345",
            state_code="US_ND",
            assessment_class=StateAssessmentClass.RISK,
            assessment_class_raw_text="RISK",
            assessment_type=StateAssessmentType.LSIR,
            assessment_type_raw_text="LSIR",
            assessment_date=datetime.date(year=2016, month=7, day=14),
            assessment_score=30,
            assessment_metadata=metadata_12345,
            person=person_1_dup,
        )
        assessment_12346 = entities.StateAssessment.new_with_defaults(
            external_id="12346",
            state_code="US_ND",
            assessment_class=StateAssessmentClass.RISK,
            assessment_class_raw_text="RISK",
            assessment_type=StateAssessmentType.LSIR,
            assessment_type_raw_text="LSIR",
            assessment_date=datetime.date(year=2017, month=1, day=13),
            assessment_score=24,
            assessment_metadata=metadata_12346,
            person=person_1_dup,
        )
        person_1_dup.assessments.append(assessment_12345)
        person_1_dup.assessments.append(assessment_12346)
        expected_people.append(person_1_dup)

        person_2_dup = entities.StatePerson.new_with_defaults(state_code=_STATE_CODE)
        person_2_dup_external_id_2 = entities.StatePersonExternalId.new_with_defaults(
            external_id="241896",
            id_type=US_ND_SID,
            state_code=_STATE_CODE,
            person=person_2_dup,
        )
        person_2_dup.external_ids.append(person_2_dup_external_id_2)
        assessment_55555 = entities.StateAssessment.new_with_defaults(
            external_id="55555",
            state_code="US_ND",
            assessment_class=StateAssessmentClass.RISK,
            assessment_class_raw_text="RISK",
            assessment_type=StateAssessmentType.LSIR,
            assessment_type_raw_text="LSIR",
            assessment_date=datetime.date(year=2018, month=12, day=10),
            assessment_score=33,
            assessment_metadata=metadata_55555,
            person=person_2_dup,
        )
        person_2_dup.assessments.append(assessment_55555)
        expected_people.append(person_2_dup)

        # Act
        self._run_ingest_job_for_filename("docstars_lsi_chronology.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # ELITE OFFENDER IDENTIFIERS
        ######################################
        # Arrange
        person_2_external_id_3 = entities.StatePersonExternalId.new_with_defaults(
            external_id="11111",
            id_type=US_ND_ELITE,
            state_code=_STATE_CODE,
            person=person_2,
        )
        person_2.external_ids.append(person_2_external_id_3)

        person_1_dup_external_id.person = person_1
        assessment_12345.person = person_1
        assessment_12346.person = person_1
        person_1.external_ids.append(person_1_dup_external_id)
        person_1.assessments.append(assessment_12345)
        person_1.assessments.append(assessment_12346)
        person_1_dup.assessments = []
        person_1_dup.external_ids = []

        person_2_dup_external_id_2.person = person_2
        assessment_55555.person = person_2
        person_2.external_ids.append(person_2_dup_external_id_2)
        person_2.assessments.append(assessment_55555)
        person_2_dup.external_ids = []
        person_2_dup.assessments = []

        person_3 = entities.StatePerson.new_with_defaults(state_code=_STATE_CODE)
        person_3_external_id_1 = entities.StatePersonExternalId.new_with_defaults(
            external_id="12345",
            id_type=US_ND_SID,
            state_code=_STATE_CODE,
            person=person_3,
        )
        person_3_external_id_2 = entities.StatePersonExternalId.new_with_defaults(
            external_id="92237",
            id_type=US_ND_ELITE,
            state_code=_STATE_CODE,
            person=person_3,
        )
        person_3.external_ids.append(person_3_external_id_1)
        person_3.external_ids.append(person_3_external_id_2)
        expected_people.append(person_3)

        # Act
        self._run_ingest_job_for_filename("elite_offenderidentifier.csv")

        # Assert
        self.assert_expected_db_people(expected_people)

        # Rerun for sanity
        self._do_ingest_job_rerun_for_tags(
            ["elite_offenders", "docstars_lsi_chronology", "elite_offenderidentifier"]
        )

        self.assert_expected_db_people(expected_people)
