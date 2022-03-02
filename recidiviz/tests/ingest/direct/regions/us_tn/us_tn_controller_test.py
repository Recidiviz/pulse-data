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
"""Unit and integration tests for US_TN direct ingest."""
import datetime
from typing import Type

from recidiviz.common.constants.shared_enums.charge import ChargeStatus
from recidiviz.common.constants.shared_enums.person_characteristics import (
    Ethnicity,
    Gender,
    Race,
)
from recidiviz.common.constants.state.external_id_types import US_TN_DOC
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_charge import StateChargeClassificationType
from recidiviz.common.constants.state.state_court_case import (
    StateCourtCaseStatus,
    StateCourtType,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactType,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.state.state_supervision_sentence import (
    StateSupervisionSentenceSupervisionType,
)
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.regions.us_tn.us_tn_controller import UsTnController
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.entity.state import entities
from recidiviz.tests.ingest.direct.regions.base_direct_ingest_controller_tests import (
    BaseDirectIngestControllerTests,
)
from recidiviz.tests.ingest.direct.regions.utils import (
    add_assessment_to_person,
    add_incarceration_period_to_person,
    add_incarceration_sentence_to_person,
    add_supervision_contact_to_person,
    add_supervision_period_to_person,
    add_supervision_sentence_to_person,
    build_state_person_entity,
)

_STATE_CODE_UPPER = "US_TN"


class TestUsTnController(BaseDirectIngestControllerTests):
    """Unit tests for each US_TN file to be ingested."""

    @classmethod
    def region_code(cls) -> str:
        return _STATE_CODE_UPPER.lower()

    @classmethod
    def controller_cls(cls) -> Type[BaseDirectIngestController]:
        return UsTnController

    @classmethod
    def schema_type(cls) -> SchemaType:
        return SchemaType.STATE

    def test_run_full_ingest_all_files_specific_order(self) -> None:
        ######################################
        # OffenderName
        ######################################
        # Arrange
        person_1 = build_state_person_entity(
            state_code=_STATE_CODE_UPPER,
            full_name='{"given_names": "FIRST1", "middle_names": "MIDDLE1", "name_suffix": "", "surname": "LAST1"}',
            gender=Gender.FEMALE,
            gender_raw_text="F",
            birthdate=datetime.date(year=1985, month=3, day=7),
            id_type=US_TN_DOC,
            external_id="00000001",
            race_raw_text="W",
            race=Race.WHITE,
            ethnicity_raw_text="NOT_HISPANIC",
            ethnicity=Ethnicity.NOT_HISPANIC,
        )

        person_2 = build_state_person_entity(
            state_code=_STATE_CODE_UPPER,
            full_name='{"given_names": "FIRST2", "middle_names": "MIDDLE2", "name_suffix": "", "surname": "LAST2"}',
            gender=Gender.MALE,
            gender_raw_text="M",
            birthdate=datetime.date(year=1969, month=2, day=1),
            external_id="00000002",
            id_type=US_TN_DOC,
            race_raw_text="B",
            race=Race.BLACK,
            ethnicity_raw_text="NOT_HISPANIC",
            ethnicity=Ethnicity.NOT_HISPANIC,
        )

        person_3 = build_state_person_entity(
            state_code=_STATE_CODE_UPPER,
            full_name='{"given_names": "FIRST3", "middle_names": "MIDDLE3", "name_suffix": "", "surname": "LAST3"}',
            gender=Gender.FEMALE,
            gender_raw_text="F",
            birthdate=datetime.date(year=1947, month=1, day=11),
            external_id="00000003",
            id_type=US_TN_DOC,
            race_raw_text="A",
            race=Race.ASIAN,
            ethnicity_raw_text="NOT_HISPANIC",
            ethnicity=Ethnicity.NOT_HISPANIC,
        )

        person_4 = build_state_person_entity(
            state_code=_STATE_CODE_UPPER,
            full_name='{"given_names": "FIRST4", "middle_names": "MIDDLE4", "name_suffix": "", "surname": "LAST4"}',
            gender=Gender.MALE,
            gender_raw_text="M",
            birthdate=datetime.date(year=1994, month=3, day=12),
            ethnicity_raw_text="HISPANIC",
            ethnicity=Ethnicity.HISPANIC,
            external_id="00000004",
            id_type=US_TN_DOC,
        )

        expected_people = [person_1, person_2, person_3, person_4]

        # Act
        self._run_ingest_job_for_filename("OffenderName")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # OffenderMovementIncarcerationPeriod
        ######################################

        add_incarceration_period_to_person(
            person=person_2,
            state_code=_STATE_CODE_UPPER,
            external_id="00000002-2",
            admission_date=datetime.date(year=2021, month=6, day=20),
            release_date=None,
            facility="088",
            custodial_authority=StateCustodialAuthority.COURT,
            custodial_authority_raw_text="JA",
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="CTFA-NEWAD",
            release_reason=None,
            release_reason_raw_text=None,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="CTFA-NEWAD",
        )

        # Person 3 moves from parole to facility.
        add_incarceration_period_to_person(
            person=person_3,
            state_code=_STATE_CODE_UPPER,
            external_id="00000003-1",
            admission_date=datetime.date(year=2010, month=2, day=5),
            release_date=datetime.date(year=2010, month=2, day=26),
            facility="79A",
            custodial_authority=StateCustodialAuthority.COURT,
            custodial_authority_raw_text="JA",
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text="PAFA-VIOLW",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="FAFA-JAILT",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="PAFA-VIOLW",
        )
        # Person 3 transfers facilities.
        add_incarceration_period_to_person(
            person=person_3,
            state_code=_STATE_CODE_UPPER,
            external_id="00000003-2",
            admission_date=datetime.date(year=2010, month=2, day=26),
            release_date=datetime.date(year=2010, month=4, day=6),
            facility="WTSP",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="IN",
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="FAFA-JAILT",
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            release_reason_raw_text="PAFA-PAVOK",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="FAFA-JAILT",
        )
        # Person 3 is released to supervision.
        add_incarceration_period_to_person(
            person=person_3,
            state_code=_STATE_CODE_UPPER,
            external_id="00000003-3",
            admission_date=datetime.date(year=2010, month=4, day=6),
            release_date=datetime.date(year=2010, month=11, day=4),
            facility="WTSP",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="IN",
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="PAFA-PAVOK",
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_TO_SUPERVISION,
            release_reason_raw_text="FAPA-RELEL",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="PAFA-PAVOK",
        )

        expected_people = [person_1, person_2, person_3, person_4]

        # Act
        self._run_ingest_job_for_filename("OffenderMovementIncarcerationPeriod")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # AssignedStaffSupervisionPeriod
        ######################################

        shared_supervising_officer: entities.StateAgent = entities.StateAgent.new_with_defaults(
            external_id="ABCDEF01",
            agent_type=StateAgentType.SUPERVISION_OFFICER,
            full_name='{"given_names": "DALE", "middle_names": "", "name_suffix": "JR.", "surname": "COOPER"}',
            state_code=_STATE_CODE_UPPER,
        )

        add_supervision_period_to_person(
            person=person_2,
            state_code=_STATE_CODE_UPPER,
            external_id="00000002-1",
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_type_raw_text="PRO",
            start_date=datetime.date(year=2015, month=7, day=13),
            termination_date=datetime.date(year=2015, month=11, day=9),
            supervision_site="P39F",
            supervising_officer=shared_supervising_officer,
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            admission_reason_raw_text="NEWCS",
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            termination_reason_raw_text="RNO",
        )

        add_supervision_period_to_person(
            person=person_2,
            state_code=_STATE_CODE_UPPER,
            external_id="00000002-2",
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_type_raw_text="PRO",
            start_date=datetime.date(year=2015, month=11, day=9),
            termination_date=datetime.date(year=2016, month=10, day=10),
            supervision_site="SDR",
            supervising_officer=shared_supervising_officer,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            admission_reason_raw_text="TRANS",
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            termination_reason_raw_text="DIS",
        )

        add_supervision_period_to_person(
            person=person_3,
            state_code=_STATE_CODE_UPPER,
            external_id="00000003-1",
            supervision_type=StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT,
            supervision_type_raw_text="CCC",
            start_date=datetime.date(year=2011, month=1, day=26),
            termination_date=datetime.date(year=2011, month=2, day=8),
            supervision_site="PDR",
            supervising_officer=shared_supervising_officer,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            admission_reason_raw_text="MULRE",
            termination_reason=StateSupervisionPeriodTerminationReason.EXPIRATION,
            termination_reason_raw_text="EXP",
        )

        add_supervision_period_to_person(
            person=person_3,
            state_code=_STATE_CODE_UPPER,
            external_id="00000003-2",
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_type_raw_text="PAO",
            start_date=datetime.date(year=2017, month=7, day=22),
            termination_date=None,
            supervision_site="SDR",
            supervising_officer=shared_supervising_officer,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            admission_reason_raw_text="TRPRB",
            termination_reason=None,
            termination_reason_raw_text=None,
        )

        # Only person 2 and 3 have supervision periods.
        expected_people = [person_1, person_2, person_3, person_4]

        # Act
        self._run_ingest_job_for_filename("AssignedStaffSupervisionPeriod")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # VantagePointAssessments
        ######################################

        add_assessment_to_person(
            person=person_2,
            state_code=_STATE_CODE_UPPER,
            external_id="00000002-201",
            assessment_class=StateAssessmentClass.RISK,
            assessment_type=StateAssessmentType.STRONG_R,
            assessment_date=datetime.date(year=2015, month=8, day=14),
            assessment_score=None,
            assessment_level=StateAssessmentLevel.LOW,
            assessment_level_raw_text="LOW",
            assessment_metadata='{"AGGRESSION_NEED_LEVEL": "LOW", "ALCOHOL_DRUG_NEED_LEVEL": "LOW", "ATTITUDE_BEHAVIOR_NEED_LEVEL": "LOW", "EDUCATION_NEED_LEVEL": "MOD", "EMPLOYMENT_NEED_LEVEL": "LOW", "FAMILY_NEED_LEVEL": "HIGH", "FRIENDS_NEED_LEVEL": "HIGH", "MENTAL_HEALTH_NEED_LEVEL": "MOD", "RESIDENT_NEED_LEVEL": "LOW"}',
            conducting_agent=shared_supervising_officer,
        )

        add_assessment_to_person(
            person=person_2,
            state_code=_STATE_CODE_UPPER,
            external_id="00000002-202",
            assessment_class=StateAssessmentClass.RISK,
            assessment_type=StateAssessmentType.STRONG_R,
            assessment_date=datetime.date(year=2015, month=10, day=14),
            assessment_score=None,
            assessment_level=StateAssessmentLevel.LOW,
            assessment_level_raw_text="LOW",
            assessment_metadata='{"AGGRESSION_NEED_LEVEL": "LOW", "ALCOHOL_DRUG_NEED_LEVEL": "LOW", "ATTITUDE_BEHAVIOR_NEED_LEVEL": "LOW", "EDUCATION_NEED_LEVEL": "MOD", "EMPLOYMENT_NEED_LEVEL": "LOW", "FAMILY_NEED_LEVEL": "MOD", "FRIENDS_NEED_LEVEL": "MOD", "MENTAL_HEALTH_NEED_LEVEL": "LOW", "RESIDENT_NEED_LEVEL": "LOW"}',
            conducting_agent=shared_supervising_officer,
        )

        # Act
        self._run_ingest_job_for_filename("VantagePointAssessments")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # SentencesChargesAndCourtCases
        ######################################

        person_2_incarceration_sentence = add_incarceration_sentence_to_person(
            person=person_2,
            external_id="00000002-088-2021-S51773-13",
            state_code="US_TN",
            status=StateSentenceStatus.SUSPENDED,
            status_raw_text="RLSD-AC",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="TD",
            date_imposed=datetime.date(2021, 5, 25),
            start_date=datetime.date(2008, 8, 12),
            projected_max_release_date=datetime.date(2021, 6, 1),
            completion_date=datetime.date(2026, 11, 7),
            county_code="088",
            max_length_days=730,
            initial_time_served_days=0,
            conditions="[{'NOTE_UPDATE_DATE': '2003-10-27T00:00:00', 'CONDITIONS_ON_DATE': 'DEFENDANT TO SERVE 60 DAYS JAIL TIME IN THE, COUNTY JAIL NO J/C LISTED'}]",
        )

        person_2_charge = entities.StateCharge(
            external_id="00000002-088-2021-S51773-13",
            state_code="US_TN",
            status=ChargeStatus.SENTENCED,
            offense_date=datetime.date(2021, 3, 15),
            date_charged=datetime.date(2021, 5, 24),
            county_code="088",
            statute="3371",
            description="SOL KIDNAPPING",
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="F",
            classification_subtype="E",
            is_violent=True,
            is_sex_offense=False,
            charging_entity="J",
            person=person_2,
            incarceration_sentences=[person_2_incarceration_sentence],
        )

        person_2_charge_court_case = entities.StateCourtCase(
            external_id="00000002-088-2021-S51773-13",
            state_code="US_TN",
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            date_convicted=datetime.date(2021, 5, 25),
            county_code="088",
            judicial_district_code="007",
            person=person_2,
            charges=[person_2_charge],
            judge=entities.StateAgent(
                agent_type=StateAgentType.JUDGE,
                state_code="US_TN",
                full_name='{"full_name": "BOB ROSS"}',
            ),
        )

        person_2_charge.court_case = person_2_charge_court_case
        person_2_incarceration_sentence.charges = [person_2_charge]

        person_3_supervision_sentence_1 = add_supervision_sentence_to_person(
            person=person_3,
            external_id="00000003-013-2011-9577-0",
            state_code="US_TN",
            status=StateSentenceStatus.SERVING,
            status_raw_text="NONE-PB",
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            supervision_type_raw_text="PB-S-TD",
            date_imposed=datetime.date(2011, 7, 5),
            start_date=datetime.date(2011, 7, 5),
            projected_completion_date=datetime.date(2015, 7, 5),
            completion_date=datetime.date(2015, 7, 5),
            min_length_days=100,
            county_code="013",
        )

        person_3_charge_1 = entities.StateCharge(
            external_id="00000003-013-2011-9577-0",
            state_code="US_TN",
            status=ChargeStatus.SENTENCED,
            offense_date=datetime.date(2011, 6, 14),
            date_charged=datetime.date(2011, 7, 3),
            county_code="013",
            statute="8039",
            description="DUI, 4TH OFFENSE & SUBSEQUENT",
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="F",
            classification_subtype="E",
            is_violent=False,
            is_sex_offense=False,
            charging_entity="B",
            person=person_3,
            supervision_sentences=[person_3_supervision_sentence_1],
        )

        person_3_charge_1_court_case = entities.StateCourtCase(
            external_id="00000003-013-2011-9577-0",
            state_code="US_TN",
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            date_convicted=datetime.date(2011, 7, 5),
            county_code="013",
            judicial_district_code="006",
            person=person_3,
            charges=[person_3_charge_1],
            judge=entities.StateAgent(
                agent_type=StateAgentType.JUDGE,
                state_code="US_TN",
                full_name='{"full_name": "ROSS"}',
            ),
        )

        person_3_charge_1.court_case = person_3_charge_1_court_case
        person_3_supervision_sentence_1.charges = [person_3_charge_1]

        person_3_supervision_sentence_2 = add_supervision_sentence_to_person(
            person=person_3,
            external_id="00000003-013-2011-9577-1",
            state_code="US_TN",
            status=StateSentenceStatus.SERVING,
            status_raw_text="VOJO-CC",
            supervision_type=StateSupervisionSentenceSupervisionType.COMMUNITY_CORRECTIONS,
            supervision_type_raw_text="CC-NONE-TD",
            date_imposed=datetime.date(2015, 7, 5),
            start_date=datetime.date(2015, 7, 5),
            projected_completion_date=datetime.date(2020, 7, 5),
            completion_date=datetime.date(2020, 11, 5),
            county_code="013",
            sentence_metadata='{"CONSECUTIVE_SENTENCE_ID": "00000003-013-2011-9577-0"}',
        )

        person_3_charge_2 = entities.StateCharge(
            external_id="00000003-013-2011-9577-1",
            state_code="US_TN",
            status=ChargeStatus.SENTENCED,
            offense_date=datetime.date(2015, 5, 27),
            date_charged=datetime.date(2015, 7, 1),
            county_code="013",
            statute="3699",
            description="ATTEMPT TO COMMIT FELONY/SEX OFFENSE",
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="F",
            is_violent=True,
            is_sex_offense=True,
            charging_entity="B",
            person=person_3,
            supervision_sentences=[person_3_supervision_sentence_2],
        )

        person_3_charge_2_court_case = entities.StateCourtCase(
            external_id="00000003-013-2011-9577-1",
            state_code="US_TN",
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            date_convicted=datetime.date(2015, 7, 5),
            county_code="013",
            judicial_district_code="007",
            person=person_3,
            charges=[person_3_charge_2],
            judge=entities.StateAgent(
                agent_type=StateAgentType.JUDGE,
                state_code="US_TN",
                full_name='{"full_name": "BOB ROSS"}',
            ),
        )

        person_3_charge_2.court_case = person_3_charge_2_court_case
        person_3_supervision_sentence_2.charges = [person_3_charge_2]

        # Only person 2 and 3 have supervision and/or incarceration sentences
        expected_people = [person_1, person_2, person_3, person_4]

        # Act
        self._run_ingest_job_for_filename("SentencesChargesAndCourtCases")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # SupervisionContacts
        ######################################

        add_supervision_contact_to_person(
            person=person_3,
            external_id="00000003-1",
            state_code="US_TN",
            contact_date=datetime.date(1996, 4, 8),
            contact_type=StateSupervisionContactType.EXTERNAL_UNKNOWN,
            contact_type_raw_text="JOIC",
        )

        add_supervision_contact_to_person(
            person=person_3,
            external_id="00000003-2",
            state_code="US_TN",
            contact_date=datetime.date(2011, 7, 29),
            contact_type=StateSupervisionContactType.EXTERNAL_UNKNOWN,
            contact_type_raw_text="ITSS",
            contacted_agent=shared_supervising_officer,
        )

        add_supervision_contact_to_person(
            person=person_2,
            external_id="00000002-1",
            state_code="US_TN",
            contact_date=datetime.date(2015, 5, 14),
            contact_type=StateSupervisionContactType.EXTERNAL_UNKNOWN,
            contact_type_raw_text="JOIC",
        )

        # Only person 2 and 3 have supervision contacts
        expected_people = [person_1, person_2, person_3, person_4]

        # Act
        self._run_ingest_job_for_filename("SupervisionContactsPre1990")
        self._run_ingest_job_for_filename("SupervisionContacts1990to1995")
        self._run_ingest_job_for_filename("SupervisionContacts1995to1997")
        self._run_ingest_job_for_filename("SupervisionContacts1997to2000")
        self._run_ingest_job_for_filename("SupervisionContacts2000to2003")
        self._run_ingest_job_for_filename("SupervisionContacts2003to2005")
        self._run_ingest_job_for_filename("SupervisionContacts2005to2007")
        self._run_ingest_job_for_filename("SupervisionContacts2007to2010")
        self._run_ingest_job_for_filename("SupervisionContacts2010to2013")
        self._run_ingest_job_for_filename("SupervisionContacts2013to2015")
        self._run_ingest_job_for_filename("SupervisionContacts2015to2017")
        self._run_ingest_job_for_filename("SupervisionContacts2017to2020")
        self._run_ingest_job_for_filename("SupervisionContactsPost2020")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # FULL RERUN FOR IDEMPOTENCE
        ######################################

        self._do_ingest_job_rerun_for_tags(self.controller.get_file_tag_rank_list())

        self.assert_expected_db_people(expected_people)
