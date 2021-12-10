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
"""Ingest view parser tests for US_TN direct ingest."""
import csv
import datetime
import unittest

from recidiviz.cloud_storage.gcs_file_system import GcsfsFileContentsHandle
from recidiviz.common.constants.person_characteristics import Ethnicity, Gender, Race
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.entity.state.entities import (
    StateAgent,
    StateIncarcerationPeriod,
    StatePerson,
    StatePersonEthnicity,
    StatePersonExternalId,
    StatePersonRace,
    StateSupervisionPeriod,
)
from recidiviz.tests.ingest.direct.fixture_util import direct_ingest_fixture_path
from recidiviz.tests.ingest.direct.regions.state_ingest_view_parser_test_base import (
    StateIngestViewParserTestBase,
)


class UsTnIngestViewParserTest(StateIngestViewParserTestBase, unittest.TestCase):
    """Parser unit tests for each US_TN ingest view file to be ingested."""

    @classmethod
    def schema_type(cls) -> SchemaType:
        return SchemaType.STATE

    @classmethod
    def region_code(cls) -> str:
        return "US_TN"

    @property
    def test(self) -> unittest.TestCase:
        return self

    def test_parse_OffenderName(self) -> None:
        expected_output = [
            StatePerson(
                state_code="US_TN",
                full_name='{"given_names": "FIRST1", "middle_names": "MIDDLE1", "name_suffix": "", "surname": "LAST1"}',
                birthdate=datetime.date(1985, 3, 7),
                gender=Gender.FEMALE,
                gender_raw_text="F",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_TN", external_id="00000001", id_type="US_TN_DOC"
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code="US_TN", race=Race.WHITE, race_raw_text="W"
                    )
                ],
                ethnicities=[
                    StatePersonEthnicity(
                        state_code="US_TN",
                        ethnicity=Ethnicity.NOT_HISPANIC,
                        ethnicity_raw_text="NOT_HISPANIC",
                    )
                ],
            ),
            StatePerson(
                state_code="US_TN",
                full_name='{"given_names": "FIRST2", "middle_names": "MIDDLE2", "name_suffix": "", "surname": "LAST2"}',
                birthdate=datetime.date(1969, 2, 1),
                gender=Gender.MALE,
                gender_raw_text="M",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_TN", external_id="00000002", id_type="US_TN_DOC"
                    )
                ],
                aliases=[],
                races=[
                    StatePersonRace(
                        state_code="US_TN", race=Race.BLACK, race_raw_text="B"
                    )
                ],
                ethnicities=[
                    StatePersonEthnicity(
                        state_code="US_TN",
                        ethnicity=Ethnicity.NOT_HISPANIC,
                        ethnicity_raw_text="NOT_HISPANIC",
                    )
                ],
            ),
            StatePerson(
                state_code="US_TN",
                full_name='{"given_names": "FIRST3", "middle_names": "MIDDLE3", "name_suffix": "", "surname": "LAST3"}',
                birthdate=datetime.date(1947, 1, 11),
                gender=Gender.FEMALE,
                gender_raw_text="F",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_TN", external_id="00000003", id_type="US_TN_DOC"
                    )
                ],
                aliases=[],
                races=[
                    StatePersonRace(
                        state_code="US_TN", race=Race.ASIAN, race_raw_text="A"
                    )
                ],
                ethnicities=[
                    StatePersonEthnicity(
                        state_code="US_TN",
                        ethnicity=Ethnicity.NOT_HISPANIC,
                        ethnicity_raw_text="NOT_HISPANIC",
                    )
                ],
            ),
            StatePerson(
                state_code="US_TN",
                full_name='{"given_names": "FIRST4", "middle_names": "MIDDLE4", "name_suffix": "", "surname": "LAST4"}',
                birthdate=datetime.date(1994, 3, 12),
                gender=Gender.MALE,
                gender_raw_text="M",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_TN", external_id="00000004", id_type="US_TN_DOC"
                    )
                ],
                ethnicities=[
                    StatePersonEthnicity(
                        state_code="US_TN",
                        ethnicity=Ethnicity.HISPANIC,
                        ethnicity_raw_text="HISPANIC",
                    )
                ],
            ),
        ]

        self._run_parse_ingest_view_test("OffenderName", expected_output)

    def test_parse_OffenderMovementIncarcerationPeriod(self) -> None:
        expected_output = [
            # Person 2 only has one movement.
            StatePerson(
                state_code="US_TN",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_TN", external_id="00000002", id_type="US_TN_DOC"
                    )
                ],
                incarceration_periods=[
                    StateIncarcerationPeriod(
                        external_id="00000002-2",
                        state_code="US_TN",
                        incarceration_type=StateIncarcerationType.STATE_PRISON,
                        incarceration_type_raw_text=None,
                        admission_date=datetime.date(2021, 6, 20),
                        release_date=None,
                        county_code=None,
                        facility="088",
                        custodial_authority=StateCustodialAuthority.COURT,
                        custodial_authority_raw_text="JA",
                        admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                        admission_reason_raw_text="CTFA-NEWAD",
                        release_reason=None,
                        release_reason_raw_text="NONE-NONE",
                    )
                ],
            ),
            # Person 3 moves from parole to facility.
            StatePerson(
                state_code="US_TN",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_TN", external_id="00000003", id_type="US_TN_DOC"
                    )
                ],
                incarceration_periods=[
                    StateIncarcerationPeriod(
                        external_id="00000003-1",
                        state_code="US_TN",
                        incarceration_type=StateIncarcerationType.STATE_PRISON,
                        admission_date=datetime.date(2010, 2, 5),
                        release_date=datetime.date(2010, 2, 26),
                        county_code=None,
                        facility="79A",
                        custodial_authority=StateCustodialAuthority.COURT,
                        custodial_authority_raw_text="JA",
                        admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
                        admission_reason_raw_text="PAFA-VIOLW",
                        release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
                        release_reason_raw_text="FAFA-JAILT",
                    )
                ],
            ),
            # Person 3 transfers facilities.
            StatePerson(
                state_code="US_TN",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_TN", external_id="00000003", id_type="US_TN_DOC"
                    )
                ],
                incarceration_periods=[
                    StateIncarcerationPeriod(
                        external_id="00000003-2",
                        state_code="US_TN",
                        incarceration_type=StateIncarcerationType.STATE_PRISON,
                        admission_date=datetime.date(2010, 2, 26),
                        release_date=datetime.date(2010, 4, 6),
                        county_code=None,
                        facility="WTSP",
                        custodial_authority=StateCustodialAuthority.STATE_PRISON,
                        custodial_authority_raw_text="IN",
                        admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
                        admission_reason_raw_text="FAFA-JAILT",
                        release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
                        release_reason_raw_text="PAFA-PAVOK",
                    )
                ],
            ),
            # Person 3 is released to supervision.
            StatePerson(
                state_code="US_TN",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_TN", external_id="00000003", id_type="US_TN_DOC"
                    )
                ],
                incarceration_periods=[
                    StateIncarcerationPeriod(
                        external_id="00000003-3",
                        state_code="US_TN",
                        incarceration_type=StateIncarcerationType.STATE_PRISON,
                        admission_date=datetime.date(2010, 4, 6),
                        release_date=datetime.date(2010, 11, 4),
                        county_code=None,
                        facility="WTSP",
                        custodial_authority=StateCustodialAuthority.STATE_PRISON,
                        custodial_authority_raw_text="IN",
                        admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
                        admission_reason_raw_text="PAFA-PAVOK",
                        release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_TO_SUPERVISION,
                        release_reason_raw_text="FAPA-RELEL",
                    )
                ],
            ),
        ]

        self._run_parse_ingest_view_test(
            "OffenderMovementIncarcerationPeriod", expected_output
        )

    def test_parse_OffenderMovementIncarcerationPeriod_AdmissionReasons(self) -> None:
        manifest_ast = self._parse_manifest("OffenderMovementIncarcerationPeriod")
        enum_parser_manifest = (
            # Drill down to get admission reasons.
            manifest_ast.field_manifests["incarceration_periods"]
            .child_manifests[0]  # type: ignore[attr-defined]
            .field_manifests["admission_reason"]
        )
        self._parse_enum_manifest_test(
            "OffenderMovementIncarcerationPeriod_AdmissionReasons", enum_parser_manifest
        )

    def test_parse_OffenderMovementIncarcerationPeriod_ReleaseReasons(self) -> None:
        manifest_ast = self._parse_manifest("OffenderMovementIncarcerationPeriod")
        enum_parser_manifest = (
            # Drill down to get release reasons.
            manifest_ast.field_manifests["incarceration_periods"]
            .child_manifests[0]  # type: ignore[attr-defined]
            .field_manifests["release_reason"]
        )

        # The values in the ReleaseReasons are all possible (StartMovementType,StartMovementReason)
        # and (EndMovementType,EndMovementReason) combinations.
        self._parse_enum_manifest_test(
            "OffenderMovementIncarcerationPeriod_ReleaseReasons", enum_parser_manifest
        )

    def test_parse_OffenderMovementIncarcerationPeriod_AdmissionReasonIsSubsetOfReleaseReason(
        self,
    ) -> None:
        admission_reason_file = "OffenderMovementIncarcerationPeriod_AdmissionReasons"
        release_reason_file = "OffenderMovementIncarcerationPeriod_ReleaseReasons"

        admissions_fixture_path = direct_ingest_fixture_path(
            region_code=self.region_code(),
            file_name=f"{admission_reason_file}.csv",
        )

        releases_fixture_path = direct_ingest_fixture_path(
            region_code=self.region_code(),
            file_name=f"{release_reason_file}.csv",
        )

        admissions_contents_handle = GcsfsFileContentsHandle(
            admissions_fixture_path, cleanup_file=False
        )
        admission_reasons = []
        for row in csv.DictReader(admissions_contents_handle.get_contents_iterator()):
            admission_reason: str = (
                row["StartMovementType"] + "-" + row["StartMovementReason"]
            )
            admission_reasons.append(admission_reason)

        releases_contents_handle = GcsfsFileContentsHandle(
            releases_fixture_path, cleanup_file=False
        )
        release_reasons = []
        for row in csv.DictReader(releases_contents_handle.get_contents_iterator()):
            release_reason: str = (
                row["EndMovementType"] + "-" + row["EndMovementReason"]
            )
            release_reasons.append(release_reason)

        # Assert that every pair of MovementType and MovementReason present in the list of Admissions reasons
        # is also present in the list of Release reasons. This is to confirm that we properly handle all potential
        # (valid or invalid) release reasons.
        self.assertTrue(set(admission_reasons) <= set(release_reasons))

    def test_parse_AssignedStaffSupervisionPeriod(self) -> None:
        expected_output = [
            # Person 2 starts a new supervision period.
            StatePerson(
                state_code="US_TN",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_TN", external_id="00000002", id_type="US_TN_DOC"
                    )
                ],
                supervision_periods=[
                    StateSupervisionPeriod(
                        external_id="00000002-1",
                        state_code="US_TN",
                        start_date=datetime.date(2015, 7, 13),
                        termination_date=datetime.date(2015, 11, 9),
                        supervision_site="P39F",
                        supervising_officer=StateAgent(
                            external_id="ABCDEF01",
                            agent_type=StateAgentType.SUPERVISION_OFFICER,
                            state_code="US_TN",
                        ),
                        admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                        admission_reason_raw_text="NEWCS",
                        termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
                        termination_reason_raw_text="RNO",
                        supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                        supervision_type_raw_text="PRO",
                    )
                ],
            ),
            # Person 2 moves from one facility to another, and then is discharged.
            StatePerson(
                state_code="US_TN",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_TN", external_id="00000002", id_type="US_TN_DOC"
                    )
                ],
                supervision_periods=[
                    StateSupervisionPeriod(
                        external_id="00000002-2",
                        state_code="US_TN",
                        start_date=datetime.date(2015, 11, 9),
                        termination_date=datetime.date(2016, 10, 10),
                        supervision_site="SDR",
                        supervising_officer=StateAgent(
                            external_id="ABCDEF01",
                            agent_type=StateAgentType.SUPERVISION_OFFICER,
                            state_code="US_TN",
                        ),
                        admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
                        admission_reason_raw_text="TRANS",
                        termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                        termination_reason_raw_text="DIS",
                        supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                        supervision_type_raw_text="PRO",
                    )
                ],
            ),
            # Person 3 has one supervision period in community corrections.
            StatePerson(
                state_code="US_TN",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_TN", external_id="00000003", id_type="US_TN_DOC"
                    )
                ],
                supervision_periods=[
                    StateSupervisionPeriod(
                        external_id="00000003-1",
                        state_code="US_TN",
                        start_date=datetime.date(2011, 1, 26),
                        termination_date=datetime.date(2011, 2, 8),
                        supervision_site="PDR",
                        supervising_officer=StateAgent(
                            external_id="ABCDEF01",
                            agent_type=StateAgentType.SUPERVISION_OFFICER,
                            state_code="US_TN",
                        ),
                        admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
                        admission_reason_raw_text="MULRE",
                        termination_reason=StateSupervisionPeriodTerminationReason.EXPIRATION,
                        termination_reason_raw_text="EXP",
                        supervision_type=StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT,
                        supervision_type_raw_text="CCC",
                    )
                ],
            ),
            # Person 3 has one open supervision period in parole.
            StatePerson(
                state_code="US_TN",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_TN", external_id="00000003", id_type="US_TN_DOC"
                    )
                ],
                supervision_periods=[
                    StateSupervisionPeriod(
                        external_id="00000003-2",
                        state_code="US_TN",
                        start_date=datetime.date(2017, 7, 22),
                        termination_date=None,
                        supervision_site="SDR",
                        supervising_officer=StateAgent(
                            external_id="ABCDEF01",
                            agent_type=StateAgentType.SUPERVISION_OFFICER,
                            state_code="US_TN",
                        ),
                        admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
                        admission_reason_raw_text="TRPRB",
                        termination_reason=None,
                        termination_reason_raw_text=None,
                        supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                        supervision_type_raw_text="PAO",
                    )
                ],
            ),
        ]

        self._run_parse_ingest_view_test(
            "AssignedStaffSupervisionPeriod", expected_output
        )

    def test_parse_AssignedStaffSupervisionPeriod_AdmissionReasons(self) -> None:
        manifest_ast = self._parse_manifest("AssignedStaffSupervisionPeriod")
        enum_parser_manifest = (
            # Drill down to get admission reasons.
            manifest_ast.field_manifests["supervision_periods"]
            .child_manifests[0]  # type: ignore[attr-defined]
            .field_manifests["admission_reason"]
        )
        self._parse_enum_manifest_test(
            "AssignedStaffSupervisionPeriod_AdmissionReasons", enum_parser_manifest
        )

    def test_parse_AssignedStaffSupervisionPeriod_TerminationReasons(self) -> None:
        manifest_ast = self._parse_manifest("AssignedStaffSupervisionPeriod")
        enum_parser_manifest = (
            # Drill down to get termination reasons.
            manifest_ast.field_manifests["supervision_periods"]
            .child_manifests[0]  # type: ignore[attr-defined]
            .field_manifests["termination_reason"]
        )
        self._parse_enum_manifest_test(
            "AssignedStaffSupervisionPeriod_TerminationReasons", enum_parser_manifest
        )
