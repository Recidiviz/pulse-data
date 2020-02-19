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
"""Tests for the us_mo_enum_helpers.py."""

import unittest
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodAdmissionReason, \
    StateSupervisionPeriodTerminationReason
from recidiviz.common.str_field_utils import normalize
from recidiviz.ingest.direct.regions.us_mo.us_mo_enum_helpers import supervision_period_admission_reason_mapper, \
    supervision_period_termination_reason_mapper, supervising_officer_mapper


_STATE_CODE_UPPER = 'US_MO'


class TestUsMoEnumHelpers(unittest.TestCase):
    """Tests for the US MO enum helpers."""

    def test_parse_supervision_admission_reason_empty(self):
        input_statuses = ''

        reason = supervision_period_admission_reason_mapper(input_statuses)
        self.assertEqual(StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE, reason)

    def test_parse_supervision_admission_reason_single_has_mapping(self):
        # Status meanings and mappings, in relative order:
        #   - Court Probation Reinstated -> RETURN_FROM_SUSPENSION
        input_statuses = '65I2015'

        reason = supervision_period_admission_reason_mapper(input_statuses)
        self.assertEqual(StateSupervisionPeriodAdmissionReason.RETURN_FROM_SUSPENSION, reason)

    def test_parse_supervision_admission_reason_single_has_no_mapping(self):
        # Status meanings and mappings, in relative order:
        #   - unknown, unmapped
        input_statuses = '40O0000'

        reason = supervision_period_admission_reason_mapper(input_statuses)
        self.assertEqual(StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN, reason)

    def test_parse_supervision_admission_reason_multiple_one_is_mapped(self):
        # Status meanings and mappings, in relative order:
        #   - unknown, unmapped
        #   - Parole Release -> CONDITIONAL_RELEASE
        input_statuses = '40O0000 40O1010'

        reason = supervision_period_admission_reason_mapper(input_statuses)
        self.assertEqual(StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE, reason)

    def test_parse_supervision_admission_reason_multiple_all_unmapped(self):
        # Status meanings and mappings, in relative order:
        #   - unknown, unmapped
        #   - unknown, unmapped
        input_statuses = '40O0000 40O9999'

        reason = supervision_period_admission_reason_mapper(input_statuses)
        self.assertEqual(StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN, reason)

    def test_parse_supervision_admission_reason_rank_statuses(self):
        # Status meanings and mappings, in relative order:
        #   - Release to Probation -> CONDITIONAL_RELEASE
        #   - New Court Probation -> COURT_SENTENCE (higher priority)
        input_statuses = '40O9010 15I1000'

        reason = supervision_period_admission_reason_mapper(input_statuses)
        self.assertEqual(StateSupervisionPeriodAdmissionReason.COURT_SENTENCE, reason)

    def test_parse_supervision_admission_reason_two_statuses_same_rank(self):
        # Status meanings and mappings, in relative order:
        #   - Court Probation Reinstated -> COURT_SENTENCE
        #   - New Court Probation -> COURT_SENTENCE
        input_statuses = '65I2015 15I1000'

        reason = supervision_period_admission_reason_mapper(input_statuses)
        # COURT_SENTENCE is chosen as the final admission reason, as statuses are evaluated alphabetically when they
        # tie in rank.
        self.assertEqual(StateSupervisionPeriodAdmissionReason.COURT_SENTENCE, reason)

    def test_parse_supervision_termination_reason_empty(self):
        input_statuses = ''

        reason = supervision_period_termination_reason_mapper(input_statuses)
        self.assertEqual(StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE, reason)

    def test_parse_supervision_termination_reason_single_has_mapping(self):
        # Status meanings and mappings, in relative order:
        #   - Suicide-Institution -> DEATH
        input_statuses = '99O9020'

        reason = supervision_period_termination_reason_mapper(input_statuses)
        self.assertEqual(StateSupervisionPeriodTerminationReason.DEATH, reason)

    def test_parse_supervision_termination_reason_single_has_no_mapping(self):
        # Status meanings and mappings, in relative order:
        #   - unknown, unmapped
        input_statuses = '40I0000'

        reason = supervision_period_termination_reason_mapper(input_statuses)
        self.assertEqual(StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN, reason)

    def test_parse_supervision_termination_reason_multiple_one_is_mapped(self):
        # Status meanings and mappings, in relative order:
        #   - unknown, unmapped
        #   - Field Commutation -> DISCHARGE
        input_statuses = '40I0000 99O1025'

        reason = supervision_period_termination_reason_mapper(input_statuses)
        self.assertEqual(StateSupervisionPeriodTerminationReason.DISCHARGE, reason)

    def test_parse_supervision_termination_reason_multiple_all_unmapped(self):
        # Status meanings and mappings, in relative order:
        #   - unknown, unmapped
        #   - unknown, unmapped
        input_statuses = '12O3456 56I7890'

        reason = supervision_period_termination_reason_mapper(input_statuses)
        self.assertEqual(StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN, reason)

    def test_parse_supervision_termination_reason_rank_statuses(self):
        # Status meanings and mappings, in relative order:
        #   -  Offender re-engaged -> RETURN_FROM_ABSCONSCION
        #   -  Parole discharge -> DISCHARGE (highest rank)
        #   - unknown, unmapped
        input_statuses = '65N9500 99O2010 40O1010'

        reason = supervision_period_termination_reason_mapper(input_statuses)

        # 99O statuses ranked higher than the others
        self.assertEqual(StateSupervisionPeriodTerminationReason.DISCHARGE, reason)

    def test_parse_supervision_termination_reason_two_statuses_same_rank(self):
        # Status meanings and mappings, in relative order:
        #   -  Parole Ret-No Violation -> REVOCATION
        #   -  Offender declared absconder -> ABSCONSION
        input_statuses = '40I1021 65L9100'

        reason = supervision_period_termination_reason_mapper(input_statuses)

        # REVOCATION is chosen as the termination reason because statuses are evaluated alphabetically when they tie in
        # rank
        self.assertEqual(StateSupervisionPeriodTerminationReason.REVOCATION, reason)

    def test_supervising_officer_mapper_po_roles(self):
        """Tests that all PO job titles for MO are properly classified."""
        parole_officer_roles = [
            'P&P OF I',
            'PROBATION/PAROLE OFCR II',
            'PROBATION & PAROLE OFCR I',
            'P&P UNIT SPV',
            'PROBATION/PAROLE UNIT SPV',
            'PROBATION/PAROLE OFCR I',
            'DIST ADMIN II (P & P)',
            'PROBATION & PAROLE UNIT S',
            'DIST ADMIN I (P & P)',
            'P&P OF II',
            'P&P ASST I',
            'PROBATION/PAROLE ASST I',
            'PROBATION/PAROLE OFCR III',
            'PROBATION/PAROLE ASST II',
            'PROBATION & PAROLE ASST I',
            'P&P ASST II',
            'P&P ADMIN',
            'PROBATION & PAROLE OFCR 1',
            'PROBATION/PAROLE OFCER II',
            'PROBATION?PAROLE OFCR I',
            'P&P OFF I',
            'P&P UNIT SUPV',
            'PROBATION 7 PAROLE OFCR I',
            'PROBATION & PAROLE OFCR I',
        ]
        normalized_po_roles = [normalize(role, remove_punctuation=True) for role in parole_officer_roles]
        for role in normalized_po_roles:
            self.assertEqual(StateAgentType.SUPERVISION_OFFICER, supervising_officer_mapper(role))

    def test_supervising_officer_mapper_unknown_role(self):
        self.assertEqual(StateAgentType.INTERNAL_UNKNOWN, supervising_officer_mapper('UNMAPPED ROLE'))
