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
"""Tests for classes and helpers in us_mo_sentence_classification.py."""
import datetime
import unittest

from recidiviz.pipelines.utils.state_utils.us_mo.us_mo_sentence_classification import (
    UsMoSentenceStatus,
)


class UsMoSentenceStatusTest(unittest.TestCase):
    """Tests for UsMoSentenceStatus"""

    def _build_test_status(
        self, status_code: str, status_description: str
    ) -> UsMoSentenceStatus:
        return UsMoSentenceStatus(
            status_code=status_code,
            status_date=datetime.date(year=2019, month=5, day=10),
            sequence_num=0,
            status_description=status_description,
        )

    def test_incarceration_supervision_in_out_statuses(self) -> None:
        sup_in_status = self._build_test_status(
            status_code="15I1000", status_description="New Court Probation"
        )
        sup_out_status = self._build_test_status(
            status_code="45O3010", status_description="CR Ret-Tech Viol"
        )
        inc_in_status = self._build_test_status(
            status_code="40I0050", status_description="Board Holdover"
        )
        inc_out_status = self._build_test_status(
            status_code="70O3010", status_description="MO Inmate-Interstate Transfer"
        )

        self.assertTrue(sup_in_status.is_supervision_in_status)
        self.assertFalse(sup_out_status.is_supervision_in_status)
        self.assertFalse(inc_in_status.is_supervision_in_status)
        self.assertFalse(inc_out_status.is_supervision_in_status)

        self.assertFalse(sup_in_status.is_supervision_out_status)
        self.assertTrue(sup_out_status.is_supervision_out_status)
        self.assertFalse(inc_in_status.is_supervision_out_status)
        self.assertFalse(inc_out_status.is_supervision_out_status)

        self.assertFalse(sup_in_status.is_incarceration_in_status)
        self.assertFalse(sup_out_status.is_incarceration_in_status)
        self.assertTrue(inc_in_status.is_incarceration_in_status)
        self.assertFalse(inc_out_status.is_incarceration_in_status)

        self.assertFalse(sup_in_status.is_incarceration_out_status)
        self.assertFalse(sup_out_status.is_incarceration_out_status)
        self.assertFalse(inc_in_status.is_incarceration_out_status)
        self.assertTrue(inc_out_status.is_incarceration_out_status)

    def test_status_description_converter(self) -> None:
        stat = self._build_test_status(
            status_code="05I5000",
            status_description="New Pre-Sentence Investigation",
        )
        self.assertEqual(stat.status_description, "NEW PRE-SENTENCE INVESTIGATION")

        code, descript = '40I7000@@"Field Supv to DAI-Oth Sentence"'.split("@@")
        stat = self._build_test_status(status_code=code, status_description=descript)
        self.assertEqual(stat.status_description, '"FIELD SUPV TO DAI-OTH SENTENCE"')

    def test_is_investigation_status(self) -> None:
        investigation_statuses = [
            self._build_test_status(
                status_code="05I5000",
                status_description="New Pre-Sentence Investigation",
            ),
            self._build_test_status(
                status_code="05I5100", status_description="New Community Corr Court Ref"
            ),
            self._build_test_status(
                status_code="05I5200",
                status_description="New Interstate Compact-Invest",
            ),
            self._build_test_status(
                status_code="05I5210",
                status_description="IS Comp-Reporting Instr Given",
            ),
            self._build_test_status(
                status_code="05I5300", status_description="New Exec Clemency-Invest"
            ),
            self._build_test_status(
                status_code="05I5400", status_description="New Bond Investigation"
            ),
            self._build_test_status(
                status_code="05I5500", status_description="New Diversion Investigation"
            ),
            self._build_test_status(
                status_code="05I5600", status_description="New Sentencing Assessment"
            ),
            self._build_test_status(
                status_code="25I5000", status_description="PSI-Addl Charge"
            ),
            self._build_test_status(
                status_code="25I5100",
                status_description="Comm Corr Crt Ref-Addl Charge",
            ),
            self._build_test_status(
                status_code="25I5200",
                status_description="IS Compact-Invest-Addl Charge",
            ),
            self._build_test_status(
                status_code="25I5210",
                status_description="IS Comp-Rep Instr Giv-Addl Chg",
            ),
            self._build_test_status(
                status_code="25I5300",
                status_description="Exec Clemency-Invest-Addl Chg",
            ),
            self._build_test_status(
                status_code="25I5400",
                status_description="Bond Investigation-Addl Charge",
            ),
            self._build_test_status(
                status_code="25I5500", status_description="Diversion Invest-Addl Charge"
            ),
            self._build_test_status(
                status_code="25I5600",
                status_description="Sentencing Assessment-Addl Chg",
            ),
            self._build_test_status(
                status_code="35I5000", status_description="PSI-Revisit"
            ),
            self._build_test_status(
                status_code="35I5100", status_description="Comm Corr Crt Ref-Revisit"
            ),
            self._build_test_status(
                status_code="35I5200", status_description="IS Compact-Invest-Revisit"
            ),
            self._build_test_status(
                status_code="35I5210",
                status_description="IS Comp-Rep Instr Giv-Revisit",
            ),
            self._build_test_status(
                status_code="35I5400", status_description="Bond Investigation-Revisit"
            ),
            self._build_test_status(
                status_code="35I5500", status_description="Diversion Invest-Revisit"
            ),
            self._build_test_status(
                status_code="35I5600",
                status_description="Sentencing Assessment-Revisit",
            ),
            self._build_test_status(
                status_code="95O5000", status_description="PSI Completed"
            ),
            self._build_test_status(
                status_code="95O5010", status_description="PSI Probation Denied-Other"
            ),
            self._build_test_status(
                status_code="95O5015", status_description="PSI Plea Withdrawn"
            ),
            self._build_test_status(
                status_code="95O5020", status_description="PSI Probation Denied-Jail"
            ),
            self._build_test_status(
                status_code="95O5025", status_description="PSI Other Disposition"
            ),
            self._build_test_status(
                status_code="95O5030", status_description="PSI Cancelled by Court"
            ),
            self._build_test_status(
                status_code="95O5100", status_description="Comm Corr Court Ref Closed"
            ),
            self._build_test_status(
                status_code="95O5200", status_description="Interstate Invest Closed"
            ),
            self._build_test_status(
                status_code="95O5210",
                status_description="IS Comp-Report Instruct Closed",
            ),
            self._build_test_status(
                status_code="95O5310", status_description="Executive Clemency Inv Comp."
            ),
            self._build_test_status(
                status_code="95O5400", status_description="Bond Investigation Closed"
            ),
            self._build_test_status(
                status_code="95O5405", status_description="Bond Invest-No Charge"
            ),
            self._build_test_status(
                status_code="95O5500", status_description="Diversion Invest Completed"
            ),
            self._build_test_status(
                status_code="95O5510", status_description="Diversion Invest Denied"
            ),
            self._build_test_status(
                status_code="95O5600", status_description="SAR Completed"
            ),
            self._build_test_status(
                status_code="95O560Z",
                status_description="SAR Completed      MUST VERIFY",
            ),
            self._build_test_status(
                status_code="95O5610", status_description="SAR Probation Denied-Other"
            ),
            self._build_test_status(
                status_code="95O5615", status_description="SAR Plea Withdrawn"
            ),
            self._build_test_status(
                status_code="95O5620", status_description="SAR Probation Denied-Jail"
            ),
            self._build_test_status(
                status_code="95O5625", status_description="SAR Other Disposition"
            ),
            self._build_test_status(
                status_code="95O5630", status_description="SAR Cancelled by Court"
            ),
            self._build_test_status(
                status_code="95O51ZZ",
                status_description="Comm Corr Ref Comp MUST VERIFY",
            ),
            self._build_test_status(
                status_code="95O52ZZ",
                status_description="Interstate Inv Cls MUST VERIFY",
            ),
        ]

        for status in investigation_statuses:
            self.assertTrue(status.is_investigation_status)

        non_investigation_status = self._build_test_status(
            "35I4000", "IS Compact-Prob-Revisit"
        )
        self.assertFalse(non_investigation_status.is_investigation_status)

    def test_is_sentence_termimination_status(self) -> None:
        institutional_sentence_end = self._build_test_status(
            status_code="90O2010", status_description="Parole Completion"
        )
        supervision_sentence_end = self._build_test_status(
            status_code="95O1000", status_description="Court Probation Completion"
        )
        death_status = self._build_test_status(
            status_code="99O9999", status_description="Execution"
        )
        lifetime_supv_status = self._build_test_status(
            status_code="90O1070", status_description="Director's Rel Comp-Life Supv"
        )
        investigation_completion_status = self._build_test_status(
            status_code="95O5500", status_description="Diversion Invest Completed"
        )

        self.assertTrue(institutional_sentence_end.is_sentence_termimination_status)
        self.assertTrue(supervision_sentence_end.is_sentence_termimination_status)
        self.assertTrue(death_status.is_sentence_termimination_status)
        self.assertFalse(lifetime_supv_status.is_sentence_termimination_status)
        self.assertFalse(
            investigation_completion_status.is_sentence_termimination_status
        )

    def test_is_lifetime_supervision_start_status(self) -> None:
        lifetime_supv_status1 = self._build_test_status(
            status_code="35I6010", status_description="Release from DMH for SVP Supv"
        )
        lifetime_supv_status2 = self._build_test_status(
            status_code="90O1070", status_description="Director's Rel Comp-Life Supv"
        )

        self.assertTrue(lifetime_supv_status1.is_lifetime_supervision_start_status)
        self.assertTrue(lifetime_supv_status2.is_lifetime_supervision_start_status)
