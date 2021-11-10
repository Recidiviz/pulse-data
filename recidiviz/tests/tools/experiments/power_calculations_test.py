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
"""Tests for PowerCalc methods"""

import unittest

from recidiviz.tools.experiments.power_calculations import PowerCalc


class TestPowerCalc(unittest.TestCase):
    """Tests for the PowerCalc class"""

    def test_checks(self) -> None:
        """Verify that checks() defines all necessary params"""

        # check icc defined, calculated from tau2
        pc = PowerCalc(n_clusters=2, icc=None, sigma2=1, tau2=0)
        pc.checks()
        self.assertEqual(pc.icc, 0)

        # check error raised if icc and tau disagree
        pc = PowerCalc(n_clusters=2, icc=0.5, tau2=0, sigma2=1)
        with self.assertRaises(ValueError):
            pc.checks()

        # check error if not icc and not (sigma2 and tau2)
        pc = PowerCalc(n_clusters=2, icc=None, sigma2=1, tau2=None)
        with self.assertRaises(ValueError):
            pc.checks()
        pc = PowerCalc(n_clusters=2, icc=None, sigma2=None, tau2=0)
        with self.assertRaises(ValueError):
            pc.checks()

        # check error if pre or post periods < 1, or only one defined
        pc = PowerCalc(n_clusters=2, pre_periods=None, post_periods=1)
        with self.assertRaises(ValueError):
            pc.checks()
        pc = PowerCalc(n_clusters=2, pre_periods=1, post_periods=None)
        with self.assertRaises(ValueError):
            pc.checks()
        pc = PowerCalc(n_clusters=2, pre_periods=0, post_periods=1)
        with self.assertRaises(ValueError):
            pc.checks()
        pc = PowerCalc(n_clusters=2, pre_periods=1, post_periods=0)
        with self.assertRaises(ValueError):
            pc.checks()

        # check warning if icc zero but n-per_cluster > 1
        pc = PowerCalc(n_clusters=2, n_per_cluster=2, icc=0)
        with self.assertLogs("", level="WARNING") as log_check:
            pc.checks()
            self.assertEqual(
                log_check.output,
                ["WARNING:root:`icc` == 0 but `n_per_cluster` > 1, which is unlikely."],
            )

        # check correct bounds
        pc = PowerCalc(n_clusters=2, mde=0)
        with self.assertRaises(ValueError):
            pc.checks()
        pc = PowerCalc(n_clusters=2, sigma2=-1)
        with self.assertRaises(ValueError):
            pc.checks()
        pc = PowerCalc(n_clusters=2, icc=None, tau2=-1)
        with self.assertRaises(ValueError):
            pc.checks()
        pc = PowerCalc(n_clusters=2, alpha=0)
        with self.assertRaises(ValueError):
            pc.checks()
        pc = PowerCalc(n_clusters=2, power=0)
        with self.assertRaises(ValueError):
            pc.checks()
        pc = PowerCalc(n_clusters=2, percent_treated=0)
        with self.assertRaises(ValueError):
            pc.checks()
        pc = PowerCalc(n_clusters=2, icc=-1)
        with self.assertRaises(ValueError):
            pc.checks()
        pc = PowerCalc(n_clusters=2, compliance_rate=-1)
        with self.assertRaises(ValueError):
            pc.checks()

    def test_adjust_mde(self) -> None:
        """Verify that adjust_mde() correctly adjusts mde"""

        # check abort if mde not defined
        pc = PowerCalc(n_clusters=2)
        with self.assertRaises(ValueError):
            pc.adjust_mde()

        # correct adjustment for compliance rate
        pc = PowerCalc(mde=1, compliance_rate=2)
        pc.adjust_mde()
        self.assertEqual(pc.mde, 0.5)

        # correct adjustment for pre and post periods
        pc = PowerCalc(mde=1, pre_periods=4, post_periods=4)
        pc.adjust_mde()
        print(pc.mde)
        self.assertEqual(pc.mde, 0.5 ** 0.5)

    def test_get_mde(self) -> None:
        """Verify that get_mde returns correct mde"""

        # using known power calculations from J-PAL:
        # https://www.povertyactionlab.org/media/file-research-resource/3ie-mde-calculator
        pc = PowerCalc(
            alpha=0.05,
            power=0.8,
            sigma2=2400 ** 2,
            percent_treated=0.5,
            n_clusters=1005,
        )
        mde = pc.get_mde()
        # true mde =~ 424.6
        self.assertAlmostEqual(mde, 425, delta=1)

        # clustered example, from pg 288 Glennerster and Takavarasha (2013)
        pc = PowerCalc(
            alpha=0.05,
            power=0.8,
            sigma2=1,
            percent_treated=0.5,
            n_clusters=356,
            n_per_cluster=40,
            icc=0.17,
        )
        mde = pc.get_mde()
        # true mde =~ 0.13
        self.assertAlmostEqual(mde, 0.13, delta=0.01)

    def test_plot_mde_vs(self) -> None:
        pc = PowerCalc(
            alpha=0.05,
            power=0.8,
            sigma2=2400 ** 2,
            percent_treated=0.5,
            n_clusters=1005,
        )
        # check error if lower bound is strictly greater than 1, and less than upper bound.
        for b in [0, 1, 5, 6]:
            with self.assertRaises(ValueError):
                pc.plot_mde_vs([b, 5])

        # check that `n_clusters` range contains only integer values
        with self.assertRaises(ValueError):
            pc.plot_mde_vs([2.5, 8.5], param="n_clusters")
