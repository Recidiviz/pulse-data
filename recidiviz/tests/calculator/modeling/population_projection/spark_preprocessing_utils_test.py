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
"""Tests for Spark Preprocessing Utils."""
import unittest
from warnings import catch_warnings

import pandas as pd
from pandas.testing import assert_frame_equal

from recidiviz.calculator.modeling.population_projection.utils.spark_preprocessing_utils import (
    transitions_interpolation,
    transitions_lognorm,
    transitions_uniform,
)


class TestSparkPreprocessingUtils(unittest.TestCase):
    """Tests that the validation functions in the spark bq utils work correctly."""

    def test_transitions_uniform(self) -> None:
        """
        Verify that output is as expected for transitions_uniform().
        """

        # get function output df
        df = transitions_uniform("from", "to", 12, 0.8, 5, 119, "x")

        # get expected output df
        expected = pd.DataFrame(
            {
                "compartment": ["from"] * 23,
                "outflow_to": ["to"] * 23,
                "simulation_group": ["x"] * 23,
                "compartment_duration": list(range(1, 23 + 1)),
                "cohort_portion": [0.03478] * 23,
            }
        )

        # check equal
        assert_frame_equal(expected, df)

    def test_transitions_lognorm(self) -> None:
        """
        Verify that output is as expected for transitions_lognorm().
        """

        # function params
        mean = 20.0
        std = 1.0
        periods = 10
        x_months = 10
        p_x_months = 0.1

        # get function output df
        df = transitions_lognorm(
            c_from="from",
            c_to="to",
            mean=mean,
            std=std,
            x_months=x_months,
            p_x_months=p_x_months,
            last_month=periods,
            round_digits=5,
            simulation_group="x",
            plot=False,
        )

        # correct population output
        total_pop = [
            0.00173,
            0.00542,
            0.00846,
            0.01051,
            0.01175,
            0.0124,
            0.01264,
            0.01261,
            0.0124,
            0.01208,
        ]

        # get expected output df
        expected = pd.DataFrame(
            {
                "compartment": ["from"] * periods,
                "outflow_to": ["to"] * periods,
                "simulation_group": ["x"] * periods,
                "compartment_duration": list(range(1, periods + 1)),
                "cohort_portion": total_pop,
            }
        )

        # check equal
        assert_frame_equal(expected, df)

    def test_transitions_interpolation_linear(self) -> None:
        """
        Verify that output is as expected for transitions_interpolation() in linear case.
        """

        # get function output df
        df = transitions_interpolation(
            "from",
            "to",
            [0.1],
            None,
            5,
            "x",
            False,
            False,  # linear
        )

        # correct population
        total_pop = [
            0.01597,
            0.01458,
            0.01319,
            0.01181,
            0.01042,
            0.00903,
            0.00764,
            0.00625,
            0.00486,
            0.00347,
            0.00208,
            0.00069,
        ]

        # get expected output df
        expected = pd.DataFrame(
            {
                "compartment": ["from"] * 12,
                "outflow_to": ["to"] * 12,
                "simulation_group": ["x"] * 12,
                "compartment_duration": list(range(1, 12 + 1)),
                "cohort_portion": total_pop,
            }
        )

        # check equal
        assert_frame_equal(expected, df)

    def test_transitions_interpolation_uniform(self) -> None:
        """
        Verify that output is as expected for transitions_interpolation() in linear case.
        """

        # get function output df
        df = transitions_interpolation(
            "from", "to", [0.1], None, 5, "x", True, False  # uniform
        )

        # correct population
        total_pop = [0.00833] * 12

        # get expected output df
        expected = pd.DataFrame(
            {
                "compartment": ["from"] * 12,
                "outflow_to": ["to"] * 12,
                "simulation_group": ["x"] * 12,
                "compartment_duration": list(range(1, 12 + 1)),
                "cohort_portion": total_pop,
            }
        )

        # check equal
        assert_frame_equal(expected, df)

    def test_interpolation_increasing_pdf_warning(self) -> None:
        """
        Check that warning thrown if probability density function is increasing. The PDF
        must be weakly decreasing, otherwise interpolation can produce sawtooth/jagged
        interpolated PDFs.

        Important note: By default, we fail any pytest test where aw warning is emitted.
        This test passes because spark_processing_utils.py is explicitly exempted
        from that rule in setup.cfg. This test will start failing if spark_processing_utils.py
        is moved / renamed and you will need to update setup.cfg accordingly to get it to pass
        again.
        """
        with catch_warnings(record=True) as w:
            pdf_list = [0.1, 0.2, 0.3]
            transitions_interpolation(
                "from", "to", pdf_list, None, 5, "x", False, False
            )
            self.assertTrue(len(w) == 1)  # check that one warning present
            self.assertTrue("PDF not weakly decreasing." in str(w[0].message))
