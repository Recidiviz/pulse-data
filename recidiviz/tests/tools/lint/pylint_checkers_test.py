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
"""Tests for our custom pylint checker"""
import unittest
from typing import cast

from astroid import builder, nodes
from pylint import testutils

from recidiviz.tools.lint import pylint_checkers


class PylintCheckerTestCaseWrapper(testutils.CheckerTestCase):
    CHECKER_CLASS = pylint_checkers.RecidivizChecker


class RecidivizCheckerTest(unittest.TestCase):
    """Tests our custom pylint checker"""

    def setUp(self) -> None:
        self.case_wrapper = PylintCheckerTestCaseWrapper()
        self.case_wrapper.setup_method()

    def test_plain_format_is_error(self) -> None:
        call_node = cast(
            nodes.Call, builder.extract_node('"hello {}".format("recidiviz")')
        )
        func_node = cast(nodes.Attribute, call_node.func)
        str_node = cast(nodes.Const, func_node.expr)

        with self.case_wrapper.assertAddsMessages(
            testutils.Message(
                msg_id="strict-string-format",
                node=str_node,
            ),
        ):
            cast(
                pylint_checkers.RecidivizChecker, self.case_wrapper.checker
            ).visit_const(str_node)

    def test_pct_format_is_error(self) -> None:
        op_node = cast(nodes.BinOp, builder.extract_node('"hello %s" % "recidiviz"'))
        str_node = cast(nodes.Attribute, op_node.left)

        with self.case_wrapper.assertAddsMessages(
            testutils.Message(
                msg_id="strict-string-format",
                node=str_node,
            ),
        ):
            cast(
                pylint_checkers.RecidivizChecker, self.case_wrapper.checker
            ).visit_const(str_node)

    def test_f_string_is_okay(self) -> None:
        joined_node = cast(
            nodes.JoinedStr, builder.extract_node('f"{foo} is better than {bar!r}"')
        )
        _foo_node, str_node, _bar_node = joined_node.values
        str_node = cast(nodes.Const, str_node)

        with self.case_wrapper.assertNoMessages():
            cast(
                pylint_checkers.RecidivizChecker, self.case_wrapper.checker
            ).visit_const(str_node)

    def test_str_concat_is_okay(self) -> None:
        op_node = cast(nodes.BinOp, builder.extract_node('"hello" + "recidiviz"'))
        str_node = cast(nodes.Attribute, op_node.left)

        with self.case_wrapper.assertNoMessages():
            cast(
                pylint_checkers.RecidivizChecker, self.case_wrapper.checker
            ).visit_const(str_node)
