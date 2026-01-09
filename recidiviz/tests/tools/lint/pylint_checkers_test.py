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

# TODO(apache/beam#22893): Apache beam has a strict requirement on `dill` which is used by Pylint.
# As a result, Pylint is installed separately and can be installed locally on an as-needed basis
# We try/except ImportErrors in this file as not all developers may have Pylint installed
try:
    from astroid import builder, nodes
    from pylint import testutils

    from recidiviz.tools.lint import pylint_checkers

    class PylintCheckerTestCaseWrapper(testutils.CheckerTestCase):
        CHECKER_CLASS = pylint_checkers.RecidivizChecker

    class RecidivizCheckerTestCase(unittest.TestCase):
        """Tests our custom pylint checker"""

        def setUp(self) -> None:
            self.case_wrapper = PylintCheckerTestCaseWrapper()
            self.case_wrapper.setup_method()

    class StrictStringFormatCheckerTest(RecidivizCheckerTestCase):
        """Tests the strict-string-format message"""

        def test_plain_format_is_error(self) -> None:
            call_node = cast(
                nodes.Call, builder.extract_node('"hello {}".format("recidiviz")')
            )
            func_node = cast(nodes.Attribute, call_node.func)
            str_node = cast(nodes.Const, func_node.expr)

            with self.case_wrapper.assertAddsMessages(
                testutils.MessageTest(
                    msg_id="strict-string-format",
                    node=str_node,
                    line=1,
                    col_offset=0,
                    end_line=1,
                    end_col_offset=10,
                ),
            ):
                cast(
                    pylint_checkers.RecidivizChecker, self.case_wrapper.checker
                ).visit_const(str_node)

        def test_pct_format_is_error(self) -> None:
            op_node = cast(
                nodes.BinOp, builder.extract_node('"hello %s" % "recidiviz"')
            )
            str_node = cast(nodes.Attribute, op_node.left)

            with self.case_wrapper.assertAddsMessages(
                testutils.MessageTest(
                    msg_id="strict-string-format",
                    node=str_node,
                    line=1,
                    col_offset=0,
                    end_line=1,
                    end_col_offset=10,
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

    class ArgparseNoBoolTypeTest(RecidivizCheckerTestCase):
        """Tests the argparse-no-bool-type message"""

        def test_bool_type(self) -> None:
            # The "#@" instructs the parser to only extract that statement into a node
            call_node = cast(
                nodes.Call,
                builder.extract_node(
                    """
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument( #@
        "--arg_name",
        type=bool,
    )
    """
                ),
            )

            with self.case_wrapper.assertAddsMessages(
                testutils.MessageTest(
                    msg_id="argparse-no-bool-type",
                    node=call_node,
                    line=4,
                    col_offset=0,
                    end_line=7,
                    end_col_offset=1,
                ),
            ):
                cast(
                    pylint_checkers.RecidivizChecker, self.case_wrapper.checker
                ).visit_call(call_node)

        def test_no_type(self) -> None:
            # The "#@" instructs the parser to only extract that statement into a node
            call_node = cast(
                nodes.Call,
                builder.extract_node(
                    """
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--arg_name") #@
    """
                ),
            )

            with self.case_wrapper.assertNoMessages():
                cast(
                    pylint_checkers.RecidivizChecker, self.case_wrapper.checker
                ).visit_call(call_node)

        def test_other_type(self) -> None:
            # The "#@" instructs the parser to only extract that statement into a node
            call_node = cast(
                nodes.Call,
                builder.extract_node(
                    """
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument( #@
        "--arg_name",
        type=str,
    )
    """
                ),
            )

            with self.case_wrapper.assertNoMessages():
                cast(
                    pylint_checkers.RecidivizChecker, self.case_wrapper.checker
                ).visit_call(call_node)

        def test_other_function_with_type_arg(self) -> None:
            # The "#@" instructs the parser to only extract that statement into a node
            call_node = cast(
                nodes.Call,
                builder.extract_node(
                    """
    def fn(type: type) -> None:
        pass
    
    fn(type=bool) #@
    """
                ),
            )

            with self.case_wrapper.assertNoMessages():
                cast(
                    pylint_checkers.RecidivizChecker, self.case_wrapper.checker
                ).visit_call(call_node)

except ImportError:
    pass
