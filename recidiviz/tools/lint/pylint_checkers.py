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
"""Pylint plugin to enable custom checkers."""

from astroid import bases, nodes

# TODO(apache/beam#22893): Apache beam has a strict requirement on `dill` which is used by Pylint.
# As a result, Pylint is installed separately and can be installed locally on an as-needed basis
# We try/except ImportErrors in this file as not all developers may have Pylint installed
try:
    from pylint import lint
    from pylint.checkers import base_checker, utils

    class RecidivizChecker(base_checker.BaseChecker):
        """This checker contains various checks for the recidiviz codebase."""

        name = "recidiviz"
        msgs = {
            # Pylint requires a code of the form XNNNN, where X is one of the established
            # message types: [I]nformational, [R]efactor, [C]onvention, [W]arning, [E]rror,
            # or [F]atal.
            # Convention is used here as it aligns with the category of the pylint provided
            # `consider-using-f-string`. Additionally, the first two digits must be
            # consistent across the checker and not conflict with other checkers. 50 here
            # was chosen pseudo-randomly and seems unlikely to conflict:
            # https://docs.pylint.org/en/v2.11.1/technical_reference/features.html
            "C5001": (
                "String formatting should use an f-string or StrictStringFormatter",
                "strict-string-format",
                "Emitted when a string is formatted by calling .format on it directly or "
                "via %% formatting. F-string formatting should be preferred. If f-string "
                "is not possible (variables are not known when the template string is "
                "defined) or significantly less readable, StrictStringFormatter can "
                "be used instead.",
            ),
            "W5002": (
                "argparse add_argument() should not have type=bool. Instead, use "
                "`action=argparse.BooleanOptionalAction`, `action='store_true'`, or `type=str_to_bool`. ",
                "argparse-no-bool-type",
                "Emitted when a call to add_argument() sets 'type' to 'bool'. This does not behave as "
                "expected, and arguments may unexpectedly be set to True. Instead, use "
                "`action=argparse.BooleanOptionalAction`, `action='store_true'`, or `type=str_to_bool`. "
                "See https://stackoverflow.com/questions/15008758/parsing-boolean-values-with-argparse.",
            ),
        }

        @utils.only_required_for_messages("strict-string-format")
        def visit_const(self, node: nodes.Const) -> None:
            if (
                # If this is not a string constant, skip it.
                not node.pytype() == "builtins.str"
                # If this is part of an f-string, skip it.
                or isinstance(node.parent, nodes.JoinedStr)
            ):
                return

            if (
                # If we are calling .format on the string, fail.
                (
                    isinstance(node.parent, nodes.Attribute)
                    and node.parent.attrname == "format"
                )
                # If this is using `%` formatting, fail.
                or (isinstance(node.parent, nodes.BinOp) and node.parent.op == "%")
            ):
                self.add_message(
                    "strict-string-format",
                    node=node,
                )

        @utils.only_required_for_messages("argparse-no-bool-type")
        def visit_call(self, node: nodes.Call) -> None:
            if (
                node.func
                and (func := utils.safe_infer(node.func))
                and isinstance(func, bases.Proxy)
                and func.qname() == "argparse._ActionsContainer.add_argument"
            ):
                for keyword in node.keywords:
                    if (
                        keyword.arg == "type"
                        and keyword.value
                        and (inferred_value := utils.safe_infer(keyword.value))
                        and isinstance(inferred_value, nodes.ClassDef)
                        and inferred_value.name == "bool"
                    ):
                        self.add_message("argparse-no-bool-type", node=node)

    def register(linter: lint.PyLinter) -> None:
        linter.register_checker(RecidivizChecker(linter))

except ImportError:
    pass
