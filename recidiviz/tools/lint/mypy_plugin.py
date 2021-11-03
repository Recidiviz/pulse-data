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
"""Mypy plugin to enable custom hooks."""
from typing import Callable, Optional, Type, cast

from mypy import errorcodes, nodes, types
from mypy.plugin import MethodSigContext, Plugin


def seems_like_f_string(context: nodes.CallExpr) -> bool:
    """Detects if this looks like an f-string.

    Mypy parses f-string variables as calls to format with the template
    {:{}} so we ignore these. See
    https://github.com/python/mypy/blob/f1167bc43866f6b1b663d97121e23a5cebe5f952/mypyc/irbuild/specialize.py#L409
    """
    # The callee for a `str.format` call is always a `MemberExpr`.
    callee = cast(nodes.MemberExpr, context.callee)
    return isinstance(callee.expr, nodes.StrExpr) and callee.expr.value in {
        "{:{}}",
        "{!r:{}}",
    }


def disable_str_format_signature_cb(
    method_sig_ctx: MethodSigContext,
) -> types.CallableType:
    """This callback identifies calls to str.format and marks them as errors.

    Unlike pylint, this can catch calls to format on string variables not just
    literals.
    """
    if isinstance(method_sig_ctx.context, nodes.CallExpr) and not seems_like_f_string(
        method_sig_ctx.context
    ):
        # If we are calling `str.format` and it isn't from an f-string, fail.
        method_sig_ctx.api.fail(
            "str.format calls are disabled. Instead, use f-string or StrictStringFormatter",
            method_sig_ctx.context,
            code=errorcodes.ATTR_DEFINED,
        )
    # We still return the signature even when we fail so that mypy can type check any
    # code that uses the result correctly.
    return method_sig_ctx.default_signature


class RecidivizPlugin(Plugin):
    """Mypy plugin used to enforce custom rules."""

    def get_method_signature_hook(
        self, fullname: str
    ) -> Optional[Callable[[MethodSigContext], types.CallableType]]:
        if fullname == "builtins.str.format":
            return disable_str_format_signature_cb
        return None


def plugin(_version: str) -> Type[Plugin]:
    return RecidivizPlugin
