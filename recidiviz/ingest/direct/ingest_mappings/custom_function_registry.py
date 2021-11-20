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
"""Object that can be used to retrieve custom python functions from raw manifest
descriptors.
"""

import inspect
from types import ModuleType
from typing import Any, Callable, Dict, Type, Union, get_args, get_origin

import attr

from recidiviz.common.common_utils import bidirectional_set_difference
from recidiviz.common.module_collector_mixin import ModuleCollectorMixin
from recidiviz.utils.types import T


@attr.s(kw_only=True)
class CustomFunctionRegistry(ModuleCollectorMixin):
    """Object that can be used to retrieve custom python functions from raw manifest
    descriptors.
    """

    # Module containing files (or packages) with custom python functions.
    custom_functions_root_module: ModuleType = attr.ib()

    def get_custom_python_function(
        self,
        function_reference: str,
        expected_kwarg_types: Dict[str, Type[Any]],
        expected_return_type: Type[T],
    ) -> Callable[..., T]:
        """Returns a reference to the python function specified by |function_reference|.

        Args:
            function_reference: Reference to the function, relative to the
                |custom_functions_root_module|. Example: "us_xx_custom_parsers.my_fn"
            expected_kwarg_types: Map of argument names to expected types. If the
                function specified by |function_reference| does not have arguments with
                 matching names / types, this will throw.
            expected_return_type: Expected return type for the specified function. If
                the function return type does not match, this will throw.
        """
        relative_path_parts = function_reference.split(".")
        function_name = relative_path_parts[-1]

        function_module = self.get_relative_module(
            self.custom_functions_root_module, relative_path_parts[:-1]
        )

        function = getattr(function_module, function_name)
        function_signature = inspect.signature(function)

        function_argument_names = set(function_signature.parameters.keys())
        expected_argument_names = set(expected_kwarg_types.keys())

        module_name = self.custom_functions_root_module.__name__
        extra_function_args, missing_function_args = bidirectional_set_difference(
            function_argument_names, expected_argument_names
        )

        if extra_function_args:
            raise ValueError(
                f"Found extra, unexpected arguments for function [{function_name}] in "
                f"module [{module_name}]: {extra_function_args}"
            )

        if missing_function_args:
            raise ValueError(
                f"Missing expected arguments for function [{function_name}] in module "
                f"[{module_name}]: {missing_function_args}"
            )

        for arg_name in function_argument_names:
            expected_type = expected_kwarg_types[arg_name]
            actual_type = function_signature.parameters[arg_name].annotation
            if actual_type != expected_type:
                raise ValueError(
                    f"Unexpected type for argument [{arg_name}] in function "
                    f"[{function_name}] in module [{module_name}]. Expected "
                    f"[{expected_type}], found [{actual_type}]."
                )

        is_optional_expected_return_type = (
            get_origin(function_signature.return_annotation) is Union
            and expected_return_type in get_args(function_signature.return_annotation)
            and type(None) in get_args(function_signature.return_annotation)
        )

        if (
            function_signature.return_annotation != expected_return_type
            and not is_optional_expected_return_type
        ):
            raise ValueError(
                f"Unexpected return type for function [{function_name}] in module "
                f"[{module_name}]. Expected [{expected_return_type}], found "
                f"[{function_signature.return_annotation}]."
            )
        return function
