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
from typing import (
    Any,
    Callable,
    Dict,
    Optional,
    Tuple,
    Type,
    Union,
    get_args,
    get_origin,
)

import attr
from more_itertools import one

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
    ) -> Tuple[Callable[..., Optional[T]], Type[T]]:
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
        function = self.function_from_reference(function_reference)
        function_signature = inspect.signature(function)

        function_argument_names = set(function_signature.parameters.keys())
        expected_argument_names = set(expected_kwarg_types.keys())

        module_name = self.custom_functions_root_module.__name__
        extra_function_args, missing_function_args = bidirectional_set_difference(
            function_argument_names, expected_argument_names
        )

        if extra_function_args:
            raise ValueError(
                f"Found extra, unexpected arguments for function [{function_reference}] in "
                f"module [{module_name}]: {extra_function_args}"
            )

        if missing_function_args:
            raise ValueError(
                f"Missing expected arguments for function [{function_reference}] in module "
                f"[{module_name}]: {missing_function_args}"
            )

        for arg_name in function_argument_names:
            expected_type = expected_kwarg_types[arg_name]
            actual_type = function_signature.parameters[arg_name].annotation
            if actual_type != expected_type:
                raise ValueError(
                    f"Unexpected type for argument [{arg_name}] in function "
                    f"[{function_reference}] in module [{module_name}]. Expected "
                    f"[{expected_type}], found [{actual_type}]."
                )

        return_type_non_optional = self._return_type_non_optional(
            function_reference, expected_return_type
        )

        return function, return_type_non_optional

    def function_from_reference(self, function_reference: str) -> Callable:
        relative_path_parts = function_reference.split(".")
        function_name = relative_path_parts[-1]

        function_module = self.get_relative_module(
            self.custom_functions_root_module, relative_path_parts[:-1]
        )

        return getattr(function_module, function_name)

    def _return_type_non_optional(
        self, function_reference: str, expected_return_type: Type[T]
    ) -> Type[T]:
        """Returns the actual return type of the function. For return types
        Optional[Foo], returns the inner type, Foo.

        Throws if the return type is not equal to or a subclass of the
        |expected_return_type|.
        """
        function = self.function_from_reference(function_reference)
        function_signature = inspect.signature(function)
        if get_origin(function_signature.return_annotation) is not Union:
            return_type_non_optional = function_signature.return_annotation
        else:
            type_args = get_args(function_signature.return_annotation)
            if type(None) not in type_args:
                raise ValueError(
                    f"Custom functions with Union return types other than Optional "
                    f"are not supported. Found return type: "
                    f"[{function_signature.return_annotation}]"
                )
            return_type_non_optional = one(
                t for t in type_args if not issubclass(t, type(None))
            )

        if not issubclass(return_type_non_optional, expected_return_type):
            raise ValueError(
                f"Unexpected return type for function [{function_reference}]. "
                f"Expected [{expected_return_type}], found "
                f"[{function_signature.return_annotation}]."
            )
        return return_type_non_optional
