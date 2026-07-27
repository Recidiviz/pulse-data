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
"""Verifies that code only depends on modules that are visible to it.

The per-entrypoint allowlists that use these helpers live in
recidiviz/tests/tools/validate_source_visibility_integration_test.py:

$ uv run pytest recidiviz/tests/tools/validate_source_visibility_integration_test.py

If you add a new dependency that causes this to fail, you should evaluate whether
this dependency (1) should exist at all and (2) if it should, whether it could be
cleaner. For example, if you need to access some constants related to persistence
from higher-level application code, consider pulling these constants out into a
shared module instead of importing persistence logic into this server.
"""
from typing import Dict, Iterable, List, Optional, Set, Tuple

import attr
import pygtrie

from recidiviz.tools.file_dependencies import Callsite, EntrypointDependencies


def make_module_matcher(modules: Iterable[str]) -> pygtrie.PrefixSet:
    return pygtrie.PrefixSet(
        iterable=modules, factory=pygtrie.StringTrie, separator="."
    )


def is_valid_module_dependency(
    module_name: str,
    valid_module_prefixes: pygtrie.PrefixSet,
) -> bool:
    # Checks if module or a prefix of this module is allowed
    if module_name in valid_module_prefixes:
        return True

    # Checks if a child of this module is allowed
    children = list(valid_module_prefixes.iter(prefix=module_name))
    return len(children) > 0


class InvalidSourceVisibilityError(ValueError):
    """Raised when an entrypoint's transitive dependencies do not match the set of
    module prefixes that entrypoint is allowed to depend on."""


@attr.s(frozen=True, kw_only=True)
class DependencyAnalysisResult:
    """The outcome of checking one entrypoint's transitive dependencies against the
    set of module prefixes it is allowed to depend on."""

    # The entrypoint module whose dependencies were analyzed.
    entrypoint_module: str = attr.ib()
    # Dependencies that no allowed prefix covers, each mapped to a sample call chain
    # showing how the entrypoint reaches it.
    invalid_modules: Dict[str, List[Tuple[str, Callsite]]] = attr.ib()
    # Allowed prefixes that no actual dependency relied on.
    unused_valid_module_prefixes: Set[str] = attr.ib()

    @property
    def is_valid(self) -> bool:
        return not self.invalid_modules and not self.unused_valid_module_prefixes

    def format_error_message(self) -> str:
        """Returns a description of every way this entrypoint failed validation, along
        with how to fix each one."""
        if self.is_valid:
            raise ValueError(
                f"Entrypoint [{self.entrypoint_module}] passed validation - there is "
                f"no error to format."
            )

        sections = [
            f"Entrypoint [{self.entrypoint_module}] does not match the module "
            f"prefixes it is allowed to depend on."
        ]

        if self.invalid_modules:
            lines = [
                f"Found [{len(self.invalid_modules)}] dependencies of "
                f"[{self.entrypoint_module}] that no allowed prefix covers. Either "
                f"remove the dependency or add a prefix covering it to "
                f"valid_module_prefixes:"
            ]
            for dependency, call_chain in sorted(self.invalid_modules.items()):
                lines.append(f"\t{dependency}")
                for caller, callsite in call_chain:
                    lines.append(
                        f"\t\t{caller} "
                        f"({callsite.filepath}:{callsite.lineno}:{callsite.col_offset})"
                    )
            sections.append("\n".join(lines))

        if self.unused_valid_module_prefixes:
            lines = [
                f"Found [{len(self.unused_valid_module_prefixes)}] prefixes allowed "
                f"for [{self.entrypoint_module}] that nothing depends on. Remove them "
                f"from valid_module_prefixes:"
            ]
            for dependency in sorted(self.unused_valid_module_prefixes):
                lines.append(f"\t{dependency}")
            sections.append("\n".join(lines))

        return "\n\n".join(sections)


def get_invalid_dependencies_for_entrypoint(
    entrypoint_module: str,
    valid_module_prefixes: pygtrie.PrefixSet,
    explicitly_invalid_package_dependencies: Optional[List[str]] = None,
) -> DependencyAnalysisResult:
    """Gets the transitive dependencies for the entrypoints and checks their validity.

    Returns two elements. The first is a dictionary of invalid dependency names to the
    call chain that includes them. The second is a list of dependency prefixes that
    were explicitly allowed but that no actual dependencies relied on.
    """
    dependencies = EntrypointDependencies().add_dependencies_for_entrypoint(
        entrypoint_module
    )

    valid_dependencies: Set[str] = set()
    invalid_dependencies: Dict[str, List[Tuple[str, Callsite]]] = {}

    for module_name, callers in dependencies.modules.items():
        if module_name == entrypoint_module or is_valid_module_dependency(
            module_name, valid_module_prefixes
        ):
            valid_dependencies.add(module_name)
            continue

        if not callers:
            raise ValueError(
                f"Found dependency module [{module_name}] of entrypoint "
                f"[{entrypoint_module}] with no callers. This should not be possible."
            )

        valid_callers = [
            c
            for c in callers
            if is_valid_module_dependency(c, valid_module_prefixes)
            or c == entrypoint_module
        ]

        if valid_callers:
            # If this module is directly imported by a module that is a valid
            # dependency, arbitrarily pick one of the of those parent modules and store
            # the full call chain for display later.
            caller = valid_callers[0]
            invalid_dependencies[module_name] = [
                (caller, callers[caller][0])
            ] + dependencies.sample_call_chain_for_module(caller)
        # Otherwise, this module is not called directly by any valid module. We assume
        # that one of its invalid parents in the call chain has a valid caller, so an
        # error will be collected via the block above.

    for package_name, callers in dependencies.packages.items():
        if (
            not explicitly_invalid_package_dependencies
            or package_name not in explicitly_invalid_package_dependencies
        ):
            valid_dependencies.add(package_name)
            continue

        if not callers:
            raise ValueError(
                f"Found dependency package [{package_name}] of entrypoint "
                f"[{entrypoint_module}] with no callers. This should not be possible."
            )

        valid_callers = [
            c for c in callers if is_valid_module_dependency(c, valid_module_prefixes)
        ]

        if valid_callers:
            # If this packages is directly imported by a module that is a valid
            # dependency, arbitrarily pick one of the of those parent modules and store
            # the full call chain for display later.
            caller = valid_callers[0]
            invalid_dependencies[package_name] = [
                (caller, callers[caller][0])
            ] + dependencies.sample_call_chain_for_module(caller)

    unused_valid = valid_module_prefixes - valid_dependencies

    return DependencyAnalysisResult(
        entrypoint_module=entrypoint_module,
        invalid_modules=invalid_dependencies,
        unused_valid_module_prefixes={"".join(entry) for entry in unused_valid},
    )


# Define a constant list of disallowed module prefixes
DISALLOWED_MODULE_PREFIXES = [
    "recidiviz.research",
    "recidiviz.NOT_FOR_PRODUCTION_USE",
]


def validate_dependencies_for_entrypoint(
    entrypoint_module: str,
    valid_module_prefixes: pygtrie.PrefixSet,
    explicitly_invalid_package_dependencies: Optional[List[str]] = None,
) -> None:
    """Analyzes the transitive dependencies of the given entrypoint, raising an
    InvalidSourceVisibilityError describing every failure if they do not exactly match
    the set of modules the entrypoint is allowed to depend on.
    """
    # Check if any of the valid_module_prefixes match disallowed modules
    for disallowed_prefix in DISALLOWED_MODULE_PREFIXES:
        if any(
            str(prefix).startswith(disallowed_prefix)
            for prefix in valid_module_prefixes
        ):
            raise ValueError(
                f"Invalid configuration: {disallowed_prefix} is a disallowed module "
                f"and cannot be included in valid_module_prefixes for {entrypoint_module}."
            )

    dependency_result = get_invalid_dependencies_for_entrypoint(
        entrypoint_module,
        valid_module_prefixes=valid_module_prefixes,
        explicitly_invalid_package_dependencies=explicitly_invalid_package_dependencies,
    )

    if not dependency_result.is_valid:
        raise InvalidSourceVisibilityError(dependency_result.format_error_message())
