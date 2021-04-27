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
"""Verifies that code only depends on modules that are visible to it

Example usage:
$ python -m recidiviz.tools.validate_source_visibility

If you add a new dependency that causes this to fail, you should evaluate whether
this dependency (1) should exist at all and (2) if it should, whether it could be
cleaner. For example, if you need to access some constants related to persistence
from higher-level application code, consider pulling these constants out into a
shared module instead of importing persistence logic into this server.
"""
from importlib.util import find_spec
import sys
from typing import Dict, Iterable, List, Optional, Set, Tuple

import pygtrie

from recidiviz.calculator.pipeline.incarceration import (
    pipeline as incarceration_pipeline,
)
from recidiviz.calculator.pipeline.program import pipeline as program_pipeline
from recidiviz.calculator.pipeline.recidivism import pipeline as recidivism_pipeline
from recidiviz.calculator.pipeline.supervision import pipeline as supervision_pipeline

from recidiviz.vendor.modulefinder import modulefinder

PIPELINES = {
    incarceration_pipeline,
    program_pipeline,
    recidivism_pipeline,
    supervision_pipeline,
}


def make_module_matcher(modules: Iterable[str]) -> pygtrie.PrefixSet:
    return pygtrie.PrefixSet(
        iterable=modules, factory=pygtrie.StringTrie, separator="."
    )


def module_is_package(module_name: str) -> bool:
    spec = find_spec(module_name)
    if spec is None:
        raise ImportError(f"No module named {module_name}")
    return spec.submodule_search_locations is not None


def is_invalid_dependency(
    name: str,
    valid_module_prefixes: pygtrie.PrefixSet,
) -> bool:
    return (
        name.startswith("recidiviz")
        and not module_is_package(name)
        and name not in valid_module_prefixes
    )


def get_invalid_dependencies_for_entrypoint(
    entrypoint_file: str,
    valid_module_prefixes: pygtrie.PrefixSet,
    allowed_missing_module_prefixes: Optional[pygtrie.PrefixSet] = None,
) -> Tuple[Dict[str, List[str]], Set[str]]:
    """Gets the transitive dependencies for the entrypoints and checks their validity.

    Returns two elements. The first is a dictionary of invalid dependency names to the
    call chain that includes them. The second is a list of dependency prefixes that
    were explicitly allowed but that no actual dependencies relied on.
    """
    if allowed_missing_module_prefixes is None:
        allowed_missing_module_prefixes = make_module_matcher(set())

    m = modulefinder.ModuleFinder()
    m.run_script(entrypoint_file)

    valid_dependencies: Set[str] = set()
    invalid_dependencies: Dict[str, List[str]] = {}

    name: str
    for name in m.modules:
        if is_invalid_dependency(name, valid_module_prefixes):
            call_chain = m.call_chain_for_name(name)
            if not is_invalid_dependency(call_chain[0], valid_module_prefixes):
                invalid_dependencies[name] = call_chain
        else:
            valid_dependencies.add(name)

    for name in m.badmodules:
        if name.startswith("recidiviz") and name not in allowed_missing_module_prefixes:
            invalid_dependencies[name] = m.call_chain_for_name(name)
        else:
            valid_dependencies.add(name)

    unused_valid = valid_module_prefixes - valid_dependencies
    unused_allowed_missing = allowed_missing_module_prefixes - valid_dependencies

    return (
        invalid_dependencies,
        {"".join(entry) for entry in unused_valid | unused_allowed_missing},
    )


def check_dependencies_for_entrypoint(
    entrypoint_file: str,
    valid_module_prefixes: pygtrie.PrefixSet,
    allowed_missing_module_prefixes: Optional[pygtrie.PrefixSet] = None,
) -> bool:
    invalid_dependencies, unused_valid = get_invalid_dependencies_for_entrypoint(
        entrypoint_file,
        valid_module_prefixes=valid_module_prefixes,
        allowed_missing_module_prefixes=allowed_missing_module_prefixes,
    )

    if invalid_dependencies:
        print(f"Invalid dependencies for {entrypoint_file}:")
        for dependency, call_chain in sorted(invalid_dependencies.items()):
            print(f"\t{dependency}")
            for caller in call_chain:
                print(f"\t\t{caller}")
        return False

    if unused_valid:
        print(f"Unused valid dependencies for {entrypoint_file}:")
        for dependency in sorted(unused_valid):
            print(f"\t{dependency}")

    return True


def main() -> int:
    """Analyzes each entrypoint in our codebase and ensures that it only depends on
    code from a fixed set of modules.

    This analysis includes transitive dependencies, not just direct ones.

    This is the inverse of how most code visibility enforcement works, where a module
    would define what other modules can depend on it. In the future it may be useful to
    move to that model.

    Note, this currently only deals with recidiviz source, it does not check external
    package dependencies and whether they should be allowed. This could be a
    potential extension, but would be easiest if we used a dependency analysis tool
    that allowed us to limit to only the first layer of external packages, and omit
    any packages that those packages depend on.
    """
    # TODO(#6862): Move entrypoint/visibility configuration to a global yaml or package
    # specific yamls.
    # TODO(#6861): Support enforcing which external packages can be used as well.
    success = True

    for pipeline in PIPELINES:
        success &= check_dependencies_for_entrypoint(
            pipeline.__file__,
            valid_module_prefixes=make_module_matcher(
                {
                    "recidiviz.big_query.big_query_view",
                    "recidiviz.calculator",
                    "recidiviz.common",
                    # TODO(#6795): Get rid of this dependency
                    "recidiviz.ingest.aggregate",
                    "recidiviz.persistence",
                    "recidiviz.utils",
                }
            ),
        )

    success &= check_dependencies_for_entrypoint(
        "recidiviz/airflow/dag/calculation_pipeline_dag.py",
        valid_module_prefixes=make_module_matcher(
            {
                "recidiviz.airflow",
                "recidiviz.utils.yaml_dict",
            }
        ),
        allowed_missing_module_prefixes=make_module_matcher(
            {"recidiviz_dataflow_operator"}
        ),
    )

    success &= check_dependencies_for_entrypoint(
        "recidiviz/cloud_functions/main.py",
        valid_module_prefixes=make_module_matcher(set()),
    )

    success &= check_dependencies_for_entrypoint(
        "recidiviz/server.py",
        valid_module_prefixes=make_module_matcher(
            {
                "recidiviz.admin_panel",
                "recidiviz.backup",
                "recidiviz.big_query",
                "recidiviz.calculator",
                "recidiviz.case_triage.opportunities.types",
                "recidiviz.case_triage.ops_routes",
                "recidiviz.case_triage.state_utils",
                "recidiviz.case_triage.views",
                "recidiviz.cloud_functions",
                "recidiviz.cloud_memorystore",
                "recidiviz.cloud_sql",
                "recidiviz.cloud_storage",
                "recidiviz.common",
                "recidiviz.datasets",
                "recidiviz.ingest",
                "recidiviz.metrics",
                "recidiviz.persistence",
                "recidiviz.reporting",
                # TODO(#6795): Get rid of this dependency
                "recidiviz.tests.ingest.fixtures",
                # TODO(#4472): Refactor justice counts code out of the tools directory
                "recidiviz.tools.justice_counts.manual_upload",
                "recidiviz.tools.gsutil_shell_helpers",
                "recidiviz.utils",
                "recidiviz.validation",
                "recidiviz.view_registry",
            }
        ),
    )

    success &= check_dependencies_for_entrypoint(
        "recidiviz/case_triage/server.py",
        valid_module_prefixes=make_module_matcher(
            {
                # TODO(##6859): Get rid of this dependency
                "recidiviz.calculator.pipeline",
                "recidiviz.case_triage",
                "recidiviz.cloud_storage",
                # TODO(##6859): Get rid of this dependency
                "recidiviz.ingest.direct.direct_ingest_region_utils",
                "recidiviz.common",
                "recidiviz.persistence",
                "recidiviz.utils",
            }
        ),
    )

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
