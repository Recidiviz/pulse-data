# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Analyzes dependencies between python source files."""

import ast
import os
from collections import defaultdict, deque
from typing import DefaultDict, Dict, List, Optional, Set, Tuple

import attr

import recidiviz

RECIDIVIZ_PACKAGE = recidiviz.__name__
RECIDIVIZ_PATH = os.path.dirname(recidiviz.__file__)


@attr.s
class Callsite:
    filepath: str = attr.ib()
    lineno: int = attr.ib()
    col_offset: int = attr.ib()


@attr.s
class EntrypointDependencies:
    """Maintains the dependency graph of one or more python source files."""

    # Recidiviz modules that are used by added entrypoints, and the set of direct
    # callers that import that module (edges of our graph) and where they import them.
    modules: Dict[str, Dict[str, List[Callsite]]] = attr.ib(factory=dict)
    # External packages relied upon by added entrypoints, and the set of direct callers
    # that import that module (edges of our graph) and where they import them.
    packages: Dict[str, Dict[str, List[Callsite]]] = attr.ib(factory=dict)

    def sample_call_chain_for_module(self, module: str) -> List[Tuple[str, Callsite]]:
        chain = []
        while callers := self.modules[module]:
            module, callsites = next(iter(callers.items()))
            chain.append((module, callsites[0]))
        return chain

    def sample_call_chain_for_package(self, package: str) -> List[Tuple[str, Callsite]]:
        calling_module, callsites = next(iter(self.packages[package].items()))
        return [(calling_module, callsites[0])] + self.sample_call_chain_for_module(
            calling_module
        )

    def add_dependencies_for_entrypoint(
        self, entrypoint: str
    ) -> "EntrypointDependencies":
        if entrypoint in self.modules:
            return self

        self.modules[entrypoint] = {}
        to_visit = deque([entrypoint])
        while to_visit:
            module_to_visit = to_visit.pop()
            module_deps, package_deps = _get_direct_dependencies(module_to_visit)

            for module_dep, callsites in module_deps.items():
                if module_dep not in self.modules:
                    to_visit.append(module_dep)
                    self.modules[module_dep] = {module_to_visit: callsites}
                else:
                    self.modules[module_dep][module_to_visit] = callsites
            for package_dep, callsites in package_deps.items():
                if package_dep not in self.packages:
                    self.packages[package_dep] = {module_to_visit: callsites}
                else:
                    self.packages[package_dep][module_to_visit] = callsites

        return self

    @property
    def all_module_dependency_source_files(self) -> Set[str]:
        return set(
            convert_recidiviz_module_to_filepath(module) for module in self.modules
        )


def _convert_recidiviz_module_to_directory_path(module: str) -> str:
    """Does not add suffix"""
    if not module.startswith(RECIDIVIZ_PACKAGE):
        raise ValueError(f"'{module}' must start with '{RECIDIVIZ_PACKAGE}'")
    return module.replace(".", "/").replace(RECIDIVIZ_PACKAGE, RECIDIVIZ_PATH, 1)


def convert_recidiviz_module_to_filepath(module: str) -> str:
    path = _convert_recidiviz_module_to_directory_path(module)
    if os.path.isdir(path):
        return os.path.join(path, "__init__.py")
    return path + ".py"


def convert_path_to_recidiviz_module(path: str) -> str:
    if not path.startswith(RECIDIVIZ_PATH):
        raise ValueError(f"'{path}' must start with '{RECIDIVIZ_PATH}'")
    return (
        os.path.splitext(path)[0]
        .replace(RECIDIVIZ_PATH, RECIDIVIZ_PACKAGE, 1)
        .replace("/", ".")
    )


NodeLineage = dict[ast.AST, ast.AST | None]


class Lineage(ast.NodeTransformer):
    parent: ast.AST | None

    def __init__(self, node_lineage: NodeLineage) -> None:
        self.parent = None
        self.node_lineage = node_lineage

    def visit(self, node: ast.AST) -> ast.AST:
        # set parent for this node
        self.node_lineage[node] = self.parent
        # This node becomes the new parent
        self.parent = node
        # Do any work required by super class
        node = super().visit(node)
        # If we have a valid node (ie. node not being removed)
        if isinstance(node, ast.AST):
            # update the parent, since this may have been transformed
            # to a different node by super
            self.parent = self.node_lineage[node]
        return node


def matching_test_guard_node_in_lineage(
    node: ast.AST,
    node_lineage: NodeLineage,
) -> bool:
    """Walks the node parent lineage to see if there is a matching in_test guard node"""
    parent_node: ast.AST | None = node
    while parent_node in node_lineage:
        match parent_node:
            case ast.If(
                test=ast.Call(
                    func=ast.Attribute(value=ast.Name(id="environment"), attr="in_test")
                )
            ):
                return True

        parent_node = node_lineage[parent_node]

    return False


def _get_direct_dependencies(
    module: str,
) -> Tuple[Dict[str, List[Callsite]], Dict[str, List[Callsite]]]:
    """
    Returns a set of all modules that the initial module depends on.

    Includes any module or package that is imported directly by this module, as well as
    the parent package of this module (if there is one).
    """
    module_dependencies: DefaultDict[str, List[Callsite]] = defaultdict(list)
    package_dependencies: DefaultDict[str, List[Callsite]] = defaultdict(list)

    filepath = convert_recidiviz_module_to_filepath(module)

    if parent := parent_package(module):
        module_dependencies[parent] = [
            Callsite(filepath=filepath, lineno=0, col_offset=0)
        ]

    with open(filepath, encoding="utf-8") as fh:
        root = ast.parse(fh.read(), filepath)

    # Create map of nodes to parent nodes
    node_lineage: NodeLineage = {}
    Lineage(node_lineage).visit(root)

    for node in ast.walk(root):
        if isinstance(node, ast.ImportFrom) and node.module:
            callsite = Callsite(
                filepath=filepath, lineno=node.lineno, col_offset=node.col_offset
            )
            if node.module.startswith("recidiviz.tests"):
                is_guarded_by_in_test = matching_test_guard_node_in_lineage(
                    node=node,
                    node_lineage=node_lineage,
                )

                # Test modules that are only imported while environment.in_test() are not added to module dependencies
                if is_guarded_by_in_test:
                    continue

            if node.module.startswith(RECIDIVIZ_PACKAGE):
                if os.path.isdir(
                    _convert_recidiviz_module_to_directory_path(node.module)
                ):
                    for name in node.names:
                        module_dependencies[f"{node.module}.{name.name}"].append(
                            callsite
                        )
                else:
                    module_dependencies[node.module].append(callsite)
            else:
                package_dependencies[node.module.split(".")[0]].append(callsite)
        if isinstance(node, ast.Import):
            callsite = Callsite(
                filepath=filepath, lineno=node.lineno, col_offset=node.col_offset
            )

            for name in node.names:
                if name.name.startswith(RECIDIVIZ_PACKAGE):
                    module_dependencies[name.name].append(callsite)
                else:
                    package_dependencies[name.name].append(callsite)

    if len(module_dependencies) != len(set(module_dependencies)):
        raise ValueError(f"Duplicate imports in '{module}': {module_dependencies}")
    if len(package_dependencies) != len(set(package_dependencies)):
        raise ValueError(f"Duplicate imports in '{module}': {package_dependencies}")

    return module_dependencies, package_dependencies


def parent_package(module: str) -> Optional[str]:
    module_parts = module.split(".")
    return ".".join(module_parts[:-1]) if len(module_parts) > 1 else None


def get_dependencies_for_entrypoint(entrypoint: str) -> EntrypointDependencies:
    deps = EntrypointDependencies()
    deps.add_dependencies_for_entrypoint(entrypoint)
    return deps
