# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""A script which will be called using a pre-commit githook to generate markdown
tables counting how many files in a package import each recidiviz function, so
developers can find reusable query fragments and builders. It writes one doc per
FunctionUsageDocConfig, e.g. one for task eligibility criteria and one for
classification views.

For example, if three criteria files each contain

    from recidiviz.calculator.query.sessions_query_fragments import (
        create_sub_sessions_with_attributes,
    )

...the criteria doc's table includes the row:

    | create_sub_sessions_with_attributes | recidiviz.calculator.query.sessions_query_fragments | 3 |

Can be run on-demand via:
    $ uv run python -m recidiviz.tools.docs.function_usage_documentation_generator
"""
import ast
import functools
import inspect
import logging
import os
import re
import sys
from collections import Counter

import attr

import recidiviz
from recidiviz.calculator.query.state.views import classification
from recidiviz.common import attr_validators
from recidiviz.common.file_system import get_all_files_recursive
from recidiviz.task_eligibility import criteria
from recidiviz.tools.docs.summary_file_generator import update_summary_file
from recidiviz.tools.docs.utils import (
    DOCS_ROOT_PATH,
    markdown_table,
    persist_file_contents,
)
from recidiviz.utils.string import StrictStringFormatter

REPO_ROOT_PATH = os.path.dirname(os.path.dirname(recidiviz.__file__))

_SUMMARY_SECTION_TITLE = "## Function Usage Catalog"

# Functions imported by fewer files than this are left out, which keeps one-off
# state-specific helpers from burying the widely reused ones.
MIN_IMPORT_COUNT = 3

_RECIDIVIZ_MODULE_PREFIX = "recidiviz."

# Ends a docstring's first sentence at a period followed by whitespace and a
# capital letter or digit, so abbreviations like "e.g. employed" stay in the
# sentence.
_FIRST_SENTENCE_REGEX = re.compile(r"^(.*?\.)\s+(?=[A-Z0-9])")

# Google-style docstring section headers. A docstring that opens with one of
# these has no summary to show.
_DOCSTRING_SECTION_HEADER_REGEX = re.compile(
    r"^(Args|Arguments|Params|Returns|Yields|Raises|Examples?|Notes?):"
)

# Functions from modules with this last segment look up dataset names rather than
# build query logic, so they would crowd out the reusable query fragments and
# builders these docs exist to surface.
_DATASET_CONFIG_MODULE_NAME = "dataset_config"

# Other modules whose functions are called for type narrowing rather than query
# logic.
_EXCLUDED_SOURCE_MODULES = frozenset({"recidiviz.utils.types"})

_DOC_TEMPLATE = """# {title}

How many {scanned_files_description} import each recidiviz function, attributed to the module that defines it. Functions imported by fewer than {min_import_count} files are left out.

{function_usage_table}"""


@attr.define(frozen=True, kw_only=True)
class FunctionUsageDocConfig:
    """Configuration for one function usage doc: which files to scan and where
    to write the doc.
    """

    title: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Title of the doc, also used as its link text in SUMMARY.md."""

    scanned_package_path: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Directory of the package whose subpackages are scanned. Files directly in
    this directory are not scanned."""

    excluded_subpackage_names: list[str] = attr.ib(
        validator=attr_validators.is_list_of_non_empty_str
    )
    """Names of direct subpackages of scanned_package_path to leave out, such as
    a utils package that defines the helpers rather than using them."""

    documentation_relative_path: str = attr.ib(
        validator=attr_validators.is_non_empty_str
    )
    """Path of the generated doc, relative to the docs root."""

    @property
    def documentation_path(self) -> str:
        return os.path.join(DOCS_ROOT_PATH, self.documentation_relative_path)

    @property
    def scanned_files_description(self) -> str:
        """Returns a phrase naming the scanned files, for the doc's intro."""
        relative_package_path = os.path.relpath(
            self.scanned_package_path, REPO_ROOT_PATH
        )
        description = f"files in the subpackages of `{relative_package_path}/`"
        if not self.excluded_subpackage_names:
            return description
        excluded = ", ".join(
            f"`{name}/`" for name in sorted(self.excluded_subpackage_names)
        )
        return f"{description} other than {excluded}"


CRITERIA_FUNCTION_USAGE_DOC_CONFIG = FunctionUsageDocConfig(
    title="Task eligibility criteria function usage",
    scanned_package_path=os.path.dirname(criteria.__file__),
    excluded_subpackage_names=[],
    documentation_relative_path=os.path.join(
        "engineering", "task_eligibility", "criteria_function_usage.md"
    ),
)

CLASSIFICATION_FUNCTION_USAGE_DOC_CONFIG = FunctionUsageDocConfig(
    title="Classification function usage",
    scanned_package_path=os.path.dirname(classification.__file__),
    excluded_subpackage_names=["utils"],
    documentation_relative_path=os.path.join(
        "engineering", "classification", "classification_function_usage.md"
    ),
)

FUNCTION_USAGE_DOC_CONFIGS = [
    CRITERIA_FUNCTION_USAGE_DOC_CONFIG,
    CLASSIFICATION_FUNCTION_USAGE_DOC_CONFIG,
]


@attr.define(frozen=True, kw_only=True)
class FunctionUsage:
    """One function, what it does, and the number of scanned files that import
    it.
    """

    function_name: str = attr.ib(validator=attr_validators.is_str)
    """Name of the function as defined in its source module."""

    source_module: str = attr.ib(validator=attr_validators.is_str)
    """Module that defines the function, even when files import it through a
    module that re-exports it."""

    function_description: str = attr.ib(validator=attr_validators.is_str)
    """The function's docstring summary from docstring_summary, or an empty
    string if it has none."""

    import_count: int = attr.ib(validator=attr_validators.is_positive_int)
    """Number of scanned files that import the function."""


@attr.define(frozen=True, kw_only=True)
class FunctionDefinition:
    """A top-level function definition found by parsing its module's source."""

    function_name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Name of the function in the module that defines it."""

    source_module: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Module that defines the function."""

    docstring: str | None = attr.ib(validator=attr_validators.is_opt_str)
    """The function's docstring, or None if it has none."""


def docstring_summary(docstring: str | None) -> str:
    """Returns the first sentence of docstring's first paragraph on one line, or
    an empty string if docstring is missing or opens with a section header such
    as "Args:".
    """
    if not docstring:
        return ""
    first_paragraph = " ".join(
        inspect.cleandoc(docstring).split("\n\n", maxsplit=1)[0].split()
    )
    if _DOCSTRING_SECTION_HEADER_REGEX.match(first_paragraph):
        return ""
    if match := _FIRST_SENTENCE_REGEX.match(first_paragraph):
        return match.group(1)
    return first_paragraph


def _scanned_file_paths(
    *, scanned_package_path: str, excluded_subpackage_names: list[str]
) -> list[str]:
    scanned_subpackage_paths = [
        entry.path
        for entry in os.scandir(scanned_package_path)
        if entry.is_dir()
        and entry.name not in excluded_subpackage_names
        and os.path.exists(os.path.join(entry.path, "__init__.py"))
    ]
    return [
        file_path
        for subpackage_path in scanned_subpackage_paths
        for file_path in get_all_files_recursive(subpackage_path)
        if file_path.endswith(".py") and os.path.basename(file_path) != "__init__.py"
    ]


def _module_file_path(*, source_root_path: str, module: str) -> str | None:
    """Returns the file that defines module under source_root_path, which is
    the package's __init__.py when module is a package, or None if module has no
    file there, as for a namespace package.
    """
    module_path = os.path.join(source_root_path, *module.split("."))
    if os.path.isfile(init_path := os.path.join(module_path, "__init__.py")):
        return init_path
    if os.path.isfile(file_path := f"{module_path}.py"):
        return file_path
    return None


def _module_exists(*, source_root_path: str, module: str) -> bool:
    """Returns whether module is a module, package, or namespace package (a
    directory without __init__.py) under source_root_path.
    """
    return _module_file_path(
        source_root_path=source_root_path, module=module
    ) is not None or os.path.isdir(os.path.join(source_root_path, *module.split(".")))


@attr.define(frozen=True, kw_only=True)
class _ReExport:
    """A top-level "from source_module import name" in another module."""

    source_module: str | None = attr.ib(validator=attr_validators.is_opt_str)
    """Module the name is imported from, or None for a relative import."""

    name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Name as defined in source_module."""


@attr.define(frozen=True, kw_only=True)
class _ModuleBindings:
    """What each top-level name in one module refers to, read from its source."""

    bindings: dict[str, FunctionDefinition | _ReExport | None] = attr.ib(
        validator=attr_validators.is_dict
    )
    """Maps each top-level name to the function the module defines under it, the
    import it re-exports, or None for a class, assignment, or module alias."""

    has_star_import: bool = attr.ib(validator=attr_validators.is_bool)
    """Whether the module has a "from x import *", which hides where names
    missing from bindings come from."""


def _module_level_statements(statements: list[ast.stmt]) -> list[ast.stmt]:
    """Returns statements with the bodies of any if, try, and with blocks
    spliced in, recursively, so every name bound at module level is visible.
    That includes "if TYPE_CHECKING:" blocks, which declare names that a module
    __getattr__ provides at runtime, such as StateCode in
    recidiviz.common.constants.states.
    """
    flattened: list[ast.stmt] = []
    for node in statements:
        if isinstance(node, ast.If):
            flattened.extend(_module_level_statements([*node.body, *node.orelse]))
        elif isinstance(node, (ast.Try, ast.TryStar)):
            # Except-handler fallbacks come first so the try body's definitions,
            # which are what runs when nothing fails, take precedence.
            flattened.extend(
                _module_level_statements(
                    [
                        *[s for handler in node.handlers for s in handler.body],
                        *node.body,
                        *node.orelse,
                        *node.finalbody,
                    ]
                )
            )
        elif isinstance(node, ast.With):
            flattened.extend(_module_level_statements(node.body))
        else:
            flattened.append(node)
    return flattened


def _names_bound_by_assignment(node: ast.stmt) -> set[str]:
    if isinstance(node, ast.Assign):
        targets = node.targets
    elif isinstance(node, (ast.AnnAssign, ast.AugAssign)):
        targets = [node.target]
    else:
        return set()
    return {
        name_node.id
        for target in targets
        for name_node in ast.walk(target)
        if isinstance(name_node, ast.Name) and isinstance(name_node.ctx, ast.Store)
    }


@functools.cache
def _module_bindings(*, source_root_path: str, module: str) -> _ModuleBindings:
    """Returns what each top-level name in module refers to, parsed once per
    module from its source file under source_root_path.
    """
    file_path = _module_file_path(source_root_path=source_root_path, module=module)
    if file_path is None:
        if _module_exists(source_root_path=source_root_path, module=module):
            # A namespace package has no __init__.py, so it binds no names.
            return _ModuleBindings(bindings={}, has_star_import=False)
        raise ValueError(
            f"Found no source file for module [{module}] under [{source_root_path}]"
        )
    with open(file_path, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=file_path)

    bindings: dict[str, FunctionDefinition | _ReExport | None] = {}
    has_star_import = False
    # Later statements rebind earlier ones, so each assignment below overwrites.
    for node in _module_level_statements(tree.body):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            bindings[node.name] = FunctionDefinition(
                function_name=node.name,
                source_module=module,
                docstring=ast.get_docstring(node),
            )
        elif isinstance(node, ast.ClassDef):
            bindings[node.name] = None
        elif isinstance(node, ast.Import):
            for alias in node.names:
                bindings[alias.asname or alias.name.split(".")[0]] = None
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name == "*":
                    has_star_import = True
                    continue
                bindings[alias.asname or alias.name] = _ReExport(
                    source_module=None if node.level else node.module,
                    name=alias.name,
                )
        else:
            for name in _names_bound_by_assignment(node):
                bindings[name] = None
    return _ModuleBindings(bindings=bindings, has_star_import=has_star_import)


def _resolve_imported_name(
    *,
    source_root_path: str,
    module: str,
    name: str,
    resolved_imports: tuple[tuple[str, str], ...],
) -> FunctionDefinition | None:
    """Returns the function definition that "from module import name" refers
    to, following re-exports to the module that defines it, by parsing source
    files rather than importing them. Returns None when name is a class,
    constant, module, or anything defined outside recidiviz.

    Args:
        source_root_path: Directory that contains the recidiviz package source.
        module: Module that the name is imported from.
        name: Name imported from module.
        resolved_imports: The (module, name) pairs already followed to reach
            this one, used to detect re-export cycles.
    """
    if not module.startswith(_RECIDIVIZ_MODULE_PREFIX):
        return None
    if (module, name) in resolved_imports:
        # A package __init__ can import its own submodule by absolute path,
        # e.g. "from recidiviz.a import b" inside recidiviz/a/__init__.py.
        if _module_exists(source_root_path=source_root_path, module=f"{module}.{name}"):
            return None
        raise ValueError(
            f"Found a re-export cycle resolving [{name}] from [{module}]: "
            f"{resolved_imports}"
        )
    module_bindings = _module_bindings(source_root_path=source_root_path, module=module)

    if name in module_bindings.bindings:
        binding = module_bindings.bindings[name]
        if not isinstance(binding, _ReExport):
            return binding
        if binding.source_module is None:
            raise ValueError(
                f"Cannot resolve [{name}] in [{module}]: it is imported with a "
                f"relative import, which this generator does not support. Make "
                f"the import absolute."
            )
        return _resolve_imported_name(
            source_root_path=source_root_path,
            module=binding.source_module,
            name=binding.name,
            resolved_imports=(*resolved_imports, (module, name)),
        )

    if _module_exists(source_root_path=source_root_path, module=f"{module}.{name}"):
        return None
    if module_bindings.has_star_import:
        raise ValueError(
            f"Cannot resolve [{name}] in [{module}]: the module has a star import, "
            f"so the generator cannot tell where [{name}] comes from. Import "
            f"[{name}] explicitly in [{module}]."
        )
    raise ValueError(
        f"Cannot resolve [{name}] in [{module}]: it is not a top-level function, "
        f"class, assignment, import, or submodule there. If [{module}] builds it "
        f"at runtime, for example in a module __getattr__, declare it in an "
        f"'if TYPE_CHECKING:' block there."
    )


@functools.cache
def _resolve_top_level_import(
    *, source_root_path: str, module: str, name: str
) -> FunctionDefinition | None:
    """Returns _resolve_imported_name for an import in a scanned file, cached
    because the same imports repeat across hundreds of scanned files.
    """
    return _resolve_imported_name(
        source_root_path=source_root_path,
        module=module,
        name=name,
        resolved_imports=(),
    )


def _is_excluded_source_module(module: str) -> bool:
    return (
        module in _EXCLUDED_SOURCE_MODULES
        or module.rsplit(".", maxsplit=1)[-1] == _DATASET_CONFIG_MODULE_NAME
    )


def _imported_functions_in_file(
    *, source_root_path: str, file_path: str
) -> set[FunctionDefinition]:
    """Returns the functions that the file at file_path imports at module level
    from recidiviz modules, leaving out functions defined in dataset_config
    modules or _EXCLUDED_SOURCE_MODULES. Imports nested in any block, such as the file's
    __main__ block, are skipped, and so are imported classes, constants, and
    modules.
    """
    with open(file_path, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=file_path)

    imported_functions = set()
    for node in tree.body:
        if not isinstance(node, ast.ImportFrom) or node.module is None:
            continue
        for alias in node.names:
            try:
                function = _resolve_top_level_import(
                    source_root_path=source_root_path,
                    module=node.module,
                    name=alias.name,
                )
            except ValueError as e:
                raise ValueError(
                    f"Failed to resolve import of [{alias.name}] from "
                    f"[{node.module}] in [{file_path}]"
                ) from e
            if function is None or _is_excluded_source_module(function.source_module):
                continue
            imported_functions.add(function)
    return imported_functions


def collect_function_usages(
    *,
    source_root_path: str,
    scanned_package_path: str,
    excluded_subpackage_names: list[str],
    min_import_count: int,
) -> list[FunctionUsage]:
    """Returns how many files import each recidiviz function, for functions
    imported by at least min_import_count files, sorted by count descending and
    then by function name.

    Args:
        source_root_path: Directory that contains the recidiviz package source,
            used to find the module files that imported names come from.
        scanned_package_path: Directory of the package whose subpackages are
            scanned. Files directly in this directory are not scanned.
        excluded_subpackage_names: Names of direct subpackages of
            scanned_package_path to leave out.
        min_import_count: Functions imported by fewer files than this are left
            out.
    """
    import_counts: Counter[FunctionDefinition] = Counter()
    for file_path in _scanned_file_paths(
        scanned_package_path=scanned_package_path,
        excluded_subpackage_names=excluded_subpackage_names,
    ):
        import_counts.update(
            _imported_functions_in_file(
                source_root_path=source_root_path, file_path=file_path
            )
        )

    function_usages = [
        FunctionUsage(
            function_name=function.function_name,
            source_module=function.source_module,
            function_description=docstring_summary(function.docstring),
            import_count=count,
        )
        for function, count in import_counts.items()
        if count >= min_import_count
    ]
    return sorted(
        function_usages,
        key=lambda u: (-u.import_count, u.function_name.lower(), u.source_module),
    )


def generate_markdown(
    *,
    config: FunctionUsageDocConfig,
    function_usages: list[FunctionUsage],
    min_import_count: int,
) -> str:
    """Returns the markdown doc for config, listing function_usages, which were
    collected with the given min_import_count.
    """
    function_usage_table = markdown_table(
        headers=[
            "**Function Name**",
            "**Source Module**",
            "**Files**",
            "**Description**",
        ],
        value_matrix=[
            [
                f"`{u.function_name}`",
                f"`{u.source_module}`",
                u.import_count,
                # A bare pipe, as in a |param| reference, would end the cell.
                u.function_description.replace("|", "\\|"),
            ]
            for u in function_usages
        ],
    )
    return StrictStringFormatter().format(
        _DOC_TEMPLATE,
        title=config.title,
        scanned_files_description=config.scanned_files_description,
        min_import_count=min_import_count,
        function_usage_table=function_usage_table,
    )


def generate_documentation(config: FunctionUsageDocConfig) -> bool:
    """Writes the function usage doc for config. Returns whether its contents
    changed.
    """
    function_usages = collect_function_usages(
        source_root_path=REPO_ROOT_PATH,
        scanned_package_path=config.scanned_package_path,
        excluded_subpackage_names=config.excluded_subpackage_names,
        min_import_count=MIN_IMPORT_COUNT,
    )
    return persist_file_contents(
        generate_markdown(
            config=config,
            function_usages=function_usages,
            min_import_count=MIN_IMPORT_COUNT,
        ),
        config.documentation_path,
    )


def main() -> int:
    modified = False
    for config in FUNCTION_USAGE_DOC_CONFIGS:
        modified |= generate_documentation(config)
    if modified:
        update_summary_file(
            [
                f"{_SUMMARY_SECTION_TITLE}\n\n",
                *[
                    f"- [{config.title}]({config.documentation_relative_path})\n"
                    for config in FUNCTION_USAGE_DOC_CONFIGS
                ],
            ],
            _SUMMARY_SECTION_TITLE,
        )
    return 1 if modified else 0


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    sys.exit(main())
