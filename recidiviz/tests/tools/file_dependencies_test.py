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
"""Tests for file dependency utilities."""
import importlib
import os
import unittest
from functools import partial
from types import ModuleType
from typing import Any, Callable

from mock import patch

import recidiviz
from recidiviz.common.module_collector_mixin import ModuleCollectorMixin
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tests.tools.fixtures import test_importing
from recidiviz.tests.utils.patch_helpers import before
from recidiviz.tools.file_dependencies import (
    DYNAMICALLY_COLLECTED_PYTHON_DEPENDENCIES,
    Callsite,
    EntrypointDependencies,
    _collect_modules_for_dynamic_collection,
    parent_package,
)
from recidiviz.utils.metadata import local_project_id_override

# Do a bit of path munging here since absolute paths will vary by machine.
builtin_open = open
builtin_isdir = os.path.isdir


def fake_open(filepath: str, *, encoding: str) -> Any:
    filepath = filepath.replace("r", os.path.dirname(recidiviz.__file__), 1)
    return builtin_open(filepath, encoding=encoding)


def fake_isdir(filepath: str) -> bool:
    filepath = filepath.replace("r", os.path.dirname(recidiviz.__file__), 1)
    return builtin_isdir(filepath)


@patch("recidiviz.tools.file_dependencies.RECIDIVIZ_PATH", "r")
@patch("os.path.isdir", fake_isdir)
@patch("builtins.open", fake_open)
class FileDependenciesTest(unittest.TestCase):
    """Tests for file dependency utilities."""

    maxDiff = None

    def test_get_correct_source_files(self) -> None:
        source_files = (
            EntrypointDependencies()
            .add_dependencies_for_entrypoint(test_importing.__name__)
            .all_module_dependency_source_files
        )
        assert source_files == {
            "r/tests/tools/fixtures/a/__init__.py",
            "r/tests/tools/fixtures/__init__.py",
            "r/tests/tools/fixtures/a/b/__init__.py",
            "r/tests/tools/fixtures/a/c.py",
            "r/tests/tools/fixtures/a/b/b.py",
            "r/tests/tools/fixtures/a/b/e.py",
            "r/tests/tools/fixtures/a/b/i.py",
            "r/tests/tools/fixtures/test_importing.py",
            "r/tests/tools/__init__.py",
            "r/tests/__init__.py",
            "r/__init__.py",
        }

    def test_add_dependencies_for_entrypoint(self) -> None:
        actual_dependencies = EntrypointDependencies().add_dependencies_for_entrypoint(
            "recidiviz.tests.tools.fixtures.example_dependency_entrypoint"
        )
        expected_dependencies = EntrypointDependencies(
            modules={
                "recidiviz": {
                    "recidiviz.common": [
                        Callsite(
                            filepath="r/common/__init__.py", lineno=0, col_offset=0
                        )
                    ],
                    "recidiviz.tests": [
                        Callsite(filepath="r/tests/__init__.py", lineno=0, col_offset=0)
                    ],
                    "recidiviz.utils": [
                        Callsite(filepath="r/utils/__init__.py", lineno=0, col_offset=0)
                    ],
                    "recidiviz.utils.environment": [
                        Callsite(
                            filepath="r/utils/environment.py", lineno=33, col_offset=0
                        )
                    ],
                },
                "recidiviz.common.attr_validators": {
                    "recidiviz.common.date": [
                        Callsite(filepath="r/common/date.py", lineno=40, col_offset=0)
                    ]
                },
                "recidiviz.common": {
                    "recidiviz.common.date": [
                        Callsite(filepath="r/common/date.py", lineno=0, col_offset=0)
                    ],
                    "recidiviz.common.attr_validators": [
                        Callsite(
                            filepath="r/common/attr_validators.py",
                            lineno=0,
                            col_offset=0,
                        )
                    ],
                },
                "recidiviz.common.date": {
                    "recidiviz.tests.tools.fixtures.example_dependency_entrypoint": [
                        Callsite(
                            filepath="r/tests/tools/fixtures/example_dependency_entrypoint.py",
                            lineno=21,
                            col_offset=0,
                        )
                    ]
                },
                "recidiviz.tests": {
                    "recidiviz.tests.tools": [
                        Callsite(
                            filepath="r/tests/tools/__init__.py", lineno=0, col_offset=0
                        )
                    ]
                },
                "recidiviz.tests.tools": {
                    "recidiviz.tests.tools.fixtures": [
                        Callsite(
                            filepath="r/tests/tools/fixtures/__init__.py",
                            lineno=0,
                            col_offset=0,
                        )
                    ]
                },
                "recidiviz.tests.tools.fixtures": {
                    "recidiviz.tests.tools.fixtures.example_dependency_entrypoint": [
                        Callsite(
                            filepath="r/tests/tools/fixtures/example_dependency_entrypoint.py",
                            lineno=0,
                            col_offset=0,
                        )
                    ]
                },
                "recidiviz.tests.tools.fixtures.example_dependency_entrypoint": {},
                "recidiviz.utils": {
                    "recidiviz.utils.environment": [
                        Callsite(
                            filepath="r/utils/environment.py", lineno=0, col_offset=0
                        )
                    ],
                    "recidiviz.utils.metadata": [
                        Callsite(filepath="r/utils/metadata.py", lineno=0, col_offset=0)
                    ],
                    "recidiviz.utils.secrets": [
                        Callsite(filepath="r/utils/secrets.py", lineno=0, col_offset=0)
                    ],
                    "recidiviz.utils.types": [
                        Callsite(filepath="r/utils/types.py", lineno=0, col_offset=0)
                    ],
                },
                "recidiviz.utils.environment": {
                    "recidiviz.utils.metadata": [
                        Callsite(
                            filepath="r/utils/metadata.py", lineno=28, col_offset=0
                        ),
                        Callsite(
                            filepath="r/utils/metadata.py", lineno=29, col_offset=0
                        ),
                    ],
                    "recidiviz.utils.secrets": [
                        Callsite(
                            filepath="r/utils/secrets.py", lineno=28, col_offset=0
                        ),
                        Callsite(
                            filepath="r/utils/secrets.py", lineno=29, col_offset=0
                        ),
                    ],
                },
                "recidiviz.utils.metadata": {
                    "recidiviz.utils.secrets": [
                        Callsite(filepath="r/utils/secrets.py", lineno=28, col_offset=0)
                    ]
                },
                "recidiviz.utils.secrets": {
                    "recidiviz.tests.tools.fixtures.example_dependency_entrypoint": [
                        Callsite(
                            filepath="r/tests/tools/fixtures/example_dependency_entrypoint.py",
                            lineno=22,
                            col_offset=0,
                        )
                    ]
                },
                "recidiviz.utils.types": {
                    "recidiviz.common.date": [
                        Callsite(filepath="r/common/date.py", lineno=41, col_offset=0)
                    ]
                },
            },
            packages={
                "abc": {
                    "recidiviz.common.date": [
                        Callsite(filepath="r/common/date.py", lineno=21, col_offset=0)
                    ]
                },
                "attr": {
                    "recidiviz.common.date": [
                        Callsite(filepath="r/common/date.py", lineno=36, col_offset=0)
                    ],
                    "recidiviz.utils.metadata": [
                        Callsite(
                            filepath="r/utils/metadata.py", lineno=25, col_offset=0
                        )
                    ],
                    "recidiviz.common.attr_validators": [
                        Callsite(
                            filepath="r/common/attr_validators.py",
                            lineno=30,
                            col_offset=0,
                        )
                    ],
                },
                "calendar": {
                    "recidiviz.common.date": [
                        Callsite(filepath="r/common/date.py", lineno=22, col_offset=0)
                    ]
                },
                "datetime": {
                    "recidiviz.common.date": [
                        Callsite(filepath="r/common/date.py", lineno=18, col_offset=0)
                    ],
                    "recidiviz.common.attr_validators": [
                        Callsite(
                            filepath="r/common/attr_validators.py",
                            lineno=26,
                            col_offset=0,
                        )
                    ],
                },
                "enum": {
                    "recidiviz.utils.environment": [
                        Callsite(
                            filepath="r/utils/environment.py", lineno=28, col_offset=0
                        )
                    ],
                    "recidiviz.utils.metadata": [
                        Callsite(
                            filepath="r/utils/metadata.py", lineno=19, col_offset=0
                        )
                    ],
                },
                "functools": {
                    "recidiviz.utils.environment": [
                        Callsite(
                            filepath="r/utils/environment.py", lineno=29, col_offset=0
                        )
                    ]
                },
                "google": {
                    "recidiviz.utils.secrets": [
                        Callsite(
                            filepath="r/utils/secrets.py", lineno=25, col_offset=0
                        ),
                        Callsite(
                            filepath="r/utils/secrets.py", lineno=26, col_offset=0
                        ),
                    ]
                },
                "importlib": {
                    "recidiviz.utils.environment": [
                        Callsite(
                            filepath="r/utils/environment.py", lineno=30, col_offset=0
                        )
                    ]
                },
                "json": {
                    "recidiviz.utils.metadata": [
                        Callsite(
                            filepath="r/utils/metadata.py", lineno=20, col_offset=0
                        )
                    ]
                },
                "logging": {
                    "recidiviz.utils.environment": [
                        Callsite(
                            filepath="r/utils/environment.py", lineno=25, col_offset=0
                        )
                    ],
                    "recidiviz.utils.metadata": [
                        Callsite(
                            filepath="r/utils/metadata.py", lineno=21, col_offset=0
                        )
                    ],
                    "recidiviz.utils.secrets": [
                        Callsite(filepath="r/utils/secrets.py", lineno=20, col_offset=0)
                    ],
                },
                "os": {
                    "recidiviz.utils.environment": [
                        Callsite(
                            filepath="r/utils/environment.py", lineno=26, col_offset=0
                        )
                    ],
                    "recidiviz.utils.metadata": [
                        Callsite(
                            filepath="r/utils/metadata.py", lineno=22, col_offset=0
                        )
                    ],
                    "recidiviz.utils.secrets": [
                        Callsite(filepath="r/utils/secrets.py", lineno=21, col_offset=0)
                    ],
                },
                "pandas": {
                    "recidiviz.common.date": [
                        Callsite(filepath="r/common/date.py", lineno=37, col_offset=0)
                    ]
                },
                "pathlib": {
                    "recidiviz.utils.secrets": [
                        Callsite(filepath="r/utils/secrets.py", lineno=22, col_offset=0)
                    ]
                },
                "re": {
                    "recidiviz.common.date": [
                        Callsite(filepath="r/common/date.py", lineno=20, col_offset=0)
                    ],
                    "recidiviz.common.attr_validators": [
                        Callsite(
                            filepath="r/common/attr_validators.py",
                            lineno=27,
                            col_offset=0,
                        )
                    ],
                },
                "requests": {
                    "recidiviz.tests.tools.fixtures.example_dependency_entrypoint": [
                        Callsite(
                            filepath="r/tests/tools/fixtures/example_dependency_entrypoint.py",
                            lineno=19,
                            col_offset=0,
                        )
                    ],
                    "recidiviz.utils.metadata": [
                        Callsite(
                            filepath="r/utils/metadata.py", lineno=26, col_offset=0
                        )
                    ],
                },
                "sys": {
                    "recidiviz.utils.environment": [
                        Callsite(
                            filepath="r/utils/environment.py", lineno=27, col_offset=0
                        )
                    ]
                },
                "tempfile": {
                    "recidiviz.tests.tools.fixtures.example_dependency_entrypoint": [
                        Callsite(
                            filepath="r/tests/tools/fixtures/example_dependency_entrypoint.py",
                            lineno=32,
                            col_offset=8,
                        )
                    ]
                },
                "typing": {
                    "recidiviz.common.attr_validators": [
                        Callsite(
                            filepath="r/common/attr_validators.py",
                            lineno=28,
                            col_offset=0,
                        )
                    ],
                    "recidiviz.common.date": [
                        Callsite(filepath="r/common/date.py", lineno=23, col_offset=0)
                    ],
                    "recidiviz.utils.environment": [
                        Callsite(
                            filepath="r/utils/environment.py", lineno=31, col_offset=0
                        )
                    ],
                    "recidiviz.utils.metadata": [
                        Callsite(
                            filepath="r/utils/metadata.py", lineno=23, col_offset=0
                        )
                    ],
                    "recidiviz.utils.secrets": [
                        Callsite(filepath="r/utils/secrets.py", lineno=23, col_offset=0)
                    ],
                    "recidiviz.utils.types": [
                        Callsite(filepath="r/utils/types.py", lineno=19, col_offset=0)
                    ],
                },
                "pytz": {
                    "recidiviz.common.attr_validators": [
                        Callsite(
                            filepath="r/common/attr_validators.py",
                            lineno=31,
                            col_offset=0,
                        )
                    ],
                    "recidiviz.common.date": [
                        Callsite(filepath="r/common/date.py", lineno=38, col_offset=0)
                    ],
                },
                "itertools": {
                    "recidiviz.common.date": [
                        Callsite(filepath="r/common/date.py", lineno=19, col_offset=0)
                    ]
                },
            },
        )
        # Asserting the name of the key and the match
        # makes this a bit easier to debug/see diffs
        for module in actual_dependencies.modules:
            assert module and (
                actual_dependencies.modules[module]
                == expected_dependencies.modules[module]
            )
        for package in actual_dependencies.packages:
            assert package and (
                actual_dependencies.packages[package]
                == expected_dependencies.packages[package]
            )
        for module in expected_dependencies.modules:
            assert module and (
                expected_dependencies.modules[module]
                == actual_dependencies.modules[module]
            )
        for package in expected_dependencies.packages:
            assert package and (
                expected_dependencies.packages[package]
                == actual_dependencies.packages[package]
            )

    def test_add_dependencies_missing_init(self) -> None:
        with self.assertRaisesRegex(
            FileNotFoundError,
            r"recidiviz/tests/tools/fixtures/missing_init/__init__\.py",
        ):
            EntrypointDependencies().add_dependencies_for_entrypoint(
                "recidiviz.tests.tools.fixtures.missing_init.test"
            )

    def test_call_chain_for_module(self) -> None:
        deps = EntrypointDependencies().add_dependencies_for_entrypoint(
            "recidiviz.tests.tools.fixtures.example_dependency_entrypoint"
        )
        assert deps.sample_call_chain_for_module("recidiviz.utils.environment") == [
            ("recidiviz.utils.secrets", Callsite("r/utils/secrets.py", 28, 0)),
            (
                "recidiviz.tests.tools.fixtures.example_dependency_entrypoint",
                Callsite(
                    "r/tests/tools/fixtures/example_dependency_entrypoint.py", 22, 0
                ),
            ),
        ]

    def test_call_chain_for_parent(self) -> None:
        deps = EntrypointDependencies().add_dependencies_for_entrypoint(
            "recidiviz.tests.tools.fixtures.example_dependency_entrypoint"
        )
        assert deps.sample_call_chain_for_module("recidiviz.common") == [
            ("recidiviz.common.date", Callsite("r/common/date.py", 0, 0)),
            (
                "recidiviz.tests.tools.fixtures.example_dependency_entrypoint",
                Callsite(
                    "r/tests/tools/fixtures/example_dependency_entrypoint.py", 21, 0
                ),
            ),
        ]

    def test_call_chain_for_recidiviz(self) -> None:
        deps = EntrypointDependencies().add_dependencies_for_entrypoint(
            "recidiviz.tests.tools.fixtures.example_dependency_entrypoint"
        )
        assert deps.sample_call_chain_for_module("recidiviz") == [
            ("recidiviz.utils.environment", Callsite("r/utils/environment.py", 33, 0)),
            ("recidiviz.utils.secrets", Callsite("r/utils/secrets.py", 28, 0)),
            (
                "recidiviz.tests.tools.fixtures.example_dependency_entrypoint",
                Callsite(
                    "r/tests/tools/fixtures/example_dependency_entrypoint.py", 22, 0
                ),
            ),
        ]

    def test_call_chain_for_module_self(self) -> None:
        deps = EntrypointDependencies().add_dependencies_for_entrypoint(
            "recidiviz.tests.tools.fixtures.example_dependency_entrypoint"
        )

        assert not deps.sample_call_chain_for_module(
            "recidiviz.tests.tools.fixtures.example_dependency_entrypoint"
        )

    def test_call_chain_for_module_unused(self) -> None:
        deps = EntrypointDependencies().add_dependencies_for_entrypoint(
            "recidiviz.tests.tools.fixtures.example_dependency_entrypoint"
        )
        with self.assertRaisesRegex(KeyError, r"recidiviz\.utils\.string"):
            deps.sample_call_chain_for_module("recidiviz.utils.string")

    def test_call_chain_for_package(self) -> None:
        deps = EntrypointDependencies().add_dependencies_for_entrypoint(
            "recidiviz.tests.tools.fixtures.example_dependency_entrypoint"
        )
        assert deps.sample_call_chain_for_package("re") == [
            ("recidiviz.common.date", Callsite("r/common/date.py", 20, 0)),
            (
                "recidiviz.tests.tools.fixtures.example_dependency_entrypoint",
                Callsite(
                    "r/tests/tools/fixtures/example_dependency_entrypoint.py", 21, 0
                ),
            ),
        ]

    def test_call_chain_for_package_unused(self) -> None:
        deps = EntrypointDependencies().add_dependencies_for_entrypoint(
            "recidiviz.tests.tools.fixtures.example_dependency_entrypoint"
        )
        with self.assertRaisesRegex(KeyError, r"twilio"):
            deps.sample_call_chain_for_package("twilio")

    def test_call_chain_for_package_module(self) -> None:
        deps = EntrypointDependencies().add_dependencies_for_entrypoint(
            "recidiviz.tests.tools.fixtures.example_dependency_entrypoint"
        )
        with self.assertRaisesRegex(KeyError, r"recidiviz\.utils\.secrets"):
            deps.sample_call_chain_for_package("recidiviz.utils.secrets")

    def test_parent_package(self) -> None:
        assert parent_package("recidiviz.utils.secrets") == "recidiviz.utils"

    def test_parent_package_recidiviz(self) -> None:
        assert parent_package("recidiviz") is None

    def test_parent_package_other_package(self) -> None:
        assert parent_package("twilio") is None

    def test_parent_package_other_module(self) -> None:
        assert parent_package("twilio.sms") == "twilio"


class DynamicallyCollectedFileDependenciesTest(unittest.TestCase):
    """Unit tests for finding dynamically collected file dependencies"""

    def test_collect_modules_for_dynamic_collection__simple(self) -> None:
        modules = _collect_modules_for_dynamic_collection(
            base_path="recidiviz/tests/tools/fixtures", glob_pattern="a/*/*.py"
        )

        assert set(modules) == {
            "recidiviz.tests.tools.fixtures.a.b",
            "recidiviz.tests.tools.fixtures.a.b.b",
            "recidiviz.tests.tools.fixtures.a.b.d",
            "recidiviz.tests.tools.fixtures.a.b.e",
            "recidiviz.tests.tools.fixtures.a.b.i",
        }

    def test_collect_modules_for_dynamic_collection__empty(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"glob pattern \[.*\] returned no matches",
        ):

            _collect_modules_for_dynamic_collection(
                base_path="recidiviz/tests/tools/fixtures", glob_pattern="a/e/*.py"
            )

    @staticmethod
    def ingest_view_builder_collectors(
        module: ModuleType,
    ) -> list[tuple[type, Callable]]:
        return [
            (
                module.DirectIngestViewQueryBuilderCollector,
                module.DirectIngestViewQueryBuilderCollector.from_state_code(
                    state_code
                ).get_query_builders,
            )
            for state_code in get_existing_direct_ingest_states()
        ]

    @staticmethod
    def migration_view_builder_collectors(
        module: ModuleType,
    ) -> list[tuple[type, Callable]]:
        return [
            (
                module.DirectIngestRawTableMigrationCollector,
                module.DirectIngestRawTableMigrationCollector(
                    region_code=state_code.value,
                    # this doesn't matter
                    instance=DirectIngestInstance.PRIMARY,
                ).collect_raw_table_migrations,
            )
            for state_code in get_existing_direct_ingest_states()
        ]

    def test_all_defined_modules_only_match_python_files(self) -> None:
        def _add_module_to_cache(
            module_name: str, *, imported_modules: set[str]
        ) -> None:
            if module_name.startswith(recidiviz.__name__):
                imported_modules.add(module_name)

        for (
            module_str,
            module_patterns,
        ) in DYNAMICALLY_COLLECTED_PYTHON_DEPENDENCIES.items():
            for path, pattern in module_patterns:
                assert pattern.endswith(".py")
                assert path.startswith(recidiviz.__name__)
                assert module_str.startswith(recidiviz.__name__)

            collection_classes: list[tuple[type, Callable]]
            module = importlib.import_module(module_str)

            match module_str:
                case "recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector":
                    collection_classes = self.ingest_view_builder_collectors(module)
                case "recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_migration_collector":
                    collection_classes = self.migration_view_builder_collectors(module)
                case _:
                    raise ValueError(
                        "Please add mapping for dynamic collection module to ModuleCollectionMixin subclass"
                    )

            assert len(collection_classes) != 0

            imported_modules: set[str] = set()

            _add = partial(_add_module_to_cache, imported_modules=imported_modules)

            # we use before to capture the calls to importlib.import_module while
            # still allowing the module to be imported
            with local_project_id_override("recidiviz-456"), before(
                "recidiviz.common.module_collector_mixin.importlib.import_module",
                _add,
                once=False,
            ):
                for (collection_class, collection_callable) in collection_classes:
                    assert issubclass(collection_class, ModuleCollectorMixin)
                    _ = collection_callable()

            glob_collection = set(
                result
                for (path, pattern) in module_patterns
                for result in _collect_modules_for_dynamic_collection(path, pattern)
            )

            self.assertSetEqual(
                imported_modules - glob_collection,
                set(),
                msg=f"Found modules that were imported via [{collection_classes[0][0]}]'s "
                f"dynamic module collection that were not captured by the provided glob "
                f"patterns for [{module_str}]",
            )

            self.assertSetEqual(
                glob_collection - imported_modules,
                set(),
                msg=f"Found modules that were matched by glob patterns for [{module_str}] "
                f"that were not actually imported by [{collection_classes[0][0]}]'s "
                f"dynamic module collection.",
            )
