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
import os
import unittest
from typing import Any

from mock import patch

import recidiviz
from recidiviz.tests.tools.fixtures import test_importing
from recidiviz.tools.file_dependencies import (
    Callsite,
    EntrypointDependencies,
    parent_package,
)

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
        assert EntrypointDependencies().add_dependencies_for_entrypoint(
            "recidiviz.tests.tools.fixtures.example_dependency_entrypoint"
        ) == EntrypointDependencies(
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
                "recidiviz.common": {
                    "recidiviz.common.date": [
                        Callsite(filepath="r/common/date.py", lineno=0, col_offset=0)
                    ]
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
                            filepath="r/utils/metadata.py", lineno=27, col_offset=0
                        ),
                        Callsite(
                            filepath="r/utils/metadata.py", lineno=28, col_offset=0
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
                        Callsite(filepath="r/common/date.py", lineno=27, col_offset=0)
                    ]
                },
            },
            packages={
                "abc": {
                    "recidiviz.common.date": [
                        Callsite(filepath="r/common/date.py", lineno=20, col_offset=0)
                    ]
                },
                "attr": {
                    "recidiviz.common.date": [
                        Callsite(filepath="r/common/date.py", lineno=24, col_offset=0)
                    ],
                    "recidiviz.utils.metadata": [
                        Callsite(
                            filepath="r/utils/metadata.py", lineno=24, col_offset=0
                        )
                    ],
                },
                "calendar": {
                    "recidiviz.common.date": [
                        Callsite(filepath="r/common/date.py", lineno=21, col_offset=0)
                    ]
                },
                "datetime": {
                    "recidiviz.common.date": [
                        Callsite(filepath="r/common/date.py", lineno=18, col_offset=0)
                    ]
                },
                "enum": {
                    "recidiviz.utils.environment": [
                        Callsite(
                            filepath="r/utils/environment.py", lineno=28, col_offset=0
                        )
                    ]
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
                            filepath="r/utils/metadata.py", lineno=19, col_offset=0
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
                            filepath="r/utils/metadata.py", lineno=20, col_offset=0
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
                            filepath="r/utils/metadata.py", lineno=21, col_offset=0
                        )
                    ],
                    "recidiviz.utils.secrets": [
                        Callsite(filepath="r/utils/secrets.py", lineno=21, col_offset=0)
                    ],
                },
                "pandas": {
                    "recidiviz.common.date": [
                        Callsite(filepath="r/common/date.py", lineno=25, col_offset=0)
                    ]
                },
                "pathlib": {
                    "recidiviz.utils.secrets": [
                        Callsite(filepath="r/utils/secrets.py", lineno=22, col_offset=0)
                    ]
                },
                "re": {
                    "recidiviz.common.date": [
                        Callsite(filepath="r/common/date.py", lineno=19, col_offset=0)
                    ]
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
                            filepath="r/utils/metadata.py", lineno=25, col_offset=0
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
                    "recidiviz.common.date": [
                        Callsite(filepath="r/common/date.py", lineno=22, col_offset=0)
                    ],
                    "recidiviz.utils.environment": [
                        Callsite(
                            filepath="r/utils/environment.py", lineno=31, col_offset=0
                        )
                    ],
                    "recidiviz.utils.metadata": [
                        Callsite(
                            filepath="r/utils/metadata.py", lineno=22, col_offset=0
                        )
                    ],
                    "recidiviz.utils.secrets": [
                        Callsite(filepath="r/utils/secrets.py", lineno=23, col_offset=0)
                    ],
                    "recidiviz.utils.types": [
                        Callsite(filepath="r/utils/types.py", lineno=19, col_offset=0)
                    ],
                },
            },
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
            ("recidiviz.common.date", Callsite("r/common/date.py", 19, 0)),
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
