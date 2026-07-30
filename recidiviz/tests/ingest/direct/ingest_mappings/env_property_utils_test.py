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
"""Tests for the `$env` property helpers used by the ingest mappings compiler."""
import unittest

from recidiviz.ingest.direct.feature_flags_registry import all_feature_flag_names
from recidiviz.ingest.direct.ingest_mappings.env_property_utils import (
    BUILT_IN_ENV_PROPERTY_NAMES,
    FEATURE_FLAG_ENV_PROPERTY_PREFIX,
    env_property_name_for_flag,
    env_property_type,
    feature_flag_for_env_property,
    is_feature_flag_env_property,
)


def _all_env_property_names() -> list[str]:
    """Returns the sorted set of all valid `$env` property names, including both
    the built-in environment properties and every registered feature flag.

    Lives here rather than in production code because nothing in production needs
    the full list -- the compiler validates one property name at a time.
    """
    return sorted(
        [
            *BUILT_IN_ENV_PROPERTY_NAMES,
            *(
                env_property_name_for_flag(flag_name)
                for flag_name in all_feature_flag_names()
            ),
        ]
    )


class TestEnvPropertyUtils(unittest.TestCase):
    """Tests for the `$env` property helpers used by the ingest mappings compiler."""

    def test_registered_flag_names_do_not_include_prefix(self) -> None:
        for flag_name in all_feature_flag_names():
            self.assertFalse(
                flag_name.startswith(FEATURE_FLAG_ENV_PROPERTY_PREFIX),
                f"Feature flag [{flag_name}] must not include the "
                f"[{FEATURE_FLAG_ENV_PROPERTY_PREFIX}] prefix - that prefix is "
                f"added only when referencing the flag from ingest mapping YAML.",
            )

    def test_env_property_name_round_trip(self) -> None:
        for flag_name in [
            *all_feature_flag_names(),
            "some_unregistered_flag",
        ]:
            with self.subTest(flag_name=flag_name):
                property_name = env_property_name_for_flag(flag_name)
                self.assertTrue(is_feature_flag_env_property(property_name))
                self.assertEqual(
                    flag_name, feature_flag_for_env_property(property_name)
                )

    def test_built_in_properties_are_not_feature_flags(self) -> None:
        for property_name in BUILT_IN_ENV_PROPERTY_NAMES:
            self.assertFalse(is_feature_flag_env_property(property_name))
            self.assertIsNone(feature_flag_for_env_property(property_name))

    def test_bare_prefix_throws(self) -> None:
        expected_error = (
            rf"^Found `\$env` property \[{FEATURE_FLAG_ENV_PROPERTY_PREFIX}\] with "
            rf"the feature flag prefix \[{FEATURE_FLAG_ENV_PROPERTY_PREFIX}\] "
            rf"but no flag name after it\.$"
        )
        with self.assertRaisesRegex(ValueError, expected_error):
            feature_flag_for_env_property(FEATURE_FLAG_ENV_PROPERTY_PREFIX)
        with self.assertRaisesRegex(ValueError, expected_error):
            is_feature_flag_env_property(FEATURE_FLAG_ENV_PROPERTY_PREFIX)
        with self.assertRaisesRegex(ValueError, expected_error):
            env_property_type(FEATURE_FLAG_ENV_PROPERTY_PREFIX)

    def test_all_env_property_names_sorted_and_unique(self) -> None:
        property_names = _all_env_property_names()
        self.assertCountEqual(set(property_names), property_names)
        self.assertEqual(sorted(property_names), property_names)

    def test_env_property_type_valid_names(self) -> None:
        for property_name in _all_env_property_names():
            with self.subTest(property_name=property_name):
                self.assertEqual(bool, env_property_type(property_name))

    def test_env_property_type_allows_unregistered_flag(self) -> None:
        """The compiler validates only the shape of a feature flag reference.
        That the flag is actually registered is enforced by
        direct_ingest_region_structure_test.py.
        """
        self.assertEqual(
            bool,
            env_property_type(env_property_name_for_flag("some_unregistered_flag")),
        )

    def test_env_property_type_unknown_name_throws(self) -> None:
        for property_name in [
            "not_a_property",
            # A single underscore is not the feature flag prefix.
            "feature_tomis_2_0_enabled",
        ]:
            with self.subTest(property_name=property_name):
                with self.assertRaisesRegex(
                    ValueError,
                    rf"^Unexpected environment property: \[{property_name}\]$",
                ):
                    env_property_type(property_name)
