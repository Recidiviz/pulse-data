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
"""Tests for YAMLDict."""
import unittest

from recidiviz.utils.yaml_dict import YAMLDict


class TestYAMLDict(unittest.TestCase):
    """Tests for YAMLDict."""

    def test_pop_optionals_no_field(self) -> None:
        yaml_dict = YAMLDict(raw_yaml={"my_key": "my_value"})
        self.assertIsNone(yaml_dict.peek_optional("missing_key", str))
        self.assertIsNone(yaml_dict.pop_optional("missing_key", str))
        self.assertIsNone(yaml_dict.pop_dict_optional("missing_key"))
        self.assertIsNone(yaml_dict.pop_dicts_optional("missing_key"))
        self.assertIsNone(yaml_dict.pop_list_optional("missing_key", str))

    def test_pop_optional_none_value(self) -> None:
        yaml_dict = YAMLDict(raw_yaml={"my_key": None})
        self.assertIsNone(yaml_dict.peek_optional("my_key", str))
        self.assertIsNone(yaml_dict.pop_optional("my_key", str))
        self.assertIsNone(yaml_dict.pop_dict_optional("my_key"))
        self.assertIsNone(yaml_dict.pop_dicts_optional("my_key"))
        self.assertIsNone(yaml_dict.pop_list_optional("my_key", str))

    def test_pop_none_value_throws(self) -> None:
        key = "my_key"

        yaml_dict = YAMLDict(raw_yaml={key: None})
        with self.assertRaisesRegex(
            ValueError, r"The field \[my_key\] must be of type \[<class 'object'>\]"
        ):
            _ = yaml_dict.peek_type(key)

        yaml_dict = YAMLDict(raw_yaml={key: None})
        with self.assertRaisesRegex(
            ValueError, r"The field \[my_key\] must be of type \[<class 'str'>\]"
        ):
            _ = yaml_dict.peek(key, str)

        yaml_dict = YAMLDict(raw_yaml={key: None})
        with self.assertRaisesRegex(
            ValueError, r"The field \[my_key\] must be of type \[<class 'str'>\]"
        ):
            _ = yaml_dict.pop(key, str)

        yaml_dict = YAMLDict(raw_yaml={key: None})
        with self.assertRaisesRegex(
            ValueError, r"The field \[my_key\] must be of type \[<class 'dict'>\]"
        ):
            _ = yaml_dict.pop_dict(key)

        yaml_dict = YAMLDict(raw_yaml={key: None})
        with self.assertRaisesRegex(
            ValueError, r"The field \[my_key\] must be of type \[<class 'list'>\]"
        ):
            _ = yaml_dict.pop_dicts(key)

        yaml_dict = YAMLDict(raw_yaml={key: None})
        with self.assertRaisesRegex(
            ValueError, r"The field \[my_key\] must be of type \[<class 'list'>\]"
        ):
            _ = yaml_dict.pop_list(key, str)

    def test_pop_missing_key_throws(self) -> None:
        key = "my_key"
        error_regex = r"Expected nonnull \[my_key\] in input"

        yaml_dict = YAMLDict(raw_yaml={})
        with self.assertRaisesRegex(KeyError, error_regex):
            _ = yaml_dict.peek_type(key)

        yaml_dict = YAMLDict(raw_yaml={})
        with self.assertRaisesRegex(KeyError, error_regex):
            _ = yaml_dict.peek(key, str)

        yaml_dict = YAMLDict(raw_yaml={})
        with self.assertRaisesRegex(KeyError, error_regex):
            _ = yaml_dict.pop(key, str)

        yaml_dict = YAMLDict(raw_yaml={})
        with self.assertRaisesRegex(KeyError, error_regex):
            _ = yaml_dict.pop_dict(key)

        yaml_dict = YAMLDict(raw_yaml={})
        with self.assertRaisesRegex(KeyError, error_regex):
            _ = yaml_dict.pop_dicts(key)

        yaml_dict = YAMLDict(raw_yaml={})
        with self.assertRaisesRegex(KeyError, error_regex):
            _ = yaml_dict.pop_list(key, str)

    def test_pop(self) -> None:
        yaml_dict = YAMLDict(
            raw_yaml={
                "my_key": "my_str",
                "bad_type_str": 123,
            }
        )

        self.assertEqual(str, yaml_dict.peek_type("my_key"))
        self.assertEqual(int, yaml_dict.peek_type("bad_type_str"))
        self.assertEqual("my_str", yaml_dict.pop("my_key", str))
        with self.assertRaisesRegex(
            ValueError,
            r"Invalid \[bad_type_str\] value, expected type \[<class 'str'>\] but "
            r"received: ",
        ):
            _ = yaml_dict.pop("bad_type_str", str)
        self.assertEqual(0, len(yaml_dict))

    def test_pop_optional(self) -> None:
        yaml_dict = YAMLDict(
            raw_yaml={
                "my_key": "my_str",
                "bad_type_str": 123,
            }
        )

        self.assertEqual(str, yaml_dict.peek_type("my_key"))
        self.assertEqual(int, yaml_dict.peek_type("bad_type_str"))
        self.assertEqual("my_str", yaml_dict.pop_optional("my_key", str))
        with self.assertRaisesRegex(
            ValueError,
            r"Invalid \[bad_type_str\] value, expected type \[<class 'str'>\] but "
            r"received: ",
        ):
            _ = yaml_dict.pop_optional("bad_type_str", str)
        self.assertEqual(0, len(yaml_dict))

    def test_pop_dicts(self) -> None:
        yaml_dict = YAMLDict(
            raw_yaml={
                "empty_list": [],
                "my_key": [{"inner_key": "inner_value"}],
                "bad_types": ["my_str", {}],
            }
        )

        self.assertEqual(list, yaml_dict.peek_type("empty_list"))
        self.assertEqual(list, yaml_dict.peek_type("my_key"))
        self.assertEqual([], yaml_dict.pop_dicts("empty_list"))
        self.assertEqual(
            [YAMLDict({"inner_key": "inner_value"})], yaml_dict.pop_dicts("my_key")
        )
        with self.assertRaisesRegex(
            ValueError,
            r"Invalid \[bad_types\] value, expected type \[<class 'dict'>\] but "
            r"received: <class 'str'>",
        ):
            _ = yaml_dict.pop_dicts("bad_types")
        self.assertEqual(0, len(yaml_dict))

    def test_pop_dicts_optional(self) -> None:
        yaml_dict = YAMLDict(
            raw_yaml={
                "empty_list": [],
                "my_key": [{"inner_key": "inner_value"}],
                "bad_types": ["my_str", {}],
            }
        )

        self.assertEqual(list, yaml_dict.peek_type("empty_list"))
        self.assertEqual(list, yaml_dict.peek_type("my_key"))
        self.assertEqual([], yaml_dict.pop_dicts_optional("empty_list"))
        self.assertEqual(
            [YAMLDict({"inner_key": "inner_value"})],
            yaml_dict.pop_dicts_optional("my_key"),
        )

        with self.assertRaisesRegex(
            ValueError,
            r"Invalid \[bad_types\] value, expected type \[<class 'dict'>\] but "
            r"received: <class 'str'>",
        ):
            _ = yaml_dict.pop_dicts_optional("bad_types")
        self.assertEqual(0, len(yaml_dict))

    def test_pop_dict(self) -> None:
        yaml_dict = YAMLDict(
            raw_yaml={"empty_dict": {}, "my_key": {"inner_key": "inner_value"}}
        )

        self.assertEqual(dict, yaml_dict.peek_type("empty_dict"))
        self.assertEqual(dict, yaml_dict.peek_type("my_key"))
        self.assertEqual(YAMLDict({}), yaml_dict.pop_dict("empty_dict"))
        self.assertEqual(
            YAMLDict({"inner_key": "inner_value"}), yaml_dict.pop_dict("my_key")
        )
        self.assertEqual(0, len(yaml_dict))

    def test_pop_dict_optional(self) -> None:
        yaml_dict = YAMLDict(
            raw_yaml={"empty_dict": {}, "my_key": {"inner_key": "inner_value"}}
        )

        self.assertEqual(dict, yaml_dict.peek_type("empty_dict"))
        self.assertEqual(dict, yaml_dict.peek_type("my_key"))
        self.assertEqual(YAMLDict({}), yaml_dict.pop_dict_optional("empty_dict"))
        self.assertEqual(
            YAMLDict({"inner_key": "inner_value"}),
            yaml_dict.pop_dict_optional("my_key"),
        )
        self.assertEqual(0, len(yaml_dict))

    def test_pop_list(self) -> None:
        yaml_dict = YAMLDict(
            raw_yaml={
                "empty_list": [],
                "my_key": ["foo", "bar"],
                "list_bad_values": ["foo", 8],
            }
        )

        self.assertEqual([], yaml_dict.pop_list("empty_list", str))
        self.assertEqual(["foo", "bar"], yaml_dict.pop_list("my_key", str))

        with self.assertRaisesRegex(
            ValueError,
            r"Invalid \[list_bad_values\] value, expected type \[<class 'int'>\] "
            r"but received: <class 'str'>",
        ):
            _ = yaml_dict.pop_list("list_bad_values", int)
        self.assertEqual(0, len(yaml_dict))

    def test_pop_list_optional(self) -> None:
        yaml_dict = YAMLDict(
            raw_yaml={
                "empty_list": [],
                "my_key": ["foo", "bar"],
                "list_bad_values": ["foo", 8],
            }
        )

        self.assertEqual([], yaml_dict.pop_list_optional("empty_list", str))
        self.assertEqual(["foo", "bar"], yaml_dict.pop_list_optional("my_key", str))
        with self.assertRaisesRegex(
            ValueError,
            r"Invalid \[list_bad_values\] value, expected type \[<class 'int'>\] "
            r"but received: <class 'str'>",
        ):
            _ = yaml_dict.pop_list_optional("list_bad_values", int)
        self.assertEqual(0, len(yaml_dict))
