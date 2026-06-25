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
"""Tests for llm_model_registry.py."""
import re
from unittest import TestCase

import attr

from recidiviz.documents.extraction.models.llm_model_registry import (
    BaseLLMModel,
    LLMAPIProvider,
    LLMModelConfig,
    LLMModelFamily,
    LLMModelFloatParameterDefinition,
    LLMModelFloatParameterValue,
    LLMModelIntegerParameterDefinition,
    LLMModelIntegerParameterValue,
    LLMModelRegistry,
    load_llm_model_registry,
)
from recidiviz.tests.documents import fake_config
from recidiviz.tests.ingest import fixtures

_ACME_FAMILY = LLMModelFamily(
    name="acme", default_api_provider=LLMAPIProvider.VERTEX_AI
)
_GLOBEX_FAMILY = LLMModelFamily(
    name="globex", default_api_provider=LLMAPIProvider.VERTEX_AI
)

_ACME_SMALL_TEMPERATURE_PARAMETER = LLMModelFloatParameterDefinition(
    name="temperature",
    min_value_inclusive=0,
    max_value_inclusive=2,
    description="Controls randomness. 0.0 = deterministic.",
    is_optional=False,
)
_ACME_SMALL_MAX_OUTPUT_TOKENS_PARAMETER = LLMModelIntegerParameterDefinition(
    name="max_output_tokens",
    min_value_inclusive=1,
    max_value_inclusive=100,
    description="Maximum tokens in the response.",
    is_optional=False,
)
_ACME_LARGE_TEMPERATURE_PARAMETER = LLMModelFloatParameterDefinition(
    name="temperature",
    min_value_inclusive=0.0,
    max_value_inclusive=2.0,
    description="Controls randomness. 0.0 = deterministic.",
    is_optional=False,
)
_ACME_LARGE_THINKING_BUDGET_TOKENS_PARAMETER = LLMModelIntegerParameterDefinition(
    name="thinking_budget_tokens",
    min_value_inclusive=0,
    max_value_inclusive=1024,
    description=(
        "Maximum tokens for internal reasoning. May be omitted to use dynamic "
        "thinking."
    ),
    is_optional=True,
)

_ACME_SMALL = BaseLLMModel(
    name="acme-small",
    family=_ACME_FAMILY,
    is_thinking_model=False,
    input_token_limit=10000,
    supports_structured_output=True,
    supports_implicit_caching_in_batch=False,
    known_pinned_versions=["acme-small-001", "acme-small-002"],
    parameters={
        "temperature": _ACME_SMALL_TEMPERATURE_PARAMETER,
        "max_output_tokens": _ACME_SMALL_MAX_OUTPUT_TOKENS_PARAMETER,
    },
)
_ACME_LARGE = BaseLLMModel(
    name="acme-large",
    family=_ACME_FAMILY,
    is_thinking_model=True,
    input_token_limit=200000,
    supports_structured_output=True,
    supports_implicit_caching_in_batch=True,
    known_pinned_versions=["acme-large-001"],
    parameters={
        "temperature": _ACME_LARGE_TEMPERATURE_PARAMETER,
        "thinking_budget_tokens": _ACME_LARGE_THINKING_BUDGET_TOKENS_PARAMETER,
    },
)
_GLOBEX_BASIC = BaseLLMModel(
    name="globex-basic",
    family=_GLOBEX_FAMILY,
    is_thinking_model=False,
    input_token_limit=5000,
    supports_structured_output=False,
    supports_implicit_caching_in_batch=False,
    known_pinned_versions=["globex-basic-001"],
    parameters={},
)


def _bad_registry_path(filename: str) -> str:
    """Returns the path to a fixture registry file that is invalid in exactly
    one way.
    """
    return fixtures.as_filepath(filename, subdir="fixtures/bad_llm_model_registry")


class LLMModelParameterDefinitionTest(TestCase):
    """Tests for the LLMModelNumericalParameterDefinition subclasses."""

    def test_validate_value_bounds_are_inclusive(self) -> None:
        _ACME_SMALL_MAX_OUTPUT_TOKENS_PARAMETER.validate_value(1)
        _ACME_SMALL_MAX_OUTPUT_TOKENS_PARAMETER.validate_value(100)
        _ACME_LARGE_TEMPERATURE_PARAMETER.validate_value(0.0)
        _ACME_LARGE_TEMPERATURE_PARAMETER.validate_value(2.0)
        # An int value is valid for a FLOAT parameter.
        _ACME_LARGE_TEMPERATURE_PARAMETER.validate_value(1)

    def test_validate_value_below_min_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Value [0] for parameter [max_output_tokens] is outside the "
                "allowed range [1, 100]."
            ),
        ):
            _ACME_SMALL_MAX_OUTPUT_TOKENS_PARAMETER.validate_value(0)

    def test_validate_value_above_max_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Value [2.5] for parameter [temperature] is outside the "
                "allowed range [0.0, 2.0]."
            ),
        ):
            _ACME_LARGE_TEMPERATURE_PARAMETER.validate_value(2.5)

    def test_min_greater_than_max_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Parameter [max_output_tokens] declares an invalid range: "
                "minimum [100] is greater than maximum [1]."
            ),
        ):
            LLMModelIntegerParameterDefinition(
                name="max_output_tokens",
                min_value_inclusive=100,
                max_value_inclusive=1,
                description="Maximum tokens in the response.",
                is_optional=False,
            )

    def test_non_integer_bound_on_integer_definition_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Field [_min_value_inclusive] on "
                "[LLMModelIntegerParameterDefinition] must be an int."
            ),
        ):
            LLMModelIntegerParameterDefinition(
                name="max_output_tokens",
                min_value_inclusive=0.5,  # type: ignore[arg-type]
                max_value_inclusive=100,
                description="Maximum tokens in the response.",
                is_optional=False,
            )


class LLMModelParameterValueTest(TestCase):
    """Tests for LLMModelIntegerParameterValue and LLMModelFloatParameterValue."""

    def test_valid_values(self) -> None:
        integer_value = LLMModelIntegerParameterValue(
            parameter_definition=_ACME_SMALL_MAX_OUTPUT_TOKENS_PARAMETER, value=50
        )
        self.assertEqual(50, integer_value.value)

        # An int value is valid for a FLOAT parameter.
        float_value = LLMModelFloatParameterValue(
            parameter_definition=_ACME_LARGE_TEMPERATURE_PARAMETER, value=1
        )
        self.assertEqual(1, float_value.value)

    def test_out_of_range_value_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Value [101] for parameter [max_output_tokens] is outside the "
                "allowed range [1, 100]."
            ),
        ):
            LLMModelIntegerParameterValue(
                parameter_definition=_ACME_SMALL_MAX_OUTPUT_TOKENS_PARAMETER, value=101
            )

    def test_bool_value_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Field [value] on [LLMModelIntegerParameterValue] must be an int."
            ),
        ):
            LLMModelIntegerParameterValue(
                parameter_definition=_ACME_SMALL_MAX_OUTPUT_TOKENS_PARAMETER, value=True
            )


class BaseLLMModelTest(TestCase):
    """Tests for BaseLLMModel."""

    def test_parameter_definition(self) -> None:
        self.assertEqual(
            _ACME_SMALL_TEMPERATURE_PARAMETER,
            _ACME_SMALL.parameter_definition("temperature"),
        )

    def test_parameter_definition_unknown_parameter_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Parameter [nonsense] is not a tunable parameter of base model "
                "[acme-small]. Allowed parameters: "
                "['max_output_tokens', 'temperature']."
            ),
        ):
            _ACME_SMALL.parameter_definition("nonsense")

    def test_duplicate_pinned_versions_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Base model [acme-small] declares duplicate pinned versions: "
                "['acme-small-001']."
            ),
        ):
            attr.evolve(
                _ACME_SMALL,
                known_pinned_versions=["acme-small-001", "acme-small-001"],
            )

    def test_parameter_key_name_mismatch_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Base model [acme-small] has a parameter registered under key "
                "[wrong_name] whose definition is named [temperature]."
            ),
        ):
            attr.evolve(
                _ACME_SMALL,
                parameters={"wrong_name": _ACME_SMALL_TEMPERATURE_PARAMETER},
            )


class LLMModelConfigTest(TestCase):
    """Tests for LLMModelConfig."""

    def test_properties_delegate_to_base_model(self) -> None:
        config = LLMModelConfig(
            name="ACME_LARGE_TEST",
            base_model=_ACME_LARGE,
            model="acme-large-001",
            parameter_values={
                "temperature": LLMModelFloatParameterValue(
                    parameter_definition=_ACME_LARGE_TEMPERATURE_PARAMETER, value=1.0
                )
            },
        )
        self.assertEqual(LLMAPIProvider.VERTEX_AI, config.api_provider)
        self.assertTrue(config.is_thinking_model)
        self.assertEqual(200000, config.input_token_limit)
        self.assertTrue(config.supports_structured_output)
        self.assertTrue(config.supports_implicit_caching_in_batch)

    def test_model_not_in_known_pinned_versions_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Model config [GLOBEX_BAD] uses model [globex-basic-999], which "
                "is not in base model [globex-basic]'s known_pinned_versions: "
                "['globex-basic-001']."
            ),
        ):
            LLMModelConfig(
                name="GLOBEX_BAD",
                base_model=_GLOBEX_BASIC,
                model="globex-basic-999",
                parameter_values={},
            )

    def test_missing_required_parameter_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Model config [ACME_SMALL_INCOMPLETE] is missing values for "
                "required parameters of base model [acme-small]: "
                "['max_output_tokens']."
            ),
        ):
            LLMModelConfig(
                name="ACME_SMALL_INCOMPLETE",
                base_model=_ACME_SMALL,
                model="acme-small-001",
                parameter_values={
                    "temperature": LLMModelFloatParameterValue(
                        parameter_definition=_ACME_SMALL_TEMPERATURE_PARAMETER,
                        value=1.0,
                    )
                },
            )

    def test_optional_parameter_may_be_omitted(self) -> None:
        config = LLMModelConfig(
            name="ACME_LARGE_DYNAMIC",
            base_model=_ACME_LARGE,
            model="acme-large-001",
            parameter_values={
                "temperature": LLMModelFloatParameterValue(
                    parameter_definition=_ACME_LARGE_TEMPERATURE_PARAMETER, value=1.0
                )
            },
        )
        self.assertNotIn("thinking_budget_tokens", config.parameter_values)


class LLMModelRegistryTest(TestCase):
    """Tests for LLMModelRegistry construction and lookups."""

    def setUp(self) -> None:
        self.config = LLMModelConfig(
            name="GLOBEX_BASIC_DEFAULT",
            base_model=_GLOBEX_BASIC,
            model="globex-basic-001",
            parameter_values={},
        )
        self.registry = LLMModelRegistry(
            model_families=[_GLOBEX_FAMILY],
            base_models=[_GLOBEX_BASIC],
            model_configs=[self.config],
        )

    def test_get_model_config(self) -> None:
        self.assertEqual(
            self.config, self.registry.get_model_config("GLOBEX_BASIC_DEFAULT")
        )

    def test_get_model_config_unknown_name_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "No model config named [NOT_A_CONFIG] in the model registry. "
                "Known configs: ['GLOBEX_BASIC_DEFAULT']."
            ),
        ):
            self.registry.get_model_config("NOT_A_CONFIG")

    def test_duplicate_family_names_raise(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape("Found multiple model families with name [globex]."),
        ):
            LLMModelRegistry(
                model_families=[_GLOBEX_FAMILY, _GLOBEX_FAMILY],
                base_models=[],
                model_configs=[],
            )

    def test_duplicate_base_model_names_raise(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape("Found multiple base models with name [globex-basic]."),
        ):
            LLMModelRegistry(
                model_families=[_GLOBEX_FAMILY],
                base_models=[_GLOBEX_BASIC, attr.evolve(_GLOBEX_BASIC)],
                model_configs=[],
            )

    def test_duplicate_model_config_names_raise(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape("Found multiple model configs with name [GLOBEX_BASIC_DEFAULT]."),
        ):
            LLMModelRegistry(
                model_families=[_GLOBEX_FAMILY],
                base_models=[_GLOBEX_BASIC],
                model_configs=[self.config, attr.evolve(self.config)],
            )


class ParseModelRegistryTest(TestCase):
    """Tests for parsing model registry YAML files."""

    def test_load_llm_model_registry(self) -> None:
        """Tests that the real, production model registry parses and validates."""
        load_llm_model_registry()

    def test_parse_fake_config_registry(self) -> None:
        """Tests the fake registry against a fully built expected instance,
        covering the valid edge cases the fixture is constructed to exercise.
        """
        expected = LLMModelRegistry(
            model_families=[_ACME_FAMILY, _GLOBEX_FAMILY],
            base_models=[_ACME_SMALL, _ACME_LARGE, _GLOBEX_BASIC],
            model_configs=[
                LLMModelConfig(
                    name="ACME_SMALL_ALL_MINS",
                    base_model=_ACME_SMALL,
                    model="acme-small-001",
                    parameter_values={
                        "temperature": LLMModelFloatParameterValue(
                            parameter_definition=_ACME_SMALL_TEMPERATURE_PARAMETER,
                            value=0,
                        ),
                        "max_output_tokens": LLMModelIntegerParameterValue(
                            parameter_definition=_ACME_SMALL_MAX_OUTPUT_TOKENS_PARAMETER,
                            value=1,
                        ),
                    },
                ),
                LLMModelConfig(
                    name="ACME_SMALL_ALL_MAXES",
                    base_model=_ACME_SMALL,
                    model="acme-small-002",
                    parameter_values={
                        "temperature": LLMModelFloatParameterValue(
                            parameter_definition=_ACME_SMALL_TEMPERATURE_PARAMETER,
                            value=2.0,
                        ),
                        "max_output_tokens": LLMModelIntegerParameterValue(
                            parameter_definition=_ACME_SMALL_MAX_OUTPUT_TOKENS_PARAMETER,
                            value=100,
                        ),
                    },
                ),
                LLMModelConfig(
                    name="ACME_LARGE_DYNAMIC_THINKING",
                    base_model=_ACME_LARGE,
                    model="acme-large-001",
                    parameter_values={
                        "temperature": LLMModelFloatParameterValue(
                            parameter_definition=_ACME_LARGE_TEMPERATURE_PARAMETER,
                            value=1.0,
                        ),
                    },
                ),
                LLMModelConfig(
                    name="ACME_LARGE_FIXED_THINKING",
                    base_model=_ACME_LARGE,
                    model="acme-large-001",
                    parameter_values={
                        "temperature": LLMModelFloatParameterValue(
                            parameter_definition=_ACME_LARGE_TEMPERATURE_PARAMETER,
                            value=0.5,
                        ),
                        "thinking_budget_tokens": LLMModelIntegerParameterValue(
                            parameter_definition=_ACME_LARGE_THINKING_BUDGET_TOKENS_PARAMETER,
                            value=512,
                        ),
                    },
                ),
                LLMModelConfig(
                    name="ACME_LARGE_NO_THINKING",
                    base_model=_ACME_LARGE,
                    model="acme-large-001",
                    parameter_values={
                        "temperature": LLMModelFloatParameterValue(
                            parameter_definition=_ACME_LARGE_TEMPERATURE_PARAMETER,
                            value=0.5,
                        ),
                        "thinking_budget_tokens": LLMModelIntegerParameterValue(
                            parameter_definition=_ACME_LARGE_THINKING_BUDGET_TOKENS_PARAMETER,
                            value=0,
                        ),
                    },
                ),
                LLMModelConfig(
                    name="ACME_LARGE_DETERMINISTIC",
                    base_model=_ACME_LARGE,
                    model="acme-large-001",
                    parameter_values={
                        "temperature": LLMModelFloatParameterValue(
                            parameter_definition=_ACME_LARGE_TEMPERATURE_PARAMETER,
                            value=0.0,
                        ),
                        "thinking_budget_tokens": LLMModelIntegerParameterValue(
                            parameter_definition=_ACME_LARGE_THINKING_BUDGET_TOKENS_PARAMETER,
                            value=0,
                        ),
                    },
                ),
                LLMModelConfig(
                    name="GLOBEX_BASIC_DEFAULT",
                    base_model=_GLOBEX_BASIC,
                    model="globex-basic-001",
                    parameter_values={},
                ),
            ],
        )
        self.assertEqual(expected, load_llm_model_registry(config_module=fake_config))

    def test_parse_param_value_below_min(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Value [0] for parameter [max_output_tokens] is outside the "
                "allowed range [1, 100]."
            ),
        ):
            LLMModelRegistry.from_yaml(_bad_registry_path("param_value_below_min.yaml"))

    def test_parse_param_value_above_max(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Value [101] for parameter [max_output_tokens] is outside the "
                "allowed range [1, 100]."
            ),
        ):
            LLMModelRegistry.from_yaml(_bad_registry_path("param_value_above_max.yaml"))

    def test_parse_unknown_pinned_version(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Model config [ACME_SMALL_DEFAULT] uses model [acme-small-999], "
                "which is not in any base model's known_pinned_versions. Known "
                "versions: ['acme-small-001']."
            ),
        ):
            LLMModelRegistry.from_yaml(
                _bad_registry_path("unknown_pinned_version.yaml")
            )

    def test_parse_unknown_parameter(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Parameter [nonsense] is not a tunable parameter of base model "
                "[acme-small]. Allowed parameters: ['max_output_tokens']."
            ),
        ):
            LLMModelRegistry.from_yaml(_bad_registry_path("unknown_parameter.yaml"))

    def test_parse_missing_required_parameter_value(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Model config [ACME_SMALL_DEFAULT] is missing values for "
                "required parameters of base model [acme-small]: "
                "['max_output_tokens']."
            ),
        ):
            LLMModelRegistry.from_yaml(
                _bad_registry_path("missing_required_parameter_value.yaml")
            )

    def test_parse_float_value_for_integer_parameter(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Parameter [max_output_tokens] in model config "
                "[ACME_SMALL_DEFAULT] must be an integer, found [50.5]."
            ),
        ):
            LLMModelRegistry.from_yaml(
                _bad_registry_path("float_value_for_integer_parameter.yaml")
            )

    def test_parse_unknown_model_family(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Base model [acme-small] references unknown model family "
                "[tyrell]. Known families: ['acme']."
            ),
        ):
            LLMModelRegistry.from_yaml(_bad_registry_path("unknown_model_family.yaml"))

    def test_parse_duplicate_pinned_version_across_base_models(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Pinned version [acme-small-001] is declared by multiple base "
                "models: [acme-small] and [acme-small-copy]."
            ),
        ):
            LLMModelRegistry.from_yaml(
                _bad_registry_path("duplicate_pinned_version_across_base_models.yaml")
            )

    def test_parse_allowed_range_wrong_length(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Parameter [max_output_tokens] must declare its allowed_range "
                "as a two-element [min, max] list, found [[1, 50, 100]]."
            ),
        ):
            LLMModelRegistry.from_yaml(
                _bad_registry_path("allowed_range_wrong_length.yaml")
            )

    def test_parse_allowed_range_min_greater_than_max(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Parameter [max_output_tokens] declares an invalid range: "
                "minimum [100] is greater than maximum [1]."
            ),
        ):
            LLMModelRegistry.from_yaml(
                _bad_registry_path("allowed_range_min_greater_than_max.yaml")
            )
